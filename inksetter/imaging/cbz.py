"""
CBZ repacking for the download path.
"""

import re
import time
import logging
import zipfile
import collections
import concurrent.futures

from .pipeline import render_page, same_picture
from .profiles import Profile, embedded_cover_for

log = logging.getLogger(__name__)

DEFAULT_PAGE_WORKERS = 3
IMAGE_SUFFIXES = ('.jpg', '.jpeg', '.png', '.webp', '.gif', '.bmp', '.avif', '.jxl')


_NUM = re.compile(r'(\d+)')
_COMICINFO = 'comicinfo.xml'
_JPEG_SOI = b'\xff\xd8\xff'
_PNG_SIG = b'\x89PNG\r\n\x1a\n'


def natural_key(name: str):
    return [int(t) if t.isdigit() else t.lower() for t in _NUM.split(name)]


def is_image(name: str) -> bool:
    lower = name.lower()
    return lower.endswith(IMAGE_SUFFIXES) and not lower.split('/')[-1].startswith('.')


def _suffix_for(blob: bytes, fallback_name: str) -> str:
    if blob.startswith(_JPEG_SOI):
        return '.jpg'
    if blob.startswith(_PNG_SIG):
        return '.png'
    dot = fallback_name.rfind('.')
    return fallback_name[dot:] if dot > 0 else '.jpg'


def _entries(zin, names):
    index = 0
    for name in names:
        if not is_image(name):
            yield ('copy', name, zin.read(name))
            continue
        index += 1
        yield ('page', index, name, zin.read(name))


def _emit(zout, job, blob: bytes | None) -> None:
    if job[0] == 'copy':
        zout.writestr(job[1], job[2])
        return
    _, index, name, piece = job
    if blob is None:
        zout.writestr(f'{index:04d}{_suffix_for(piece, name)}', piece)
        return
    zout.writestr(f'{index:04d}{_suffix_for(blob, name)}', blob)


def _run(jobs, zout, profile: Profile, workers: int):
    if workers > 1:
        return _run_pooled(jobs, zout, profile, workers)
    return _run_serial(jobs, zout, profile)


def _run_serial(jobs, zout, profile: Profile):
    for job in jobs:
        if job[0] == 'copy':
            _emit(zout, job, None)
            yield job[0]
            continue
        try:
            blob, _ = render_page(job[3], profile)
        except Exception:
            log.warning('page %s failed to render; shipping it unchanged', job[2], exc_info=True)
            blob = None
        _emit(zout, job, blob)
        yield job[0]


def _run_pooled(jobs, zout, profile: Profile, workers: int):
    window = max(2, workers * 2)
    pending: collections.deque = collections.deque()

    def flush_one() -> str:
        job, fut = pending.popleft()
        if fut is None:
            _emit(zout, job, None)
            return job[0]
        try:
            blob, _ = fut.result()
        except Exception:
            log.warning('page %s failed to render; shipping it unchanged', job[2], exc_info=True)
            blob = None
        _emit(zout, job, blob)
        return job[0]

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix='repack') as pool:
        try:
            for job in jobs:
                fut = None if job[0] == 'copy' else pool.submit(render_page, job[3], profile)
                pending.append((job, fut))
                while len(pending) >= window:
                    yield flush_one()
            while pending:
                yield flush_one()
        except BaseException:
            for _, fut in pending:
                if fut is not None:
                    fut.cancel()
            raise


class _StreamSink:
    __slots__ = ('_parts',)

    def __init__(self):
        self._parts: list[bytes] = []

    def write(self, data) -> int:
        self._parts.append(bytes(data))
        return len(data)

    def flush(self) -> None:
        pass

    def drain(self) -> bytes:
        if not self._parts:
            return b''
        out = b''.join(self._parts)
        self._parts.clear()
        return out


def repack_iter(
    src,
    profile: Profile,
    workers: int = DEFAULT_PAGE_WORKERS,
    *,
    cover: bytes | None = None,
    comicinfo=None,
    progress=None,
):
    zin = zipfile.ZipFile(src)
    infos = [i for i in zin.infolist() if not i.is_dir()]
    declared = sum(i.file_size for i in infos)
    names = sorted((i.filename for i in infos), key=natural_key)
    images = sum(1 for x in names if is_image(x))
    log.info(
        'repack: %d entries, %d image(s), %.0f MB declared, %d worker(s)',
        len(infos),
        images,
        declared / 1e6,
        workers,
    )

    lead = None
    if cover:
        first = next((n for n in names if is_image(n)), None)
        if first is not None and not same_picture(cover, zin.read(first)):
            lead = cover
            log.info('repack: prepending the server cover, %d kB', len(cover) // 1024)
        else:
            log.debug('repack: the cover is already page 1, not prepending')

    meta_blob = None
    if comicinfo is not None and not any(n.lower().endswith(_COMICINFO) for n in names):
        try:
            meta_blob = comicinfo(images + (lead is not None))
        except Exception:
            meta_blob = None

    total_pages = images + (lead is not None)

    def tell(done: int) -> None:
        if progress is not None:
            progress(done, total_pages)

    def chunks():
        begun = time.perf_counter()
        written = 0
        pages = 0
        tell(0)
        try:
            sink = _StreamSink()
            zout = zipfile.ZipFile(sink, 'w', zipfile.ZIP_STORED)
            try:
                if meta_blob:
                    zout.writestr('ComicInfo.xml', meta_blob)
                    block = sink.drain()
                    if block:
                        yield block
                if lead is not None:
                    try:
                        blob, _mime = render_page(lead, embedded_cover_for(profile))
                    except Exception:
                        blob = None
                    if blob:
                        zout.writestr(f'0000{_suffix_for(blob, "cover.png")}', blob)
                        block = sink.drain()
                        if block:
                            yield block
                    pages += 1
                    tell(pages)

                for kind in _run(_entries(zin, names), zout, profile, workers):
                    written += 1
                    if kind == 'page':
                        pages += 1
                        tell(pages)
                    block = sink.drain()
                    if block:
                        yield block
            finally:
                zout.close()
            tail = sink.drain()
            if tail:
                yield tail
            log.info('repack: finished, %d entr(y/ies) written in %.1f s', written, time.perf_counter() - begun)
        finally:
            zin.close()

    return chunks()


def repack_to(
    src,
    dst,
    profile: Profile,
    workers: int = DEFAULT_PAGE_WORKERS,
) -> None:
    for block in repack_iter(src, profile, workers):
        dst.write(block)
