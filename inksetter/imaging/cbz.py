"""
CBZ repacking for the download path.
"""

import re
import time
import pyvips
import logging
import zipfile
import itertools
import collections
import concurrent.futures

from . import webtoon
from .pipeline import RenderFailed, render_page, same_picture
from .profiles import Profile, embedded_cover_for
from .. import cores

log = logging.getLogger(__name__)

DEFAULT_PAGE_WORKERS = cores.page_workers()
IMAGE_SUFFIXES = ('.jpg', '.jpeg', '.png', '.webp', '.gif', '.bmp', '.avif', '.jxl')


_NUM = re.compile(r'(\d+)')
_COMICINFO = 'comicinfo.xml'
_JPEG_SOI = b'\xff\xd8\xff'
_PNG_SIG = b'\x89PNG\r\n\x1a\n'


def natural_key(name: str):
    return [int(t) if t.isdecimal() else t.lower() for t in _NUM.split(name)]


def is_image(name: str) -> bool:
    lower = name.lower()
    return lower.endswith(IMAGE_SUFFIXES) and not lower.split('/')[-1].startswith('.')


def _suffix_for(blob: bytes, fallback_name: str) -> str:
    if blob.startswith(_JPEG_SOI):
        return '.jpg'
    if blob.startswith(_PNG_SIG):
        return '.png'
    return fallback_name[fallback_name.rfind('.') :]


def _entries(zin, infos):
    index = 0
    for info in infos:
        if not is_image(info.filename):
            yield ('copy', info.filename, zin.read(info))
            continue
        index += 1
        yield ('page', index, info.filename, zin.read(info))


def _render(job, profile: Profile):
    if job[0] == 'cover':
        return render_page(job[1], embedded_cover_for(profile))
    if job[0] == 'tile':
        return webtoon.render_tile(job, profile)
    return render_page(job[3], profile)


_UNWRITTEN = object()


def _emit_cover(zout, pad: int, render, *args):
    try:
        blob, _mime = render(*args)
    except Exception:
        blob = None
    if not blob:
        return _UNWRITTEN
    zout.writestr(f'{0:0{pad}d}{_suffix_for(blob, "cover.png")}', blob)
    return _dimensions(blob)


def _dimensions(blob: bytes) -> tuple[int, int] | None:
    try:
        im = pyvips.Image.new_from_buffer(blob, '')
    except pyvips.Error:
        return None
    return im.width, im.height


def _emit(zout, job, blob: bytes | None, pad: int) -> tuple[int, int] | None:
    if job[0] == 'copy':
        zout.writestr(job[1], job[2])
        return None
    index, name, piece = job[1:4]
    if blob is None:
        blob = piece if job[0] == 'page' else webtoon.tile_image(piece).pngsave_buffer()
    zout.writestr(f'{index:0{pad}d}{_suffix_for(blob, name)}', blob)
    return _dimensions(blob)


def _emit_rendered(zout, job, pad: int, render, *args) -> tuple[int, int] | None:
    try:
        blob, _ = render(*args)
    except Exception as exc:
        log.warning(
            'page %s failed to render; shipping it unchanged', job[2], exc_info=not isinstance(exc, RenderFailed)
        )
        blob = None
    return _emit(zout, job, blob, pad)


def _run(jobs, zout, profile: Profile, workers: int, pad: int):
    if workers > 1:
        return _run_pooled(jobs, zout, profile, workers, pad)
    return _run_serial(jobs, zout, profile, pad)


def _run_serial(jobs, zout, profile: Profile, pad: int):
    for job in jobs:
        if job[0] == 'copy':
            yield job, _emit(zout, job, None, pad)
        elif job[0] == 'cover':
            yield job, _emit_cover(zout, pad, _render, job, profile)
        else:
            yield job, _emit_rendered(zout, job, pad, _render, job, profile)


def _run_pooled(jobs, zout, profile: Profile, workers: int, pad: int):
    window = workers * 2
    pending: collections.deque = collections.deque()

    def flush_one() -> tuple[tuple, tuple[int, int] | None]:
        job, fut = pending.popleft()
        if fut is None:
            return job, _emit(zout, job, None, pad)
        if job[0] == 'cover':
            return job, _emit_cover(zout, pad, fut.result)
        return job, _emit_rendered(zout, job, pad, fut.result)

    def ready() -> bool:
        fut = pending[0][1]
        return len(pending) >= window or fut is None or fut.done()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix='repack') as pool:
        try:
            for job in jobs:
                fut = None if job[0] == 'copy' else pool.submit(_render, job, profile)
                pending.append((job, fut))
                while pending and ready():
                    yield flush_one()
            while pending:
                yield flush_one()
        except BaseException:
            pool.shutdown(cancel_futures=True)
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
    infos = sorted((i for i in zin.infolist() if not i.is_dir()), key=lambda i: natural_key(i.filename))
    declared = sum(i.file_size for i in infos)
    images = sum(1 for i in infos if is_image(i.filename))
    log.info(
        'repack: %d entries, %d image(s), %.0f MB declared, %d worker(s)',
        len(infos),
        images,
        declared / 1e6,
        workers,
    )

    lead = None
    if cover:
        first = next((i for i in infos if is_image(i.filename)), None)
        if first is not None and not same_picture(cover, zin.read(first)):
            lead = cover
            log.info('repack: prepending the server cover, %d kB', len(cover) // 1024)
        else:
            log.debug('repack: the cover is already page 1, not prepending')

    if any(i.filename.lower().endswith(_COMICINFO) for i in infos):
        comicinfo = None

    strip = profile.reslice
    pad = webtoon.STRIP_PAD if strip else 4
    if strip:
        log.info('repack: re-cutting the strip into %d px pages', profile.height)

    total_pages = images + (lead is not None)

    def tell(done: int) -> None:
        if progress is not None:
            progress(done, total_pages)

    def chunks():
        begun = time.perf_counter()
        written = 0
        pages = 0
        sizes: list[tuple[int, int] | None] = []
        tell(0)
        try:
            sink = _StreamSink()
            zout = zipfile.ZipFile(sink, 'w', zipfile.ZIP_STORED)
            try:
                consumed: dict[int, int] = {}
                jobs = webtoon.strip_tiles(_entries(zin, infos), profile, consumed) if strip else _entries(zin, infos)
                if lead is not None:
                    jobs = itertools.chain([('cover', lead)], jobs)
                for job, size in _run(jobs, zout, profile, workers, pad):
                    if job[0] == 'cover':
                        if size is not _UNWRITTEN:
                            sizes.append(size)
                        pages += 1
                        tell(pages)
                        block = sink.drain()
                        if block:
                            yield block
                        continue
                    written += 1
                    if job[0] != 'copy':
                        sizes.append(size)
                    if job[0] == 'page':
                        pages += 1
                        tell(pages)
                    elif job[0] == 'tile':
                        tell(pages + min(consumed.pop(job[1]), max(0, images - 1)))
                    block = sink.drain()
                    if block:
                        yield block
                if strip:
                    tell(total_pages)
                meta = _metadata(comicinfo, sizes)
                if meta:
                    zout.writestr('ComicInfo.xml', meta)
            finally:
                zout.close()
            tail = sink.drain()
            if tail:
                yield tail
            log.info('repack: finished, %d entr(y/ies) written in %.1f s', written, time.perf_counter() - begun)
        finally:
            zin.close()

    return chunks()


def _metadata(comicinfo, sizes: list[tuple[int, int] | None]) -> bytes | None:
    if comicinfo is None:
        return None
    try:
        return comicinfo(sizes)
    except Exception:
        log.warning('repack: the metadata factory failed; the volume ships without ComicInfo', exc_info=True)
        return None
