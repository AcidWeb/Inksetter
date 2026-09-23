"""
CBZ repacking for the download path.
"""

import re
import time
import numpy as np
import pyvips
import logging
import zipfile
import functools
import dataclasses
import collections
import concurrent.futures

from . import pipeline
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
    return [int(t) if t.isdecimal() else t.lower() for t in _NUM.split(name)]


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


def _entries(zin, infos):
    index = 0
    for info in infos:
        if not is_image(info.filename):
            yield ('copy', info.filename, zin.read(info))
            continue
        index += 1
        yield ('page', index, info.filename, zin.read(info))


STRIP_GUTTER_FLOOR = 0.50
STRIP_OVERSHOOT = 2.00
STRIP_MIN_FILL = 0.85
STRIP_QUIET = 12
STRIP_GUTTER_MIN = 0.01
STRIP_TOP_MARGIN = 0.02
STRIP_GUTTER_WHITE = 235
STRIP_GUTTER_BLACK = 30
STRIP_PAD = 5


def _strip_rows(blob: bytes, p: Profile) -> np.ndarray | None:
    try:
        im = pipeline.open_image(blob).autorot()
        if im.hasalpha():
            im = im.flatten(background=255)
        im = pipeline.fit_to_width(im, p.width, p)
        if im.bands == 1:
            im = im.colourspace('srgb')
        elif im.bands > 3:
            im = im.extract_band(0, n=3)
        a = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width, im.bands))
    except Exception:
        log.warning('strip: an entry would not decode; it is left out of the page', exc_info=True)
        return None
    return a if a.size else None


def _spread(rows: np.ndarray) -> np.ndarray:
    return rows.max(axis=(1, 2)).astype(np.int16) - rows.min(axis=(1, 2))


def _gutter(rows: np.ndarray) -> np.ndarray:
    lo, hi = rows.min(axis=(1, 2)), rows.max(axis=(1, 2))
    flat = hi.astype(np.int16) - lo <= STRIP_QUIET
    return flat & ((lo >= STRIP_GUTTER_WHITE) | (hi <= STRIP_GUTTER_BLACK))


def _long_runs(mask: np.ndarray, at_least: int) -> np.ndarray:
    edges = np.flatnonzero(np.diff(np.concatenate([[0], mask.astype(np.int8), [0]])))
    out = np.zeros(len(mask), bool)
    for start, stop in zip(edges[::2], edges[1::2], strict=True):
        if stop - start >= at_least:
            out[start:stop] = True
    return out


def _seam(block: np.ndarray, limit: int) -> int:
    lo = max(1, int(limit * STRIP_GUTTER_FLOOR))
    head = block[: round(limit * STRIP_OVERSHOOT)]
    whole = _spread(head)
    runs = _long_runs(whole <= STRIP_QUIET, max(1, round(limit * STRIP_GUTTER_MIN)))
    quiet, spread = runs[lo:], whole[lo:]
    up = np.flatnonzero(quiet[: limit - lo])
    if len(up):
        return lo + int(up[-1])
    down = np.flatnonzero(quiet[limit - lo :])
    if len(down):
        at = limit + int(down[0])
        blank = _gutter(head[at:])
        inside = np.flatnonzero(blank & runs[at:])
        if len(inside) and runs[at : at + int(inside[0])].all():
            return at + int(inside[0])
        return at
    floor = max(lo, int(limit * STRIP_MIN_FILL))
    tail = spread[floor - lo : limit - lo]
    return floor + len(tail) - 1 - int(np.argmin(tail[::-1]))


def _leading_blank(a: np.ndarray, step: int) -> int:
    done = 0
    while done < len(a):
        blank = _gutter(a[done : done + step])
        if not blank.all():
            return done + int(np.argmin(blank))
        done += step
    return len(a)


def _strip_tiles(zin, infos, p: Profile, consumed: dict[int, int]):
    limit = round(p.width * p.aspect)
    reach = round(limit * STRIP_OVERSHOOT)
    margin = round(limit * STRIP_TOP_MARGIN)
    held: list[np.ndarray] = []
    rows = index = seen = 0
    opening = True
    lead = None

    def take(a: np.ndarray) -> None:
        nonlocal rows, opening, lead
        if opening:
            art = _leading_blank(a, limit)
            if art:
                run = a[:art] if lead is None else np.concatenate([lead, a[:art]])
                lead = run[len(run) - min(margin, len(run)) :]
            if art == len(a):
                return
            a = a[art:] if lead is None or not len(lead) else np.concatenate([lead, a[art:]])
            lead, opening = None, False
        held.append(a)
        rows += len(a)

    def cut_one(name: str):
        nonlocal held, rows, index, opening
        block = held[0] if len(held) == 1 else np.concatenate(held)
        at = _seam(block, limit) if len(block) > limit else len(block)
        index += 1
        consumed[index] = seen
        tile = block[:at].copy()
        held, rows, opening = [], 0, True
        if at < len(block):
            take(block[at:])
        return ('tile', index, name, tile)

    last = ''
    for info in infos:
        name = info.filename
        if not is_image(name):
            yield ('copy', name, zin.read(info))
            continue
        seen += 1
        last = name
        a = _strip_rows(zin.read(info), p)
        if a is not None:
            take(a)
            while rows > reach:
                yield cut_one(name)
    while rows > limit:
        yield cut_one(last)
    if rows:
        yield cut_one(last)


@functools.lru_cache(maxsize=16)
def _refit(p: Profile, fit: str) -> Profile:
    return dataclasses.replace(p, fit=fit, rotate_wide=False, autocrop=False, strip_folio=False)


def _tile_image(a: np.ndarray) -> pyvips.Image:
    return pyvips.Image.new_from_memory(a.tobytes(), a.shape[1], a.shape[0], a.shape[2], 'uchar')


def _render(job, profile: Profile):
    if job[0] == 'tile':
        tile = job[3]
        tall = len(tile) > round(profile.width * profile.aspect)
        return render_page(b'', _refit(profile, 'box' if tall else 'none'), source=_tile_image(tile))
    return render_page(job[3], profile)


def _dimensions(blob: bytes) -> tuple[int, int] | None:
    try:
        im = pyvips.Image.new_from_buffer(blob, '')
    except pyvips.Error:
        return None
    return im.width, im.height


def _emit(zout, job, blob: bytes | None, pad: int = 4) -> tuple[int, int] | None:
    if job[0] == 'copy':
        zout.writestr(job[1], job[2])
        return None
    _, index, name, piece = job
    if blob is None:
        blob = piece if job[0] == 'page' else _tile_image(piece).pngsave_buffer()
    zout.writestr(f'{index:0{pad}d}{_suffix_for(blob, name)}', blob)
    return _dimensions(blob)


def _run(jobs, zout, profile: Profile, workers: int, pad: int = 4):
    if workers > 1:
        return _run_pooled(jobs, zout, profile, workers, pad)
    return _run_serial(jobs, zout, profile, pad)


def _run_serial(jobs, zout, profile: Profile, pad: int = 4):
    for job in jobs:
        if job[0] == 'copy':
            yield job, _emit(zout, job, None, pad)
            continue
        try:
            blob, _ = _render(job, profile)
        except Exception:
            log.warning('page %s failed to render; shipping it unchanged', job[2], exc_info=True)
            blob = None
        yield job, _emit(zout, job, blob, pad)


def _run_pooled(jobs, zout, profile: Profile, workers: int, pad: int = 4):
    window = max(2, workers * 2)
    pending: collections.deque = collections.deque()

    def flush_one() -> tuple[tuple, tuple[int, int] | None]:
        job, fut = pending.popleft()
        if fut is None:
            return job, _emit(zout, job, None, pad)
        try:
            blob, _ = fut.result()
        except Exception:
            log.warning('page %s failed to render; shipping it unchanged', job[2], exc_info=True)
            blob = None
        return job, _emit(zout, job, blob, pad)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix='repack') as pool:
        try:
            for job in jobs:
                fut = None if job[0] == 'copy' else pool.submit(_render, job, profile)
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
    pad = STRIP_PAD if strip else 4
    if strip:
        log.info('repack: re-cutting the strip into %d px pages', round(profile.width * profile.aspect))

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
                if lead is not None:
                    try:
                        blob, _mime = render_page(lead, embedded_cover_for(profile))
                    except Exception:
                        blob = None
                    if blob:
                        zout.writestr(f'{0:0{pad}d}{_suffix_for(blob, "cover.png")}', blob)
                        sizes.append(_dimensions(blob))
                        block = sink.drain()
                        if block:
                            yield block
                    pages += 1
                    tell(pages)

                consumed: dict[int, int] = {}
                jobs = _strip_tiles(zin, infos, profile, consumed) if strip else _entries(zin, infos)
                for job, size in _run(jobs, zout, profile, workers, pad):
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
