"""
OCR module that erase a page number on the bottom of the page, before autocrop runs.
"""

import re
import logging
import numpy as np
import pyvips
import threading

from .. import cores

log = logging.getLogger(__name__)

BAND = 0.10  # of page height, measured from the bottom of the raw scan
SPECK_W, SPECK_H = 0.003, 0.004  # smaller than this is noise, not a mark
MARK_W, MARK_H = 0.12, 0.05  # larger than this is not a page number
JOIN_X, JOIN_Y = 0.014, 0.004  # digits this close are one number
HALO = 0.006  # clear ink required around a mark, of page size
LINE_W = 0.05  # a straight run of ink this long, of page width, is a panel border or a rule
DARK = 128  # a pixel at or below this is dark, above it light
GROUND = 0.5  # share of the ring around a mark that must be paper, light in long runs, not a stroke
BLACK = 64  # a white folio's ground is black, at or below this and in long runs, ...
BLACK_GROUND = 0.95  # ... over this share of its ring: a white mark on grey or textured art is art
CTX_X, CTX_Y = 0.02, 0.015  # context the reader is given around the mark
READ_W = 420  # crops are upscaled to at least this wide before reading
OVERLAP = 0.5  # how much the read text and the mark must cover each other
MIN_SCORE = 0.95  # a read the reader is less sure of than this is not trusted
LINE_SCORE = 0.9  # a mark read off as one line, two digits or more at this confidence, needs no detector
TALL = 3.0  # a mark this many times taller than wide may be set vertically, which only the detector reads
MAX_READS = 2  # marks read per page, lowest first, those on the page edge last
OCR_THREADS = 4  # onnxruntime otherwise takes every core and spins them between reads

_DASH = '-\\u2013\\u2014'
_FOLIO = re.compile(rf'^[{_DASH}.\s\[\(]*(\d{{1,4}})[{_DASH}.\s\]\)]*$')
_DIGIT = re.compile(r'\d')

_engine = None
_engine_lock = threading.Lock()
_engine_failed = False


def _reader():
    global _engine, _engine_failed
    if _engine is not None or _engine_failed:
        return _engine
    with _engine_lock:
        if _engine is None and not _engine_failed:
            try:
                from rapidocr_onnxruntime import RapidOCR
            except ImportError:
                _engine_failed = True
                log.warning('strip_folio is set but rapidocr-onnxruntime is absent; pages ship unchanged')
                return None
            try:
                _engine = RapidOCR(intra_op_num_threads=min(OCR_THREADS, cores.available()))
            except Exception:
                _engine_failed = True
                log.warning('strip_folio could not start its reader; pages ship unchanged', exc_info=True)
    return _engine


def _long_runs(mask: np.ndarray, length: int) -> np.ndarray:
    h, w = mask.shape
    step = np.diff(np.pad(mask, ((0, 0), (1, 1))).view(np.int8), axis=1).ravel()
    starts, ends = np.flatnonzero(step == 1), np.flatnonzero(step == -1)
    long = ends - starts >= length
    marker = np.zeros(step.size, np.int8)
    marker[starts[long]] = 1
    marker[ends[long]] = -1
    return np.cumsum(marker, dtype=np.int8).view(bool).reshape(h, w + 1)[:, :w]


def _grow(mask: np.ndarray) -> np.ndarray:
    rows = mask.copy()
    rows[1:] |= mask[:-1]
    rows[:-1] |= mask[1:]
    out = rows.copy()
    out[:, 1:] |= rows[:, :-1]
    out[:, :-1] |= rows[:, 1:]
    return out


def _first_row(h: int) -> int:
    return max(0, int(h * (1.0 - BAND)) - max(2, int(h * HALO)))


def _ruled(ink: np.ndarray, h: int, w: int) -> np.ndarray:
    ruled = _long_runs(ink, max(1, int(w * LINE_W)))
    ruled |= _long_runs(np.ascontiguousarray(ink.T), max(1, int(h * MARK_H))).T
    return ruled


def _marks(
    ink: np.ndarray,
    h: int,
    w: int,
    ruled: np.ndarray | None = None,
    ground: np.ndarray | None = None,
    share: float = GROUND,
) -> list[tuple[int, int, int, int]]:
    top = _first_row(h)
    y0 = int(h * (1.0 - BAND))
    if not ink[y0 - top :].any():
        return []
    hx, hy = max(2, int(w * HALO)), max(2, int(h * HALO))
    if ruled is None:
        ruled = _ruled(ink, h, w)
    free = ink & ~ruled
    band = free[y0 - top :]
    if not band.any():
        return []
    touching = _grow(ruled)
    bh, bw = band.shape
    img = pyvips.Image.new_from_memory((band.view(np.uint8) * np.uint8(255)).tobytes(), bw, bh, 1, 'uchar')
    jx, jy = max(1, int(w * JOIN_X)), max(1, int(h * JOIN_Y))
    row = pyvips.Image.new_from_list([[255] * (2 * jx + 1)])
    column = pyvips.Image.new_from_list([[255]] * (2 * jy + 1))
    grown = img.morph(row, 'dilate').morph(column, 'dilate')
    lab = np.ndarray(buffer=grown.labelregions().write_to_memory(), dtype=np.int32, shape=(bh, bw))
    ys, xs = np.nonzero(band)
    ids = lab[ys, xs]
    order = np.argsort(ids, kind='stable')
    ids, ys, xs = ids[order], ys[order], xs[order]
    bounds = [*np.searchsorted(ids, np.unique(ids)), len(ids)]
    out = []
    for i in range(len(bounds) - 1):
        s, e = bounds[i], bounds[i + 1]
        x0, x1 = int(xs[s:e].min()), int(xs[s:e].max())
        my0, my1 = y0 + int(ys[s:e].min()), y0 + int(ys[s:e].max())
        mw, mh = x1 - x0 + 1, my1 - my0 + 1
        if mw < w * SPECK_W or mh < h * SPECK_H or mw > w * MARK_W or mh > h * MARK_H:
            continue
        my, mx = ys[s:e], xs[s:e]
        edge = touching[my + (y0 - top), mx]
        if edge.any():
            core_y, core_x = my[~edge], mx[~edge]
            if not core_x.size:
                continue
            if core_x.max() - core_x.min() + 1 < w * SPECK_W or core_y.max() - core_y.min() + 1 < h * SPECK_H:
                continue
            mine = np.zeros((mh, mw), bool)
            mine[my - (my0 - y0), mx - x0] = True
            held = np.zeros_like(mine)
            held[my[edge] - (my0 - y0), mx[edge] - x0] = True
            if (_grow(held) & mine & ~held).any():
                continue
        ox0, oy0 = max(0, x0 - hx), max(0, my0 - hy)
        ox1, oy1 = min(w, x1 + 1 + hx), min(h, my1 + 1 + hy)
        halo = free[oy0 - top : oy1 - top, ox0:ox1].copy()
        halo[my0 - oy0 : my1 + 1 - oy0, x0 - ox0 : x1 + 1 - ox0] = False
        if halo.any():
            continue
        if ground is not None:
            ring = ground[oy0 - top : oy1 - top, ox0:ox1].sum() - ground[my0 - top : my1 + 1 - top, x0 : x1 + 1].sum()
            if ring < share * ((ox1 - ox0) * (oy1 - oy0) - mw * mh):
                continue
        out.append((x0, my0, x1, my1))
    return out


def _pixels(crop: pyvips.Image) -> np.ndarray:
    a = np.ndarray(buffer=crop.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(crop.height, crop.width))
    return np.repeat(a[:, :, None], 3, axis=2)


def _read(luma: pyvips.Image, mark: tuple[int, int, int, int], h: int, w: int):
    engine = _reader()
    if engine is None:
        return None
    x0, y0, x1, y1 = mark
    if y1 - y0 + 1 <= TALL * (x1 - x0 + 1):
        pad = max(2, (y1 - y0 + 1) // 4)
        lx0, ly0 = max(0, x0 - pad), max(0, y0 - pad)
        lx1, ly1 = min(w, x1 + 1 + pad), min(h, y1 + 1 + pad)
        line, _elapsed = engine(_pixels(luma.crop(lx0, ly0, lx1 - lx0, ly1 - ly0)), use_det=False, use_cls=False)
        text, score = (str(line[0][0]).strip(), float(line[0][1])) if line else ('', 0.0)
        m = _FOLIO.match(text)
        if m and len(m.group(1)) >= 2 and score >= LINE_SCORE:
            return mark
        if not _DIGIT.search(text):
            return None
    cx, cy = int(w * CTX_X), int(h * CTX_Y)
    rx0, ry0 = max(0, x0 - cx), max(0, y0 - cy)
    rx1, ry1 = min(w, x1 + 1 + cx), min(h, y1 + 1 + cy)
    crop = luma.crop(rx0, ry0, rx1 - rx0, ry1 - ry0)
    scale = max(1.0, READ_W / max(crop.width, 1))
    if scale > 1.0:
        crop = crop.resize(scale)
    found, _elapsed = engine(_pixels(crop))
    for item in found or []:
        m = _FOLIO.match(str(item[1]).strip())
        if not m:
            continue
        if m.group(1) == '0' or float(item[2]) < MIN_SCORE:
            continue
        pt = np.array(item[0], dtype=float) / scale
        tx0, tx1 = rx0 + pt[:, 0].min(), rx0 + pt[:, 0].max()
        ty0, ty1 = ry0 + pt[:, 1].min(), ry0 + pt[:, 1].max()
        inter = max(0.0, min(tx1, x1) - max(tx0, x0)) * max(0.0, min(ty1, y1) - max(ty0, y0))
        if inter / (float((x1 - x0) * (y1 - y0)) or 1.0) < OVERLAP:
            continue
        mx, my = (tx0 + tx1) / 2.0, (ty0 + ty1) / 2.0
        gx, gy = (x1 - x0) * 0.25, (y1 - y0) * 0.25
        if not (x0 - gx <= mx <= x1 + gx and y0 - gy <= my <= y1 + gy):
            continue
        return int(min(x0, tx0)), int(min(y0, ty0)), int(max(x1, tx1)), int(max(y1, ty1))
    return None


def _ground_colour(im: pyvips.Image, box: tuple[int, int, int, int], h: int, w: int) -> list[float]:
    x0, y0, x1, y1 = box
    hx, hy = max(2, int(w * HALO)), max(2, int(h * HALO))
    ox0, oy0 = max(0, x0 - hx), max(0, y0 - hy)
    ox1, oy1 = min(w, x1 + 1 + hx), min(h, y1 + 1 + hy)
    region = im.crop(ox0, oy0, ox1 - ox0, oy1 - oy0).cast('uchar')
    px = np.ndarray(buffer=region.write_to_memory(), dtype=np.uint8, shape=(oy1 - oy0, ox1 - ox0, im.bands))
    ring = np.ones(px.shape[:2], bool)
    ring[y0 - oy0 : y1 + 1 - oy0, x0 - ox0 : x1 + 1 - ox0] = False
    return [float(v) for v in np.median(px[ring], axis=0)]


def strip(im: pyvips.Image, luma: pyvips.Image, threshold: int) -> pyvips.Image:
    if _engine_failed:
        return im
    try:
        h, w = luma.height, luma.width
        if min(h, w) < 64:
            return im
        first = _first_row(h)
        rows = luma.crop(0, first, w, h - first).cast('uchar')
        a = np.ndarray(buffer=rows.write_to_memory(), dtype=np.uint8, shape=(h - first, w))
        dark, light = a < (255 - threshold), a > DARK
        druled, lruled = _ruled(dark, h, w), _ruled(light, h, w)
        paper, bleed = lruled, druled & (a <= BLACK)
        found = []
        for ink, ruled, ground, share, white in (
            (dark, druled, paper, GROUND, False),
            (light, lruled, bleed, BLACK_GROUND, True),
        ):
            found += [(m, ruled, white) for m in _marks(ink, h, w, ruled, ground, share)]
        found.sort(key=lambda f: (f[0][0] == 0 or f[0][2] == w - 1 or f[0][3] == h - 1, -f[0][3]))
        boxes = [(b, ruled, white) for m, ruled, white in found[:MAX_READS] if (b := _read(luma, m, h, w))]
        if not boxes:
            return im
        log.debug('    folio erased %d mark(s) from the bottom margin', len(boxes))
        out = im
        for (x0, y0, x1, y1), ruled, white in boxes:
            left, top = max(0, x0 - 2), max(0, y0 - 2)
            right, bottom = min(w, x1 + 3), min(h, y1 + 3)
            fill = _ground_colour(im, (left, top, right - 1, bottom - 1), h, w) if white else [255] * im.bands
            patch = pyvips.Image.black(right - left, bottom - top).new_from_image(fill).cast(im.format)
            keep = np.zeros((bottom - top, right - left), bool)
            if bottom > first:
                keep[max(0, first - top) :] = ruled[max(0, top - first) : bottom - first, left:right]
            if keep.any():
                mask = pyvips.Image.new_from_memory(
                    (keep * np.uint8(255)).tobytes(), right - left, bottom - top, 1, 'uchar'
                )
                patch = mask.ifthenelse(out.crop(left, top, right - left, bottom - top), patch)
            out = out.insert(patch, left, top)
    except pyvips.Error, MemoryError, ValueError:
        return im
    return out
