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
CTX_X, CTX_Y = 0.02, 0.015  # context the reader is given around the mark
READ_W = 420  # crops are upscaled to at least this wide before reading
OVERLAP = 0.5  # how much the read text and the mark must cover each other
MAX_READS = 2  # marks read per page, lowest first
OCR_THREADS = 4  # onnxruntime otherwise takes every core and spins them between reads

_DASH = '-\\u2013\\u2014'
_FOLIO = re.compile(rf'^[{_DASH}.\s\[\(]*(\d{{1,4}})[{_DASH}.\s\]\)]*$')

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


def _marks(ink: np.ndarray, h: int, w: int) -> list[tuple[int, int, int, int]]:
    y0 = int(h * (1.0 - BAND))
    band = ink[y0:, :]
    if not band.any():
        return []
    bh, bw = band.shape
    img = pyvips.Image.new_from_memory((band * 255).astype(np.uint8).tobytes(), bw, bh, 1, 'uchar')
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
        hx, hy = max(2, int(w * HALO)), max(2, int(h * HALO))
        ox0, oy0 = max(0, x0 - hx), max(0, my0 - hy)
        ox1, oy1 = min(w, x1 + 1 + hx), min(h, my1 + 1 + hy)
        halo = ink[oy0:oy1, ox0:ox1].copy()
        halo[my0 - oy0 : my1 + 1 - oy0, x0 - ox0 : x1 + 1 - ox0] = False
        if halo.any():
            continue
        out.append((x0, my0, x1, my1))
    return out


def _read(luma: pyvips.Image, mark: tuple[int, int, int, int], h: int, w: int):
    x0, y0, x1, y1 = mark
    cx, cy = int(w * CTX_X), int(h * CTX_Y)
    rx0, ry0 = max(0, x0 - cx), max(0, y0 - cy)
    rx1, ry1 = min(w, x1 + 1 + cx), min(h, y1 + 1 + cy)
    crop = luma.crop(rx0, ry0, rx1 - rx0, ry1 - ry0)
    scale = max(1.0, READ_W / max(crop.width, 1))
    if scale > 1.0:
        crop = crop.resize(scale)
    a = np.ndarray(buffer=crop.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(crop.height, crop.width))
    engine = _reader()
    if engine is None:
        return None
    found, _elapsed = engine(np.repeat(a[:, :, None], 3, axis=2))
    for item in found or []:
        m = _FOLIO.match(str(item[1]).strip())
        if not m:
            continue
        if m.group(1) == '0':
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


def strip(im: pyvips.Image, luma: pyvips.Image, threshold: int) -> pyvips.Image:
    if _engine_failed:
        return im
    try:
        h, w = luma.height, luma.width
        if min(h, w) < 64:
            return im
        a = np.ndarray(buffer=luma.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(h, w))
        ink = a < (255 - threshold)
        marks = sorted(_marks(ink, h, w), key=lambda m: -m[3])[:MAX_READS]
        boxes = [b for b in (_read(luma, m, h, w) for m in marks) if b is not None]
        if not boxes:
            return im
        log.debug('    folio erased %d mark(s) from the bottom margin', len(boxes))
        out = im
        for x0, y0, x1, y1 in boxes:
            left, top = max(0, x0 - 2), max(0, y0 - 2)
            right, bottom = min(w, x1 + 3), min(h, y1 + 3)
            white = pyvips.Image.black(right - left, bottom - top).new_from_image([255] * im.bands)
            out = out.insert(white.cast(im.format), left, top)
    except pyvips.Error, MemoryError, ValueError:
        return im
    return out
