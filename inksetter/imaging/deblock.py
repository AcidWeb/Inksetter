"""
Smoothing of the flat areas of a colour page.
Lower quality input is not cooperating with Kindle Scribe Colorsoft rendering pipeline.
This filter mitigates part of the problem.
"""

import functools
import pyvips
import numpy as np
from scipy import ndimage

FLAT_L = 1.0
FLAT_C = 1.5
FLAT_BLUR = 1.5
FLAT_ERODE = 5

REACH_L = 16
REACH_C = 32
_ACROSS = (12 / 44, 20 / 44, 12 / 44)

RANGE = 2.5
_NEAR = (0.472, 0.050)

MOST_L = 2.5
MOST_C = 5.0

SQUARE = 4
_BILINEAR = pyvips.Interpolate.new('bilinear')


def _array(im: pyvips.Image) -> np.ndarray:
    shape = (im.height, im.width, im.bands)
    return np.ndarray(buffer=im.cast('float').write_to_memory(), dtype=np.float32, shape=shape)


def _image(a: np.ndarray) -> pyvips.Image:
    a = np.ascontiguousarray(a, dtype=np.float32)
    return pyvips.Image.new_from_memory(a, a.shape[1], a.shape[0], a.shape[2], 'float')


def _enlarged(im: pyvips.Image, zoom: int) -> pyvips.Image:
    at = 0.5 - 0.5 / zoom
    area = [0, 0, im.width * zoom, im.height * zoom]
    return im.affine([zoom, 0, 0, zoom], interpolate=_BILINEAR, idx=at, idy=at, oarea=area, extend='copy')


def flat_mask(lab: pyvips.Image) -> pyvips.Image:
    fine = (lab - lab.gaussblur(FLAT_BLUR)).abs()
    return (fine < [FLAT_L, FLAT_C, FLAT_C]).bandand().rank(FLAT_ERODE, FLAT_ERODE, 0)


@functools.lru_cache(maxsize=8)
def _cells(shape: tuple[int, int], zoom: int) -> tuple[np.ndarray, tuple[tuple[np.ndarray, np.ndarray], ...]]:
    height, width = shape
    cols = width // zoom
    half = zoom // 2
    ys, xs = np.arange(height), np.arange(width)
    own = ((ys // zoom)[:, None] * cols + (xs // zoom)[None, :]).astype(np.intp)
    cy, cx = (ys + half) // zoom, (xs + half) // zoom
    fy = (((ys + half) % zoom + 0.5) / zoom).astype(np.float32)
    fx = (((xs + half) % zoom + 0.5) / zoom).astype(np.float32)
    corners = []
    for dy, wy in ((0, 1 - fy), (1, fy)):
        for dx, wx in ((0, 1 - fx), (1, fx)):
            cell = ((cy + dy)[:, None] * (cols + 2) + (cx + dx)[None, :]).astype(np.intp)
            corners.append((cell, np.outer(wy, wx)))
    for kept in (own, *(a for corner in corners for a in corner)):
        kept.setflags(write=False)
    return own, tuple(corners)


def _gathered(
    low: np.ndarray, part: np.ndarray, share: np.ndarray, mean: np.ndarray, levels: int, zoom: int
) -> np.ndarray:
    rows, cols = low.shape[0] // zoom, low.shape[1] // zoom
    at = (_cells(low.shape, zoom)[0] * levels + low).reshape(-1)
    above = at + 1
    size = rows * cols * levels
    upper = (share * part).reshape(-1)
    lower = share.reshape(-1) - upper
    grid = np.empty((rows, cols, levels, 2), np.float32)
    for plane in (1, 0):
        if plane == 0:
            value = mean.reshape(-1)
            upper *= value
            lower *= value
        summed = np.bincount(at, lower, size)
        summed += np.bincount(above, upper, size)
        grid[:, :, :, plane] = summed.reshape(rows, cols, levels)
    return grid


def _grid(low: np.ndarray, part: np.ndarray, share: np.ndarray, mean: np.ndarray, levels: int, zoom: int) -> np.ndarray:
    grid = _gathered(low, part, share, mean, levels, zoom)
    spread = ndimage.correlate1d(grid, (*_NEAR[::-1], 1.0, *_NEAR), axis=2, mode='constant')
    ndimage.correlate1d(spread, _ACROSS, axis=0, mode='nearest', output=grid)
    ndimage.correlate1d(grid, _ACROSS, axis=1, mode='nearest', output=spread)
    return spread.view(np.complex64)[:, :, :, 0]


def _read(grid: np.ndarray, low: np.ndarray, part: np.ndarray, zoom: int) -> np.ndarray:
    rows, cols, levels = grid.shape
    edged = np.zeros((rows + 2, cols + 2, levels), np.complex64)
    edged[1:-1, 1:-1] = grid
    values = edged.reshape(-1)
    at = np.empty(low.shape, np.intp)
    below, above = np.empty(low.shape, np.complex64), np.empty(low.shape, np.complex64)
    out = np.zeros(low.shape, np.complex64)
    for cell, weight in _cells(low.shape, zoom)[1]:
        np.multiply(cell, levels, out=at)
        at += low
        np.take(values, at, out=below, mode='clip')
        at += 1
        np.take(values, at, out=above, mode='clip')
        above -= below
        above *= part
        above += below
        above *= weight
        out += above
    return out


def _averaged(squares: np.ndarray, band: int, zoom: int) -> np.ndarray:
    share = squares[:, :, 3]
    held = share > 0
    mean = squares[:, :, band] / np.where(held, share, 1)
    spread = len(_NEAR) + 1
    first = int(np.floor(np.min(mean, where=held, initial=np.inf) / RANGE)) - spread
    levels = int(np.floor(np.max(mean, where=held, initial=-np.inf) / RANGE)) + spread + 2 - first
    at = mean / RANGE
    at -= first
    at[~held] = spread
    low = at.astype(np.intp)
    at -= low
    found = _read(_grid(low, at, share, mean, levels, zoom), low, at, zoom)
    return np.divide(found.real, found.imag, out=np.zeros(share.shape, np.float32), where=held)


def smooth_flats(lab: pyvips.Image) -> pyvips.Image:
    lab = lab.copy_memory()
    width, height = lab.width, lab.height
    wide, tall = -(-width // REACH_C) * REACH_C, -(-height // REACH_C) * REACH_C
    padded = lab.embed(0, 0, wide, tall, extend='copy')
    flat = flat_mask(lab).copy_memory()
    share = flat.embed(0, 0, wide, tall, extend='copy') / 255.0
    squares = _array((padded * share).bandjoin(share).shrink(SQUARE, SQUARE))
    held = squares[:, :, 3] > 0
    if not held.any():
        return lab
    averages = [_averaged(squares, band, reach // SQUARE) for band, reach in enumerate((REACH_L, REACH_C, REACH_C))]
    big = _enlarged(_image(np.stack([*averages, held], axis=2)), SQUARE).crop(0, 0, width, height)
    change = big.extract_band(0, n=3) / big[3] - lab
    light = change[0].clamp(min=-MOST_L, max=MOST_L)
    colour = change.extract_band(1, n=2).clamp(min=-MOST_C, max=MOST_C)
    return flat.ifthenelse(lab + light.bandjoin(colour), lab).copy_memory()
