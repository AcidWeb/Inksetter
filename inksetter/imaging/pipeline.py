"""
The e-ink page pipeline.

MONO (mono panels, and the B/W pages auto_mono sends there on Kaleido 3):
  decode+autorot -> flatten alpha -> folio remover -> autocrop
         -> rotate wide spreads -> descreen (FFT notch, only when downscaling)
         -> downscale or upscale -> pad to the panel box
         -> grayscale -> levels+gamma -> unsharp -> defringe
         -> quantise/dither -> encode (4-bit PNG, png8 or JPEG)

COLOUR (Kaleido 3):
  decode+autorot -> flatten alpha -> folio remover -> autocrop
         -> rotate wide spreads -> descreen (FFT notch, only when downscaling)
         -> downscale or upscale -> pad to the panel box
         -> LCh saturation boost -> Lab: blur a/b only
         -> levels+gamma, unsharp and defringe on L only
         -> sRGB -> re-flatten the pad -> encode (palette PNG, PNG or JPEG)
"""

import os
import time
import logging
import functools
import threading
import numpy as np
import pyvips
import scipy.fft as sfft
from typing import NamedTuple

from . import folio
from .profiles import UPSCALE_MAX_CEILING, Profile
from .. import cores


log = logging.getLogger(__name__)


# Control pyvips concurrency
def vips_threads(cores: int) -> int:
    return min(8, max(2, cores // 2))


def _size_vips() -> None:
    if not os.environ.get('VIPS_CONCURRENCY'):
        pyvips.concurrency_set(vips_threads(cores.available()))


_size_vips()

# Bump this whenever anything in this module changes in a way that alters output pixels. Bump invalidates the cache.
PIPELINE_VERSION = '8'

_BAYER_N = 8

NO_HEIGHT_CAP = 10_000_000


def _bayer(n: int = _BAYER_N) -> np.ndarray:
    m = np.array([[0]], dtype=np.float32)
    while m.shape[0] < n:
        m = np.block([[4 * m, 4 * m + 2], [4 * m + 3, 4 * m + 1]])
    return (m + 0.5) / m.size


_BAYER = _bayer()


def _to_numpy(im: pyvips.Image) -> np.ndarray:
    return np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width))


def _clamp(im: pyvips.Image, lo: float, hi: float) -> pyvips.Image:
    return (im < lo).ifthenelse(lo, (im > hi).ifthenelse(hi, im))


def _bayer_lut() -> np.ndarray:
    levels = np.arange(256, dtype=np.float32)
    rows = [np.clip(np.rint((levels + (t - 0.5) * 17.0) / 17.0), 0, 15) for t in _BAYER.ravel()]
    return np.concatenate(rows).astype(np.uint8)


_BAYER_LUT = _bayer_lut()


@functools.lru_cache(maxsize=8)
def _bayer_cells(h: int, w: int) -> np.ndarray:
    cell = (np.arange(h)[:, None] % _BAYER_N) * _BAYER_N + np.arange(w)[None, :] % _BAYER_N
    return (cell * 256).astype(np.int16)


_LEVEL_LUT = np.clip(np.rint(np.arange(256, dtype=np.float32) / 17.0), 0, 15).astype(np.uint8)


def _quantise16(a: np.ndarray, dither: str) -> np.ndarray:
    if dither == 'bayer':
        return _BAYER_LUT[_bayer_cells(*a.shape) + a]
    return _LEVEL_LUT[a]


# --------------------------------------------------------------------------
# Descreen
# --------------------------------------------------------------------------

_FFT_SLOTS = threading.BoundedSemaphore(2)


class UnreadableImage(ValueError):
    pass


def _open(buf: bytes) -> pyvips.Image:
    try:
        return _eight_bit(pyvips.Image.new_from_buffer(buf, ''))
    except pyvips.Error as exc:
        raise UnreadableImage(f'not a readable image ({len(buf)} bytes)') from exc


_INTEGER_TOP = {'ushort': 65535.0, 'short': 32767.0, 'uint': 4294967295.0, 'int': 2147483647.0, 'char': 127.0}


def _levels8(im: pyvips.Image, scale: float) -> pyvips.Image:
    return _clamp(im * scale, 0.0, 255.0).rint().cast('uchar')


def _eight_bit(im: pyvips.Image) -> pyvips.Image:
    if im.format == 'uchar' and im.interpretation != 'cmyk':
        return im
    if im.interpretation == 'cmyk' or (im.format == 'ushort' and im.interpretation in ('grey16', 'rgb16')):
        return im.colourspace('srgb')
    alpha = None
    if im.hasalpha():
        im, alpha = im.extract_band(0, n=im.bands - 1), im.extract_band(im.bands - 1)
    if im.format in ('float', 'double') and im.max() <= 1.0:
        colour = im if im.bands >= 3 else im.bandjoin([im, im])
        out = colour.copy(interpretation='scrgb').colourspace('srgb')
        out, scale = (out if im.bands >= 3 else out.extract_band(0)), 255.0
    elif im.format in ('float', 'double'):
        out, scale = _levels8(im, 1.0), 1.0
    elif im.format in _INTEGER_TOP:
        scale = 255.0 / _INTEGER_TOP[im.format]
        out = _levels8(im, scale)
    else:
        return (im if alpha is None else im.bandjoin(alpha)).colourspace('srgb')
    if alpha is not None:
        out = out.bandjoin(_levels8(alpha, scale))
    return out.copy(interpretation='b-w' if out.bands < 3 else 'srgb')


def _find_peaks(mag: np.ndarray, pixels: int, floor_freq: float, width: int, p: Profile) -> list[tuple[int, int]]:
    height, half = mag.shape
    floor = float(np.median(mag[::4, ::4]))
    cutoff = max(floor * p.descreen_peak_ratio, p.descreen_min_amplitude * pixels / 2.0)
    rows = min(height // 2, int(np.ceil(floor_freq * height)) + 1)
    cols = min(half, int(np.ceil(floor_freq * width)) + 1)
    mag[:rows, :cols] = 0.0
    mag[height - rows :, :cols] = 0.0
    sy = max(1, int(p.descreen_width * height))
    sx = max(1, int(p.descreen_width * width))

    peaks: list[tuple[int, int]] = []
    for _ in range(p.descreen_peaks):
        flat = int(np.argmax(mag))
        iy, ix = divmod(flat, half)
        if mag[iy, ix] <= cutoff:
            break
        peaks.append((iy, ix))
        y0, y1 = max(0, iy - sy), min(height, iy + sy + 1)
        x0, x1 = max(0, ix - sx), min(half, ix + sx + 1)
        mag[y0:y1, x0:x1] = 0.0
    return peaks


def _apply_notches(
    spectrum: np.ndarray,
    freq_y: np.ndarray,
    freq_x: np.ndarray,
    peaks: list[tuple[int, int]],
    p: Profile,
) -> bool:
    if not peaks:
        return False
    span = 3.0 * p.descreen_width
    denom = 2.0 * p.descreen_width * p.descreen_width
    for iy, ix in peaks:
        dy = freq_y - freq_y[iy]
        dy -= np.round(dy)
        rows = np.nonzero(np.abs(dy) <= span)[0]
        cols = np.nonzero(np.abs(freq_x - freq_x[ix]) <= span)[0]
        d2 = dy[rows][:, None] ** 2 + (freq_x[cols] - freq_x[ix])[None, :] ** 2
        window = 1.0 - p.descreen_strength * np.exp(-d2 / denom)
        spectrum[np.ix_(rows, cols)] *= window
    return True


def _descreen_plane(a: np.ndarray, floor_freq: float, p: Profile) -> np.ndarray | None:
    h, w = a.shape
    ph, pw = _next_fast_len(h), _next_fast_len(w)
    freq_y = np.fft.fftfreq(ph).astype(np.float32)
    freq_x = np.fft.rfftfreq(pw).astype(np.float32)

    spectrum = sfft.rfft2(a if (ph, pw) == (h, w) else np.pad(a, ((0, ph - h), (0, pw - w)), mode='edge'))
    magnitude = np.abs(spectrum)
    peaks = _find_peaks(magnitude, h * w, floor_freq, pw, p)
    del magnitude
    log.debug(
        '    descreen notching %d peak(s) of at most %d above %.3f cyc/px', len(peaks), p.descreen_peaks, floor_freq
    )

    if not _apply_notches(spectrum, freq_y, freq_x, peaks, p):
        del spectrum
        return None
    out = sfft.irfft(sfft.ifft(spectrum, axis=0, overwrite_x=True), n=pw, axis=1)
    del spectrum
    return out[:h, :w]


def descreen(im: pyvips.Image, p: Profile, scale: float) -> pyvips.Image:
    if im.width * im.height > p.descreen_max_megapixels * 1_000_000:
        log.debug(
            '    descreen skipped: %.2f Mpx over the %.0f Mpx ceiling',
            im.width * im.height / 1e6,
            p.descreen_max_megapixels,
        )
        return im

    floor_freq = max(p.descreen_min_freq, scale / 2.0)
    if floor_freq >= 0.45:
        log.debug('    descreen skipped: floor_freq %.3f, nothing left to alias', floor_freq)
        return im
    try:
        with _FFT_SLOTS:
            plane = _to_numpy(im if im.bands == 1 else im.colourspace('b-w')).astype(np.float32)
            filtered = _descreen_plane(plane, floor_freq, p)
            if filtered is None:
                log.debug('    descreen found no periodic peak worth notching')
                return im
            if p.descreen_deadband > (0.5 if im.bands == 1 else 0.0):
                change = np.subtract(filtered, plane)
                np.abs(change, out=change)
                np.copyto(filtered, plane, where=change < p.descreen_deadband)
                del change
            if im.bands == 1:
                np.rint(filtered, out=filtered)
                np.clip(filtered, 0, 255, out=filtered)
                out = filtered.astype(np.uint8)
                return pyvips.Image.new_from_memory(out.tobytes(), im.width, im.height, 1, 'uchar')
            np.subtract(filtered, plane, out=filtered)
            delta = filtered.astype(np.float32, copy=False)
            correction = pyvips.Image.new_from_memory(delta.tobytes(), im.width, im.height, 1, 'float')
            return (im.cast('float') + correction).rint().cast('uchar')
    except pyvips.Error, MemoryError, ValueError:
        return im


# --------------------------------------------------------------------------
# Defringe
# --------------------------------------------------------------------------
# Adapted from Kindle Comic Converter.


@functools.lru_cache(maxsize=64)
def _next_fast_len(n: int, limit: int = 7) -> int:
    m = n
    while True:
        rest = m
        for f in range(2, limit + 1):
            while rest % f == 0:
                rest //= f
        if rest == 1:
            return m
        m += 1


@functools.lru_cache(maxsize=4)
def _diagonal_attenuation(
    height: int,
    width: int,
    angle: float,
    tolerance: float,
    min_freq: float,
    strength: float,
) -> np.ndarray:
    freq_y = np.fft.fftfreq(height).astype(np.float32)
    freq_x = np.fft.rfftfreq(width).astype(np.float32)

    radial = np.sqrt(freq_y[:, None] ** 2 + freq_x[None, :] ** 2)
    np.maximum(radial, 1e-6, out=radial)
    high = 1.0 / (1.0 + (np.float32(min_freq) / radial) ** 4)
    del radial

    ang = np.rad2deg(np.arctan2(freq_y[:, None], freq_x[None, :]))
    ang -= np.float32(angle)
    ang %= np.float32(90.0)
    np.minimum(ang, 90.0 - ang, out=ang)
    wedge = np.exp(-((ang / np.float32(tolerance)) ** 2))
    del ang

    high *= wedge
    del wedge
    high *= np.float32(strength)
    return (1.0 - high).astype(np.float32)


def defringe(plane: pyvips.Image, p: Profile, top: float = 100.0, height: int = 0) -> pyvips.Image:
    if p.defringe != 'diagonal':
        return plane
    try:
        with _FFT_SLOTS:
            a = np.ndarray(
                buffer=plane.cast('float').write_to_memory(),
                dtype=np.float32,
                shape=(plane.height, plane.width),
            )
            h, w = a.shape
            ph, pw = _next_fast_len(max(h, height)), _next_fast_len(w)
            src = a if (ph, pw) == (h, w) else np.pad(a, ((0, ph - h), (0, pw - w)), mode='edge')
            spectrum = sfft.rfft2(src)
            atten = _diagonal_attenuation(
                ph,
                pw,
                p.defringe_angle,
                p.defringe_tolerance,
                p.defringe_min_freq,
                p.defringe_strength,
            )
            spectrum *= atten
            out = sfft.irfft2(spectrum, s=(ph, pw))
            del spectrum
            if (ph, pw) != (h, w):
                out = out[:h, :w]
            deadband = p.defringe_deadband * top / 100.0
            if deadband > 0:
                out = np.where(np.abs(out - a) < deadband, a, out)

            return pyvips.Image.new_from_memory(np.ascontiguousarray(out, dtype=np.float32).tobytes(), w, h, 1, 'float')
    except pyvips.Error, MemoryError, ValueError:
        return plane


_OPEN_DIVISOR = 133.0
_OPEN_MAX = 21
_OPEN_INK_TOLERANCE = 0.002


def _open_window(im: pyvips.Image) -> int:
    n = round(min(im.width, im.height) / _OPEN_DIVISOR) | 1
    return max(3, min(_OPEN_MAX, n))


def _ink_in(mask: pyvips.Image, box: tuple[int, int, int, int]) -> float:
    left, top, w, h = box
    if w <= 0 or h <= 0:
        return 0.0
    return float(mask.crop(left, top, w, h).avg()) * w * h


_LINE_INK = 0.9  # how solid the boundary line has to be
_LINE_CLEAR = 0.05  # how clear the strip just inside it has to be
_LINE_LOOK = 6  # how far inside to look
_LINE_MAX = 4  # never shave more than this off one side


def _strip_edge_lines(luma: pyvips.Image, threshold: int) -> tuple[int, int, int, int]:
    try:
        a = np.ndarray(
            buffer=luma.cast('uchar').write_to_memory(),
            dtype=np.uint8,
            shape=(luma.height, luma.width),
        )
    except pyvips.Error, ValueError:
        return (0, 0, 0, 0)
    ink = a < 255 - threshold
    if min(ink.shape) <= _LINE_LOOK * 2:
        return (0, 0, 0, 0)

    def shave(get_line, get_inside) -> int:
        n = 0
        while n < _LINE_MAX and get_line(n).mean() > _LINE_INK:
            n += 1
        if n == 0 or get_inside(n - 1).mean() > _LINE_CLEAR:
            return 0
        return n

    left = shave(lambda n: ink[:, n], lambda n: ink[:, n + 1 : n + 1 + _LINE_LOOK])
    right = shave(lambda n: ink[:, -1 - n], lambda n: ink[:, -1 - n - _LINE_LOOK : -1 - n])
    top = shave(lambda n: ink[n, :], lambda n: ink[n + 1 : n + 1 + _LINE_LOOK, :])
    bottom = shave(lambda n: ink[-1 - n, :], lambda n: ink[-1 - n - _LINE_LOOK : -1 - n, :])
    return (left, top, right, bottom)


def _dark_edges(luma: pyvips.Image) -> tuple[bool, bool, bool, bool]:
    try:
        return tuple(iqr <= _PAD_FLAT_IQR and med < _PAD_MIDPOINT for med, iqr in _side_stats(luma))
    except pyvips.Error, ValueError:
        return (False, False, False, False)


def _opened_trim(luma: pyvips.Image, window: int, fill: float, threshold: int):
    edge = window // 2
    ones = pyvips.Image.new_from_list([[1] * window])

    def find(background: int) -> list[int]:
        far = (luma < 255 - threshold) if background == 255 else (luma > threshold)
        count = (
            (far & 1)
            .cast('ushort')
            .embed(
                edge,
                edge,
                luma.width + 2 * edge,
                luma.height + 2 * edge,
                extend='background',
                background=[1 if abs(fill - background) > threshold else 0],
            )
            .convsep(ones, precision='integer')
        )
        opened = (count > (window * window) // 2).crop(edge, edge, luma.width, luma.height)
        return opened.find_trim(threshold=threshold, background=0)

    return find


def _autocrop_box(luma: pyvips.Image, p: Profile) -> tuple[int, int, int, int] | None:
    threshold = p.autocrop_threshold
    edges = _dark_edges(luma)
    all_dark = all(edges)
    sl, st, sr, sb = (0, 0, 0, 0) if all_dark else _strip_edge_lines(luma, threshold)
    ox, oy = sl, st
    if sl or st or sr or sb:
        luma = luma.crop(sl, st, luma.width - sl - sr, luma.height - st - sb)
        edges = _dark_edges(luma)

    def _trim(find):
        white = find(255)
        if not any(edges):
            return white
        ink = find(0)
        wl, wt, ww, wh = white
        il, it, iw, ih = ink
        x0 = il if edges[0] else wl
        y0 = it if edges[2] else wt
        x1 = (il + iw) if edges[1] else (wl + ww)
        y1 = (it + ih) if edges[3] else (wt + wh)
        return [x0, y0, max(0, x1 - x0), max(0, y1 - y0)]

    try:
        blurred = luma.gaussblur(2)
        plain = _trim(lambda background: blurred.find_trim(threshold=threshold, background=background))
        opened = _trim(_opened_trim(luma, _open_window(luma), 0.0 if all_dark else 255.0, threshold))
    except pyvips.Error:
        return None

    left, top, w, h = plain
    if min(opened[2], opened[3]) > 0 and opened[2] * opened[3] <= w * h:
        mask = (luma > threshold) if all_dark else (luma < (255 - threshold))
        base = _ink_in(mask, plain)
        if base > 0 and (base - _ink_in(mask, opened)) / base <= _OPEN_INK_TOLERANCE:
            left, top, w, h = opened
    if w <= 0 or h <= 0:
        return None
    if (w * h) / float(luma.width * luma.height) < (1.0 - p.autocrop_max_frac):
        return None
    pad = 2
    left += ox
    top += oy
    left = max(0, left - pad)
    top = max(0, top - pad)
    full_w, full_h = luma.width + sl + sr, luma.height + st + sb
    return (
        left,
        top,
        min(full_w - left, w + 2 * pad),
        min(full_h - top, h + 2 * pad),
    )


def _chroma_of(im: pyvips.Image) -> float:
    if im.bands < 3:
        return 0.0
    t = im.thumbnail_image(64, size='down')
    if t.hasalpha():
        t = t.flatten(background=255)
    return float(t.colourspace('lch')[1].avg())


class _Geom(NamedTuple):
    image: pyvips.Image
    content: tuple[int, int, int, int]
    upscale: float
    pad: float | None = None


def _geometry(buf: bytes, p: Profile, tw: int, th: int, mono: bool, page: pyvips.Image | None = None) -> _Geom:
    chatty = log.isEnabledFor(logging.DEBUG)
    mark = time.perf_counter() if chatty else 0.0

    def step(what: str, extra: str = '') -> None:
        nonlocal mark
        now = time.perf_counter()
        log.debug('  %-10s %5.0f ms  %dx%d %s', what, (now - mark) * 1000, im.width, im.height, extra)
        mark = now

    im = _open(buf).autorot() if page is None else page
    if chatty:
        im = im.copy_memory()
        step('decode', f'{im.bands} band(s), {len(buf) / 1024:.0f} kB in')
    if im.hasalpha():
        im = im.flatten(background=255)
        if chatty:
            step('flatten', 'alpha over white')
    luma = None
    if p.strip_folio:
        before = im
        luma = im if im.bands == 1 else im.colourspace('b-w').copy_memory()
        im = folio.strip(im, luma, p.autocrop_threshold)
        if im is not before:
            luma = None
            if chatty:
                step('folio', 'page number erased from the bottom margin')
    if p.autocrop:
        if luma is None:
            luma = im if im.bands == 1 else im.colourspace('b-w')
        box = _autocrop_box(luma, p)
        if box is not None:
            im = im.crop(*box)
        if chatty:
            step('autocrop', f'box={box}' if box else 'refused, whole page kept')

    if p.rotate_wide and im.width > im.height and th > tw:
        im = im.rot90()
        if chatty:
            step('rotate', 'landscape spread onto a portrait panel')

    upscale = 1.0
    scale = min(tw / im.width, th / im.height)
    if scale < 1.0:
        if p.descreen == 'always' or (p.descreen == 'mono' and mono):
            before = (im.width, im.height)
            im = descreen(im, p, scale)
            if chatty:
                step(
                    'descreen',
                    f'{im.width * im.height / 1e6:.2f} Mpx, ceiling {p.descreen_max_megapixels:.0f}'
                    + ('' if (im.width, im.height) == before else ' (resized?)'),
                )
        im = im.thumbnail_image(tw, height=th, size='down', linear=p.linear_light)
        if chatty:
            step('downscale', f'scale {scale:.3f}')
    elif p.upscale == 'panel' and scale > 1.005:
        upscale = min(scale, p.upscale_max)
        im = im.resize(upscale, kernel=p.upscale_kernel)
        if chatty:
            step('upscale', f'{upscale:.3f}x by {p.upscale_kernel}, cap {p.upscale_max}')

    content = (0, 0, im.width, im.height)
    pad = None
    if p.fit == 'box' and (im.width < tw or im.height < th):
        x, y = (tw - im.width) // 2, (th - im.height) // 2
        content = (x, y, im.width, im.height)
        pad = _mono_pad_level(im, im.width < tw, im.height < th) if mono or p.colour_pad == 'mono' else None
        im = _pad_to_box(im, tw, th, pad)
        if chatty:
            step('pad', f'content {content}, level {pad}')
    return _Geom(im, content, upscale, pad)


# --------------------------------------------------------------------------
# Enlargement
# --------------------------------------------------------------------------

_PAD_MIDPOINT = 128.0
_PAD_FLAT_IQR = 17.0
_BORDER = 2


def _sides(im: pyvips.Image) -> tuple[pyvips.Image, ...]:
    d = _BORDER
    return (
        im.crop(0, 0, min(d, im.width), im.height),
        im.crop(max(0, im.width - d), 0, min(d, im.width), im.height),
        im.crop(0, 0, im.width, min(d, im.height)),
        im.crop(0, max(0, im.height - d), im.width, min(d, im.height)),
    )


def _side_stats(im: pyvips.Image) -> tuple[tuple[float, float], ...]:
    out = []
    for part in _sides(im):
        v = _band_stack(part)
        q1, med, q3 = np.percentile(v, (25, 50, 75))
        out.append((float(med), float(q3 - q1)))
    return tuple(out)


def _mono_pad_level(im: pyvips.Image, horizontal: bool, vertical: bool) -> float:
    try:
        left, right, top, bottom = _side_stats(im if im.bands == 1 else im.colourspace('b-w'))
    except pyvips.Error, ValueError:
        return 255.0
    sides = []
    if horizontal:
        sides += [left, right]
    if vertical:
        sides += [top, bottom]
    flat = [med for med, iqr in sides if iqr <= _PAD_FLAT_IQR]
    if not flat:
        return 255.0
    ends = {0.0 if med < _PAD_MIDPOINT else 255.0 for med in flat}
    return ends.pop() if len(ends) == 1 else 255.0


def _border_background(im: pyvips.Image) -> list[float]:
    frame = np.concatenate([_band_stack(part) for part in _sides(im)])
    return [float(v) for v in np.median(frame, axis=0)]


def _band_stack(part: pyvips.Image) -> np.ndarray:
    buf = np.frombuffer(part.cast('uchar').write_to_memory(), dtype=np.uint8)
    return buf.reshape(-1, part.bands)


def _pad_to_box(im: pyvips.Image, tw: int, th: int, level: float | None = None) -> pyvips.Image:
    if level is not None:
        background = [level] * im.bands
    else:
        try:
            background = _border_background(im)
        except pyvips.Error, ValueError:
            background = [255.0] * im.bands
    return im.embed(
        (tw - im.width) // 2,
        (th - im.height) // 2,
        tw,
        th,
        extend='background',
        background=background,
    )


def _tone(chan: pyvips.Image, p: Profile, top: float) -> pyvips.Image:
    black = p.black * top / 255.0
    white = p.white * top / 255.0
    if p.black > 0 or p.white < 255:
        chan = _clamp((chan - black) * (top / max(1e-3, white - black)), 0, top)
    if abs(p.gamma - 1.0) > 1e-3:
        chan = ((chan / top) ** (1.0 / p.gamma)) * top
    return chan


@functools.lru_cache(maxsize=64)
def _tone_lut(p: Profile) -> pyvips.Image:
    return _tone(pyvips.Image.identity().cast('float'), p, 255.0).copy_memory()


def _unsharp(chan: pyvips.Image, p: Profile, upscale: float = 1.0) -> pyvips.Image:
    if p.usm_amount <= 0:
        return chan
    sigma = p.usm_sigma * max(1.0, upscale)
    return chan + p.usm_amount * (chan - chan.gaussblur(sigma))


def _flatten_pad(a: np.ndarray, content: tuple[int, int, int, int], level: float) -> np.ndarray:
    x, y, w, h = content
    if w >= a.shape[1] and h >= a.shape[0]:
        return a
    mask = np.ones(a.shape[:2], bool)
    mask[y : y + h, x : x + w] = False
    a[mask] = round(level)
    return a


def _render_mono(
    buf: bytes, p: Profile, tw: int, th: int, fmt: str, page: pyvips.Image | None = None
) -> tuple[bytes, str]:
    geom = _geometry(buf, p, tw, th, mono=True, page=page)
    g = geom.image
    if g.bands > 1:
        g = g.colourspace('b-w')
    toned = g.maplut(_tone_lut(p)) if g.format == 'uchar' else _tone(g.cast('float'), p, 255.0)
    g = _unsharp(toned, p, geom.upscale)
    g = defringe(g, p, 255.0, th).rint().cast('uchar')

    if fmt == 'jpeg':
        return (
            g.jpegsave_buffer(Q=p.jpeg_quality, optimize_coding=True, strip=True, subsample_mode='off'),
            'image/jpeg',
        )
    pad = 255.0 if geom.pad is None else geom.pad
    if fmt == 'png8':
        flat = _flatten_pad(_to_numpy(g).copy(), geom.content, pad)
        g = pyvips.Image.new_from_memory(flat.tobytes(), flat.shape[1], flat.shape[0], 1, 'uchar')
        return g.pngsave_buffer(compression=p.png_compression, strip=True), 'image/png'
    idx = _flatten_pad(_quantise16(_to_numpy(g), p.dither), geom.content, pad / 17.0)
    q = pyvips.Image.new_from_memory(idx.tobytes(), idx.shape[1], idx.shape[0], 1, 'uchar')
    return q.pngsave_buffer(bitdepth=4, compression=p.png_compression, strip=True), 'image/png'


def _render_colour(buf: bytes, p: Profile, tw: int, th: int, page: pyvips.Image | None = None) -> tuple[bytes, str]:
    geom = _geometry(buf, p, tw, th, mono=False, page=page)
    im = geom.image
    if im.bands == 1:
        im = im.colourspace('srgb')

    lch = im.colourspace('lch')
    if abs(p.saturation - 1.0) > 1e-3:
        lch = lch * [1.0, p.saturation, 1.0]

    lab = lch.colourspace('lab')
    L, a, b = lab[0], lab[1], lab[2]

    sigma = p.chroma_sigma
    if sigma > 0.05:
        a, b = a.gaussblur(sigma), b.gaussblur(sigma)

    L = _unsharp(_tone(L, p, 100.0), p, geom.upscale)
    L = defringe(L, p, height=th)

    out = L.bandjoin([a, b]).copy(interpretation='lab').colourspace('srgb').cast('uchar')

    cx, cy, cw, ch = geom.content
    if (cw, ch) != (out.width, out.height):
        inner = out.crop(cx, cy, cw, ch)
        out = inner.embed(
            cx,
            cy,
            out.width,
            out.height,
            extend='background',
            background=_border_background(inner) if geom.pad is None else [geom.pad] * out.bands,
        )

    if p.fmt == 'jpegc':
        return (
            out.jpegsave_buffer(
                Q=p.jpeg_quality,
                optimize_coding=True,
                strip=True,
                subsample_mode='on',
            ),
            'image/jpeg',
        )
    if p.palette:
        return (
            out.pngsave_buffer(
                palette=True,
                bitdepth=8,
                colours=p.palette_colours,
                dither=1.0,
                effort=p.png_effort,
                compression=p.png_compression,
                strip=True,
            ),
            'image/png',
        )
    return out.pngsave_buffer(compression=p.png_compression, strip=True), 'image/png'


def same_picture(a: bytes, b: bytes, threshold: float = 0.85) -> bool:
    try:
        pa, pb = _norm_grid(a), _norm_grid(b)
    except UnreadableImage:
        return False
    return float((pa * pb).mean()) > threshold


def _norm_grid(buf: bytes, w: int = 72, h: int = 96) -> np.ndarray:
    try:
        im = pyvips.Image.thumbnail_buffer(buf, w, height=h, size='force')
        if im.hasalpha():
            im = im.flatten(background=255)
        im = im.colourspace('b-w')
        a = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(h, w)).astype(np.float32)
    except pyvips.Error as exc:
        raise UnreadableImage(f'not a readable image ({len(buf)} bytes)') from exc
    return (a - a.mean()) / (float(a.std()) + 1e-6)


def open_image(buf: bytes) -> pyvips.Image:
    return _open(buf)


def fit_to_width(im: pyvips.Image, width: int, p: Profile) -> pyvips.Image:
    scale = width / im.width
    if scale < 1.0:
        im = im.thumbnail_image(width, height=NO_HEIGHT_CAP, size='down', linear=p.linear_light)
    elif scale > 1.0:
        im = im.resize(min(scale, UPSCALE_MAX_CEILING), kernel=p.upscale_kernel)
    if im.width > width:
        im = im.crop(0, 0, width, im.height)
    elif im.width < width:
        im = im.embed(0, 0, width, im.height, extend='copy')
    return im


def render_page(
    buf: bytes,
    p: Profile,
    max_width: int | None = None,
    source: pyvips.Image | None = None,
) -> tuple[bytes, str]:
    if p.fmt == 'raw':
        log.debug('render: fmt=raw, %d kB passed through untouched', len(buf) // 1024)
        return buf, ''
    try:
        return _render_page(buf, p, max_width, source)
    except pyvips.Error as exc:
        raise UnreadableImage(f'not a readable image ({len(buf)} bytes)') from exc


def _render_page(buf: bytes, p: Profile, max_width: int | None, source: pyvips.Image | None) -> tuple[bytes, str]:
    started = time.perf_counter()

    tw = p.width
    if max_width and 0 < max_width < tw:
        tw = max_width
    th = round(tw * p.aspect)

    if not p.is_colour:
        log.debug('render: %s %dx%d, mono profile -> %s', p.name, tw, th, p.fmt)
        return _summarise(_render_mono(buf, p, tw, th, p.fmt, page=source), buf, p, started, 'mono')

    page = source if source is not None else _open(buf).autorot()
    if p.auto_mono and (chroma := _chroma_of(page)) < p.mono_chroma_threshold:
        log.debug(
            'render: %s %dx%d, chroma %.3f < %.1f -> mono %s',
            p.name,
            tw,
            th,
            chroma,
            p.mono_chroma_threshold,
            p.mono_fmt,
        )
        return _summarise(_render_mono(buf, p, tw, th, p.mono_fmt, page=page), buf, p, started, 'auto-mono')

    log.debug('render: %s %dx%d -> colour %s', p.name, tw, th, p.fmt)
    return _summarise(_render_colour(buf, p, tw, th, page=page), buf, p, started, 'colour')


def _summarise(result, buf: bytes, p: Profile, started: float, path: str):
    blob, ctype = result
    if log.isEnabledFor(logging.INFO):
        log.info(
            'page rendered: %s %s %d kB -> %d kB %s in %.0f ms',
            p.name,
            path,
            len(buf) // 1024,
            len(blob) // 1024,
            ctype,
            (time.perf_counter() - started) * 1000,
        )
    return result
