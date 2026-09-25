"""
Webtoon re-slicing.
"""

import numpy as np
import pyvips
import math
import logging
import functools
import dataclasses

from . import pipeline
from .pipeline import render_page
from .profiles import Profile

log = logging.getLogger(__name__)


STRIP_GUTTER_FLOOR = 0.50
STRIP_OVERSHOOT = 2.00
STRIP_MIN_FILL = 0.85
STRIP_QUIET = 15
STRIP_FAINT = 48
STRIP_GUTTER_MIN = 0.01
STRIP_TOP_MARGIN = 0.02
STRIP_GUTTER_WHITE = 235
STRIP_GUTTER_BLACK = 30
STRIP_EDGE = 0.005
STRIP_HARD = 128
STRIP_HARD_SPAN = 3
STRIP_PAD = 5
STRIP_BAND_MIN = 0.03
STRIP_FLAT_REACH = 1.50
STRIP_OVERLAP_NEAR = 0.04
STRIP_OVERLAP_FAR = 0.25
STRIP_EVEN_FLOOR = 0.80
STRIP_EVEN_SLACK = 0.06
STRIP_PLAN_REACH = 8.00


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


def _inner(rows: np.ndarray) -> np.ndarray:
    edge = round(rows.shape[1] * STRIP_EDGE)
    return rows[:, edge : rows.shape[1] - edge] if edge else rows


def _lohi(rows: np.ndarray) -> np.ndarray:
    rows = _inner(rows)
    return np.stack([rows.min(axis=(1, 2)), rows.max(axis=(1, 2))], axis=1)


def _spread(lohi: np.ndarray) -> np.ndarray:
    return lohi[:, 1].astype(np.int16) - lohi[:, 0]


def _classes(lohi: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    lo, hi = lohi[:, 0], lohi[:, 1]
    spread = hi.astype(np.int16) - lo
    quiet = spread <= STRIP_QUIET
    return spread, quiet & (lo >= STRIP_GUTTER_WHITE), quiet & (hi <= STRIP_GUTTER_BLACK)


def _runs(mask: np.ndarray, at_least: int) -> list[tuple[int, int]]:
    edges = np.flatnonzero(np.diff(np.concatenate([[0], mask.astype(np.int8), [0]])))
    return [(int(s), int(e)) for s, e in zip(edges[::2], edges[1::2], strict=True) if e - s >= at_least]


def _long_runs(mask: np.ndarray, at_least: int) -> np.ndarray:
    out = np.zeros(len(mask), bool)
    for start, stop in _runs(mask, at_least):
        out[start:stop] = True
    return out


def _proper(white: np.ndarray, black: np.ndarray, minrun: int) -> np.ndarray:
    return _long_runs(white, minrun) | _long_runs(black, minrun)


def _row_busy(rows: np.ndarray) -> np.ndarray:
    y = _inner(rows).astype(np.float32) @ np.array([0.299, 0.587, 0.114], np.float32)
    hard = (np.abs(y[:, STRIP_HARD_SPAN:] - y[:, :-STRIP_HARD_SPAN]) >= STRIP_HARD).mean(axis=1)
    detail = np.abs(np.diff(y, axis=1)).mean(axis=1)
    return hard + detail * 1e-4


class _Busy:
    def __init__(self, rows: np.ndarray, reach: int):
        self.rows, self.reach = rows, reach
        self.per = np.full(len(rows), np.nan)

    def __getitem__(self, at) -> np.ndarray:
        at = np.asarray(at)
        n = len(self.per)
        lo, hi = max(0, int(at.min()) - self.reach), min(n, int(at.max()) + self.reach + 1)
        gap = np.flatnonzero(np.isnan(self.per[lo:hi]))
        if len(gap):
            a, b = lo + int(gap[0]), lo + int(gap[-1]) + 1
            self.per[a:b] = np.concatenate([_row_busy(self.rows[i : min(i + 2048, b)]) for i in range(a, b, 2048)])
        total = np.concatenate([[0.0], np.cumsum(self.per[lo:hi])])
        start, stop = np.maximum(at - self.reach, 0) - lo, np.minimum(at + self.reach + 1, n) - lo
        return (total[stop] - total[start]) / (stop - start)


def _busy(rows: np.ndarray, reach: int) -> np.ndarray:
    return _Busy(rows, reach)[np.arange(len(rows))] if len(rows) else np.zeros(0)


def _run_at(mask: np.ndarray, i: int) -> tuple[int, int]:
    before, after = mask[i::-1], mask[i:]
    start = 0 if before.all() else i - int(np.argmin(before)) + 1
    end = len(mask) if after.all() else i + int(np.argmin(after))
    return start, end


def _gutter_seam(lohi: np.ndarray, limit: int) -> int | None:
    lo = max(1, int(limit * STRIP_GUTTER_FLOOR))
    minrun = max(1, round(limit * STRIP_GUTTER_MIN))
    head = lohi[: round(limit * STRIP_OVERSHOOT)]
    runs = _long_runs(_spread(head) <= STRIP_QUIET, minrun)
    quiet = runs[lo:]
    up = np.flatnonzero(quiet[: limit - lo])
    if len(up):
        at = lo + int(up[-1])
        start, end = _run_at(runs, at)
        first = max(start, lo)
        _, white, black = _classes(head[start:end])
        proper = np.flatnonzero(_proper(white, black, minrun)[first - start : at - start + 1])
        return first + int(proper[-1]) if len(proper) else at
    down = np.flatnonzero(quiet[limit - lo :])
    if len(down):
        at = limit + int(down[0])
        _, white, black = _classes(head[at : _run_at(runs, at)[1]])
        for inside in (np.flatnonzero(_proper(white, black, minrun)), np.flatnonzero(white | black)):
            if len(inside):
                return at + int(inside[0])
        return at
    return None


def _least_busy(block: np.ndarray, limit: int) -> int:
    lo = max(1, int(limit * STRIP_GUTTER_FLOOR))
    minrun = max(1, round(limit * STRIP_GUTTER_MIN))
    floor = max(lo, int(limit * STRIP_MIN_FILL))
    start = max(0, floor - minrun)
    busy = _busy(block[start : limit + minrun], minrun)[floor - start : limit - start]
    return floor + len(busy) - 1 - int(np.argmin(busy[::-1]))


def _loose_gutter(head: np.ndarray, limit: int, margin: int) -> tuple[int, int, float | None] | None:
    lo = max(1, int(limit * STRIP_GUTTER_FLOOR))
    lows, highs = head[:, 0], head[:, 1]
    runs = _runs((lows >= STRIP_GUTTER_WHITE) | (highs <= STRIP_GUTTER_BLACK), round(limit * STRIP_BAND_MIN))
    before = [(s, e) for s, e in runs if e > lo and s < limit]
    past = [(s, e) for s, e in runs if s >= limit]
    if not before and not past:
        return None
    s, e = before[-1] if before else past[0]
    at = min(e, limit) if before else s
    white, black = (lows[s:e] >= STRIP_GUTTER_WHITE).all(), (highs[s:e] <= STRIP_GUTTER_BLACK).all()
    return at, max(at, e - margin), 255.0 if white else 0.0 if black else None


def _flat_band(head: np.ndarray, limit: int) -> int | None:
    lo = max(1, int(limit * STRIP_GUTTER_FLOOR))
    band = round(limit * STRIP_BAND_MIN)
    runs = _runs(_spread(head[: round(limit * STRIP_FLAT_REACH)]) <= STRIP_FAINT, band)
    before = [(max(s, lo), min(e, limit)) for s, e in runs]
    before = [(s, e) for s, e in before if e - s >= band]
    past = [(max(s, limit), e) for s, e in runs]
    past = [(s, e) for s, e in past if e - s >= band]
    if not before and not past:
        return None
    s, e = before[-1] if before else past[0]
    return (s + e) // 2


def _overlap_start(block: np.ndarray, at: int, limit: int) -> int:
    minrun = max(1, round(limit * STRIP_GUTTER_MIN))
    a0, a1 = at - round(limit * STRIP_OVERLAP_FAR), at - round(limit * STRIP_OVERLAP_NEAR)
    busy = _busy(block[a0 - minrun : a1 + minrun], minrun)[minrun : minrun + a1 - a0]
    return a0 + int(np.argmin(busy))


def _cut(
    block: np.ndarray, lohi: np.ndarray, limit: int, margin: int, resumed: bool
) -> tuple[int, int, float | None, str]:
    minrun = max(1, round(limit * STRIP_GUTTER_MIN))
    if len(block) <= limit:
        return len(block), len(block), None, 'top'
    at = _gutter_seam(lohi, limit)
    if at is not None:
        return at, at, _gutter_at(lohi, at, minrun) if at < len(block) else None, 'top'
    head = lohi[: round(limit * STRIP_OVERSHOOT)]
    loose = _loose_gutter(head, limit, margin)
    if loose is not None:
        return *loose, 'top'
    at = _flat_band(head, limit)
    if at is not None:
        return at, at, None, 'top'
    at = limit if resumed else _least_busy(block, limit)
    return at, _overlap_start(block, at, limit), None, 'top' if resumed else 'bottom'


def _place(busy: np.ndarray, end: int, h: int, k: int, limit: int) -> tuple[float, list[int]] | None:
    lo, hi = round(h * STRIP_OVERLAP_NEAR), round(h * STRIP_OVERLAP_FAR)
    total = k * h - end
    if h == limit:
        need = max((k - 1) * lo, total - h + round(limit * STRIP_GUTTER_FLOOR)), min((k - 1) * hi, total)
    else:
        need = total, total
    if need[0] > need[1]:
        return None
    a = used = 0
    score, starts = float(busy[h]), [0]
    for i in range(1, k):
        c, left = a + h, k - 1 - i
        cand = np.arange(c - min(hi, need[1] - used - left * lo), c - max(lo, need[0] - used - left * hi) + 1)
        if not len(cand):
            return None
        cost = busy[cand] + busy[cand + h] if left else busy[cand]
        j = int(np.argmin(cost))
        a = int(cand[j])
        score += float(cost[j])
        used += c - a
        starts.append(a)
    return score, starts


def _even(block: np.ndarray, end: int, pieces: int, limit: int) -> tuple[int, list[int]] | None:
    fewest = 2
    while end / (fewest - (fewest - 1) * STRIP_OVERLAP_NEAR) > limit / STRIP_EVEN_FLOOR:
        fewest += 1
    minrun = max(1, round(limit * STRIP_GUTTER_MIN))
    busy = _Busy(block[: end + minrun], minrun)
    for k in range(max(fewest, pieces), fewest - 1, -1):
        least = max(limit, math.ceil(end / (k - (k - 1) * STRIP_OVERLAP_NEAR)))
        most = least if least == limit else min(int(least * (1 + STRIP_EVEN_SLACK)), int(limit / STRIP_EVEN_FLOOR))
        best = None
        for h in range(least, max(least, most) + 1):
            got = _place(busy, end, h, k, limit)
            if got is not None and (best is None or got[0] < best[0]):
                best = got[0], h, got[1]
        if best is not None:
            return best[1], best[2]
    return None


def _opening(lohi: np.ndarray, step: int, minrun: int) -> tuple[int, int, bool]:
    parts = []
    art = None
    done = 0
    while done < len(lohi):
        chunk = lohi[done : done + step]
        parts.append(_classes(chunk))
        done += len(chunk)
        spread, white, black = (np.concatenate(x) for x in zip(*parts, strict=True))
        ink = spread > STRIP_FAINT
        soft = _long_runs(~ink & ~white & ~black, minrun)
        found = [x for x in (np.flatnonzero(ink), np.flatnonzero(soft)) if len(x)]
        if found:
            art = min(int(x[0]) for x in found)
            break
    stop = done if art is None else art
    gutters = []
    for level in (white[:stop], black[:stop]):
        runs = np.flatnonzero(_long_runs(level, minrun))
        if len(runs):
            gutters.append(_run_at(level, int(runs[-1])))
    start, end = max(gutters, key=lambda r: r[1], default=(0, 0))
    return start, end, art is not None


def _gutter_at(lohi: np.ndarray, at: int, minrun: int) -> float | None:
    lo = max(0, at - minrun)
    _, white, black = _classes(lohi[lo : at + minrun])
    for level, mask in ((255.0, white), (0.0, black)):
        if at - lo < len(mask) and _long_runs(mask, minrun)[at - lo]:
            return level
    return None


class _Held:
    def __init__(self, headroom: int):
        self.headroom = headroom
        self.pixels = self.lohi = None
        self.start = self.stop = 0

    def __len__(self) -> int:
        return self.stop - self.start

    def add(self, a: np.ndarray) -> None:
        if self.pixels is None or self.stop + len(a) > len(self.pixels):
            live = len(self)
            pixels = np.empty((live + len(a) + self.headroom, *a.shape[1:]), np.uint8)
            lohi = np.empty((len(pixels), 2), np.uint8)
            if live:
                pixels[:live], lohi[:live] = self.rows(), self.stats()
            self.pixels, self.lohi, self.start, self.stop = pixels, lohi, 0, live
        self.pixels[self.stop : self.stop + len(a)] = a
        self.lohi[self.stop : self.stop + len(a)] = _lohi(a)
        self.stop += len(a)

    def drop(self, n: int) -> None:
        self.start += n

    def rows(self) -> np.ndarray:
        return self.pixels[self.start : self.stop]

    def stats(self) -> np.ndarray:
        return self.lohi[self.start : self.stop]


def strip_tiles(entries, p: Profile, consumed: dict[int, int]):
    limit = p.height
    reach = round(limit * STRIP_OVERSHOOT)
    margin = round(limit * STRIP_TOP_MARGIN)
    minrun = max(1, round(limit * STRIP_GUTTER_MIN))
    held = _Held(2 * reach)
    index = seen = 0
    opening = True
    resumed = False
    chain = None

    def rows() -> int:
        return 0 if opening else len(held)

    def settle() -> None:
        nonlocal opening
        if opening and len(held):
            start, end, art = _opening(held.stats(), limit, minrun)
            held.drop(max(start, end - margin))
            opening = not art

    def tile(name: str, piece: np.ndarray, edges, anchor: str):
        nonlocal index
        index += 1
        consumed[index] = seen
        return ('tile', index, name, piece.copy(), edges, anchor)

    def carry_on(nxt: int, again: bool) -> None:
        nonlocal opening, resumed
        held.drop(nxt)
        opening, resumed = True, again
        settle()

    def cut(name: str, final: bool) -> list:
        nonlocal chain
        block, lohi = held.rows(), held.stats()
        if chain is None:
            at, nxt, below, anchor = _cut(block, lohi, limit, margin, resumed)
            if anchor != 'bottom':
                out = tile(name, block[:at], (_gutter_at(lohi[:at], 0, minrun), below), anchor)
                carry_on(nxt, nxt < at)
                return [out]
            chain = [0, 0, (at, nxt, below)]
        plan = None
        while chain[0] + reach <= STRIP_PLAN_REACH * limit:
            pos = chain[0]
            if not final and len(block) - pos <= reach:
                return []
            at, nxt, below = chain[2] if chain[1] == 0 else _cut(block[pos:], lohi[pos:], limit, margin, True)[:3]
            chain[1] += 1
            if nxt >= at:
                plan = _even(block, pos + at, chain[1], limit)
                break
            chain[0] += nxt
        if plan is None:
            (at, nxt, below), chain = chain[2], None
            log.debug('strip: an artwork could not be planned whole; cut as it comes')
            out = tile(name, block[:at], (_gutter_at(lohi[:at], 0, minrun), below), 'bottom')
            carry_on(nxt, True)
            return [out]
        chain = None
        h, starts = plan
        end, after = pos + at, pos + nxt
        level = _gutter_at(lohi, 0, minrun)
        level = below if level is None else level
        stops = [a + h for a in starts[:-1]] + [end]
        out = [tile(name, block[a:stop], (level, level), 'top') for a, stop in zip(starts, stops, strict=True)]
        carry_on(after, False)
        return out

    last = ''
    for entry in entries:
        if entry[0] == 'copy':
            yield entry
            continue
        _, seen, name, blob = entry
        last = name
        a = _strip_rows(blob, p)
        if a is not None:
            held.add(a)
            settle()
            while rows() > reach:
                got = cut(name, False)
                if not got:
                    break
                yield from got
    while rows():
        yield from cut(last, True)


@functools.lru_cache(maxsize=16)
def _refit(p: Profile, fit: str) -> Profile:
    return dataclasses.replace(p, fit=fit, rotate_wide=False, autocrop=False, strip_folio=False)


def tile_image(a: np.ndarray) -> pyvips.Image:
    a = np.ascontiguousarray(a)
    return pyvips.Image.new_from_memory(a, a.shape[1], a.shape[0], a.shape[2], 'uchar')


def render_tile(job, profile: Profile):
    _, _, _, tile, (above, below), anchor = job
    if len(tile) > profile.height:
        known = {level for level in (above, below) if level is not None}
        fit, level = 'box', known.pop() if len(known) == 1 else None
    elif anchor == 'bottom':
        fit, level = 'bottom', above
    else:
        fit, level = 'top', below if below is not None else above
    return render_page(b'', _refit(profile, fit), source=tile_image(tile), pad_level=level)
