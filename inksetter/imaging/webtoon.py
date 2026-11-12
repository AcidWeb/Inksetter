"""
Webtoon re-slicing.
"""

import numpy as np
import pyvips
import math
import bisect
import logging
import functools
import itertools
import collections
import dataclasses
import concurrent.futures

from . import folio, pipeline
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
STRIP_LETTERING = 0.025
STRIP_READ_ROWS = 736
STRIP_DECODE_THREADS = 2
STRIP_WIDTH_SAMPLE = 8
STRIP_FILL_BLANK = 0.40
STRIP_FILL_FLOOR = 0.90
STRIP_FILL_READ = 0.08
STRIP_SPECK = 3e-5
STRIP_GROUND_REACH = 0.15
STRIP_GROUND_LONG = 0.03
STRIP_GROUND_FLAT = 10
STRIP_GROUND_MATCH = 12
STRIP_GROUND_SHARE = 0.30
STRIP_GROUND_SIDE = 24
STRIP_GROUND_END = 8
STRIP_PAPER_SPECKS = 0.0025

STRIP_PART_BYTES = 3_500_000_000
STRIP_BYTES_PER_MPX = {'pngc': 200_000, 'webpc': 60_000}

_LUMA = np.array([0.299, 0.587, 0.114], np.float32)
_GROUNDS = frozenset({'gutter', 'loose', 'paper', 'flat'})


def strip_parts(sizes: list[tuple[int, int]], count: int, p: Profile) -> int:
    if count <= 0:
        return 1
    widths = collections.Counter(w for w, _ in sizes)
    if widths:
        width = min(p.width, round(widths.most_common(1)[0][0] * p.strip_scale_max))
        rows = count * sum(h * width / w for w, h in sizes) / len(sizes)
    else:
        rows = count * 2 * p.width
    size = rows * p.width / 1e6 * STRIP_BYTES_PER_MPX.get(p.fmt, STRIP_BYTES_PER_MPX['pngc'])
    return max(1, math.ceil(size / STRIP_PART_BYTES))


def _slice_width(blob: bytes) -> int | None:
    try:
        return pipeline.open_image(blob).autorot().width
    except Exception:
        return None


def _strip_width(blobs: list[bytes], p: Profile) -> int | None:
    widths = collections.Counter(w for w in map(_slice_width, blobs) if w)
    if not widths:
        return None
    return min(p.width, round(widths.most_common(1)[0][0] * p.strip_scale_max))


def _strip_rows(blob: bytes, p: Profile, width: int | None = None) -> np.ndarray | None:
    try:
        im = pipeline.open_image(blob).autorot()
        if im.hasalpha():
            im = im.flatten(background=255)
        im = pipeline.fit_to_width(im, width or p.width, p)
        if im.bands == 1:
            im = im.colourspace('srgb')
        elif im.bands > 3:
            im = im.extract_band(0, n=3)
        a = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width, im.bands))
    except Exception:
        log.warning('strip: an entry would not decode; it is left out of the page', exc_info=True)
        return None
    return a if a.size else None


def _decoded(entries, p: Profile):
    source, sample, pages = iter(entries), [], 0
    while pages < STRIP_WIDTH_SAMPLE and (entry := next(source, None)) is not None:
        sample.append(entry)
        pages += entry[0] == 'page'
    width = _strip_width([entry[3] for entry in sample if entry[0] == 'page'], p) or p.width
    source = itertools.chain(sample, source)

    if STRIP_DECODE_THREADS <= 0:
        for entry in source:
            yield entry, _strip_rows(entry[3], p, width) if entry[0] == 'page' else None
        return
    pool = concurrent.futures.ThreadPoolExecutor(STRIP_DECODE_THREADS, thread_name_prefix='strip-decode')
    ahead = collections.deque()
    try:
        while True:
            while len(ahead) < 2 * STRIP_DECODE_THREADS:
                entry = next(source, None)
                if entry is None:
                    break
                rows = pool.submit(_strip_rows, entry[3], p, width) if entry[0] == 'page' else None
                ahead.append((entry, rows))
            if not ahead:
                return
            entry, decoding = ahead.popleft()
            yield entry, decoding.result() if decoding is not None else None
    finally:
        pool.shutdown(cancel_futures=True)


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
    y = _inner(rows).astype(np.float32) @ _LUMA
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


def _between_lines(
    block: np.ndarray, head: np.ndarray, start: int, end: int, limit: int, longest: float | None
) -> bool:
    near = (head[:, 0] >= STRIP_GUTTER_WHITE) | (head[:, 1] <= STRIP_GUTTER_BLACK)
    a, b = _run_at(near, start) if near[start] else (start, end)
    reach = round(limit * STRIP_LETTERING)
    tall = longest is not None and max(b, end) - min(a, start) >= round(limit * longest)
    if tall or max(b, end) == len(head):
        return False
    engine = folio._reader()
    if engine is None:
        return False
    rows = min(len(block), STRIP_READ_ROWS)
    top = max(0, min((start + end - rows) // 2, len(block) - rows))
    try:
        found, _ = engine(block[top : top + rows].copy(), use_det=True, use_cls=False, use_rec=False)
    except Exception:
        log.debug('strip: the text detector failed on a gap; it is taken for a gutter', exc_info=True)
        return False
    boxes = [
        (top + q[:, 1].min(), top + q[:, 1].max(), q[:, 0].min(), q[:, 0].max()) for q in map(np.asarray, found or [])
    ]
    above = [q for q in boxes if q[0] < start and start - reach <= q[1] <= end]
    below = [q for q in boxes if q[1] > end and start <= q[0] <= end + reach]
    if not above or not below:
        return False
    left = max(min(q[2] for q in above), min(q[2] for q in below))
    return min(max(q[3] for q in above), max(q[3] for q in below)) > left


def _gutter_seam(lohi: np.ndarray, limit: int, block: np.ndarray | None = None, fill: bool = False) -> int | None:
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
        inside = _proper(white, black, minrun)
        proper = np.flatnonzero(inside[first - start : at - start + 1])
        at = first + int(proper[-1]) if len(proper) else at
        gap = np.flatnonzero(inside)
        blank = limit - start - (int(gap[0]) if len(gap) else end - start)
        if not fill or end >= limit or blank < limit * STRIP_FILL_BLANK:
            return at
        past = _past_seam(head, runs, limit, minrun, block)
        if past is None or past > limit / STRIP_FILL_FLOOR:
            return at
        band = round(limit * STRIP_BAND_MIN)
        _, white, black = _classes(head[past : past + band])
        if len(white) < band or not (white.all() or black.all()):
            return at
        s, e = _run_at(runs, past)
        return at if block is not None and _between_lines(block, head, s, e, limit, STRIP_FILL_READ) else past
    return _past_seam(head, runs, limit, minrun, block)


def _past_seam(head: np.ndarray, runs: np.ndarray, limit: int, minrun: int, block: np.ndarray | None) -> int | None:
    lettered = False
    for at, end in _runs(runs[limit:], 1):
        at, end = limit + at, limit + end
        if block is not None and _between_lines(block, head, at, end, limit, None if lettered else STRIP_LETTERING):
            log.debug('strip: a gap between two lines of lettering is not a gutter')
            lettered = True
            continue
        _, white, black = _classes(head[at:end])
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


def _paper_rows(rows: np.ndarray, lohi: np.ndarray) -> np.ndarray:
    out = _spread(lohi) <= STRIP_QUIET
    read = np.flatnonzero(~out)
    inner = _inner(rows)
    allow = STRIP_PAPER_SPECKS * inner.shape[1]
    for at in range(0, len(read), 64):
        pick = read[at : at + 64]
        y = inner[pick].astype(np.float32) @ _LUMA
        off = (np.abs(y - y.mean(axis=1, keepdims=True)) > STRIP_FAINT / 2).sum(axis=1)
        out[pick] = (y.std(axis=1) <= STRIP_GROUND_FLAT) & (off <= allow)
    return out


class _Paper:
    def __init__(self, rows: np.ndarray, lohi: np.ndarray, known: np.ndarray | None = None):
        self.rows, self.lohi = rows, lohi
        self.known = np.full(len(rows), -1, np.int8) if known is None else known

    def __len__(self) -> int:
        return len(self.known)

    def __getitem__(self, at: slice | np.ndarray) -> np.ndarray:
        rows = np.arange(*at.indices(len(self.known))) if isinstance(at, slice) else at
        new = rows[self.known[rows] < 0]
        if len(new) and new[-1] - new[0] + 1 == len(new):
            span = slice(int(new[0]), int(new[-1]) + 1)
            self.known[span] = _paper_rows(self.rows[span], self.lohi[span])
        elif len(new):
            self.known[new] = _paper_rows(self.rows[new], self.lohi[new])
        return self.known[rows] == 1

    def after(self, pos: int) -> _Paper:
        return _Paper(self.rows[pos:], self.lohi[pos:], self.known[pos:])


def _paper_runs(paper: _Paper, first: int, end: int, band: int) -> list[tuple[int, int]]:
    runs, done = [], first
    probes = np.arange(first + band - 1, end, band)
    for at in probes[paper[probes]].tolist():
        if at < done:
            continue
        s, e = at, at + 1
        while s > first:
            up = paper[max(first, s - 64) : s]
            stop = np.flatnonzero(~up)
            s -= len(up) - (int(stop[-1]) + 1 if len(stop) else 0)
            if len(stop):
                break
        while e < end:
            down = paper[e : min(end, e + 64)]
            stop = np.flatnonzero(~down)
            e += int(stop[0]) if len(stop) else len(down)
            if len(stop):
                break
        done = e
        if e - s >= band:
            runs.append((s, e))
    return runs


def _paper_band(paper: _Paper, limit: int, margin: int) -> tuple[int, int] | None:
    lo = max(1, int(limit * STRIP_GUTTER_FLOOR))
    band = round(limit * STRIP_BAND_MIN)
    runs = _paper_runs(paper, max(0, lo - band), min(len(paper), round(limit * STRIP_OVERSHOOT)), band)
    before = [(s, e) for s, e in runs if e > lo and s < limit]
    past = [(s, e) for s, e in runs if limit <= s <= round(limit * STRIP_FLAT_REACH)]
    if not before and not past:
        return None
    s, e = before[-1] if before else past[0]
    at = min(e, limit) if before else s
    return at, max(at, e - margin)


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
    block: np.ndarray,
    lohi: np.ndarray,
    limit: int,
    margin: int,
    resumed: bool,
    lettering: bool = False,
    paper: _Paper | None = None,
) -> tuple[int, int, float | None, str, str]:
    minrun = max(1, round(limit * STRIP_GUTTER_MIN))
    if len(block) <= limit:
        return len(block), len(block), None, 'top', 'end'
    at = _gutter_seam(lohi, limit, block if lettering else None, not resumed)
    if at is not None:
        return at, at, _gutter_at(lohi, at, minrun) if at < len(block) else None, 'top', 'gutter'
    head = lohi[: round(limit * STRIP_OVERSHOOT)]
    loose = _loose_gutter(head, limit, margin)
    if loose is not None:
        return *loose, 'top', 'loose'
    found = _paper_band(_Paper(block, lohi) if paper is None else paper, limit, margin)
    if found is not None:
        return *found, None, 'top', 'paper'
    at = _flat_band(head, limit)
    if at is not None:
        return at, at, None, 'top', 'flat'
    at = limit if resumed else _least_busy(block, limit)
    return at, _overlap_start(block, at, limit), None, 'top' if resumed else 'bottom', 'art'


def _ends_in(tier: str, lohi: np.ndarray, at: int, limit: int, paper: _Paper | None = None) -> bool:
    if tier == 'loose':
        lo, hi = lohi[at - 1]
        return bool(lo >= STRIP_GUTTER_WHITE or hi <= STRIP_GUTTER_BLACK)
    if tier == 'paper':
        return paper is not None and bool(paper[at - 1 : at][0])
    if tier == 'gutter':
        return at < limit and bool(_spread(lohi[at - 1 : at])[0] <= STRIP_QUIET)
    return tier == 'flat'


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


def _empty(lohi: np.ndarray, rows: np.ndarray, budget: float) -> bool:
    ink = np.flatnonzero(_spread(lohi) > STRIP_FAINT)
    seen = 0
    for at in range(0, len(ink), 64):
        y = _inner(rows[ink[at : at + 64]]).astype(np.float32) @ _LUMA
        seen += int((np.abs(y - y.mean(axis=1, keepdims=True)) > STRIP_FAINT / 2).sum())
        if seen > budget:
            return False
    return True


def _gutter_at(lohi: np.ndarray, at: int, minrun: int) -> float | None:
    lo = max(0, at - minrun)
    _, white, black = _classes(lohi[lo : at + minrun])
    for level, mask in ((255.0, white), (0.0, black)):
        if at - lo < len(mask) and _long_runs(mask, minrun)[at - lo]:
            return level
    return None


@dataclasses.dataclass(frozen=True)
class _Ground:
    top: bool = False
    bottom: bool = False
    plain: bool = False
    colour: tuple[float, float, float] | None = None
    above: np.ndarray | None = dataclasses.field(default=None, compare=False, repr=False)


def _smooth(a: np.ndarray, k: int = 5) -> np.ndarray:
    padded = np.pad(a, ((k // 2, k // 2),) + ((0, 0),) * (a.ndim - 1), mode='edge')
    c = np.concatenate([np.zeros((1, *a.shape[1:])), np.cumsum(padded, axis=0, dtype=np.float64)])
    return (c[k:] - c[:-k]) / k


def _row_stats(rows: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    part = rows.astype(np.float32)
    return part.mean(axis=1), (part @ _LUMA).std(axis=1)


def _ground(rows: np.ndarray, limit: int) -> tuple[tuple[float, float, float], int] | None:
    reach = round(limit * STRIP_GROUND_REACH)
    minrun = max(1, round(limit * STRIP_GUTTER_MIN))
    rows = _inner(rows[: 2 * reach])
    n = len(rows)
    colour, spread = np.empty((n, 3)), np.empty(n)
    done = 0
    while done < n:
        colour[done : done + 64], spread[done : done + 64] = _row_stats(rows[done : done + 64])
        done = min(n, done + 64)
        sure = done if done == n else done - 2
        flat = _smooth(spread[:done])[:sure] <= STRIP_GROUND_FLAT
        edges = np.flatnonzero(np.diff(np.concatenate([[0], flat.astype(np.int8), [0]])))
        waiting = False
        for s, e in zip(edges[::2], edges[1::2], strict=True):
            if s > reach:
                return None
            open_ = e == sure < n
            if e - s < minrun and not open_:
                continue
            if open_ and e - s < max(minrun, 5):
                waiting = True
                break
            c = colour[s:e]
            off = np.flatnonzero(np.abs(c - c[:5].mean(axis=0)).max(axis=1) > STRIP_GROUND_MATCH)
            if len(off) or not open_:
                c = c[: off[0] if len(off) else len(c)]
                return (tuple(float(v) for v in c.mean(axis=0)), len(c)) if len(c) else None
            waiting = True
            break
        if not waiting and sure > reach:
            return None
    return None


def _plain(lohi: np.ndarray, minrun: int) -> bool:
    _, white, black = _classes(lohi)
    return not _proper(white, black, minrun).any()


class _Held:
    def __init__(self, headroom: int):
        self.headroom = headroom
        self.pixels = self.lohi = self.known = None
        self.start = self.stop = 0
        self.dropped = 0

    def __len__(self) -> int:
        return self.stop - self.start

    def add(self, a: np.ndarray) -> None:
        if self.pixels is None or self.stop + len(a) > len(self.pixels):
            live = len(self)
            pixels = np.empty((live + len(a) + self.headroom, *a.shape[1:]), np.uint8)
            lohi = np.empty((len(pixels), 2), np.uint8)
            known = np.empty(len(pixels), np.int8)
            if live:
                pixels[:live], lohi[:live] = self.rows(), self.stats()
                known[:live] = self.known[self.start : self.stop]
            self.pixels, self.lohi, self.known, self.start, self.stop = pixels, lohi, known, 0, live
        self.pixels[self.stop : self.stop + len(a)] = a
        self.lohi[self.stop : self.stop + len(a)] = _lohi(a)
        self.known[self.stop : self.stop + len(a)] = -1
        self.stop += len(a)

    def drop(self, n: int) -> None:
        self.start += n
        self.dropped += n

    def rows(self) -> np.ndarray:
        return self.pixels[self.start : self.stop]

    def stats(self) -> np.ndarray:
        return self.lohi[self.start : self.stop]

    def paper(self) -> _Paper:
        return _Paper(self.rows(), self.stats(), self.known[self.start : self.stop])


def strip_tiles(entries, p: Profile, consumed: dict[int, int], origins: dict[int, int] | None = None):
    limit = p.height
    reach = round(limit * STRIP_OVERSHOOT)
    margin = round(limit * STRIP_TOP_MARGIN)
    minrun = max(1, round(limit * STRIP_GUTTER_MIN))
    lettering = p.strip_folio
    ahead = reach + (STRIP_READ_ROWS // 2 if lettering else 0)
    held = _Held(2 * reach)
    index = seen = 0
    opening = True
    resumed = False
    chain = None
    firsts: list[int] = []
    slices: list[int] = []
    spare = None
    dropped = 0
    specks = STRIP_SPECK * p.width * p.height
    grounded, tail = False, None
    ground_rows = 2 * round(limit * STRIP_GROUND_REACH)

    def rows() -> int:
        return 0 if opening else len(held)

    def settle() -> None:
        nonlocal opening
        if opening and len(held):
            start, end, art = _opening(held.stats(), limit, minrun)
            held.drop(max(start, end - margin))
            opening = not art

    def tile(name: str, piece: np.ndarray, lohi: np.ndarray, edges, anchor: str, ground: _Ground, offset: int = 0):
        nonlocal spare, dropped
        origin = None
        if origins is not None and firsts:
            origin = slices[max(0, bisect.bisect_right(firsts, held.dropped + offset) - 1)]
        if _empty(lohi, piece, specks):
            spare, dropped = (name, piece.copy(), edges, anchor, ground, origin), dropped + 1
            return None
        return numbered(name, piece.copy(), edges, anchor, ground, origin)

    def numbered(name: str, piece: np.ndarray, edges, anchor: str, ground: _Ground, origin: int | None):
        nonlocal index
        index += 1
        consumed[index] = seen
        if origin is not None:
            origins[index] = origin
        return ('tile', index, name, piece, edges, anchor, ground)

    def carry_on(nxt: int, again: bool) -> None:
        nonlocal opening, resumed
        held.drop(nxt)
        opening, resumed = True, again
        settle()

    def lay(tier: str, ended: bool, end: int) -> None:
        nonlocal grounded, tail
        grounded = tier in _GROUNDS
        tail = held.rows()[max(0, end - ground_rows) : end].copy() if ended else None

    def cut(name: str, final: bool) -> list | None:
        nonlocal chain
        block, lohi, paper = held.rows(), held.stats(), held.paper()
        if chain is None:
            at, nxt, below, anchor, tier = _cut(block, lohi, limit, margin, resumed, lettering, paper)
            if anchor != 'bottom':
                ended = _ends_in(tier, lohi, at, limit, paper)
                ground = _Ground(grounded, ended, _plain(lohi[:at], minrun), above=tail if grounded else None)
                out = tile(name, block[:at], lohi[:at], (_gutter_at(lohi[:at], 0, minrun), below), anchor, ground)
                lay(tier, ended, at)
                carry_on(nxt, nxt < at)
                return [out] if out is not None else []
            chain = [0, 0, (at, nxt, below, tier)]
        plan = None
        while chain[0] + reach <= STRIP_PLAN_REACH * limit:
            pos = chain[0]
            if not final and len(block) - pos <= ahead:
                return None
            if chain[1] == 0:
                at, nxt, below, tier = chain[2]
            else:
                at, nxt, below, _, tier = _cut(
                    block[pos:], lohi[pos:], limit, margin, True, lettering, paper.after(pos)
                )
            chain[1] += 1
            if nxt >= at:
                plan = _even(block, pos + at, chain[1], limit)
                break
            chain[0] += nxt
        if plan is None:
            (at, nxt, below, _), chain = chain[2], None
            log.debug('strip: an artwork could not be planned whole; cut as it comes')
            ground = _Ground(grounded, plain=_plain(lohi[:at], minrun), above=tail if grounded else None)
            out = tile(name, block[:at], lohi[:at], (_gutter_at(lohi[:at], 0, minrun), below), 'bottom', ground)
            lay('art', False, at)
            carry_on(nxt, True)
            return [out] if out is not None else []
        chain = None
        h, starts = plan
        end, after = pos + at, pos + nxt
        level = _gutter_at(lohi, 0, minrun)
        level = below if level is None else level
        found = _ground(block, limit) if grounded else None
        if found is None and grounded and tail is not None:
            found = _ground(tail[::-1], limit)
        ended = _ends_in(tier, lohi[pos:], at, limit, paper.after(pos))
        if found is None and tier in _GROUNDS:
            found = _ground(block[:end][::-1] if ended else block[end:], limit)
        colour = found[0] if found else None
        stops = [a + h for a in starts[:-1]] + [end]
        out = [
            tile(
                name,
                block[a:stop],
                lohi[a:stop],
                (level, level),
                'top',
                _Ground(colour=colour),
                a,
            )
            for a, stop in zip(starts, stops, strict=True)
        ]
        lay(tier, ended, end)
        carry_on(after, False)
        return [t for t in out if t is not None]

    last = ''
    decoded = _decoded(entries, p)
    try:
        for entry, a in decoded:
            if entry[0] == 'copy':
                yield entry
                continue
            _, seen, name, _ = entry
            last = name
            if a is not None:
                if origins is not None:
                    firsts.append(held.dropped + len(held))
                    slices.append(seen)
                held.add(a)
                settle()
                while rows() > ahead:
                    got = cut(name, False)
                    if got is None:
                        break
                    yield from got
    finally:
        decoded.close()
    while rows():
        yield from cut(last, True)
    if not index and spare is not None:
        dropped -= 1
        yield numbered(*spare)
    if dropped:
        log.info('strip: %d page(s) with nothing on them left out', dropped)


@functools.lru_cache(maxsize=16)
def _refit(p: Profile, fit: str) -> Profile:
    return dataclasses.replace(p, fit=fit, rotate_wide=False, autocrop=False, strip_folio=False, upscale='none')


def tile_image(a: np.ndarray) -> pyvips.Image:
    a = np.ascontiguousarray(a)
    return pyvips.Image.new_from_memory(a, a.shape[1], a.shape[0], a.shape[2], 'uchar')


def _pad_border(tile: np.ndarray, sides: bool, scale: float, end: str | None):
    w = tile.shape[1]
    colours, flats, ends = [], [], []
    if sides:
        edge, cols = round(w * STRIP_EDGE), max(1, round(STRIP_GROUND_SIDE / scale))
        for band in (tile[:, edge : edge + cols], tile[:, w - edge - cols : w - edge]):
            colour, spread = _row_stats(band)
            colours.append(_smooth(colour))
            flats.append(_smooth(spread) <= STRIP_GROUND_FLAT)
            ends.append(np.zeros(len(tile), bool))
    if end is not None:
        rows = _inner(tile[-STRIP_GROUND_END:] if end == 'below' else tile[:STRIP_GROUND_END]).astype(np.float32)
        colours.append(rows.mean(axis=0))
        flats.append((rows @ _LUMA).std(axis=0) <= STRIP_GROUND_FLAT)
        ends.append(np.ones(rows.shape[1], bool))
    return np.concatenate(colours), np.concatenate(flats), np.concatenate(ends)


def _ground_pad(tile: np.ndarray, p: Profile, fit: str, level: float | None, ground: _Ground):
    if not p.is_colour or p.auto_mono or (level is None and p.colour_pad != 'mono'):
        return None
    limit, (n, w) = p.height, tile.shape[:2]
    scale = min(1.0, limit / n)
    sides, short = round(w * scale) < p.width, n < limit
    if not sides and not short:
        return None
    if level is None:
        im = tile_image(tile)
        if scale < 1.0:
            im = im.thumbnail_image(p.width, height=limit, size='down', linear=p.linear_light)
        level = pipeline.mono_pad_level(im, im.width < p.width, short and fit != 'top', short and fit != 'bottom')
    today = np.full(3, level)
    colours, flats, ends = _pad_border(tile, sides, scale, ('below' if fit == 'top' else 'above') if short else None)
    held = flats & (np.abs(colours - today).max(axis=1) <= STRIP_GROUND_MATCH)
    if flats.mean() < STRIP_GROUND_SHARE or not (flats & ~held).any():
        return None
    top = _ground(tile, limit) if ground.top or ground.plain else None
    bottom = _ground(tile[::-1], limit) if ground.bottom or ground.plain else None
    opened = top if top is not None or ground.above is None else _ground(ground.above[::-1], limit)
    found = [] if ground.colour is None else [ground.colour]
    found += [g[0] for g, wanted in ((opened, ground.top), (bottom, ground.bottom)) if g and wanted]
    if ground.plain:
        found += [g[0] for g in (top, bottom) if g and g[1] >= limit * STRIP_GROUND_LONG]
    found = [np.array(c) for c in found]
    if all(np.abs(c - today).max() <= STRIP_GROUND_MATCH for c in found):
        return None
    covered = [held] + [flats & (np.abs(colours - c).max(axis=1) <= STRIP_GROUND_MATCH) for c in found]
    shares = [m.mean() for m in covered]
    best = int(np.argmax(shares))
    if best == 0 or shares[best] < STRIP_GROUND_SHARE:
        return None
    if any(part.any() and covered[best][part].mean() < covered[0][part].mean() for part in (ends, ~ends)):
        return None
    colour = colours[covered[best]].mean(axis=0)
    for pure in (0.0, 255.0):
        if np.abs(colour - pure).max() <= STRIP_GROUND_MATCH:
            return None if pure == level else pure
    return tuple(float(v) for v in colour)


def render_tile(job, profile: Profile):
    _, _, _, tile, (above, below), anchor, *ground = job
    if len(tile) > profile.height:
        known = {level for level in (above, below) if level is not None}
        fit, level = 'box', known.pop() if len(known) == 1 else None
    elif anchor == 'bottom':
        fit, level = 'bottom', above
    else:
        fit, level = 'top', below if below is not None else above
    colour = _ground_pad(tile, profile, fit, level, ground[0]) if ground else None
    return render_page(
        b'', _refit(profile, fit), source=tile_image(tile), pad_level=level if colour is None else colour
    )
