"""
Status page.
Kept in memory, status based solely on the current run.
"""

import time
import logging
import threading
import statistics
import collections

KEPT_DOWNLOADS = 20
KEPT_PROBLEMS = 30
KEPT_TIMINGS = 500
STALE = 900.0


class Download:
    def __init__(self, board: Board, profile: str, name: str, mirror) -> None:
        self._board, self._mirror = board, mirror
        self.profile, self.name = profile, name
        self.stage, self.done, self.total, self.sent = 'fetching', 0, 0, 0
        self.began = self.touched = time.monotonic()
        self.ended: float | None = None

    def update(self, **fields) -> None:
        with self._board.lock:
            if self.ended is not None:
                return
            for key, value in fields.items():
                setattr(self, key, value)
            self.touched = time.monotonic()
        if 'stage' in fields or 'done' in fields:
            self._mirror(**{k: v for k, v in fields.items() if k in ('stage', 'done', 'total')})

    def finish(self, outcome: str) -> None:
        with self._board.lock:
            if self.ended is not None:
                return
            self.stage, self.ended = outcome, time.monotonic()
            self._board.retire(self)
        self._mirror(stage='error' if outcome == 'stopped' else outcome)

    def row(self, now: float) -> dict:
        return {
            'profile': self.profile,
            'name': self.name,
            'stage': self.stage,
            'done': self.done,
            'total': self.total,
            'sent': self.sent,
            'seconds': round((self.ended or now) - self.began, 1),
            'finished': self.ended is not None,
        }


class Problems(logging.Handler):
    def __init__(self, board: Board) -> None:
        super().__init__(logging.WARNING)
        self._board = board

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
            if record.exc_info and record.exc_info[0] is not None:
                message = f'{message} ({record.exc_info[0].__name__})'
            self._board.problem(record.created, record.levelname, record.name, message)
        except Exception:
            self.handleError(record)


class Board:
    def __init__(self, redact=lambda text: text, keep: int = KEPT_DOWNLOADS, problems: int = KEPT_PROBLEMS) -> None:
        self.redact = redact
        self.lock = threading.Lock()
        self.began = time.monotonic()
        self._active: list[Download] = []
        self._recent: collections.deque[Download] = collections.deque(maxlen=keep)
        self._problems: collections.deque[dict] = collections.deque(maxlen=problems)
        self._served = {kind: {'served': 0, 'hits': 0} for kind in ('page', 'cover')}
        self._renders = {kind: collections.deque(maxlen=KEPT_TIMINGS) for kind in ('page', 'ahead')}
        self._rendered = dict.fromkeys(self._renders, 0)
        self._answers: collections.deque[float] = collections.deque(maxlen=KEPT_TIMINGS)

    def begin(self, profile: str, name: str, mirror=lambda **_: None) -> Download:
        dl = Download(self, profile, self.redact(name), mirror)
        with self.lock:
            self._active.append(dl)
        return dl

    def retire(self, dl: Download) -> None:
        if dl in self._active:
            self._active.remove(dl)
            self._recent.append(dl)

    def served(self, kind: str, hit: bool, seconds: float) -> None:
        with self.lock:
            counts = self._served.get(kind)
            if counts is None:
                return
            counts['served'] += 1
            counts['hits'] += hit
            if kind == 'page' and not hit:
                self._answers.append(seconds)

    def rendered(self, kind: str, seconds: float) -> None:
        with self.lock:
            if kind in self._renders:
                self._renders[kind].append(seconds)
                self._rendered[kind] += 1

    def problem(self, at: float, level: str, where: str, message: str) -> None:
        with self.lock:
            self._problems.append(
                {'at': at, 'level': level, 'where': where.removeprefix('inksetter.'), 'message': self.redact(message)}
            )

    def snapshot(self) -> dict:
        now = time.monotonic()
        with self.lock:
            for dl in [d for d in self._active if now - d.touched > STALE]:
                dl.stage, dl.ended = 'lost', now
                self.retire(dl)
            renders = [*self._renders['page'], *self._renders['ahead']]
            return {
                'uptime': round(now - self.began),
                'downloads': [d.row(now) for d in self._active] + [d.row(now) for d in reversed(self._recent)],
                'pages': {
                    **self._served['page'],
                    'rendered': sum(self._rendered.values()),
                    'ahead': self._rendered['ahead'],
                    'render_ms': _spread(renders),
                    'answer_ms': _spread(self._answers),
                },
                'covers': dict(self._served['cover']),
                'problems': list(reversed(self._problems)),
            }


def _spread(seconds) -> dict | None:
    kept = sorted(seconds)
    if not kept:
        return None
    return {
        'median': round(1000 * statistics.median(kept)),
        'p90': round(1000 * kept[min(len(kept) - 1, int(0.9 * len(kept)))]),
        'of': len(kept),
    }
