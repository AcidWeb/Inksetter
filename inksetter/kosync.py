"""
KOReader progress sync for repacked downloads.
"""

import os
import re
import json
import bisect
import hashlib
import logging
import threading
from pathlib import Path

log = logging.getLogger(__name__)

SAMPLE = 1024
OFFSETS = (0, *(1024 << 2 * i for i in range(11)))
KEPT = 5000
_DIGEST = re.compile(r'[0-9a-fA-F]{32}')


def is_digest(value) -> bool:
    return isinstance(value, str) and bool(_DIGEST.fullmatch(value))


def partial_md5(sample, size: int) -> str:
    h = hashlib.md5(usedforsecurity=False)
    for offset in OFFSETS:
        if offset >= size:
            break
        h.update(sample(offset, SAMPLE))
    return h.hexdigest()


def file_md5(f) -> str:
    size = f.seek(0, os.SEEK_END)
    sample = getattr(f, 'sample', None)
    if sample is None:

        def sample(offset: int, n: int) -> bytes:
            f.seek(offset)
            return f.read(n)

    try:
        return partial_md5(sample, size)
    finally:
        f.seek(0)


class Digest:
    def __init__(self) -> None:
        self.size = 0
        self._samples = [bytearray() for _ in OFFSETS]

    def feed(self, block: bytes) -> None:
        start, end = self.size, self.size + len(block)
        for kept, offset in zip(self._samples, OFFSETS, strict=True):
            if offset >= end:
                break
            lo, hi = max(offset, start), min(offset + SAMPLE, end)
            if lo < hi:
                kept += block[lo - start : hi - start]
        self.size = end

    def hexdigest(self) -> str:
        samples = dict(zip(OFFSETS, self._samples, strict=True))
        return partial_md5(lambda offset, _: bytes(samples[offset]), self.size)


def to_source(pages: list[int], page: int) -> int:
    if not pages:
        return page
    return max(1, pages[min(max(page, 1), len(pages)) - 1])


def from_source(pages: list[int], page: int) -> int:
    if not pages:
        return page
    return min(bisect.bisect_left(pages, page) + 1, len(pages))


def page_for(record: dict, source: int) -> int:
    pushed = record.get('pushed')
    if isinstance(pushed, list) and len(pushed) == 2 and pushed[1] == source and isinstance(pushed[0], int):
        return pushed[0]
    return from_source(record['pages'], source)


class Books:
    def __init__(self, root: Path, kept: int = KEPT) -> None:
        self.root = root
        self.kept = kept
        self._held: int | None = None
        self._lock = threading.Lock()

    def _path(self, digest: str) -> Path | None:
        return self.root / f'{digest.lower()}.json' if is_digest(digest) else None

    def get(self, digest: str) -> dict | None:
        path = self._path(digest)
        if path is None:
            return None
        try:
            record = json.loads(path.read_text(encoding='utf-8'))
        except OSError, ValueError:
            return None
        if not isinstance(record, dict) or not is_digest(record.get('source')):
            return None
        pages = record.get('pages')
        if not isinstance(pages, list) or not pages or not all(isinstance(n, int) for n in pages):
            return None
        return record

    def put(self, digest: str, record: dict) -> None:
        path = self._path(digest)
        if path is None:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        fresh = not path.exists()
        tmp = path.with_suffix('.tmp')
        try:
            tmp.write_text(json.dumps(record, separators=(',', ':')), encoding='utf-8')
            tmp.replace(path)
        except OSError:
            tmp.unlink(missing_ok=True)
            raise
        if fresh:
            with self._lock:
                if self._held is not None:
                    self._held += 1

    def remember(self, digest: str, page: int, source: int) -> None:
        record = self.get(digest)
        if record is not None and record.get('pushed') != [page, source]:
            self.put(digest, {**record, 'pushed': [page, source]})

    def prune(self) -> None:
        with self._lock:
            if self._held is not None and self._held <= self.kept:
                return
        try:
            held = sorted(self.root.glob('*.json'), key=lambda p: p.stat().st_mtime)
        except OSError:
            return
        gone = 0
        if len(held) > self.kept:
            for path in held[: len(held) - (self.kept - self.kept // 10)]:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    continue
                gone += 1
        with self._lock:
            self._held = len(held) - gone


class Pending:
    def __init__(self, books: Books, source: str) -> None:
        self.books, self.source = books, source
        self.pages: list[int] = []
        self.digest = Digest()

    def feed(self, block: bytes) -> None:
        self.digest.feed(block)

    def finish(self) -> str | None:
        if not self.pages:
            return None
        digest = self.digest.hexdigest()
        self.books.put(digest, {'source': self.source, 'pages': self.pages})
        self.books.prune()
        log.debug('kosync: download %s recorded against %s, %d page(s)', digest, self.source, len(self.pages))
        return digest
