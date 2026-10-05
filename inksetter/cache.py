"""
Content-addressed disk cache for rendered pages.
Used by OPDS-PSE.
"""

import os
import time
import asyncio
import hashlib
import contextlib
import dataclasses
from pathlib import Path

from .imaging.profiles import Profile
from .imaging.pipeline import PIPELINE_VERSION

_KEY_IGNORED_FIELDS = frozenset({'name', 'reslice', 'strip_scale_max'})


def render_key(url: str, profile: Profile, max_width: int | None, credentials: str = '') -> str:
    fields = {k: v for k, v in dataclasses.asdict(profile).items() if k not in _KEY_IGNORED_FIELDS}
    h = hashlib.sha256()
    h.update(PIPELINE_VERSION.encode())
    h.update(b'\0')
    h.update(repr(sorted(fields.items())).encode())
    h.update(b'\0')
    h.update(str(max_width or 0).encode())
    h.update(b'\0')
    h.update(url.encode())
    if credentials:
        h.update(b'\0')
        h.update(credentials.encode())
    return h.hexdigest()


class DiskCache:
    def __init__(self, root: Path, max_bytes: int) -> None:
        self.root = root
        self.max_bytes = max_bytes
        self._locks: dict[str, asyncio.Lock] = {}
        self._trimming = False
        self._size: int | None = None
        self._written_during_trim = 0
        self._next_trim = 0.0
        self.root.mkdir(parents=True, exist_ok=True)

    def _paths(self, key: str) -> tuple[Path, Path]:
        d = self.root / key[:2]
        return d / f'{key}.blob', d / f'{key}.type'

    def get(self, key: str) -> tuple[bytes, str] | None:
        blob, meta = self._paths(key)
        try:
            data = blob.read_bytes()
            ctype = meta.read_text().strip()
        except OSError:
            return None
        try:
            os.utime(blob, None)
        except OSError:
            pass
        return data, ctype

    def has(self, key: str) -> bool:
        return self._paths(key)[0].exists()

    def put(self, key: str, data: bytes, ctype: str) -> None:
        blob, meta = self._paths(key)
        blob.parent.mkdir(parents=True, exist_ok=True)
        tmp = blob.with_suffix('.tmp')
        try:
            old_size: int | None = blob.stat().st_size
        except OSError:
            old_size = None
        try:
            tmp.write_bytes(data)
            meta.write_text(ctype)
            tmp.replace(blob)
        except OSError:
            tmp.unlink(missing_ok=True)
            if old_size is None:
                meta.unlink(missing_ok=True)
            return
        if self._trimming:
            self._written_during_trim += len(data)
        if self._size is not None:
            self._size += len(data) - (old_size or 0)

    def lock(self, key: str) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            if len(self._locks) > 4096:
                self._locks = {k: v for k, v in self._locks.items() if v.locked()}
            lock = self._locks[key] = asyncio.Lock()
        return lock

    async def maybe_trim(self) -> None:
        if self._trimming or time.monotonic() < self._next_trim:
            return
        if self._size is not None and self._size <= self.max_bytes:
            return
        self._trimming = True
        self._written_during_trim = 0
        try:
            total = await asyncio.to_thread(self._trim)
            self._size = total + self._written_during_trim
            if total > self.max_bytes:
                self._next_trim = time.monotonic() + self._TRIM_COOLDOWN
        finally:
            self._trimming = False

    _ORPHAN_AGE = 60.0
    _TRIM_COOLDOWN = 300.0

    def _sweep_orphans(self) -> None:
        cutoff = time.time() - self._ORPHAN_AGE
        for meta in self.root.rglob('*.type'):
            try:
                if meta.stat().st_mtime < cutoff and not meta.with_suffix('.blob').exists():
                    meta.unlink(missing_ok=True)
            except OSError:
                continue

    def _trim(self) -> int:
        self._sweep_orphans()
        entries: list[tuple[float, int, Path]] = []
        total = 0
        for blob in self.root.rglob('*.blob'):
            try:
                st = blob.stat()
            except OSError:
                continue
            entries.append((st.st_mtime, st.st_size, blob))
            total += st.st_size
        if total <= self.max_bytes:
            return total
        entries.sort()
        target = int(self.max_bytes * 0.9)
        for _, size, blob in entries:
            if total <= target:
                break
            try:
                blob.unlink(missing_ok=True)
            except OSError:
                continue
            total -= size
            with contextlib.suppress(OSError):
                blob.with_suffix('.type').unlink(missing_ok=True)
        return total


class Prefetcher:
    def __init__(self) -> None:
        self._seen: set[str] = set()
        self._tasks: set[asyncio.Task] = set()

    def spawn(self, coro, dedup_key: str) -> None:
        if dedup_key in self._seen:
            coro.close()
            return
        self._seen.add(dedup_key)
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        task.add_done_callback(lambda _: self._seen.discard(dedup_key))

    async def drain(self) -> None:
        tasks = [t for t in self._tasks if not t.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
