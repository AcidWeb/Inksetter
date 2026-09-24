"""
Helper for detection of the number of available CPU cores
"""

import os
import math
from pathlib import Path

CGROUP = Path('/sys/fs/cgroup')


def _quota(root: Path) -> float | None:
    try:
        quota, period = (root / 'cpu.max').read_text().split()
        return None if quota == 'max' else int(quota) / int(period)
    except OSError, ValueError, ZeroDivisionError:
        pass
    try:
        quota = int((root / 'cpu' / 'cpu.cfs_quota_us').read_text())
        period = int((root / 'cpu' / 'cpu.cfs_period_us').read_text())
        return quota / period if quota > 0 else None
    except OSError, ValueError, ZeroDivisionError:
        return None


def available(root: Path = CGROUP) -> int:
    cores = os.process_cpu_count() or 4
    quota = _quota(root)
    return min(cores, math.ceil(quota)) if quota and quota > 0 else cores


def page_workers() -> int:
    return min(8, max(3, available() // 2))
