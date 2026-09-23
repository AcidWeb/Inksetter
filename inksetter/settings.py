"""
Runtime configuration.
All via environment variables.
"""

import os
from pathlib import Path
from dataclasses import dataclass


def _int(name: str, default: int, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.environ.get(name, default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    # Full upstream catalog URL, including any API key in the path.
    #   Kavita: http://kavita:5000/api/opds/<api-key>
    #   Komga:  http://komga:25600/opds/v1.2/catalog
    #   Codex:  http://codex:9810/opds/v1.2
    upstream_catalog: str = os.environ.get('UPSTREAM_CATALOG', '').rstrip('/')

    # How this proxy is reachable from the reader.
    # Only needed when reverse proxy is in use.
    public_base: str = os.environ.get('PUBLIC_BASE', '').rstrip('/')

    # Location of cache used by OPDS-PSE.
    cache_dir: Path = Path(os.environ.get('CACHE_DIR', '/cache'))

    # Maximum cache size.
    cache_max_bytes: int = _int('CACHE_MAX_BYTES', 8 * 1024**3)

    # Pages fetched ahead in the background after each served page.
    prefetch: int = _int('PREFETCH', 3)

    # Concurrent libvips renders.
    render_workers: int = _int('RENDER_WORKERS', 3, minimum=1)

    # Concurrent repack STEPS - not concurrent downloads.
    repack_workers: int = _int('REPACK_WORKERS', 2, minimum=1)

    # Pages rendered concurrently within ONE repack.
    # Three rather than the core count, because libvips is internally threaded.
    repack_page_workers: int = _int('REPACK_PAGE_WORKERS', 3, minimum=1)

    # Downloads and repacks stream through a SpooledTemporaryFile: RAM up to this, then disk.
    spool_max_bytes: int = _int('SPOOL_MAX_BYTES', 32 * 1024**2)

    # Where a spilled spool file lands.
    spool_dir: str = os.environ.get('SPOOL_DIR', '') or str(Path(os.environ.get('CACHE_DIR', '/cache')))


settings = Settings()
