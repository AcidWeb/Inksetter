"""
Logging setup.
"""

import os
import logging

NOISY = ('httpx', 'httpcore', 'uvicorn.access', 'pyvips')

FORMAT = '%(asctime)s %(levelname)-7s %(name)s %(message)s'


def level() -> int:
    raw = os.environ.get('LOG_LEVEL', 'INFO').strip().upper()
    value = logging.getLevelNamesMapping().get(raw)
    return value if isinstance(value, int) else logging.INFO


def configure() -> int:
    want = level()
    root = logging.getLogger()
    if not any(getattr(h, '_inksetter', False) for h in root.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(FORMAT))
        handler._inksetter = True
        root.addHandler(handler)
    root.setLevel(want)
    logging.getLogger('inksetter').setLevel(want)
    for name in NOISY:
        logging.getLogger(name).setLevel(logging.WARNING)
    return want
