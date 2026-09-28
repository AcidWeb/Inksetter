"""
Entry point for binary build.
"""

import os
import sys
import tomllib
import traceback
import subprocess
from pathlib import Path

SETTING_NAMES = frozenset(
    {
        'UPSTREAM_CATALOG',
        'PUBLIC_BASE',
        'HOST',
        'PORT',
        'CACHE_DIR',
        'CACHE_MAX_BYTES',
        'SPOOL_DIR',
        'SPOOL_MAX_BYTES',
        'PREFETCH',
        'RENDER_WORKERS',
        'REPACK_WORKERS',
        'REPACK_PAGE_WORKERS',
        'OCR_ENABLED',
        'LOG_LEVEL',
        'VIPS_CONCURRENCY',
    }
)

TEMPLATE = """\
# Inksetter settings.

# The full OPDS catalog URL of your server, including any API key in it:
#   Kavita: "http://kavita:5000/api/opds/<api-key>"
#   Komga:  "http://komga:25600/opds/v1.2/catalog"
#   Codex:  "http://codex:9810/opds/v1.2"
upstream_catalog = ""

# Where the proxy listens.
# host = "0.0.0.0"
# port = 8080

# Skip removing page numbers, the slowest part of the pipeline.
# ocr_enabled = false
"""


WINDOWS_PATH = "A Windows path goes in single quotes, as in 'D:\\Inksetter\\cache'."


class ConfigError(Exception):
    pass


def config_path() -> Path:
    if sys.platform == 'win32':
        return Path(os.environ.get('LOCALAPPDATA') or Path.home()) / 'Inksetter' / 'inksetter.toml'
    return Path(os.environ.get('XDG_CONFIG_HOME') or Path.home() / '.config') / 'Inksetter' / 'inksetter.toml'


def load(path: Path) -> dict[str, str]:
    try:
        text = path.read_bytes().decode('utf-8-sig')
    except FileNotFoundError:
        return {}
    except UnicodeDecodeError:
        raise ConfigError(f'{path} is not UTF-8 text') from None
    try:
        raw = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        lines = text.splitlines()
        line = lines[exc.lineno - 1] if 0 < exc.lineno <= len(lines) else ''
        raise ConfigError(f'{path}: {exc}.' + (f' {WINDOWS_PATH}' if '\\' in line else '')) from None
    values = {}
    for key, value in raw.items():
        name = key.upper()
        if name not in SETTING_NAMES:
            raise ConfigError(f'{path}: there is no setting called {key!r}')
        if isinstance(value, bool):
            value = 'true' if value else 'false'
        elif not isinstance(value, str | int):
            raise ConfigError(f'{path}: {key} must be text, a whole number, or true or false')
        elif isinstance(value, str) and any(ord(c) < 32 for c in value):
            raise ConfigError(f'{path}: {key} holds a control character. {WINDOWS_PATH}')
        values[name] = str(value)
    return values


def apply(values: dict[str, str]) -> None:
    for name, value in values.items():
        if value and not os.environ.get(name):
            os.environ[name] = value


def _own_console() -> bool:
    if sys.platform != 'win32' or not sys.stdin or not sys.stdin.isatty():
        return False
    import ctypes

    return ctypes.windll.kernel32.GetConsoleProcessList((ctypes.c_uint * 2)(), 2) == 1


def _edit(path: Path) -> None:
    subprocess.Popen(['notepad.exe', str(path)])


def main() -> int:
    path = config_path()
    try:
        apply(load(path))
        port = int(os.environ.get('PORT', '8080'))
    except ConfigError as exc:
        print(exc)
        return 1
    except ValueError:
        print(f'PORT must be a whole number, not {os.environ["PORT"]!r}')
        return 1

    if not os.environ.get('UPSTREAM_CATALOG'):
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(TEMPLATE, encoding='utf-8')
        print(f'No upstream catalog is set. Put its URL in {path}, save it and start Inksetter again.')
        if _own_console():
            _edit(path)
        return 1

    print(f'Settings from {path}' if path.exists() else f'Settings from the environment; {path} does not exist')
    import uvicorn

    from inksetter.app import app

    uvicorn.run(app, host=os.environ.get('HOST', '0.0.0.0'), port=port)
    return 0


def run() -> None:
    try:
        code = main()
    except SystemExit as exc:
        code = exc.code
    except Exception:
        traceback.print_exc()
        code = 1
    if code and _own_console():
        try:
            input('Press Enter to close.')
        except EOFError:
            pass
    sys.exit(code)


if __name__ == '__main__':
    run()
