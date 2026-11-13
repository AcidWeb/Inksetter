"""
Komga specific features.
The mirror of Komga's own OPDS layout under each profile, and the three REST calls used by readers.
"""

import re
import json
import httpx
from urllib.parse import quote, unquote, urlsplit

from .settings import settings
from .opds.rewrite import ATOM
from .opds.upstream import UpstreamError, client, host_key, status_error

_CATALOG = re.compile(r'(.*/opds/v1\.2)/catalog')
MIRROR = '/opds/v1.2'
SIGNATURE = 'komga'
_ID = re.compile(r'[A-Za-z0-9_-]{1,64}')
_STREAM = re.compile(r'books/[A-Za-z0-9_-]{1,64}/pages/\{pageNumber\}')
_ADDED = ('maxWidth', 'count')
_PATH_SAFE = "/!$&'()*+,;=:@"
_REFUSED = frozenset({'.', '..', 'file'})
_PROGRESS = {'page': int, 'completed': bool}
PROGRESS_MAX = 1024


def opds_root() -> str:
    catalog = settings.upstream_catalog or ''
    try:
        parts = urlsplit(catalog)
    except ValueError:
        return ''
    found = _CATALOG.fullmatch(parts.path)
    if not found or not parts.netloc or '@' in parts.netloc or parts.scheme.lower() not in ('http', 'https'):
        return ''
    return f'{parts.scheme}://{parts.netloc}{found.group(1)}'


def mirror_root() -> str:
    root = opds_root()
    if not root:
        raise UpstreamError(404, 'not a Komga catalog')
    return root


def signs(feed) -> bool:
    author = feed.find(f'{{{ATOM}}}author')
    names = '' if author is None else ' '.join(author.findtext(f'{{{ATOM}}}{tag}') or '' for tag in ('name', 'uri'))
    return SIGNATURE in names.lower()


def _under(url: str, root: str) -> tuple[str, str] | None:
    try:
        parts, home = urlsplit(url), urlsplit(root)
        here = (
            bool(root)
            and parts.scheme.lower() == home.scheme.lower()
            and host_key(url) == host_key(root)
            and parts.path.startswith(f'{home.path}/')
        )
    except ValueError:
        return None
    if not here or parts.fragment or '{' in parts.query or '}' in parts.query:
        return None
    return parts.path[len(home.path) + 1 :], parts.query


def _servable(rest: str) -> bool:
    return not any(step.partition(';')[0] in _REFUSED for step in rest.split('/'))


def _with(query: str) -> str:
    return f'?{query}' if query else ''


def feed_link(url: str, root: str) -> str | None:
    found = _under(url, root)
    if found is None:
        return None
    rest, query = found
    exact = quote(unquote(rest), safe=_PATH_SAFE) == rest
    if not rest or not exact or not _servable(rest):
        return None
    return f'{MIRROR}/{rest}{_with(query)}'


def stream_link(url: str, root: str) -> str | None:
    found = _under(url, root)
    if found is None:
        return None
    rest, query = found
    names = [pair.partition('=')[0] for pair in query.split('&')] if query else []
    if not _STREAM.fullmatch(rest) or set(names) & set(_ADDED):
        return None
    return f'{MIRROR}/{rest}{_with(query)}'


def mirror_feed(rest: str) -> str:
    root = mirror_root()
    if not _servable(rest):
        raise UpstreamError(404, f'not a feed: {rest[:40]}')
    return f'{root}/{quote(rest, safe=_PATH_SAFE)}'


def _id(value: str, what: str) -> str:
    if not _ID.fullmatch(value or ''):
        raise UpstreamError(404, f'not a Komga {what} id: {value[:24]}')
    return value


def unmirror(book: str, query: str) -> str:
    root = mirror_root()
    kept = [pair for pair in query.split('&') if pair.partition('=')[0] not in _ADDED] if query else []
    return f'{root}/books/{_id(book, "book")}/pages/{{pageNumber}}{_with("&".join(kept))}'


def _api() -> str:
    return mirror_root()[: -len(MIRROR)]


def book_url(book: str) -> str:
    return f'{_api()}/api/v1/books/{_id(book, "book")}'


def series_thumbnail_url(series: str) -> str:
    return f'{_api()}/api/v1/series/{_id(series, "series")}/thumbnail'


def progress_body(raw: bytes) -> bytes:
    try:
        body = json.loads(raw) if len(raw) <= PROGRESS_MAX else None
    except ValueError:
        body = None
    fits = isinstance(body, dict) and body and set(body) <= set(_PROGRESS)
    if not fits or any(type(value) is not _PROGRESS[name] for name, value in body.items()):
        raise UpstreamError(400, 'a progress report is a JSON object of a page and whether the book is completed')
    if 'page' in body and body['page'] < 1:
        raise UpstreamError(400, 'a progress report counts pages from 1')
    return json.dumps(body, separators=(',', ':')).encode()


async def report(book: str, raw: bytes, headers: dict[str, str]) -> httpx.Response:
    url = f'{book_url(book)}/read-progress'
    body = progress_body(raw)
    try:
        resp = await client.request('PATCH', url, content=body, headers={**headers, 'content-type': 'application/json'})
    except httpx.HTTPError, httpx.InvalidURL:
        raise UpstreamError(502, 'Komga did not answer the progress report') from None
    if resp.status_code >= 500 or resp.status_code in (401, 407):
        raise status_error(resp.status_code, resp.headers)
    return resp
