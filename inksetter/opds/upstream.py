"""
Upstream fetching.
"""

import re
import httpx
import asyncio
import collections
from urllib.parse import urlsplit

from ..settings import settings

FORWARD_REQUEST = (
    'authorization',
    'cookie',
    'x-api-key',
    'accept-language',
    'user-agent',
)
CREDENTIAL_REQUEST = frozenset({'authorization', 'cookie', 'x-api-key'})
FORWARD_RESPONSE = (
    'content-type',
    'content-disposition',
    'last-modified',
    'etag',
)
DEFAULT_CHALLENGE = 'Basic realm="Inksetter"'
MAX_REDIRECTS = 5
TIMEOUT = 30.0

_DEFAULT_PORTS = {'http': '80', 'https': '443'}
_RANGE_BLOCK = 1 << 20
_RANGE_CACHE_BLOCKS = 8
_CONTENT_RANGE = re.compile(r'bytes\s+\d+-\d+/(\d+)', re.I)


class UpstreamError(Exception):
    def __init__(self, status: int, detail: str = '', headers: dict[str, str] | None = None) -> None:
        super().__init__(detail or f'upstream status {status}')
        self.status = status
        self.headers = headers or {}


def host_key(url: str) -> str:
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        return ''
    host = (parts.hostname or '').lower()
    if port is None or str(port) == _DEFAULT_PORTS.get(parts.scheme.lower()):
        return host
    return f'{host}:{port}'


def _allowed_key() -> str:
    return host_key(settings.upstream_catalog) if settings.upstream_catalog else ''


def check_host(url: str) -> None:
    allowed = _allowed_key()
    if not allowed:
        raise UpstreamError(500, 'no upstream configured: set UPSTREAM_CATALOG')
    host = host_key(url)
    if host != allowed:
        raise UpstreamError(403, f'host not allowed: {host or url[:40]}')


def _upstream_failure(exc: Exception) -> UpstreamError:
    if isinstance(exc, httpx.TransportError):
        return UpstreamError(502, f'upstream unreachable: {type(exc).__name__}')
    return UpstreamError(502, f'upstream answered badly: {type(exc).__name__}')


def _status_error(status: int, headers) -> UpstreamError:
    if status >= 500:
        return UpstreamError(502, f'upstream is failing: status {status}')
    if status == 407:
        return UpstreamError(502, 'the outbound proxy wants credentials; the reader cannot give them')
    if status != 401:
        return UpstreamError(status)
    return UpstreamError(401, headers={'www-authenticate': headers.get('www-authenticate') or DEFAULT_CHALLENGE})


def _redirect_target(url: str, resp: httpx.Response) -> str | None:
    if not resp.is_redirect:
        return None
    location = resp.headers.get('location')
    if not location:
        return None
    return str(httpx.URL(url).join(location))


def _hop_headers(headers: dict[str, str], url: str, origin: str) -> dict[str, str]:
    if host_key(url) == origin:
        return headers
    return {k: v for k, v in headers.items() if k.lower() not in CREDENTIAL_REQUEST}


def request_headers(incoming, *, forward_accept: bool = False, accept: str = '*/*') -> dict[str, str]:
    out = {}
    for name in FORWARD_REQUEST:
        value = incoming.get(name)
        if value:
            out[name] = value
    client_accept = incoming.get('accept') if forward_accept else None
    out['accept'] = client_accept or accept
    return out


class Client:
    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            verify=False,
            timeout=httpx.Timeout(TIMEOUT),
            limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
            follow_redirects=False,
        )

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def raw(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError('client not started')
        return self._client

    async def get(self, url: str, headers: dict[str, str]) -> httpx.Response:
        origin = host_key(url)
        try:
            for _ in range(MAX_REDIRECTS + 1):
                check_host(url)
                resp = await self.raw.get(url, headers=_hop_headers(headers, url, origin))
                target = _redirect_target(url, resp)
                if target is None:
                    if resp.status_code >= 400:
                        raise _status_error(resp.status_code, resp.headers)
                    return resp
                url = target
        except (httpx.HTTPError, httpx.InvalidURL) as exc:
            raise _upstream_failure(exc) from exc
        raise UpstreamError(502, f'more than {MAX_REDIRECTS} redirects')

    async def stream(self, url: str, headers: dict[str, str], sink=None) -> httpx.Response:
        origin = host_key(url)
        try:
            for _ in range(MAX_REDIRECTS + 1):
                check_host(url)
                async with self.raw.stream('GET', url, headers=_hop_headers(headers, url, origin)) as resp:
                    target = _redirect_target(url, resp)
                    if target is None:
                        if resp.status_code >= 400:
                            raise _status_error(resp.status_code, resp.headers)
                        if sink is not None:
                            async for chunk in resp.aiter_bytes(1 << 20):
                                await asyncio.to_thread(sink.write, chunk)
                        return resp
                url = target
        except httpx.HTTPError as exc:
            raise _upstream_failure(exc) from exc
        raise UpstreamError(502, f'more than {MAX_REDIRECTS} redirects')


client = Client()


class RangeReader:
    def __init__(self, url, headers, client, size, origin, resp_headers):
        self._url = url
        self._headers = headers
        self._client = client
        self._size = size
        self._origin = origin
        self.headers = resp_headers
        self._pos = 0
        self._blocks: collections.OrderedDict = collections.OrderedDict()
        self.closed = False

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            pos = offset
        elif whence == 1:
            pos = self._pos + offset
        elif whence == 2:
            pos = self._size + offset
        else:
            raise ValueError(f'invalid whence: {whence}')
        self._pos = max(0, min(pos, self._size))
        return self._pos

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = self._size - self._pos
        size = min(size, self._size - self._pos)
        if size <= 0:
            return b''
        out = self._read_at(self._pos, size)
        self._pos += len(out)
        return out

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._blocks.clear()
            self._client.close()

    def _read_at(self, pos: int, size: int) -> bytes:
        if size >= _RANGE_BLOCK:
            return self._fetch(pos, pos + size - 1)
        parts = []
        while size > 0:
            index = pos // _RANGE_BLOCK
            block = self._block(index)
            start = pos - index * _RANGE_BLOCK
            piece = block[start : start + size]
            if not piece:
                break
            parts.append(piece)
            pos += len(piece)
            size -= len(piece)
        return b''.join(parts)

    def _block(self, index: int) -> bytes:
        cached = self._blocks.get(index)
        if cached is not None:
            self._blocks.move_to_end(index)
            return cached
        start = index * _RANGE_BLOCK
        block = self._fetch(start, min(start + _RANGE_BLOCK, self._size) - 1)
        self._blocks[index] = block
        while len(self._blocks) > _RANGE_CACHE_BLOCKS:
            self._blocks.popitem(last=False)
        return block

    def _fetch(self, first: int, last: int) -> bytes:
        url = self._url
        try:
            return self._fetch_inner(url, first, last)
        except httpx.HTTPError as exc:
            raise _upstream_failure(exc) from exc

    def _fetch_inner(self, url: str, first: int, last: int) -> bytes:
        for _ in range(MAX_REDIRECTS + 1):
            check_host(url)
            headers = dict(_hop_headers(self._headers, url, self._origin))
            headers['range'] = f'bytes={first}-{last}'
            headers['accept-encoding'] = 'identity'
            with self._client.stream('GET', url, headers=headers) as resp:
                target = _redirect_target(url, resp)
                if target is None:
                    if resp.status_code != 206:
                        raise UpstreamError(502, f'range request answered {resp.status_code}')
                    resp.read()
                    return resp.content
            url = target
        raise UpstreamError(502, f'more than {MAX_REDIRECTS} redirects')


def open_range(url: str, headers: dict[str, str]) -> RangeReader | None:
    try:
        client = httpx.Client(
            verify=False,
            timeout=httpx.Timeout(TIMEOUT),
            follow_redirects=False,
        )
    except Exception:
        return None
    try:
        origin = host_key(url)
        target = url
        status, resp_headers = None, None
        for _ in range(MAX_REDIRECTS + 1):
            check_host(target)
            probe = dict(_hop_headers(headers, target, origin))
            probe['range'] = 'bytes=0-0'
            probe['accept-encoding'] = 'identity'
            with client.stream('GET', target, headers=probe) as resp:
                nxt = _redirect_target(target, resp)
                if nxt is None:
                    status, resp_headers = resp.status_code, resp.headers
                    break
            target = nxt
        else:
            client.close()
            return None
        if status != 206:
            client.close()
            return None
        match = _CONTENT_RANGE.search(resp_headers.get('content-range', ''))
        if not match:
            client.close()
            return None
        size = int(match.group(1))
        if size <= 0:
            client.close()
            return None
        return RangeReader(target, headers, client, size, origin, resp_headers)
    except Exception:
        client.close()
        return None
