"""
HTTP surface.
"""

import os
import re
import time
import socket
import asyncio
import logging
import tempfile
import functools
import threading
import collections
import contextlib
import importlib.resources
from email.message import Message
from urllib.parse import quote, unquote, urljoin, urlsplit, urlunsplit
from fastapi import FastAPI, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)

from . import __version__, cores, kavita, kosync, logs, status, web
from .cache import DiskCache, Prefetcher, render_key
from .imaging import azw3, cbz, profiles
from .imaging.pipeline import RenderFailed, UnreadableImage, render_page
from .imaging.profiles import Profile, cover_for
from .opds import rewrite
from .opds.upstream import CREDENTIAL_REQUEST, FORWARD_RESPONSE, UpstreamError, client, open_range, request_headers
from .settings import settings

log = logging.getLogger(__name__)

logs.configure()
profiles.use_folio(settings.ocr_enabled)


def _redact(text: str) -> str:
    key = kavita.api_key()
    return re.sub(re.escape(key), '\u2026', text, flags=re.I) if key else text


board = status.Board(_redact)
logging.getLogger('inksetter').addHandler(status.Problems(board))

cache = DiskCache(settings.cache_dir, settings.cache_max_bytes)
books = kosync.Books(settings.cache_dir / 'kosync')
prefetcher = Prefetcher()
render_sem = asyncio.Semaphore(settings.render_workers)
repack_slots = asyncio.Semaphore(settings.repack_workers)
CPUS = cores.available()

JOB_ID = re.compile(r'^[A-Za-z0-9-]{1,64}$')


class Jobs:
    def __init__(self, limit: int = 64, ttl: float = 900.0) -> None:
        self.limit, self.ttl = limit, ttl
        self._lock = threading.Lock()
        self._board: collections.OrderedDict[str, dict] = collections.OrderedDict()

    def _prune(self, now: float) -> None:
        for key in [k for k, v in self._board.items() if now - v['at'] > self.ttl]:
            del self._board[key]
        while len(self._board) > self.limit:
            self._board.popitem(last=False)

    def set(self, job: str, **fields) -> None:
        if not JOB_ID.match(job):
            return
        now = time.monotonic()
        with self._lock:
            state = self._board.pop(job, {'stage': 'starting', 'done': 0, 'total': 0})
            state.update(fields, at=now)
            self._board[job] = state
            self._prune(now)

    def get(self, job: str) -> dict | None:
        with self._lock:
            state = self._board.get(job)
            return {k: v for k, v in state.items() if k != 'at'} if state else None


jobs = Jobs()


def _reporter(track: status.Download):
    def report(done: int, total: int) -> None:
        track.update(stage='repacking', done=done, total=total)

    return report


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001 - signature fixed by FastAPI
    await client.start()
    try:
        yield
    finally:
        await prefetcher.drain()
        await client.stop()


app = FastAPI(lifespan=lifespan, openapi_url=None)


def _profile_or_404(name: str) -> Profile:
    p = profiles.get(name)
    if p is None:
        raise UpstreamError(404, f'unknown profile: {name}')
    return p


def _refuse_stream(p: Profile) -> None:
    if p.reslice:
        raise UpstreamError(
            404, 'this profile does not stream pages: download the book, which is re-cut for the screen'
        )


def _public_base(request: Request) -> str:
    if settings.public_base:
        return settings.public_base
    return str(request.base_url).rstrip('/')


def _ctx(request: Request, profile: str, upstream_url: str) -> rewrite.Ctx:
    return rewrite.Ctx(profile=profile, public_base=_public_base(request), base_url=upstream_url)


def _error(request: Request, status: int, detail: str, headers: dict[str, str] | None = None) -> Response:
    detail = _redact(detail)
    if 'text/html' in request.headers.get('accept', ''):
        body = web.error(status, detail, _public_base(request))
        return HTMLResponse(body, status_code=status, headers=headers)
    return JSONResponse({'error': detail}, status_code=status, headers=headers)


@app.exception_handler(UpstreamError)
async def _upstream_error(request: Request, exc: UpstreamError):
    return _error(request, exc.status, str(exc), exc.headers)


@app.exception_handler(rewrite.TokenError)
async def _token_error(request: Request, exc: rewrite.TokenError):
    return _error(request, 400, str(exc))


@app.exception_handler(rewrite.FeedError)
async def _feed_error(request: Request, exc: rewrite.FeedError):
    return _error(request, 502, str(exc))


LOGO_BYTES = importlib.resources.files(__package__).joinpath(web.LOGO).read_bytes()


@app.api_route(f'/{web.LOGO}', methods=['GET', 'HEAD'])
@app.api_route('/favicon.ico', methods=['GET', 'HEAD'])
async def logo():
    return Response(
        LOGO_BYTES,
        media_type='image/png',
        headers={'Cache-Control': 'public, max-age=604800'},
    )


LOOPBACK = frozenset({'localhost', '127.0.0.1', '::1'})
CONTAINER_MARKS = ('/.dockerenv', '/run/.containerenv')
ROUTE_PROBE = ('192.0.2.1', 9)


def _lan_address() -> str:
    if any(os.path.exists(mark) for mark in CONTAINER_MARKS):
        return ''
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect(ROUTE_PROBE)
            address = probe.getsockname()[0]
    except OSError:
        return ''
    return '' if address.startswith('127.') or address == '0.0.0.0' else address


def _for_readers(base: str) -> str:
    if settings.public_base:
        return base
    parts = urlsplit(base)
    host = parts.hostname or ''
    if host not in LOOPBACK and not host.endswith('.localhost'):
        return base
    lan = _lan_address()
    if not lan:
        return base
    netloc = f'{lan}:{parts.port}' if parts.port else lan
    return urlunsplit(parts._replace(netloc=netloc)).rstrip('/')


@app.api_route('/', methods=['GET', 'HEAD'], response_class=HTMLResponse)
async def index(request: Request):
    base = _public_base(request)
    root = f'api/opds/{kavita.MIRROR_KEY}' if kavita.opds_root() else 'catalog'
    return web.index(base, sorted(profiles.PROFILES.items()), _for_readers(base), root)


def _upstream_host() -> str:
    try:
        parts = urlsplit(settings.upstream_catalog)
        host, port = parts.hostname or '', parts.port
    except ValueError:
        return ''
    if ':' in host:
        host = f'[{host}]'
    shown = f'{host}:{port}' if port else host
    return f'{shown} (Kavita)' if shown and kavita.api_key() else shown


@app.api_route('/status', methods=['GET', 'HEAD'])
async def status_page(request: Request):
    snap = {
        'version': __version__,
        'upstream': _upstream_host(),
        'ocr': settings.ocr_enabled,
        'cpus': CPUS,
        'workers': {
            'render': settings.render_workers,
            'repack': settings.repack_workers,
            'pages': settings.repack_page_workers,
        },
        'cache': {'dir': str(settings.cache_dir), 'size': cache.size, 'cap': cache.max_bytes},
        **board.snapshot(),
    }
    if 'text/html' in request.headers.get('accept', ''):
        return HTMLResponse(web.status(snap, _public_base(request)))
    return JSONResponse(snap)


# --------------------------------------------------------------------------
# Feeds
# --------------------------------------------------------------------------


async def _serve_feed(request: Request, profile_name: str, url: str) -> Response:
    p = _profile_or_404(profile_name)
    resp = await client.get(url, request_headers(request.headers, forward_accept=True))
    ctype = resp.headers.get('content-type', '')
    page_mime = None if p.fmt == 'raw' else p.mime
    if rewrite.is_feed(ctype, resp.content) or rewrite.looks_like_feed(resp.content):
        body = rewrite.rewrite(resp.content, _ctx(request, profile_name, str(resp.url)), page_mime, not p.reslice)
    else:
        body = resp.content
    return Response(content=body, media_type=ctype or 'application/atom+xml')


@app.api_route('/{profile}/catalog', methods=['GET', 'HEAD'])
async def catalog(profile: str, request: Request):
    return await _serve_feed(request, profile, settings.upstream_catalog)


def _with_query(url: str, request: Request) -> str:
    if not request.url.query:
        return url
    joiner = '&' if '?' in url else '?'
    return f'{url}{joiner}{request.url.query}'


@app.api_route('/{profile}/f/{token}', methods=['GET', 'HEAD'])
async def feed(profile: str, token: str, request: Request):
    return await _serve_feed(request, profile, _with_query(rewrite.decode_token(token), request))


@app.api_route('/{profile}/osd/{token}', methods=['GET', 'HEAD'])
async def opensearch(profile: str, token: str, request: Request):
    _profile_or_404(profile)
    url = rewrite.decode_token(token)
    resp = await client.get(url, request_headers(request.headers, forward_accept=True))
    body = rewrite.rewrite(resp.content, _ctx(request, profile, str(resp.url)), None)
    return Response(body, media_type='application/opensearchdescription+xml')


@app.api_route('/{profile}/s/{token}', methods=['GET', 'HEAD'])
async def search(
    profile: str,
    token: str,
    request: Request,
    q: str = '',
    query: str = '',
    searchTerms: str = '',
):
    term = q or query or searchTerms
    url = rewrite.fill_search(rewrite.decode_token(token), term)
    return await _serve_feed(request, profile, url)


# --------------------------------------------------------------------------
# Browsing
# --------------------------------------------------------------------------

BROWSE_ACCEPT = 'application/atom+xml, application/xml;q=0.9'


async def _serve_html(request: Request, profile_name: str, url: str, term: str = '') -> Response:
    p = _profile_or_404(profile_name)
    resp = await client.get(url, request_headers(request.headers, accept=BROWSE_ACCEPT))
    base = _public_base(request)
    feed = web.parse(resp.content, _ctx(request, profile_name, str(resp.url)))
    if feed is None:
        return HTMLResponse(web.unsupported(p, base))
    trail = request.query_params.get(web.TRAIL_PARAM, '')
    return HTMLResponse(web.page(feed, p, base, term, trail, rewrite.encode_token(url)))


@app.api_route('/{profile}/browse', methods=['GET', 'HEAD'], response_class=HTMLResponse)
async def browse_root(profile: str, request: Request):
    return await _serve_html(request, profile, settings.upstream_catalog)


@app.api_route('/{profile}/b/{token}', methods=['GET', 'HEAD'], response_class=HTMLResponse)
async def browse(profile: str, token: str, request: Request):
    return await _serve_html(request, profile, rewrite.decode_token(token))


@app.api_route('/{profile}/bs/{token}', methods=['GET', 'HEAD'], response_class=HTMLResponse)
async def browse_search(profile: str, token: str, request: Request, q: str = ''):
    _profile_or_404(profile)
    url = rewrite.decode_token(token)
    if not rewrite.is_search_template(url):
        resp = await client.get(url, request_headers(request.headers, accept=BROWSE_ACCEPT))
        template = rewrite.search_template(resp.content)
        if template is None:
            raise UpstreamError(502, 'upstream offers no search template')
        try:
            url = urljoin(str(resp.url), template)
        except ValueError as exc:
            raise UpstreamError(502, 'upstream search template is malformed') from exc
    return await _serve_html(request, profile, rewrite.fill_search(url, q), q)


# --------------------------------------------------------------------------
# Images
# --------------------------------------------------------------------------


def _credentials(headers: dict[str, str]) -> str:
    return '\n'.join(f'{k}={headers[k]}' for k in sorted(CREDENTIAL_REQUEST) if headers.get(k))


async def _render_cached(
    url: str, p: Profile, max_width: int | None, headers: dict[str, str], kind: str = 'page'
) -> tuple[bytes, str]:
    began = time.monotonic()
    key = render_key(url, p, max_width, _credentials(headers))
    hit = await asyncio.to_thread(cache.get, key)
    if hit is not None:
        board.served(kind, True, time.monotonic() - began)
        return hit

    async with cache.lock(key):
        hit = await asyncio.to_thread(cache.get, key)
        if hit is not None:
            board.served(kind, True, time.monotonic() - began)
            return hit
        resp = await client.get(url, headers)
        if p.fmt == 'raw':
            blob = resp.content
            ctype = resp.headers.get('content-type', 'application/octet-stream')
            if rewrite.looks_like_feed(blob):
                raise UpstreamError(502, 'upstream answered a feed, not a page')
        else:
            async with render_sem:
                start = time.monotonic()
                try:
                    blob, ctype = await asyncio.to_thread(render_page, resp.content, p, max_width)
                except UnreadableImage as exc:
                    detail = str(exc) if isinstance(exc, RenderFailed) else f'upstream page: {exc}'
                    raise UpstreamError(502, detail) from exc
                board.rendered(kind, time.monotonic() - start)
        await asyncio.to_thread(cache.put, key, blob, ctype)

    prefetcher.spawn(cache.maybe_trim(), 'cache-trim')
    board.served(kind, False, time.monotonic() - began)
    return blob, ctype


def _query_int(raw: str) -> int:
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


@app.api_route('/{profile}/p/{token}', methods=['GET', 'HEAD'])
@app.api_route('/{profile}/p/{token}/api/opds/{key}/image', methods=['GET', 'HEAD'])
async def page(profile: str, token: str, request: Request, page: str = '0', maxWidth: str = '0', count: str = '0'):
    p = _profile_or_404(profile)
    _refuse_stream(p)
    template = rewrite.decode_token(token)
    return await _stream(request, p, template, _query_int(page), _query_int(maxWidth), _query_int(count))


async def _stream(request: Request, p: Profile, template: str, page_no: int, max_width: int, pages: int) -> Response:
    headers = request_headers(request.headers)
    told = kavita.streams(template)
    fetch = kavita.unread(headers) if told else headers

    def build(n: int) -> str:
        return template.replace('{pageNumber}', str(n)).replace('{maxWidth}', str(max(p.width, max_width) or ''))

    blob, ctype = await _render_cached(build(page_no), p, max_width or None, fetch)
    if told and request.method == 'GET':
        lane = kavita.reading.note(template, build(page_no), headers)
        if lane is not None:
            prefetcher.spawn(lane)

    ahead = page_no + 1 + settings.prefetch
    for n in range(page_no + 1, min(ahead, pages) if pages else ahead):
        url = build(n)
        key = render_key(url, p, max_width or None, _credentials(headers))
        prefetcher.spawn(_prefetch(url, p, max_width or None, fetch, key), key)

    return Response(
        blob,
        media_type=ctype,
        headers={'Cache-Control': 'private, max-age=86400'},
    )


async def _prefetch(url: str, p: Profile, max_width: int | None, headers: dict[str, str], key: str) -> None:
    with contextlib.suppress(Exception):
        if not await asyncio.to_thread(cache.has, key):
            await _render_cached(url, p, max_width, headers, 'ahead')


@app.api_route('/{profile}/pf/{token}', methods=['GET', 'HEAD'])
async def page_fixed(profile: str, token: str, request: Request, maxWidth: str = '0'):
    p = _profile_or_404(profile)
    _refuse_stream(p)
    url = rewrite.decode_token(token)
    blob, ctype = await _render_cached(url, p, _query_int(maxWidth) or None, request_headers(request.headers))
    return Response(blob, media_type=ctype, headers={'Cache-Control': 'private, max-age=86400'})


@app.api_route('/{profile}/img/{token}', methods=['GET', 'HEAD'])
async def image(profile: str, token: str, request: Request):
    p = _profile_or_404(profile)
    url = rewrite.decode_token(token)
    blob, ctype = await _render_cached(url, cover_for(p), None, request_headers(request.headers), 'cover')
    return Response(blob, media_type=ctype, headers={'Cache-Control': 'private, max-age=604800'})


# --------------------------------------------------------------------------
# Kavita's layout, mirrored under each profile
# --------------------------------------------------------------------------


@app.api_route('/{profile}/api/opds/{key}/image', methods=['GET', 'HEAD'])
async def mirrored_page(profile: str, request: Request, maxWidth: str = '0', count: str = '0'):
    p = _profile_or_404(profile)
    _refuse_stream(p)
    template, page_no = kavita.unmirror(request.url.query)
    return await _stream(request, p, template, _query_int(page_no), _query_int(maxWidth), _query_int(count))


@app.post('/{profile}/api/Plugin/authenticate')
async def stream_sign_in(profile: str):
    _refuse_stream(_profile_or_404(profile))
    kavita.mirror_root()
    return JSONResponse({'token': kavita.LOOKUP_TOKEN, 'refreshToken': kavita.LOOKUP_TOKEN})


@app.get('/{profile}/api/Reader/get-progress')
async def stream_progress(profile: str, chapterId: str = ''):
    _refuse_stream(_profile_or_404(profile))
    resp = await kavita.progress(chapterId)
    if resp is None:
        raise UpstreamError(502, 'Kavita refused the progress lookup')
    return _relay(resp)


@app.api_route('/{profile}/catalog/{rest:path}', methods=['GET', 'HEAD'])
@app.api_route('/{profile}/api/opds/{key}', methods=['GET', 'HEAD'])
@app.api_route('/{profile}/api/opds/{key}/{rest:path}', methods=['GET', 'HEAD'])
async def mirrored_feed(profile: str, request: Request, rest: str = ''):
    _profile_or_404(profile)
    url = kavita.mirror_feed(rest) if rest else settings.upstream_catalog
    return await _serve_feed(request, profile, _with_query(url, request))


# --------------------------------------------------------------------------
# Downloads
# --------------------------------------------------------------------------


def _drain(fobj, chunk: int = 1 << 18):
    try:
        while True:
            block = fobj.read(chunk)
            if not block:
                return
            yield block
    finally:
        fobj.close()


def _next_block(chunks, turn: threading.Lock):
    with turn:
        return next(chunks, None)


def _wind_down(chunks, body, turn: threading.Lock) -> None:
    with turn:
        with contextlib.suppress(Exception):
            chunks.close()
        with contextlib.suppress(Exception):
            body.close()


def _record(sync: kosync.Pending) -> None:
    try:
        sync.finish()
    except Exception:
        log.warning('progress sync: the download could not be recorded, so it will not sync', exc_info=True)


async def _repacking(chunks, body, track: status.Download, sync: kosync.Pending | None = None):
    turn = threading.Lock()
    outcome = 'error'
    try:
        while True:
            async with repack_slots:
                block = await asyncio.to_thread(_next_block, chunks, turn)
            if block is None:
                outcome = 'done'
                if sync is not None:
                    await asyncio.to_thread(_record, sync)
                return
            if not block:
                continue
            track.update(sent=track.sent + len(block))
            if sync is not None:
                sync.feed(block)
            yield block
    except GeneratorExit, asyncio.CancelledError:
        outcome = 'stopped'
        raise
    finally:
        track.finish(outcome)
        threading.Thread(target=_wind_down, args=(chunks, body, turn), name='repack-close', daemon=True).start()


def _spool():
    return tempfile.SpooledTemporaryFile(max_size=settings.spool_max_bytes, dir=settings.spool_dir)


_ZIP_MAGIC = (b'PK\x03\x04', b'PK\x05\x06', b'PK\x07\x08')


_EPUB_TYPE = 'application/epub+zip'


def _is_epub(ctype: str, head: bytes) -> bool:
    if ctype.split(';')[0].strip().lower() == _EPUB_TYPE:
        return True
    return head[30:38] == b'mimetype' and head[38:58] == _EPUB_TYPE.encode()


def _head_of(body, size: int = 64) -> bytes:
    pos = body.tell()
    try:
        body.seek(0)
        return body.read(size)
    finally:
        body.seek(pos)


def _is_feed_body(body) -> bool:
    head = _head_of(body, 4096)
    if rewrite.looks_like_feed(head):
        return True
    return rewrite.is_json(head) and rewrite.looks_like_feed(_head_of(body, 32 << 20))


def _is_comic_zip(ctype: str, body) -> bool:
    head = _head_of(body)
    return not _is_epub(ctype, head) and head[:4] in _ZIP_MAGIC


async def _cover_bytes(url: str | None, request: Request) -> bytes | None:
    if not url:
        return None
    try:
        resp = await client.get(url, request_headers(request.headers))
    except Exception:
        return None
    return resp.content or None


async def _pick_cover(url: str, feed_cover: str | None, request: Request) -> bytes | None:
    return await _cover_bytes(await kavita.cover_url(url), request) or await _cover_bytes(feed_cover, request)


def _stale(headers: dict[str, str]) -> dict[str, str]:
    out = {k: v for k, v in headers.items() if k.lower() in FORWARD_RESPONSE}
    for key in ('etag', 'last-modified', 'content-type'):
        out.pop(key, None)
    return out


async def _repack_over_ranges(url: str, cover_url: str | None, p: Profile, request: Request, track, pack=None):
    headers = request_headers(request.headers)
    reader = await asyncio.to_thread(open_range, url, headers)
    if reader is None:
        return None
    try:
        ctype = reader.headers.get('content-type', 'application/octet-stream')
        if not await asyncio.to_thread(_is_comic_zip, ctype, reader):
            await asyncio.to_thread(reader.close)
            return None
        return await (pack or _repack)(reader, url, cover_url, p, request, track, reader.headers)
    except Exception:
        await asyncio.to_thread(reader.close)
        return None
    except BaseException:
        await asyncio.to_thread(reader.close)
        raise


def _download_name(url: str, headers) -> str:
    try:
        disposition = Message()
        disposition['content-disposition'] = headers.get('content-disposition', '')
        name = disposition.get_filename() or unquote(urlsplit(url).path.rstrip('/').rpartition('/')[2])
    except Exception:
        name = ''
    return _redact(name or 'download')


async def _sync_for(src) -> kosync.Pending | None:
    if not kavita.api_key():
        return None
    try:
        source = await asyncio.to_thread(kosync.file_md5, src)
    except Exception:
        log.warning('progress sync: the source archive could not be hashed, so this download will not sync')
        return None
    return kosync.Pending(books, source)


async def _repack(src, url: str, cover_url: str | None, p: Profile, request: Request, track, upstream):
    track.update(name=_download_name(url, upstream))
    cover = await _pick_cover(url, cover_url, request)
    meta = await kavita.for_download(url)
    sync = await _sync_for(src)
    chunks = await asyncio.to_thread(
        cbz.repack_iter,
        src,
        p,
        settings.repack_page_workers,
        cover=cover,
        comicinfo=meta,
        progress=_reporter(track),
        sources=sync.pages if sync else None,
    )
    return StreamingResponse(
        _repacking(chunks, src, track, sync),
        media_type='application/vnd.comicbook+zip',
        headers=_stale(upstream),
    )


def _attachment(name: str) -> str:
    plain = ''.join(ch if ' ' <= ch <= '~' and ch not in '"\\' else '_' for ch in name)
    if plain == name:
        return f'attachment; filename="{name}"'
    return f'attachment; filename="{plain}"; filename*=UTF-8\'\'{quote(name, safe="")}'


def _kindle_name(url: str, upstream) -> tuple[str, str]:
    named = _download_name(url, upstream)
    stem, dot, suffix = named.rpartition('.')
    title = stem if dot and stem and suffix.lower() in ('cbz', 'zip') else named
    return title, f'{title}{azw3.SUFFIX}'


async def _kindle_book(src, url: str, cover_url: str | None, p: Profile, request: Request, track, upstream):
    title, name = _kindle_name(url, upstream)
    track.update(name=name)
    cover = await _pick_cover(url, cover_url, request)
    meta = await kavita.for_download(url)
    book = azw3.Book(_spool(), p.width, p.height, title, rewrite.encode_token(url))
    try:
        chunks = await asyncio.to_thread(
            cbz.repack_iter,
            src,
            p,
            settings.repack_page_workers,
            cover=cover,
            comicinfo=meta,
            progress=_reporter(track),
            out=book,
        )
    except BaseException:
        book.close()
        raise
    return StreamingResponse(
        _repacking(chunks, src, track),
        media_type=azw3.MIME,
        headers={'content-disposition': _attachment(name)},
    )


@app.api_route('/{profile}/dl-status/{job}', methods=['GET', 'HEAD'])
async def download_status(profile: str, job: str):
    _profile_or_404(profile)
    state = jobs.get(job)
    if state is None:
        raise UpstreamError(404, 'no such download')
    return JSONResponse(state)


@app.api_route('/{profile}/dl/{token}', methods=['GET', 'HEAD'])
async def download(profile: str, token: str, request: Request, job: str = ''):
    p = _profile_or_404(profile)
    url, cover_url = rewrite.decode_parts(token)
    if request.method == 'HEAD':
        resp = await client.stream(url, request_headers(request.headers))
        ctype = resp.headers.get('content-type', 'application/octet-stream')
        out_headers = {k: v for k, v in resp.headers.items() if k.lower() in FORWARD_RESPONSE}
        if p.fmt != 'raw' and cbz.probably_zip(url, ctype):
            out_headers = _stale(out_headers)
            ctype = 'application/vnd.comicbook+zip'
        head = Response(media_type=ctype, headers=out_headers)
        del head.headers['content-length']
        return head

    track = board.begin(profile, _download_name(url, {}), functools.partial(jobs.set, job))
    track.update(stage='fetching', done=0, total=0)
    try:
        return await _deliver(p, url, cover_url, request, track)
    except BaseException as exc:
        track.finish('stopped' if isinstance(exc, asyncio.CancelledError) else 'error')
        raise


async def _deliver(p: Profile, url: str, cover_url: str | None, request: Request, track) -> Response:
    if p.fmt != 'raw':
        ranged = await _repack_over_ranges(url, cover_url, p, request, track)
        if ranged is not None:
            return ranged

    body = _spool()
    try:
        resp = await client.stream(url, request_headers(request.headers), body)
    except BaseException:
        body.close()
        raise

    ctype = resp.headers.get('content-type', 'application/octet-stream')
    out_headers = {k: v for k, v in resp.headers.items() if k.lower() in FORWARD_RESPONSE}

    if p.fmt != 'raw' and _is_comic_zip(ctype, body):
        try:
            body.seek(0)
            return await _repack(body, url, cover_url, p, request, track, out_headers)
        except Exception:
            body.seek(0)
        except BaseException:
            body.close()
            raise

    if _is_feed_body(body):
        body.close()
        raise UpstreamError(502, 'upstream answered a feed, not a book')
    body.seek(0, os.SEEK_END)
    out_headers['content-length'] = str(body.tell())
    body.seek(0)
    track.update(name=_download_name(url, out_headers), sent=int(out_headers['content-length']))
    track.finish('done')
    return StreamingResponse(_drain(body), media_type=ctype, headers=out_headers)


@app.api_route('/{profile}/azw3/{token}.azw3', methods=['GET', 'HEAD'])
@app.api_route('/{profile}/azw3/{job}/{token}.azw3', methods=['GET', 'HEAD'])
async def kindle_download(profile: str, token: str, request: Request, job: str = ''):
    p = azw3.profile_for(_profile_or_404(profile))
    if p is None:
        raise UpstreamError(404, 'this profile makes no AZW3 book: the Kindle profiles do, bar the webtoon ones')
    url, cover_url = rewrite.decode_parts(token)
    if request.method == 'HEAD':
        resp = await client.stream(url, request_headers(request.headers))
        _, name = _kindle_name(url, resp.headers)
        head = Response(media_type=azw3.MIME, headers={'content-disposition': _attachment(name)})
        del head.headers['content-length']
        return head

    track = board.begin(profile, _download_name(url, {}), functools.partial(jobs.set, job))
    track.update(stage='fetching', done=0, total=0)
    try:
        return await _deliver_kindle(p, url, cover_url, request, track)
    except BaseException as exc:
        track.finish('stopped' if isinstance(exc, asyncio.CancelledError) else 'error')
        raise


async def _deliver_kindle(p: Profile, url: str, cover_url: str | None, request: Request, track) -> Response:
    ranged = await _repack_over_ranges(url, cover_url, p, request, track, _kindle_book)
    if ranged is not None:
        return ranged
    body = _spool()
    try:
        resp = await client.stream(url, request_headers(request.headers), body)
    except BaseException:
        body.close()
        raise
    if not _is_comic_zip(resp.headers.get('content-type', ''), body):
        body.close()
        raise UpstreamError(502, 'an AZW3 book is made from a comic archive, and upstream sent something else')
    body.seek(0)
    upstream = {k: v for k, v in resp.headers.items() if k.lower() in FORWARD_RESPONSE}
    try:
        return await _kindle_book(body, url, cover_url, p, request, track, upstream)
    except Exception as exc:
        body.close()
        raise UpstreamError(502, 'upstream sent a comic archive that cannot be read') from exc
    except BaseException:
        body.close()
        raise


# --------------------------------------------------------------------------
# KOReader progress sync
# --------------------------------------------------------------------------


def _page_number(value) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdecimal():
        return int(value)
    return None


def _relay(resp) -> Response:
    if resp.status_code >= 500:
        raise UpstreamError(502, f'Kavita is failing: status {resp.status_code}')
    return Response(resp.content, status_code=resp.status_code, media_type=resp.headers.get('content-type'))


def _json_of(resp) -> dict | None:
    try:
        answer = resp.json()
    except ValueError:
        return None
    return answer if isinstance(answer, dict) else None


def _document(value) -> str:
    if not kosync.is_digest(value):
        raise UpstreamError(400, 'progress sync expects a document hash')
    return value


@app.get('/kosync/users/auth')
async def kosync_auth():
    return _relay(await kavita.sync('GET', 'users/auth'))


@app.put('/kosync/syncs/progress')
async def kosync_push(request: Request):
    try:
        body = await request.json()
    except ValueError:
        raise UpstreamError(400, 'progress sync expects a JSON body') from None
    document = _document(body.get('document') if isinstance(body, dict) else None)
    record = await asyncio.to_thread(books.get, document)
    page = _page_number(body.get('progress'))
    if record is None or page is None:
        return _relay(await kavita.sync('PUT', 'syncs/progress', body))
    source = kosync.to_source(record['pages'], page)
    resp = await kavita.sync('PUT', 'syncs/progress', {**body, 'document': record['source'], 'progress': str(source)})
    answer = _json_of(resp)
    if resp.status_code != 200 or answer is None:
        return _relay(resp)
    await asyncio.to_thread(books.remember, document, page, source)
    return JSONResponse({**answer, 'document': document})


@app.get('/kosync/syncs/progress/{document}')
async def kosync_pull(document: str):
    record = await asyncio.to_thread(books.get, _document(document))
    resp = await kavita.sync('GET', f'syncs/progress/{record["source"] if record else document}')
    answer = _json_of(resp)
    source = _page_number(answer.get('progress')) if answer else None
    if record is None or resp.status_code != 200 or source is None:
        return _relay(resp)
    page = kosync.page_for(record, source)
    return JSONResponse(
        {**answer, 'document': document, 'progress': str(page), 'percentage': page / len(record['pages'])}
    )
