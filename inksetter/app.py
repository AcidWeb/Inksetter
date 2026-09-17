"""
HTTP surface.
"""

import os
import asyncio
import tempfile
import threading
import contextlib
from html import escape
from urllib.parse import quote
from fastapi import FastAPI, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)

from . import kavita, logs
from .cache import DiskCache, Prefetcher, render_key
from .imaging import cbz, profiles
from .imaging.pipeline import UnreadableImage, render_page
from .imaging.profiles import Profile, cover_for
from .opds import rewrite
from .opds.upstream import FORWARD_RESPONSE, UpstreamError, client, open_range, request_headers
from .settings import settings

logs.configure()

cache = DiskCache(settings.cache_dir, settings.cache_max_bytes)
prefetcher = Prefetcher(settings.prefetch)
render_sem = asyncio.Semaphore(settings.render_workers)
repack_slots = threading.BoundedSemaphore(settings.repack_workers)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001 - signature fixed by FastAPI
    await client.start()
    try:
        yield
    finally:
        await prefetcher.drain()
        await client.stop()


app = FastAPI(title='Inksetter', lifespan=lifespan, docs_url=None, redoc_url=None)


def _profile_or_404(name: str) -> Profile:
    p = profiles.get(name)
    if p is None:
        raise UpstreamError(404, f'unknown profile: {name}')
    return p


def _public_base(request: Request) -> str:
    if settings.public_base:
        return settings.public_base
    return str(request.base_url).rstrip('/')


def _ctx(request: Request, profile: str, upstream_url: str) -> rewrite.Ctx:
    return rewrite.Ctx(profile=profile, public_base=_public_base(request), base_url=upstream_url)


@app.exception_handler(UpstreamError)
async def _upstream_error(request: Request, exc: UpstreamError):  # noqa: ARG001 - handler signature
    return JSONResponse({'error': str(exc)}, status_code=exc.status)


@app.exception_handler(rewrite.TokenError)
async def _token_error(request: Request, exc: rewrite.TokenError):  # noqa: ARG001 - handler signature
    return JSONResponse({'error': str(exc)}, status_code=400)


@app.api_route('/healthz', methods=['GET', 'HEAD'])
async def healthz():
    return {'ok': True, 'profiles': sorted(profiles.PROFILES)}


@app.api_route('/', methods=['GET', 'HEAD'], response_class=HTMLResponse)
async def index(request: Request):
    base = _public_base(request)
    rows = ''.join(
        f'<tr><td><code>{escape(n)}</code></td>'
        f'<td>{p.width}x{p.height}</td><td>{escape(p.fmt)}</td>'
        f'<td><code>{escape(base)}/{escape(quote(n))}/catalog</code></td></tr>'
        for n, p in sorted(profiles.PROFILES.items())
    )
    return (
        '<h1>Inksetter</h1><p>Add one of these as an OPDS catalog in '
        'the client, using your normal server credentials.</p>'
        '<table border=1 cellpadding=4><tr><th>profile</th><th>panel</th>'
        f'<th>format</th><th>catalog URL</th></tr>{rows}</table>'
    )


# --------------------------------------------------------------------------
# Feeds
# --------------------------------------------------------------------------


async def _serve_feed(request: Request, profile_name: str, url: str) -> Response:
    p = _profile_or_404(profile_name)
    resp = await client.get(url, request_headers(request.headers, forward_accept=True))
    ctype = resp.headers.get('content-type', '')
    page_mime = None if p.fmt == 'raw' else p.mime
    if rewrite.is_feed(ctype):
        body = rewrite.rewrite(resp.content, ctype, _ctx(request, profile_name, url), page_mime)
    else:
        body = resp.content
    return Response(content=body, media_type=ctype or 'application/atom+xml')


@app.api_route('/{profile}/catalog', methods=['GET', 'HEAD'])
async def catalog(profile: str, request: Request):
    if not settings.upstream_catalog:
        raise UpstreamError(500, 'UPSTREAM_CATALOG is not configured')
    return await _serve_feed(request, profile, settings.upstream_catalog)


@app.api_route('/{profile}/f/{token}', methods=['GET', 'HEAD'])
async def feed(profile: str, token: str, request: Request):
    url = rewrite.decode_token(token)
    if request.url.query:
        joiner = '&' if '?' in url else '?'
        url = f'{url}{joiner}{request.url.query}'
    return await _serve_feed(request, profile, url)


@app.api_route('/{profile}/osd/{token}', methods=['GET', 'HEAD'])
async def opensearch(profile: str, token: str, request: Request):
    url = rewrite.decode_token(token)
    resp = await client.get(url, request_headers(request.headers, forward_accept=True))
    body = rewrite.rewrite_opensearch(resp.content, _ctx(request, profile, url))
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
# Images
# --------------------------------------------------------------------------


async def _render_cached(url: str, p: Profile, max_width: int | None, headers: dict[str, str]) -> tuple[bytes, str]:
    key = render_key(url, p, max_width)
    hit = await asyncio.to_thread(cache.get, key)
    if hit is not None:
        return hit

    async with cache.lock(key):
        hit = await asyncio.to_thread(cache.get, key)
        if hit is not None:
            return hit
        resp = await client.get(url, headers)
        if p.fmt == 'raw':
            blob = resp.content
            ctype = resp.headers.get('content-type', 'application/octet-stream')
        else:
            async with render_sem:
                try:
                    blob, ctype = await asyncio.to_thread(render_page, resp.content, p, max_width)
                except UnreadableImage as exc:
                    raise UpstreamError(502, f'upstream page: {exc}') from exc
        await asyncio.to_thread(cache.put, key, blob, ctype)

    await cache.maybe_trim()
    return blob, ctype


def _query_int(raw: str, default: int = 0) -> int:
    try:
        return max(0, int(raw))
    except TypeError, ValueError:
        return default


@app.api_route('/{profile}/p/{token}', methods=['GET', 'HEAD'])
async def page(profile: str, token: str, request: Request, page: str = '0', maxWidth: str = '0'):
    p = _profile_or_404(profile)
    template = rewrite.decode_token(token)
    headers = request_headers(request.headers)
    page_no = _query_int(page)
    max_width = _query_int(maxWidth)

    def build(n: int) -> str:
        return template.replace('{pageNumber}', str(n)).replace('{maxWidth}', str(max(p.width, max_width) or ''))

    blob, ctype = await _render_cached(build(page_no), p, max_width or None, headers)

    for n in range(page_no + 1, page_no + 1 + settings.prefetch):
        url = build(n)
        prefetcher.spawn(
            _prefetch(url, p, max_width or None, headers),
            render_key(url, p, max_width or None),
        )

    return Response(
        blob,
        media_type=ctype,
        headers={'Cache-Control': 'private, max-age=86400'},
    )


async def _prefetch(url: str, p: Profile, max_width: int | None, headers: dict[str, str]) -> None:
    key = render_key(url, p, max_width)
    try:
        await _render_cached(url, p, max_width, headers)
    except Exception:
        prefetcher.forget(key)


@app.api_route('/{profile}/pf/{token}', methods=['GET', 'HEAD'])
async def page_fixed(profile: str, token: str, request: Request, maxWidth: str = '0'):
    p = _profile_or_404(profile)
    url = rewrite.decode_token(token)
    blob, ctype = await _render_cached(url, p, _query_int(maxWidth) or None, request_headers(request.headers))
    return Response(blob, media_type=ctype, headers={'Cache-Control': 'private, max-age=86400'})


@app.api_route('/{profile}/img/{token}', methods=['GET', 'HEAD'])
async def image(profile: str, token: str, request: Request):
    p = _profile_or_404(profile)
    url = rewrite.decode_token(token)
    blob, ctype = await _render_cached(url, cover_for(p), None, request_headers(request.headers))
    return Response(blob, media_type=ctype, headers={'Cache-Control': 'private, max-age=604800'})


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


def _next_block(chunks):
    with repack_slots:
        return next(chunks, None)


async def _repacking(chunks, body):
    try:
        while True:
            block = await asyncio.to_thread(_next_block, chunks)
            if block is None:
                return
            yield block
    finally:
        with contextlib.suppress(Exception):
            chunks.close()
        with contextlib.suppress(Exception):
            body.close()


def _spool():
    return tempfile.SpooledTemporaryFile(max_size=settings.spool_max_bytes, dir=settings.spool_dir)


_ZIP_MAGIC = (b'PK\x03\x04', b'PK\x05\x06', b'PK\x07\x08')
_ZIP_TYPES = frozenset(
    {
        'application/vnd.comicbook+zip',
        'application/x-cbz',
        'application/zip',
        'application/x-zip-compressed',
    }
)


def _is_zip(url: str, ctype: str, body) -> bool:
    pos = body.tell()
    try:
        body.seek(0)
        head = body.read(4)
    finally:
        body.seek(pos)
    if head in _ZIP_MAGIC:
        return True
    return not head and (url.lower().endswith(('.cbz', '.zip')) or 'zip' in ctype)


def _probably_zip(url: str, ctype: str) -> bool:
    return url.lower().endswith(('.cbz', '.zip')) or ctype.split(';')[0].strip().lower() in _ZIP_TYPES


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


async def _repack_over_ranges(url: str, cover_url: str | None, p: Profile, request: Request):
    headers = request_headers(request.headers)
    reader = await asyncio.to_thread(open_range, url, headers)
    if reader is None:
        return None
    try:
        ctype = reader.headers.get('content-type', 'application/octet-stream')
        if not await asyncio.to_thread(_is_zip, url, ctype, reader):
            await asyncio.to_thread(reader.close)
            return None
        cover = await _pick_cover(url, cover_url, request)
        meta = await kavita.for_download(url, p.width, round(p.width * p.aspect))
        chunks = await asyncio.to_thread(
            cbz.repack_iter,
            reader,
            p,
            settings.repack_page_workers,
            cover=cover,
            comicinfo=meta,
        )
    except Exception:
        await asyncio.to_thread(reader.close)
        return None
    except BaseException:
        await asyncio.to_thread(reader.close)
        raise
    return StreamingResponse(
        _repacking(chunks, reader),
        media_type='application/vnd.comicbook+zip',
        headers=_stale(reader.headers),
    )


@app.api_route('/{profile}/dl/{token}', methods=['GET', 'HEAD'])
async def download(profile: str, token: str, request: Request):
    p = _profile_or_404(profile)
    url, cover_url = rewrite.decode_parts(token)

    if request.method == 'HEAD':
        resp = await client.probe(url, request_headers(request.headers))
        ctype = resp.headers.get('content-type', 'application/octet-stream')
        out_headers = {k: v for k, v in resp.headers.items() if k.lower() in FORWARD_RESPONSE}
        if p.fmt != 'raw' and _probably_zip(url, ctype):
            for stale in ('etag', 'last-modified', 'content-type'):
                out_headers.pop(stale, None)
            ctype = 'application/vnd.comicbook+zip'
        head = Response(media_type=ctype, headers=out_headers)
        del head.headers['content-length']
        return head

    if p.fmt != 'raw':
        ranged = await _repack_over_ranges(url, cover_url, p, request)
        if ranged is not None:
            return ranged

    body = _spool()
    try:
        resp = await client.download(url, request_headers(request.headers), body)
    except BaseException:
        body.close()
        raise

    ctype = resp.headers.get('content-type', 'application/octet-stream')
    out_headers = {k: v for k, v in resp.headers.items() if k.lower() in FORWARD_RESPONSE}

    if p.fmt != 'raw' and _is_zip(url, ctype, body):
        cover = await _pick_cover(url, cover_url, request)
        meta = await kavita.for_download(url, p.width, round(p.width * p.aspect))
        try:
            body.seek(0)
            chunks = await asyncio.to_thread(
                cbz.repack_iter,
                body,
                p,
                settings.repack_page_workers,
                cover=cover,
                comicinfo=meta,
            )
        except Exception:
            body.seek(0)
        except BaseException:
            body.close()
            raise
        else:
            out_headers = _stale(out_headers)
            return StreamingResponse(
                _repacking(chunks, body),
                media_type='application/vnd.comicbook+zip',
                headers=out_headers,
            )

    body.seek(0, os.SEEK_END)
    out_headers['content-length'] = str(body.tell())
    body.seek(0)
    return StreamingResponse(_drain(body), media_type=ctype, headers=out_headers)
