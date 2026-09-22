"""

python -m tests.smoke

These tests were created entirely by Claude.
The way is shut. It was made by those who are dead, and the dead keep it, until the time comes. The way is shut.

"""

import ast
import base64
import asyncio
import gzip
import inspect
import io
import json
import math
import os
import pathlib
import re
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import zipfile
from unittest import mock

TMP = tempfile.mkdtemp(prefix='inksetter-smoke-')
os.environ.setdefault('UPSTREAM_CATALOG', 'http://127.0.0.1:8899/opds/v1.2/catalog')
os.environ.setdefault('PUBLIC_BASE', 'http://proxy.test')
os.environ.setdefault('CACHE_DIR', TMP)
os.environ.setdefault('PREFETCH', '2')
os.environ.setdefault('LOG_LEVEL', 'WARNING')

import dataclasses  # noqa: E402
import socket  # noqa: E402
import importlib  # noqa: E402
import importlib.resources  # noqa: E402

import httpx  # noqa: E402
import numpy as np  # noqa: E402
import pyvips  # noqa: E402
import uvicorn  # noqa: E402
from fastapi import FastAPI, Header, Request, Response  # noqa: E402
from fastapi.responses import StreamingResponse  # noqa: E402

from inksetter import cache as cache_mod  # noqa: E402
from inksetter import settings as settings_mod  # noqa: E402
from inksetter import app as app_mod  # noqa: E402
from inksetter.app import app  # noqa: E402
from inksetter.imaging import cbz, folio, pipeline, profiles  # noqa: E402
from inksetter import web  # noqa: E402
from inksetter.opds import rewrite  # noqa: E402
from inksetter.opds.rewrite import encode_token  # noqa: E402
from inksetter.opds.upstream import client as up_client  # noqa: E402

PSE_REL = 'http://vaemendis.net/opds-pse/stream'

FEED = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:pse="http://vaemendis.net/opds-pse/ns">
 <title>Fake</title>
 <link rel="self" type="application/atom+xml;profile=opds-catalog"
       href="/opds/v1.2/catalog"/>
 <link rel="search" type="application/opensearchdescription+xml" href="/opds/v1.2/search"/>
 <entry><title>Vol 1</title>
  <link rel="http://opds-spec.org/image" type="image/jpeg"
        href="/opds/v1.2/books/7/thumbnail"/>
  <link rel="http://opds-spec.org/acquisition" type="application/vnd.comicbook+zip"
        href="/opds/v1.2/books/7/file" length="41943040"/>
  <link rel="__PSE_REL__" type="image/jpeg"
        href="/opds/v1.2/books/7/pages/{pageNumber}?zero_based=true&amp;maxWidth={maxWidth}"
        pse:count="3" pse:lastRead="2"/>
 </entry>
</feed>""".replace('__PSE_REL__', PSE_REL)


NAV_FEED = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
 <title>Shelves</title>
 <link rel="self" type="application/atom+xml;profile=opds-catalog" href="/opds/v1.2/nav"/>
 <link rel="up" type="application/atom+xml;profile=opds-catalog" href="/opds/v1.2/catalog"/>
 <link rel="next" type="application/atom+xml;profile=opds-catalog" href="/opds/v1.2/nav?p=2"/>
 <entry><title>Shelf &lt;script&gt;alert(1)&lt;/script&gt;</title>
  <summary>twelve of them</summary>
  <link rel="subsection" type="application/atom+xml;profile=opds-catalog"
        href="/opds/v1.2/mid"/>
  <link rel="previous" type="application/atom+xml;profile=opds-catalog"
        href="/opds/v1.2/nav?entrys-own"/>
  <link rel="http://opds-spec.org/image" type="image/jpeg"
        href="/opds/v1.2/books/7/thumbnail"/>
 </entry>
</feed>"""


def fake_colour_page(w: int = 1600, h: int = 2400) -> bytes:
    rgb = np.full((h, w, 3), 255, np.uint8)
    _yy, xx = np.mgrid[0:h, 0:w]
    rgb[200 : h // 2, 150 : w // 2] = (200, 40, 40)
    rgb[200 : h // 2, w // 2 : w - 150] = (40, 90, 200)
    rgb[h // 2 + 100 : h - 300, 150 : w - 150] = (240, 200, 60)
    fine = np.zeros((h, w), bool)
    fine[h // 2 + 200 : h - 400, 250 : w - 250] = ((xx // 2) % 2).astype(bool)[h // 2 + 200 : h - 400, 250 : w - 250]
    rgb[fine] = (20, 160, 80)
    rgb[h - 250 : h - 200, 200 : w - 200] = 0
    return pyvips.Image.new_from_memory(rgb.tobytes(), w, h, 3, 'uchar').jpegsave_buffer(Q=95)


def fake_page(n: int, w: int = 1600, h: int = 2400) -> bytes:
    a = np.full((h, w), 255, np.uint8)
    yy, xx = np.mgrid[0:h, 0:w]
    tone = (((xx // 3 + yy // 3) % 2) * 255).astype(np.uint8)
    a[200 : h - 200, 150 : w - 150] = tone[200 : h - 200, 150 : w - 150]
    a[300 * (n + 1) : 300 * (n + 1) + 80, 200 : w - 200] = 0
    return pyvips.Image.new_from_memory(a.tobytes(), w, h, 1, 'uchar').jpegsave_buffer(Q=92)


def _cbz_bytes(pages: int = 3) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as z:
        z.writestr('ComicInfo.xml', '<ComicInfo/>')
        for i in range(pages):
            z.writestr(f'p{i:03d}.jpg', fake_page(i))
    return buf.getvalue()


upstream = FastAPI()


@upstream.get('/opds/v1.2/catalog')
def _catalog():
    return Response(FEED, media_type='application/atom+xml;profile=opds-catalog')


MID_FEED = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
 <title>Manga</title>
 <link rel="self" type="application/atom+xml;profile=opds-catalog" href="/opds/v1.2/mid"/>
 <entry><title>A Series</title>
  <link rel="subsection" type="application/atom+xml;profile=opds-catalog"
        href="/opds/v1.2/catalog"/>
 </entry>
</feed>"""


@upstream.get('/opds/v1.2/mid')
def _mid():
    return Response(MID_FEED, media_type='application/atom+xml;profile=opds-catalog')


@upstream.get('/opds/v1.2/guarded')
def _guarded(authorization: str = Header(default='')):
    if authorization != 'Basic Zm9vOmJhcg==':
        return Response(
            'denied',
            status_code=401,
            headers={'www-authenticate': 'Basic realm="Komga", charset="UTF-8"'},
        )
    return Response(FEED, media_type='application/atom+xml;profile=opds-catalog')


@upstream.get('/opds/v1.2/silent401')
def _silent_401():
    return Response('denied', status_code=401)


@upstream.get('/opds/v1.2/forbidden')
def _forbidden():
    return Response('no', status_code=403, headers={'www-authenticate': 'Basic realm="nope"'})


@upstream.get('/opds/v1.2/nav')
def _nav():
    return Response(NAV_FEED, media_type='application/atom+xml;profile=opds-catalog')


@upstream.get('/opds/v1.2/negotiate')
def _negotiate(accept: str = Header(default='')):
    if 'text/html' in accept:
        return Response('<html><body>upstream html</body></html>', media_type='text/html')
    return Response(FEED, media_type='application/atom+xml;profile=opds-catalog')


@upstream.get('/opds/v1.2/search')
def _osd():
    return Response(
        '<?xml version="1.0"?>'
        '<OpenSearchDescription xmlns="http://a9.com/-/spec/opensearch/1.1/">'
        '<Url type="application/atom+xml"'
        ' template="http://127.0.0.1:8899/opds/v1.2/series?search={searchTerms}"/>'
        '</OpenSearchDescription>',
        media_type='application/opensearchdescription+xml',
    )


@upstream.get('/opds/v1.2/series')
def _series(search: str = ''):
    return Response(FEED.replace('<title>Fake', f'<title>hits:{search}'), media_type='application/atom+xml')


@upstream.get('/opds/v1.2/books/7/pages/{n}')
def _page(n: int, zero_based: bool = False, maxWidth: int = 0):  # noqa: ARG001
    if n >= 100:
        return Response(fake_colour_page(2400, 3200), media_type='image/jpeg')
    if n >= 50:
        return Response(fake_page(n, 2400, 3200), media_type='image/jpeg')
    return Response(fake_page(n), media_type='image/jpeg')


@upstream.get('/opds/v1.2/books/7/thumbnail')
def _thumb():
    return Response(fake_page(0), media_type='image/jpeg')


FEED_V2 = {
    'metadata': {'title': 'Fake v2'},
    'links': [
        {'rel': 'self', 'href': '/opds/v2/catalog', 'type': 'application/opds+json'},
        {'rel': 'search', 'href': '/opds/v2/search{?query}', 'type': 'application/opds+json', 'templated': True},
    ],
    'publications': [
        {
            'metadata': {'title': 'Vol 1'},
            'images': [{'href': '/opds/v1.2/books/7/thumbnail', 'type': 'image/jpeg'}],
            'links': [
                {
                    'rel': 'http://opds-spec.org/acquisition',
                    'href': '/opds/v1.2/books/7/file',
                    'type': 'application/vnd.comicbook+zip',
                },
                {'rel': 'alternate', 'href': '/opds/v2/books/7/manifest', 'type': 'application/divina+json'},
            ],
        }
    ],
}


@upstream.get('/opds/v2/catalog')
def _catalog_v2():
    return Response(json.dumps(FEED_V2), media_type='application/opds+json')


@upstream.get('/opds/v2/search')
def _search_v2(query: str = ''):
    doc = dict(FEED_V2)
    doc['metadata'] = {'title': f'v2hits:{query}'}
    return Response(json.dumps(doc), media_type='application/opds+json')


@upstream.get('/opds/nego')
def _negotiated(accept: str = Header(default='')):
    if 'opds+json' in accept:
        return Response(json.dumps(FEED_V2), media_type='application/opds+json')
    return Response(FEED, media_type='application/atom+xml;profile=opds-catalog')


@upstream.get('/opds/v2/books/7/manifest')
def _manifest():
    return Response(
        json.dumps(
            {
                'metadata': {'title': 'Vol 1', 'numberOfPages': 3},
                'links': [{'rel': 'self', 'href': '/opds/v2/books/7/manifest', 'type': 'application/divina+json'}],
                'images': [{'href': '/opds/v1.2/books/7/thumbnail', 'type': 'image/jpeg'}],
                'readingOrder': [
                    {'href': f'/opds/v1.2/books/7/pages/{n}', 'type': 'image/jpeg', 'width': 2400, 'height': 3200}
                    for n in (50, 51, 52)
                ],
            }
        ),
        media_type='application/divina+json',
    )


@upstream.get('/opds/v2/books/8/manifest')
def _manifest_epub():
    return Response(
        json.dumps(
            {
                'metadata': {'title': 'A novel'},
                'readingOrder': [
                    {'href': '/opds/v2/books/8/ch1.xhtml', 'type': 'application/xhtml+xml'},
                ],
                'resources': [
                    {'href': '/opds/v2/books/8/style.css', 'type': 'text/css'},
                    {'href': '/opds/v1.2/books/7/thumbnail', 'type': 'image/jpeg'},
                ],
            }
        ),
        media_type='application/webpub+json',
    )


@upstream.get('/opds/v2/books/8/ch1.xhtml')
def _chapter():
    return Response('<html><body>chapter one</body></html>', media_type='application/xhtml+xml')


@upstream.get('/opds/v1.2/redirect')
def _redirect():
    return Response(status_code=302, headers={'location': 'http://169.254.169.254/latest/meta-data'})


@upstream.get('/opds/v1.2/gz/file.cbz')
def _gz_file():
    inner = _cbz_bytes()
    body = gzip.compress(inner)
    return Response(
        body,
        media_type='application/vnd.comicbook+zip',
        headers={'content-encoding': 'gzip', 'content-length': str(len(body))},
    )


_RANGED = {'requests': 0, 'bytes': 0}
_RANGED_BODY = _cbz_bytes(4)


@upstream.get('/opds/v1.2/ranged/file.cbz')
def _ranged_file(request: Request):
    body = _RANGED_BODY
    rng = request.headers.get('range')
    if 'identity' not in (request.headers.get('accept-encoding') or ''):
        packed = gzip.compress(body)
        return Response(
            packed,
            media_type='application/vnd.comicbook+zip',
            headers={'content-encoding': 'gzip'},
        )
    if not rng:
        return Response(body, media_type='application/vnd.comicbook+zip')
    first, _, last = rng.partition('=')[2].partition('-')
    lo = int(first)
    hi = int(last) if last else len(body) - 1
    if lo >= len(body) or lo > hi:
        return Response(status_code=416, headers={'content-range': f'bytes */{len(body)}'})
    hi = min(hi, len(body) - 1)
    piece = body[lo : hi + 1]
    _RANGED['requests'] += 1
    _RANGED['bytes'] += len(piece)
    return Response(
        piece,
        status_code=206,
        media_type='application/vnd.comicbook+zip',
        headers={'content-range': f'bytes {lo}-{hi}/{len(body)}'},
    )


_BIG = {'yielded': 0}
_BIG_CHUNK = 64 * 1024
_BIG_TOTAL = 32 * 1024 * 1024


@upstream.get('/opds/v1.2/norange-big/file.cbz')
def _norange_big():
    def gen():
        blob = b'\0' * _BIG_CHUNK
        sent = 0
        while sent < _BIG_TOTAL:
            _BIG['yielded'] += _BIG_CHUNK
            sent += _BIG_CHUNK
            yield blob

    return StreamingResponse(gen(), media_type='application/vnd.comicbook+zip')


@upstream.get('/opds/v1.2/liar/file.cbz')
def _liar_file():
    return Response(
        _RANGED_BODY,
        media_type='application/vnd.comicbook+zip',
        headers={'content-range': f'bytes 0-0/{len(_RANGED_BODY)}'},
    )


@upstream.get('/opds/v1.2/norange/file.cbz')
def _norange_file():
    return Response(_RANGED_BODY, media_type='application/vnd.comicbook+zip')


@upstream.get('/opds/v1.2/books/7/file')
def _file():
    return Response(
        _cbz_bytes(),
        media_type='application/vnd.comicbook+zip',
        headers={'content-disposition': 'attachment; filename="Fake Vol 1.cbz"'},
    )


CREDS_SEEN: dict[str, str | None] = {}


@upstream.get('/opds/v1.2/sidehop')
def _sidehop():
    return Response(status_code=302, headers={'location': 'http://localhost:8899/opds/v1.2/collect'})


@upstream.get('/opds/v1.2/collect')
def _collect(authorization: str = Header(default=''), cookie: str = Header(default='')):
    CREDS_SEEN['authorization'] = authorization
    CREDS_SEEN['cookie'] = cookie
    return Response(b'collected', media_type='application/octet-stream')


@upstream.get('/opds/v1.2/empty-feed')
def _empty_feed():
    return Response(b'', media_type='application/atom+xml')


@upstream.get('/opds/v1.2/binary-feed')
def _binary_feed():
    return Response(b'\x00\x01\x02not xml', media_type='application/atom+xml')


@upstream.get('/opds/v1.2/tarball.tgz')
def _tarball():
    return Response(gzip.compress(b'definitely not a zip'), media_type='application/gzip')


@upstream.get('/opds/v1.2/corrupt.cbz')
def _corrupt():
    return Response(b'PK\x03\x04truncated-and-broken', media_type='application/vnd.comicbook+zip')


@upstream.get('/opds/v1.2/plain.cbz')
def _plain_cbz():
    return Response(_cbz_bytes(4), media_type='application/vnd.comicbook+zip')


@upstream.get('/opds/v1.2/coverart')
def _cover_art():
    return Response(fake_page(77, 600, 900), media_type='image/jpeg')


@upstream.get('/opds/v1.2/coverispage1')
def _cover_is_page1():
    return Response(fake_page(0), media_type='image/jpeg')


@upstream.get('/opds/v1.2/withicon')
def _feed_with_icon():
    body = (
        '<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">'
        '<title>t</title><id>urn:x</id>'
        '<icon>/api/opds/SECRETKEY123/favicon</icon>'
        '<logo>http://127.0.0.1:8899/api/opds/SECRETKEY123/logo.png</logo>'
        '<link rel="start" href="/api/opds/SECRETKEY123" '
        'type="application/atom+xml;profile=opds-catalog"/>'
        '</feed>'
    )
    return Response(body, media_type='application/atom+xml;profile=opds-catalog')


@upstream.get('/opds/v1.2/notanimage')
def _not_an_image():
    return Response(b'<html>no</html>', media_type='image/jpeg')


@upstream.get('/opds/v1.2/emptyimage')
def _empty_image():
    return Response(b'', media_type='image/jpeg')


@upstream.get('/opds/v1.2/upstream500')
def _upstream_500():
    return Response(status_code=503)


def start_upstream() -> uvicorn.Server:
    config = uvicorn.Config(upstream, host='127.0.0.1', port=8899, log_level='warning')
    server = uvicorn.Server(config)
    threading.Thread(target=server.run, daemon=True).start()
    for _ in range(80):
        if server.started:
            return server
        time.sleep(0.1)
    raise RuntimeError('fake upstream did not start')


def grey_levels(blob: bytes) -> tuple[int, int, int]:
    im = pyvips.Image.new_from_buffer(blob, '')
    arr = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width))
    return im.width, im.height, len(np.unique(arr))


def describe(blob: bytes) -> tuple[int, int, int, float]:
    im = pyvips.Image.new_from_buffer(blob, '')
    chroma = float(im.colourspace('lch')[1].avg()) if im.bands >= 3 else 0.0
    return im.width, im.height, im.bands, chroma


def screentone_page(w: int = 2400, h: int = 3200, cell: int = 2) -> bytes:
    a = np.full((h, w), 255, np.uint8)
    yy, xx = np.mgrid[0:h, 0:w]
    tone = (((xx // cell + yy // cell) % 2) * 255).astype(np.uint8)
    a[200 : h - 600, 150 : w - 150] = tone[200 : h - 600, 150 : w - 150]
    a[h - 500 : h - 400, 150 : w - 150] = 128
    a[h - 300 : h - 240, 200 : w - 200] = 0
    return pyvips.Image.new_from_memory(a.tobytes(), w, h, 1, 'uchar').jpegsave_buffer(Q=94)


def smooth_page(w: int = 1500, h: int = 2000) -> bytes:
    a = np.full((h, w), 255, np.uint8)
    a[300 : h - 300, 200 : w - 200] = 90
    a[800:860, 250 : w - 250] = 0
    return pyvips.Image.new_from_memory(a.tobytes(), w, h, 1, 'uchar').jpegsave_buffer(Q=94)


def low_freq_modulation(blob: bytes, y0f: float, y1f: float) -> float:
    im = pyvips.Image.new_from_buffer(blob, '')
    a = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width)).astype(np.float32)
    band = a[
        int(im.height * y0f) : int(im.height * y1f),
        int(im.width * 0.18) : int(im.width * 0.82),
    ]
    bh, bw = band.shape[0] // 16 * 16, band.shape[1] // 16 * 16
    blocks = band[:bh, :bw].reshape(bh // 16, 16, bw // 16, 16).mean(axis=(1, 3))
    return float(blocks.std())


def check_descreen() -> None:
    clara = profiles.PROFILES['kobo-clara-hd-2e-bw']
    on = dataclasses.replace(clara, autocrop=False, descreen='always')
    off = dataclasses.replace(clara, autocrop=False, descreen='none')

    toned = screentone_page()
    blob_on, _ = pipeline.render_page(toned, on)
    blob_off, _ = pipeline.render_page(toned, off)
    m_on = low_freq_modulation(blob_on, 0.15, 0.60)
    m_off = low_freq_modulation(blob_off, 0.15, 0.60)
    check(
        'FFT descreen reduces moire on aliased screentone',
        m_on < m_off * 0.85,
        f'none={m_off:.3f} fft={m_on:.3f}',
    )

    shallow = screentone_page(w=1170, h=1536, cell=2)
    check(
        'descreen is inert when nothing can alias',
        pipeline.render_page(shallow, on)[0] == pipeline.render_page(shallow, off)[0],
        'scale 0.92: no aliasing, so no notch',
    )

    smooth = smooth_page()
    check(
        'a page with no screentone is byte-identical',
        pipeline.render_page(smooth, on)[0] == pipeline.render_page(smooth, off)[0],
    )

    capped = dataclasses.replace(on, descreen_max_megapixels=0.5)
    check(
        'descreen is skipped above descreen_max_megapixels',
        pipeline.render_page(toned, capped)[0] == blob_off,
    )

    for label, (w, h) in (('1x1', (1, 1)), ('1xN', (1, 40)), ('tiny', (3, 3))):
        tiny = pyvips.Image.new_from_memory(np.full((h, w), 200, np.uint8).tobytes(), w, h, 1, 'uchar').pngsave_buffer()
        try:
            pipeline.render_page(tiny, on)
            check(f'descreen survives a {label} source', True)
        except Exception as exc:
            check(f'descreen survives a {label} source', False, f'{type(exc).__name__}: {exc}')

    try:
        profiles.validate(dataclasses.replace(clara, descreen=0.30))
        check('descreen=0.30 (the old pre-blur form) is rejected', False, 'accepted')
    except ValueError as exc:
        check(
            'descreen=0.30 (the old pre-blur form) is rejected',
            'used to be a pre-blur' in str(exc),
            str(exc)[:64],
        )


def diagonal_colour_page(w: int = 1600, h: int = 2200, axis: str = 'diagonal') -> bytes:
    rgb = np.full((h, w, 3), 235, np.uint8)
    yy, xx = np.mgrid[0:h, 0:w]
    rgb[150 : h // 3, 150 : w - 150] = (200, 60, 60)
    rgb[h // 3 : 2 * h // 3, 150 : w - 150] = (60, 110, 200)
    grating = ((xx + yy) % 2) if axis == 'diagonal' else (xx % 2)
    mask = np.zeros((h, w), bool)
    band = slice(2 * h // 3 + 40, h - 80)
    mask[band, 150 : w - 150] = grating.astype(bool)[band, 150 : w - 150]
    rgb[mask] = (30, 170, 90)
    return pyvips.Image.new_from_memory(rgb.tobytes(), w, h, 3, 'uchar').jpegsave_buffer(Q=96)


def wedge_energy(blob: bytes, centre: float, tol: float = 12.0, fmin: float = 0.30) -> float:
    im = pyvips.Image.new_from_buffer(blob, '')
    if im.bands > 1:
        im = im.colourspace('b-w')
    a = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width)).astype(np.float32)
    spec = np.abs(np.fft.rfft2(a))
    fy = np.fft.fftfreq(a.shape[0])[:, None]
    fx = np.fft.rfftfreq(a.shape[1])[None, :]
    sel = (fy**2 + fx**2) >= fmin**2
    ang = np.rad2deg(np.arctan2(fy, fx)) % 360.0
    near = np.zeros(spec.shape, bool)
    for c in (centre, (centre + 180.0) % 360.0):
        lo, hi = (c - tol) % 360.0, (c + tol) % 360.0
        near |= (ang >= lo) | (ang <= hi) if lo > hi else (ang >= lo) & (ang <= hi)
    sel &= near
    return float(spec[sel].mean()) if sel.any() else 0.0


def plain_page(w: int, h: int, level: int = 235) -> bytes:
    a = np.full((h, w), level, np.uint8)
    a[h // 6 : h - h // 6, w // 8 : w - w // 8] = 150
    a[h // 3 : h // 3 + max(2, h // 40), w // 6 : w - w // 6] = 20
    return pyvips.Image.new_from_memory(a.tobytes(), w, h, 1, 'uchar').pngsave_buffer()


def check_fit_and_upscale() -> None:
    clara = dataclasses.replace(profiles.PROFILES['kobo-clara-hd-2e-bw'], autocrop=False)
    tw, th = clara.width, clara.height

    for label, (w, h) in (
        ('smaller than panel', (1170, 1536)),
        ('much smaller', (600, 800)),
        ('larger than panel', (2400, 3200)),
        ('very wide aspect', (1600, 1000)),
        ('very tall aspect', (900, 3000)),
    ):
        blob, _ = pipeline.render_page(plain_page(w, h), clara)
        im = pyvips.Image.new_from_buffer(blob, '')
        check(
            f'{label} lands on the panel box',
            (im.width, im.height) == (tw, th),
            f'{w}x{h} -> {im.width}x{im.height}, wanted {tw}x{th}',
        )

    blob, _ = pipeline.render_page(plain_page(1170, 1536), clara, max_width=800)
    im = pyvips.Image.new_from_buffer(blob, '')
    check(
        'a client-narrowed box is still exact',
        (im.width, im.height) == (800, round(800 * clara.aspect)),
        f'{im.width}x{im.height}',
    )

    blob, _ = pipeline.render_page(plain_page(1170, 1536), clara)
    im = pyvips.Image.new_from_buffer(blob, '')
    a = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width))
    check('an enlarged page still quantises to 16 levels', len(np.unique(a)) <= 16)

    capped = dataclasses.replace(clara, upscale_max=1.2)
    blob, _ = pipeline.render_page(plain_page(600, 800), capped)
    im = pyvips.Image.new_from_buffer(blob, '')
    inner = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width))
    check('upscale_max caps the enlargement', (im.width, im.height) == (tw, th))
    pad_cols = (tw - int(600 * 1.2)) // 2
    left = inner[:, : pad_cols - 4]
    check(
        'a capped page is letterboxed, not blown up',
        pad_cols > 100 and float(left.std()) < 30.0,
        f'{pad_cols}px padding per side, std {float(left.std()):.1f}',
    )

    dark = pipeline.render_page(plain_page(900, 3000, level=30), clara)[0]
    dim = pyvips.Image.new_from_buffer(dark, '')
    da = np.ndarray(buffer=dim.write_to_memory(), dtype=np.uint8, shape=(dim.height, dim.width))
    check(
        'padding takes the page margin colour, not white',
        da[:, :8].max() < 120,
        f'left padding max={da[:, :8].max()} on a dark page',
    )

    loose = dataclasses.replace(clara, fit='none', upscale='none')
    blob, _ = pipeline.render_page(plain_page(600, 800), loose)
    im = pyvips.Image.new_from_buffer(blob, '')
    check(
        'fit=none, upscale=none ships the source size',
        (im.width, im.height) == (600, 800),
        f'{im.width}x{im.height}',
    )

    for field, bad in (
        ('fit', 'letterbox'),
        ('upscale', 'always'),
        ('upscale_kernel', 'bilinear'),
        ('upscale_max', 99.0),
    ):
        try:
            profiles.validate(dataclasses.replace(clara, **{field: bad}))
            check(f'{field}={bad!r} rejected', False, 'accepted')
        except ValueError as exc:
            check(f'{field}={bad!r} rejected', field in str(exc), str(exc)[:52])


def check_dither_ties() -> None:
    for index in range(16):
        flat = np.full((256, 256), index * 17, np.uint8)
        out = pipeline._quantise16(flat, 'bayer')
        check(
            f'quantiser leaves rung {index} ({index * 17}) flat',
            len(np.unique(out)) == 1 and int(out[0, 0]) == index,
            f'{len(np.unique(out))} indices: {sorted(int(v) for v in np.unique(out))[:4]}',
        )

    mid = pipeline._quantise16(np.full((400, 400), 127, np.uint8), 'bayer')
    check('a value between rungs still dithers', len(np.unique(mid)) > 1, f'{len(np.unique(mid))} indices')

    clara = dataclasses.replace(profiles.PROFILES['kobo-clara-hd-2e-bw'], autocrop=False)
    for level in (255, 0):
        page = pyvips.Image.new_from_memory(
            np.full((1600, 1103), level, np.uint8).tobytes(), 1103, 1600, 1, 'uchar'
        ).pngsave_buffer()
        blob, _ = pipeline.render_page(page, clara)
        im = pyvips.Image.new_from_buffer(blob, '')
        a = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width))
        check(
            f'a uniform page at {level} renders flat end to end',
            len(np.unique(a)) == 1,
            f'{len(np.unique(a))} levels: {sorted(int(v) for v in np.unique(a))[:4]}',
        )


def check_path_awareness() -> None:
    clara = dataclasses.replace(profiles.PROFILES['kobo-clara-hd-2e-bw'], autocrop=False)
    toned = screentone_page()

    m_on, _ = pipeline.render_page(toned, dataclasses.replace(clara, descreen='mono'))
    m_off, _ = pipeline.render_page(toned, dataclasses.replace(clara, descreen='none'))
    check('descreen=mono runs on the mono path', m_on != m_off)

    colour = dataclasses.replace(profiles.PROFILES['kobo-clara-colour'], autocrop=False, auto_mono=False)
    c_mono, _ = pipeline.render_page(fake_colour_page(2400, 3200), dataclasses.replace(colour, descreen='mono'))
    c_none, _ = pipeline.render_page(fake_colour_page(2400, 3200), dataclasses.replace(colour, descreen='none'))
    check('descreen=mono leaves the colour path alone', c_mono == c_none)

    c_all, _ = pipeline.render_page(fake_colour_page(2400, 3200), dataclasses.replace(colour, descreen='always'))
    check('descreen=always reaches the colour path', c_all != c_none)

    check(
        'panel is declared on every kaleido profile',
        all(q.panel == 'kaleido' for q in profiles.PROFILES.values() if q.is_colour),
    )
    try:
        profiles.validate(dataclasses.replace(clara, panel='eink'))
        check("panel='eink' rejected", False, 'accepted')
    except ValueError as exc:
        check("panel='eink' rejected", 'panel' in str(exc), str(exc)[:48])


def check_mask_cache_and_padding() -> None:
    pipeline._diagonal_attenuation.cache_clear()
    scribe = dataclasses.replace(profiles.PROFILES['kindle-scribe-colorsoft'], autocrop=False, auto_mono=False)
    page = fake_colour_page(2400, 3200)
    for _ in range(3):
        pipeline.render_page(page, scribe)
    info = pipeline._diagonal_attenuation.cache_info()
    check(
        'the defringe mask is built once for a whole volume',
        info.misses == 1 and info.hits >= 2,
        f'{info.misses} miss(es), {info.hits} hit(s)',
    )

    clara = profiles.PROFILES['kobo-clara-hd-2e-bw']
    blob, _ = pipeline.render_page(plain_page(1170, 1300), clara)
    im = pyvips.Image.new_from_buffer(blob, '')
    a = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width))
    top = a[:6, :]
    check(
        'the padding comes out flat, not dithered',
        len(np.unique(top)) == 1,
        f'{len(np.unique(top))} distinct values in the top frame',
    )


def check_unsharp_scaling() -> None:
    scribe = dataclasses.replace(profiles.PROFILES['kindle-scribe-3'], autocrop=False)
    small = fake_page(0, 1170, 1536)
    big = plain_page(2400, 3200)

    def gradient(blob):
        im = pyvips.Image.new_from_buffer(blob, '')
        a = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width)).astype(np.float32)
        return float(np.abs(np.diff(a, axis=1)).mean())

    factor = min(scribe.width / 1170, scribe.height / 1536)

    auto = gradient(pipeline.render_page(small, scribe)[0])
    unscaled = gradient(
        pipeline.render_page(small, dataclasses.replace(scribe, usm_sigma=scribe.usm_sigma / factor))[0]
    )
    check(
        'an enlarged page is sharpened at the enlarged scale',
        auto > unscaled,
        f'sigma x{factor:.2f} gives {auto:.3f} against {unscaled:.3f} un-scaled',
    )

    geom = pipeline._geometry(big, scribe, scribe.width, scribe.height, mono=True)
    check('a downscaled page keeps the tuned sigma', geom.upscale == 1.0, f'upscale={geom.upscale}')


def check_defringe() -> None:
    scribe = dataclasses.replace(
        profiles.PROFILES['kindle-scribe-colorsoft'],
        autocrop=False,
        auto_mono=False,
        descreen='none',
    )
    off = dataclasses.replace(scribe, defringe='none')

    diag = diagonal_colour_page(axis='diagonal')
    on_blob, _ = pipeline.render_page(diag, scribe)
    off_blob, _ = pipeline.render_page(diag, off)

    d_on, d_off = wedge_energy(on_blob, 135.0), wedge_energy(off_blob, 135.0)
    a_on, a_off = wedge_energy(on_blob, 0.0), wedge_energy(off_blob, 0.0)
    check(
        'defringe cuts diagonal high frequencies',
        d_on < d_off * 0.75,
        f'diag {d_off:.0f} -> {d_on:.0f}',
    )
    check(
        'defringe leaves axial frequencies alone',
        abs(a_on - a_off) < a_off * 0.05,
        f'axial {a_off:.0f} -> {a_on:.0f}',
    )
    check(
        'defringe does not balloon the page',
        len(on_blob) < len(off_blob) * 1.35,
        f'{len(off_blob) / 1024:.1f} kB -> {len(on_blob) / 1024:.1f} kB',
    )

    check(
        'mono profiles have defringe off',
        all(q.defringe == 'none' for q in profiles.PROFILES.values() if not q.is_colour),
    )
    check(
        'Kaleido profiles have defringe on',
        all(q.defringe == 'diagonal' for q in profiles.PROFILES.values() if q.is_colour),
    )
    try:
        profiles.validate(dataclasses.replace(profiles.PROFILES['kobo-clara-hd-2e-bw'], defringe='diagonal'))
        check('defringe on a mono panel is rejected', False, 'accepted')
    except ValueError as exc:
        check('defringe on a mono panel is rejected', 'filter array' in str(exc), str(exc)[:56])

    tiny = pyvips.Image.new_from_memory(np.full((4, 4, 3), 180, np.uint8).tobytes(), 4, 4, 3, 'uchar').pngsave_buffer()
    try:
        pipeline.render_page(tiny, scribe)
        check('defringe survives a tiny colour source', True)
    except Exception as exc:
        check('defringe survives a tiny colour source', False, f'{type(exc).__name__}: {exc}')


async def check_upstream_ca() -> None:
    from inksetter.opds import upstream as _up
    from inksetter.settings import settings as _s

    check(
        'the CA bundle setting is gone',
        not hasattr(_s, 'upstream_ca_bundle'),
        'still present' if hasattr(_s, 'upstream_ca_bundle') else '',
    )
    check('and so is the helper that built a context', not hasattr(_up, 'verify_for'))

    tree = ast.parse(inspect.getsource(_up))
    values = [
        kw.value for node in ast.walk(tree) if isinstance(node, ast.Call) for kw in node.keywords if kw.arg == 'verify'
    ]
    passed = [v.value for v in values if isinstance(v, ast.Constant)]
    check(
        'every client passes verify=False',
        len(values) == 2 and passed == [False, False],
        f'{len(values)} verify= argument(s), values {passed}',
    )

    client = _up.Client()
    await client.start()
    try:
        ctx = client.raw._transport._pool._ssl_context
        check(
            'the live client does not verify',
            ctx.verify_mode == ssl.CERT_NONE and ctx.check_hostname is False,
            f'verify_mode={ctx.verify_mode!r} check_hostname={ctx.check_hostname}',
        )
    except AttributeError as exc:
        check('the live client does not verify', False, f'could not reach the context: {exc}')
    finally:
        await client.stop()


def check_logging() -> None:
    import logging as _logging

    from inksetter import logs as _logs

    for name in ('pyvips', 'httpx', 'httpcore', 'uvicorn.access'):
        check(f'{name} is named as noisy', name in _logs.NOISY)

    for raw, want in (('DEBUG', _logging.DEBUG), ('warning', _logging.WARNING), ('', _logging.INFO)):
        with mock.patch.dict(os.environ, {'LOG_LEVEL': raw}):
            got = _logs.level()
        check(f'LOG_LEVEL={raw!r} reads as {_logging.getLevelName(want)}', got == want, _logging.getLevelName(got))
    with mock.patch.dict(os.environ, {'LOG_LEVEL': 'not-a-level'}):
        check('a nonsense LOG_LEVEL falls back to INFO', _logs.level() == _logging.INFO)

    with mock.patch.dict(os.environ, {'LOG_LEVEL': 'DEBUG'}):
        _logs.configure()
    try:
        check(
            'our own logger follows LOG_LEVEL',
            _logging.getLogger('inksetter').level == _logging.DEBUG,
            _logging.getLevelName(_logging.getLogger('inksetter').level),
        )
        for name in _logs.NOISY:
            lg = _logging.getLogger(name)
            check(
                f'{name} stays at WARNING even at DEBUG',
                lg.level == _logging.WARNING and not lg.isEnabledFor(_logging.INFO),
                _logging.getLevelName(lg.level),
            )
        before = len(_logging.getLogger().handlers)
        _logs.configure()
        check('configure() is idempotent', len(_logging.getLogger().handlers) == before, f'{before} handler(s)')
    finally:
        with mock.patch.dict(os.environ, {'LOG_LEVEL': 'WARNING'}):
            _logs.configure()

    page = fake_page(0, 900, 1300)
    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']
    plog = _logging.getLogger('inksetter.imaging.pipeline')
    for level, want_stage in ((_logging.DEBUG, True), (_logging.INFO, False)):
        records: list = []

        class Grab(_logging.Handler):
            def __init__(self, sink):
                super().__init__()
                self.sink = sink

            def emit(self, record):
                self.sink.append(record)

        handler = Grab(records)
        plog.addHandler(handler)
        old_level = plog.level
        plog.setLevel(level)
        try:
            pipeline.render_page(page, prof)
        finally:
            plog.setLevel(old_level)
            plog.removeHandler(handler)
        msgs = [r.getMessage() for r in records]
        stages = [m for m in msgs if m.lstrip().startswith(('decode', 'autocrop', 'downscale', 'upscale', 'pad'))]
        summary = [m for m in msgs if 'page rendered' in m]
        name = _logging.getLevelName(level)
        check(f'at {name} the page reports itself once', len(summary) == 1, f'{len(summary)} summary line(s)')
        if want_stage:
            check(f'at {name} every stage is logged', len(stages) >= 3, f'{len(stages)} stage line(s): {stages[:2]}')
        else:
            check(f'at {name} the stages stay quiet', not stages, f'{len(stages)} leaked')

    big_page = fake_page(0, 2400, 3200)
    records = []
    handler = Grab(records)
    plog.addHandler(handler)
    old_level = plog.level
    plog.setLevel(_logging.DEBUG)
    try:
        pipeline.render_page(big_page, prof)
    finally:
        plog.setLevel(old_level)
        plog.removeHandler(handler)
    decode = [m for m in (r.getMessage() for r in records) if m.lstrip().startswith('decode')]
    logged = int(decode[0].split()[1]) if decode else -1
    _t = time.perf_counter()
    pyvips.Image.new_from_buffer(big_page, '').copy_memory()
    truth = (time.perf_counter() - _t) * 1000
    check(
        'the decode is billed to the decode, not to the next stage',
        logged >= truth * 0.5,
        f'logged {logged} ms against a real decode of {truth:.0f} ms',
    )


def check_host_normalisation() -> None:
    from inksetter.opds import upstream as up

    for label, catalog, target in (
        ('userinfo in the catalog URL', 'http://u:pw@komga:25600/opds', 'http://komga:25600/opds/x'),
        ('mixed case host', 'http://KomGa:25600/opds', 'http://komga:25600/opds/x'),
        ('explicit default port', 'http://komga:80/opds', 'http://komga/opds/x'),
        ('implicit default port', 'http://komga/opds', 'http://komga:80/opds/x'),
    ):
        saved = dataclasses.replace(settings_mod.settings, upstream_catalog=catalog)
        real = settings_mod.settings
        up.settings = saved
        try:
            up.check_host(target)
            check(f'allow-list accepts its own catalog: {label}', True)
        except up.UpstreamError as exc:
            check(f'allow-list accepts its own catalog: {label}', False, f'{exc.status}: {exc}')
        finally:
            up.settings = real

    blank = dataclasses.replace(settings_mod.settings, upstream_catalog='')
    real = up.settings
    up.settings = blank
    try:
        for label, target in (
            ('a plain host', 'http://komga:25600/opds/x'),
            ('cloud metadata', 'http://169.254.169.254/latest/meta-data/'),
        ):
            try:
                up.check_host(target)
                check(f'no catalog allows nothing: {label}', False, 'accepted')
            except up.UpstreamError as exc:
                check(f'no catalog allows nothing: {label}', exc.status == 500, f'{exc.status}: {exc}')
    finally:
        up.settings = real

    one = dataclasses.replace(settings_mod.settings, upstream_catalog='http://komga:25600/opds')
    real = up.settings
    up.settings = one
    try:
        for label, target in (
            ('another name', 'http://cdn.example/thumb.jpg'),
            ('another port on the same name', 'http://komga:8080/opds/x'),
        ):
            try:
                up.check_host(target)
                check(f'only the catalog host is allowed: {label}', False, 'accepted')
            except up.UpstreamError as exc:
                check(f'only the catalog host is allowed: {label}', exc.status == 403, f'{exc.status}: {exc}')
    finally:
        up.settings = real

    spoof = dataclasses.replace(settings_mod.settings, upstream_catalog='http://komga:25600/opds')
    real = up.settings
    up.settings = spoof
    try:
        up.check_host('http://komga:25600@evil.example/meta')
        check('userinfo cannot spoof an allowed host', False, 'accepted evil.example')
    except up.UpstreamError:
        check('userinfo cannot spoof an allowed host', True)
    finally:
        up.settings = real


def check_cache_accounting() -> None:
    root = pathlib.Path(tempfile.mkdtemp())
    c = cache_mod.DiskCache(root, max_bytes=10**9)
    c._size = 0

    real_write_text = pathlib.Path.write_text

    def fail_on_type(self, *a, **k):
        if self.suffix == '.type':
            raise OSError('simulated disk full')
        return real_write_text(self, *a, **k)

    pathlib.Path.write_text = fail_on_type
    try:
        c.put('a' * 64, b'x' * 100, 'image/png')
    finally:
        pathlib.Path.write_text = real_write_text

    check(
        'failed .type write strands nothing on disk',
        not list(root.rglob('*.blob')) and not list(root.rglob('*.type')),
        f'{len(list(root.rglob("*.blob")))} blob(s), {len(list(root.rglob("*.type")))} type(s)',
    )
    check('failed put leaves the size total intact', c._size == 0, f'_size={c._size}')

    c.put('b' * 64, b'y' * 50, 'image/png')
    first = c._size
    c.put('b' * 64, b'z' * 70, 'image/png')
    check(
        'overwrite adjusts the size by the delta',
        c._size == 70,
        f'50 -> {first} -> {c._size} (expected 70, not None)',
    )
    check('overwritten entry is still readable', c.get('b' * 64) == (b'z' * 70, 'image/png'))

    orphan = root / 'cc' / ('c' * 64 + '.type')
    orphan.parent.mkdir(parents=True, exist_ok=True)
    orphan.write_text('image/png')
    os.utime(orphan, (0, 0))
    fresh = root / 'dd' / ('d' * 64 + '.type')
    fresh.parent.mkdir(parents=True, exist_ok=True)
    fresh.write_text('image/png')

    c._trim()
    check('a stranded .type is reclaimed', not orphan.exists())
    check('a .type from a put still in flight is left alone', fresh.exists())
    check(
        'the sweep runs even though the cache is under budget',
        c._size == 70,
        f'_size={c._size}',
    )
    check('an entry that still has its blob is untouched', c.get('b' * 64) == (b'z' * 70, 'image/png'))


def check_module_boundary() -> None:
    cases = {
        'imaging': (
            ['inksetter.imaging.profiles', 'inksetter.imaging.pipeline', 'inksetter.imaging.cbz'],
            [
                'httpx',
                'fastapi',
                'starlette',
                'lxml',
                'inksetter.settings',
                'inksetter.opds',
                'inksetter.app',
                'inksetter.cache',
                'inksetter.web',
            ],
        ),
        'opds': (
            ['inksetter.opds.rewrite', 'inksetter.opds.upstream'],
            ['pyvips', 'numpy', 'inksetter.imaging', 'inksetter.app', 'inksetter.cache', 'inksetter.web'],
        ),
    }
    code = (
        'import sys, json\n'
        'for m in {mods!r}:\n'
        '    __import__(m)\n'
        'print(json.dumps(sorted(set({banned!r}) & set(sys.modules))))\n'
    )
    for half, (mods, banned) in cases.items():
        r = subprocess.run(
            [sys.executable, '-c', code.format(mods=mods, banned=banned)],
            capture_output=True,
            text=True,
            cwd=os.getcwd(),
        )
        if r.returncode != 0:
            check(f'{half} imports in isolation', False, r.stderr.strip()[-90:])
            continue
        leaked = json.loads(r.stdout.strip().splitlines()[-1])
        check(f'{half} pulls in nothing from the other half', not leaked, f'leaked {leaked}' if leaked else '')


def check_repack_parallel() -> None:
    import io as _io
    import zipfile as _zf

    from inksetter.imaging import cbz as _cbz

    src = _io.BytesIO()
    with _zf.ZipFile(src, 'w') as z:
        z.writestr('ComicInfo.xml', '<ComicInfo><Series>t</Series></ComicInfo>')
        z.writestr('002.jpg', fake_page(2, 900, 1300))
        z.writestr('010.jpg', fake_page(10, 1200, 1840))
        z.writestr('bad.jpg', b'not an image at all')
        z.writestr('spread.png', pyvips.Image.black(1600, 900).invert().pngsave_buffer())
        z.writestr('notes.txt', 'kept verbatim')
        z.writestr('001.jpg', fake_page(1, 1100, 1500))
    raw = src.getvalue()

    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']

    def run(workers):
        out = _io.BytesIO()
        _cbz.repack_to(_io.BytesIO(raw), out, prof, workers=workers)
        zin = _zf.ZipFile(_io.BytesIO(out.getvalue()))
        return [(i.filename, zin.read(i.filename)) for i in zin.infolist()]

    ref = run(1)
    for workers in (2, 3):
        got = run(workers)
        same_order = [nm for nm, _ in ref] == [nm for nm, _ in got]
        same_bytes = same_order and all(a == b for (_, a), (_, b) in zip(ref, got, strict=True))
        check(
            f'repack with {workers} workers matches serial',
            same_bytes,
            '' if same_bytes else f'{[nm for nm, _ in ref]} vs {[nm for nm, _ in got]}',
        )
    names = [nm for nm, _ in ref]
    payloads = dict(ref)
    check(
        'corrupt page shipped raw, not dropped',
        b'not an image at all' in payloads.values(),
        f'names={names}',
    )
    check(
        'non-image entries survive verbatim',
        payloads.get('notes.txt') == b'kept verbatim' and 'ComicInfo.xml' in payloads,
        f'names={names}',
    )

    src_pages = sum(_cbz.is_image(n) for n in _zf.ZipFile(_io.BytesIO(raw)).namelist())
    out_pages = sum(1 for nm, _ in ref if nm[0].isdigit())
    check(
        'the repack preserves the page count',
        out_pages == src_pages,
        f'{src_pages} in, {out_pages} out',
    )
    rotated = [pyvips.Image.new_from_buffer(b, '') for nm, b in ref if nm[0].isdigit() and b != b'not an image at all']
    check(
        'the landscape spread is rotated to portrait, not cut',
        all(im.height >= im.width for im in rotated),
        f'{[(im.width, im.height) for im in rotated]}',
    )


async def check_comicinfo() -> None:
    from lxml import etree

    from inksetter import kavita

    data = {
        'series': {
            'name': 'Example Manga',
            'localizedName': 'Example Manga',
            'format': 1,
            'aniListId': 424242,
        },
        'meta': {
            'totalCount': 18,
            'releaseYear': 2010,
            'summary': 'A synopsis with an & ampersand and a <tag>.',
            'language': '',
            'ageRating': 9,
            'writers': [{'name': 'First Writer'}],
            'coverArtists': [{'name': 'First Artist'}],
            'pencillers': [],
            'publishers': [],
            'genres': [{'id': 2, 'title': 'Action'}, {'id': 5, 'title': 'Drama'}],
            'characters': [{'name': 'Hero One'}, {'name': 'Hero Two'}],
            'teams': [],
            'locations': [],
        },
        'numbers': {265: 1},
    }

    xml = kavita.render(data, '1', 188, 1272, 1696)
    doc = etree.fromstring(xml)

    got = {el.tag: (el.text or '') for el in doc if el.tag != 'Pages'}
    for tag, want in (
        ('Series', 'Example Manga'),
        ('Number', '1'),
        ('Count', '18'),
        ('Year', '2010'),
        ('Writer', 'First Writer'),
        ('CoverArtist', 'First Artist'),
        ('Genre', 'Action, Drama'),
        ('Characters', 'Hero One, Hero Two'),
        ('PageCount', '188'),
        ('Manga', 'Yes'),
        ('AgeRating', 'Mature 17+'),
        ('Web', 'https://anilist.co/manga/424242'),
    ):
        check(f'ComicInfo {tag} = {want!r}', got.get(tag) == want, f'got {got.get(tag)!r}')

    check(
        'ampersands and angle brackets are escaped, not raw', '&amp;' in xml.decode() and '&lt;tag&gt;' in xml.decode()
    )
    check(
        'empty fields are omitted, not written blank',
        'LanguageISO' not in got and 'Publisher' not in got and 'Penciller' not in got,
        f'{sorted(got)}',
    )
    check('a localizedName equal to the name is not repeated as AlternateSeries', 'AlternateSeries' not in got)

    order = [el.tag for el in doc if el.tag != 'Pages']
    expected = [x for x in kavita._ORDER if x in order]
    check('elements are in schema order', order == expected, f'{order}')
    check('Pages is last', doc[-1].tag == 'Pages', f'{doc[-1].tag}')

    pages = doc.findall('Pages/Page')
    check('one Page per page, numbered from zero', len(pages) == 188 and pages[0].get('Image') == '0', f'{len(pages)}')
    check('page 0 is marked FrontCover', pages[0].get('Type') == 'FrontCover')
    check('later pages carry no Type', pages[1].get('Type') is None)
    check(
        'Page geometry is the panel geometry',
        pages[5].get('ImageWidth') == '1272' and pages[5].get('ImageHeight') == '1696',
    )

    empty = {
        'series': {'name': 'X', 'format': 2},
        'meta': {'summary': '-100000', 'releaseYear': 0, 'language': None, 'ageRating': 0, 'writers': [], 'genres': []},
        'numbers': {},
    }
    xml2 = kavita.render(empty, None, 2, 100, 200).decode()
    check('the -100000 sentinel never reaches the file', '-100000' not in xml2, xml2[:120])
    check('a non-manga format omits <Manga>', '<Manga>' not in xml2)

    for catalog, want in (
        ('http://kavita:5000/api/opds/ABC123', 'ABC123'),
        ('http://kavita:5000/api/opds/ABC123/', 'ABC123'),
        ('https://k/api/OPDS/MiXeDcAsE/more?x=1', 'MiXeDcAsE'),
        ('http://komga:25600/opds/v1.2/catalog', ''),
        ('http://codex:9810/opds/v1.2', ''),
        ('', ''),
    ):
        patched = dataclasses.replace(settings_mod.settings, upstream_catalog=catalog)
        real = kavita.settings
        kavita.settings = patched
        try:
            got = kavita.api_key()
        finally:
            kavita.settings = real
        check(
            f'api key from {catalog[:38]!r} -> {want!r}',
            got == want,
            f'got {got!r}',
        )

    real = kavita.settings
    kavita.settings = dataclasses.replace(
        settings_mod.settings, upstream_catalog='http://komga:25600/opds/v1.2/catalog'
    )
    try:
        off = await kavita.for_download('http://komga/opds/v1.2/books/7/file', 100, 200)
    finally:
        kavita.settings = real
    check('no Kavita upstream means no metadata lookup', off is None, f'{off!r}')

    check(
        'series id is read from a Kavita acquisition URL',
        kavita.series_id('http://h/api/opds/k/series/21/volume/265/chapter/283/download/x.cbz') == 21,
    )
    check('a non-Kavita URL yields no series id', kavita.series_id('http://komga/opds/v1.2/books/7/file') is None)

    import io as _io
    import zipfile as _zf

    from inksetter.imaging import cbz as _cbz

    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']

    bare = _io.BytesIO()
    with _zf.ZipFile(bare, 'w') as z:
        for i in range(3):
            z.writestr(f'{i:03d}.jpg', fake_page(i))
    raw = bare.getvalue()

    with_own = _io.BytesIO()
    with _zf.ZipFile(with_own, 'w') as z:
        z.writestr('ComicInfo.xml', '<ComicInfo><Series>Hand written</Series></ComicInfo>')
        z.writestr('001.jpg', fake_page(1))

    out = b''.join(_cbz.repack_iter(_io.BytesIO(raw), prof, comicinfo=lambda n: f'<x n="{n}"/>'.encode()))
    z1 = _zf.ZipFile(_io.BytesIO(out))
    check('a ComicInfo is added when the archive has none', 'ComicInfo.xml' in z1.namelist())
    check(
        'it is told the OUTPUT page count',
        z1.read('ComicInfo.xml') == b'<x n="3"/>',
        f'{z1.read("ComicInfo.xml")!r}',
    )

    out2 = b''.join(_cbz.repack_iter(_io.BytesIO(with_own.getvalue()), prof, comicinfo=lambda _n: b'<generated/>'))
    z2 = _zf.ZipFile(_io.BytesIO(out2))
    check("an archive's own ComicInfo is kept verbatim", b'Hand written' in z2.read('ComicInfo.xml'))
    check('and only one is present', sum(n.lower().endswith('comicinfo.xml') for n in z2.namelist()) == 1)

    class _R:
        def __init__(self, status, payload=None):
            self.status_code = status
            self._payload = payload

        def json(self):
            return self._payload

    class _Api:
        def __init__(self):
            self.auths = 0
            self.metadata_calls = 0
            self.reachable = True
            self.reject = set()

        async def post(self, _url, params=None, **_kw):  # noqa: ARG002
            self.auths += 1
            return _R(200, {'token': f'JWT-{self.auths}'})

        async def get(self, url, params=None, headers=None, **_kw):  # noqa: ARG002
            if not self.reachable:
                raise OSError('connection refused')
            if (headers or {}).get('Authorization', '').removeprefix('Bearer ') in self.reject:
                return _R(401)
            if '/api/Series/metadata' in url:
                self.metadata_calls += 1
                return _R(200, {'summary': 'S', 'writers': [], 'genres': []})
            if '/api/Series/volumes' in url:
                return _R(
                    200,
                    [
                        {'id': 265, 'minNumber': 1, 'coverImage': 'v265.png'},
                        {'id': 266, 'minNumber': 2, 'coverImage': ''},
                    ],
                )
            return _R(200, {'name': 'Example Manga', 'format': 1})

    class _Stub:
        def __init__(self, api):
            self.raw = api

    async def with_kavita(api, body, ttl=None):
        real_client, real_settings, real_ttl = kavita.client, kavita.settings, kavita._NEGATIVE_TTL
        kavita.client = _Stub(api)
        kavita.settings = dataclasses.replace(settings_mod.settings, upstream_catalog='http://kavita:5000/api/opds/KEY')
        if ttl is not None:
            kavita._NEGATIVE_TTL = ttl
        try:
            return await body(kavita._Kavita())
        finally:
            kavita.client, kavita.settings, kavita._NEGATIVE_TTL = real_client, real_settings, real_ttl

    api = _Api()

    async def happy(k):
        first = await k.series_metadata(21)
        second = await k.series_metadata(21)
        return first, second

    first, second = await with_kavita(api, happy)
    check(
        'a series is fetched from Kavita', first is not None and first['series']['name'] == 'Example Manga', f'{first}'
    )
    check(
        'the volume id maps to its NUMBER',
        (first or {}).get('numbers') == {265: 1, 266: 2},
        f'{(first or {}).get("numbers")}',
    )
    check(
        'a successful answer is cached, not re-fetched',
        second is first and api.metadata_calls == 1,
        f'{api.metadata_calls} calls, same object: {second is first}',
    )
    check('one authentication served both', api.auths == 1, f'{api.auths} auths')

    api = _Api()
    api.reject = {'JWT-1'}
    got = await with_kavita(api, lambda k: k.series_metadata(21))
    check('a 401 is retried with a fresh token', got is not None, f'{got}')
    check('and exactly one re-auth is spent on it', api.auths == 2, f'{api.auths} auths')

    api = _Api()
    api.reject = {'JWT-1', 'JWT-2', 'JWT-3'}
    got = await with_kavita(api, lambda k: k.series_metadata(21))
    check('a token rejected twice gives up', got is None, f'{got}')
    check('without a third attempt', api.auths == 2, f'{api.auths} auths')

    api = _Api()
    api.reachable = False

    async def recovers(k):
        down = await k.series_metadata(21)
        api.reachable = True
        return down, await k.series_metadata(21)

    down, up = await with_kavita(api, recovers, ttl=0.0)
    check('an unreachable Kavita yields no metadata', down is None, f'{down}')
    check('and is retried once the negative entry expires', up is not None, f'{up}')

    api = _Api()
    api.reachable = False

    async def stays_down(k):
        await k.series_metadata(21)
        await k.series_metadata(21)
        return None

    await with_kavita(api, stays_down, ttl=300.0)
    check('a failure inside the TTL costs one round trip, not two', api.auths == 1, f'{api.auths} auths')

    api = _Api()
    vol1 = '/api/opds/KEY/series/21/volume/265/chapter/283/download/x.cbz'
    vol2 = '/api/opds/KEY/series/21/volume/266/chapter/284/download/x.cbz'

    async def covers(_k):
        return (
            await kavita.cover_url(f'http://kavita:5000{vol1}'),
            await kavita.cover_url(f'http://kavita:5000{vol2}'),
            await kavita.cover_url('http://kavita:5000/api/opds/KEY/series/21'),
        )

    got1, got2, got_novol = await with_kavita(api, covers)
    check(
        'the volume cover URL comes from the API, not the feed',
        got1 == 'http://kavita:5000/api/image/volume-cover?volumeId=265&apiKey=KEY',
        f'{got1!r}',
    )
    check('a volume with no cover offers none', got2 is None, f'{got2!r}')
    check('a URL naming no volume offers none', got_novol is None, f'{got_novol!r}')
    check(
        'asking for the cover costs no extra round trip',
        api.metadata_calls == 1,
        f'{api.metadata_calls} metadata calls for three cover lookups',
    )

    real_s = kavita.settings
    kavita.settings = dataclasses.replace(
        settings_mod.settings, upstream_catalog='http://komga:25600/opds/v1.2/catalog'
    )
    try:
        off = await kavita.cover_url('http://komga/opds/v1.2/books/7/file')
    finally:
        kavita.settings = real_s
    check('a non-Kavita upstream asks nothing', off is None, f'{off!r}')

    check(
        'the volume id is read from an acquisition URL',
        kavita.volume_id(vol1) == 265 and kavita.volume_id('/api/opds/k/series/21') is None,
        f'{kavita.volume_id(vol1)}',
    )

    from inksetter import app as _app

    async def pick(api_url, api_bytes, feed_bytes):
        real_cu, real_cb = _app.kavita.cover_url, _app._cover_bytes

        async def fake_cover_url(_u):
            return api_url

        async def fake_bytes(u, _rq):
            if u is None:
                return None
            return api_bytes if u == api_url else feed_bytes

        _app.kavita.cover_url = fake_cover_url
        _app._cover_bytes = fake_bytes
        acq = 'http://k/api/opds/K/series/1/volume/2/chapter/3/download/x.cbz'
        try:
            return await _app._pick_cover(acq, 'FEED', None)
        finally:
            _app.kavita.cover_url, _app._cover_bytes = real_cu, real_cb

    check(
        "the API's cover is preferred over the feed's",
        await pick('API', b'jacket', b'title-page') == b'jacket',
    )
    check(
        'the feed cover is used when the API offers none',
        await pick(None, None, b'title-page') == b'title-page',
    )
    check(
        'the feed cover is used when the API cover fails to fetch',
        await pick('API', None, b'title-page') == b'title-page',
    )
    check(
        'no cover at all is not an error',
        await pick(None, None, None) is None,
    )

    def boom(_n):
        raise RuntimeError('kavita fell over')

    out3 = b''.join(_cbz.repack_iter(_io.BytesIO(raw), prof, comicinfo=boom))
    z3 = _zf.ZipFile(_io.BytesIO(out3))
    check(
        'a failing metadata factory costs the metadata, not the volume',
        z3.testzip() is None
        and 'ComicInfo.xml' not in z3.namelist()
        and sum(1 for n in z3.namelist() if n.endswith('.png')) == 3,
    )


def _rich_colour_page(w: int = 900, h: int = 1200) -> bytes:
    yy, xx = np.mgrid[0:h, 0:w]
    rgb = np.empty((h, w, 3), np.float32)
    rgb[:, :, 0] = 40 + 200 * (xx / w)
    rgb[:, :, 1] = 30 + 180 * (yy / h)
    rgb[:, :, 2] = 120 + 100 * np.sin(6.0 * np.pi * (xx + yy) / w)
    rgb += (18 * np.sin(0.7 * xx) * np.cos(0.7 * yy))[:, :, None]
    a = np.clip(rgb, 0, 255).astype(np.uint8)
    a[h // 3 : h // 3 + 60, 80 : w - 80] = 0
    return pyvips.Image.new_from_memory(a.tobytes(), w, h, 3, 'uchar').pngsave_buffer()


def check_png_effort() -> None:
    prof = profiles.PROFILES['kindle-colorsoft']
    check('the shipped default is the measured one', prof.png_effort == 4, f'{prof.png_effort}')

    src = _rich_colour_page()
    lo, _ = pipeline.render_page(src, dataclasses.replace(prof, png_effort=1))
    hi, _ = pipeline.render_page(src, dataclasses.replace(prof, png_effort=10))
    check('png_effort reaches the encoder', lo != hi, f'{len(lo)} vs {len(hi)} bytes')
    check(
        'and both still decode at the panel geometry',
        all(
            (lambda i: (i.width, i.height))(pyvips.Image.new_from_buffer(b, '')) == (prof.width, prof.height)
            for b in (lo, hi)
        ),
    )

    ref = pyvips.Image.new_from_buffer(src, '').colourspace('srgb')
    kw = {'palette': True, 'bitdepth': 8, 'colours': 256, 'dither': 1.0, 'compression': 7, 'strip': True}

    def plane(im):
        im = im.colourspace('srgb')
        return np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width, im.bands)).astype(
            np.float32
        )

    def blocks(a, k=8):
        hh, ww, c = a.shape
        a = a[: hh // k * k, : ww // k * k]
        return a.reshape(hh // k, k, ww // k, k, c).mean((1, 3))

    base = blocks(plane(ref))
    err = {}
    for eff in (4, 7):
        got = pyvips.Image.new_from_buffer(ref.pngsave_buffer(effort=eff, **kw), '')
        err[eff] = float(np.abs(blocks(plane(got)) - base).mean())
    check(
        'effort 4 holds local tone as well as 7 does',
        err[4] <= err[7] + 0.10,
        f'8x8 error: effort 4 {err[4]:.3f}, effort 7 {err[7]:.3f}',
    )

    for bad in (0, 11, -1):
        try:
            profiles.validate(dataclasses.replace(prof, png_effort=bad))
            ok = False
        except ValueError:
            ok = True
        check(f'png_effort={bad} is refused', ok)


def check_colour_pad_ring() -> None:
    import collections

    prof = profiles.PROFILES['kindle-colorsoft']
    tw, th = prof.width, round(prof.width * prof.aspect)

    w, h = 900, 1000
    yy, xx = np.mgrid[0:h, 0:w]
    r = (40 + 200 * xx / w).astype(np.uint8)
    g = (30 + 200 * yy / h).astype(np.uint8)
    b = (220 - 150 * xx / w).astype(np.uint8)
    rgb = np.dstack([r, g, b])
    rgb[::3, ::3] = [255, 40, 90]
    src = pyvips.Image.new_from_memory(np.ascontiguousarray(rgb).tobytes(), w, h, 3, 'uchar')
    blob = src.pngsave_buffer()

    check(
        'the probe page is routed to the colour path',
        pipeline.chroma_metric(blob) >= prof.mono_chroma_threshold,
        f'chroma {pipeline.chroma_metric(blob):.1f}, threshold {prof.mono_chroma_threshold}',
    )
    cx, cy, cw, ch = pipeline._geometry(blob, prof, tw, th, mono=False).content
    check(
        'the probe page really is padded',
        (cw, ch) != (tw, th),
        f'content {cw}x{ch} fills {tw}x{th}',
    )

    out, _mime = pipeline.render_page(blob, prof)
    im = pyvips.Image.new_from_buffer(out, '')
    a = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width, im.bands))
    mask = np.ones(a.shape[:2], bool)
    mask[cy : cy + ch, cx : cx + cw] = False
    pad = a[mask]
    modal = collections.Counter(map(tuple, pad)).most_common(1)[0][0]
    off = ~np.all(pad == np.array(modal), axis=1)
    frac = off.sum() / max(mask.sum(), 1)

    ys, xs = np.nonzero(mask)
    dx = np.maximum(np.maximum(cx - xs, xs - (cx + cw - 1)), 0)
    dy = np.maximum(np.maximum(cy - ys, ys - (cy + ch - 1)), 0)
    depth = int(np.maximum(dx, dy)[off].max()) if off.any() else 0

    check(
        'colour pad contamination stays within a few pixels of the edge',
        depth <= 3,
        f'reaches {depth} px into a pad {max(cx, cy)} px deep',
    )
    check(
        'colour pad contamination stays under 1% of the pad',
        frac < 0.01,
        f'{frac:.3%} of {mask.sum()} pad pixels',
    )
    mono_out, _ = pipeline.render_page(blob, profiles.PROFILES['kobo-clara-hd-2e-bw'])
    mi = pyvips.Image.new_from_buffer(mono_out, '')
    ma = np.ndarray(buffer=mi.write_to_memory(), dtype=np.uint8, shape=(mi.height, mi.width))
    mcx, mcy, mcw, mch = pipeline._geometry(
        blob, profiles.PROFILES['kobo-clara-hd-2e-bw'], 1072, 1448, mono=True
    ).content
    mm = np.ones(ma.shape, bool)
    mm[mcy : mcy + mch, mcx : mcx + mcw] = False
    check(
        'the mono pad has no contamination at all',
        not mm.any() or len(np.unique(ma[mm])) == 1,
        f'{len(np.unique(ma[mm])) if mm.any() else 1} values',
    )


def check_width_fit() -> None:
    strips = {n: p for n, p in profiles.PROFILES.items() if p.fit == 'width'}
    check('the table carries webtoon profiles', bool(strips), 'none define fit = "width"')
    prof = profiles.PROFILES['kindle-colorsoft-webtoon']
    box = profiles.PROFILES['kindle-colorsoft']
    check(
        'every webtoon profile fits by width and leaves the strip alone',
        all(p.fit == 'width' and not p.autocrop and not p.rotate_wide for p in strips.values()),
        f'{[(n, p.fit, p.autocrop, p.rotate_wide) for n, p in strips.items() if p.autocrop or p.rotate_wide]}',
    )
    check(
        'and the control shares its panel, differing only in the fit',
        (box.width, box.height) == (prof.width, prof.height) and box.fit == 'box',
        f'{box.width}x{box.height} fit={box.fit} vs {prof.width}x{prof.height} fit={prof.fit}',
    )

    def strip(w: int, h: int) -> bytes:
        a = np.full((h, w), 255, np.uint8)
        a[h // 4 : 3 * h // 4, w // 4 : 3 * w // 4] = 40
        return pyvips.Image.new_from_memory(a.tobytes(), w, h, 1, 'uchar').pngsave_buffer()

    seen = []
    for sh in (400, 1000, 1280, 2400):
        geom = pipeline._geometry(strip(800, sh), prof, prof.width, prof.height, mono=True)
        seen.append((geom.image.width, geom.image.height, geom.pad, round(geom.content[2] / 800, 4)))
    widths = {s[0] for s in seen}
    check('every slice lands on exactly the panel width', widths == {prof.width}, f'{sorted(widths)}')
    check('no slice is padded', all(s[2] is None for s in seen), f'{[s[2] for s in seen]}')
    check('and they share one scale factor', len({s[3] for s in seen}) == 1, f'{[s[3] for s in seen]}')
    for (_w, h, _p, _s), src_h in zip(seen, (400, 1000, 1280, 2400), strict=True):
        want = round(src_h * prof.width / 800)
        check(
            f'a {src_h} px slice keeps its aspect ({want} px tall)',
            abs(h - want) <= 1,
            f'got {h}, wanted {want}',
        )

    geom = pipeline._geometry(strip(800, 330), prof, prof.width, prof.height, mono=True)
    check(
        'a short slice is not blown up to the panel height',
        geom.image.height < prof.height // 2,
        f'{geom.image.width}x{geom.image.height}',
    )

    narrow = dataclasses.replace(prof, upscale_max=1.1)
    geom = pipeline._geometry(strip(400, 900), narrow, narrow.width, narrow.height, mono=True)
    check(
        'upscale_max does not cap a width fit',
        geom.image.width == narrow.width,
        f'{geom.image.width} != {narrow.width} (would need {narrow.width / 400:.2f}x, cap {narrow.upscale_max})',
    )

    box = profiles.PROFILES['kobo-clara-hd-2e-bw']
    g = pipeline._geometry(strip(800, 1000), box, box.width, box.height, mono=True)
    check(
        'a box-fit profile still pads to the full panel',
        (g.image.width, g.image.height) == (box.width, box.height) and g.pad is not None,
        f'{g.image.width}x{g.image.height} pad={g.pad}',
    )


def check_pad_seam() -> None:
    prof = profiles.PROFILES['kindle-colorsoft']
    tw = prof.width
    th = round(tw * prof.aspect)

    for tone in (0, 60, 127, 128, 200, 247, 255):
        sw, sh = 700, 1000
        a = np.full((sh, sw), tone, np.uint8)
        blob = pyvips.Image.new_from_memory(a.tobytes(), sw, sh, 1, 'uchar').pngsave_buffer()

        geom = pipeline._geometry(blob, prof, tw, th, mono=True)
        cx, cy, cw, ch = geom.content
        if (cw, ch) == (tw, th):
            continue
        out, _mime = pipeline.render_page(blob, prof)
        im = pyvips.Image.new_from_buffer(out, '')
        arr = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width))
        mask = np.ones(arr.shape, bool)
        mask[cy : cy + ch, cx : cx + cw] = False
        pad = np.unique(arr[mask])
        want = 0 if tone < pipeline._PAD_MIDPOINT else 255
        check(
            f'pad is one flat value (source tone {tone})',
            len(pad) == 1,
            f'{len(pad)} distinct values',
        )
        check(
            f'pad is paper-white or frame-black (source tone {tone})',
            len(pad) == 1 and int(pad[0]) in (0, 255),
            f'pad={[int(v) for v in pad]}',
        )
        check(
            f'pad took the nearer end of the ladder (source tone {tone})',
            len(pad) == 1 and int(pad[0]) == want,
            f'pad={[int(v) for v in pad]}, wanted {want}',
        )

    def level(draw, horizontal=True, vertical=True) -> float:
        a = np.full((1000, 700), 250, np.uint8)
        draw(a)
        im = pyvips.Image.new_from_memory(a.tobytes(), 700, 1000, 1, 'uchar')
        return pipeline._mono_pad_level(im, horizontal, vertical)

    def dark_all(a):
        a[:] = 10
        a[400:600, 300:400] = 250

    def dark_left(a):
        a[:, :60] = 10

    def dark_lr(a):
        a[:, :60] = 10
        a[:, -60:] = 10

    def dark_tb(a):
        a[:60, :] = 10
        a[-60:, :] = 10

    check('a white page is padded white', level(lambda _a: None) == 255.0)
    check('a dark page is padded black', level(dark_all) == 0.0, f'{level(dark_all)}')
    check(
        'padded edges that disagree fall back to white',
        level(dark_left) == 255.0,
        f'{level(dark_left)}',
    )
    check(
        'both padded sides dark is still black',
        level(dark_lr, horizontal=True, vertical=False) == 0.0,
        f'{level(dark_lr, True, False)}',
    )
    check(
        'a page dark on one side and white on the other pads white',
        level(dark_left, horizontal=True, vertical=False) == 255.0,
        f'{level(dark_left, True, False)}',
    )
    check(
        'an unpadded edge gets no vote',
        level(dark_tb, horizontal=True, vertical=False) == 255.0,
        f'{level(dark_tb, True, False)}',
    )
    check(
        'and it does vote when that side IS padded',
        level(dark_tb, horizontal=False, vertical=True) == 0.0,
        f'{level(dark_tb, False, True)}',
    )

    def flat_dark_left_mixed_right(a):
        a[:, :2] = 10
        a[:400, -2:] = 0
        a[400:, -2:] = 250

    def both_edges_graded(a):
        ramp = np.linspace(0, 120, 1000).astype(np.uint8)[:, None]
        a[:, :2] = ramp
        a[:, -2:] = ramp

    check(
        'a mixed edge abstains rather than forcing a tie-break',
        level(flat_dark_left_mixed_right, horizontal=True, vertical=False) == 0.0,
        f'{level(flat_dark_left_mixed_right, True, False)}',
    )
    check(
        'when no consulted edge is flat, the pad is white',
        level(both_edges_graded, horizontal=True, vertical=False) == 255.0,
        f'{level(both_edges_graded, True, False)}',
    )

    def noisy_dark_edges(a):
        noise = np.where(np.arange(1000) % 2 == 0, 8, 14).astype(np.uint8)[:, None]
        a[:, :2] = noise
        a[:, -2:] = noise

    check(
        'an edge with ordinary scan noise still votes',
        level(noisy_dark_edges, horizontal=True, vertical=False) == 0.0,
        f'{level(noisy_dark_edges, True, False)}',
    )

    a = np.full((1000, 700), 255, np.uint8)
    a[:12, :] = 0
    a[-12:, :] = 0
    haloing = pyvips.Image.new_from_memory(a.tobytes(), 700, 1000, 1, 'uchar').pngsave_buffer()
    geom = pipeline._geometry(haloing, prof, tw, th, mono=True)
    cx, cy, cw, ch = geom.content
    out, _mime = pipeline.render_page(haloing, prof)
    im = pyvips.Image.new_from_buffer(out, '')
    arr = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width))
    mask = np.ones(arr.shape, bool)
    mask[cy : cy + ch, cx : cx + cw] = False
    vals = np.unique(arr[mask])
    check(
        'an unsharp halo does not survive in the margin',
        len(vals) == 1 and int(vals[0]) == 255,
        f'{len(vals)} distinct pad values: {[int(v) for v in vals[:4]]}',
    )

    seen = []
    real_flatten = pipeline._flatten_pad

    def spy(a_, content, level):
        seen.append((float(level), int(a_.max())))
        return real_flatten(a_, content, level)

    pipeline._flatten_pad = spy
    try:
        pipeline.render_page(haloing, prof)
    finally:
        pipeline._flatten_pad = real_flatten
    check(
        'the png4 flatten is given a ladder index, not a 0-255 level',
        len(seen) == 1 and seen[0][0] <= 15.0 and seen[0][1] <= 15,
        f'level={seen[0][0] if seen else None}, array max={seen[0][1] if seen else None}',
    )

    rgb = np.zeros((900, 600, 3), np.uint8)
    rgb[:, :, 0], rgb[:, :, 1] = 240, 40
    cblob = pyvips.Image.new_from_memory(rgb.tobytes(), 600, 900, 3, 'uchar').pngsave_buffer()
    cgeom = pipeline._geometry(cblob, prof, tw, th, mono=False)
    check('the colour path is not given a snapped pad', cgeom.pad is None, f'{cgeom.pad!r}')


def check_edge_line() -> None:
    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']
    thr = prof.autocrop_threshold
    w, h = 800, 1200

    def page(draw):
        a = np.full((h, w), 255, np.uint8)
        a[60 : h - 60, 50 : w - 50] = 90
        draw(a)
        return pyvips.Image.new_from_memory(a.tobytes(), w, h, 1, 'uchar')

    clean = page(lambda _a: None)
    check(
        'a clean page is not shaved',
        pipeline._strip_edge_lines(clean, thr) == (0, 0, 0, 0),
        str(pipeline._strip_edge_lines(clean, thr)),
    )

    def one_line(a):
        a[:, w - 1] = 0

    lined = page(one_line)
    check(
        'a 1px line welded to the right edge is shaved',
        pipeline._strip_edge_lines(lined, thr) == (0, 0, 1, 0),
        str(pipeline._strip_edge_lines(lined, thr)),
    )

    def draw_all(a):
        a[:, 0] = 0
        a[:, w - 1] = 0
        a[0, :] = 0
        a[h - 1, :] = 0

    check(
        'lines on all four edges are all shaved',
        pipeline._strip_edge_lines(page(draw_all), thr) == (1, 1, 1, 1),
        str(pipeline._strip_edge_lines(page(draw_all), thr)),
    )

    def bleeds(a):
        a[:, w - 30 :] = 40

    check(
        'artwork bleeding off the edge is NOT shaved',
        pipeline._strip_edge_lines(page(bleeds), thr)[2] == 0,
        str(pipeline._strip_edge_lines(page(bleeds), thr)),
    )

    def dashed(a):
        a[::4, w - 1] = 0

    check(
        'a sparse mark along the edge is NOT shaved',
        pipeline._strip_edge_lines(page(dashed), thr)[2] == 0,
        str(pipeline._strip_edge_lines(page(dashed), thr)),
    )

    def band(a):
        a[:, w - 40 :] = 0

    shaved = pipeline._strip_edge_lines(page(band), thr)
    check(
        'a thick band is capped, never shaved away wholesale',
        shaved[2] <= pipeline._LINE_MAX,
        f'shaved {shaved[2]} with a cap of {pipeline._LINE_MAX}',
    )

    box = pipeline._autocrop_box(lined, prof)
    check(
        'autocrop returns a box that excludes the edge line',
        box is not None and box[0] + box[2] < w,
        f'box={box} on a {w}px page',
    )
    clean_box = pipeline._autocrop_box(clean, prof)
    check(
        'stripping the line yields exactly the unlined box',
        box == clean_box,
        f'lined={box} clean={clean_box}',
    )


def check_dark_margin_crop() -> None:
    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']
    w, h, inset = 900, 1300, 90

    def bordered(ink_border: bool, paper_left: bool = False) -> pyvips.Image:
        bg, fg = (0, 255) if ink_border else (255, 40)
        a = np.full((h, w), bg, np.uint8)
        a[inset : h - inset, inset : w - inset] = fg
        if paper_left:
            a[:, :inset] = 255
        return pyvips.Image.new_from_memory(a.tobytes(), w, h, 1, 'uchar')

    ink, paper, mixed = bordered(True), bordered(False), bordered(True, paper_left=True)
    check('every edge of an ink-bordered page reads as ink', all(pipeline._dark_edges(ink)))
    check('no edge of a paper-bordered page does', not any(pipeline._dark_edges(paper)))
    check(
        'and the decision is per edge, not per page',
        pipeline._dark_edges(mixed) == (False, True, True, True),
        f'{pipeline._dark_edges(mixed)}',
    )

    box = pipeline._autocrop_box(ink, prof)
    check('an ink border is cropped away', box is not None, 'box refused')
    if box:
        left, top, bw, bh = box
        check(
            'and the crop lands on the content, not the border',
            abs(left - inset) <= 4
            and abs(top - inset) <= 4
            and abs(bw - (w - 2 * inset)) <= 8
            and abs(bh - (h - 2 * inset)) <= 8,
            f'box={box}, wanted about ({inset}, {inset}, {w - 2 * inset}, {h - 2 * inset})',
        )

    band = np.full((h, w), 255, np.uint8)
    band[:, :120] = 0
    band[200 : h - 200, 300 : w - 200] = 40
    one_side = pyvips.Image.new_from_memory(band.tobytes(), w, h, 1, 'uchar')
    check(
        'a band down one side reads as ink on that edge only',
        pipeline._dark_edges(one_side) == (True, False, False, False),
        f'{pipeline._dark_edges(one_side)}',
    )
    ob = pipeline._autocrop_box(one_side, prof)
    check('and that band is trimmed away', ob is not None and ob[0] >= 110, f'box={ob}')

    solid = pyvips.Image.new_from_memory(np.full((h, w), 0, np.uint8).tobytes(), w, h, 1, 'uchar')
    check('a page that is solid ink is refused, not cropped away', pipeline._autocrop_box(solid, prof) is None)


def check_autocrop_open() -> None:
    w, h = 900, 1300
    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']

    def page(mark=None, art_to_edge=False) -> bytes:
        a = np.full((h, w), 255, np.uint8)
        a[80 : h - 80, 60 : w - 60] = 60
        if art_to_edge:
            a[80 : h - 80, 60:] = 60
        if mark == 'thin':
            a[h - 60 : h - 36, w // 2 : w // 2 + 2] = 0
        elif mark == 'dense':
            for x in (w // 2 - 6, w // 2 + 2):
                a[h - 60 : h - 36, x : x + 3] = 0
            a[h - 38 : h - 36, w // 2 + 2 : w // 2 + 12] = 0
        return pyvips.Image.new_from_memory(a.tobytes(), w, h, 1, 'uchar').pngsave_buffer()

    def box(blob):
        return pipeline._autocrop_box(pyvips.Image.new_from_buffer(blob, '').colourspace('b-w'), prof)

    clean = box(page())
    thin = box(page('thin'))
    check(
        'a thin mark no longer anchors the bottom edge',
        clean is not None and thin is not None and abs(thin[3] - clean[3]) <= 6,
        f'clean={clean} thin={thin}',
    )

    raw = pyvips.Image.new_from_buffer(page('thin'), '').colourspace('b-w')
    naive = raw.gaussblur(2).find_trim(threshold=prof.autocrop_threshold, background=255)
    check(
        'the un-opened box really was defeated by it',
        thin is not None and naive[1] + naive[3] > thin[1] + thin[3] + 15,
        f'un-opened bottom={naive[1] + naive[3]} opened bottom={thin[1] + thin[3] if thin else None}',
    )

    dense = box(page('dense'))
    check(
        'a dense glyph survives, as documented',
        dense is not None and clean is not None and dense[3] > clean[3] + 15,
        f'clean={clean} dense={dense}',
    )

    edge = box(page(art_to_edge=True))
    check(
        'artwork reaching an edge still holds it',
        edge is not None and edge[0] + edge[2] >= w - 4,
        f'{edge} on a {w}px page',
    )

    a = np.full((h, w), 255, np.uint8)
    a[80 : h - 80, 60 : w - 60] = 60
    a[h - 70 : h - 30, 60 : w - 60] = 0
    banded = pyvips.Image.new_from_memory(a.tobytes(), w, h, 1, 'uchar').pngsave_buffer()
    bim = pyvips.Image.new_from_buffer(banded, '').colourspace('b-w')
    bb = pipeline._autocrop_box(bim, prof)
    check(
        'a margin band full of ink is kept, not opened away',
        bb is not None and bb[1] + bb[3] >= h - 34,
        f'{bb} on a {h}px page',
    )

    ws = pipeline._open_window(pyvips.Image.black(700, 1000))
    wb = pipeline._open_window(pyvips.Image.black(3000, 4200))
    check(
        'the median window scales with the page and stays odd',
        ws % 2 == 1 and wb % 2 == 1 and 3 <= ws < wb <= 21,
        f'700px->{ws}  3000px->{wb}',
    )


def check_cover_delivery() -> None:
    import io as _io
    import zipfile as _zf

    from inksetter.imaging import cbz as _cbz

    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']
    raw = _cbz_bytes(3)
    page1 = _zf.ZipFile(_io.BytesIO(raw)).read('p000.jpg')

    thumb = pyvips.Image.new_from_buffer(page1, '').thumbnail_image(160).jpegsave_buffer(Q=50)
    check(
        'a downscaled re-encode is the same picture',
        pipeline.same_picture(page1, thumb),
        f'corr={float((pipeline._norm_grid(page1) * pipeline._norm_grid(thumb)).mean()):.3f}',
    )
    block = np.full((2400, 1600), 255, np.uint8)
    block[100:1200, 100:1400] = 0
    unrelated = pyvips.Image.new_from_memory(block.tobytes(), 1600, 2400, 1, 'uchar').jpegsave_buffer(Q=92)
    check(
        'a different picture is not the same picture',
        not pipeline.same_picture(page1, unrelated),
        f'corr={float((pipeline._norm_grid(page1) * pipeline._norm_grid(unrelated)).mean()):.3f}',
    )
    check(
        'unreadable bytes are not the same picture as anything',
        not pipeline.same_picture(page1, b'<html>no</html>'),
    )

    def pages(blob):
        z = _zf.ZipFile(_io.BytesIO(blob))
        return [n for n in z.namelist() if _cbz.is_image(n)], z

    base, _ = pages(b''.join(_cbz.repack_iter(_io.BytesIO(raw), prof)))

    art = fake_page(77, 600, 900)
    got, z = pages(b''.join(_cbz.repack_iter(_io.BytesIO(raw), prof, cover=art)))
    check(
        'a cover outside the archive is prepended',
        len(got) == len(base) + 1 and got[0] == '0000.png',
        f'{base} -> {got}',
    )
    check(
        'the prepended page is that cover, rendered',
        z.read(got[0]) == pipeline.render_page(art, prof)[0],
    )

    same, _ = pages(b''.join(_cbz.repack_iter(_io.BytesIO(raw), prof, cover=thumb)))
    check(
        'a cover that is already page 1 is not prepended',
        same == base,
        f'{base} -> {same}',
    )

    bad, _ = pages(b''.join(_cbz.repack_iter(_io.BytesIO(raw), prof, cover=b'not an image')))
    check('an unreadable cover is ignored, not fatal', bad == base, f'{base} -> {bad}')

    tw = prof.width
    th = round(tw * prof.aspect)
    sa = np.full((420, 300), 245, np.uint8)
    sa[40:380, 30:270] = np.tile(np.linspace(10, 240, 240).astype(np.uint8), (340, 1))
    sa[150:250, 90:210] = 20
    small = pyvips.Image.new_from_memory(sa.tobytes(), 300, 420, 1, 'uchar').pngsave_buffer()
    capped = pipeline._geometry(small, prof, tw, th, mono=True)
    lifted = pipeline._geometry(small, profiles.embedded_cover_for(prof), tw, th, mono=True)
    check(
        'the cover profile lifts the enlargement cap',
        profiles.embedded_cover_for(prof).upscale_max > prof.upscale_max,
        f'{prof.upscale_max} -> {profiles.embedded_cover_for(prof).upscale_max}',
    )

    rejected = []
    for _name, _dev in profiles.PROFILES.items():
        try:
            profiles.validate(profiles.embedded_cover_for(_dev), 'cover')
        except ValueError as _exc:
            rejected.append(f'{_name}: {_exc}')
    check(
        'every embedded-cover profile still passes the validator',
        not rejected,
        f'{len(rejected)} rejected: ' + '; '.join(rejected[:2]),
    )

    def covered(g):
        return (g.content[2] * g.content[3]) / float(tw * th)

    check(
        'an uncapped cover fills much more of the panel',
        covered(lifted) > covered(capped) * 1.5,
        f'{covered(capped):.1%} capped vs {covered(lifted):.1%} uncapped',
    )
    check(
        'and it is still exactly the panel geometry',
        (lifted.image.width, lifted.image.height) == (tw, th),
        f'{lifted.image.width}x{lifted.image.height}',
    )
    got2, z2 = pages(b''.join(_cbz.repack_iter(_io.BytesIO(raw), prof, cover=small)))
    check(
        'a small cover is prepended',
        len(got2) == len(base) + 1 and got2[0].startswith('0000.'),
        f'{got2}',
    )
    check(
        'the prepended cover is the UNCAPPED render',
        z2.read(got2[0]) == pipeline.render_page(small, profiles.embedded_cover_for(prof))[0],
    )
    check(
        'which is not what the page path would have produced',
        z2.read(got2[0]) != pipeline.render_page(small, prof)[0],
    )
    tiny = _io.BytesIO()
    with _zf.ZipFile(tiny, 'w') as zz:
        for i in range(2):
            zz.writestr(f'p{i:03d}.png', small)
    tiny_raw = tiny.getvalue()
    got3, z3 = pages(b''.join(_cbz.repack_iter(_io.BytesIO(tiny_raw), prof)))
    check(
        'a small page IS affected by the cap, so this fixture can tell',
        pipeline.render_page(small, prof)[0] != pipeline.render_page(small, profiles.embedded_cover_for(prof))[0],
    )
    check(
        'ordinary pages keep the enlargement cap',
        z3.read(got3[0]) == pipeline.render_page(small, prof)[0],
    )
    check(
        'and are not rendered like the cover',
        z3.read(got3[0]) != pipeline.render_page(small, profiles.embedded_cover_for(prof))[0],
    )

    opaque = pyvips.Image.new_from_buffer(page1, '')
    flat = opaque.copy()
    rgb = flat.colourspace('srgb')
    a16 = np.ndarray(buffer=flat.write_to_memory(), dtype=np.uint8, shape=(flat.height, flat.width))
    encodings = {
        'grey8 png': flat.pngsave_buffer(),
        'rgb8 png': rgb.pngsave_buffer(),
        'rgba8 png': rgb.bandjoin(255).pngsave_buffer(),
        'grey+alpha png': flat.bandjoin(255).pngsave_buffer(),
        'cmyk jpeg': rgb.colourspace('cmyk').jpegsave_buffer(Q=92),
        'grey16 png': (
            pyvips.Image.new_from_memory((a16.astype(np.uint16) * 257).tobytes(), flat.width, flat.height, 1, 'ushort')
            .copy(interpretation='grey16')
            .pngsave_buffer()
        ),
    }
    ref = pipeline._norm_grid(encodings['grey8 png'])
    for label, blob in encodings.items():
        corr = float((pipeline._norm_grid(blob) * ref).mean())
        check(f'{label} grids the same as plain 8-bit grey', corr > 0.99, f'corr={corr:.3f}')
        check(f'{label} is the same picture as the page', pipeline.same_picture(blob, page1))

    alpha_cbz = _io.BytesIO()
    with _zf.ZipFile(alpha_cbz, 'w') as z:
        z.writestr('p000.png', encodings['rgba8 png'])
        z.writestr('p001.jpg', fake_page(1))
    a_cover = opaque.colourspace('srgb').thumbnail_image(160).jpegsave_buffer(Q=50)
    a_pages, _ = pages(b''.join(_cbz.repack_iter(_io.BytesIO(alpha_cbz.getvalue()), prof, cover=a_cover)))
    check(
        'a transparent page 1 does not get its own cover prepended',
        len(a_pages) == 2,
        f'{a_pages}',
    )


def check_cover_token() -> None:
    url, cover = 'http://h/book.cbz', 'http://h/cover.jpg'
    check(
        'a two-URL token round-trips',
        rewrite.decode_parts(encode_token(url, cover)) == (url, cover),
        f'{rewrite.decode_parts(encode_token(url, cover))}',
    )
    check(
        'a one-URL token still decodes, with no cover',
        rewrite.decode_parts(encode_token(url)) == (url, None),
    )
    check(
        'decode_token ignores the cover half',
        rewrite.decode_token(encode_token(url, cover)) == url,
    )

    ctx = rewrite.Ctx(profile='kobo-clara-hd-2e-bw', public_base='http://p', base_url='http://up/f')
    feed = (
        b'<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">'
        b'<link rel="self" href="/self"/>'
        b'<entry><title>b</title>'
        b'<link rel="http://opds-spec.org/image/thumbnail" href="/thumb.jpg"/>'
        b'<link rel="http://opds-spec.org/image" href="/cover.jpg"/>'
        b'<link rel="http://opds-spec.org/acquisition" '
        b'type="application/vnd.comicbook+zip" href="/book.cbz"/>'
        b'</entry></feed>'
    )
    out = rewrite.rewrite_atom(feed, ctx, 'image/png').decode()
    dl = re.search(r'/dl/([\w-]+)', out).group(1)
    _got_url, got_cover = rewrite.decode_parts(dl)
    check(
        'the dl token carries the entry cover',
        got_cover == 'http://up/cover.jpg',
        f'{got_cover!r}',
    )
    check(
        'the full image wins over the thumbnail',
        got_cover is not None and 'thumb' not in got_cover,
        f'{got_cover!r}',
    )
    check(
        'the cover URL points at the ORIGIN, not back at this proxy',
        got_cover is not None and 'http://p/' not in got_cover,
        f'{got_cover!r}',
    )
    multi = feed.replace(
        b'rel="http://opds-spec.org/image"',
        b'rel="http://opds-spec.org/image related"',
    )
    out_multi = rewrite.rewrite_atom(multi, ctx, 'image/png').decode()
    multi_cover = rewrite.decode_parts(re.search(r'/dl/([\w-]+)', out_multi).group(1))[1]
    check(
        'a multi-valued rel still yields the cover',
        multi_cover == 'http://up/cover.jpg',
        f'{multi_cover!r}',
    )

    self_tok = re.search(r'/f/([\w-]+)', out).group(1)
    check(
        'a feed-level link carries no cover',
        rewrite.decode_parts(self_tok)[1] is None,
        f'{rewrite.decode_parts(self_tok)}',
    )

    pub = json.dumps(
        {
            'metadata': {'title': 'b'},
            'publications': [
                {
                    'metadata': {'title': 'b'},
                    'images': [{'href': '/cover.jpg', 'type': 'image/jpeg'}],
                    'links': [
                        {
                            'href': '/book.cbz',
                            'type': 'application/vnd.comicbook+zip',
                            'rel': 'http://opds-spec.org/acquisition',
                        }
                    ],
                }
            ],
        }
    ).encode()
    jout = json.loads(rewrite.rewrite_json(pub, ctx, 'image/png'))
    jtok = re.search(r'/dl/([\w-]+)', jout['publications'][0]['links'][0]['href']).group(1)
    check(
        'an OPDS 2.0 publication ties its cover to its download',
        rewrite.decode_parts(jtok)[1] == 'http://up/cover.jpg',
        f'{rewrite.decode_parts(jtok)}',
    )


def check_concurrency_defaults() -> None:
    import os as _os

    check(
        'only one render may be inside an FFT',
        pipeline._FFT_SLOTS._initial_value == 1,
        f'{pipeline._FFT_SLOTS._initial_value} slot(s)',
    )
    with mock.patch.dict(_os.environ, {'FFT_WORKERS': '8'}):
        again = importlib.reload(pipeline)
        moved = again._FFT_SLOTS._initial_value
    importlib.reload(pipeline)
    check('and no environment variable moves it', moved == 1, f'FFT_WORKERS=8 gave {moved}')

    fresh = {k: v for k, v in _os.environ.items() if k != 'RENDER_WORKERS'}
    with mock.patch.dict(_os.environ, fresh, clear=True):
        reloaded = importlib.reload(settings_mod).settings
        check(
            'RENDER_WORKERS defaults to 3',
            reloaded.render_workers == 3,
            f'{reloaded.render_workers}',
        )
        check(
            'the default does not scale with cpu_count',
            reloaded.render_workers != max(1, (_os.cpu_count() or 2) - 1) or (_os.cpu_count() or 2) == 4,
            f'cpu_count={_os.cpu_count()} workers={reloaded.render_workers}',
        )
        check(
            'settings and imaging agree on the page-worker default',
            reloaded.repack_page_workers == cbz.DEFAULT_PAGE_WORKERS,
            f'settings {reloaded.repack_page_workers}, cbz {cbz.DEFAULT_PAGE_WORKERS}',
        )
        check(
            'REPACK_WORKERS defaults to 2',
            reloaded.repack_workers == 2,
            f'{reloaded.repack_workers}',
        )
    importlib.reload(settings_mod)

    want = min(8, max(2, (_os.cpu_count() or 4) // 2))
    check(
        'libvips threads are set from the core count',
        pyvips.concurrency_get() == want,
        f'cpu_count={_os.cpu_count()} wanted {want}, libvips reports {pyvips.concurrency_get()}',
    )
    check(
        'and it is applied by the pipeline, not by the image',
        'concurrency_set' in inspect.getsource(pipeline),
        'nothing in pipeline.py calls concurrency_set',
    )
    for cores, expect in ((1, 2), (2, 2), (4, 2), (8, 4), (16, 8), (32, 8), (128, 8)):
        got = pipeline.vips_threads(cores)
        check(f'{cores} cores would give {expect} threads', got == expect, f'got {got}')


def check_png_compression() -> None:
    from inksetter.cache import render_key as _rk

    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']
    check('default png_compression is 7', prof.png_compression == 7, str(prof.png_compression))

    for bad in (-1, 10):
        try:
            profiles.validate(dataclasses.replace(prof, png_compression=bad), 'test')
            check(f'png_compression={bad} rejected', False, 'validate accepted it')
        except ValueError as exc:
            check(f'png_compression={bad} rejected', 'png_compression' in str(exc), str(exc)[:60])

    check(
        'png_compression reaches the cache key',
        _rk('http://u/p', prof, None) != _rk('http://u/p', dataclasses.replace(prof, png_compression=9), None),
    )

    page = fake_page(3, 1200, 1840)
    for label, base in (('png4', prof), ('png8', dataclasses.replace(prof, fmt='png8'))):
        lo, _ = pipeline.render_page(page, dataclasses.replace(base, png_compression=1))
        hi, _ = pipeline.render_page(page, dataclasses.replace(base, png_compression=9))
        check(f'png_compression changes {label} output size', len(lo) > len(hi), f'{len(lo)} vs {len(hi)}')
        a = pyvips.Image.new_from_buffer(lo, '')
        b = pyvips.Image.new_from_buffer(hi, '')
        ident = np.array_equal(
            np.ndarray(buffer=a.write_to_memory(), dtype=np.uint8, shape=(a.height, a.width, a.bands)),
            np.ndarray(buffer=b.write_to_memory(), dtype=np.uint8, shape=(b.height, b.width, b.bands)),
        )
        check(f'png_compression leaves {label} pixels untouched', ident)


def check_range_repack() -> None:
    import io as _io
    import zipfile as _zf

    from inksetter.opds.upstream import open_range

    base = 'http://127.0.0.1:8899/opds/v1.2'
    hdrs = {}

    before = dict(_RANGED)
    reader = open_range(f'{base}/ranged/file.cbz', hdrs)
    check('a range-serving origin yields a reader', reader is not None)
    if reader is None:
        return
    whole = _RANGED_BODY
    check('it knows the total length', reader._size == len(whole), f'{reader._size} vs {len(whole)}')
    check('it reads from the front', reader.read(4) == whole[:4], f'{reader.read!r}')
    reader.seek(-64, 2)
    check('it reads from the end, which is where a zip directory lives', reader.read(64) == whole[-64:])
    reader.seek(11)
    check('it honours an absolute seek', reader.read(9) == whole[11:20])
    reader.seek(5, 1)
    check('it honours a relative seek', reader.read(5) == whole[25:30])
    check('reading past the end returns nothing', (reader.seek(0, 2), reader.read(16))[1] == b'')

    z = _zf.ZipFile(reader)
    check(
        'zipfile reads the directory straight off the origin',
        sorted(z.namelist()) == sorted(_zf.ZipFile(_io.BytesIO(whole)).namelist()),
        f'{sorted(z.namelist())}',
    )
    check(
        'and an entry read that way is byte-identical',
        z.read('p001.jpg') == _zf.ZipFile(_io.BytesIO(whole)).read('p001.jpg'),
    )
    fetched = _RANGED['bytes'] - before['bytes']
    check(
        'the whole file was never downloaded to do it',
        fetched < len(whole),
        f'{fetched} of {len(whole)} bytes fetched in {_RANGED["requests"] - before["requests"]} requests',
    )
    reader.close()
    check('closing is idempotent', (reader.close(), reader.closed)[1] is True)

    check(
        'an origin that ignores Range yields no reader',
        open_range(f'{base}/norange/file.cbz', hdrs) is None,
    )
    check(
        'and neither does one that is not there',
        open_range(f'{base}/opds/v1.2/nothing-here.cbz', hdrs) is None,
    )

    check(
        'a host outside the allow-list is refused a reader',
        open_range('http://127.0.0.2:8899/opds/v1.2/ranged/file.cbz', hdrs) is None,
    )
    check(
        'a 200 dressed up with a content-range is not a partial response',
        open_range(f'{base}/liar/file.cbz', hdrs) is None,
    )

    _BIG['yielded'] = 0
    check(
        'an origin that ignores Range still yields no reader',
        open_range(f'{base}/norange-big/file.cbz', hdrs) is None,
    )
    time.sleep(0.3)
    pulled = _BIG['yielded']
    check(
        'and the probe did not drag the whole file across to say so',
        pulled < _BIG_TOTAL // 4,
        f'{pulled / 1024**2:.1f} MB of {_BIG_TOTAL / 1024**2:.0f} MB pulled',
    )

    r2 = open_range(f'{base}/ranged/file.cbz', hdrs)
    check('a reader for the bounds checks', r2 is not None)
    if r2 is not None:
        r2.seek(r2._size - 100)
        try:
            tail = r2.read(4 << 20)
            over = None
        except Exception as exc:
            tail, over = None, f'{type(exc).__name__}: {exc}'
        check(
            'a read longer than what is left stops at the end',
            tail == whole[-100:],
            over or f'{len(tail or b"")} bytes',
        )
        r2.seek(0, 2)
        check('and a read at the very end asks for nothing', r2.read(4 << 20) == b'')
        r2.close()


def check_repack_streaming() -> None:
    import io as _io
    import zipfile as _zf

    from inksetter.imaging import cbz as _cbz

    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']

    src = _io.BytesIO()
    with _zf.ZipFile(src, 'w') as z:
        z.writestr('ComicInfo.xml', '<ComicInfo/>')
        z.writestr('001.jpg', fake_page(1, 900, 1300))
        z.writestr('002.jpg', fake_page(2, 900, 1300))
        z.writestr('bad.jpg', b'not an image at all')
    raw = src.getvalue()

    for label, args, kwargs in (('not a zip', (_io.BytesIO(b'PK\x03\x04' + b'\x00' * 200), prof), {}),):
        try:
            _cbz.repack_iter(*args, **kwargs)
            check(f'repack_iter rejects {label} up front', False, 'returned an iterator')
        except Exception as exc:
            check(f'repack_iter rejects {label} up front', True, type(exc).__name__)

    streamed = b''.join(_cbz.repack_iter(_io.BytesIO(raw), prof))
    sz = _zf.ZipFile(_io.BytesIO(streamed))
    check('streamed archive is a valid zip', sz.testzip() is None)
    expected = [
        ('0001.png', pipeline.render_page(fake_page(1, 900, 1300), prof)[0]),
        ('0002.png', pipeline.render_page(fake_page(2, 900, 1300), prof)[0]),
        ('0003.jpg', b'not an image at all'),
        ('ComicInfo.xml', b'<ComicInfo/>'),
    ]
    check(
        'streamed archive holds exactly the expected entries, in reading order',
        sz.namelist() == [nm for nm, _ in expected],
        f'{sz.namelist()}',
    )
    wrong = [nm for nm, want in expected if nm in sz.namelist() and sz.read(nm) != want]
    check('every streamed entry is byte-for-byte the rendered page', not wrong, f'differ: {wrong}')

    it = _cbz.repack_iter(_io.BytesIO(raw), prof)
    first = next(it)
    check('first chunk arrives before the archive is finished', len(first) > 0, f'{len(first)} bytes')
    rest = b''.join(it)
    check(
        'first chunk is a prefix of the whole archive',
        streamed.startswith(first[:4]) and len(first) < len(first + rest),
    )

    it2 = _cbz.repack_iter(_io.BytesIO(raw), prof)
    next(it2)
    it2.close()
    check('closing a streamed repack early is clean', True)


async def check_repack_slot() -> None:
    import io as _io
    import zipfile as _zf

    from inksetter import app as _app
    from inksetter.imaging import cbz as _cbz

    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']
    src = _io.BytesIO()
    with _zf.ZipFile(src, 'w') as z:
        for i in range(6):
            z.writestr(f'{i:03d}.jpg', fake_page(i, 700, 1000))
    raw = src.getvalue()

    def free_slots() -> int:
        taken = 0
        while _app.repack_slots.acquire(blocking=False):
            taken += 1
        for _ in range(taken):
            _app.repack_slots.release()
        return taken

    budget = free_slots()
    check('repack slots start free', budget >= 1, f'{budget} free')

    chunks = _cbz.repack_iter(_io.BytesIO(raw), prof)
    _app._next_block(chunks)
    check('slot is free after one repack step', free_slots() == budget, f'{free_slots()} of {budget}')
    chunks.close()

    body = _io.BytesIO(raw)
    gen = _app._repacking(_cbz.repack_iter(_io.BytesIO(raw), prof), body)
    first = await gen.__anext__()
    check('streamed response yields before it finishes', len(first) > 0, f'{len(first)} bytes')
    await gen.aclose()
    check(
        'slot is free after the reader hangs up mid-repack',
        free_slots() == budget,
        f'{free_slots()} of {budget}',
    )
    check('spool is closed after the reader hangs up', body.closed)

    body2 = _io.BytesIO(raw)
    gen2 = _app._repacking(_cbz.repack_iter(_io.BytesIO(raw), prof), body2)
    whole = b''.join([blk async for blk in gen2])
    check('fully drained stream is a valid zip', _zf.ZipFile(_io.BytesIO(whole)).testzip() is None)
    check('slot is free after a completed repack', free_slots() == budget, f'{free_slots()} of {budget}')
    check('spool is closed after a completed repack', body2.closed)


def check_every_profile_geometry() -> None:
    fixture = fake_page(7, 1600, 2400)
    wrong, levels_wrong = [], []
    for name, prof in sorted(profiles.PROFILES.items()):
        if prof.fmt == 'raw':
            continue
        blob, _ = pipeline.render_page(fixture, prof)
        im = pyvips.Image.new_from_buffer(blob, '')
        if prof.fit == 'width':
            want_h = round(2400 * prof.width / 1600)
            if im.width != prof.width or abs(im.height - want_h) > 1:
                wrong.append(f'{name} wanted {prof.width}x{want_h} got {im.width}x{im.height}')
        elif (im.width, im.height) != (prof.width, prof.height):
            wrong.append(f'{name} wanted {prof.width}x{prof.height} got {im.width}x{im.height}')
        if prof.fmt == 'png4':
            grey = np.ndarray(
                buffer=im.colourspace('b-w').write_to_memory(),
                dtype=np.uint8,
                shape=(im.height, im.width),
            )
            if len(np.unique(grey)) > 16:
                levels_wrong.append(f'{name}: {len(np.unique(grey))} levels')

    check(
        'every profile renders at exactly its own geometry',
        not wrong,
        '; '.join(wrong) if wrong else f'{len(profiles.PROFILES)} profiles',
    )
    check('every png4 profile stays within 16 levels', not levels_wrong, '; '.join(levels_wrong))

    families = ('kobo-clara', 'kobo-libra', 'kindle-pw', 'kindle-oasis', 'kindle-scribe')
    bare = [f for f in families if f in profiles.PROFILES]
    check('no ambiguous family name is exposed', not bare, f'bare: {bare}')


def check_fft_backend() -> None:
    import numpy.fft as _npfft
    import scipy.fft as _spfft

    check(
        'the pipeline transforms are scipy.fft',
        pipeline.sfft is _spfft,
        getattr(pipeline.sfft, '__name__', repr(pipeline.sfft)),
    )
    check('numpy.fft is not used for transforms', 'np.fft.rfft2' not in _pipeline_source())

    rng = np.random.default_rng(7)
    plane = (rng.random((1200, 900), dtype=np.float32) * 255.0).astype(np.float32)
    back = _spfft.irfft2(_spfft.rfft2(plane), s=plane.shape)
    err = float(np.abs(back - plane).max())
    check('a single-precision round trip stays under a grey level', err < 1.0, f'max {err:.4f}')

    ours = _spfft.irfft2(_spfft.rfft2(plane), s=plane.shape)
    theirs = _npfft.irfft2(_npfft.rfft2(plane), s=plane.shape)
    gap = float(np.abs(ours - theirs).max())
    check('scipy and numpy agree within a grey level', gap < 1.0, f'max {gap:.4f}')

    prof = profiles.PROFILES['kindle-colorsoft']
    notched = pipeline._descreen_plane(plane.copy(), 0.2, prof)
    check(
        'the notch still returns a usable plane',
        notched is None or (notched.shape == plane.shape and np.isfinite(notched).all()),
        'None (no peak)' if notched is None else f'{notched.shape}',
    )


def _pipeline_source() -> str:
    import inspect

    return inspect.getsource(pipeline)


def check_fft_padding() -> None:
    fast = pipeline._next_fast_len
    check('a 7-smooth length is left alone', fast(1440) == 1440 and fast(1920) == 1920)
    check('a large prime is padded away', fast(1986) == 2000 and fast(2648) == 2688, f'{fast(1986)}, {fast(2648)}')
    check('the padded length is never smaller', all(fast(k) >= k for k in range(1000, 1100)))
    worst = max(fast(k) / k for k in range(1000, 3000))
    check('padding never costs much', worst < 1.05, f'worst overshoot {worst:.3f}x')

    prof = profiles.PROFILES['kindle-scribe-colorsoft']
    src = pyvips.Image.new_from_buffer(fake_page(5, 900, 1200), '').colourspace('b-w')
    plane = src.thumbnail_image(prof.width, height=prof.height, size='force').cast('float')

    real = pipeline._next_fast_len
    try:
        got = pipeline._to_numpy(pipeline.defringe(plane, prof).cast('uchar')).astype(int)
        pipeline._next_fast_len = lambda k, limit=7: k  # noqa: ARG005
        pipeline._diagonal_attenuation.cache_clear()
        ref = pipeline._to_numpy(pipeline.defringe(plane, prof).cast('uchar')).astype(int)
    finally:
        pipeline._next_fast_len = real
        pipeline._diagonal_attenuation.cache_clear()
    d = np.abs(got - ref)
    check(
        'padded defringe matches unpadded within a level',
        d.max() <= 2,
        f'max {d.max()}, mean {d.mean():.4f}, pixels differing {100 * (d > 0).mean():.3f}%',
    )

    mono = profiles.PROFILES['kobo-clara-hd-2e-bw']
    page = fake_page(9, 2400, 3200)
    a = pipeline.render_page(page, mono)[0]
    pipeline._next_fast_len = lambda k, limit=7: k  # noqa: ARG005
    try:
        b = pipeline.render_page(page, mono)[0]
    finally:
        pipeline._next_fast_len = real
    check('descreen is not padded', a == b, f'{len(a)} vs {len(b)} bytes')


def check_big_panel_cap() -> None:
    raised = {n for n, p in profiles.PROFILES.items() if p.upscale_max > 2.0}
    check(
        'only the Scribe-class panels raise the cap',
        raised == {'kindle-scribe-1-2', 'kindle-scribe-3', 'kindle-scribe-colorsoft'},
        f'{sorted(raised)}',
    )
    check(
        'the smaller panels keep the conservative default',
        profiles.PROFILES['kobo-clara-hd-2e-bw'].upscale_max == 2.0
        and profiles.PROFILES['kindle-colorsoft'].upscale_max == 2.0,
    )
    ksc = profiles.PROFILES['kindle-scribe-colorsoft']
    check(
        'a chained base keeps the Kaleido fields as well as the cap',
        (ksc.upscale_max, ksc.panel, ksc.defringe, ksc.fmt) == (2.2, 'kaleido', 'diagonal', 'pngc'),
        f'{ksc.upscale_max} {ksc.panel} {ksc.defringe} {ksc.fmt}',
    )
    small = fake_page(4, 760, 1200)
    for name, want_capped in (('kindle-scribe-3', 2.2), ('kobo-clara-hd-2e-bw', None)):
        prof = profiles.PROFILES[name]
        g = pipeline._geometry(small, prof, prof.width, prof.height, True)
        if want_capped:
            check(
                f'{name} enlarges up to the raised cap',
                abs(g.upscale - want_capped) < 0.01,
                f'upscale {g.upscale:.3f}',
            )
        else:
            check(f'{name} is unaffected by the cap', g.upscale < 2.0, f'upscale {g.upscale:.3f}')


def check_profile_source() -> None:
    shipped = importlib.resources.files('inksetter.imaging').joinpath('profiles.toml')
    check('shipped profiles.toml travels with the package', shipped.is_file())
    check(
        'every profile comes from the TOML, none from Python',
        not hasattr(profiles, '_BUILTIN') and not hasattr(profiles, '_KALEIDO'),
    )
    check(
        'the override feature is gone',
        not hasattr(profiles, '_override_path') and 'PROFILES_FILE' not in os.environ,
    )

    cwd = pathlib.Path.cwd()
    stray = cwd / 'profiles.toml'
    before = dict(profiles.PROFILES)
    wrote_stray = False
    try:
        if not stray.exists():
            stray.write_text('[profiles.kobo-clara-hd-2e-bw]\nwidth = 1\nheight = 1\n', encoding='utf-8')
            wrote_stray = True
        os.environ['PROFILES_FILE'] = str(stray)
        importlib.reload(profiles)
        check(
            'PROFILES_FILE is ignored',
            profiles.PROFILES['kobo-clara-hd-2e-bw'].width == before['kobo-clara-hd-2e-bw'].width,
            f'width={profiles.PROFILES["kobo-clara-hd-2e-bw"].width}',
        )
        check('a stray profiles.toml in the cwd is ignored', set(profiles.PROFILES) == set(before))
    finally:
        os.environ.pop('PROFILES_FILE', None)
        if wrote_stray:
            stray.unlink(missing_ok=True)
        importlib.reload(profiles)

    def build(body: str):
        try:
            return profiles._build(body, 'test.toml'), None
        except ValueError as exc:
            return None, exc

    got, err = build(
        '[mixins.tiny]\nwidth = 600\nheight = 800\n'
        '[profiles.a]\nbase = "tiny"\ngamma = 1.3\n'
        '[profiles.b]\nbase = "a"\nusm_amount = 0.7\n'
    )
    if err:
        check('mixins and base chains resolve', False, str(err)[:70])
    else:
        check('a mixin supplies fields', got['a'].width == 600 and got['a'].gamma == 1.3, f'{got["a"].width}')
        check(
            'a base chain resolves through the mixin',
            (got['b'].width, got['b'].gamma, got['b'].usm_amount) == (600, 1.3, 0.7),
            f'{got["b"].width} {got["b"].gamma} {got["b"].usm_amount}',
        )
        check('a mixin is not itself a profile', 'tiny' not in got, f'{sorted(got)}')

    table = profiles._build(shipped.read_text(encoding='utf-8'), 'shipped')
    cc = table['kobo-clara-colour']
    check(
        'a shipped Kaleido profile inherits the mixin',
        (cc.panel, cc.fmt, cc.defringe, cc.saturation) == ('kaleido', 'pngc', 'diagonal', 1.45),
        f'panel={cc.panel} fmt={cc.fmt} defringe={cc.defringe} sat={cc.saturation}',
    )
    check(
        'covers are built from the table, not from Python',
        (profiles.COVER.name, profiles.COVER_COLOUR.fmt) == ('cover', 'jpegc'),
    )
    check(
        'covers stay off the catalog list',
        'cover' not in profiles.PROFILES and 'cover-colour' not in profiles.PROFILES,
    )

    for label, body, needle in (
        ('base cycle', '[profiles.a]\nbase = "b"\n[profiles.b]\nbase = "a"\n', 'cycle'),
        ('unknown base', '[profiles.x]\nwidth = 9\nheight = 9\nbase = "nope"\n', 'unknown base'),
        ('mistyped section', '[profile.kobo-clara-hd-2e-bw]\ngamma = 1.2\n', 'unknown section'),
        ('name in both tables', '[mixins.dup]\ngamma = 1.1\n[profiles.dup]\nwidth = 9\nheight = 9\n', 'both'),
        ('profile with no size', '[profiles.sizeless]\ngamma = 1.1\n', 'missing'),
        ('profiles is a scalar', 'profiles = 3\n', 'table of tables'),
        ('a profile is a scalar', '[profiles]\nx = 3\n', 'must be a table'),
    ):
        got, err = build(body)
        check(f'profiles.toml {label} rejected', err is not None and needle in str(err), str(err or '')[:64])

    importlib.reload(profiles)
    shipped_names = set(tomllib.loads(shipped.read_text(encoding='utf-8'))['profiles']) - {'cover', 'cover-colour'}
    check(
        'the loaded table is exactly the shipped devices',
        set(profiles.PROFILES) == shipped_names,
        f'{len(profiles.PROFILES)} loaded, {len(shipped_names)} shipped',
    )


LF = chr(10)


def _strip_cbz(heights, width=800, gutter=None, noise=0) -> bytes:
    gutters = [gutter] if isinstance(gutter, tuple) else list(gutter or [])
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as z:
        z.writestr('ComicInfo.xml', '<ComicInfo/>')
        for i, h in enumerate(heights):
            yy, xx = np.mgrid[0:h, 0:width]
            rgb = np.full((h, width, 3), (240, 238, 232), np.uint8)
            rgb[(yy // 5 + xx // 5) % 3 == 0] = (40 + (i * 7) % 180, 90, 200 - (i * 5) % 150)
            rgb[((yy % 97) < 3) & (xx < width * 3 // 4)] = (25, 25, 30)
            for g in gutters:
                at, row = g[0], g[1]
                depth = g[2] if len(g) > 2 else 24
                colour = np.array(g[3] if len(g) > 3 else (255, 255, 255), np.int16)
                grain = g[4] if len(g) > 4 else noise
                if at == i:
                    span = rgb[row : row + depth]
                    jitter = np.random.default_rng(i).integers(0, grain + 1, span.shape)
                    span[:] = np.clip(colour - jitter, 0, 255).astype(np.uint8)
            z.writestr(
                f'{i:04d}.png', pyvips.Image.new_from_memory(rgb.tobytes(), width, h, 3, 'uchar').pngsave_buffer()
            )
    return buf.getvalue()


def _pages_of(blob: bytes):
    z = zipfile.ZipFile(io.BytesIO(blob))
    out = []
    for n in sorted(z.namelist()):
        if n.lower().endswith(('.png', '.jpg')):
            im = pyvips.Image.new_from_buffer(z.read(n), '')
            out.append((n, im.width, im.height))
    return out


def _panel_strip(*segments, width=1272, seed=7):
    rng = np.random.default_rng(seed)
    parts = []
    for kind, n in segments:
        if kind == 'art':
            parts.append(rng.integers(0, 256, (n, width, 3), dtype=np.uint8))
        elif kind == 'white':
            parts.append(np.full((n, width, 3), 255, np.uint8))
        elif kind == 'grey':
            parts.append(np.full((n, width, 3), 154, np.uint8))
        elif kind == 'bubble':
            b = np.full((n, width, 3), 255, np.uint8)
            b[:, 387:391] = 20
            b[:, 1203:1208] = 20
            parts.append(b)
    return np.concatenate(parts)


def check_seam_defects() -> None:
    p = profiles.PROFILES['kindle-colorsoft-webtoon']
    L = round(p.width * p.aspect)

    row = _panel_strip(('bubble', 1))
    check(
        'the test bubble reproduces the trap: invisible to every-8th-column sampling',
        int(row[:, ::8].max()) - int(row[:, ::8].min()) == 0,
    )
    check(
        'but its outline is seen at full resolution, so it is not quiet',
        int(cbz._spread(row)[0]) > cbz.STRIP_QUIET and not cbz._gutter(row)[0],
        f'spread {int(cbz._spread(row)[0])}',
    )
    first, bub, mid, gap = round(0.60 * L), round(0.03 * L), round(0.42 * L), round(0.10 * L)
    blk = _panel_strip(('art', first), ('bubble', bub), ('art', mid), ('white', gap), ('art', L))
    at = cbz._seam(blk, L)
    check(
        'a page is not cut through a bubble whose outline falls between samples',
        at == first + bub + mid,
        f'cut at {at}, bubble {first}..{first + bub}, gutter from {first + bub + mid}',
    )

    minrun = max(1, round(L * cbz.STRIP_GUTTER_MIN))
    short = 8
    check('an eight-row gap between lines is under the gutter minimum', short < minrun, f'minimum {minrun}')
    blk = _panel_strip(
        ('art', round(0.80 * L)), ('white', short), ('art', round(0.25 * L) - short), ('white', gap), ('art', L)
    )
    at = cbz._seam(blk, L)
    check(
        'a blank run too short to be a gutter - a gap between lines of text - is not cut in',
        at == round(0.80 * L) + round(0.25 * L),
        f'cut at {at}, short run of {short} at {round(0.80 * L)}, minimum {minrun}',
    )

    lo = int(L * cbz.STRIP_GUTTER_FLOOR)
    blk = _panel_strip(('art', lo - 150), ('white', 150 + short), ('art', 2 * L))
    at = cbz._seam(blk, L)
    check(
        'a gutter reaching only a few rows into the window is measured whole',
        at == lo + short - 1,
        f'cut at {at}, gutter {lo - 150}..{lo + short}, window from {lo}',
    )

    edge = round(1.05 * L)
    blk = _panel_strip(('art', edge), ('grey', 1), ('white', round(0.40 * L)), ('art', L))
    at = cbz._seam(blk, L)
    check(
        'a gutter that opens on a flat grey line is entered past the line',
        at == edge + 1,
        f'cut at {at}, grey line at {edge}, white from {edge + 1}',
    )
    blk = _panel_strip(('art', edge), ('grey', round(0.20 * L)), ('art', round(0.20 * L)), ('white', gap), ('art', L))
    check(
        'and a flat run with no blank page in it is still entered at its top',
        cbz._seam(blk, L) == edge,
        f'cut at {cbz._seam(blk, L)}, grey run from {edge}',
    )


def check_reslice() -> None:
    p = profiles.PROFILES['kindle-colorsoft-webtoon']
    limit = round(p.width * p.aspect)
    floor = int(limit * cbz.STRIP_MIN_FILL)
    check('the webtoon profiles ask to be re-cut', p.reslice and p.fit == 'width')

    heights = [1280, 1000, 1000, 1, 1280, 640, 1000]
    src = _strip_cbz(heights)
    pages = _pages_of(b''.join(cbz.repack_iter(io.BytesIO(src), p, 1)))

    check('a re-cut strip still produces pages', bool(pages), f'{len(pages)}')
    check(
        'every page is exactly the panel width', {w for _, w, _ in pages} == {p.width}, f'{ {w for _, w, _ in pages} }'
    )
    over = [h for _, _, h in pages if h > limit]
    check('and none is taller than the panel, so the zoom stays 1.0', not over, f'{over}')
    short = [h for _, _, h in pages[:-1] if h < floor]
    check('nor shorter than the fill floor, bar the last', not short, f'{short}')

    scaled = 0
    for i in range(len(heights)):
        one = pyvips.Image.new_from_buffer(zipfile.ZipFile(io.BytesIO(src)).read(f'{i:04d}.png'), '')
        scaled += pipeline.fit_to_width(one, p.width, p).height
    check(
        'every row of the strip comes out again, once',
        sum(h for _, _, h in pages) == scaled,
        f'{sum(h for _, _, h in pages)} out vs {scaled} in',
    )
    check('a one-pixel entry does not derail it', 1 in heights and len(pages) >= 3)
    check(
        'the page numbers are wide enough to sort past 9999',
        all(re.fullmatch(r'\d{5}\.(png|jpg)', n) for n, _, _ in pages),
        f'{pages[0][0]}',
    )
    check('non-image entries are still carried over', 'ComicInfo.xml' in zipfile.ZipFile(io.BytesIO(src)).namelist())

    scale = p.width / 800
    band, deep = 990, 24
    lo, hi = round(band * scale), round((band + deep) * scale)
    check('the gutter really is inside the window', floor <= lo and hi <= limit, f'{lo}..{hi} in {floor}..{limit}')
    gut = _strip_cbz([1280, 1280], gutter=(0, band))
    cut = _pages_of(b''.join(cbz.repack_iter(io.BytesIO(gut), p, 1)))[0][2]
    check(
        'the cut is drawn to a quiet row rather than the nominal one',
        lo - 6 <= cut <= hi + 6,
        f'cut at {cut}, gutter spans {lo}..{hi}, panel would be {limit}',
    )
    check(
        'and to the far side of it, so the page fills up',
        cut >= (lo + hi) // 2,
        f'cut at {cut}, gutter spans {lo}..{hi}',
    )

    high = _strip_cbz([1280, 1280], gutter=(0, 200))
    first = _pages_of(b''.join(cbz.repack_iter(io.BytesIO(high), p, 1)))[0][2]
    check(
        'a quiet row above the floor cannot drag the cut up to it',
        first >= floor,
        f'page came out {first} tall, floor is {floor}',
    )

    reach = round(limit * cbz.STRIP_OVERSHOOT)
    low = int(limit * cbz.STRIP_GUTTER_FLOOR)

    def at(frac):
        return round(frac * limit / scale)

    def band(frac):
        return round(at(frac) * scale), round((at(frac) + 24) * scale)

    def tiles(blob):
        z = zipfile.ZipFile(io.BytesIO(blob))
        order = sorted(z.namelist(), key=cbz.natural_key)
        return [len(t[3]) for t in cbz._strip_tiles(z, order, p, lambda _n: None) if t[0] == 'tile']

    lo60, hi60 = band(0.60)
    t = tiles(_strip_cbz([1280, 1280], gutter=(0, at(0.60))))
    check(
        'a gutter well above the old 85% floor now ends the page',
        lo60 - 4 <= t[0] <= hi60 + 4 and t[0] < floor,
        f'page {t[0]}, gutter {lo60}..{hi60}, old floor {floor}',
    )
    lo30, hi30 = band(0.30)
    t = tiles(_strip_cbz([1280, 1280], gutter=(0, at(0.30))))
    check(
        'but one below the gutter floor still does not',
        t[0] >= low and not (lo30 - 4 <= t[0] <= hi30 + 4),
        f'page {t[0]}, gutter {lo30}..{hi30}, gutter floor {low}',
    )
    lo105, _ = band(1.05)
    t = tiles(_strip_cbz([1280, 1280], gutter=(0, at(1.05))))
    check(
        'with nothing above the fold, a gutter just past it is taken',
        limit < t[0] <= reach and lo105 - 4 <= t[0] <= lo105 + 4,
        f'page {t[0]}, gutter starts {lo105}, panel {limit}, reach {reach}',
    )
    check(
        'no page is shrunk below half size',
        1 / cbz.STRIP_OVERSHOOT >= 0.50 - 1e-9,
        f'overshoot {cbz.STRIP_OVERSHOOT} shrinks to {1 / cbz.STRIP_OVERSHOOT:.0%}',
    )
    beyond = at(cbz.STRIP_OVERSHOOT + 0.10)
    t_far = tiles(_strip_cbz([1280, 1280], gutter=(0, beyond) if beyond < 1280 else (1, beyond - 1280)))
    check(
        'a gutter further past it than the overshoot is not',
        round(beyond * scale) > reach and t_far[0] <= limit,
        f'page {t_far[0]}, gutter at {round(beyond * scale)}, reach {reach}',
    )
    t = tiles(_strip_cbz([1280, 1280], gutter=[(0, at(0.60)), (0, at(1.05))]))
    check(
        'given a gutter either side of the fold, the unshrunk one wins',
        lo60 - 4 <= t[0] <= hi60 + 4,
        f'page {t[0]}',
    )

    t = tiles(_strip_cbz([1280, 1280], gutter=(0, at(0.60)), noise=12))
    check(
        'a gutter with a little JPEG noise in it still counts as one',
        lo60 - 4 <= t[0] <= hi60 + 4,
        f'page {t[0]}, gutter {lo60}..{hi60}',
    )
    lead_rows = round((limit + 4) / scale)
    t = tiles(_strip_cbz([lead_rows, 1280], gutter=(1, at(1.05) - lead_rows)))
    check(
        'a gutter past the fold is seen even when it is still in the next slice',
        limit < t[0] <= reach and lo105 - 4 <= t[0] <= lo105 + 4,
        f'page {t[0]}, gutter starts {lo105}, first slice ends at {round(lead_rows * scale)}',
    )
    t = tiles(_strip_cbz([at(1.05)]))
    check(
        'the strip running out is no reason to shrink its last page',
        len(t) == 2 and max(t) <= limit,
        f'{t}',
    )

    def strip_of(blob):
        z = zipfile.ZipFile(io.BytesIO(blob))
        parts = [cbz._strip_rows(z.read(n), p) for n in sorted(z.namelist(), key=cbz.natural_key) if cbz.is_image(n)]
        return np.concatenate([x for x in parts if x is not None])

    def cut(blob):
        z = zipfile.ZipFile(io.BytesIO(blob))
        order = sorted(z.namelist(), key=cbz.natural_key)
        return [t[3] for t in cbz._strip_tiles(z, order, p, lambda _n: None) if t[0] == 'tile']

    def blank_rows(rows):
        s = rows.astype(np.int16)
        lo, hi = s.min(axis=(1, 2)), s.max(axis=(1, 2))
        return (hi - lo <= cbz.STRIP_QUIET) & ((lo >= 235) | (hi <= 30))

    def only_gutter_gone(blob):
        """Walk the strip and the output together: whatever the output skips must be blank gutter."""
        strip, out = strip_of(blob), np.concatenate(cut(blob))
        blank = blank_rows(strip)
        i = j = gone = 0
        while j < len(out):
            if i == len(strip):
                return False, gone, 'output ran past the strip'
            if np.array_equal(strip[i], out[j]):
                i, j = i + 1, j + 1
            elif blank[i]:
                i, gone = i + 1, gone + 1
            else:
                return False, gone, f'artwork row {i} is missing'
        rest = blank[i:]
        return bool(rest.all()), gone + len(rest), 'artwork dropped off the end' if not rest.all() else ''

    margin = round(limit * cbz.STRIP_TOP_MARGIN)

    faint = _strip_cbz([1280, 1280], gutter=[(0, at(1.05), 60), (0, at(1.05) + 60, 80, (252, 252, 252), 16)])
    band = strip_of(faint)[round((at(1.05) + 64) * scale) : round((at(1.05) + 136) * scale)]
    light = band.min(axis=(1, 2)) >= 235
    check(
        'the faint band is light everywhere and blank nowhere, as intended',
        bool(light.all()) and not blank_rows(band).any(),
        f'{int(light.sum())}/{len(band)} light, {int(blank_rows(band).sum())} blank',
    )
    fp = cut(faint)
    kept = fp[1][margin:]
    check(
        'faint artwork under a gutter is not swallowed with it',
        len(kept) and not blank_rows(kept[:1]).any() and bool((kept[:40].min(axis=(1, 2)) >= 235).all()),
        f'row after the margin blank: {bool(blank_rows(kept[:1]).any())}',
    )

    everything = [
        _strip_cbz(heights),
        _strip_cbz([1280, 1280], gutter=(0, at(1.05))),
        _strip_cbz([1280, 1000, 1280, 640], gutter=[(0, at(1.05)), (2, 300)]),
        _strip_cbz([1280, 1280], gutter=(0, at(1.05), 150)),
        _strip_cbz([1500, 1280], gutter=[(0, 1120, 380), (1, 0, 100)]),
        faint,
    ]
    verdicts = [only_gutter_gone(b) for b in everything]
    check(
        'no row of artwork is ever lost; only blank gutter is dropped',
        all(ok for ok, _, _ in verdicts),
        '; '.join(f'fixture {k}: {why}' for k, (ok, _, why) in enumerate(verdicts) if not ok),
    )
    check(
        'and some gutter really was dropped, so that is not vacuous',
        sum(g for _, g, _ in verdicts) > 0,
        f'{[g for _, g, _ in verdicts]} rows',
    )
    tallest = max(len(t) for b in everything for t in cut(b))
    check('and no tile ever runs past the overshoot', tallest <= reach, f'tallest {tallest}, reach {reach}')

    def blank_at_top(tile):
        g = blank_rows(tile)
        return len(g) if g.all() else int(np.argmin(g))

    t = cut(_strip_cbz([1280, 1280], gutter=(0, at(1.05), 150)))
    check(
        'a page that would open on a deep gutter keeps only the margin of it',
        blank_at_top(t[1]) == margin,
        f'{blank_at_top(t[1])} blank rows at the top, margin {margin}',
    )
    t = cut(_strip_cbz([1500, 1280], gutter=[(0, 1120, 380), (1, 0, 12)]))
    check(
        'even when that gutter runs on into the next entry',
        blank_at_top(t[1]) == margin and round(12 * scale) < margin,
        f'{blank_at_top(t[1])} blank rows at the top, margin {margin}, second entry adds {round(12 * scale)}',
    )
    night = cut(_strip_cbz([1280, 1280], gutter=(0, at(1.05), 150, (6, 6, 8))))
    check(
        'black gutter is dropped the same as white',
        blank_at_top(night[1]) == margin,
        f'{blank_at_top(night[1])} blank rows at the top',
    )
    t = cut(_strip_cbz([1280, 1280], gutter=(0, 0, 150)))
    check(
        'the first page of the strip does not open on gutter either',
        blank_at_top(t[0]) == margin,
        f'{blank_at_top(t[0])} blank rows at the top',
    )
    t = cut(_strip_cbz([1280, 1280], gutter=(0, at(0.60))))
    check(
        'a gutter shorter than the margin is left as it is',
        0 < blank_at_top(t[1]) <= margin,
        f'{blank_at_top(t[1])} blank rows at the top',
    )
    grey = cut(_strip_cbz([1280, 1280], gutter=(0, at(1.05), 150, (128, 128, 128))))
    check(
        'a flat grey run at the top of a page is artwork, and stays',
        blank_at_top(grey[1]) == 0 and len(grey[1]) and cbz._spread(grey[1][:100]).max() <= cbz.STRIP_QUIET,
        f'{blank_at_top(grey[1])} blank rows at the top',
    )
    mid = cut(_strip_cbz([1280, 1280], gutter=(0, at(0.30), 60)))
    held = strip_of(_strip_cbz([1280, 1280], gutter=(0, at(0.30), 60)))
    check(
        'a gutter in the middle of a page is left whole',
        int(blank_rows(mid[0]).sum()) == int(blank_rows(held[: len(mid[0])]).sum()) > margin,
        f'{int(blank_rows(mid[0]).sum())} blank rows kept',
    )
    grey_line = [
        (0, at(1.05), 1, (154, 154, 154)),
        (0, at(1.05) + 1, 1280 - at(1.05) - 1),
        (1, 0, 400),
    ]
    g4 = cut(_strip_cbz([1280, 1280], gutter=grey_line))
    worst = max(float(blank_rows(t).mean()) for t in g4[:-1])
    check(
        'no page is left holding nothing but gutter',
        worst < 0.90,
        f'{len(g4)} pages, the blankest {worst:.0%} blank',
    )
    end = cut(_strip_cbz([1280, 800], gutter=(1, 400, 400)))
    check(
        'a strip that ends on gutter leaves no blank last page',
        not blank_rows(end[-1]).all(),
        f'{len(end)} pages, last {len(end[-1])} rows',
    )

    tall = zipfile.ZipFile(
        io.BytesIO(b''.join(cbz.repack_iter(io.BytesIO(_strip_cbz([1280, 1280], gutter=(0, at(1.05)))), p, 1)))
    )
    first = pyvips.Image.new_from_buffer(tall.read('00001.png'), '').colourspace('srgb')
    a = np.ndarray(buffer=first.write_to_memory(), dtype=np.uint8, shape=(first.height, first.width, first.bands))
    check(
        'a page that ran past the fold still ships at exactly the panel size',
        (first.width, first.height) == (p.width, limit),
        f'{first.width}x{first.height}',
    )

    def pad_width(cols, edge):
        n = 0
        for c in cols:
            if not (a[:, c] == a[:, edge]).all():
                break
            n += 1
        return n

    left = pad_width(range(first.width), 0)
    right = pad_width(range(first.width - 1, -1, -1), first.width - 1)
    content = first.width - left - right
    check(
        'shrunk to fit and padded, rather than cropped',
        left >= 10 and right >= 10 and abs(left - right) <= 2,
        f'pad {left} left, {right} right',
    )
    check(
        'by no more than the overshoot allows',
        content >= p.width / cbz.STRIP_OVERSHOOT - 2,
        f'content {content} px of {p.width}, floor {p.width / cbz.STRIP_OVERSHOOT:.0f}',
    )

    covered = sorted(
        n
        for n in zipfile.ZipFile(
            io.BytesIO(b''.join(cbz.repack_iter(io.BytesIO(src), p, 1, cover=fake_page(9, 800, 1200))))
        ).namelist()
        if n[0].isdigit()
    )
    check(
        'a prepended cover is numbered as wide as the pages after it',
        len({len(n.split('.')[0]) for n in covered}) == 1 and covered[0].startswith('00000.'),
        f'{covered[:3]}',
    )
    check(
        'so it sorts first by digits alone, not by a full stop outranking a 1',
        covered == sorted(covered, key=lambda n: int(n.split('.')[0])),
    )

    box = profiles.PROFILES['kindle-colorsoft']
    boxed = sorted(
        n
        for n in zipfile.ZipFile(
            io.BytesIO(b''.join(cbz.repack_iter(io.BytesIO(src), box, 1, cover=fake_page(9, 800, 1200))))
        ).namelist()
        if n[0].isdigit()
    )
    check('a paged profile still names its cover 0000', boxed[0].split('.')[0] == '0000', f'{boxed[:2]}')
    plain = _pages_of(b''.join(cbz.repack_iter(io.BytesIO(src), box, 1)))
    check(
        'a paged profile is left alone by all of this',
        len(plain) == len(heights) and all(re.fullmatch(r'\d{4}\.(png|jpg)', n) for n, _, _ in plain),
        f'{len(plain)} pages, first {plain[0][0]}',
    )
    off = dataclasses.replace(p, reslice=False)
    check(
        'and so is a strip profile with the re-cut turned off',
        len(_pages_of(b''.join(cbz.repack_iter(io.BytesIO(src), off, 1)))) == len(heights),
    )


def check_family_duplicates() -> None:
    def kin(a: str, b: str) -> bool:
        first, second = a.split('-'), b.split('-')
        shared = 0
        for x, y in zip(first, second, strict=False):
            if x != y:
                break
            shared += 1
        return shared >= 2

    def shape(p) -> tuple:
        return tuple(sorted((f.name, getattr(p, f.name)) for f in dataclasses.fields(p) if f.name != 'name'))

    names = sorted(profiles.PROFILES)
    twinned = [
        (a, b)
        for i, a in enumerate(names)
        for b in names[i + 1 :]
        if kin(a, b) and shape(profiles.PROFILES[a]) == shape(profiles.PROFILES[b])
    ]
    check(
        'the family test sees past a generation suffix',
        kin('kobo-elipsa', 'kobo-elipsa-2e')
        and kin('kindle-pw-3-4', 'kindle-pw-5')
        and not kin('kobo-clara-hd-2e-bw', 'kobo-glo-hd')
        and not kin('kobo-forma', 'kobo-sage'),
    )
    check(
        'no device family carries the same profile twice over',
        not twinned,
        '; '.join(f'{a} = {b}' for a, b in twinned),
    )
    subject = profiles.PROFILES[sorted(n for n, p in profiles.PROFILES.items() if p.fmt == 'png4')[0]]
    clone = dataclasses.replace(subject, name='some-other-device')
    page = fake_page(0, 900, 1200)
    check(
        'renaming a profile does not move a single pixel',
        pipeline.render_page(page, subject) == pipeline.render_page(page, clone),
    )
    merged = {
        'kindle-pw-3-4',
        'kindle-oasis-2-3',
        'kindle-basic-1-2',
        'kindle-scribe-1-2',
        'kobo-clara-hd-2e-bw',
        'kobo-libra-h2o-2',
        'kobo-elipsa',
    }
    check(
        'the generations that share a panel share a profile',
        merged <= set(profiles.PROFILES),
        f'missing {sorted(merged - set(profiles.PROFILES))}',
    )
    check(
        'and the models that only look alike kept their own',
        {'kobo-glo-hd', 'kobo-aura-one', 'kobo-forma', 'kobo-sage', 'kindle-voyage', 'kindle-oasis-1'}
        <= set(profiles.PROFILES),
    )
    check(
        'and the panels that only look alike stayed apart',
        profiles.PROFILES['kindle-pw-6'].panel != profiles.PROFILES['kindle-colorsoft'].panel
        and (profiles.PROFILES['kindle-pw-6'].width, profiles.PROFILES['kindle-pw-6'].height)
        == (profiles.PROFILES['kindle-colorsoft'].width, profiles.PROFILES['kindle-colorsoft'].height),
    )


def check_profile_config() -> None:
    for label, body, needle in (
        ('unknown base', 'base = "kobo-clarra"', 'unknown base'),
        ('unknown field', 'saturaton = 1.6', 'unknown field'),
        ('width = 0', 'width = 0', 'width=0'),
        ('gamma = 0', 'gamma = 0', 'gamma=0'),
        ('fmt typo', 'fmt = "png9"', "fmt='png9'"),
        ('bad dither', 'dither = "floyd"', "dither='floyd'"),
        ('inverted levels', 'black = 250\nwhite = 10', 'black < white'),
        ('bad png level', 'png_compression = 11', 'png_compression=11'),
        ('defringe without a CFA', 'defringe = "diagonal"', 'filter array'),
        ('palette needing 3 bits', 'palette_colours = 8', 'palette_colours=8'),
        ('palette needing 5 bits', 'palette_colours = 17', 'palette_colours=17'),
        ('palette needing 6 bits', 'palette_colours = 64', 'palette_colours=64'),
        ('palette needing 7 bits', 'palette_colours = 128', 'palette_colours=128'),
        ('negative blur radius', 'usm_sigma = -1.0', 'usm_sigma=-1.0'),
        ('zero blur radius', 'usm_sigma = 0.0', 'usm_sigma=0.0'),
        ('sharpening that blurs', 'usm_amount = -5.0', 'usm_amount=-5.0'),
        ('crop guard that never fires', 'autocrop_max_frac = 1.5', 'autocrop_max_frac=1.5'),
        ('crop guard that always fires', 'autocrop_max_frac = -0.5', 'autocrop_max_frac=-0.5'),
        ('ink threshold off the scale', 'autocrop_threshold = 300', 'autocrop_threshold=300'),
        ('negative ink threshold', 'autocrop_threshold = -20', 'autocrop_threshold=-20'),
        ('auto_mono that can never fire', 'mono_chroma_threshold = -1.0', 'mono_chroma_threshold=-1.0'),
        ('a descreen ceiling no box can hold', 'descreen_max_megapixels = 320', 'descreen_max_megapixels=320'),
    ):
        size = ''.join(f'{k} = {v}\n' for k, v in (('width', 1072), ('height', 1448)) if f'{k} =' not in body)
        body = f'[profiles.x]\n{size}{body}\n'
        try:
            profiles._build(body, 'test.toml')
            check(f'profiles.toml {label} rejected', False, 'no error raised')
        except ValueError as exc:
            check(f'profiles.toml {label} rejected', needle in str(exc), str(exc)[:64])

    body = '\n'.join(('[profiles.x]', 'width = 1072', 'height = 1448', 'usm_amount = 0.0', ''))
    for good in (2, 4, 16, 129, 256):
        ok = f'[profiles.x]{LF}width = 1072{LF}height = 1448{LF}palette_colours = {good}{LF}'
        try:
            profiles._build(ok, 'test.toml')
            check(f'palette_colours = {good} is accepted', True)
        except ValueError as exc:
            check(f'palette_colours = {good} is accepted', False, str(exc)[:56])

    try:
        off = profiles._build(body, 'test.toml')
        check('usm_amount = 0 still turns sharpening off', off['x'].usm_amount == 0.0)
    except ValueError as exc:
        check('usm_amount = 0 still turns sharpening off', False, str(exc)[:64])

    try:
        profiles._build(
            importlib.resources.files('inksetter.imaging').joinpath('profiles.toml').read_text(encoding='utf-8'),
            'shipped',
        )
        check('the shipped table passes its own validator', True)
    except ValueError as exc:
        check('the shipped table passes its own validator', False, str(exc)[:70])


PASS, FAIL = [], []


def _folio_page(number: str = '123', attached: bool = False, w: int = 900) -> bytes:
    h = w * 13 // 9
    a = np.full((h, w), 255, np.uint8)
    k = w / 900
    sc = lambda *v: tuple(int(x * k) for x in v)  # noqa: E731
    a[sc(60)[0] : sc(1130)[0], sc(60)[0] : sc(840)[0]] = 245
    for y0, y1, x0, x1 in ((60, 66, 60, 840), (1124, 1130, 60, 840), (60, 1130, 60, 66), (60, 1130, 834, 840)):
        y0, y1, x0, x1 = sc(y0, y1, x0, x1)
        a[y0:y1, x0:x1] = 20
    a[sc(300)[0] : sc(700)[0], sc(200)[0] : sc(640)[0]] = 90
    if number:
        t = pyvips.Image.text(number, dpi=300, font='DejaVu Sans')
        g = 255 - np.ndarray(buffer=t.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(t.height, t.width))
        top = int(h * (1.0 - folio.BAND))
        y = top + 2 if attached else top + int(h * folio.BAND * 0.45)
        x = (w - t.width) // 2
        a[y : y + t.height, x : x + t.width] = np.minimum(a[y : y + t.height, x : x + t.width], g)
        if attached:
            a[y - 8 : y - 2, x + 6 : x + 26] = 20
    return pyvips.Image.new_from_memory(a.tobytes(), w, h, 1, 'uchar').pngsave_buffer()


def _paged() -> dict:
    return {n: p for n, p in profiles.PROFILES.items() if p.fit != 'width'}


def check_strip_folio() -> None:
    off = dataclasses.replace(profiles.PROFILES['kobo-clara-hd-2e-bw'], strip_folio=False)
    on = dataclasses.replace(off, strip_folio=True)
    numbered, blank = _folio_page(), _folio_page(number='')
    attached = _folio_page(attached=True)

    paged = {n: p for n, p in profiles.PROFILES.items() if p.fit != 'width'}
    check(
        'every paged device profile strips folios',
        all(p.strip_folio for p in paged.values()),
        f'off: {[n for n, p in paged.items() if not p.strip_folio]}',
    )
    check(
        'and the webtoon profiles do not',
        not any(p.strip_folio for n, p in profiles.PROFILES.items() if p.fit == 'width'),
    )
    check(
        'cover profiles do not',
        not profiles.COVER.strip_folio and not profiles.COVER_COLOUR.strip_folio,
        f'cover={profiles.COVER.strip_folio} colour={profiles.COVER_COLOUR.strip_folio}',
    )
    _dev = profiles.PROFILES['kobo-clara-hd-2e-bw']
    _emb = profiles.embedded_cover_for(_dev)
    check(
        'nor the cover prepended into an archive',
        _dev.strip_folio and not _emb.strip_folio,
        f'device={_dev.strip_folio} embedded={_emb.strip_folio}',
    )
    check(
        'and lifting the cap still works alongside it',
        _emb.upscale_max == profiles.UPSCALE_MAX_CEILING,
        f'upscale_max={_emb.upscale_max}',
    )
    before_key = cache_mod.render_key('u', profiles.PROFILES['kobo-clara-hd-2e-bw'], None)
    os.environ['OCR_ENABLED'] = 'false'
    try:
        importlib.reload(profiles)
        check(
            'OCR_ENABLED=false turns the pass off for every profile',
            not any(p.strip_folio for p in _paged().values()),
            f'still on: {[n for n, p in profiles.PROFILES.items() if p.strip_folio]}',
        )
        check(
            'and that reaches the cache key, so nothing is re-rendered',
            cache_mod.render_key('u', profiles.PROFILES['kobo-clara-hd-2e-bw'], None) != before_key,
        )
        for junk in ('typo', 'yes', ''):
            os.environ['OCR_ENABLED'] = junk
            importlib.reload(profiles)
            if not all(p.strip_folio for p in _paged().values()):
                break
        else:
            junk = None
        check('only an explicit false/0/no/off disables it', junk is None, f'{junk!r} disabled it')
    finally:
        os.environ.pop('OCR_ENABLED', None)
        importlib.reload(profiles)
    check(
        'and the table comes back',
        all(p.strip_folio for p in _paged().values()),
    )
    check(
        'a folio-free page is untouched by the strip',
        pipeline.render_page(blank, on)[0] == pipeline.render_page(blank, off)[0],
    )
    erased = pipeline.render_page(numbered, on)[0]
    kept = pipeline.render_page(numbered, off)[0]
    check('erasing the folio changes the page', erased != kept)
    check(
        'a page with its folio erased renders as one that never had it',
        erased == pipeline.render_page(blank, on)[0],
    )
    raw = pyvips.Image.new_from_buffer(numbered, '')
    box_off = pipeline._autocrop_box(raw, off)
    box_on = pipeline._autocrop_box(folio.strip(raw, raw, off.autocrop_threshold), off)
    check(
        'the trim box tightens once the folio is gone',
        box_on is not None and box_off is not None and box_on[3] < box_off[3],
        f'{box_off[3] if box_off else None} -> {box_on[3] if box_on else None} px tall',
    )
    check(
        'a folio touching artwork is left alone',
        pipeline.render_page(attached, on)[0] == pipeline.render_page(attached, off)[0],
    )
    check(
        'a folio-shaped mark reading as a lone zero is refused',
        pipeline.render_page(_folio_page(number='0'), on)[0] == pipeline.render_page(_folio_page(number='0'), off)[0],
    )
    check(
        'a word in the margin is not erased',
        pipeline.render_page(_folio_page(number='END'), on)[0]
        == pipeline.render_page(_folio_page(number='END'), off)[0],
    )
    crowded = pyvips.Image.new_from_buffer(_folio_page(), '')
    ca = np.ndarray(
        buffer=crowded.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(crowded.height, crowded.width)
    )
    band_top = int(crowded.height * (1.0 - folio.BAND))
    for k in range(5):
        y = band_top + 6 + k * 14
        ca[y : y + 10, 120 + k * 90 : 134 + k * 90] = 20
    crowded_png = pyvips.Image.new_from_memory(ca.tobytes(), crowded.width, crowded.height, 1, 'uchar').pngsave_buffer()
    seen_marks = folio._marks((255 - ca.astype(np.int16)) > off.autocrop_threshold, crowded.height, crowded.width)
    reads = []
    real_read = folio._read

    def counting_read(luma_, mark, h_, w_):
        reads.append(mark)
        return real_read(luma_, mark, h_, w_)

    folio._read = counting_read
    try:
        out = pipeline.render_page(crowded_png, on)[0]
    finally:
        folio._read = real_read
    check(
        'a crowded band offers more marks than the reader is asked about',
        len(seen_marks) > folio.MAX_READS,
        f'{len(seen_marks)} marks, MAX_READS={folio.MAX_READS}',
    )
    check(
        'no more than MAX_READS reads, lowest mark first',
        len(reads) <= folio.MAX_READS and reads and reads[0][3] == max(m[3] for m in seen_marks),
        f'{len(reads)} reads',
    )
    check(
        'and the folio is still erased despite the decoys',
        out != pipeline.render_page(crowded_png, off)[0],
    )
    _seen: list = []
    _real_marks = folio._marks

    def _capturing_marks(ink, h_, w_):
        _seen.append(ink)
        return _real_marks(ink, h_, w_)

    folio._marks = _capturing_marks
    try:
        for _thr in (0, 1, 11, 12, 13, 254):
            _seen.clear()
            folio.strip(pyvips.Image.new_from_buffer(numbered, ''), pyvips.Image.new_from_buffer(numbered, ''), _thr)
            _page = pyvips.Image.new_from_buffer(numbered, '')
            _pa = np.ndarray(
                buffer=_page.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(_page.height, _page.width)
            )
            _want = (255 - _pa.astype(np.int16)) > _thr
            if not (_seen and np.array_equal(_seen[0], _want)):
                break
        else:
            _thr = None
        check('the gate thresholds exactly as (255 - a) > threshold did', _thr is None, f'differs at threshold={_thr}')
    finally:
        folio._marks = _real_marks

    _wide_png = _folio_page(w=2400)
    _widths: list = []
    _real_engine = folio._reader()

    def _measuring_engine(img, **kw):
        _widths.append(img.shape[1])
        return _real_engine(img, **kw)

    folio._engine = _measuring_engine
    try:
        pipeline.render_page(_wide_png, on)
    finally:
        folio._engine = _real_engine
    check(
        'the reader is handed a narrow strip, not a fifth of the page',
        _widths and max(_widths) <= 480,
        f'crop {max(_widths) if _widths else 0} px wide from a 2400 px page',
    )

    raw3 = pyvips.Image.new_from_buffer(_folio_page(number='123'), '')
    a3 = np.ndarray(buffer=raw3.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(raw3.height, raw3.width))
    marks = folio._marks((255 - a3.astype(np.int16)) > off.autocrop_threshold, raw3.height, raw3.width)
    check('a three-digit folio is offered to the reader as one mark', len(marks) == 1, f'{len(marks)} marks')
    speck = _folio_page(number='')
    im = pyvips.Image.new_from_buffer(speck, '')
    a = np.ndarray(buffer=im.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(im.height, im.width))
    a[1250:1268, 430:448] = 20
    blob = pyvips.Image.new_from_memory(a.tobytes(), im.width, im.height, 1, 'uchar').pngsave_buffer()
    check(
        'an isolated blob that reads as nothing is not erased',
        pipeline.render_page(blob, on)[0] == pipeline.render_page(blob, off)[0],
    )


def _decoded(token: str) -> tuple[str, str | None]:
    try:
        return rewrite.decode_parts(token)
    except rewrite.TokenError:
        return '', None


async def check_no_stream(c) -> None:
    up = 'http://127.0.0.1:8899'
    strip = profiles.PROFILES['kindle-colorsoft-webtoon']
    check('premise: the webtoon profile is re-sliced', strip.resliced)
    check('a strip kept whole is not re-sliced', not dataclasses.replace(strip, reslice=False).resliced)
    check('a paged fit is never re-sliced, whatever it asks', not dataclasses.replace(strip, fit='box').resliced)

    r = await c.get(f'/{strip.name}/catalog')
    feed = r.text
    check('a webtoon catalog is served', r.status_code == 200, f'status={r.status_code}')
    check('a webtoon feed offers no page stream', f'/{strip.name}/p/' not in feed and PSE_REL not in feed)
    check('nor the pse:count and pse:lastRead that go with it', 'pse:count' not in feed and 'pse:lastRead' not in feed)
    check('nor a stream placeholder', '{pageNumber}' not in feed and '{maxWidth}' not in feed)
    check(
        'the feed, download, cover and search links stay',
        all(f'/{strip.name}/{k}/' in feed for k in ('f', 'dl', 'img', 'osd')),
    )
    check('no upstream host leaks from a webtoon feed', up not in feed)

    token = encode_token(f'{up}/opds/v1.2/books/7/pages/{{pageNumber}}?zero_based=true&maxWidth={{maxWidth}}')
    kept = dataclasses.replace(strip, name='smoke-webtoon-kept', reslice=False)
    profiles.PROFILES[kept.name] = kept
    try:
        r = await c.get(f'/{kept.name}/catalog')
        found = re.search(rf'/{kept.name}/p/([\w-]+)\?page=', r.text)
        check('a width-fit profile that keeps the strip still streams', found is not None and 'pse:count="3"' in r.text)
        check(
            'premise: the refusal below uses the token the feed offers', found is not None and found.group(1) == token
        )
        r = await c.get(f'/{kept.name}/p/{token}', params={'page': 0, 'maxWidth': strip.width})
        check('and its stream serves a page', r.status_code == 200 and r.headers['content-type'].startswith('image/'))
    finally:
        del profiles.PROFILES[kept.name]

    r = await c.get(f'/{strip.name}/p/{token}', params={'page': 0, 'maxWidth': strip.width})
    check('the stream route refuses a webtoon profile', r.status_code == 404, f'status={r.status_code}')
    said = r.json().get('error', '') if r.headers.get('content-type', '').startswith('application/json') else ''
    check('and says to download the book instead', 'download' in said, f'{said!r}')
    r = await c.request('HEAD', f'/{strip.name}/p/{token}', params={'page': 0})
    check('HEAD is refused alike', r.status_code == 404, f'status={r.status_code}')
    r = await c.get(f'/{strip.name}/p/' + encode_token(f'{up}/opds/v1.2/upstream500'))
    check('the refusal comes before the upstream is asked', r.status_code == 404, f'status={r.status_code}')

    ctx = rewrite.Ctx(profile=strip.name, public_base='http://proxy.test', base_url=f'{up}/opds/v2/catalog')
    doc = {
        'links': [{'rel': 'self', 'href': '/opds/v2/catalog', 'type': 'application/opds+json'}],
        'publications': [
            {
                'metadata': {'title': 'Vol 1'},
                'links': [
                    {
                        'rel': 'http://opds-spec.org/acquisition',
                        'href': '/opds/v1.2/books/7/file',
                        'type': 'application/vnd.comicbook+zip',
                    },
                    {'rel': [PSE_REL], 'href': '/opds/v1.2/books/7/stream', 'type': 'image/jpeg'},
                    {'href': '/opds/v1.2/books/7/pages/{pageNumber}', 'type': 'image/jpeg', 'templated': True},
                ],
            }
        ],
    }
    body = json.dumps(doc).encode()
    on = json.loads(rewrite.rewrite(body, 'application/opds+json', ctx, 'image/png'))
    off = json.loads(rewrite.rewrite(body, 'application/opds+json', ctx, 'image/png', stream=False))
    check(
        'premise: both OPDS 2 stream links map to /p/ when streaming',
        sum('/p/' in link['href'] for link in on['publications'][0]['links']) == 2,
    )
    kinds = [link['href'].split('/')[4] for link in off['publications'][0]['links']]
    check('OPDS 2 drops a stream link found by rel or by placeholder, and only those', kinds == ['dl'], f'{kinds}')
    check('the feed-level links are untouched', '/f/' in off['links'][0]['href'])


async def check_browse(c) -> None:
    up = 'http://127.0.0.1:8899'
    r = await c.get('/kobo-clara-hd-2e-bw/browse')
    html = r.text
    check('browse root served as html', r.status_code == 200 and 'text/html' in r.headers['content-type'])
    check('no upstream host reaches the page', up not in html)
    check('no upstream path reaches the page', '/opds/v1.2/' not in html)
    check('the page names the profile it rendered for', 'kobo-clara-hd-2e-bw' in html and '1072' in html)

    dl = re.findall(r'/kobo-clara-hd-2e-bw/dl/([\w-]+)', html)
    check('the acquisition entry offers one download', len(dl) == 1, f'{len(dl)} links')
    token = dl[0] if dl else ''
    check(
        'the download token points back at the upstream file',
        _decoded(token)[0] == f'{up}/opds/v1.2/books/7/file',
    )
    check(
        'the download token carries the cover, as the feed route does',
        _decoded(token)[1] == f'{up}/opds/v1.2/books/7/thumbnail',
    )
    check('the cover is rendered through the img route', len(re.findall(r'/kobo-clara-hd-2e-bw/img/[\w-]+', html)) == 1)
    box = math.gcd(profiles.COVER.width, profiles.COVER.height)
    check(
        'the css cover box is the cover profile box, so nothing is cropped',
        f'aspect-ratio:{profiles.COVER.width // box}/{profiles.COVER.height // box};' in web.CSS,
        f'{profiles.COVER.width}x{profiles.COVER.height}',
    )
    check('pse:count is shown as a page count', '3 pages' in html)
    check('the acquisition length is shown', '40.0 MB' in html)
    check('an acquisition feed renders as rows', 'Download CBZ' in html and 'class="grid"' not in html)

    form = re.search(r'<form class="q" action="([^"]+)"', html)
    check('a search form is offered', form is not None)
    action = form.group(1) if form else ''
    stok = action.rsplit('/', 1)[-1]
    check('the search form targets the browse search route', '/kobo-clara-hd-2e-bw/bs/' in action)
    check(
        'the search token is the description document, not a filled template',
        _decoded(stok)[0] == f'{up}/opds/v1.2/search',
    )
    r = await c.get(f'/kobo-clara-hd-2e-bw/bs/{stok}', params={'q': 'dune'})
    check('search resolves the description and runs the query', 'hits:dune' in r.text)
    check('the search box keeps the term', 'value="dune"' in r.text)

    ntok = encode_token(f'{up}/opds/v1.2/nav')
    nav = (await c.get(f'/kobo-clara-hd-2e-bw/b/{ntok}')).text
    check('a feed with no acquisitions renders as a grid', 'class="grid"' in nav)
    check('navigation entries link into the browse route', '/kobo-clara-hd-2e-bw/b/' in nav)
    check('navigation entries do not link into the machine-readable feed route', '/kobo-clara-hd-2e-bw/f/' not in nav)
    check('an entry title is escaped', '<script>' not in nav and '&lt;script&gt;' in nav)
    check('a summary stands in when there is no page count', 'twelve of them' in nav)
    check(
        'the next link is offered as a browse link',
        f'/kobo-clara-hd-2e-bw/b/{encode_token(f"{up}/opds/v1.2/nav?p=2")}' in nav,
    )
    check(
        'the up link is offered as a browse link',
        f'/kobo-clara-hd-2e-bw/b/{encode_token(f"{up}/opds/v1.2/catalog")}' in nav,
    )
    check(
        "an entry's own navigation link is not hoisted into the pager",
        encode_token(f'{up}/opds/v1.2/nav?entrys-own') not in nav,
    )

    gtok = encode_token(f'{up}/opds/v1.2/negotiate')
    r = await c.get(f'/kobo-clara-hd-2e-bw/b/{gtok}', headers={'accept': 'text/html,application/xhtml+xml'})
    check(
        "the browser's Accept is not forwarded upstream",
        'upstream html' not in r.text and 'Download CBZ' in r.text,
    )

    v2 = encode_token(f'{up}/opds/v2/catalog')
    r = await c.get(f'/kobo-clara-hd-2e-bw/b/{v2}')
    check(
        'an OPDS 2.0 upstream gets an explanation, not a traceback',
        r.status_code == 200 and 'do not render' in r.text,
    )

    r = await c.get('/passthrough/browse')
    panel = web._panel(profiles.PROFILES['passthrough'])
    check(
        'a sizeless profile prints no dimensions at all',
        'original' in panel and not any(ch.isdigit() for ch in panel),
        panel,
    )
    check('and the page it renders says so', r.status_code == 200 and 'original' in r.text)
    check('browse on an unknown profile is 404', (await c.get('/nope/browse')).status_code == 404)

    r = await c.get(f'/kobo-clara-hd-2e-bw/dl/{token}')
    check('a download link taken from the page returns a cbz', r.content[:4] == b'PK\x03\x04')

    r = await c.get(f'/kobo-clara-hd-2e-bw/dl/{token}', params={'job': 'job-1'})
    state = (await c.get('/kobo-clara-hd-2e-bw/dl-status/job-1')).json()
    check('a finished repack reports itself done', state['stage'] == 'done', f'{state}')
    check('and it got through every page', state['done'] == state['total'] == 3, f'{state}')
    check('the download itself is unaffected by being watched', r.content[:4] == b'PK\x03\x04')
    check(
        'an unknown job is a 404, not an empty answer',
        (await c.get('/kobo-clara-hd-2e-bw/dl-status/never-started')).status_code == 404,
    )
    check(
        'the status route checks the profile like every other',
        (await c.get('/nope/dl-status/job-1')).status_code == 404,
    )
    bad = encode_token(f'{up}/opds/v1.2/missing')
    await c.get(f'/kobo-clara-hd-2e-bw/dl/{bad}', params={'job': 'job-2'})
    check(
        'a download that never arrives is reported as failed',
        (await c.get('/kobo-clara-hd-2e-bw/dl-status/job-2')).json()['stage'] == 'error',
    )
    ptok = encode_token(f'{up}/opds/v1.2/books/7/file')
    await c.get(f'/passthrough/dl/{ptok}', params={'job': 'job-3'})
    check(
        'a profile that repacks nothing still finishes the job',
        (await c.get('/passthrough/dl-status/job-3')).json()['stage'] == 'done',
    )
    check(
        'the browser is told the upstream filename, so the token is not the name',
        r.headers.get('content-disposition') == 'attachment; filename="Fake Vol 1.cbz"',
        repr(r.headers.get('content-disposition')),
    )

    guard = encode_token(f'{up}/opds/v1.2/guarded')
    r = await c.get(f'/kobo-clara-hd-2e-bw/b/{guard}')
    check('a guarded upstream still answers 401', r.status_code == 401, f'status={r.status_code}')
    check(
        'and the challenge is forwarded, so the browser can prompt',
        r.headers.get('www-authenticate') == 'Basic realm="Komga", charset="UTF-8"',
        repr(r.headers.get('www-authenticate')),
    )
    r = await c.get(f'/kobo-clara-hd-2e-bw/b/{guard}', headers={'authorization': 'Basic Zm9vOmJhcg=='})
    check(
        'the credentials the browser then sends get through',
        r.status_code == 200 and 'Download CBZ' in r.text,
        f'status={r.status_code}',
    )
    r = await c.get(f'/kobo-clara-hd-2e-bw/f/{guard}')
    check(
        'the same challenge reaches a reader on the feed route',
        r.status_code == 401 and 'realm="Komga"' in r.headers.get('www-authenticate', ''),
    )
    r = await c.get(f'/kobo-clara-hd-2e-bw/b/{encode_token(f"{up}/opds/v1.2/silent401")}')
    check(
        'a 401 with no challenge of its own is still given one',
        r.status_code == 401 and r.headers.get('www-authenticate') == 'Basic realm="Inksetter"',
        repr(r.headers.get('www-authenticate')),
    )
    r = await c.get(f'/kobo-clara-hd-2e-bw/b/{encode_token(f"{up}/opds/v1.2/forbidden")}')
    check(
        'a 403 is not dressed up as a login prompt',
        r.status_code == 403 and 'www-authenticate' not in {k.lower() for k in r.headers},
    )
    r = await c.get(f'/kobo-clara-hd-2e-bw/b/{encode_token(f"{up}/opds/v1.2/missing")}')
    check(
        'and neither is a 404',
        r.status_code == 404 and 'www-authenticate' not in {k.lower() for k in r.headers},
    )
    html_accept = {'accept': 'text/html,application/xhtml+xml'}
    r = await c.get(f'/kobo-clara-hd-2e-bw/b/{guard}', headers=html_accept)
    check(
        'a browser that cancels the prompt gets a page, not raw json',
        'Sign in required' in r.text and r.text.lstrip()[:1] == '<',
        r.text[:60],
    )
    check(
        'and that page still carries the challenge',
        r.headers.get('www-authenticate') == 'Basic realm="Komga", charset="UTF-8"',
    )
    r = await c.get(f'/kobo-clara-hd-2e-bw/f/{guard}')
    check(
        'while a reader is still answered in json',
        r.headers['content-type'].startswith('application/json') and 'upstream status 401' in r.text,
        r.headers['content-type'],
    )
    r = await c.get('/nope/browse', headers=html_accept)
    check(
        'an unknown profile reads as a page in a browser too',
        r.status_code == 404 and 'Not found' in r.text and 'unknown profile' in r.text,
    )
    mangled = '/kobo-clara-hd-2e-bw/b/!!!not-a-token'
    r = await c.get(mangled, headers=html_accept)
    check(
        'a mangled link reads as a page in a browser, not raw json',
        r.status_code == 400 and r.headers['content-type'].startswith('text/html') and 'Bad request' in r.text,
        f'{r.status_code} {r.headers["content-type"]}',
    )
    r = await c.get('/kobo-clara-hd-2e-bw/b/<script>alert(1)', headers=html_accept)
    check(
        'and a hostile token echoed back on that page is escaped',
        r.status_code == 400 and '<script>' not in r.text and '&lt;script&gt;' in r.text,
        f'{r.status_code}',
    )
    r = await c.get(mangled)
    check(
        'while a reader is still told in json',
        r.status_code == 400 and r.headers['content-type'].startswith('application/json'),
        r.headers['content-type'],
    )

    home = (await c.get('/')).text
    check('the index links each profile into the browse pages', '/kobo-clara-hd-2e-bw/browse' in home)
    check('the index still shows the catalog URL for readers', '/kobo-clara-hd-2e-bw/catalog' in home)
    check('the index does not name itself twice in the tab', '<title>Inksetter</title>' in home)


def _grid_columns(width: float) -> int:
    track = max(web.GRID_MIN, (width - (web.GRID_COLUMNS - 1) * web.GRID_GAP) / web.GRID_COLUMNS)
    n = 1
    while (n + 1) * track + n * web.GRID_GAP <= width:
        n += 1
    return n


def check_grid_columns() -> None:
    check(
        'the css says a fifth of the row, gaps taken out',
        'calc((100% - 64px)/5)' in web.CSS,
        next((ln for ln in web.CSS.splitlines() if ln.startswith('.grid{')), 'no .grid rule'),
    )
    check('a full-width window shows five covers', _grid_columns(1000 - 48) == 5)
    check('and a window twice that still shows five', _grid_columns(2000) == 5)
    check('and one ten times that still shows five', _grid_columns(10000) == 5)
    narrow = {w: _grid_columns(w) for w in (343, 500, 700)}
    check(
        'a narrow window drops rather than squeezing below the floor',
        narrow == {343: 2, 500: 3, 700: 4},
        f'{narrow}',
    )
    check(
        'no width anywhere overflows the cap',
        max(_grid_columns(w) for w in range(200, 4000, 7)) == web.GRID_COLUMNS,
        f'max {max(_grid_columns(w) for w in range(200, 4000, 7))}',
    )


def _entries_of(archive: bytes) -> dict[str, bytes]:
    z = zipfile.ZipFile(io.BytesIO(archive))
    return {n: z.read(n) for n in z.namelist()}


def check_repack_progress() -> None:
    seen = []
    blob = _cbz_bytes(4)
    out = b''.join(
        cbz.repack_iter(
            io.BytesIO(blob),
            profiles.PROFILES['kobo-clara-hd-2e-bw'],
            1,
            progress=lambda done, total: seen.append((done, total)),
        )
    )
    check('a repack reports before it has done anything', seen[:1] == [(0, 4)], f'{seen[:1]}')
    check('it counts every page exactly once, in order', seen == [(n, 4) for n in range(5)], f'{seen}')
    check('and the archive it produced is still whole', len(zipfile.ZipFile(io.BytesIO(out)).namelist()) == 5)

    big = _cbz_bytes(8)
    one, many = [], []
    b1 = b''.join(
        cbz.repack_iter(
            io.BytesIO(big),
            profiles.PROFILES['kobo-clara-hd-2e-bw'],
            1,
            progress=lambda done, total: one.append((done, total)),
        )
    )
    b2 = b''.join(
        cbz.repack_iter(
            io.BytesIO(big),
            profiles.PROFILES['kobo-clara-hd-2e-bw'],
            3,
            progress=lambda done, total: many.append((done, total)),
        )
    )
    check('eight pages are counted one by one', one == [(n, 8) for n in range(9)], f'{one}')
    check('the pooled repack counts the same way', many == one, f'{many}')
    check('and produces the same entries as the serial one', _entries_of(b2) == _entries_of(b1))

    counted = []
    cover = fake_page(9, 800, 1200)
    b3 = b''.join(
        cbz.repack_iter(
            io.BytesIO(blob),
            profiles.PROFILES['kobo-clara-hd-2e-bw'],
            1,
            cover=cover,
            progress=lambda done, total: counted.append((done, total)),
        )
    )
    check('a prepended cover is counted as a page too', counted[-1] == (5, 5), f'{counted[-1]}')
    check('and it really is in the archive', len(zipfile.ZipFile(io.BytesIO(b3)).namelist()) == 6)

    no_cb = b''.join(cbz.repack_iter(io.BytesIO(blob), profiles.PROFILES['kobo-clara-hd-2e-bw'], 1))
    check('watching a repack does not change what it produces', _entries_of(no_cb) == _entries_of(out))


def check_job_board() -> None:
    board = app_mod.Jobs(limit=3, ttl=900.0)
    board.set('a', stage='fetching')
    check('a job can be read back', board.get('a')['stage'] == 'fetching')
    board.set('a', stage='repacking', done=7, total=9)
    check('and updated in place', board.get('a') == {'stage': 'repacking', 'done': 7, 'total': 9})
    check('an unknown job is unknown', board.get('zzz') is None)
    check('a bogus id is refused outright', (board.set('../etc', stage='x'), board.get('../etc'))[1] is None)
    check('an over-long id is refused too', (board.set('x' * 65, stage='x'), board.get('x' * 65))[1] is None)
    for name in ('b', 'c', 'd', 'e'):
        board.set(name, stage='fetching')
    check('the board cannot grow past its limit', len(board._board) == 3, f'{len(board._board)}')
    check('and it is the oldest that goes', board.get('a') is None and board.get('e') is not None)

    stale = app_mod.Jobs(limit=8, ttl=-1.0)
    stale.set('old', stage='fetching')
    stale.set('new', stage='fetching')
    check('an entry older than the ttl is swept', stale.get('old') is None)


async def check_logo(c) -> None:
    packaged = pathlib.Path(app_mod.__file__).with_name(web.LOGO).read_bytes()
    for path in (f'/{web.LOGO}', '/favicon.ico'):
        r = await c.get(path)
        check(f'{path} is served', r.status_code == 200, f'status={r.status_code}')
        check(f'{path} is a png', r.headers['content-type'] == 'image/png')
        check(f'{path} is the packaged file, byte for byte', r.content == packaged, f'{len(r.content)} bytes')
        check(f'{path} is cacheable', 'max-age' in r.headers.get('cache-control', ''))

    im = pyvips.Image.new_from_buffer(packaged, '')
    check('the logo is square and has transparency', im.width == im.height and im.hasalpha(), f'{im.width}x{im.height}')

    home = (await c.get('/')).text
    feed = (await c.get('/kobo-clara-hd-2e-bw/browse')).text
    for name, html in (('landing', home), ('browse', feed)):
        check(f'the {name} page declares the icon', 'rel="icon" type="image/png"' in html)
        check(f'the {name} page points the icon at the served route', f'/{web.LOGO}"' in html)

    check('the landing page wears the logo in its header', 'class="mark"' in home)
    check('and the header is the big one', "class='hero'" in home)
    check('which is centred', 'header.hero{align-items:center;justify-content:center' in web.CSS)
    check('the opening line is its own thing, not a breadcrumb', '<p class="lede">' in home)
    lede = re.search(r'p\.lede\{font-size:([\d.]+)px', web.CSS)
    crumbs = re.search(r'\.crumbs\{font-size:([\d.]+)px', web.CSS)
    check(
        'and reads larger than the breadcrumbs it used to borrow from',
        lede and crumbs and float(lede.group(1)) > float(crumbs.group(1)),
        f'lede {lede.group(1) if lede else "?"}px vs crumbs {crumbs.group(1) if crumbs else "?"}px',
    )
    check('an inner page does not repeat the hero', 'class="mark"' not in feed and "class='hero'" not in feed)
    check(
        'the logo rides in its own request, not inside the page',
        'data:image' not in home and len(home) < 60_000,
        f'{len(home)} bytes',
    )
    err = (await c.get('/nope/browse', headers={'accept': 'text/html'})).text
    check('even an error page gets a favicon', 'rel="icon"' in err)


def check_landing_groups() -> None:
    titles = [t for t, _ in web.GROUPS]
    placed = {n: web.group_of(p) for n, p in profiles.PROFILES.items()}
    check('every profile lands in a group', set(placed.values()) <= set(titles), f'{set(placed.values())}')
    check('and no group is left empty', set(placed.values()) == set(titles), f'{set(titles) - set(placed.values())}')

    by_group: dict[str, list[str]] = {t: [] for t in titles}
    for name, title in placed.items():
        by_group.setdefault(title, []).append(name)
    check(
        'the panel sorts the devices, not the names',
        all(profiles.PROFILES[n].panel == 'kaleido' for n in by_group['Colour'])
        and all(profiles.PROFILES[n].panel == 'mono' for n in by_group['Black and white']),
    )
    check(
        'a webtoon profile is filed as webtoon, not as the colour panel it runs on',
        {n for n, p in profiles.PROFILES.items() if p.fit == 'width'} == set(by_group['Webtoon'])
        and all(profiles.PROFILES[n].panel == 'kaleido' for n in by_group['Webtoon']),
        f'{sorted(by_group["Webtoon"])}',
    )
    check(
        'a profile that renders nothing is not called black and white',
        by_group['No processing'] == ['passthrough'],
        f'{by_group["No processing"]}',
    )

    home = web.index('http://proxy.test', sorted(profiles.PROFILES.items()))
    for title in titles:
        check(f'the page has a {title.lower()} section', f'>{title}<' in home)
    shown = re.findall(r'<h2 class="group">([^<]+)<span>(\d+)</span>', home)
    check(
        'each heading counts what is under it',
        [(t, str(len(by_group[t]))) for t in titles] == shown,
        f'{shown}',
    )
    check(
        'every profile is still listed exactly once',
        [home.count(f'/{n}/browse') for n in profiles.PROFILES] == [1] * len(profiles.PROFILES),
    )
    check(
        'and still shows its catalog URL',
        all(f'/{n}/catalog' in home for n in profiles.PROFILES),
    )
    order = re.findall(r'href="http://proxy\.test/([\w-]+)/browse"', home)
    check(
        'names run alphabetically inside a group',
        all(sorted(by_group[t]) == [n for n in order if placed[n] == t] for t in titles),
    )
    walked = list(dict.fromkeys(placed[n] for n in order))
    check(
        'the groups run in the declared order, each one only once',
        walked == [t for t in titles if by_group[t]],
        f'{walked}',
    )


def check_progress_script() -> None:
    p = profiles.PROFILES['kobo-clara-hd-2e-bw']
    item = web.Item(title='V1', href='/kobo-clara-hd-2e-bw/dl/tok', cover=None, note='', download=True)
    feed = web.Feed(title='S', items=(item,), search=None, up=None, nxt=None, prev=None)
    html = web.page(feed, p, 'http://proxy.test')
    check('a page with downloads carries the script', '<script>' in html)
    check(
        'and points it at the status route for this profile',
        'data-status="http://proxy.test/kobo-clara-hd-2e-bw/dl-status/"' in html,
    )
    check('the plain link survives for a browser without js', 'href="/kobo-clara-hd-2e-bw/dl/tok"' in html)

    shown = list(web.STAGE_LABELS.values())
    check(
        'every stage the badge can show reads as a capitalised word',
        all(s and s[0].isupper() for s in shown),
        f'{[s for s in shown if not (s and s[0].isupper())]}',
    )
    check(
        'the wire stages stay lower case, so the protocol is unchanged',
        all(k.islower() for k in web.STAGE_LABELS),
        f'{[k for k in web.STAGE_LABELS if not k.islower()]}',
    )
    missing = [s for s in shown if f'"{s}"' not in html]
    check('the script is handed every label', not missing, f'{missing}')
    body = html.split('<script>')[1].split('</script>')[0]
    skeleton = body.replace(json.dumps(web.STAGE_LABELS).replace('<', '\\u003c'), '')
    spoken = {s.lower() for s in shown}
    hardcoded = [q for q in re.findall(r"'([^']*)'", skeleton) if q.lower() in spoken]
    check(
        'and speaks no stage of its own, in any case',
        not hardcoded,
        f'{hardcoded}',
    )
    check(
        'and every stage the server reports has a label',
        {'starting', 'fetching', 'repacking', 'done', 'error'} <= set(web.STAGE_LABELS),
    )

    nav = web.Item(title='S', href='/kobo-clara-hd-2e-bw/b/tok', cover=None, note='', download=False)
    grid = web.page(
        web.Feed(title='S', items=(nav,), search=None, up=None, nxt=None, prev=None), p, 'http://proxy.test'
    )
    check('a page with nothing to download carries no script', '<script>' not in grid)
    check('and no status hook either', 'data-status' not in grid)


def _crumbs(html: str) -> tuple[list[tuple[str, str]], str]:
    nav = re.search(r'<nav class="crumbs">(.*?)</nav>', html, re.S)
    inner = nav.group(1) if nav else ''
    here = re.search(r'<b>([^<]*)</b>', inner)
    return re.findall(r'<a href="([^"]*)">([^<]*)</a>', inner), (here.group(1) if here else '')


async def check_trail(c) -> None:
    up = 'http://127.0.0.1:8899'
    nav = encode_token(f'{up}/opds/v1.2/nav')

    top = (await c.get(f'/kobo-clara-hd-2e-bw/b/{nav}')).text
    links, here = _crumbs(top)
    check(
        'with no trail yet, only the feed\'s own rel="up" stands in',
        here == 'Shelves' and [t for _, t in links] == ['Profiles', 'Up'],
        f'{[t for _, t in links]}',
    )

    step = re.search(r'<a class="t" href="([^"]+)"', top).group(1).replace('&amp;', '&')
    check('a navigation link carries a trail', f'{web.TRAIL_PARAM}=' in step, step[-40:])

    mid = (await c.get(step.replace('http://proxy.test', ''))).text
    links, here = _crumbs(mid)
    titles = [t for _, t in links]
    check(
        'one level down, the parent becomes a crumb', here == 'Manga' and titles == ['Profiles', 'Shelves'], f'{titles}'
    )
    check(
        'and that crumb points back at the parent',
        len(links) > 1 and f'/kobo-clara-hd-2e-bw/b/{nav}' in links[1][0],
        links[1][0] if len(links) > 1 else 'no crumb',
    )

    step2 = re.search(r'<a class="t" href="([^"]+)"', mid).group(1).replace('&amp;', '&')
    deep = (await c.get(step2.replace('http://proxy.test', ''))).text
    links, here = _crumbs(deep)
    check(
        'two levels down, the whole walk is on show',
        here == 'Fake' and [t for _, t in links] == ['Profiles', 'Shelves', 'Manga'],
        f'{[t for _, t in links]}',
    )
    deep_links = links + [('', '')] * 3
    check(
        'the first crumb drops the trail it no longer needs',
        f'{web.TRAIL_PARAM}=' not in deep_links[1][0],
        deep_links[1][0],
    )
    own = web.decode_trail(deep_links[2][0].partition(f'{web.TRAIL_PARAM}=')[2])
    check(
        'while the second keeps just its own ancestors',
        len(own) == 1 and own[0][1] == 'Shelves',
        f'{[t for _, t in own]}',
    )

    back = (await c.get(deep_links[1][0].replace('http://proxy.test', '') or '/')).text
    _, here = _crumbs(back)
    check('clicking a crumb lands back on that page', here == 'Shelves')

    check('a download link is not burdened with a trail', f'?{web.TRAIL_PARAM}=' not in _dl_href(deep))
    nxt = re.search(r'<a href="([^"]*)">Next', top)
    check(
        "next keeps the page's own trail, not its children's",
        nxt is not None and f'{web.TRAIL_PARAM}=' not in nxt.group(1),
        nxt.group(1) if nxt else 'no next',
    )

    junk = await c.get(f'/kobo-clara-hd-2e-bw/b/{nav}', params={web.TRAIL_PARAM: 'not~valid~base64!!'})
    check(
        'a corrupt trail is dropped, not fatal',
        junk.status_code == 200 and _crumbs(junk.text)[1] == 'Shelves',
        f'status={junk.status_code}',
    )
    check('and is never echoed back into a link', 'not~valid~base64' not in junk.text)

    hostile = web.encode_trail([('javascript:alert(1)', 'Evil'), ('../../etc', 'Worse')])
    r = await c.get(f'/kobo-clara-hd-2e-bw/b/{nav}', params={web.TRAIL_PARAM: hostile})
    check(
        'a trail cannot smuggle a link of its own onto the page',
        'javascript:' not in r.text and '../../etc' not in r.text,
    )

    pairs12 = [(encode_token(f'{up}/opds/v1.2/n{i}'), f'L{i}') for i in range(12)]
    long_trail = web.encode_trail(pairs12)
    links, _ = _crumbs((await c.get(f'/kobo-clara-hd-2e-bw/b/{nav}', params={web.TRAIL_PARAM: long_trail})).text)
    check(
        'a trail cannot grow without bound',
        len(links) == web.TRAIL_MAX + 1,
        f'{len(links) - 1} crumbs, cap {web.TRAIL_MAX}',
    )
    written = base64.urlsafe_b64decode(long_trail + '=' * (-len(long_trail) % 4)).decode()
    check(
        'and it is capped where it is written, not only where it is read',
        written.count('\n') + 1 == web.TRAIL_MAX,
        f'{written.count(chr(10)) + 1} entries written',
    )

    form = re.search(r'<form class="q".*?</form>', deep, re.S).group(0)
    check('the search form carries the trail forward too', f'name="{web.TRAIL_PARAM}"' in form)


def _dl_href(html: str) -> str:
    found = re.search(r'<a class="dl" href="([^"]*)"', html)
    return found.group(1) if found else ''


def check(name: str, ok: bool, detail: str = '') -> None:
    (PASS if ok else FAIL).append(name)
    print(f'  {"PASS" if ok else "FAIL"}  {name}{"  " + detail if detail else ""}')


async def main() -> int:
    server = start_upstream()
    await up_client.start()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url='http://proxy.test', timeout=60) as c:
        print('feed rewriting')
        r = await c.get('/kobo-clara-hd-2e-bw/catalog')
        feed = r.text
        check('catalog served', r.status_code == 200)
        check('{pageNumber} survives', '{pageNumber}' in feed)
        check('{maxWidth} survives', '{maxWidth}' in feed)
        check('pse:count preserved', 'pse:count="3"' in feed)
        check('pse:lastRead preserved', 'pse:lastRead="2"' in feed)
        check('PSE type matches output', 'type="image/png"' in feed)
        check('no upstream host leaks', '127.0.0.1:8899' not in feed)

        pse = re.search(r'/kobo-clara-hd-2e-bw/p/([\w-]+)\?page=', feed).group(1)
        img = re.search(r'/kobo-clara-hd-2e-bw/img/([\w-]+)', feed).group(1)
        dl = re.search(r'/kobo-clara-hd-2e-bw/dl/([\w-]+)', feed).group(1)
        osd = re.search(r'/kobo-clara-hd-2e-bw/osd/([\w-]+)', feed).group(1)

        print('page rendering')
        t = time.time()
        r1 = await c.get(f'/kobo-clara-hd-2e-bw/p/{pse}', params={'page': 0, 'maxWidth': 1072})
        cold = time.time() - t
        t = time.time()
        r2 = await c.get(f'/kobo-clara-hd-2e-bw/p/{pse}', params={'page': 0, 'maxWidth': 1072})
        warm = time.time() - t
        w, h, levels = grey_levels(r1.content)
        check('png returned', r1.headers['content-type'] == 'image/png')
        check('fits panel box', w <= 1072 and h <= 1448, f'{w}x{h}')
        check('<= 16 grey levels', levels <= 16, f'levels={levels}')
        check(
            'levels are real greys',
            set(
                np.unique(
                    np.ndarray(
                        buffer=pyvips.Image.new_from_buffer(r1.content, '').write_to_memory(),
                        dtype=np.uint8,
                        shape=(h, w),
                    )
                )
            )
            <= {i * 17 for i in range(16)},
        )

        flat = dataclasses.replace(
            profiles.PROFILES['kobo-clara-hd-2e-bw'],
            autocrop=False,
            black=0,
            white=255,
            gamma=1.0,
            usm_amount=0.0,
        )
        ramp = np.tile(np.linspace(0, 255, 1200, dtype=np.uint8), (1600, 1))
        ramp_png = pyvips.Image.new_from_memory(ramp.tobytes(), 1200, 1600, 1, 'uchar').pngsave_buffer()
        mean4 = pyvips.Image.new_from_buffer(pipeline.render_page(ramp_png, flat)[0], '').avg()
        mean8 = pyvips.Image.new_from_buffer(
            pipeline.render_page(ramp_png, dataclasses.replace(flat, fmt='png8'))[0], ''
        ).avg()
        check(
            '4-bit tones match 8-bit',
            abs(mean4 - mean8) < 6,
            f'png4={mean4:.1f} png8={mean8:.1f}',
        )
        check('cache hit identical', r1.content == r2.content)
        check('cache hit is fast', warm < cold / 5, f'cold={cold * 1000:.0f}ms warm={warm * 1000:.0f}ms')

        await asyncio.sleep(2.0)
        t = time.time()
        r3 = await c.get(f'/kobo-clara-hd-2e-bw/p/{pse}', params={'page': 1, 'maxWidth': 1072})
        pf = time.time() - t
        check('next page prefetched', r3.status_code == 200 and pf < 0.2, f'{pf * 1000:.0f}ms')

        r = await c.get(f'/kobo-clara-hd-2e-bw/p/{pse}', params={'page': 0, 'maxWidth': 600})
        w2, _, _ = grey_levels(r.content)
        check('maxWidth clamps down', w2 <= 600, f'width={w2}')

        print('covers, search, download')
        r = await c.get(f'/kobo-clara-hd-2e-bw/img/{img}')
        check('cover shrunk', r.status_code == 200 and len(r.content) < 30_000, f'{len(r.content)}B')

        r = await c.get(f'/kobo-clara-hd-2e-bw/osd/{osd}')
        check('{searchTerms} survives', '{searchTerms}' in r.text)
        s = re.search(r'/kobo-clara-hd-2e-bw/s/([\w-]+)\?q=', r.text).group(1)
        r = await c.get(f'/kobo-clara-hd-2e-bw/s/{s}', params={'q': 'sample query'})
        check('search term reaches upstream', 'hits:sample query' in r.text)

        raw = await c.get(f'/passthrough/dl/{dl}')
        packed = await c.get(f'/kobo-clara-hd-2e-bw/dl/{dl}')
        names = zipfile.ZipFile(io.BytesIO(packed.content)).namelist()
        check(
            'repack shrinks volume',
            len(packed.content) < len(raw.content) / 4,
            f'{len(raw.content)} -> {len(packed.content)}',
        )
        check('metadata kept in repack', 'ComicInfo.xml' in names)
        check('all pages kept', sum(n.endswith('.png') for n in names) == 3)

        print('kaleido 3 colour path')
        r = await c.get('/kindle-scribe-colorsoft/catalog')
        feed_c = r.text
        check('colour feed advertises png', 'type="image/png"' in feed_c)
        pse_c = re.search(r'/kindle-scribe-colorsoft/p/([\w-]+)\?page=', feed_c).group(1)

        col = await c.get(f'/kindle-scribe-colorsoft/p/{pse_c}', params={'page': 100})
        mon = await c.get(f'/kindle-scribe-colorsoft/p/{pse_c}', params={'page': 50})
        cw, ch, cb, cc = describe(col.content)
        mw, mh, mb, _ = describe(mon.content)
        check('colour page stays colour', cb >= 3, f'bands={cb}')
        ksc = profiles.PROFILES['kindle-scribe-colorsoft']
        check(
            'colour fills panel',
            (cw, ch) == (ksc.width, ksc.height),
            f'{cw}x{ch}, profile says {ksc.width}x{ksc.height}',
        )
        src_chroma = describe(fake_colour_page(2400, 3200))[3]
        check('saturation boosted', cc > src_chroma, f'{src_chroma:.1f} -> {cc:.1f}')
        check('grey page routed to mono', mb == 1, f'bands={mb}')
        check('mono route keeps full luma res', mw > 1800 and mw >= cw - 2, f'mono {mw}x{mh} vs colour {cw}x{ch}')
        check('mono route is 4-bit', grey_levels(mon.content)[2] <= 16)
        check('one MIME for both paths', col.headers['content-type'] == mon.headers['content-type'] == 'image/png')
        check(
            'colour cover stays colour', describe((await c.get(f'/kindle-scribe-colorsoft/img/{img}')).content)[2] >= 3
        )

        print('opds 2.0 and readium manifests')
        r = await c.get(
            '/kobo-clara-hd-2e-bw/f/' + encode_token('http://127.0.0.1:8899/opds/nego'),
            headers={'accept': 'application/opds+json, application/atom+xml, */*'},
        )
        check(
            'Accept forwarded (negotiation works)', 'opds+json' in r.headers['content-type'], r.headers['content-type']
        )
        v2 = r.json()
        check('v2 self link rewritten', '/kobo-clara-hd-2e-bw/f/' in v2['links'][0]['href'])
        srch = next(x for x in v2['links'] if x.get('rel') == 'search')
        check(
            'form-style search normalised', '{query}' in srch['href'] and '{?query}' not in srch['href'], srch['href']
        )
        check('search link stays templated', srch.get('templated') is True)
        check('v2 cover routed to img', '/kobo-clara-hd-2e-bw/img/' in v2['publications'][0]['images'][0]['href'])
        check(
            'v2 acquisition routed to dl',
            any('/kobo-clara-hd-2e-bw/dl/' in x['href'] for x in v2['publications'][0]['links']),
        )

        stok = re.search(r'/kobo-clara-hd-2e-bw/s/([\w-]+)', srch['href']).group(1)
        r = await c.get(
            f'/kobo-clara-hd-2e-bw/s/{stok}',
            params={'query': 'sample query'},
            headers={'accept': 'application/opds+json'},
        )
        check('v2 search term reaches upstream', 'v2hits:sample query' in r.text, r.text[:60])

        mtok = encode_token('http://127.0.0.1:8899/opds/v2/books/7/manifest')
        man = (await c.get(f'/kindle-scribe-colorsoft/f/{mtok}')).json()
        check('divina manifest rewritten', '127.0.0.1' not in json.dumps(man))
        ro = man['readingOrder']
        check('readingOrder routed to /pf/', all('/kindle-scribe-colorsoft/pf/' in e['href'] for e in ro))
        check('readingOrder type set to output mime', all(e['type'] == 'image/png' for e in ro))
        check('stale width/height dropped', all('width' not in e and 'height' not in e for e in ro))
        check('manifest cover still /img/', '/kindle-scribe-colorsoft/img/' in man['images'][0]['href'])

        pf = re.search(r'/pf/([\w-]+)', ro[0]['href']).group(1)
        r = await c.get(f'/kindle-scribe-colorsoft/pf/{pf}')
        pw, ph, _pb, _ = describe(r.content)
        check('readingOrder page rendered at panel size', pw > 1800, f'{pw}x{ph}')
        check('readingOrder page is not a thumbnail', pw > 640)

        etok = encode_token('http://127.0.0.1:8899/opds/v2/books/8/manifest')
        eman = (await c.get(f'/kobo-clara-hd-2e-bw/f/{etok}')).json()
        check(
            'epub readingOrder NOT sent to image pipeline',
            '/kobo-clara-hd-2e-bw/dl/' in eman['readingOrder'][0]['href'],
            eman['readingOrder'][0]['href'].split('/kobo-clara-hd-2e-bw/')[-1][:6],
        )
        check('css resource passed through', '/kobo-clara-hd-2e-bw/dl/' in eman['resources'][0]['href'])
        check('image resource still rendered', '/kobo-clara-hd-2e-bw/pf/' in eman['resources'][1]['href'])
        ctok = re.search(r'/dl/([\w-]+)', eman['readingOrder'][0]['href']).group(1)
        r = await c.get(f'/kobo-clara-hd-2e-bw/dl/{ctok}')
        check('xhtml chapter served untouched', 'chapter one' in r.text)

        print('HEAD')
        from inksetter import app as _app

        feed_tok = encode_token('http://127.0.0.1:8899/opds/v1.2/catalog')
        for label, path in (
            ('healthz', '/healthz'),
            ('index', '/'),
            ('catalog', '/kobo-clara-hd-2e-bw/catalog'),
            ('feed', f'/kobo-clara-hd-2e-bw/f/{feed_tok}'),
            ('osd', f'/kobo-clara-hd-2e-bw/osd/{osd}'),
            ('page', f'/kobo-clara-hd-2e-bw/p/{pse}?page=1'),
            ('cover', f'/kobo-clara-hd-2e-bw/img/{img}'),
            ('download', f'/kobo-clara-hd-2e-bw/dl/{dl}'),
        ):
            r = await c.request('HEAD', path)
            check(f'HEAD {label} is not 405', r.status_code == 200, f'status={r.status_code}')
            check(f'HEAD {label} has no body', not r.content, f'{len(r.content)} bytes')

        await c.get(f'/kobo-clara-hd-2e-bw/p/{pse}?page=2')
        g = await c.get(f'/kobo-clara-hd-2e-bw/p/{pse}?page=2')
        h = await c.request('HEAD', f'/kobo-clara-hd-2e-bw/p/{pse}?page=2')
        keys = ('content-type', 'content-length')
        check(
            'HEAD on a page reports the same headers as GET',
            {k: g.headers.get(k) for k in keys} == {k: h.headers.get(k) for k in keys},
            f'GET={ {k: g.headers.get(k) for k in keys} } HEAD={ {k: h.headers.get(k) for k in keys} }',
        )

        ptok2 = encode_token('http://127.0.0.1:8899/opds/v1.2/plain.cbz')
        hg = await c.get(f'/kobo-clara-hd-2e-bw/dl/{ptok2}')
        hh = await c.request('HEAD', f'/kobo-clara-hd-2e-bw/dl/{ptok2}')
        check(
            'HEAD on a download matches the GET content-type',
            hh.headers.get('content-type') == hg.headers.get('content-type') == 'application/vnd.comicbook+zip',
            f'HEAD={hh.headers.get("content-type")!r} GET={hg.headers.get("content-type")!r}',
        )
        check(
            'HEAD on a download sends NO content-length, not 0',
            'content-length' not in {k.lower() for k in hh.headers},
            f'content-length={hh.headers.get("content-length")!r}',
        )
        check('HEAD on a download has no body', not hh.content, f'{len(hh.content)} bytes')

        check(
            'a HEAD does not mistake gzip for a zip',
            not _app._probably_zip('http://x/f.tgz', 'application/gzip'),
        )
        check(
            'a HEAD recognises a comic archive by type',
            _app._probably_zip('http://x/f', 'application/vnd.comicbook+zip; charset=binary'),
        )
        check('a HEAD recognises a comic archive by name', _app._probably_zip('http://x/BOOK.CBZ', 'text/plain'))

        for raw, want in (('7', 7), ('0', 0), ('-5', 0), ('{pageNumber}', 0), ('', 0), ('12x', 0)):
            check(f'_query_int({raw!r}) -> {want}', _app._query_int(raw) == want, f'got {_app._query_int(raw)}')

        for label, target, want in (
            ('upstream 5xx', 'http://127.0.0.1:8899/opds/v1.2/upstream500', 502),
            ('upstream 4xx', 'http://127.0.0.1:8899/opds/v1.2/missing', 404),
            ('blocked host', 'http://169.254.169.254/latest/', 403),
        ):
            r = await c.request('HEAD', '/kobo-clara-hd-2e-bw/dl/' + encode_token(target))
            check(f'HEAD forwards {label}', r.status_code == want, f'status={r.status_code} want={want}')
        r = await c.request('HEAD', '/kobo-clara-hd-2e-bw/dl/!!!not-base64!!!')
        check('HEAD on a malformed token is 400', r.status_code == 400, f'status={r.status_code}')
        r = await c.request('HEAD', '/nope/catalog')
        check('HEAD on an unknown profile is 404', r.status_code == 404, f'status={r.status_code}')

        print('guards')
        r = await c.get('/nope/catalog')
        check('unknown profile 404', r.status_code == 404)
        r = await c.get('/kobo-clara-hd-2e-bw/f/' + encode_token('http://169.254.169.254/latest/'))
        check('SSRF blocked', r.status_code == 403)
        r = await c.get('/healthz')
        check('healthz', r.status_code == 200)

        print('regressions')

        r = await c.get('/kobo-clara-hd-2e-bw/f/' + encode_token('http://127.0.0.1:8899/opds/v1.2/redirect'))
        check('SSRF blocked across a redirect', r.status_code == 403, f'status={r.status_code}')

        from inksetter.opds import upstream as _upmod

        with socket.socket() as _probe:
            _probe.bind(('127.0.0.1', 0))
            dead_port = _probe.getsockname()[1]
        dead = f'http://127.0.0.1:{dead_port}/opds'
        real_settings = _upmod.settings
        _upmod.settings = dataclasses.replace(real_settings, upstream_catalog=dead)
        try:
            for label, route in (
                ('feed', 'f'),
                ('page', 'p'),
                ('image', 'img'),
                ('download', 'dl'),
            ):
                r = await c.get(f'/kobo-clara-hd-2e-bw/{route}/' + encode_token(f'{dead}/x.cbz'))
                check(
                    f'an unreachable upstream is 502 on the {label} route',
                    r.status_code == 502,
                    f'status={r.status_code} body={r.text[:60]}',
                )
                check(
                    f'...and says so rather than blaming the proxy ({label})',
                    'unreachable' in r.text,
                    r.text[:60],
                )
        finally:
            _upmod.settings = real_settings

        for label, tok in (('not base64', '!!!not-base64!!!'), ('bad utf-8', '_w==')):
            r = await c.get(f'/kobo-clara-hd-2e-bw/f/{tok}')
            check(f'malformed token ({label}) is 400', r.status_code == 400, f'status={r.status_code}')

        gz = encode_token('http://127.0.0.1:8899/opds/v1.2/gz/file.cbz')
        r = await c.get(f'/passthrough/dl/{gz}')
        check(
            'content-length matches the decoded body',
            int(r.headers['content-length']) == len(r.content),
            f'header={r.headers["content-length"]} body={len(r.content)}',
        )
        check(
            'compressed download is still a valid zip',
            'ComicInfo.xml' in zipfile.ZipFile(io.BytesIO(r.content)).namelist(),
        )
        check(
            'byte ranges are not advertised (no route honours them)',
            'accept-ranges' not in r.headers,
            f'keys={sorted(r.headers)}',
        )

        r = await c.get(f'/passthrough/img/{img}')
        check(
            'passthrough leaves covers untouched',
            r.content == fake_page(0),
            f'{len(r.content)}B vs source {len(fake_page(0))}B',
        )

        ctx = rewrite.Ctx(
            profile='kobo-clara-hd-2e-bw', public_base='http://proxy.test', base_url='http://127.0.0.1:8899/m'
        )
        nested = json.loads(
            rewrite.rewrite_json(
                json.dumps(
                    {
                        'readingOrder': [
                            {
                                'href': '/p1.jpg',
                                'type': 'image/jpeg',
                                'links': [{'href': '/chapters', 'rel': 'collection', 'type': 'application/opds+json'}],
                            }
                        ]
                    }
                ).encode(),
                ctx,
                'image/png',
            )
        )
        check(
            'nav link nested in readingOrder stays a feed',
            '/kobo-clara-hd-2e-bw/f/' in nested['readingOrder'][0]['links'][0]['href'],
            nested['readingOrder'][0]['links'][0]['href'].split('clara')[-1][:6],
        )

        alt = json.loads(
            rewrite.rewrite_json(
                json.dumps(
                    {
                        'readingOrder': [
                            {
                                'href': '/p1.jpg',
                                'type': 'image/jpeg',
                                'alternate': [{'href': '/p1-lo.jpg', 'type': 'image/jpeg'}],
                            }
                        ]
                    }
                ).encode(),
                ctx,
                'image/png',
            )
        )
        check(
            'alternate encoding of a page stays a page',
            '/kobo-clara-hd-2e-bw/pf/' in alt['readingOrder'][0]['alternate'][0]['href'],
            alt['readingOrder'][0]['alternate'][0]['href'].split('clara')[-1][:6],
        )

        check(
            'templated acquisition stays a download',
            rewrite.classify(
                'http://opds-spec.org/acquisition', 'application/vnd.comicbook+zip', '/books/7/file{?format}'
            )
            == 'dl',
        )

        CREDS_SEEN.clear()
        r = await c.get(
            '/kobo-clara-hd-2e-bw/dl/' + encode_token('http://127.0.0.1:8899/opds/v1.2/sidehop'),
            headers={'authorization': 'Basic c2VjcmV0', 'cookie': 'session=abc'},
        )
        check('cross-origin redirect is refused', r.status_code == 403, f'status={r.status_code}')
        check(
            'nothing at all reaches the other origin',
            not CREDS_SEEN,
            f'target saw {CREDS_SEEN!r}',
        )
        from inksetter.opds import upstream as _up

        stripped = _up._hop_headers(
            {'authorization': 'Basic c2VjcmV0', 'cookie': 'session=abc', 'user-agent': 'x'},
            'http://localhost:8899/x',
            '127.0.0.1:8899',
        )
        check(
            'Authorization dropped crossing origins',
            'authorization' not in stripped,
            f'kept {sorted(stripped)}',
        )
        check('Cookie dropped crossing origins', 'cookie' not in stripped, f'kept {sorted(stripped)}')
        check('non-credential headers survive the hop', stripped.get('user-agent') == 'x', f'{stripped}')

        CREDS_SEEN.clear()
        await c.get(
            '/kobo-clara-hd-2e-bw/dl/' + encode_token('http://127.0.0.1:8899/opds/v1.2/collect'),
            headers={'authorization': 'Basic c2VjcmV0'},
        )
        check(
            'Authorization kept on the origin it was meant for',
            CREDS_SEEN.get('authorization') == 'Basic c2VjcmV0',
            f'target saw {CREDS_SEEN.get("authorization")!r}',
        )

        for label, path in (('empty', 'empty-feed'), ('binary', 'binary-feed')):
            r = await c.get('/kobo-clara-hd-2e-bw/f/' + encode_token(f'http://127.0.0.1:8899/opds/v1.2/{path}'))
            check(
                f'{label} body under a feed content-type is not a 500', r.status_code < 500, f'status={r.status_code}'
            )

        r = await c.get('/kobo-clara-hd-2e-bw/dl/' + encode_token('http://127.0.0.1:8899/opds/v1.2/tarball.tgz'))
        check('gzip is not mistaken for a zip', r.status_code == 200, f'status={r.status_code}')
        r = await c.get('/kobo-clara-hd-2e-bw/dl/' + encode_token('http://127.0.0.1:8899/opds/v1.2/corrupt.cbz'))
        check(
            'corrupt archive falls back to passthrough',
            r.status_code == 200 and r.content.startswith(b'PK\x03\x04'),
            f'status={r.status_code}',
        )

        before = dict(_RANGED)
        ranged = await c.get(
            '/kobo-clara-hd-2e-bw/dl/' + encode_token('http://127.0.0.1:8899/opds/v1.2/ranged/file.cbz')
        )
        used = _RANGED['requests'] - before['requests']
        plain = await c.get(
            '/kobo-clara-hd-2e-bw/dl/' + encode_token('http://127.0.0.1:8899/opds/v1.2/norange/file.cbz')
        )
        check('a range download succeeds', ranged.status_code == 200, f'status={ranged.status_code}')
        check('so does one that must fall back', plain.status_code == 200, f'status={plain.status_code}')
        check('the range path was actually taken', used > 0, f'{used} range requests')

        zr = zipfile.ZipFile(io.BytesIO(ranged.content))
        zp = zipfile.ZipFile(io.BytesIO(plain.content))
        check('both archives are valid', zr.testzip() is None and zp.testzip() is None)
        check(
            'the two paths list the same entries, in the same order',
            zr.namelist() == zp.namelist(),
            f'{zr.namelist()} vs {zp.namelist()}',
        )
        differing = [n for n in zr.namelist() if zr.read(n) != zp.read(n)]
        check(
            'and every entry is byte-for-byte identical',
            not differing,
            f'differ: {differing}',
        )
        check(
            'a repacked range download still sends no content-length',
            'content-length' not in {k.lower() for k in ranged.headers},
            f'{ranged.headers.get("content-length")!r}',
        )

        r = await c.get('/kobo-clara-hd-2e-bw/f/' + encode_token('http://127.0.0.1:8899/opds/v1.2/withicon'))
        check('feed with an icon still serves', r.status_code == 200, f'status={r.status_code}')
        check(
            'no upstream credential survives in the feed text',
            'SECRETKEY123' not in r.text,
            r.text[:90],
        )
        for tag in ('icon', 'logo'):
            m = re.search(rf'<{tag}>([^<]*)</{tag}>', r.text)
            check(
                f'<{tag}> is rewritten through the proxy',
                bool(m) and '/kobo-clara-hd-2e-bw/img/' in m.group(1),
                (m.group(1)[:70] if m else 'element gone'),
            )

        for label, path in (('undecodable', 'notanimage'), ('empty body', 'emptyimage')):
            r = await c.get('/kobo-clara-hd-2e-bw/p/' + encode_token(f'http://127.0.0.1:8899/opds/v1.2/{path}'))
            check(
                f'{label} upstream page is 502, not 500',
                r.status_code == 502,
                f'status={r.status_code} {r.text[:60]}',
            )
        r = await c.get('/kobo-clara-hd-2e-bw/p/' + encode_token('http://127.0.0.1:8899/opds/v1.2/upstream500'))
        check('upstream 5xx becomes 502', r.status_code == 502, f'status={r.status_code}')
        r = await c.get('/kobo-clara-hd-2e-bw/p/' + encode_token('http://127.0.0.1:8899/opds/v1.2/missing'))
        check('upstream 4xx is forwarded as itself', r.status_code == 404, f'status={r.status_code}')

        from inksetter.imaging.pipeline import UnreadableImage as _Unreadable

        try:
            pipeline.render_page(b'<html>no</html>', profiles.PROFILES['kobo-clara-hd-2e-bw'])
            check('an unreadable page raises UnreadableImage', False, 'no error raised')
        except _Unreadable as exc:
            check('an unreadable page raises UnreadableImage', True, str(exc)[:50])
        except Exception as exc:
            check('an unreadable page raises UnreadableImage', False, f'{type(exc).__name__}')

        ptok = encode_token('http://127.0.0.1:8899/opds/v1.2/plain.cbz')
        rp = await c.get(f'/kobo-clara-hd-2e-bw/dl/{ptok}')
        inner = zipfile.ZipFile(io.BytesIO(rp.content))
        check(
            'repacked download is a readable cbz',
            rp.status_code == 200 and inner.testzip() is None and len(inner.namelist()) == 5,
            f'status={rp.status_code} entries={inner.namelist()}',
        )
        check(
            'repacked download sends no content-length',
            'content-length' not in {k.lower() for k in rp.headers},
            f'content-length={rp.headers.get("content-length")!r}',
        )
        r0 = await c.get(f'/passthrough/dl/{ptok}')
        check(
            'passthrough download keeps content-length',
            r0.headers.get('content-length') == str(len(r0.content)),
            f'header={r0.headers.get("content-length")!r} body={len(r0.content)}',
        )

        r = await c.get(f'/kobo-clara-hd-2e-bw/p/{pse}', params={'page': '{pageNumber}', 'maxWidth': '{maxWidth}'})
        check(
            'unsubstituted placeholders still serve page 0',
            r.status_code == 200 and r.headers['content-type'] == 'image/png',
            f'status={r.status_code}',
        )
        r = await c.get(f'/kindle-scribe-colorsoft/pf/{pf}', params={'maxWidth': '{maxWidth}'})
        check('/pf/ tolerates an unsubstituted maxWidth', r.status_code == 200, f'status={r.status_code}')

        print('webtoon profiles do not stream')
        await check_no_stream(c)

        print('browse pages')
        await check_browse(c)
        await check_trail(c)
        await check_logo(c)
        check_grid_columns()
        check_repack_progress()
        check_job_board()
        check_progress_script()
        check_landing_groups()

    print('repack + profile config')
    jc = dataclasses.replace(profiles.PROFILES['kindle-scribe-colorsoft'], fmt='jpegc', auto_mono=False)
    zf = zipfile.ZipFile(io.BytesIO(b''.join(cbz.repack_iter(io.BytesIO(_cbz_bytes(2)), jc))))
    pages = [n for n in zf.namelist() if n[0].isdigit()]
    check('jpegc repack names JPEG pages .jpg', pages and all(n.endswith('.jpg') for n in pages), f'{pages}')
    check('jpegc repack really contains JPEG', all(zf.read(n)[:3] == b'\xff\xd8\xff' for n in pages))
    zf4 = zipfile.ZipFile(
        io.BytesIO(b''.join(cbz.repack_iter(io.BytesIO(_cbz_bytes(2)), profiles.PROFILES['kobo-clara-hd-2e-bw'])))
    )
    check('png4 repack still names pages .png', all(n.endswith('.png') for n in zf4.namelist() if n[0].isdigit()))

    bomb = io.BytesIO()
    with zipfile.ZipFile(bomb, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('big.jpg', bytes(8 * 1024**2))
    try:
        cbz.repack_to(io.BytesIO(bomb.getvalue()), io.BytesIO(), profiles.PROFILES['kobo-clara-hd-2e-bw'])
        check('a highly compressible archive is no longer refused', True)
    except ValueError as exc:
        check('a highly compressible archive is no longer refused', False, str(exc)[:52])
    left = [a for a in ('MAX_ENTRIES', 'MAX_TOTAL_BYTES') if hasattr(cbz, a)]
    check('no ceiling constant is left in the module', not left, ', '.join(left))
    params = set(inspect.signature(cbz.repack_iter).parameters)
    check(
        'and repack_iter takes no limit arguments',
        not {'max_entries', 'max_total_bytes'} & params,
        f'{sorted(params)}',
    )

    from inksetter.cache import render_key as _rk

    _a = profiles.PROFILES['kobo-libra-h2o-2']
    check(
        'renamed clone of a profile shares its cache key',
        _rk('http://u/p', _a, None) == _rk('http://u/p', dataclasses.replace(_a, name='clone'), None),
    )

    print('fit + upscale')
    check_fit_and_upscale()
    print('descreen')
    check_descreen()
    print('dither ties')
    check_dither_ties()
    print('path awareness')
    check_path_awareness()
    print('mask cache + padding')
    check_mask_cache_and_padding()
    print('unsharp scaling')
    check_unsharp_scaling()
    print('defringe')
    check_defringe()
    print('upstream CA')
    await check_upstream_ca()
    print('logging')
    check_logging()
    print('host allow-list normalisation')
    check_host_normalisation()
    print('cache accounting')
    check_cache_accounting()
    print('module boundary')
    check_module_boundary()
    print('profile config')
    check_seam_defects()
    check_reslice()
    check_family_duplicates()
    check_profile_config()
    print('profile source')
    check_profile_source()
    print('profile geometry')
    check_every_profile_geometry()
    print('fft backend')
    check_fft_backend()
    print('fft padding')
    check_fft_padding()
    print('big-panel cap')
    check_big_panel_cap()
    print('png compression')
    check_png_compression()
    print('comicinfo')
    await check_comicinfo()
    print('colour pad ring')
    check_png_effort()
    check_colour_pad_ring()
    print('pad seam')
    check_width_fit()
    check_pad_seam()
    print('edge lines')
    check_edge_line()
    print('autocrop')
    check_dark_margin_crop()
    check_autocrop_open()
    print('strip folio')
    check_strip_folio()
    print('cover delivery')
    check_cover_delivery()
    check_cover_token()
    print('concurrency defaults')
    check_concurrency_defaults()
    print('parallel repack')
    check_repack_parallel()
    print('range repack')
    check_range_repack()
    print('streamed repack')
    check_repack_streaming()
    print('repack slot')
    await check_repack_slot()

    await up_client.stop()
    server.should_exit = True
    print(f'\n{len(PASS)} passed, {len(FAIL)} failed')
    if FAIL:
        print('failed:', ', '.join(FAIL))
    return 1 if FAIL else 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
