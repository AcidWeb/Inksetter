"""

python -m tests.smoke

These tests were created entirely by Claude.
The way is shut. It was made by those who are dead, and the dead keep it, until the time comes. The way is shut.

"""

import ast
import base64
import contextlib
import asyncio
import gzip
import inspect
import io
import itertools
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
from inksetter import cores  # noqa: E402
from inksetter import __main__ as entry  # noqa: E402
from inksetter.imaging import cbz, folio, pipeline, profiles, webtoon  # noqa: E402
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


def half_decodable() -> bytes:
    a = np.full((1500, 800, 3), 255, np.uint8)
    a[200:1300, 100:700] = (200, 30, 30)
    blob = bytearray(pyvips.Image.new_from_memory(a.tobytes(), 800, 1500, 3, 'uchar').gifsave_buffer())
    for i in range(len(blob) // 2, len(blob) - 2):
        blob[i] = (blob[i] * 7 + 13) & 0xFF
    return bytes(blob)


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


_RANGED = {'requests': 0, 'bytes': 0, 'whole': 0}
_RANGED_BODY = _cbz_bytes(4)


@upstream.get('/opds/v1.2/ranged/file.cbz')
def _ranged_file(request: Request):
    body = _RANGED_BODY
    rng = request.headers.get('range')
    if 'identity' not in (request.headers.get('accept-encoding') or ''):
        _RANGED['whole'] += 1
        packed = gzip.compress(body)
        return Response(
            packed,
            media_type='application/vnd.comicbook+zip',
            headers={'content-encoding': 'gzip'},
        )
    if not rng:
        _RANGED['whole'] += 1
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


@upstream.get('/opds/v1.2/halfimage')
def _half_image():
    return Response(half_decodable(), media_type='image/gif')


def _epub_bytes(strict: bool) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
        if strict:
            z.writestr(zipfile.ZipInfo('mimetype'), 'application/epub+zip', compress_type=zipfile.ZIP_STORED)
        z.writestr('META-INF/container.xml', '<?xml version="1.0"?><container version="1.0"/>')
        z.writestr('OEBPS/ch1.xhtml', '<html><body><img src="images/p1.jpg"/></body></html>')
        z.writestr('OEBPS/images/p1.jpg', fake_page(1, 400, 600))
        if not strict:
            z.writestr('mimetype', 'application/epub+zip')
    return buf.getvalue()


EPUBS = {
    'strict': (_epub_bytes(True), 'application/octet-stream'),
    'loose': (_epub_bytes(False), 'application/epub+zip'),
}


@upstream.get('/opds/v1.2/epub/{how}/{kind}')
def _epub(how: str, kind: str, request: Request):
    body, media = EPUBS[kind]
    rng = request.headers.get('range')
    if how != 'ranged' or not rng:
        return Response(body, media_type=media)
    first, _, last = rng.partition('=')[2].partition('-')
    lo = int(first)
    hi = int(last) if last else len(body) - 1
    if lo >= len(body) or lo > hi:
        return Response(status_code=416, headers={'content-range': f'bytes */{len(body)}'})
    hi = min(hi, len(body) - 1)
    return Response(
        body[lo : hi + 1],
        status_code=206,
        media_type=media,
        headers={'content-range': f'bytes {lo}-{hi}/{len(body)}'},
    )


BROKEN_URL = 'http://[broken/'
BROKEN_HREF_FEED = FEED.replace(
    '<entry><title>Vol 1</title>',
    '<entry><title>Vol 0</title><link rel="http://opds-spec.org/acquisition" '
    'type="application/vnd.comicbook+zip" href="http://[broken/"/></entry><entry><title>Vol 1</title>',
)


SEAL_KEY = 'SealTest42key'


@upstream.get('/api/opds/{key}/feed')
def _kavita_like_feed(key: str):
    if key != SEAL_KEY:
        return Response('unknown key', status_code=401)
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
 <title>Kavita-like</title>
 <icon>/api/opds/{key}/favicon</icon>
 <link rel="self" type="application/atom+xml;profile=opds-catalog" href="/api/opds/{key}/feed"/>
 <link rel="search" type="application/opensearchdescription+xml" href="/api/opds/{key}/search"/>
 <entry><title>Shelf</title>
  <link rel="subsection" type="application/atom+xml;profile=opds-catalog" href="/api/opds/{key}/feed?shelf=1"/>
 </entry>
 <entry><title>Vol 1</title>
  <link rel="http://opds-spec.org/image" type="image/jpeg" href="/api/opds/{key}/image?seriesId=1&amp;apiKey={key}"/>
  <link rel="http://opds-spec.org/acquisition" type="application/vnd.comicbook+zip"
        href="/api/opds/{key}/series/1/volume/2/chapter/3/download/x.cbz"/>
 </entry>
</feed>"""
    return Response(body, media_type='application/atom+xml;profile=opds-catalog')


@upstream.get('/api/opds/{key}/odd/{shape}')
def _kavita_odd(key: str, shape: str):
    feed = _kavita_like_feed(key).body.decode()
    keyed = json.dumps({'links': [{'href': f'/api/opds/{key}/feed'}]})
    body, media = {
        'html': (feed, 'text/html; charset=utf-8'),
        'utf16': (feed.replace('encoding="utf-8"', 'encoding="utf-16"').encode('utf-16'), 'application/atom+xml'),
        'broken': (keyed[:-2], 'application/opds+json'),
        'utf16-json': (keyed.encode('utf-16'), 'application/octet-stream'),
        'very-deep': ('[' * 200000 + ']' * 200000, 'application/json'),
    }[shape]
    return Response(body, media_type=media)


LONG_JSON = json.dumps(
    {'metadata': {'title': 'Long', 'description': 'x' * 8000}, 'links': [{'href': f'/api/opds/{SEAL_KEY}/feed'}]}
)


@upstream.get('/opds/v1.2/long-json')
def _long_json():
    return Response(LONG_JSON, media_type='application/opds+json')


GUARDED_PAGE = {'hits': 0}


@upstream.get('/opds/v1.2/guarded-page')
def _guarded_page(authorization: str = Header(default='')):
    GUARDED_PAGE['hits'] += 1
    if authorization != 'Basic Zm9vOmJhcg==':
        return Response('denied', status_code=401, headers={'www-authenticate': 'Basic realm="Komga"'})
    return Response(fake_page(4), media_type='image/jpeg')


@upstream.get('/opds/v1.2/untyped/{shape}/{kind}')
def _untyped(shape: str, kind: str):
    body = {'atom': FEED, 'bom': '﻿' + FEED, 'json': json.dumps(FEED_V2), 'json-array': json.dumps([FEED_V2])}[shape]
    media = {'plain': 'text/plain', 'octet': 'application/octet-stream', 'none': None}[kind]
    return Response(body, media_type=media)


@upstream.get('/opds/v1.2/osd-as-xml')
def _osd_as_xml():
    body = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<OpenSearchDescription xmlns="http://a9.com/-/spec/opensearch/1.1/">'
        '<Url type="application/atom+xml" template="/opds/v1.2/search?query={searchTerms}"/>'
        '</OpenSearchDescription>'
    )
    return Response(body, media_type='application/xml')


@upstream.get('/opds/v1.2/hostile')
def _hostile():
    body = FEED.replace(
        '<entry><title>Vol 1</title>',
        '<entry><title>Trap</title><link rel="subsection" type="application/atom+xml;profile=opds-catalog" '
        'href="javascript:alert(document.domain)"/></entry>'
        '<entry><title>Trap 2</title><link rel="subsection" type="application/atom+xml;profile=opds-catalog" '
        'href="data:text/html,&lt;script&gt;alert(1)&lt;/script&gt;"/></entry>'
        '<entry><title>Vol 1</title>',
    )
    return Response(body, media_type='application/atom+xml;profile=opds-catalog')


MOVED_FEED = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
 <title>Moved</title>
 <entry><title>Relative shelf</title>
  <link rel="subsection" type="application/atom+xml;profile=opds-catalog" href="r/0/2"/>
 </entry>
</feed>"""


@upstream.get('/opds/v1.2/moved')
def _moved():
    return Response(status_code=301, headers={'location': '/opds/v1.2/moved/'})


@upstream.get('/opds/v1.2/moved/')
def _moved_here():
    return Response(MOVED_FEED, media_type='application/atom+xml;profile=opds-catalog')


@upstream.get('/opds/v1.2/moved/search.xml')
def _moved_osd():
    body = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<OpenSearchDescription xmlns="http://a9.com/-/spec/opensearch/1.1/">'
        '<Url type="application/atom+xml" template="found?q={searchTerms}"/></OpenSearchDescription>'
    )
    return Response(body, media_type='application/opensearchdescription+xml')


@upstream.get('/opds/v1.2/moved/found')
def _moved_found(q: str = ''):
    return Response(
        MOVED_FEED.replace('<title>Moved</title>', f'<title>found:{q}</title>'), media_type='application/atom+xml'
    )


@upstream.get('/opds/v1.2/moved-search')
def _moved_search():
    return Response(status_code=302, headers={'location': '/opds/v1.2/moved/search.xml'})


@upstream.get('/opds/v1.2/proxyauth')
def _proxy_auth():
    return Response('who are you', status_code=407, headers={'proxy-authenticate': 'Basic realm="corp"'})


@upstream.get('/opds/v1.2/badhref')
def _bad_href():
    return Response(BROKEN_HREF_FEED, media_type='application/atom+xml;profile=opds-catalog')


@upstream.get('/opds/v1.2/badosd')
def _bad_osd():
    body = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<OpenSearchDescription xmlns="http://a9.com/-/spec/opensearch/1.1/">'
        '<Url type="application/atom+xml" template="http://[x/?q={searchTerms}"/></OpenSearchDescription>'
    )
    return Response(body, media_type='application/opensearchdescription+xml')


@upstream.get('/opds/v1.2/badgzip.{kind}')
def _bad_gzip(kind: str):
    media = {'xml': 'application/atom+xml', 'jpg': 'image/jpeg', 'cbz': 'application/vnd.comicbook+zip'}[kind]
    return Response(b'plainly not gzip', media_type=media, headers={'content-encoding': 'gzip'})


@upstream.get('/opds/v1.2/upstream500')
def _upstream_500():
    return Response(status_code=503)


@upstream.get('/opds/v1.2/bad-location')
def _bad_location():
    return Response(status_code=302, headers={'location': 'http://[::1/'})


@upstream.get('/opds/v1.2/deep-json')
def _deep_json():
    return Response('{"links":' * 3000 + '[]' + '}' * 3000, media_type='application/opds+json')


@upstream.get('/opds/v1.2/form-search')
def _form_search():
    return Response(
        '<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"><title>Form search</title>'
        '<link rel="search" type="application/atom+xml" href="/opds/v1.2/series{?search}"/></feed>',
        media_type='application/atom+xml',
    )


class _Escaped:
    status_code = 500

    def __init__(self, exc: Exception) -> None:
        self.text = f'{type(exc).__name__} escaped the route'
        self.headers: dict[str, str] = {}


async def _get(c, url: str, **kw):
    try:
        return await c.get(url, **kw)
    except Exception as exc:
        return _Escaped(exc)


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
            'descreen' in str(exc),
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
        ('a little larger than panel', (1170, 1536)),
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

    small = plain_page(600, 800)
    g = pipeline._geometry(small, clara, tw, th, True)
    check('premise: that page is enlarged', g.upscale > 1.5, f'upscale {g.upscale:.3f}')
    blob, _ = pipeline.render_page(small, clara)
    im = pyvips.Image.new_from_buffer(blob, '')
    a = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width))
    check('an enlarged page still quantises to 16 levels', len(np.unique(a)) <= 16, f'{len(np.unique(a))} levels')

    capped = dataclasses.replace(clara, upscale_max=1.2)
    blob, _ = pipeline.render_page(plain_page(600, 800), capped)
    im = pyvips.Image.new_from_buffer(blob, '')
    inner = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width))
    ink = np.flatnonzero((inner < 200).any(axis=0))
    band = int(ink[-1] - ink[0] + 1) if len(ink) else 0
    want = round((600 - 2 * (600 // 8)) * 1.2)
    check(
        'a capped page is letterboxed, not blown up',
        abs(band - want) <= 4,
        f'its grey band is {band}px wide, {want} at the cap',
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
        ('fit', 'width'),
        ('colour_pad', 'grey'),
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

    noise = np.random.default_rng(12).integers(0, 256, (515, 509), dtype=np.uint8)
    yy, xx = np.mgrid[0:515, 0:509]
    n = pipeline._BAYER_N
    t = pipeline._BAYER[yy % n, xx % n]
    want = np.clip(np.rint((noise.astype(np.float32) + (t - 0.5) * 17.0) / 17.0), 0, 15).astype(np.uint8)
    combos = np.unique(((yy % n) * n + xx % n) * 256 + noise).size
    check('premise: the fixture holds every level in every Bayer cell', combos == n * n * 256, f'{combos}')
    got = pipeline._quantise16(noise, 'bayer')
    check(
        'the dither is the Bayer threshold for every level in every cell, on a page no multiple of the cell',
        np.array_equal(got, want),
        f'{int((got != want).sum())} pixels differ',
    )

    levels = np.tile(np.arange(256, dtype=np.uint8), (1203, 4))[:, :907]
    ramp = pyvips.Image.new_from_memory(np.ascontiguousarray(levels).tobytes(), 907, 1203, 1, 'uchar')
    shipped = profiles.PROFILES['kobo-clara-hd-2e-bw']
    tones = (
        ('the shipped', shipped),
        ('a steep', dataclasses.replace(shipped, black=30, white=200, gamma=1.6)),
        ('a linear', dataclasses.replace(shipped, black=0, white=255, gamma=1.0)),
    )
    for label, prof in tones:
        direct = pipeline._tone(ramp.cast('float'), prof, 255.0)
        tabled = ramp.maplut(pipeline._tone_lut(prof))
        a = np.ndarray(buffer=direct.write_to_memory(), dtype=np.float32, shape=(1203, 907))
        b = np.ndarray(buffer=tabled.write_to_memory(), dtype=np.float32, shape=(1203, 907))
        check(
            f'{label} tone table gives every pixel of every level exactly what the tone curve gives it',
            np.array_equal(a, b),
            f'{int((a != b).sum())} pixels differ, largest by {float(np.abs(a - b).max()):.2e}',
        )

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
    big_colour = fake_colour_page(2400, 3200)
    c_mono, _ = pipeline.render_page(big_colour, dataclasses.replace(colour, descreen='mono'))
    c_none, _ = pipeline.render_page(big_colour, dataclasses.replace(colour, descreen='none'))
    check('descreen=mono leaves the colour path alone', c_mono == c_none)

    c_all, _ = pipeline.render_page(big_colour, dataclasses.replace(colour, descreen='always'))
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

    strip = profiles.PROFILES['kindle-scribe-colorsoft-webtoon']
    heights = (700, 1300, 1900, strip.height)
    check(
        'premise: those page heights pad to four different lengths on their own',
        len({pipeline._next_fast_len(h) for h in heights}) == 4,
        f'{sorted({pipeline._next_fast_len(h) for h in heights})}',
    )
    pipeline._diagonal_attenuation.cache_clear()
    for rows in heights:
        tile = np.full((rows, strip.width, 3), (200, 120, 60), np.uint8)
        tile[rows // 4 : rows // 2, strip.width // 4 : strip.width // 2] = (40, 90, 200)
        cbz._render(('tile', 1, 'x.png', tile, (None, None), 'top'), strip)
    info = pipeline._diagonal_attenuation.cache_info()
    check(
        'a webtoon of short pages builds one defringe mask, not one for every height',
        info.misses == 1 and info.hits == len(heights) - 1,
        f'{info.misses} miss(es), {info.hits} hit(s)',
    )

    n = 1875
    check('premise: the padding produces an odd length', pipeline._next_fast_len(n) == n and n % 2 == 1)
    mask = pipeline._diagonal_attenuation(
        n, n, scribe.defringe_angle, scribe.defringe_tolerance, scribe.defringe_min_freq, scribe.defringe_strength
    )
    corner = mask[: n // 2 + 1, : n // 2 + 1]
    check(
        'on a square grid of odd length the defringe mask is symmetric about its diagonal',
        mask.shape == (n, n // 2 + 1) and np.allclose(corner, corner.T, atol=1e-5),
        f'shape {mask.shape}, off by up to {float(np.abs(corner - corner.T).max()):.2e}',
    )
    floor = 0.1499
    check(
        'premise: at that floor a width of n and of n - 1 leave out different column counts',
        math.ceil(floor * n) != math.ceil(floor * (n - 1)),
    )
    flat = np.ones((n, n // 2 + 1), np.float32)
    found = pipeline._find_peaks(flat, 1, floor, n, profiles.PROFILES['kobo-clara-hd-2e-bw'])
    rows_out, cols_out = int((flat[: n // 2, 0] == 0).sum()), int((flat[0] == 0).sum())
    check(
        "descreen's peak search leaves out a square round DC on a square grid of odd length",
        not found and rows_out == cols_out,
        f'{rows_out} rows by {cols_out} columns, {len(found)} peak(s)',
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

    blurs: list = []

    def recording(self, sigma, **kwargs):
        blurs.append(sigma)
        return pyvips.Operation.call('gaussblur', self, sigma, **kwargs)

    with mock.patch.object(pyvips.Image, 'gaussblur', recording, create=True):
        pipeline.render_page(big, scribe)
    check('a downscaled page is sharpened at the tuned radius', blurs == [scribe.usm_sigma], f'blurred at {blurs}')


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
    axial = diagonal_colour_page(axis='axial')
    a_on, a_off = (wedge_energy(pipeline.render_page(axial, q)[0], 0.0) for q in (scribe, off))
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
    tiny = pyvips.Image.new_from_memory(np.full((4, 4, 3), 180, np.uint8).tobytes(), 4, 4, 3, 'uchar').pngsave_buffer()
    try:
        pipeline.render_page(tiny, scribe)
        check('defringe survives a tiny colour source', True)
    except Exception as exc:
        check('defringe survives a tiny colour source', False, f'{type(exc).__name__}: {exc}')


async def check_upstream_ca() -> None:
    from inksetter.opds import upstream as _up

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
    check('an entry that still has its blob is untouched', c.get('b' * 64) == (b'z' * 70, 'image/png'))


def check_module_boundary() -> None:
    cases = {
        'imaging': (
            [
                'inksetter.imaging.profiles',
                'inksetter.imaging.pipeline',
                'inksetter.imaging.cbz',
                'inksetter.imaging.webtoon',
            ],
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


def _landscape_art() -> bytes:
    a = np.full((900, 1600), 255, np.uint8)
    a[100:800, 100:1500] = 40
    return pyvips.Image.new_from_memory(a.tobytes(), 1600, 900, 1, 'uchar').pngsave_buffer()


def _ink_shape(blob: bytes) -> tuple[int, int]:
    im = pyvips.Image.new_from_buffer(blob, '')
    im = im.colourspace('b-w') if im.bands > 2 else im[0]
    ink = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width)) < 128
    rows, cols = np.flatnonzero(ink.any(axis=1)), np.flatnonzero(ink.any(axis=0))
    if not len(rows):
        return 0, 0
    return int(cols[-1] - cols[0] + 1), int(rows[-1] - rows[0] + 1)


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
        z.writestr('spread.png', _landscape_art())
        z.writestr('notes.txt', 'kept verbatim')
        z.writestr('001.jpg', fake_page(1, 1100, 1500))
    raw = src.getvalue()

    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']

    def run(workers):
        zin = _zf.ZipFile(_io.BytesIO(b''.join(_cbz.repack_iter(_io.BytesIO(raw), prof, workers))))
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
    shapes = [_ink_shape(b) for nm, b in ref if nm[0].isdigit() and b != b'not an image at all']
    check(
        'the landscape spread is rotated to portrait, not cut',
        all(h > w for w, h in shapes),
        f'artwork boxes {shapes}',
    )

    started: list = []

    def slow(job, _profile):
        started.append((job[1], time.perf_counter()))
        time.sleep(0.01 if job[1] == 1 else 0.1)
        return b'x', 'image/png'

    queue = [('page', i, f'{i:03d}.png', b'x') for i in range(1, 41)]
    with mock.patch.object(_cbz, '_render', slow):
        run = _cbz._run_pooled(iter(queue), _zf.ZipFile(_io.BytesIO(), 'w'), prof, 3, 4)
        next(run)
        closed = time.perf_counter()
        run.close()
    begun = sum(at < closed for _, at in started)
    check(
        'premise: renders were still queued when the repack was closed',
        begun < 2 * 3,
        f'{begun} of the {2 * 3} submitted had begun',
    )
    late = [i for i, at in started if at > closed]
    check('closing a pooled repack cancels the renders still queued', not late, f'begun after the close: {late}')


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
        'manga': True,
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

    xml = kavita.render(data, '1', [(1272, 1696)] * 188)
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
        ('AgeRating', 'MA15+'),
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
        'meta': {
            'summary': '-100000',
            'releaseYear': 0,
            'totalCount': 0,
            'language': None,
            'ageRating': 0,
            'writers': [],
            'genres': [],
        },
        'numbers': {},
    }
    xml2 = kavita.render(empty, None, [(100, 200)] * 2).decode()
    check('the -100000 sentinel never reaches the file', '-100000' not in xml2, xml2[:120])
    check('an unset year and count, both 0 in Kavita, are omitted', '<Year>' not in xml2 and '<Count>' not in xml2)
    check('a series with no library type omits <Manga>', '<Manga>' not in xml2)

    comic = dict(data, manga=False)
    check(
        'an archive in a comic library is not called manga',
        '<Manga>' not in kavita.render(comic, '1', [(100, 200)] * 2).decode(),
    )
    for code, rating in ((1, 'Rating Pending'), (4, 'G'), (8, 'Teen'), (10, 'Mature 17+'), (13, 'Adults Only 18+')):
        rated = dict(data, meta=dict(data['meta'], ageRating=code))
        got = etree.fromstring(kavita.render(rated, '1', [(100, 200)] * 2)).findtext('AgeRating')
        check(f'Kavita age rating {code} is written as {rating!r}', got == rating, f'got {got!r}')

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
        off = await kavita.for_download('http://komga/opds/v1.2/books/7/file')
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

    out = b''.join(_cbz.repack_iter(_io.BytesIO(raw), prof, comicinfo=lambda sizes: f'<x n="{len(sizes)}"/>'.encode()))
    z1 = _zf.ZipFile(_io.BytesIO(out))
    check('a ComicInfo is added when the archive has none', 'ComicInfo.xml' in z1.namelist())
    check(
        'it is handed one size per page written',
        z1.read('ComicInfo.xml') == b'<x n="3"/>',
        f'{z1.read("ComicInfo.xml")!r}',
    )

    out2 = b''.join(_cbz.repack_iter(_io.BytesIO(with_own.getvalue()), prof, comicinfo=lambda _n: b'<generated/>'))
    z2 = _zf.ZipFile(_io.BytesIO(out2))
    check("an archive's own ComicInfo is kept verbatim", b'Hand written' in z2.read('ComicInfo.xml'))
    check('and only one is present', sum(n.lower().endswith('comicinfo.xml') for n in z2.namelist()) == 1)

    described = etree.fromstring(kavita.render(data, '1', [(1272, 1696), (1272, 900), None]))
    check(
        'each Page carries its own size, and none when it is unknown',
        [pg.get('ImageHeight') for pg in described.findall('Pages/Page')] == ['1696', '900', None],
    )
    check('PageCount is the number of pages described', described.findtext('PageCount') == '3')

    def listed(sizes):
        return json.dumps(sizes).encode()

    def shipped(archive):
        out = []
        for n in sorted(archive.namelist()):
            if n.lower().endswith(('.png', '.jpg')):
                im = pyvips.Image.new_from_buffer(archive.read(n), '')
                out.append([im.width, im.height])
        return out

    heights = [1280, 1000, 1000, 1280, 640, 1000]
    with_own_info = _zf.ZipFile(_io.BytesIO(_strip_cbz(heights)))
    strip_src = _io.BytesIO()
    with _zf.ZipFile(strip_src, 'w') as z:
        for n in with_own_info.namelist():
            if not n.lower().endswith('comicinfo.xml'):
                z.writestr(n, with_own_info.read(n))
    strip = profiles.PROFILES['kindle-colorsoft-webtoon']
    z4 = _zf.ZipFile(
        _io.BytesIO(
            b''.join(
                _cbz.repack_iter(
                    _io.BytesIO(strip_src.getvalue()), strip, 1, cover=fake_page(9, 800, 1200), comicinfo=listed
                )
            )
        )
    )
    held, told = shipped(z4), json.loads(z4.read('ComicInfo.xml'))
    check('premise: re-slicing changes the page count', len(held) != len(heights) + 1, f'{len(held)} pages')
    check(
        'a re-sliced download describes the pages it holds, cover included',
        told == held,
        f'told {len(told)} pages, holds {len(held)}',
    )

    z5 = _zf.ZipFile(
        _io.BytesIO(b''.join(_cbz.repack_iter(_io.BytesIO(raw), prof, 1, cover=b'not a picture', comicinfo=listed)))
    )
    check('premise: a cover that will not render is left out', len(shipped(z5)) == 3, f'{len(shipped(z5))} pages')
    check('and the metadata does not count it', len(json.loads(z5.read('ComicInfo.xml'))) == 3)

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
            self.library_type = _R(200, 0)
            self.urls = []

        async def post(self, _url, params=None, **_kw):  # noqa: ARG002
            self.auths += 1
            return _R(200, {'token': f'JWT-{self.auths}'})

        async def get(self, url, params=None, headers=None, **_kw):
            self.urls.append(url)
            if not self.reachable:
                raise OSError('connection refused')
            if (headers or {}).get('Authorization', '').removeprefix('Bearer ') in self.reject:
                return _R(401)
            if '/api/Series/metadata' in url:
                self.metadata_calls += 1
                return _R(200, {'summary': 'S', 'writers': [], 'genres': []})
            if '/api/Library/type' in url:
                return self.library_type if (params or {}).get('libraryId') == 3 else _R(400)
            if '/api/Series/volumes' in url:
                return _R(
                    200,
                    [
                        {'id': 265, 'minNumber': 1, 'coverImage': 'v265.png'},
                        {'id': 266, 'minNumber': 2, 'coverImage': ''},
                        {'id': 267, 'minNumber': 100000, 'coverImage': ''},
                    ],
                )
            return _R(200, {'name': 'Example Manga', 'format': 1, 'libraryId': 3})

    class _Stub:
        def __init__(self, api):
            self.raw = api

    shared = kavita._kavita

    async def with_kavita(api, body, ttl=None, fresh=None, catalog='http://kavita:5000/api/opds/KEY'):
        real = kavita.client, kavita.settings, kavita._NEGATIVE_TTL, kavita._POSITIVE_TTL, kavita._kavita
        kavita.client = _Stub(api)
        kavita.settings = dataclasses.replace(settings_mod.settings, upstream_catalog=catalog)
        kavita._kavita = kavita._Kavita()
        if ttl is not None:
            kavita._NEGATIVE_TTL = ttl
        if fresh is not None:
            kavita._POSITIVE_TTL = fresh
        try:
            return await body(kavita._kavita)
        finally:
            kavita.client, kavita.settings, kavita._NEGATIVE_TTL, kavita._POSITIVE_TTL, kavita._kavita = real

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
    check('a series in a manga library is manga', (first or {}).get('manga') is True, f'{(first or {}).get("manga")}')
    check(
        'the Specials volume is given no number',
        267 not in (first or {}).get('numbers', {}),
        f'{first and first["numbers"]}',
    )

    api = _Api()
    again = await with_kavita(api, happy, fresh=0.0)
    check(
        'a successful answer is fetched again once it is stale',
        api.metadata_calls == 2 and again[1] is not again[0],
        f'{api.metadata_calls} metadata calls',
    )

    api = _Api()
    await with_kavita(api, lambda k: k.series_metadata(21), catalog='https://host.lan/kavita/api/opds/KEY')
    check(
        'a Kavita behind a reverse-proxy subpath is asked under that subpath',
        bool(api.urls) and all(u.startswith('https://host.lan/kavita/api/') for u in api.urls),
        f'{api.urls[:1]}',
    )

    class _NotJson(_R):
        def json(self):
            raise ValueError('not JSON')

    for label, answer in (
        ('a comic library', _R(200, 1)),
        ('a failed library lookup', _R(500, 0)),
        ('a library lookup that is not JSON', _NotJson(200)),
    ):
        api = _Api()
        api.library_type = answer
        got = await with_kavita(api, lambda k: k.series_metadata(21))
        check(
            f'a series in {label} is not manga, and keeps its metadata',
            got is not None and got.get('manga') is False and got['series']['name'] == 'Example Manga',
            f'{got and got.get("manga")}',
        )

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
        first = len(api.urls)
        await k.series_metadata(21)
        return first

    first = await with_kavita(api, stays_down, ttl=300.0)
    check(
        'a failure inside the TTL costs one round trip, not two',
        first > 0 and len(api.urls) == first,
        f'{first} requests, then {len(api.urls) - first} more',
    )

    class _SlowAuth(_Api):
        async def post(self, url, params=None, **kw):
            await asyncio.sleep(0.01)
            return await super().post(url, params, **kw)

    api = _SlowAuth()
    await with_kavita(api, lambda k: asyncio.gather(k.series_metadata(21), k.series_metadata(22)))
    check('two series looked up at once share one sign-in', api.auths == 1, f'{api.auths} auths')

    api = _Api()
    api.reject = {'JWT-1'}
    await with_kavita(api, lambda k: k.series_metadata(21))
    volumes = sum('/api/Series/volumes' in u for u in api.urls)
    check('a lookup refused with a stale token asks for the volumes once', volumes == 1, f'{volumes} requests')

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
    check(
        'the Kavita checks leave the process-wide client as they found it',
        kavita._kavita is shared and not shared._series,
        f'{len(shared._series)} series cached in it',
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
    check('the shipped default is the measured one', prof.png_effort == 7, f'{prof.png_effort}')

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

    chroma = pipeline._chroma_of(pipeline.open_image(blob).autorot())
    check(
        'the probe page is routed to the colour path',
        chroma >= prof.mono_chroma_threshold,
        f'chroma {chroma:.1f}, threshold {prof.mono_chroma_threshold}',
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


def check_webtoon_pad() -> None:
    strip = profiles.PROFILES['kindle-colorsoft-webtoon']
    paged = dataclasses.replace(profiles.PROFILES['kindle-colorsoft'], auto_mono=False)
    mono = profiles.PROFILES['kobo-clara-hd-2e-bw']
    limit = round(strip.width * strip.aspect)
    check(
        'every webtoon profile pads as a mono page does',
        all(p.colour_pad == 'mono' for p in profiles.PROFILES.values() if p.reslice),
    )
    check(
        'and every paged profile keeps the border colour',
        all(p.colour_pad == 'border' for p in profiles.PROFILES.values() if not p.reslice),
    )

    rng = np.random.default_rng(11)

    def page(w: int, h: int, side) -> np.ndarray:
        a = np.full((h, w, 3), (240, 170, 60), np.uint8)
        for cols in (slice(0, 60), slice(w - 60, w)):
            a[:, cols] = rng.integers(0, 256, (h, 60, 3), dtype=np.uint8) if side is None else side
        return a

    def pad_of(blob: bytes, cols: int):
        im = pyvips.Image.new_from_buffer(blob, '')
        a = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width, im.bands))
        edge = np.concatenate([a[:, :cols], a[:, -cols:]], axis=1).reshape(-1, im.bands)
        values = np.unique(edge, axis=0)
        return tuple(int(v) for v in values[0]) if len(values) == 1 else None

    cases = (
        ('dark grey', (90, 90, 90), 0, True),
        ('light grey', (170, 170, 170), 255, True),
        ('flat red', (200, 60, 60), 0, True),
        ('flat pink', (250, 190, 190), 255, True),
        ('busy', None, 255, True),
        ('black', (8, 8, 8), 0, False),
    )
    for label, side, level, differs in cases:
        tall = page(strip.width, round(limit * 1.6), side)
        blob = pyvips.Image.new_from_memory(tall.tobytes(), tall.shape[1], tall.shape[0], 3, 'uchar').pngsave_buffer()
        want = (level,) * 3

        got, _ = cbz._render(('tile', 1, 'x.png', tall, (None, None), 'top'), strip)
        check(f'a page past the fold with {label} sides pads {level}', pad_of(got, 100) == want, f'{pad_of(got, 100)}')

        ref, _ = pipeline.render_page(blob, mono)
        check(
            f'premise: a mono page with {label} sides pads {level}', pad_of(ref, 100) == (level,), f'{pad_of(ref, 100)}'
        )

        if differs:
            ctl, _ = pipeline.render_page(blob, paged)
            check(
                f'premise: the border rule would pad {label} sides otherwise',
                len(pad_of(ctl, 100) or ()) == 3 and pad_of(ctl, 100) != want,
                f'{pad_of(ctl, 100)}',
            )

        cover = page(800, 1200, side)
        cblob = pyvips.Image.new_from_memory(cover.tobytes(), 800, 1200, 3, 'uchar').pngsave_buffer()
        got, _ = pipeline.render_page(cblob, profiles.embedded_cover_for(strip))
        check(f'a prepended cover with {label} sides pads {level}', pad_of(got, 40) == want, f'{pad_of(got, 40)}')


def check_gutter_pad() -> None:
    p = profiles.PROFILES['kindle-colorsoft-webtoon']
    limit, scale = p.height, p.width / 800
    black, white = (6, 6, 8), (255, 255, 255)
    ink, paper = (0, 0, 0), (255, 255, 255)

    def at(frac: float) -> int:
        return round(frac * limit / scale)

    def pages(blob: bytes) -> list:
        z = zipfile.ZipFile(io.BytesIO(blob))
        order = sorted(z.infolist(), key=lambda i: cbz.natural_key(i.filename))
        return [t for t in webtoon.strip_tiles(cbz._entries(z, order), p, {}) if t[0] == 'tile']

    def shipped(job) -> np.ndarray:
        im = pyvips.Image.new_from_buffer(cbz._render(job, p)[0], '')
        return np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width, im.bands))

    def one(pixels: np.ndarray):
        values = np.unique(pixels.reshape(-1, pixels.shape[-1]), axis=0)
        return tuple(int(v) for v in values[0]) if len(values) == 1 else None

    def sides(a: np.ndarray):
        return one(np.concatenate([a[:, :4], a[:, -4:]], axis=1))

    def flat_sided(gutters) -> bytes:
        a = np.full((2560, 800, 3), (240, 238, 232), np.uint8)
        yy, xx = np.mgrid[0:2560, 0:800]
        a[(yy // 5 + xx // 5) % 3 == 0] = (60, 90, 200)
        a[:, :40] = a[:, -40:] = (20, 20, 20)
        for row, depth, colour in gutters:
            a[row : row + depth] = colour
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w') as z:
            z.writestr('0000.png', pyvips.Image.new_from_memory(a.tobytes(), 800, 2560, 3, 'uchar').pngsave_buffer())
        return buf.getvalue()

    past_fold = (
        (
            'busy sides between black gutters',
            _strip_cbz([1280, 1280], gutter=[(0, 0, 16, black), (0, at(1.05), 150, black)]),
            paper,
            ink,
        ),
        ('dark, flat sides between white gutters', flat_sided([(0, 16, white), (at(1.05), 150, white)]), ink, paper),
        (
            'busy sides between a white gutter and a black one',
            _strip_cbz([1280, 1280], gutter=[(0, 0, 16), (0, at(1.05), 150, black)]),
            paper,
            paper,
        ),
    )
    for label, blob, today, want in past_fold:
        job = pages(blob)[0]
        check(f'premise: the page with {label} runs past the fold', len(job[3]) > limit, f'{len(job[3])} rows')
        check(
            f'premise: its sides alone would pad it {today}',
            sides(shipped((*job[:4], (None, None), job[5]))) == today,
            f'{sides(shipped((*job[:4], (None, None), job[5])))}',
        )
        got = sides(shipped(job))
        check(f'a page past the fold with {label} is padded {want}', got == want, f'{got}')

    def short_page(label, job, want):
        a = shipped(job)
        check(f'premise: {label} is shorter than the screen', len(job[3]) < limit, f'{len(job[3])} rows')
        check(f'{label} ships at exactly the screen', a.shape[:2] == (limit, p.width), f'{a.shape[1]}x{a.shape[0]}')
        got = one(a[len(job[3]) :])
        check(f'{label} is padded below in {want}', got == want, f'{got}')
        check(f'and {label} opens on its own top, not on its pad', bool((a[0] != want).any()))

    def topped_page(label, job, want):
        a = shipped(job)
        check(f'premise: {label} is shorter than the screen', len(job[3]) < limit, f'{len(job[3])} rows')
        check(f'{label} ships at exactly the screen', a.shape[:2] == (limit, p.width), f'{a.shape[1]}x{a.shape[0]}')
        got = one(a[: limit - len(job[3])])
        check(f'{label} is padded above in {want}', got == want, f'{got}')
        check(f'and {label} ends on its own last row, at the foot of the screen', bool((a[-1] != want).any()))

    def artwork(rows: np.ndarray) -> bool:
        return bool(rows.min() < 235 and rows.max() > 30 and rows.std() > 0)

    with _unplanned():
        after_black = pages(_strip_cbz([1280, 1280], gutter=(0, at(0.60), 150, black)))
    cut_in_black = after_black[0]
    check(
        'premise: a page that opens on artwork and is cut in a black gutter',
        artwork(cut_in_black[3][:10]) and cut_in_black[3][-10:].max() <= 30,
    )
    short_page('a page cut in a black gutter', cut_in_black, ink)
    sliver = after_black[1]
    check(
        'premise: the page after it opens on a sliver of that gutter, then artwork, and no gutter ends it',
        sliver[3][0].max() <= 30 and artwork(sliver[3][4:14]) and artwork(sliver[3][-10:]),
        f'{sliver[3][:6, 0, 0]}',
    )
    topped_page('a page that opens on a sliver of a black gutter and ends in artwork', sliver, paper)

    between = pages(_strip_cbz([1280, 1280, 1280], gutter=[(0, at(1.05), 150), (1, 700, 150, black)]))
    check(
        'premise: the second page opens on a white gutter and is cut in a black one',
        len(between) >= 2 and between[1][3][:10].min() >= 235 and between[1][3][-10:].max() <= 30,
        f'{len(between)} pages',
    )
    short_page('a page between a white gutter and a black one', between[1], ink)

    with _unplanned():
        after = pages(_strip_cbz([1280, 1280], gutter=[(0, 0, 16, black), (0, at(1.05), 150, black)]))
    check(
        'premise: the page after a black gutter past the fold opens on it, and artwork ends it',
        len(after) >= 3 and after[1][3][:10].max() <= 30 and artwork(after[1][3][-10:]) and artwork(after[2][3][:10]),
        f'{len(after)} pages',
    )
    topped_page('a page no gutter ends, after a black one', after[1], ink)

    with _unplanned():
        grey_top = pages(_strip_cbz([1280, 1280], gutter=(0, 0, 150, (60, 60, 60))))[0]
    check(
        'premise: a page that opens on flat grey artwork, and that no gutter ends',
        grey_top[3][:10].std() == 0 and 30 < int(grey_top[3][0, 0, 0]) < 235 and artwork(grey_top[3][-10:]),
        f'{grey_top[3][0, 0]}',
    )
    topped_page('a page with no gutter at either end, its top flat dark grey', grey_top, ink)


def check_strip_width() -> None:
    strips = {n: p for n, p in profiles.PROFILES.items() if p.reslice}
    check('the table carries webtoon profiles', bool(strips), 'none sets reslice = true')
    prof = profiles.PROFILES['kindle-colorsoft-webtoon']
    box = profiles.PROFILES['kindle-colorsoft']
    check(
        'every webtoon profile leaves the strip alone',
        all(not p.autocrop and not p.rotate_wide for p in strips.values()),
        f'{[(n, p.autocrop, p.rotate_wide) for n, p in strips.items() if p.autocrop or p.rotate_wide]}',
    )
    check(
        'and the control shares its panel but is not re-cut',
        (box.width, box.height) == (prof.width, prof.height) and not box.reslice,
        f'{box.width}x{box.height} vs {prof.width}x{prof.height}',
    )

    def strip(w: int, h: int) -> pyvips.Image:
        a = np.full((h, w), 255, np.uint8)
        a[h // 4 : 3 * h // 4, w // 4 : 3 * w // 4] = 40
        return pyvips.Image.new_from_memory(a.tobytes(), w, h, 1, 'uchar')

    tall = (400, 1000, 1280, 2400)
    seen = [pipeline.fit_to_width(strip(800, sh), prof.width, prof) for sh in tall]
    widths = {im.width for im in seen}
    check('the strip reader puts every slice on exactly the panel width', widths == {prof.width}, f'{sorted(widths)}')
    for im, src_h in zip(seen, tall, strict=True):
        want = round(src_h * prof.width / 800)
        check(
            f'a {src_h} px slice keeps its aspect ({want} px tall)',
            abs(im.height - want) <= 1,
            f'got {im.height}, wanted {want}',
        )

    narrow = dataclasses.replace(prof, upscale_max=1.1)
    im = pipeline.fit_to_width(strip(400, 900), narrow.width, narrow)
    want = round(900 * narrow.width / 400)
    check(
        'upscale_max does not cap the strip reader',
        im.width == narrow.width and abs(im.height - want) <= 1,
        f'{im.width}x{im.height}, wanted {narrow.width}x{want} ({narrow.width / 400:.2f}x, cap {narrow.upscale_max})',
    )

    box = profiles.PROFILES['kobo-clara-hd-2e-bw']
    g = pipeline._geometry(strip(800, 1000).pngsave_buffer(), box, box.width, box.height, mono=True)
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
        check(f'premise: a page of tone {tone} is padded', (cw, ch) != (tw, th), f'content {cw}x{ch}')
        if (cw, ch) == (tw, th):
            continue
        out, _mime = pipeline.render_page(blob, prof)
        im = pyvips.Image.new_from_buffer(out, '')
        arr = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width))
        mask = np.ones(arr.shape, bool)
        mask[cy : cy + ch, cx : cx + cw] = False
        pad = np.unique(arr[mask])
        want = 0 if tone < 128 else 255
        check(
            f'pad took the nearer end of the ladder (source tone {tone})',
            len(pad) == 1 and int(pad[0]) == want,
            f'pad={[int(v) for v in pad]}, wanted {want}',
        )

    def level(draw, horizontal=True, vertical=True) -> float:
        a = np.full((1000, 700), 250, np.uint8)
        draw(a)
        im = pyvips.Image.new_from_memory(a.tobytes(), 700, 1000, 1, 'uchar')
        return pipeline._mono_pad_level(im, horizontal, vertical, vertical)

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

    check('premise: the lines below are no thicker than the cap', pipeline._LINE_MAX >= 4, f'{pipeline._LINE_MAX}')
    for thick in (2, 3, 4):

        def thick_line(a, t=thick):
            a[:, w - t :] = 0

        got = pipeline._strip_edge_lines(page(thick_line), thr)
        check(f'a {thick}px line welded to the right edge is shaved whole', got == (0, 0, thick, 0), str(got))

    def frame(a):
        a[:, :3] = 0
        a[:, w - 3 :] = 0
        a[:3, :] = 0
        a[h - 3 :, :] = 0

    got = pipeline._strip_edge_lines(page(frame), thr)
    check('3px lines on all four edges are all shaved whole', got == (3, 3, 3, 3), str(got))
    clean_crop = pipeline._autocrop_box(clean, prof)
    for thick in (1, 2, 3, 4):

        def right_line(a, t=thick):
            a[:, w - t :] = 0

        got = pipeline._autocrop_box(page(right_line), prof)
        check(
            f'autocrop finds exactly the unlined box past a {thick}px edge line',
            got == clean_crop,
            f'{got} vs {clean_crop}',
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
    check('a thick band is not a line, and is left alone', shaved[2] == 0, f'shaved {shaved[2]}')

    def five(a):
        a[:, w - 5 :] = 0

    shaved = pipeline._strip_edge_lines(page(five), thr)
    check('a line thicker than the cap is left alone', shaved[2] == 0, f'shaved {shaved[2]}')

    def grey_line(level):
        def draw(a):
            a[:, w - 1] = level

        return draw

    at = pipeline._strip_edge_lines(page(grey_line(255 - thr)), thr)
    past = pipeline._strip_edge_lines(page(grey_line(254 - thr)), thr)
    check(
        'an edge line is ink only once it is more than the threshold from paper',
        at == (0, 0, 0, 0) and past == (0, 0, 1, 0),
        f'at {255 - thr}: {at}, one level darker: {past}',
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

    thr = prof.autocrop_threshold

    def noise(shape: str, far: int, near: int, seed: int) -> pyvips.Image:
        rng = np.random.default_rng(seed)
        yy, xx = np.mgrid[0:120, 0:160]
        if shape == 'blob':
            density = np.clip(1.2 - np.hypot(xx - 80, yy - 60) / 50, 0, 1)
        else:
            density = np.where(np.minimum.reduce([xx, yy, 159 - xx, 119 - yy]) < 30, 0.3, 0.0)
        a = np.where(rng.random((120, 160)) < density, far, near).astype(np.uint8)
        return pyvips.Image.new_from_memory(a.tobytes(), 160, 120, 1, 'uchar')

    def median_trim(im: pyvips.Image, window: int, fill: float, background: int) -> list[int]:
        e = window // 2
        padded = im.embed(e, e, im.width + 2 * e, im.height + 2 * e, extend='background', background=[fill])
        ranked = padded.rank(window, window, (window * window) // 2).crop(e, e, im.width, im.height)
        return list(ranked.find_trim(threshold=thr, background=background))

    differs, moved = [], 0
    levels = [(0, 255), (0, 255 - thr), (255, 0), (255, thr)]
    for shape, windows in (('blob', (3, 7)), ('frame', (7,))):
        for n, (far, near) in enumerate(levels):
            im = noise(shape, far, near, 10 + n)
            for window in windows:
                for fill in (255.0, 0.0):
                    find = pipeline._opened_trim(im, window, fill, thr)
                    for background in (255, 0):
                        want = median_trim(im, window, fill, background)
                        if shape == 'blob':
                            moved += want != list(im.find_trim(threshold=thr, background=background))
                        if list(find(background)) != want:
                            differs.append(f'{shape} {far}/{near} window {window} pad {fill:.0f} paper {background}')
    check('premise: the median moves the box on the blobs', moved > 0, f'{moved} of 32')
    frame = noise('frame', 0, 255, 10)
    check(
        'premise: on the frame, the pad alone decides whether there is a box',
        median_trim(frame, 7, 255.0, 255) != median_trim(frame, 7, 0.0, 255),
        f'{median_trim(frame, 7, 255.0, 255)} vs {median_trim(frame, 7, 0.0, 255)}',
    )
    check(
        'the opened trim finds the box a median filter would, on either paper, pad and threshold boundary',
        not differs,
        '; '.join(differs[:4]),
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

    small_art = np.full((450, 300), 255, np.uint8)
    small_art[40:410, 30:270] = (np.indices((370, 240)).sum(axis=0) // 6 % 2 * 200 + 30).astype(np.uint8)
    art = pyvips.Image.new_from_memory(small_art.tobytes(), 300, 450, 1, 'uchar').pngsave_buffer()
    as_cover = pipeline.render_page(art, profiles.embedded_cover_for(prof))[0]
    check('premise: the cover renders unlike a page of the book', as_cover != pipeline.render_page(art, prof)[0])
    got, z = pages(b''.join(_cbz.repack_iter(_io.BytesIO(raw), prof, cover=art)))
    check(
        'a cover outside the archive is prepended',
        len(got) == len(base) + 1 and got[0] == '0000.png',
        f'{base} -> {got}',
    )
    check(
        'the prepended page is that cover, rendered as a cover',
        z.read(got[0]) == as_cover,
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
    as_cover = pipeline.render_page(small, profiles.embedded_cover_for(prof))[0]
    as_page = pipeline.render_page(small, prof)[0]
    got2, z2 = pages(b''.join(_cbz.repack_iter(_io.BytesIO(raw), prof, cover=small)))
    check(
        'a small cover is prepended',
        len(got2) == len(base) + 1 and got2[0].startswith('0000.'),
        f'{got2}',
    )
    check('the prepended cover is the UNCAPPED render', z2.read(got2[0]) == as_cover)
    tiny = _io.BytesIO()
    with _zf.ZipFile(tiny, 'w') as zz:
        for i in range(2):
            zz.writestr(f'p{i:03d}.png', small)
    tiny_raw = tiny.getvalue()
    got3, z3 = pages(b''.join(_cbz.repack_iter(_io.BytesIO(tiny_raw), prof)))
    check('premise: a small page IS affected by the cap, so this fixture can tell', as_page != as_cover)
    check('ordinary pages keep the enlargement cap', z3.read(got3[0]) == as_page)

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
        if label != 'grey8 png':
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
        'no more than two renders may be inside an FFT at once',
        pipeline._FFT_SLOTS._initial_value == 2,
        f'{pipeline._FFT_SLOTS._initial_value} slot(s)',
    )
    saved = dict(vars(settings_mod))
    fresh = {k: v for k, v in _os.environ.items() if k != 'RENDER_WORKERS'}
    with mock.patch.dict(_os.environ, fresh, clear=True):
        reloaded = importlib.reload(settings_mod).settings
        check(
            'RENDER_WORKERS defaults to 3',
            reloaded.render_workers == 3,
            f'{reloaded.render_workers}',
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
    silly = {'RENDER_WORKERS': '0', 'REPACK_WORKERS': '-3', 'REPACK_PAGE_WORKERS': '0', 'PREFETCH': '-2'}
    with mock.patch.dict(_os.environ, silly):
        reloaded = importlib.reload(settings_mod).settings
        counts = (reloaded.render_workers, reloaded.repack_workers, reloaded.repack_page_workers, reloaded.prefetch)
        check(
            'a worker count of zero or less is raised to one, and prefetch to none', counts == (1, 1, 1, 0), f'{counts}'
        )
    budgets = {}
    unset = {k: v for k, v in _os.environ.items() if k != 'REPACK_PAGE_WORKERS'}
    with mock.patch.dict(_os.environ, unset, clear=True):
        for n in (1, 2, 4, 6, 8, 12, 16, 32, 128):
            with mock.patch.object(cores, 'available', lambda n=n: n):
                budgets[n] = importlib.reload(settings_mod).settings.repack_page_workers
        _os.environ['REPACK_PAGE_WORKERS'] = '5'
        with mock.patch.object(cores, 'available', lambda: 32):
            overridden = importlib.reload(settings_mod).settings.repack_page_workers
    check(
        'page workers default to half the CPUs, never fewer than 3 nor more than 8',
        budgets == {1: 3, 2: 3, 4: 3, 6: 3, 8: 4, 12: 6, 16: 8, 32: 8, 128: 8},
        f'{budgets}',
    )
    check('and REPACK_PAGE_WORKERS still sets them', overridden == 5, f'{overridden}')
    vars(settings_mod).update(saved)
    check('the reloads leave the settings the app holds in place', settings_mod.settings is app_mod.settings)

    want = pipeline.vips_threads(cores.available())
    check(
        'libvips threads are set at import from the CPUs this process may use',
        pyvips.concurrency_get() == want,
        f'{cores.available()} CPUs, wanted {want}, libvips reports {pyvips.concurrency_get()}',
    )
    check(
        'and it is applied by the pipeline, not by the image',
        'concurrency_set' in inspect.getsource(pipeline),
        'nothing in pipeline.py calls concurrency_set',
    )
    for n, expect in ((1, 2), (2, 2), (4, 2), (8, 4), (16, 8), (32, 8), (128, 8)):
        got = pipeline.vips_threads(n)
        check(f'{n} cores would give {expect} threads', got == expect, f'got {got}')

    before = pyvips.concurrency_get()
    try:
        with mock.patch.dict(_os.environ, {}):
            _os.environ.pop('VIPS_CONCURRENCY', None)
            with mock.patch.object(cores, 'available', lambda: 6):
                pipeline._size_vips()
            budgeted = pyvips.concurrency_get()
            _os.environ['VIPS_CONCURRENCY'] = '5'
            with mock.patch.object(cores, 'available', lambda: 16):
                pipeline._size_vips()
            overridden = pyvips.concurrency_get()
    finally:
        pyvips.concurrency_set(before)
    check(
        'libvips follows the CPU budget, not the machine, and VIPS_CONCURRENCY still wins',
        (budgeted, overridden) == (3, 3),
        f'6 CPUs -> {budgeted} threads; with VIPS_CONCURRENCY set and 16 CPUs -> {overridden}',
    )

    import rapidocr_onnxruntime as _ocr

    asked: list = []

    class _Recorder:
        def __init__(self, **kw):
            asked.append(kw.get('intra_op_num_threads'))

    held = folio._engine, folio._engine_failed
    try:
        for budget in (32, 2):
            folio._engine, folio._engine_failed = None, False
            with (
                mock.patch.object(_ocr, 'RapidOCR', _Recorder),
                mock.patch.object(cores, 'available', lambda b=budget: b),
            ):
                folio._reader()
    finally:
        folio._engine, folio._engine_failed = held
    check(
        'the OCR engine runs on four threads, or fewer where fewer CPUs are allowed',
        asked == [4, 2],
        f'32 CPUs, 2 CPUs -> {asked}',
    )


def check_cache_default() -> None:
    saved = dict(vars(settings_mod))
    local = pathlib.Path(TMP) / 'Local App Data'
    home = pathlib.Path(TMP) / 'home'
    xdg = pathlib.Path(TMP) / 'xdg cache'
    unset = {k: v for k, v in os.environ.items() if k not in ('CACHE_DIR', 'SPOOL_DIR', 'XDG_CACHE_HOME')} | {
        'LOCALAPPDATA': str(local),
        'HOME': str(home),
        'USERPROFILE': str(home),
    }
    found = {}
    with mock.patch.dict(os.environ, unset, clear=True):
        for platform in ('win32', 'linux'):
            with mock.patch.object(sys, 'platform', platform):
                s = importlib.reload(settings_mod).settings
                found[platform] = (s.cache_dir, pathlib.Path(s.spool_dir))
        os.environ['XDG_CACHE_HOME'] = str(xdg)
        with mock.patch.object(sys, 'platform', 'linux'):
            s = importlib.reload(settings_mod).settings
            found['xdg'] = (s.cache_dir, pathlib.Path(s.spool_dir))
        os.environ['CACHE_DIR'] = str(pathlib.Path(TMP) / 'chosen')
        with mock.patch.object(sys, 'platform', 'win32'):
            s = importlib.reload(settings_mod).settings
            chosen = (s.cache_dir, pathlib.Path(s.spool_dir))
    vars(settings_mod).update(saved)
    want = local / 'Inksetter' / 'cache'
    check(
        'on Windows the cache and the spool default to the local app data',
        found['win32'] == (want, want),
        f'{found["win32"]}',
    )
    check(
        'elsewhere they default to the XDG cache home, or ~/.cache without one',
        found['linux'] == (home / '.cache' / 'Inksetter',) * 2 and found['xdg'] == (xdg / 'Inksetter',) * 2,
        f'{found["linux"]}, with XDG_CACHE_HOME {found["xdg"]}',
    )
    docker = (pathlib.Path(entry.__file__).parents[1] / 'Dockerfile').read_text(encoding='utf-8')
    check(
        'and Docker points CACHE_DIR at the volume it mounts',
        re.search(r'^\s*(ENV\s+)?CACHE_DIR="?/cache"?\s*\\?\s*$', docker, re.M) is not None
        and re.search(r'^VOLUME \["/cache"\]', docker, re.M) is not None,
    )
    check(
        'and CACHE_DIR still wins on Windows',
        chosen == (pathlib.Path(TMP) / 'chosen',) * 2,
        f'{chosen}',
    )
    check('the reloads leave the settings the app holds in place', settings_mod.settings is app_mod.settings)


def _load_or_error(path: pathlib.Path) -> dict | str:
    try:
        return entry.load(path)
    except entry.ConfigError as exc:
        return str(exc)


def check_settings_file() -> None:
    root = pathlib.Path(TMP) / 'settings file'
    root.mkdir()
    path = root / 'inksetter.toml'

    places = {}
    with mock.patch.dict(os.environ, {'LOCALAPPDATA': str(root / 'Local'), 'XDG_CONFIG_HOME': str(root / 'xdg')}):
        for platform in ('win32', 'linux'):
            with mock.patch.object(sys, 'platform', platform):
                places[platform] = entry.config_path()
    check(
        'the settings file is in the local app data on Windows, and in the XDG config home elsewhere',
        places
        == {
            'win32': root / 'Local' / 'Inksetter' / 'inksetter.toml',
            'linux': root / 'xdg' / 'inksetter' / 'inksetter.toml',
        },
        f'{places}',
    )

    path.write_bytes(
        '\ufeffupstream_catalog = "http://kavita:5000/api/opds/key"\n'
        "port = 8081\nocr_enabled = false\nCACHE_DIR = 'D:\\Books\\cache'\n".encode()
    )
    values = _load_or_error(path)
    check(
        'a file saved with a byte-order mark reads, keys in either case, true and false spelt as the proxy reads them',
        values
        == {
            'UPSTREAM_CATALOG': 'http://kavita:5000/api/opds/key',
            'PORT': '8081',
            'OCR_ENABLED': 'false',
            'CACHE_DIR': 'D:\\Books\\cache',
        },
        f'{values}',
    )
    check('a missing file is no settings at all', entry.load(root / 'absent.toml') == {})

    refused = {}
    for label, text in (
        ('an unknown key', 'prot = 8081\n'),
        ('a list', 'port = [8081]\n'),
        ('a fraction', 'cache_max_bytes = 8e9\n'),
        ('a table', '[upstream_catalog]\nurl = "http://x"\n'),
        ('a path failing as an escape', 'cache_dir = "D:\\Books\\cache"\n'),
        ('a path failing as a \\U escape', 'x = 1\ncache_dir = "C:\\Users\\me\\cache"\n'),
        ('a path read as a newline', 'cache_dir = "D:\\new"\n'),
    ):
        path.write_text(text, encoding='utf-8')
        refused[label] = _load_or_error(path)
    accepted = [label for label, got in refused.items() if not isinstance(got, str)]
    check('the file refuses what no setting can take', not accepted, f'accepted {accepted}')
    check(
        'and names the key it does not know', "'prot'" in str(refused['an unknown key']), f'{refused["an unknown key"]}'
    )
    unhinted = [label for label in list(refused)[4:] if 'single quotes' not in str(refused[label])]
    check('and says where a Windows path goes', not unhinted, f'no hint for {unhinted}')
    check('but not when the mistake is something else', 'single quotes' not in str(refused['a list']))

    with mock.patch.dict(os.environ, {'PORT': '9000', 'PUBLIC_BASE': ''}):
        os.environ.pop('UPSTREAM_CATALOG', None)
        os.environ.pop('LOG_LEVEL', None)
        entry.apply({'PORT': '8081', 'UPSTREAM_CATALOG': 'http://kavita:5000/api/opds/key'})
        applied = (os.environ['PORT'], os.environ['UPSTREAM_CATALOG'])
        entry.apply({'PUBLIC_BASE': 'http://proxy.lan', 'LOG_LEVEL': ''})
        empties = (os.environ['PUBLIC_BASE'], 'LOG_LEVEL' in os.environ)
    check(
        'a variable set in the environment wins over the file, and the file fills in the rest',
        applied == ('9000', 'http://kavita:5000/api/opds/key'),
        f'{applied}',
    )
    check(
        'an empty value is unset on either side: the file fills an empty variable, and sets nothing with one',
        empties == ('http://proxy.lan', False),
        f'{empties}',
    )

    path.write_text(entry.TEMPLATE, encoding='utf-8')
    shipped = _load_or_error(path)
    path.write_text(re.sub(r'(?m)^# (\w+ = )', r'\1', entry.TEMPLATE), encoding='utf-8')
    examples = _load_or_error(path)
    check('the template sets an empty catalog and nothing else', shipped == {'UPSTREAM_CATALOG': ''}, f'{shipped}')
    check(
        'and every example in it is a setting, reads as written, and shows the default where it has one',
        examples == {'UPSTREAM_CATALOG': '', 'HOST': '0.0.0.0', 'PORT': '8080', 'OCR_ENABLED': 'false'},
        f'{examples}',
    )

    read = set()
    for source in pathlib.Path(entry.__file__).parent.rglob('*.py'):
        text = source.read_text(encoding='utf-8')
        read |= set(re.findall(r"environ\.get\(\s*['\"]([A-Z_]+)['\"]", text))
        read |= set(re.findall(r"environ\[\s*['\"]([A-Z_]+)['\"]\s*\]", text))
        read |= set(re.findall(r"_int\(\s*['\"]([A-Z_]+)['\"]", text))
    read -= {'LOCALAPPDATA', 'XDG_CONFIG_HOME', 'XDG_CACHE_HOME'}
    check(
        'the settings file takes every environment variable the proxy reads, and nothing else',
        read == entry.SETTING_NAMES,
        f'unknown to it {sorted(read - entry.SETTING_NAMES)}, read nowhere {sorted(entry.SETTING_NAMES - read)}',
    )

    local = root / 'first start'
    made = local / 'Inksetter' / 'inksetter.toml'
    opened, served, own = [], [], [True]
    bare = {k: v for k, v in os.environ.items() if k not in entry.SETTING_NAMES} | {
        'LOCALAPPDATA': str(local),
        'HOME': str(root / 'home'),
        'USERPROFILE': str(root / 'home'),
    }
    with (
        mock.patch.dict(os.environ, bare, clear=True),
        mock.patch.object(sys, 'platform', 'win32'),
        mock.patch.object(entry, '_own_console', lambda: own[0]),
        mock.patch.object(entry, '_edit', opened.append),
        mock.patch.object(uvicorn, 'run', lambda _app, **kw: served.append(kw)),
        contextlib.redirect_stdout(io.StringIO()),
    ):
        first = entry.main()
        written = made.read_text(encoding='utf-8') if made.exists() else None
        made.parent.mkdir(parents=True, exist_ok=True)
        made.write_text(entry.TEMPLATE + '# mine\n', encoding='utf-8')
        again = entry.main()
        kept = made.read_text(encoding='utf-8').endswith('# mine\n')
        own[0] = False
        from_terminal = entry.main()
        made.write_text(
            'upstream_catalog = "http://kavita:5000/api/opds/key"\nhost = "127.0.0.1"\nport = 8081\n', encoding='utf-8'
        )
        started = entry.main()
        os.environ['PORT'] = '9000'
        overridden = entry.main()
        os.environ['PORT'] = 'eighty'
        bad_port = entry.main()
        made.write_text('prot = 8081\n', encoding='utf-8')
        os.environ.pop('PORT')
        bad_file = entry.main()
    check(
        'with no catalog anywhere the first start writes the template, opens it and stops',
        (first, written, opened[:1]) == (1, entry.TEMPLATE, [made]),
        f'exit {first}, template written {written == entry.TEMPLATE}, opened {opened[:1]}',
    )
    check(
        'a later start with the catalog still empty keeps the file as it is and opens it again',
        (again, kept, len(opened)) == (1, True, 2),
        f'exit {again}, kept {kept}, opened {len(opened)} times',
    )
    check(
        'from a terminal it only says so: no editor is opened',
        (from_terminal, len(opened)) == (1, 2),
        f'exit {from_terminal}, opened {len(opened)} times',
    )
    check(
        'with the catalog in the file the proxy starts where the file says, and PORT in the environment wins',
        (started, overridden, served)
        == (0, 0, [{'host': '127.0.0.1', 'port': 8081}, {'host': '127.0.0.1', 'port': 9000}]),
        f'exits {started}, {overridden}; served {served}',
    )
    check(
        'a PORT that is not a number, or a file it cannot take, stops it before it starts',
        (bad_port, bad_file, len(served)) == (1, 1, 2),
        f'exits {bad_port}, {bad_file}; served {len(served)} times',
    )

    def _raise(exc):
        def main():
            raise exc

        return main

    ends = {}
    for label, main, is_own in (
        ('a failure in its own window', lambda: 1, True),
        ('a clean stop in its own window', lambda: 0, True),
        ('uvicorn failing to start', _raise(SystemExit(3)), True),
        ('a crash', _raise(RuntimeError('boom')), True),
        ('a failure in a terminal', lambda: 1, False),
    ):
        paused = []
        with (
            mock.patch.object(entry, 'main', main),
            mock.patch.object(entry, '_own_console', lambda is_own=is_own: is_own),
            mock.patch('builtins.input', paused.append),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            try:
                entry.run()
                code = None
            except SystemExit as exc:
                code = exc.code
        ends[label] = (code, bool(paused))
    check(
        'a window of its own stays open after a failure, never after a clean stop or in a terminal',
        ends
        == {
            'a failure in its own window': (1, True),
            'a clean stop in its own window': (0, False),
            'uvicorn failing to start': (3, True),
            'a crash': (1, True),
            'a failure in a terminal': (1, False),
        },
        f'{ends}',
    )


def check_cpu_budget() -> None:
    n = os.process_cpu_count() or 4

    def budget(files: dict[str, str]) -> int:
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            for name, text in files.items():
                (root / name).parent.mkdir(parents=True, exist_ok=True)
                (root / name).write_text(text)
            return cores.available(root)

    check('premise: this machine has more CPUs than the quotas below grant', n > 2, f'{n}')
    cases = [
        ('no cgroup files', {}, n),
        ('v2 without a quota', {'cpu.max': 'max 100000\n'}, n),
        ('a v2 quota of 1.5 CPUs', {'cpu.max': '150000 100000\n'}, 2),
        ('a v2 quota of half a CPU', {'cpu.max': '50000 100000\n'}, 1),
        ('a v2 quota beyond the machine', {'cpu.max': f'{(n + 8) * 100000} 100000\n'}, n),
        ('v1 without a quota', {'cpu/cpu.cfs_quota_us': '-1\n', 'cpu/cpu.cfs_period_us': '100000\n'}, n),
        ('a v1 quota of 2 CPUs', {'cpu/cpu.cfs_quota_us': '200000\n', 'cpu/cpu.cfs_period_us': '100000\n'}, 2),
        ('an unreadable quota', {'cpu.max': 'garbage\n'}, n),
        ('a zero period', {'cpu.max': '100000 0\n'}, n),
    ]
    wrong = [f'{label}: {got}, wanted {want}' for label, files, want in cases if (got := budget(files)) != want]
    check(
        'the CPU budget is the container quota, v1 or v2, rounded up, and never more than the machine',
        not wrong,
        '; '.join(wrong),
    )


def check_pipeline_version() -> None:
    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']
    key = cache_mod.render_key('http://x/p/1', prof, None)
    real = cache_mod.PIPELINE_VERSION
    cache_mod.PIPELINE_VERSION = f'{real}-next'
    try:
        bumped = cache_mod.render_key('http://x/p/1', prof, None)
    finally:
        cache_mod.PIPELINE_VERSION = real
    check('bumping PIPELINE_VERSION changes the render key', bumped != key)


def check_png_compression() -> None:
    from inksetter.cache import render_key as _rk

    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']
    check('default png_compression is 6', prof.png_compression == 6, str(prof.png_compression))

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


def check_upscale_kernel() -> None:
    prof = profiles.PROFILES['kobo-clara-hd-2e-bw']

    check(
        'every shipped profile enlarges bicubic',
        all(q.upscale_kernel == 'cubic' for q in profiles.PROFILES.values()),
        f'{sorted({q.upscale_kernel for q in profiles.PROFILES.values()})}',
    )

    def pixels(im):
        return np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width, im.bands))

    small = pyvips.Image.new_from_buffer(fake_page(4, 700, 1000), '')
    plain = dataclasses.replace(prof, autocrop=False, strip_folio=False)
    g = pipeline._geometry(b'', plain, prof.width, prof.height, mono=True, page=small)
    got = g.image.crop(*g.content)
    want = small.resize(g.upscale, kernel='cubic')
    check('premise: the page is enlarged', g.upscale > 1.2, f'{g.upscale:.3f}')
    check(
        'and an enlarged page is exactly what bicubic makes of it',
        (got.width, got.height) == (want.width, want.height) and np.array_equal(pixels(got), pixels(want)),
        f'{got.width}x{got.height} vs {want.width}x{want.height}',
    )

    strip = profiles.PROFILES['kindle-scribe-colorsoft-webtoon']
    slice_ = pyvips.Image.new_from_buffer(fake_page(2, 800, 1000), '')
    widened = pipeline.fit_to_width(slice_, strip.width, strip)
    wanted = slice_.resize(strip.width / slice_.width, kernel='cubic')
    check(
        'the strip reader widens a webtoon slice bicubic too',
        (widened.width, widened.height) == (wanted.width, wanted.height)
        and np.array_equal(pixels(widened), pixels(wanted)),
        f'{widened.width}x{widened.height} vs {wanted.width}x{wanted.height}',
    )

    refused = []
    for kernel in sorted(profiles.UPSCALE_KERNELS):
        try:
            geom = pipeline._geometry(
                b'', dataclasses.replace(plain, upscale_kernel=kernel), prof.width, prof.height, mono=True, page=small
            )
            geom.image.copy_memory()
            pipeline.fit_to_width(slice_, strip.width, dataclasses.replace(strip, upscale_kernel=kernel)).copy_memory()
        except pyvips.Error:
            refused.append(kernel)
    check('every kernel the profiles accept is one resize takes', not refused, f'refused: {refused}')


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

    elsewhere = 'http://localhost:8899/opds/v1.2/ranged/file.cbz'
    answers = httpx.get(elsewhere, headers={'range': 'bytes=0-0', 'accept-encoding': 'identity'}, timeout=10)
    check(
        'premise: the same server answers under another host name', answers.status_code == 206, f'{answers.status_code}'
    )
    check('a host outside the allow-list is refused a reader', open_range(elsewhere, hdrs) is None)
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

    renders = [0]
    real_render = _cbz.render_page

    def counting(*args, **kwargs):
        renders[0] += 1
        return real_render(*args, **kwargs)

    _cbz.render_page = counting
    try:
        it = _cbz.repack_iter(_io.BytesIO(raw), prof, 1)
        first = next(it)
        at_first = renders[0]
        rest = b''.join(it)
    finally:
        _cbz.render_page = real_render
    check(
        'the first piece of the archive arrives before every page is rendered',
        len(first) > 0 and len(rest) > 0 and at_first < renders[0],
        f'{at_first} of {renders[0]} renders done, {len(first)} bytes then {len(rest)}',
    )


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
        return _app.repack_slots._value

    budget = free_slots()
    check('repack slots start free', budget >= 1, f'{budget} free')

    async def settled(done, timeout: float = 3.0) -> bool:
        waited = 0.0
        while not done() and waited < timeout:
            await asyncio.sleep(0.02)
            waited += 0.02
        return bool(done())

    body = _io.BytesIO(raw)
    gen = _app._repacking(_cbz.repack_iter(_io.BytesIO(raw), prof), body)
    first = await gen.__anext__()
    check('streamed response yields before it finishes', len(first) > 0, f'{len(first)} bytes')
    await gen.aclose()
    check('spool is closed after the reader hangs up', await settled(lambda: body.closed))
    check(
        'slot is free after the reader hangs up mid-repack',
        free_slots() == budget,
        f'{free_slots()} of {budget}',
    )

    body2 = _io.BytesIO(raw)
    gen2 = _app._repacking(_cbz.repack_iter(_io.BytesIO(raw), prof), body2)
    whole = b''.join([blk async for blk in gen2])
    check('fully drained stream is a valid zip', _zf.ZipFile(_io.BytesIO(whole)).testzip() is None)
    check('slot is free after a completed repack', free_slots() == budget, f'{free_slots()} of {budget}')
    check('spool is closed after a completed repack', await settled(lambda: body2.closed))

    wound = threading.Event()

    def slow_to_close():
        try:
            while True:
                yield b'x'
        finally:
            time.sleep(0.8)
            wound.set()

    body3 = _io.BytesIO()
    gen3 = _app._repacking(slow_to_close(), body3)
    await gen3.__anext__()
    gaps = []

    async def ticker():
        last = time.perf_counter()
        for _ in range(10):
            await asyncio.sleep(0.02)
            now = time.perf_counter()
            gaps.append(now - last)
            last = now

    tick = asyncio.create_task(ticker())
    await asyncio.sleep(0.01)
    begun = time.perf_counter()
    await gen3.aclose()
    took = time.perf_counter() - begun
    await tick
    check('hanging up does not wait for the repack to wind down', took < 0.3, f'{took:.2f} s')
    check('nor stall the event loop while it does', max(gaps) < 0.3, f'longest tick {max(gaps):.2f} s')
    check(
        'and the repack still winds down, off the loop',
        await asyncio.to_thread(wound.wait, 3.0) and await settled(lambda: body3.closed),
    )

    def slow_block():
        while True:
            time.sleep(0.4)
            yield b'y'

    busy = slow_block()
    body4 = _io.BytesIO()
    gen4 = _app._repacking(busy, body4)
    pending = asyncio.create_task(gen4.__anext__())
    await asyncio.sleep(0.1)
    pending.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await pending
    check(
        'a hang-up mid-block closes the repack once that block is done',
        await settled(lambda: busy.gi_frame is None and body4.closed),
        f'generator closed: {busy.gi_frame is None}, spool closed: {body4.closed}',
    )


def check_every_profile_geometry() -> None:
    fixture = fake_page(7, 1600, 2400)
    wrong, levels_wrong = [], []
    for name, prof in sorted(profiles.PROFILES.items()):
        if prof.fmt == 'raw':
            continue
        blob, _ = pipeline.render_page(fixture, prof)
        im = pyvips.Image.new_from_buffer(blob, '')
        if (im.width, im.height) != (prof.width, prof.height):
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
    import scipy.fft as _spfft

    check(
        'the pipeline transforms are scipy.fft',
        pipeline.sfft is _spfft,
        getattr(pipeline.sfft, '__name__', repr(pipeline.sfft)),
    )
    transforms = sorted(set(re.findall(r'np\.fft\.(?!r?fftfreq\b)(\w+)\(', _pipeline_source())))
    check('numpy.fft is not used for transforms, only for frequency grids', not transforms, f'{transforms}')


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
    h, w = 1203, 907
    yy, xx = np.mgrid[0:h, 0:w]
    tone = np.where((yy // 2 + xx // 2) % 2 == 0, 90.0, 170.0) + np.random.default_rng(4).uniform(-15, 15, (h, w))
    tone[: h // 6] = 255.0
    tone[h // 2 : h // 2 + 40, w // 8 : w - w // 8] = 0.0
    page = pyvips.Image.new_from_memory(np.clip(tone, 0, 255).astype(np.uint8).tobytes(), w, h, 1, 'uchar')

    def smooth(n: int) -> bool:
        for f in (2, 3, 5, 7):
            while n % f == 0:
                n //= f
        return n == 1

    shapes = []
    real_rfft2 = pipeline.sfft.rfft2

    def recording(a, *args, **kwargs):
        shapes.append(a.shape)
        return real_rfft2(a, *args, **kwargs)

    pipeline.sfft.rfft2 = recording
    try:
        got = pipeline._to_numpy(pipeline.descreen(page, mono, 0.3)).astype(int)
    finally:
        pipeline.sfft.rfft2 = real_rfft2
    check('premise: the page is no fast length itself', not (smooth(h) and smooth(w)), f'{w}x{h}')
    check(
        'descreen transforms the page padded to a 7-smooth size, never smaller',
        shapes and all(smooth(a) and smooth(b) and a >= h and b >= w for a, b in shapes),
        f'{shapes} for a {w}x{h} page',
    )
    pipeline._next_fast_len = lambda k, limit=7: k  # noqa: ARG005
    try:
        ref = pipeline._to_numpy(pipeline.descreen(page, mono, 0.3)).astype(int)
    finally:
        pipeline._next_fast_len = real
    check(
        'premise: descreen notches this screentone', got.std() < tone.std() - 5, f'{tone.std():.1f} -> {got.std():.1f}'
    )
    inner = np.abs(got - ref)[32:-32, 32:-32]
    check(
        'padded descreen matches unpadded within a level beyond 32 px of the edges',
        inner.max() <= 1,
        f'max {inner.max()}, mean {inner.mean():.4f}; whole page max {np.abs(got - ref).max()}',
    )

    handed = []
    real_ifft = pipeline.sfft.ifft

    def keeping(x, *args, **kwargs):
        handed.append(x.copy())
        return real_ifft(x, *args, **kwargs)

    plane = pipeline._to_numpy(page).astype(np.float32)
    pipeline.sfft.ifft = keeping
    try:
        mine = pipeline._descreen_plane(plane, max(mono.descreen_min_freq, 0.3 / 2.0), mono)
    finally:
        pipeline.sfft.ifft = real_ifft
    check(
        'premise: the notch hands its spectrum to the inverse', mine is not None and len(handed) == 1, f'{len(handed)}'
    )
    if mine is not None and handed:
        whole = pipeline.sfft.irfft2(handed[0], s=(real(h), real(w)))[:h, :w]
        gap = float(np.abs(mine - whole).max()) if mine.shape == whole.shape else float('inf')
        check(
            'the inverse in two passes is irfft2 to within a thousandth of a level',
            gap < 1e-3,
            f'shape {mine.shape} against {whole.shape}, largest difference {gap:.6f}',
        )


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
    for name, (w, h), cap in (('kindle-scribe-3', (700, 1000), 2.2), ('kobo-clara-hd-2e-bw', (500, 690), 2.0)):
        prof = dataclasses.replace(profiles.PROFILES[name], autocrop=False, strip_folio=False)
        free = min(prof.width / w, prof.height / h)
        g = pipeline._geometry(fake_page(4, w, h), prof, prof.width, prof.height, True)
        check(f'premise: the page would enlarge {free:.2f}x on {name}, past the cap', free > cap + 0.05)
        check(f'{name} enlarges to its cap and no further', abs(g.upscale - cap) < 1e-9, f'upscale {g.upscale:.3f}')


def check_profile_source() -> None:
    shipped = importlib.resources.files('inksetter.imaging').joinpath('profiles.toml')
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


def _archive_of(*slices: np.ndarray) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as z:
        for i, a in enumerate(slices):
            im = pyvips.Image.new_from_memory(a.tobytes(), a.shape[1], a.shape[0], 3, 'uchar')
            z.writestr(f'{i:04d}.png', im.pngsave_buffer())
    return buf.getvalue()


def _spread(rows: np.ndarray) -> np.ndarray:
    return webtoon._spread(webtoon._lohi(rows))


def _repeated(prev: np.ndarray, nxt: np.ndarray, most: int) -> int:
    for k in range(min(most, len(prev), len(nxt)), 0, -1):
        if np.array_equal(prev[-k], nxt[0]) and np.array_equal(prev[-k:], nxt[:k]):
            if _spread(prev[-k:]).max() > webtoon.STRIP_QUIET:
                return k
    return 0


@contextlib.contextmanager
def _unplanned():
    real = webtoon.STRIP_PLAN_REACH
    webtoon.STRIP_PLAN_REACH = 0.0
    try:
        yield
    finally:
        webtoon.STRIP_PLAN_REACH = real


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
        elif kind == 'black':
            parts.append(np.full((n, width, 3), 8, np.uint8))
        elif kind in ('smooth', 'soft'):
            yy, xx = np.mgrid[0:n, 0:width]
            wave = 127.5 + 127.5 * np.sin(2 * np.pi * xx / 400 + yy / 50)
            if kind == 'soft':
                wave = wave + rng.integers(-20, 21, wave.shape)
            parts.append(np.repeat(np.clip(wave, 0, 255).astype(np.uint8)[..., None], 3, axis=2))
        elif kind in ('text', 'sparse'):
            b = np.full((n, width, 3), 255, np.uint8)
            for x in range(200, width - 200, 14 if kind == 'text' else 40):
                b[:, x : x + 4] = 0
            parts.append(b)
        elif kind == 'bubble':
            b = np.full((n, width, 3), 255, np.uint8)
            b[:, 387:391] = 20
            b[:, 1203:1208] = 20
            parts.append(b)
    return np.concatenate(parts)


def _seam(block: np.ndarray, limit: int) -> int:
    at = webtoon._gutter_seam(webtoon._lohi(block), limit)
    return webtoon._least_busy(block, limit) if at is None else at


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
        int(_spread(row)[0]) > webtoon.STRIP_QUIET and not any(m[0] for m in webtoon._classes(webtoon._lohi(row))[1:]),
        f'spread {int(_spread(row)[0])}',
    )
    first, bub, mid, gap = round(0.60 * L), round(0.03 * L), round(0.42 * L), round(0.10 * L)
    blk = _panel_strip(('art', first), ('bubble', bub), ('art', mid), ('white', gap), ('art', L))
    at = _seam(blk, L)
    check(
        'a page is not cut through a bubble whose outline falls between samples',
        at == first + bub + mid,
        f'cut at {at}, bubble {first}..{first + bub}, gutter from {first + bub + mid}',
    )

    minrun = max(1, round(L * webtoon.STRIP_GUTTER_MIN))
    short = 8
    check('an eight-row gap between lines is under the gutter minimum', short < minrun, f'minimum {minrun}')
    blk = _panel_strip(
        ('art', round(0.80 * L)), ('white', short), ('art', round(0.25 * L) - short), ('white', gap), ('art', L)
    )
    at = _seam(blk, L)
    check(
        'a blank run too short to be a gutter - a gap between lines of text - is not cut in',
        at == round(0.80 * L) + round(0.25 * L),
        f'cut at {at}, short run of {short} at {round(0.80 * L)}, minimum {minrun}',
    )

    lo = int(L * webtoon.STRIP_GUTTER_FLOOR)
    blk = _panel_strip(('art', lo - 150), ('white', 150 + short), ('art', 2 * L))
    at = _seam(blk, L)
    check(
        'a gutter reaching only a few rows into the window is measured whole',
        at == lo + short - 1,
        f'cut at {at}, gutter {lo - 150}..{lo + short}, window from {lo}',
    )

    edge = round(1.05 * L)
    blk = _panel_strip(('art', edge), ('grey', 1), ('white', round(0.40 * L)), ('art', L))
    at = _seam(blk, L)
    check(
        'a gutter that opens on a flat grey line is entered past the line',
        at == edge + 1,
        f'cut at {at}, grey line at {edge}, white from {edge + 1}',
    )
    blk = _panel_strip(('art', edge), ('grey', round(0.20 * L)), ('art', round(0.20 * L)), ('white', gap), ('art', L))
    check(
        'and a flat run with no blank page in it is still entered at its top',
        _seam(blk, L) == edge,
        f'cut at {_seam(blk, L)}, grey run from {edge}',
    )

    wide = round(0.40 * L)
    blk = _panel_strip(('art', edge), ('black', 4), ('grey', 2), ('white', wide), ('art', L))
    at = _seam(blk, L)
    check(
        'a gutter past the fold is entered below the border of the panel above, so the panel keeps it',
        at == edge + 6,
        f'cut at {at}, border {edge}..{edge + 6}, white from {edge + 6}',
    )
    blk = _panel_strip(('art', edge), ('black', 4), ('white', wide), ('art', L))
    at = _seam(blk, L)
    check(
        'even when the border runs straight into white gutter',
        at == edge + 4,
        f'cut at {at}, border {edge}..{edge + 4}, white from {edge + 4}',
    )

    top = round(0.60 * L)
    blk = _panel_strip(('art', top), ('white', gap), ('grey', 1), ('black', 4), ('grey', 2), ('art', 2 * L))
    at = _seam(blk, L)
    check(
        'a page ends in its gutter, not past the border of the panel below, so that panel keeps it',
        at == top + gap - 1,
        f'cut at {at}, white {top}..{top + gap}, border from {top + gap}',
    )
    blk = _panel_strip(('art', top), ('white', gap), ('black', 4), ('art', 2 * L))
    at = _seam(blk, L)
    check(
        'even when that border is drawn straight onto the white',
        at == top + gap - 1,
        f'cut at {at}, white {top}..{top + gap}, border from {top + gap}',
    )
    blk = _panel_strip(('art', top), ('grey', gap), ('art', 2 * L))
    at = _seam(blk, L)
    check(
        'a flat grey run with no gutter in it is still cut at its far side',
        at == top + gap - 1,
        f'cut at {at}, grey {top}..{top + gap}',
    )
    floor = int(L * webtoon.STRIP_MIN_FILL)
    text_top = round(0.873 * L)
    text_bottom = text_top + 5 * 20 + 4 * 10
    lines = [('text', 20), ('white', 10)] * 4 + [('text', 20)]
    blk = _panel_strip(('smooth', text_top), *lines, ('smooth', 2 * L))
    s = blk.astype(np.int16)
    window = (s.max(axis=(1, 2)) - s.min(axis=(1, 2)))[floor:L]
    old = floor + len(window) - 1 - int(np.argmin(window[::-1]))
    check(
        'premise: with no gutter in reach, the least-spread row sits between two lines of text',
        text_top <= old < text_bottom,
        f'least spread at {old}, text {text_top}..{text_bottom}',
    )
    at = _seam(blk, L)
    check(
        'with no gutter in reach, a page is cut neither through lettering nor between its lines',
        not text_top <= at < text_bottom,
        f'cut at {at}, text {text_top}..{text_bottom}',
    )
    sparse = [('sparse', 20), ('white', 10)] * 4 + [('sparse', 20)]
    blk = _panel_strip(('soft', text_top), *sparse, ('soft', 2 * L))
    detail = np.abs(np.diff(blk[:, :, 0].astype(np.int16), axis=1)).mean(axis=1)
    check(
        'premise: the sparse lettering carries less fine detail than the texture round it',
        detail[text_top:text_bottom].mean() < detail[floor:text_top].mean(),
        f'{detail[text_top:text_bottom].mean():.1f} in the text, {detail[floor:text_top].mean():.1f} in the texture',
    )
    at = _seam(blk, L)
    check(
        'it is lettering that is stepped round, not fine detail: sparse text beside busy texture',
        not text_top <= at < text_bottom,
        f'cut at {at}, text {text_top}..{text_bottom}',
    )
    smooth_to = round(0.92 * L)
    blk = _panel_strip(('smooth', smooth_to), ('soft', 2 * L))
    at = _seam(blk, L)
    check(
        'and with no lettering in reach either, it goes where there is least fine detail',
        floor <= at < smooth_to,
        f'cut at {at}, smooth rows {floor}..{smooth_to} of the window',
    )

    head = round(0.25 * L)
    blk = _panel_strip(('art', head), ('white', gap), ('grey', 2 * L))
    at = _seam(blk, L)
    check(
        'a gutter at the head of a long flat run, above the window, does not drag the cut up out of it',
        head + gap < lo and at == L - 1,
        f'cut at {at}, white {head}..{head + gap}, window from {lo}',
    )


def check_search_templates() -> None:
    for template, term, want in (
        ('/search{?query}{&page,limit}', 'bat man', '/search?query=bat%20man'),
        ('/search{?page,query}', 'batman', '/search?query=batman'),
        ('/s?q={searchTerms}&start={startPage?}&n={count?}', 'batman', '/s?q=batman&start=&n='),
        ('/s?q={searchTerms?}', 'x', '/s?q=x'),
        ('/opds/v1.2/search?query={searchTerms}', 'one piece', '/opds/v1.2/search?query=one%20piece'),
    ):
        got = rewrite.fill_search(template, term)
        check(f'{template} is filled as {want}', got == want, got)

    check('an optional searchTerms still marks a search', rewrite.is_search_template('/s?q={searchTerms?}'))
    check('a paging template is not a search', not rewrite.is_search_template('/opds/v2/series{?page}'))

    ctx = rewrite.Ctx(
        profile='kobo-clara-hd-2e-bw', public_base='http://proxy.test', base_url='http://127.0.0.1:8899/opds/v2/catalog'
    )
    doc = {
        'links': [{'rel': 'next', 'href': '/opds/v2/series{?page}', 'type': 'application/opds+json', 'templated': True}]
    }
    link = json.loads(rewrite.rewrite(json.dumps(doc).encode(), ctx, None))['links'][0]
    check(
        'a templated paging link routes as a feed, not a search',
        '/kobo-clara-hd-2e-bw/f/' in link['href'],
        link['href'],
    )
    found = re.search(r'/kobo-clara-hd-2e-bw/\w+/([\w-]+)', link['href'])
    target = rewrite.decode_token(found.group(1)) if found else ''
    check(
        'and its template is expanded with nothing, as RFC 6570 does for undefined variables',
        target == 'http://127.0.0.1:8899/opds/v2/series',
        target,
    )
    check('so it is no longer advertised as templated', 'templated' not in link, f'{link}')


def check_rounding() -> None:
    clara = profiles.PROFILES['kobo-clara-hd-2e-bw']
    plain = dataclasses.replace(
        clara,
        fmt='png8',
        black=0,
        white=255,
        gamma=1.12,
        usm_amount=0.0,
        autocrop=False,
        strip_folio=False,
        descreen='none',
    )
    level = 100
    exact = 255.0 * (level / 255.0) ** (1.0 / plain.gamma)
    check('premise: the tone curve lands this level past the half', exact - int(exact) >= 0.5, f'{exact:.3f}')
    flat = np.full((plain.height, plain.width), level, np.uint8)
    blob = pyvips.Image.new_from_memory(flat.tobytes(), plain.width, plain.height, 1, 'uchar').pngsave_buffer()
    out = pyvips.Image.new_from_buffer(pipeline.render_page(blob, plain)[0], '')
    got = np.unique(np.ndarray(buffer=out.write_to_memory(), dtype=np.uint8, shape=(out.height, out.width)))
    check(
        'a mono page is rounded to 8 bits, not truncated',
        got.tolist() == [round(exact)],
        f'{got.tolist()} for {exact:.3f}',
    )

    yy, xx = np.mgrid[0:1200, 0:900]
    grain = np.random.default_rng(4).uniform(-15, 15, (1200, 900))
    tone = np.where((yy // 2 + xx // 2) % 2 == 0, 90.0, 170.0) + grain
    bands = [tone, tone * 0.8 + 20, tone * 0.6 + 50]
    grey = pyvips.Image.new_from_memory(np.clip(tone, 0, 255).astype(np.uint8).tobytes(), 900, 1200, 1, 'uchar')
    colour = pyvips.Image.new_from_memory(
        np.dstack([np.clip(b, 0, 255).astype(np.uint8) for b in bands]).tobytes(), 900, 1200, 3, 'uchar'
    )
    for label, im in (('grey', grey), ('colour', colour)):
        cleaned = pipeline.descreen(im, clara, 0.3)
        before = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(1200, 900, im.bands)).astype(float)
        after = np.ndarray(buffer=cleaned.write_to_memory(), dtype=np.uint8, shape=(1200, 900, im.bands)).astype(float)
        check(
            f'premise: descreen notches the {label} screentone',
            after.std() < before.std() - 5,
            f'{before.std():.1f} -> {after.std():.1f}',
        )
        drift = float(np.abs(after.mean(axis=(0, 1)) - before.mean(axis=(0, 1))).max())
        check(f'descreen keeps the {label} page at its own mean level', drift < 0.25, f'drift {drift:.3f}')

    inked = tone.copy()
    inked[:200] = 255.0
    inked[500:560, 100:800] = 0.0
    inked[800:1000, 300:340] = 255.0
    stacks = [inked, inked * 0.8 + 20, inked * 0.6 + 50]
    grey = pyvips.Image.new_from_memory(np.clip(inked, 0, 255).astype(np.uint8).tobytes(), 900, 1200, 1, 'uchar')
    colour = pyvips.Image.new_from_memory(
        np.dstack([np.clip(b, 0, 255).astype(np.uint8) for b in stacks]).tobytes(), 900, 1200, 3, 'uchar'
    )
    wide = dataclasses.replace(clara, descreen_deadband=2.0)
    floor = max(wide.descreen_min_freq, 0.3 / 2.0)
    for label, im in (('grey', grey), ('colour', colour)):
        luma = im if im.bands == 1 else im.colourspace('b-w')
        plane = np.ndarray(buffer=luma.write_to_memory(), dtype=np.uint8, shape=(1200, 900)).astype(np.float32)
        notched = pipeline._descreen_plane(plane.copy(), floor, wide)
        whole = notched is not None and notched.shape == plane.shape
        check(
            f'premise: the notch filters the {label} page and returns it at its own size',
            whole,
            f'{None if notched is None else notched.shape} for {plane.shape}',
        )
        if not whole:
            continue
        kept = np.where(np.abs(notched - plane) < wide.descreen_deadband, plane, notched)
        source = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(1200, 900, im.bands)).astype(np.float32)

        def rounded(values: np.ndarray, source=source, plane=plane) -> np.ndarray:
            shifted = values[:, :, None] if source.shape[2] == 1 else source + (values - plane)[:, :, None]
            return np.clip(np.rint(shifted), 0, 255).astype(np.uint8)

        want = rounded(kept)
        out = pipeline.descreen(im, wide, 0.3)
        got = np.ndarray(buffer=out.write_to_memory(), dtype=np.uint8, shape=(1200, 900, im.bands))
        unclipped = notched[:, :, None] if im.bands == 1 else source + (notched - plane)[:, :, None]
        check(
            f'premise: on the {label} page the deadband keeps pixels and the notch rings past black and white',
            (rounded(notched) != want).any() and unclipped.max() > 255.5 and unclipped.min() < -0.5,
            f'deadband decides {int((rounded(notched) != want).sum())} values, '
            f'unclipped range {unclipped.min():.1f}..{unclipped.max():.1f}',
        )
        check(
            f'descreen of the {label} page is the notch, less changes under the deadband, rounded and clipped',
            np.array_equal(got, want),
            f'{int((got != want).sum())} values differ, largest by {int(np.abs(got.astype(int) - want).max())}',
        )

    half = dataclasses.replace(clara, descreen_deadband=0.5)
    plane = np.ndarray(buffer=grey.write_to_memory(), dtype=np.uint8, shape=(1200, 900)).astype(np.float32)
    notched = pipeline._descreen_plane(plane.copy(), floor, half)
    if notched is not None and notched.shape == plane.shape:
        want = np.clip(np.rint(np.where(np.abs(notched - plane) < 0.5, plane, notched)), 0, 255).astype(np.uint8)
        got = pipeline._to_numpy(pipeline.descreen(grey, half, 0.3))
        same, detail = np.array_equal(got, want), f'{int((got != want).sum())} values differ'
    else:
        same, detail = False, f'the notch returned {None if notched is None else notched.shape}'
    check('a grey page at a half-level deadband comes out exactly as the deadband would leave it', same, detail)
    steps = {}
    real_copyto = np.copyto
    for band in (0.5, 2.0):
        seen = []

        def counting(*args, seen=seen, **kwargs):
            seen.append(1)
            return real_copyto(*args, **kwargs)

        with mock.patch.object(pipeline.np, 'copyto', counting):
            pipeline.descreen(grey, dataclasses.replace(clara, descreen_deadband=band), 0.3)
        steps[band] = len(seen)
    check(
        'a grey page skips the deadband step where rounding already does its work, and only there',
        steps == {0.5: 0, 2.0: 1},
        f'deadband steps run: {steps}',
    )


def check_bit_depth() -> None:
    mono = profiles.PROFILES['kobo-clara-hd-2e-bw']
    colour = dataclasses.replace(profiles.PROFILES['kindle-colorsoft'], auto_mono=False)
    strip = profiles.PROFILES['kindle-colorsoft-webtoon']

    def twins(bands: int) -> tuple[bytes, bytes]:
        a = np.full((1400, 900, bands), 255, np.uint8)
        art = min(bands, 3)
        a[200:1200, 150:450, :art] = (200, 40, 40)[:art]
        a[200:1200, 450:750, :art] = (40, 60, 190)[:art]
        a[600:700, 150:750, :art] = 20
        if bands in (2, 4):
            a[..., -1] = 0
            a[200:1200, 150:750, -1] = 255
        deep = a.astype(np.uint16) * 257
        eight = pyvips.Image.new_from_memory(a.tobytes(), 900, 1400, bands, 'uchar')
        sixteen = pyvips.Image.new_from_memory(deep.tobytes(), 900, 1400, bands, 'ushort')
        sixteen = sixteen.copy(interpretation='grey16' if bands < 3 else 'rgb16')
        return eight.pngsave_buffer(), sixteen.pngsave_buffer()

    def pixels(blob: bytes) -> np.ndarray:
        im = pyvips.Image.new_from_buffer(blob, '')
        return np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width, im.bands))

    for bands, kind in ((1, 'grey'), (2, 'grey and alpha'), (3, 'RGB'), (4, 'RGBA')):
        eight, sixteen = twins(bands)
        loaded = pyvips.Image.new_from_buffer(sixteen, '')
        check(f'premise: the 16-bit {kind} fixture loads as 16-bit', loaded.format == 'ushort', loaded.format)
        for prof in (mono, colour):
            a, b = pixels(pipeline.render_page(eight, prof)[0]), pixels(pipeline.render_page(sixteen, prof)[0])
            check(
                f'a 16-bit {kind} page renders exactly like its 8-bit twin ({prof.name})',
                a.shape == b.shape and np.array_equal(a, b),
                f'{int(np.abs(a.astype(int) - b.astype(int)).max()) if a.shape == b.shape else (a.shape, b.shape)}',
            )
        if bands != 2:
            a, b = webtoon._strip_rows(eight, strip), webtoon._strip_rows(sixteen, strip)
            check(
                f'the strip reader reads a 16-bit {kind} slice like its 8-bit twin',
                a is not None and b is not None and np.array_equal(a, b),
            )

    def every_level(bands: int) -> np.ndarray:
        x = np.arange(768) // 3
        a = np.empty((64, 768, bands), np.uint8)
        for c in range(bands):
            alpha = bands in (2, 4) and c == bands - 1
            a[:, :, c] = 255 - x if alpha else (x + 85 * c) % 256
        return a

    def linear(a: np.ndarray, bands: int) -> np.ndarray:
        v = a.astype(np.float64) / 255.0
        out = np.where(v <= 0.04045, v / 12.92, ((v + 0.055) / 1.055) ** 2.4)
        if bands in (2, 4):
            out[..., -1] = v[..., -1]
        return out.astype(np.float32)

    for bands, kind in ((1, 'grey'), (2, 'grey and alpha'), (3, 'RGB'), (4, 'RGBA')):
        a = every_level(bands)
        for label, data, fmt, slack in (
            ('float page in 0-255', a.astype(np.float32), 'float', 0),
            ('float page in 0-1, linear light', linear(a, bands), 'float', 1),
            ('signed 16-bit page', np.rint(a * (32767 / 255)).astype(np.int16), 'short', 0),
            ('signed 8-bit page', np.rint(a * (127 / 255)).astype(np.int8), 'char', 1),
            ('32-bit page', a.astype(np.uint32) * 16843009, 'uint', 0),
        ):
            blob = pyvips.Image.new_from_memory(data.tobytes(), 768, 64, bands, fmt).tiffsave_buffer()
            loaded = pipeline._open(blob)
            got = (
                np.ndarray(buffer=loaded.write_to_memory(), dtype=np.uint8, shape=(64, 768, loaded.bands))
                if loaded.format == 'uchar' and loaded.bands == bands
                else None
            )
            off = None if got is None else int(np.abs(got.astype(int) - a.astype(int)).max())
            check(
                f'a {label} ({kind}) decodes as its 8-bit twin',
                off is not None and off <= slack,
                f'{loaded.format}, {loaded.bands} band(s)' if got is None else f'off by up to {off}',
            )

    halves = np.zeros((1400, 900, 3), np.uint8)
    halves[:, :450] = (200, 30, 30)
    halves[:, 450:] = (30, 30, 200)
    cmyk = pyvips.Image.new_from_memory(halves.tobytes(), 900, 1400, 3, 'uchar').colourspace('cmyk').jpegsave_buffer()
    check('premise: the CMYK fixture loads as CMYK', pyvips.Image.new_from_buffer(cmyk, '').interpretation == 'cmyk')
    rows = webtoon._strip_rows(cmyk, strip)
    mid = None if rows is None else len(rows) // 2
    left = [] if rows is None else rows[mid, 100].tolist()
    right = [] if rows is None else rows[mid, -100].tolist()
    check(
        'the strip reader reads a CMYK slice as the colours it shows',
        bool(left) and left[0] > max(left[1:]) + 80 and right[2] > max(right[:2]) + 50,
        f'left {left} should be red, right {right} blue',
    )


def check_lazy_decode() -> None:
    bad = half_decodable()
    try:
        pyvips.Image.new_from_buffer(bad, '')
        opens = True
    except pyvips.Error:
        opens = False
    check('premise: the broken page opens, and fails only once it is decoded', opens)

    for name in ('kobo-clara-hd-2e-bw', 'kindle-colorsoft'):
        try:
            pipeline.render_page(bad, profiles.PROFILES[name])
            got = 'rendered'
        except pipeline.UnreadableImage:
            got = 'UnreadableImage'
        except Exception as exc:
            got = type(exc).__name__
        check(f'a page that fails mid-decode raises UnreadableImage ({name})', got == 'UnreadableImage', got)

    try:
        got = str(pipeline.same_picture(bad, fake_page(0)))
    except Exception as exc:
        got = type(exc).__name__
    check('same_picture calls a page that fails mid-decode different, rather than raising', got == 'False', got)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as z:
        for i in range(3):
            z.writestr(f'{i:03d}.jpg', fake_page(i, 800, 1500))
        z.writestr('003.gif', bad)
        for i in range(4, 7):
            z.writestr(f'{i:03d}.jpg', fake_page(i, 800, 1500))
    strip = profiles.PROFILES['kindle-colorsoft-webtoon']
    try:
        out = zipfile.ZipFile(io.BytesIO(b''.join(cbz.repack_iter(io.BytesIO(buf.getvalue()), strip, 1))))
        pages = [n for n in out.namelist() if n.lower().endswith(('.png', '.jpg'))]
        got = f'{len(pages)} pages' if out.testzip() is None else 'corrupt archive'
    except Exception as exc:
        pages, got = [], f'{type(exc).__name__}: {str(exc)[:40]}'
    check('a webtoon download survives a slice that fails mid-decode', len(pages) > 0, got)


def check_reslice_edges() -> None:
    import warnings

    import itertools

    strip = profiles.PROFILES['kindle-colorsoft-webtoon']

    def png(a: np.ndarray) -> bytes:
        bands = 1 if a.ndim == 2 else a.shape[2]
        return pyvips.Image.new_from_memory(a.tobytes(), a.shape[1], a.shape[0], bands, 'uchar').pngsave_buffer()

    def archive(entries) -> bytes:
        buf = io.BytesIO()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with zipfile.ZipFile(buf, 'w') as z:
                for name, blob in entries:
                    z.writestr(name, blob)
        return buf.getvalue()

    def infos(blob: bytes):
        z = zipfile.ZipFile(io.BytesIO(blob))
        return z, sorted(z.infolist(), key=lambda i: cbz.natural_key(i.filename))

    rng = np.random.default_rng(5)
    tall = archive([('001.png', png(rng.integers(0, 256, (12800, 800, 3), dtype=np.uint8)))])
    scanned = [0]
    real_lohi = webtoon._lohi

    def counting(rows):
        scanned[0] += len(rows)
        return real_lohi(rows)

    webtoon._lohi = counting
    try:
        z, order = infos(tall)
        tiles = [t[3] for t in webtoon.strip_tiles(cbz._entries(z, order), strip, {}) if t[0] == 'tile']
    finally:
        webtoon._lohi = real_lohi
    total = sum(len(t) for t in tiles)
    decoded = len(webtoon._strip_rows(zipfile.ZipFile(io.BytesIO(tall)).read('001.png'), strip))
    check('premise: one slice twelve screens tall is cut into many pages', len(tiles) >= 10, f'{len(tiles)} pages')
    check('no page keeps the whole slice alive', all(t.base is None for t in tiles))
    check(
        'every row of a tall slice is read once, however many pages and repeats it makes',
        scanned[0] == decoded,
        f'{scanned[0]} rows read of {decoded}, cut into {total}',
    )

    rows = webtoon._strip_rows(png(np.full((1000, 20, 3), 90, np.uint8)), strip)
    check(
        'a sliver of an entry is not blown up past the upscale ceiling',
        rows is not None and rows.shape[1] == strip.width and len(rows) <= 8000,
        f'{None if rows is None else rows.shape}',
    )

    def margined(h: int, i: int) -> np.ndarray:
        a = np.full((h, 800, 3), 255, np.uint8)
        a[:, 120:680] = rng.integers(0, 256, (h, 560, 3), dtype=np.uint8)
        a[:, 120:680, 0] = 40 + i
        return a

    mono = dataclasses.replace(profiles.PROFILES['kindle-pw-6'], reslice=True)
    src = archive([(f'{i:03d}.png', png(margined(h, i))) for i, h in enumerate((1300, 1100, 100))])
    z, order = infos(src)
    cut = [len(t[3]) for t in webtoon.strip_tiles(cbz._entries(z, order), mono, {}) if t[0] == 'tile']
    screen = round(mono.width * mono.aspect)
    want = [(mono.width, screen, min(h, screen)) for h in cut]
    got = []
    out = zipfile.ZipFile(io.BytesIO(b''.join(cbz.repack_iter(io.BytesIO(src), mono, 1))))
    for n in sorted(out.namelist()):
        if n.endswith(('.png', '.jpg')):
            im = pyvips.Image.new_from_buffer(out.read(n), '')
            a = np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width, im.bands))
            inked = int((a.max(axis=(1, 2)) != a.min(axis=(1, 2))).sum())
            got.append((im.width, im.height, inked))
    check('premise: the strip ends on a page wider than it is tall', bool(cut) and cut[-1] < mono.width, f'{cut}')
    check(
        'a re-cut page is never rotated or cropped, whatever the profile asks of a paged book',
        got == want,
        f'got {got} (width, height, rows with ink), wanted {want}',
    )

    dup = archive([('001.png', png(np.full((1400, 900), level, np.uint8))) for level in (60, 200)])
    dup = archive(
        [(i.filename, zipfile.ZipFile(io.BytesIO(dup)).read(i)) for i in zipfile.ZipFile(io.BytesIO(dup)).infolist()]
        + [('002.png', png(np.full((1400, 900), 128, np.uint8)))]
    )
    out = zipfile.ZipFile(
        io.BytesIO(b''.join(cbz.repack_iter(io.BytesIO(dup), profiles.PROFILES['kobo-clara-hd-2e-bw'], 1)))
    )
    means = []
    for n in sorted(out.namelist()):
        if n.endswith(('.png', '.jpg')):
            im = pyvips.Image.new_from_buffer(out.read(n), '')
            means.append(round(im.avg()))
    check(
        'two entries sharing a name both come out, each as itself',
        len(means) == 3 and len(set(means)) == 3,
        f'{means}',
    )

    try:
        got = sorted(['10.jpg', '9.jpg', '1\u00b22.jpg'], key=cbz.natural_key)
    except Exception as exc:
        got = type(exc).__name__
    check(
        'a name with a superscript digit sorts instead of failing the download',
        isinstance(got, list) and got.index('9.jpg') < got.index('10.jpg'),
        ascii(got),
    )

    calls = []
    src = _strip_cbz([1280, 1000, 1000, 1280, 640, 1000])
    out = zipfile.ZipFile(
        io.BytesIO(b''.join(cbz.repack_iter(io.BytesIO(src), strip, 1, progress=lambda d, t: calls.append((d, t)))))
    )
    pages = [n for n in out.namelist() if n.endswith(('.png', '.jpg'))]
    check(
        'webtoon progress moves once per page written',
        len(calls) == len(pages) + 2,
        f'{len(calls)} reports for {len(pages)} pages',
    )
    check(
        'never goes backwards, and reaches the whole only when the strip is done',
        all(a[0] <= b[0] for a, b in itertools.pairwise(calls))
        and calls[-1][0] == calls[-1][1]
        and all(d < t for d, t in calls[:-1]),
        f'{calls}',
    )


def check_strip_rescue() -> None:
    p = profiles.PROFILES['kindle-colorsoft-webtoon']
    limit, w = p.height, p.width
    floor = int(limit * webtoon.STRIP_MIN_FILL)
    rng = np.random.default_rng(31)

    def art(n: int, sides=None) -> np.ndarray:
        a = rng.integers(0, 256, (n, w, 3), dtype=np.uint8)
        if sides is not None:
            a[:, :60] = a[:, -60:] = sides
        return a

    def band(n: int, level: int, spread: int) -> np.ndarray:
        noise = rng.integers(0, spread + 1, (n, w, 3))
        return np.clip(level - noise if level > 128 else level + noise, 0, 255).astype(np.uint8)

    def flat(n: int) -> np.ndarray:
        return np.clip(150 + rng.integers(-10, 11, (n, w, 3)), 0, 255).astype(np.uint8)

    def smooth(n: int) -> np.ndarray:
        return _panel_strip(('smooth', n), width=w)

    def jobs(*parts: np.ndarray):
        z = zipfile.ZipFile(io.BytesIO(_archive_of(np.concatenate(parts))))
        order = sorted(z.infolist(), key=lambda i: cbz.natural_key(i.filename))
        return [t for t in webtoon.strip_tiles(cbz._entries(z, order), p, {}) if t[0] == 'tile']

    def shipped(job, q=p) -> np.ndarray:
        im = pyvips.Image.new_from_buffer(cbz._render(job, q)[0], '')
        return np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width, im.bands))

    def uniform(rows: np.ndarray):
        values = np.unique(rows.reshape(-1, rows.shape[-1]), axis=0)
        return tuple(int(v) for v in values[0]) if len(values) == 1 else None

    mono = profiles.PROFILES['kobo-clara-hd-2e-bw']
    page = np.full((1000, mono.width), 128, np.uint8)
    page[:20], page[-20:] = 0, 255
    blob = pyvips.Image.new_from_memory(page.tobytes(), mono.width, 1000, 1, 'uchar').pngsave_buffer()
    placed = {}
    for fit in ('top', 'bottom'):
        q = dataclasses.replace(mono, fit=fit, autocrop=False, rotate_wide=False)
        g = pipeline._geometry(blob, q, mono.width, mono.height, mono=True)
        placed[fit] = (g.content[1], g.pad)
    check(
        'a top fit puts a short page at the top and pads it from its bottom edge',
        placed['top'] == (0, 255.0),
        f'{placed["top"]}',
    )
    check(
        'a bottom fit puts it at the foot and pads it from its top edge',
        placed['bottom'] == (mono.height - 1000, 0.0),
        f'{placed["bottom"]}',
    )

    for label, level in (('near-white', 255), ('near-black', 8)):
        tall, short = band(80, level, 18), band(40, level, 18)
        loud = _spread(tall) > webtoon.STRIP_QUIET
        check(
            f'premise: the {label} band is too noisy for a gutter in every row, and flat enough for a loose one',
            bool(loud.all()) and bool(((tall.min(axis=(1, 2)) >= 235) | (tall.max(axis=(1, 2)) <= 30)).all()),
            f'{int(loud.sum())}/{len(tall)} too noisy',
        )
        got = jobs(art(1190), tall, art(3000))
        check(
            f'a {label} band that no gutter tier takes ends the page, with the band',
            len(got[0][3]) == 1270 and np.array_equal(got[0][3][-80:], tall),
            f'page of {len(got[0][3])} rows, band 1190..1270',
        )
        check('and the page after it opens on artwork', _spread(got[1][3][:1]).max() > webtoon.STRIP_FAINT)
        got = jobs(art(1190), short, art(3000))
        check(
            f'but a {label} band only as tall as a gap between two lines of lettering does not',
            len(got[0][3]) >= floor,
            f'page of {len(got[0][3])} rows, band 1190..1230, fill floor {floor}',
        )
    past = jobs(art(1900, sides=60), band(80, 255, 18), art(3000, sides=60))
    check(
        'premise: a page cut in a loose band past the fold runs past it',
        len(past[0][3]) == 1900 and len(past[0][3]) > limit,
        f'{len(past[0][3])} rows',
    )
    sides = uniform(np.concatenate([shipped(past[0])[:, :4], shipped(past[0])[:, -4:]], axis=1))
    alone = shipped((*past[0][:4], (None, None), past[0][5]))
    unknown = uniform(np.concatenate([alone[:, :4], alone[:, -4:]], axis=1))
    check('premise: its dark sides alone would pad it black', unknown == (0, 0, 0), f'{unknown}')
    check('it is padded in the colour of the band it was cut in', sides == (255, 255, 255), f'{sides}')
    light = past[1][3].min(axis=(1, 2)) >= 235
    lead = len(light) if light.all() else int(np.argmin(light))
    margin = round(limit * webtoon.STRIP_TOP_MARGIN)
    check(
        'and the page after keeps only the margin of the band, as it would of a gutter',
        0 < lead <= margin,
        f'{lead} light rows at the top, margin {margin}',
    )

    banded = np.concatenate([art(1190), flat(80), art(3000)])
    got = jobs(banded)
    check(
        'a band of near-flat rows ends the page in its middle, and the next page starts there',
        len(got[0][3]) == 1230 and np.array_equal(got[1][3][0], banded[1230]),
        f'page of {len(got[0][3])} rows, band 1190..1270',
    )
    got = jobs(art(1190), flat(40), art(3000))
    check(
        'but not one only as tall as a gap between two lines of lettering',
        len(got[0][3]) >= floor,
        f'page of {len(got[0][3])} rows, band 1190..1230',
    )
    got = jobs(art(1000), band(80, 255, 18), art(220), flat(80), art(3000))
    check('a loose gutter wins over a near-flat band nearer the fold', len(got[0][3]) == 1080, f'{len(got[0][3])} rows')
    got = jobs(art(1000), flat(80), art(920), flat(80), art(3000))
    check('and a band before the fold wins over one past it', len(got[0][3]) == 1040, f'{len(got[0][3])} rows')

    strip = np.concatenate([art(1250), smooth(100), art(150), smooth(60), art(3400)])
    with _unplanned():
        got = jobs(strip)
    first, second, third = (t[3] for t in got[:3])
    check(
        'premise: with no band either, the first page is cut through artwork between the fill floor and the fold',
        floor <= len(first) < limit and _spread(first[-1:]).max() > webtoon.STRIP_FAINT,
        f'{len(first)} rows',
    )
    start = next((i for i in range(len(strip)) if np.array_equal(strip[i], second[0])), -1)
    window = (len(first) - limit // 4, len(first) - round(limit * 0.04))
    check(
        'premise: the smooth stretch lies inside the window, clear of both ends of it',
        window[0] < 1250 and 1350 < window[1],
        f'window {window}, smooth 1250..1350',
    )
    check(
        'the next page starts back at the least busy row above the cut, inside the smooth stretch',
        1250 + 17 <= start < 1350 - 17,
        f'starts at row {start}, smooth 1250..1350',
    )
    repeat = _repeated(first, second, limit // 4)
    check(
        'repeating the end of the page before, at least 4% of a screen and at most a quarter',
        round(limit * 0.04) <= repeat <= limit // 4 and repeat == len(first) - start,
        f'{repeat} rows repeated',
    )
    check(
        'a page that starts that way and ends in artwork again is exactly one screen, so it needs no pad',
        len(second) == limit and _repeated(second, third, limit // 4) > 0,
        f'{len(second)} rows, the page after repeats {_repeated(second, third, limit // 4)}',
    )
    a, b = shipped(got[0]), shipped(got[1])
    check(
        'the page cut through artwork ships at the foot of its screen, padded above',
        a.shape[:2] == (limit, w) and uniform(a[: limit - len(first)]) is not None and uniform(a[-1:]) is None,
        f'pad {uniform(a[: limit - len(first)])} over {limit - len(first)} rows',
    )
    check(
        'and the page after carries on from the top of its own, with no pad anywhere',
        b.shape[:2] == (limit, w) and uniform(b[:1]) is None and uniform(b[-1:]) is None,
    )
    check(
        'the pad above takes the mono rule on the page top when it opened on no gutter',
        uniform(a[: limit - len(first)]) == (255, 255, 255),
        f'{uniform(a[: limit - len(first)])}',
    )
    bordered = dataclasses.replace(p, colour_pad='border')
    with _unplanned():
        opened = jobs(np.full((150, w, 3), 255, np.uint8), art(4000))[0]
    top = limit - len(opened[3])
    lit = shipped(opened, bordered)
    dim = shipped((*opened[:4], (None, None), 'bottom'), bordered)
    check(
        'premise: a page that opens on a white gutter and is cut through artwork, on a border-padding profile',
        opened[4][0] == 255.0 and opened[5] == 'bottom' and top > 0 and uniform(dim[:top]) != (255, 255, 255),
        f'opened on {opened[4][0]}, anchored {opened[5]}, border pad {uniform(dim[:top])}',
    )
    check(
        'even there the pad above is the gutter it opened on',
        uniform(lit[:top]) == (255, 255, 255),
        f'{uniform(lit[:top])}',
    )


def check_strip_plan() -> None:
    p = profiles.PROFILES['kindle-colorsoft-webtoon']
    limit, w = p.height, p.width
    margin = round(limit * webtoon.STRIP_TOP_MARGIN)
    rng = np.random.default_rng(37)

    def art(n: int, sides=None) -> np.ndarray:
        a = rng.integers(0, 256, (n, w, 3), dtype=np.uint8)
        if sides is not None:
            a[:, :60] = a[:, -60:] = sides
        return a

    def gutter(n: int, level: int = 255) -> np.ndarray:
        return np.full((n, w, 3), level, np.uint8)

    def archive(strip: np.ndarray, step=None) -> bytes:
        return _archive_of(*([strip] if step is None else [strip[i : i + step] for i in range(0, len(strip), step)]))

    def entries(blob: bytes):
        z = zipfile.ZipFile(io.BytesIO(blob))
        return cbz._entries(z, sorted(z.infolist(), key=lambda i: cbz.natural_key(i.filename)))

    def jobs(blob: bytes) -> list:
        return [t for t in webtoon.strip_tiles(entries(blob), p, {}) if t[0] == 'tile']

    def both(strip: np.ndarray) -> tuple[list, list]:
        blob = archive(strip)
        with _unplanned():
            today = jobs(blob)
        return jobs(blob), today

    def shipped(job) -> np.ndarray:
        im = pyvips.Image.new_from_buffer(cbz._render(job, p)[0], '')
        return np.ndarray(buffer=im.write_to_memory(), dtype=np.uint8, shape=(im.height, im.width, im.bands))

    def find(strip: np.ndarray, piece: np.ndarray) -> int:
        j = int(np.argmax(piece.max(axis=(1, 2)) != piece.min(axis=(1, 2))))
        hits = np.flatnonzero((strip == piece[j]).all(axis=(1, 2)))
        return int(hits[0]) - j if len(hits) == 1 else -1

    def flat(rows: np.ndarray) -> bool:
        return len(np.unique(rows.reshape(-1, rows.shape[-1]), axis=0)) == 1

    def pad_width(a: np.ndarray) -> tuple[int, int]:
        cols = [flat(a[:, x : x + 1]) for x in range(a.shape[1])]
        left = cols.index(False) if False in cols else len(cols)
        right = cols[::-1].index(False) if False in cols else len(cols)
        return left, right

    def sides(a: np.ndarray):
        values = np.unique(np.concatenate([a[:, :4], a[:, -4:]], axis=1).reshape(-1, a.shape[-1]), axis=0)
        return tuple(int(v) for v in values[0]) if len(values) == 1 else None

    def within(strip: np.ndarray, tiles: list, start: int, end: int) -> list:
        return [t for t in tiles if start <= find(strip, t[3]) < end]

    def fewest(span: int) -> int:
        k = 2
        while (k - (k - 1) * 0.04) * limit / span < 0.80:
            k += 1
        return k

    body = art(round(2.2 * limit))
    least = math.ceil((len(body) + margin) / 1.96)
    quiet = least + 40 - margin
    body[quiet : quiet + 40] = _panel_strip(('smooth', 40), width=w)
    second = art(round(2.2 * limit))
    strip = np.concatenate([gutter(150), body, gutter(150), second, gutter(150), art(600), gutter(150)])
    end = 150 + len(body)
    got, today = both(strip)
    pieces, before = within(strip, got, 0, end), within(strip, today, 0, end)
    heights = [len(t[3]) for t in pieces]
    check(
        "premise: today's rule cuts the artwork through, into pieces shown at different sizes",
        len(before) >= 2 and before[0][5] == 'bottom' and len({min(1.0, limit / len(t[3])) for t in before}) > 1,
        f'{[len(t[3]) for t in before]} rows',
    )
    h = heights[0]
    check(
        'a split artwork is cut into pieces of one height, taller than the screen here, so shrunk alike',
        len(pieces) >= 2 and h > limit and all(x == h for x in heights),
        f'{heights} rows, screen {limit}',
    )
    check('into no fewer pieces than today', len(pieces) >= len(before), f'{len(pieces)} against {len(before)}')
    starts = [find(strip, t[3]) for t in pieces]
    k, span = len(pieces), end - starts[0]
    best = math.ceil(span / (k - (k - 1) * 0.04))
    check(
        'shown no smaller than 80%, and at most 6% smaller than the largest size one height allows',
        limit / h >= 0.80 and best <= h <= best * 1.06,
        f'height {h}, at least {best}, size {limit / h:.3f}',
    )
    check(
        "from the artwork's first row, the gutter's margin above it kept, to the gutter under it",
        starts[0] == 150 - margin and starts[-1] + heights[-1] == end,
        f'starts {starts}, ends {starts[-1] + heights[-1]}, gutter at {end}',
    )
    check(
        'within that, the height is steered so that the cut falls in the quietest rows',
        150 + quiet <= starts[0] + h < 150 + quiet + 40,
        f'cut at {starts[0] + h}, quiet rows {150 + quiet}..{150 + quiet + 40}',
    )
    repeats = [a + len(t[3]) - b for a, t, b in zip(starts, pieces, starts[1:], strict=False)]
    check(
        'each piece repeats the end of the one before, 4% to 25% of a piece',
        all(round(0.04 * h) <= r <= round(0.25 * h) for r in repeats),
        f'{repeats} rows of {h}',
    )
    ships = [shipped(t) for t in pieces]
    pads = {pad_width(a) for a in ships}
    want = (w - round(w * limit / h)) // 2
    check(
        'every piece ships at the panel size, the same pad at either side, none above or below',
        all(a.shape[:2] == (limit, w) and (i == 0 or not flat(a[:1])) and not flat(a[-1:]) for i, a in enumerate(ships))
        and len(pads) == 1
        and all(abs(x - want) <= 1 for x in next(iter(pads))),
        f'side pads {pads}, want {want}',
    )
    after = within(strip, got, end + 150 - margin, end + 150 + len(second))
    check(
        'the next artwork, after a gutter, is planned alike',
        len(after) >= 2 and len({len(t[3]) for t in after}) == 1 and all(t[5] == 'top' for t in after),
        f'{[len(t[3]) for t in after]} rows, anchored {[t[5] for t in after]}',
    )
    check("and the page after both is the one today's rule gives", np.array_equal(got[-1][3], today[-1][3]))
    sliced = jobs(archive(strip, 700))
    check(
        'and it is all cut the same when it comes in many entries',
        len(sliced) == len(got) and all(np.array_equal(a[3], b[3]) for a, b in zip(sliced, got, strict=True)),
        f'{[len(t[3]) for t in sliced]} against {[len(t[3]) for t in got]}',
    )

    for above, below, looks, want, label in (
        (255, 0, 20, (255, 255, 255), 'opens on a white gutter and ends in a black one'),
        (0, 255, 235, (0, 0, 0), 'opens on a black gutter and ends in a white one'),
        (None, 0, 235, (0, 0, 0), 'opens on no gutter and ends in a black one'),
    ):
        head = [] if above is None else [gutter(150, above)]
        body = art(round(2.2 * limit), sides=looks)
        strip = np.concatenate([*head, body, gutter(150, below), art(600), gutter(150)])
        pieces = within(strip, jobs(archive(strip)), 0, len(strip) - 900)
        alone = {sides(shipped((*t[:4], (None, None), 'top'))) for t in pieces}
        check(
            f'premise: an artwork that {label}, whose sides alone would pad it otherwise',
            len(pieces) >= 2 and len(pieces[0][3]) > limit and alone == {tuple(255 - v for v in want)},
            f'{len(pieces)} pieces, sides alone {alone}',
        )
        padded = {sides(shipped(t)) for t in pieces}
        check(
            f'every piece of an artwork that {label} takes the one pad colour of its first gutter',
            padded == {want},
            f'{padded}',
        )

    body = art(round(2.55 * limit))
    strip = np.concatenate([gutter(150), body, gutter(150), art(600), gutter(150)])
    end = 150 + len(body)
    got, today = both(strip)
    pieces, before = within(strip, got, 0, end), within(strip, today, 0, end)
    check(
        "premise: today's rule cuts an artwork in two, one piece shown below 80%",
        len(before) == 2 and min(limit / len(t[3]) for t in before) < 0.80 and fewest(len(body) + margin) == 3,
        f'{[len(t[3]) for t in before]} rows',
    )
    check(
        'the 80% floor takes a piece more than that, no smaller than 80% and of one height bar a short last',
        len(pieces) == 3
        and all(limit / len(t[3]) >= 0.80 for t in pieces)
        and len({len(t[3]) for t in pieces[:-1]}) == 1
        and len(pieces[-1][3]) <= len(pieces[0][3]),
        f'{[len(t[3]) for t in pieces]} rows',
    )

    body = art(round(2.4 * limit))
    stretch = round(limit / 0.80) + 60 - margin
    body[stretch : stretch + 40] = _panel_strip(('smooth', 40), width=w)
    strip = np.concatenate([gutter(150), body, gutter(150), art(600), gutter(150)])
    pieces = within(strip, jobs(archive(strip)), 0, 150 + len(body))
    check(
        'the allowance for a quiet cut never takes a piece below 80%, even with the quietest rows past it',
        len(pieces) == 2 and all(limit / len(t[3]) >= 0.80 for t in pieces),
        f'{[len(t[3]) for t in pieces]} rows, quiet rows from {150 + stretch}',
    )

    body = art(round(6.9 * limit))
    strip = np.concatenate([gutter(150), body, gutter(150), art(600), gutter(150)])
    end = 150 + len(body)
    got, today = both(strip)
    pieces, before = within(strip, got, 0, end), within(strip, today, 0, end)
    check(
        'premise: a long artwork today takes more pieces than the 80% floor asks for',
        len(before) > fewest(len(body) + margin),
        f'{len(before)} pieces today, {fewest(len(body) + margin)} would do',
    )
    check(
        'it is cut into as many as today, of one height',
        len(pieces) == len(before) and len({len(t[3]) for t in pieces[:-1]}) == 1,
        f'{[len(t[3]) for t in pieces]} rows',
    )

    span = round(2.1 * limit)
    placed = webtoon._place(np.linspace(1.0, 0.0, span + 20), span, limit, 3, limit)
    check(
        'where whole screens leave room to spare, the last piece still keeps at least half a screen',
        placed is not None and limit // 2 <= span - placed[1][-1] <= limit,
        f'{None if placed is None else span - placed[1][-1]} rows',
    )

    rows = art(5000)
    scored = webtoon._busy(rows, 17)
    check(
        'busyness is scored for every row of a long artwork, each as it would be alone',
        len(scored) == len(rows)
        and np.allclose(scored[2040:2060], webtoon._busy(rows[1900:2200], 17)[140:160])
        and np.allclose(scored[4090:4110], webtoon._busy(rows[4000:4300], 17)[90:110]),
        f'{len(scored)} scores for {len(rows)} rows',
    )
    wanted = np.array([0, 5, 700, 2047, 2048, 3333, 4990, 4999])
    lazy = webtoon._Busy(rows, 17)
    check(
        'and scored on demand, as the planner reads it, a row scores as it does with the whole artwork read',
        np.allclose(lazy[wanted], scored[wanted], rtol=1e-12, atol=0) and np.allclose(lazy[123], scored[123]),
        f'{lazy[wanted]} against {scored[wanted]}',
    )

    body = art(round(2.2 * limit))
    loose = np.clip(255 - rng.integers(0, 19, (80, w, 3)), 0, 255).astype(np.uint8)
    strip = np.concatenate([gutter(150), body, loose, art(3000), gutter(150)])
    got = jobs(archive(strip))
    pieces = within(strip, got, 0, 150 + len(body))
    nxt = got[len(pieces)][3]
    light = nxt.min(axis=(1, 2)) >= 235
    lead = len(light) if light.all() else int(np.argmin(light))
    check(
        'premise: an artwork cut through that ends in a band too noisy for a gutter',
        len(pieces) >= 2 and len({len(t[3]) for t in pieces}) == 1 and bool((_spread(loose) > 15).all()),
        f'{[len(t[3]) for t in pieces]} rows',
    )
    check(
        'the page after it keeps only the margin of that band, as after any gutter',
        find(strip, pieces[-1][3]) + len(pieces[-1][3]) == 150 + len(body) and 0 < lead <= margin,
        f'{lead} light rows at the top, margin {margin}',
    )

    span = round(2.2 * limit)
    even = webtoon._even(art(span + 40), span, 4, limit)
    check(
        'asked for more pieces than whole screens can take, it takes fewer, at full size',
        even is not None and even[0] == limit and len(even[1]) == 3,
        f'{even}',
    )

    body = art(round(1.67 * limit))
    body[limit - 30 - margin : limit - margin] = _panel_strip(('smooth', 30), width=w)
    band = np.clip(150 + rng.integers(-10, 11, (80, w, 3)), 0, 255).astype(np.uint8)
    strip = np.concatenate([gutter(150), body, band, art(2500), gutter(150)])
    got, before = both(strip)
    end = find(strip, before[1][3]) + len(before[1][3])
    pieces = within(strip, got, 0, end)
    heights = [len(t[3]) for t in pieces]
    check(
        'premise: an artwork no gutter ends within two screens, but a near-flat band just after',
        before[0][5] == 'bottom'
        and 150 + len(body) <= end < 150 + len(body) + 80
        and 2 * limit - (end - 150 + margin) > limit // 4
        and len(pieces) == 2,
        f'{heights} rows, ends at {end}, two screens would repeat {2 * limit - (end - 150 + margin)}',
    )
    starts = [find(strip, t[3]) for t in pieces]
    last = shipped(pieces[-1])
    check(
        'where whole screens would repeat more than a quarter, they stay whole and the last ends short, '
        'repeating at least 4% though the quietest rows lie just above the cut',
        heights[0] == limit
        and limit // 2 <= heights[1] < limit
        and starts[1] + heights[1] == end
        and round(0.04 * limit) <= starts[0] + limit - starts[1] <= limit // 4,
        f'{heights} rows, repeat {starts[0] + limit - starts[1]}',
    )
    check(
        'shipped at full size, padded below like any page that ends before the fold',
        last.shape[:2] == (limit, w) and pad_width(last) == (0, 0) and flat(last[heights[1] :]),
    )

    strip = np.concatenate([gutter(150), art(9 * limit), gutter(150), art(600), gutter(150)])
    pulled = [0]

    def counted():
        for e in entries(archive(strip, 500)):
            pulled[0] += 1
            yield e

    first = next(t for t in webtoon.strip_tiles(counted(), p, {}) if t[0] == 'tile')
    check(
        "an artwork longer than eight screens is cut as it comes, by today's rule",
        first[5] == 'bottom' and len(first[3]) < limit,
        f'first page {len(first[3])} rows, anchored {first[5]}',
    )
    check(
        'and no more than eight screens are read to find that out',
        pulled[0] * 500 <= 8 * limit + 2 * 500 + 150,
        f'{pulled[0] * 500} rows read before the first page, eight screens {8 * limit}',
    )


def check_reslice() -> None:
    p = profiles.PROFILES['kindle-colorsoft-webtoon']
    limit = round(p.width * p.aspect)
    floor = int(limit * webtoon.STRIP_MIN_FILL)
    check('the webtoon profiles ask to be re-cut', p.reslice)

    heights = [1280, 1000, 1000, 1, 1280, 640, 1000]
    src = _strip_cbz(heights)
    recut = b''.join(cbz.repack_iter(io.BytesIO(src), p, 1))
    pages = _pages_of(recut)
    held = zipfile.ZipFile(io.BytesIO(src))
    order = sorted(held.infolist(), key=lambda i: cbz.natural_key(i.filename))
    recut_tiles = [t[3] for t in webtoon.strip_tiles(cbz._entries(held, order), p, {}) if t[0] == 'tile']
    cuts = [len(t) for t in recut_tiles]

    check('a re-cut strip still produces pages', bool(pages) and len(pages) == len(cuts), f'{len(pages)}')
    sizes = {(w, h) for _, w, h in pages}
    check(
        'every page ships at exactly the panel size, so with the status bar off the zoom is 1.0',
        sizes == {(p.width, limit)},
        f'{sizes}',
    )
    short = [h for h in cuts[:-1] if h < floor]
    check('and, with no gutter in reach, none is cut shorter than the fill floor bar the last', not short, f'{short}')

    scaled = 0
    for i in range(len(heights)):
        one = pyvips.Image.new_from_buffer(zipfile.ZipFile(io.BytesIO(src)).read(f'{i:04d}.png'), '')
        scaled += pipeline.fit_to_width(one, p.width, p).height
    repeats = [_repeated(a, b, max(limit, len(a)) // 4 + 1) for a, b in itertools.pairwise(recut_tiles)]
    shown = [max(limit, len(a)) for a in recut_tiles[:-1]]
    check(
        'every row of the strip is cut into a page, once, bar what a page repeats of the one before',
        sum(cuts) - sum(repeats) == scaled,
        f'{sum(cuts)} cut, {sum(repeats)} repeated, vs {scaled} in',
    )
    check('premise: with no gutter in reach, pages do repeat some of the one before', any(repeats), f'{repeats}')
    check(
        'and a repeat is at least 4% and at most a quarter of the page before, as far as it is shown',
        all(r == 0 or round(n * 0.04) <= r <= round(n * 0.25) for r, n in zip(repeats, shown, strict=True)),
        f'{repeats}, pages {shown}, screen {limit}',
    )
    check('a one-pixel entry does not derail it', len(pages) >= 3, f'{len(pages)} pages')
    check(
        'the page numbers are wide enough to sort past 9999',
        all(re.fullmatch(r'\d{5}\.(png|jpg)', n) for n, _, _ in pages),
        f'{pages[0][0]}',
    )
    check('non-image entries are still carried over', 'ComicInfo.xml' in zipfile.ZipFile(io.BytesIO(recut)).namelist())

    scale = p.width / 800
    band, deep = 990, 24
    lo, hi = round(band * scale), round((band + deep) * scale)
    check('the gutter really is inside the window', floor <= lo and hi <= limit, f'{lo}..{hi} in {floor}..{limit}')

    def tiles(blob):
        z = zipfile.ZipFile(io.BytesIO(blob))
        order = sorted(z.infolist(), key=lambda i: cbz.natural_key(i.filename))
        return [len(t[3]) for t in webtoon.strip_tiles(cbz._entries(z, order), p, {}) if t[0] == 'tile']

    seam = tiles(_strip_cbz([1280, 1280], gutter=(0, band)))[0]
    check(
        'the cut is drawn to a quiet row rather than the nominal one',
        lo - 6 <= seam <= hi + 6,
        f'cut at {seam}, gutter spans {lo}..{hi}, panel would be {limit}',
    )
    check(
        'and to the far side of it, so the page fills up',
        seam >= (lo + hi) // 2,
        f'cut at {seam}, gutter spans {lo}..{hi}',
    )

    reach = round(limit * webtoon.STRIP_OVERSHOOT)
    low = int(limit * webtoon.STRIP_GUTTER_FLOOR)

    def at(frac):
        return round(frac * limit / scale)

    def band(frac):
        return round(at(frac) * scale), round((at(frac) + 24) * scale)

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
        'but one below the gutter floor still does not, nor drags the cut up to it',
        t[0] >= floor and not (lo30 - 4 <= t[0] <= hi30 + 4),
        f'page {t[0]}, gutter {lo30}..{hi30}, gutter floor {low}, fill floor {floor}',
    )
    lo105, _ = band(1.05)
    past_fold = _strip_cbz([1280, 1280], gutter=(0, at(1.05)))
    t = tiles(past_fold)
    check(
        'with nothing above the fold, a gutter just past it is taken',
        limit < t[0] <= reach and lo105 - 4 <= t[0] <= lo105 + 4,
        f'page {t[0]}, gutter starts {lo105}, panel {limit}, reach {reach}',
    )
    check(
        'no page is shrunk below half size',
        1 / webtoon.STRIP_OVERSHOOT >= 0.50 - 1e-9,
        f'overshoot {webtoon.STRIP_OVERSHOOT} shrinks to {1 / webtoon.STRIP_OVERSHOOT:.0%}',
    )
    beyond = at(webtoon.STRIP_OVERSHOOT + 0.10)
    with _unplanned():
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
        'a gutter with a little noise in it, enlarged with its slice, still counts as one',
        lo60 - 4 <= t[0] <= hi60 + 4,
        f'page {t[0]}, gutter {lo60}..{hi60}',
    )

    native = round(1280 * scale)
    g60, deep60 = round(0.60 * limit), round(24 * scale)
    noisy = _strip_cbz([native, native], width=p.width, gutter=(0, g60, deep60), noise=15)
    drawn = pyvips.Image.new_from_buffer(zipfile.ZipFile(io.BytesIO(noisy)).read('0000.png'), '')
    rows = np.ndarray(buffer=drawn.write_to_memory(), dtype=np.uint8, shape=(drawn.height, drawn.width, 3))
    spreads = rows[g60 : g60 + deep60].astype(np.int16)
    spreads = spreads.max(axis=(1, 2)) - spreads.min(axis=(1, 2))
    check(
        'premise: drawn at the panel width, the noisy gutter spreads exactly 15',
        drawn.width == p.width and bool((spreads == 15).all()),
        f'{drawn.width} wide, spreads {sorted(set(spreads.tolist()))}',
    )
    t = tiles(noisy)
    check(
        'and a gutter as noisy as that still counts as one',
        g60 - 4 <= t[0] <= g60 + deep60 + 4,
        f'page {t[0]}, gutter {g60}..{g60 + deep60}',
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
        parts = [
            webtoon._strip_rows(z.read(n), p) for n in sorted(z.namelist(), key=cbz.natural_key) if cbz.is_image(n)
        ]
        return np.concatenate([x for x in parts if x is not None])

    def cut(blob):
        z = zipfile.ZipFile(io.BytesIO(blob))
        order = sorted(z.infolist(), key=lambda i: cbz.natural_key(i.filename))
        return [t[3] for t in webtoon.strip_tiles(cbz._entries(z, order), p, {}) if t[0] == 'tile']

    def blank_rows(rows):
        s = rows.astype(np.int16)
        lo, hi = s.min(axis=(1, 2)), s.max(axis=(1, 2))
        return (hi - lo <= webtoon.STRIP_QUIET) & ((lo >= 235) | (hi <= 30))

    minrun = max(1, round(limit * webtoon.STRIP_GUTTER_MIN))

    def flat_rows(rows):
        s = rows.astype(np.int16)
        return s.max(axis=(1, 2)) - s.min(axis=(1, 2)) <= webtoon.STRIP_QUIET

    def droppable(rows):
        s = rows.astype(np.int16)
        near = s.max(axis=(1, 2)) - s.min(axis=(1, 2)) <= webtoon.STRIP_FAINT
        soft = near & ~blank_rows(rows)
        edges = np.flatnonzero(np.diff(np.concatenate([[0], soft.astype(np.int8), [0]])))
        artwork = np.zeros(len(rows), bool)
        for start, stop in zip(edges[::2], edges[1::2], strict=True):
            artwork[start:stop] = stop - start >= minrun
        return near & ~artwork

    def only_gutter_gone(blob, pieces):
        strip = strip_of(blob)
        blank = droppable(strip)
        i = gone = prev = 0
        for piece in pieces:
            for back in range(min(max(limit, prev) // 4, i, len(piece)), 0, -1):
                if not np.array_equal(strip[i - back], piece[0]) or blank[i - back : i].all():
                    continue
                if np.array_equal(strip[i - back : i], piece[:back]):
                    i -= back
                    break
            j, prev = 0, len(piece)
            while j < len(piece):
                if i == len(strip):
                    return False, gone, 'output ran past the strip'
                if np.array_equal(strip[i], piece[j]):
                    i, j = i + 1, j + 1
                elif blank[i]:
                    i, gone = i + 1, gone + 1
                else:
                    return False, gone, f'artwork row {i} is missing'
        rest = blank[i:]
        return bool(rest.all()), gone + len(rest), 'artwork dropped off the end' if not rest.all() else ''

    margin = round(limit * webtoon.STRIP_TOP_MARGIN)

    f105, deep, tall = round(1.05 * limit), round(60 * scale), round(80 * scale)
    faint = _strip_cbz(
        [native, native],
        width=p.width,
        gutter=[(0, f105, deep), (0, f105 + deep, tall, (252, 252, 252), 16)],
    )
    band = strip_of(faint)[f105 + deep + 4 : f105 + deep + tall - 4]
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
    past = _strip_cbz([native, native], width=p.width, gutter=(0, f105, deep), noise=15)
    opened = cut(past)[1]
    check(
        'a page opening on a gutter noisy up to 15 trims it like a clean one',
        len(opened) > margin and np.array_equal(opened[margin], strip_of(past)[f105 + deep]),
        f'{len(opened)} rows',
    )

    fold = round(1.05 * limit)
    floating = _archive_of(
        _panel_strip(
            ('art', fold),
            ('white', 100),
            ('grey', 2),
            ('black', 4),
            ('grey', 2),
            ('white', 400),
            ('art', limit),
            width=p.width,
        )
    )
    floating_short = _archive_of(
        _panel_strip(
            ('art', fold),
            ('white', 100),
            ('grey', 2),
            ('black', 4),
            ('grey', 2),
            ('white', 24),
            ('art', limit),
            width=p.width,
        )
    )
    faint_line = (240 - np.random.default_rng(9).integers(0, 21, (3, p.width, 3))).astype(np.uint8)
    faint_lined = _archive_of(
        np.concatenate(
            [
                _panel_strip(('art', fold), ('white', 100), width=p.width),
                faint_line,
                _panel_strip(('white', 400), ('art', limit), width=p.width, seed=9),
            ]
        )
    )
    faint_mark = np.full((10, p.width, 3), 255, np.uint8)
    faint_mark[:, 400:900:6] = 183
    faint_marked = _archive_of(
        np.concatenate(
            [
                _panel_strip(('art', fold), ('white', 100), width=p.width),
                faint_mark,
                _panel_strip(('white', 400), ('art', limit), width=p.width, seed=10),
            ]
        )
    )
    banded = _archive_of(
        _panel_strip(('art', fold), ('white', 100), ('grey', 60), ('white', 400), ('art', limit), width=p.width)
    )
    dusk = _archive_of(
        _panel_strip(('art', fold), ('white', 100), ('grey', 2), ('black', 150), ('art', limit), width=p.width)
    )
    speck = _panel_strip(('white', 8), width=p.width)
    speck[:, 300:1000:40] = 0
    ellipsis = _archive_of(
        np.concatenate(
            [
                _panel_strip(('art', fold), ('white', 100), width=p.width),
                speck,
                _panel_strip(('white', 400), ('art', limit), width=p.width, seed=8),
            ]
        )
    )
    bordered = _archive_of(
        _panel_strip(('art', fold), ('black', 4), ('grey', 2), ('white', 1000), ('art', limit), width=p.width)
    )

    everything = [
        _strip_cbz(heights),
        past_fold,
        _strip_cbz([1280, 1000, 1280, 640], gutter=[(0, at(1.05)), (2, 300)]),
        _strip_cbz([1280, 1280], gutter=(0, at(1.05), 150)),
        _strip_cbz([1500, 1280], gutter=[(0, 1120, 380), (1, 0, 100)]),
        faint,
        floating,
        floating_short,
        faint_lined,
        faint_marked,
        banded,
        dusk,
        ellipsis,
        bordered,
    ]
    verdicts, tallest = [], 0
    for b in everything:
        pieces = cut(b)
        verdicts.append(only_gutter_gone(b, pieces))
        tallest = max(tallest, *(len(t) for t in pieces))
    check(
        'no row of artwork is ever lost; only gutter, and the thin lines in it, is dropped',
        all(ok for ok, _, _ in verdicts),
        '; '.join(f'fixture {k}: {why}' for k, (ok, _, why) in enumerate(verdicts) if not ok),
    )
    check(
        'and some gutter really was dropped, so that is not vacuous',
        sum(g for _, g, _ in verdicts) > 0,
        f'{[g for _, g, _ in verdicts]} rows',
    )
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
        blank_at_top(grey[1]) == 0 and len(grey[1]) and _spread(grey[1][:100]).max() <= webtoon.STRIP_QUIET,
        f'{blank_at_top(grey[1])} blank rows at the top',
    )
    middle = _strip_cbz([1280, 1280], gutter=(0, at(0.30), 60))
    mid, held = cut(middle), strip_of(middle)
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

    def opens_on(tile):
        return 'artwork' if len(tile) > margin and not flat_rows(tile[margin : margin + 1]).any() else 'a flat row'

    fl = cut(floating)
    check(
        'a line floating in a gutter goes with the gutter, so the page after opens on artwork',
        len(fl) > 1 and blank_at_top(fl[1]) == margin and opens_on(fl[1]) == 'artwork',
        f'{blank_at_top(fl[1])} blank rows at the top, then {opens_on(fl[1])}',
    )
    fs = cut(floating_short)
    ink = int(np.argmin(flat_rows(fs[1]))) if len(fs) > 1 and not flat_rows(fs[1]).all() else 0
    white = len(fs) > 1 and bool((fs[1][:ink].min(axis=(1, 2)) >= 235).all())
    check(
        'even when the gutter under the line is shorter than the margin',
        0 < ink <= margin and white,
        f'{ink} flat rows before the artwork, all white: {white}',
    )
    spreads = [int(x.astype(np.int16).max() - x.min()) for x in (faint_line, faint_mark)]
    check(
        'premise: the faint line is not flat, and the faint mark as faint as a real one measured',
        spreads[0] > webtoon.STRIP_QUIET and spreads[1] == 72,
        f'spreads {spreads}',
    )
    fz = cut(faint_lined)
    check(
        'a faint, noisy line floating in a gutter goes with it too',
        len(fz) > 1 and blank_at_top(fz[1]) == margin and not any(np.array_equal(r, faint_line[0]) for r in fz[1]),
        f'{blank_at_top(fz[1])} blank rows at the top of the page after',
    )
    fm = cut(faint_marked)
    check(
        'but a faint mark stays',
        len(fm) > 1 and any(np.array_equal(r, faint_mark[0]) for r in fm[1][: margin + len(faint_mark)]),
        f'{blank_at_top(fm[1])} blank rows at the top of the page after',
    )
    grey_row = _panel_strip(('grey', 1), width=p.width)[0]
    bn = cut(banded)
    kept = sum(np.array_equal(r, grey_row) for r in bn[1]) if len(bn) > 1 else 0
    check(
        'a flat grey band as tall as a gutter, floating in one, is artwork and stays',
        kept == 60,
        f'{kept} of its 60 rows on the page after',
    )
    dk = cut(dusk)
    top_dark = bool(len(dk) > 1 and (dk[1][:margin].max(axis=(1, 2)) <= 30).all())
    check(
        'white gutter turning black drops through the black, the last gutter before the artwork',
        len(dk) > 1 and top_dark and opens_on(dk[1]) == 'artwork',
        f'margin all black: {top_dark}, then {opens_on(dk[1]) if len(dk) > 1 else "nothing"}',
    )
    el = cut(ellipsis)
    check(
        'but a speck of ink in a gutter - a lone ellipsis - is artwork, and stays',
        len(el) > 1 and any(np.array_equal(r, speck[0]) for r in el[1][: margin + len(speck)]),
        f'{blank_at_top(el[1])} blank rows at the top of the page after',
    )
    bd = cut(bordered)
    check(
        'a panel cut past the fold keeps its bottom border',
        len(bd) > 1 and bool((bd[0][-6:-2].max(axis=(1, 2)) <= 30).all()),
        f'last rows of the page: {bd[0][-6:, 0, 0].tolist()}',
    )
    check(
        'and the page after opens on artwork, not on a screen of gutter',
        len(bd) > 1 and blank_at_top(bd[1]) == margin and opens_on(bd[1]) == 'artwork',
        f'{blank_at_top(bd[1])} blank rows at the top, then {opens_on(bd[1])}',
    )

    source = np.random.default_rng(11).integers(0, 256, (1280, 800, 3), dtype=np.uint8)
    source[640:680] = 0
    source[640:680, [0, 799]] = 18
    edged = _archive_of(source)
    for name, q in sorted(profiles.PROFILES.items()):
        if not q.reslice:
            continue
        s = q.width / 800
        z = zipfile.ZipFile(io.BytesIO(edged))
        rows = webtoon._strip_rows(z.read('0000.png'), q)[round(650 * s) : round(670 * s)]
        spread = int((rows.max(axis=(1, 2)).astype(np.int16) - rows.min(axis=(1, 2))).min())
        check(
            f'premise, {name}: the light edge of a black gutter survives the upscale',
            spread > webtoon.STRIP_QUIET,
            f'the flattest gutter row spreads {spread}',
        )
        order = sorted(z.infolist(), key=lambda i: cbz.natural_key(i.filename))
        first = next(len(t[3]) for t in webtoon.strip_tiles(cbz._entries(z, order), q, {}) if t[0] == 'tile')
        lo, hi = round(640 * s), round(680 * s)
        check(
            f'{name}: a black gutter with a light edge column, as real WebP slices have, is still a gutter',
            lo - 4 <= first <= hi + 4,
            f'page {first}, gutter {lo}..{hi}',
        )

    source = np.random.default_rng(12).integers(0, 256, (1900, 800, 3), dtype=np.uint8)
    source[1120:1270] = 0
    source[1120:1270, [0, 799]] = 18
    night_edged = cut(_archive_of(source))
    dark = night_edged[1][:, 8:-8].max(axis=(1, 2)) <= 30 if len(night_edged) > 1 else np.zeros(0, bool)
    top = len(dark) if dark.all() else int(np.argmin(dark))
    check(
        'and a page that would open on one keeps only the margin of it',
        margin <= top <= margin + 3,
        f'{top} dark rows at the top, margin {margin}',
    )

    tall = zipfile.ZipFile(io.BytesIO(b''.join(cbz.repack_iter(io.BytesIO(past_fold), p, 1))))
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
        content >= p.width / webtoon.STRIP_OVERSHOOT - 2,
        f'content {content} px of {p.width}, floor {p.width / webtoon.STRIP_OVERSHOOT:.0f}',
    )

    wrapped = zipfile.ZipFile(
        io.BytesIO(b''.join(cbz.repack_iter(io.BytesIO(src), p, 1, cover=fake_page(9, 800, 1200))))
    )
    covered = sorted(n for n in wrapped.namelist() if n[0].isdigit())
    front = pyvips.Image.new_from_buffer(wrapped.read(covered[0]), '')
    check(
        'a prepended 2:3 cover fills exactly one screen, so page 1 opens at zoom 1.0',
        (front.width, front.height) == (p.width, limit),
        f'{front.width}x{front.height} for a {p.width}x{limit} screen',
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
        'premise: the family test sees past a generation suffix',
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


PASS, FAIL = [], []


def _folio_page(number: str = '123', attached: bool = False, w: int = 900, rule: str = '', gap: int = 4) -> bytes:
    h = w * 13 // 9
    a = np.full((h, w), 255, np.uint8)
    k = w / 900
    sc = lambda *v: tuple(int(x * k) for x in v)  # noqa: E731
    a[sc(60)[0] : sc(1130)[0], sc(60)[0] : sc(840)[0]] = 245
    for y0, y1, x0, x1 in ((60, 66, 60, 840), (1124, 1130, 60, 840), (60, 1130, 60, 66), (60, 1130, 834, 840)):
        y0, y1, x0, x1 = sc(y0, y1, x0, x1)
        a[y0:y1, x0:x1] = 20
    a[sc(300)[0] : sc(700)[0], sc(200)[0] : sc(640)[0]] = 90
    t = pyvips.Image.text(number or '123', dpi=300, font='DejaVu Sans')
    g = 255 - np.ndarray(buffer=t.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(t.height, t.width))
    top = int(h * (1.0 - folio.BAND))
    y = top + 2 if attached else top + int(h * folio.BAND * 0.45)
    x = (w - t.width) // 2
    if number:
        a[y : y + t.height, x : x + t.width] = np.minimum(a[y : y + t.height, x : x + t.width], g)
        if attached:
            a[y - 8 : y - 2, x + 6 : x + 26] = 20
    rows, cols = np.nonzero((g < 243).any(axis=1))[0], np.nonzero((g < 243).any(axis=0))[0]
    gy0, gy1, gx0, gx1 = y + int(rows[0]), y + int(rows[-1]), x + int(cols[0]), x + int(cols[-1])
    if rule == 'above':
        a[gy0 - gap - 5 : gy0 - gap, sc(60)[0] : sc(840)[0]] = 20
    elif rule == 'below':
        a[gy1 + gap + 1 : gy1 + gap + 6, sc(60)[0] : sc(840)[0]] = 20
    elif rule == 'left':
        a[top - 30 : h, gx0 - gap - 5 : gx0 - gap] = 20
    elif rule == 'right':
        a[top - 30 : h, gx1 + gap + 1 : gx1 + gap + 6] = 20
    return pyvips.Image.new_from_memory(a.tobytes(), w, h, 1, 'uchar').pngsave_buffer()


def _paged() -> dict:
    return {n: p for n, p in profiles.PROFILES.items() if not p.reslice}


def check_strip_folio() -> None:
    off = dataclasses.replace(profiles.PROFILES['kobo-clara-hd-2e-bw'], strip_folio=False)
    on = dataclasses.replace(off, strip_folio=True)
    numbered, blank = _folio_page(), _folio_page(number='')
    attached = _folio_page(attached=True)

    paged = _paged()
    check(
        'every paged device profile strips folios',
        all(p.strip_folio for p in paged.values()),
        f'off: {[n for n, p in paged.items() if not p.strip_folio]}',
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
    table = profiles.PROFILES
    try:
        profiles.use_folio(False)
        check(
            'with OCR off, the pass is off for every profile, in the table everyone holds',
            not any(p.strip_folio for p in _paged().values()) and profiles.PROFILES is table,
            f'still on: {[n for n, p in profiles.PROFILES.items() if p.strip_folio]}',
        )
        check(
            'and that reaches the cache key, so no page rendered with it on is served with it off',
            cache_mod.render_key('u', profiles.PROFILES['kobo-clara-hd-2e-bw'], None) != before_key,
        )
    finally:
        profiles.use_folio(True)
    check(
        'and the table comes back',
        all(p.strip_folio for p in _paged().values()),
    )
    saved, read = dict(vars(settings_mod)), {}
    for value in ('false', ' OFF ', '0', 'no', 'true', 'yes', 'typo', ''):
        with mock.patch.dict(os.environ, {'OCR_ENABLED': value}):
            read[value] = importlib.reload(settings_mod).settings.ocr_enabled
    vars(settings_mod).update(saved)
    check(
        'OCR_ENABLED is read in settings, and only an explicit false, 0, no or off turns it off',
        [v for v, on in read.items() if not on] == ['false', ' OFF ', '0', 'no'],
        f'{read}',
    )
    code = (
        'import inksetter.app\n'
        'from inksetter.imaging import profiles\n'
        'print(sum(p.strip_folio for p in profiles.PROFILES.values()))'
    )
    started = {}
    for value in ('off', 'on'):
        r = subprocess.run(
            [sys.executable, '-c', code], env={**os.environ, 'OCR_ENABLED': value}, capture_output=True, text=True
        )
        started[value] = r.stdout.strip() or r.stderr.strip()[-200:]
    check(
        'and the app turns the pass off on every profile as it starts',
        started['off'] == '0' and started['on'] not in ('0', ''),
        f'{started}',
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
    crowded_ink = (255 - ca.astype(np.int16)) > off.autocrop_threshold
    seen_marks = folio._marks(crowded_ink[folio._first_row(crowded.height) :], crowded.height, crowded.width)
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

    def _capturing_marks(ink, h_, w_, *lines):
        _seen.append(ink)
        return _real_marks(ink, h_, w_, *lines)

    folio._marks = _capturing_marks
    try:
        for _thr in (0, 1, 11, 12, 13, 254):
            _seen.clear()
            folio.strip(pyvips.Image.new_from_buffer(numbered, ''), pyvips.Image.new_from_buffer(numbered, ''), _thr)
            _page = pyvips.Image.new_from_buffer(numbered, '')
            _pa = np.ndarray(
                buffer=_page.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(_page.height, _page.width)
            )
            _want = ((255 - _pa.astype(np.int16)) > _thr)[folio._first_row(_page.height) :]
            if not (_seen and np.array_equal(_seen[0], _want)):
                break
        else:
            _thr = None
        check('the gate thresholds exactly as (255 - a) > threshold did', _thr is None, f'differs at threshold={_thr}')
    finally:
        folio._marks = _real_marks
    searched: list = []
    with (
        mock.patch.object(folio, '_engine_failed', True),
        mock.patch.object(folio, '_marks', lambda *a: searched.append(a) or []),
    ):
        _page = pyvips.Image.new_from_buffer(numbered, '')
        folio.strip(_page, _page, off.autocrop_threshold)
    check('with no reader to ask, folio does not search the page for marks', not searched, f'{len(searched)}')

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
    ink3 = (255 - a3.astype(np.int16)) > off.autocrop_threshold
    marks = folio._marks(ink3[folio._first_row(raw3.height) :], raw3.height, raw3.width)
    check('a three-digit folio is offered to the reader as one mark', len(marks) == 1, f'{len(marks)} marks')

    fw, fh = 900, 1300
    jx, jy = max(1, int(fw * folio.JOIN_X)), max(1, int(fh * folio.JOIN_Y))

    def dots(across: int, down: int) -> int:
        ink = np.zeros((fh, fw), bool)
        y, x = fh - 70, fw // 2
        ink[y : y + 8, x : x + 4] = True
        if across:
            ink[y : y + 8, x + 4 + across : x + 8 + across] = True
        else:
            ink[y + 8 + down : y + 16 + down, x : x + 4] = True
        return len(folio._marks(ink[folio._first_row(fh) :], fh, fw))

    joins = {
        'across': (dots(2 * jx, 0), dots(2 * jx + 1, 0)),
        'down': (dots(0, 2 * jy), dots(0, 2 * jy + 1)),
    }
    check(
        'two dots whose JOIN_X and JOIN_Y growth meets are one mark, a pixel further they are two, both ways',
        all(v == (1, 2) for v in joins.values()),
        f'marks at the limit and one past it: {joins}',
    )
    speck = _folio_page(number='')
    im = pyvips.Image.new_from_buffer(speck, '')
    a = np.ndarray(buffer=im.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(im.height, im.width))
    a[1250:1268, 430:448] = 20
    blob = pyvips.Image.new_from_memory(a.tobytes(), im.width, im.height, 1, 'uchar').pngsave_buffer()
    check(
        'an isolated blob that reads as nothing is not erased',
        pipeline.render_page(blob, on)[0] == pipeline.render_page(blob, off)[0],
    )

    rng = np.random.default_rng(5)
    runs_wrong = []
    for _ in range(300):
        m = rng.random((int(rng.integers(1, 7)), int(rng.integers(1, 30)))) < rng.random()
        length = int(rng.integers(1, 33))
        want = np.zeros_like(m)
        for r, row in enumerate(m):
            start = None
            for c, v in enumerate([*row, False]):
                if v and start is None:
                    start = c
                elif not v and start is not None:
                    want[r, start:c] = c - start >= length
                    start = None
        if not np.array_equal(folio._long_runs(m, length), want):
            runs_wrong.append((m.shape, length))
    check(
        'the ruler takes exactly the pixels lying in a straight run at least as long as asked',
        not runs_wrong,
        f'{len(runs_wrong)} of 300 masks wrong, first {runs_wrong[:1]}',
    )

    def _ink(png: bytes):
        im_ = pyvips.Image.new_from_buffer(png, '')
        a_ = np.ndarray(buffer=im_.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(im_.height, im_.width))
        return a_ < (255 - off.autocrop_threshold), im_.height, im_.width

    ruled = {r: (_folio_page(rule=r), _folio_page(number='', rule=r)) for r in ('above', 'below', 'left', 'right')}
    unruled = {}
    for r, (png, _b) in ruled.items():
        ink_, h_, w_ = _ink(png)
        rows_ = ink_[folio._first_row(h_) :]
        unruled[r] = len(folio._marks(rows_, h_, w_, np.zeros_like(rows_)))
    check(
        'a border that close joins the folio, or crowds its halo, unless the ruler takes it out',
        not any(unruled.values()),
        f'marks found with no line left out: {unruled}',
    )
    tucked = {r: pipeline.render_page(n, on)[0] == pipeline.render_page(b, on)[0] for r, (n, b) in ruled.items()}
    check(
        'a folio tucked against a panel border on any side is erased and the border kept whole',
        all(tucked.values())
        and all(pipeline.render_page(n, off)[0] != pipeline.render_page(n, on)[0] for n, _b in ruled.values()),
        f'renders as the unnumbered page: {tucked}',
    )

    left_alone, taken = {}, {}
    for r in ('above', 'below', 'left', 'right'):
        at_line, one_off = _folio_page(rule=r, gap=0), _folio_page(rule=r, gap=1)
        left_alone[r] = pipeline.render_page(at_line, on)[0] == pipeline.render_page(at_line, off)[0]
        taken[r] = pipeline.render_page(one_off, on)[0] != pipeline.render_page(one_off, off)[0]
    check(
        'a mark touching a border is art cut loose from it and left alone; one clear row away it is a folio',
        all(left_alone.values()) and all(taken.values()),
        f'touching left alone: {left_alone}, a row away erased: {taken}',
    )

    bh_, bw_ = 1300, 900
    border_y = bh_ - 40

    def _bordered(attached: int, folio_there: bool = True, slivers: bool = False) -> int:
        ink_ = np.zeros((bh_, bw_), bool)
        ink_[border_y - 4 : border_y, 100:800] = True
        ink_[bh_ - 300 : border_y, 100:104] = True
        for x in range(300, 340, 6):
            ink_[border_y : border_y + attached, x : x + 3] = True
        if folio_there:
            ink_[border_y + 4 : border_y + 26, 310:330] = True
        if slivers:
            for y in range(border_y - 25, border_y - 5, 8):
                ink_[y : y + 5, 104] = True
            for x in range(104, 150, 8):
                ink_[border_y, x : x + 5] = True
            ink_[border_y + 3 : border_y + 5, 120:122] = True
        return len(folio._marks(ink_[folio._first_row(bh_) :], bh_, bw_))

    check(
        'specks hanging a pixel off a border do not cost the folio they join, two pixels are art',
        _bordered(1) == 1 and _bordered(2) == 0,
        f'marks with one-pixel specks {_bordered(1)}, with two-pixel {_bordered(2)}',
    )
    check(
        'slivers hugging a panel corner, with nothing speck-sized clear of the border, are not offered to the reader',
        _bordered(0, folio_there=False, slivers=True) == 0,
        f'{_bordered(0, folio_there=False, slivers=True)} marks',
    )
    specked = pyvips.Image.new_from_buffer(_folio_page(rule='above', gap=4), '')
    sa = np.ndarray(
        buffer=specked.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(specked.height, specked.width)
    )
    sa = sa.copy()
    line_rows = np.nonzero((sa[:, 60:840] < 128).all(axis=1))[0]
    under = int(line_rows[line_rows > sa.shape[0] * (1 - folio.BAND) - 20][-1]) + 1
    for x in range(420, 480, 9):
        sa[under, x : x + 3] = 20
    specked_png = pyvips.Image.new_from_memory(sa.tobytes(), specked.width, specked.height, 1, 'uchar').pngsave_buffer()
    check(
        'and that folio is erased, specks with it, and the border kept whole',
        pipeline.render_page(specked_png, on)[0]
        == pipeline.render_page(_folio_page(number='', rule='above', gap=4), on)[0]
        and pipeline.render_page(specked_png, on)[0] != pipeline.render_page(specked_png, off)[0],
    )

    def _eight(white: bool, number: str = '8') -> np.ndarray:
        a_ = np.full((bh_, bw_), 255, np.uint8)
        a_[60:1100, 60:840] = 245
        a_[300:700, 200:640] = 90
        if white:
            a_[bh_ - 120 :, :] = 0
        if number:
            t = pyvips.Image.text(number, dpi=480, font='DejaVu Sans Bold')
            g_ = np.ndarray(buffer=t.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(t.height, t.width))
            y_, x_ = bh_ - 10 - t.height, (bw_ - t.width) // 2
            region = a_[y_ : y_ + t.height, x_ : x_ + t.width]
            region[:] = np.maximum(region, g_) if white else np.minimum(region, 255 - g_)
        return a_

    def _block_eight(number: bool = True) -> np.ndarray:
        a_ = _eight(False, '')
        if number:
            top_ = bh_ - 80
            a_[top_ : top_ + 62, 430:470] = 0
            a_[top_ + 10 : top_ + 26, 440:460] = 255
            a_[top_ + 34 : top_ + 50, 440:460] = 255
        return a_

    counters = {}
    with mock.patch.object(folio, '_read', lambda _luma, m, _h, _w: m):
        for name, a_, blank_ in (
            ('white on black', _eight(True), _eight(True, '')),
            ('black on white', _eight(False), _eight(False, '')),
            ('hard-edged, strokes wider than the halo', _block_eight(), _block_eight(False)),
        ):
            im_ = pyvips.Image.new_from_memory(a_.tobytes(), bw_, bh_, 1, 'uchar')
            out_ = folio.strip(im_, im_, off.autocrop_threshold)
            counters[name] = np.array_equal(
                np.ndarray(buffer=out_.write_to_memory(), dtype=np.uint8, shape=(bh_, bw_)), blank_
            )
    check(
        'the counters of an 8 are not a folio: offered whatever it is shown, the pass erases the digit and no more',
        all(counters.values()),
        f'comes out as the page without it: {counters}',
    )

    def _white_folio(number: str = '123', ground=(0, 0, 0), stripes=None) -> bytes:
        wh, ww = 1300, 900
        a_ = np.full((wh, ww, 3), 255, np.uint8)
        a_[60:1100, 60:840] = 245
        a_[300:700, 200:640] = 90
        a_[wh - 110 :, :] = ground
        if stripes is not None:
            a_[wh - 110 :: 2, :] = stripes
        t = pyvips.Image.text('123', dpi=300, font='DejaVu Sans')
        g_ = np.ndarray(buffer=t.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(t.height, t.width))
        y_, x_ = wh - 70, (ww - t.width) // 2
        if number:
            region = a_[y_ : y_ + t.height, x_ : x_ + t.width]
            region[:] = np.maximum(region, g_[:, :, None])
        return pyvips.Image.new_from_memory(a_.tobytes(), ww, wh, 3, 'uchar').pngsave_buffer()

    on_colour = dataclasses.replace(profiles.PROFILES['kindle-colorsoft'], strip_folio=True)
    off_colour = dataclasses.replace(on_colour, strip_folio=False)
    white_cases = {
        'black': (_white_folio(), _white_folio(number=''), on, off),
        'dark blue, in colour': (
            _white_folio(ground=(20, 24, 70)),
            _white_folio(number='', ground=(20, 24, 70)),
            on_colour,
            off_colour,
        ),
    }
    whites = {
        k: pipeline.render_page(n, p_on)[0] == pipeline.render_page(b, p_on)[0]
        and pipeline.render_page(n, p_on)[0] != pipeline.render_page(n, p_off)[0]
        for k, (n, b, p_on, p_off) in white_cases.items()
    }
    check(
        'a white folio on dark ground is erased with the colour around it',
        all(whites.values()),
        f'renders as the page without it: {whites}',
    )
    on_grey = _white_folio(ground=(110, 110, 110))
    on_stripes = _white_folio(stripes=(110, 110, 110))
    check(
        'but a white mark on grey is art and left alone, and so is one on ground only half black',
        pipeline.render_page(on_grey, on)[0] == pipeline.render_page(on_grey, off)[0]
        and pipeline.render_page(on_stripes, on)[0] == pipeline.render_page(on_stripes, off)[0],
    )

    def _edged(number: str) -> bytes:
        im_ = pyvips.Image.new_from_buffer(_folio_page(number=number), '')
        a_ = np.ndarray(
            buffer=im_.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(im_.height, im_.width)
        ).copy()
        h_, w_ = a_.shape
        for y0_ in (h_ - 34, h_ - 12):
            a_[y0_ : y0_ + 10, :14] = 20
            a_[y0_ : y0_ + 10, w_ - 14 :] = 20
        for x0_ in (200, 650):
            a_[h_ - 12 :, x0_ : x0_ + 14] = 20
        return pyvips.Image.new_from_memory(a_.tobytes(), w_, h_, 1, 'uchar').pngsave_buffer()

    edged, edged_blank = _edged('123'), _edged('')
    ink_, h_, w_ = _ink(edged)
    found = folio._marks(ink_[folio._first_row(h_) :], h_, w_)
    folio_bottom = max(m[3] for m in found if 0 < m[0] and m[2] < w_ - 1 and m[3] < h_ - 1)
    per_edge = {
        'left': sum(m[0] == 0 and m[3] > folio_bottom for m in found),
        'right': sum(m[2] == w_ - 1 and m[3] > folio_bottom for m in found),
        'bottom': sum(m[3] == h_ - 1 for m in found),
    }
    check(
        'each edge of the page holds more marks below the folio than the reader is asked about',
        all(n >= folio.MAX_READS for n in per_edge.values()),
        f'{per_edge}, MAX_READS={folio.MAX_READS}',
    )
    check(
        'marks on the page edge are read after the rest, so art running off the page does not crowd the folio out',
        pipeline.render_page(edged, on)[0] == pipeline.render_page(edged_blank, on)[0]
        and pipeline.render_page(edged, on)[0] != pipeline.render_page(edged, off)[0],
    )

    def _fake(line=('123', 1.0), found=('123', 1.0), calls=None):
        def engine(img, use_det=True, **_kw):
            if calls is not None:
                calls.append('line' if use_det is False else 'detector')
            if use_det is False:
                return ([list(line)] if line else None), 0.0
            ih, iw = img.shape[:2]
            return ([[[[0, 0], [iw, 0], [iw, ih], [0, ih]], found[0], found[1]]] if found else None), 0.0

        return engine

    held_engine = folio._reader()

    def _erases(engine, png: bytes = numbered) -> bool:
        folio._engine = engine
        try:
            raw = pyvips.Image.new_from_buffer(png, '')
            return folio.strip(raw, raw, off.autocrop_threshold) is not raw
        finally:
            folio._engine = held_engine

    hair = float(np.nextafter(folio.MIN_SCORE, 0))
    taken = {
        'at': _erases(_fake(line=('123', 0.5), found=('123', folio.MIN_SCORE))),
        'under': _erases(_fake(line=('123', 0.5), found=('123', hair))),
    }
    check(
        'a read the detector is MIN_SCORE sure of is taken, and one a hair less sure is not',
        taken == {'at': True, 'under': False},
        f'{taken}',
    )
    line_hair = float(np.nextafter(folio.LINE_SCORE, 0))
    routes = {}
    for name, line in (
        ('two digits at LINE_SCORE', ('12', folio.LINE_SCORE)),
        ('a hair under', ('12', line_hair)),
        ('one digit', ('7', 1.0)),
        ('digits in other text', ('12a', 1.0)),
        ('no digit', ('ab', 1.0)),
        ('nothing', None),
    ):
        calls = []
        erased = _erases(_fake(line=line, found=None, calls=calls))
        routes[name] = (erased, 'detector' in calls)
    check(
        'a mark read off as one line of two digits or more, LINE_SCORE sure, is erased without the detector',
        routes['two digits at LINE_SCORE'] == (True, False),
        f'{routes}',
    )
    check(
        'any other line read with a digit in it goes to the detector, and one with none is not a folio',
        routes['a hair under'] == (False, True)
        and routes['one digit'] == (False, True)
        and routes['digits in other text'] == (False, True)
        and routes['no digit'] == (False, False)
        and routes['nothing'] == (False, False),
        f'{routes}',
    )

    def _vertical(ratio_over: bool) -> bytes:
        im_ = pyvips.Image.new_from_buffer(_folio_page(number=''), '')
        a_ = np.ndarray(
            buffer=im_.cast('uchar').write_to_memory(), dtype=np.uint8, shape=(im_.height, im_.width)
        ).copy()
        h_, w_ = a_.shape
        mw_ = 8
        mh_ = int(mw_ * folio.TALL) + (1 if ratio_over else 0)
        y_ = h_ - 20 - mh_
        a_[y_ : y_ + mh_, w_ // 2 : w_ // 2 + mw_] = 20
        return pyvips.Image.new_from_memory(a_.tobytes(), w_, h_, 1, 'uchar').pngsave_buffer()

    tall = {}
    for over in (False, True):
        calls = []
        _erases(_fake(line=('12', 1.0), found=None, calls=calls), _vertical(over))
        tall['past TALL' if over else 'at TALL'] = calls[:1]
    check(
        'a mark more than TALL times taller than wide may be set vertically, so only the detector reads it',
        tall == {'at TALL': ['line'], 'past TALL': ['detector']},
        f'first call: {tall}',
    )


def _decoded(token: str) -> tuple[str, str | None]:
    try:
        return rewrite.decode_parts(token)
    except rewrite.TokenError:
        return '', None


async def check_malformed_input(c) -> None:
    up = 'http://127.0.0.1:8899'
    ctx = rewrite.Ctx(profile='kobo-clara-hd-2e-bw', public_base='http://proxy.test', base_url=f'{up}/opds/v1.2/x')
    try:
        out = rewrite.rewrite(BROKEN_HREF_FEED.encode(), ctx, None).decode()
        got = 'rewritten'
    except Exception as exc:
        out, got = '', type(exc).__name__
    check('a feed with one malformed href is still rewritten', got == 'rewritten', got)
    check('the malformed href is dropped, not passed through', '[broken' not in out and 'href=""' in out)
    check('and the rest of the feed is rewritten as usual', '/kobo-clara-hd-2e-bw/dl/' in out and up not in out)

    doc = {'links': [{'rel': 'self', 'href': '/opds/v2/catalog', 'type': 1}]}
    try:
        out = json.loads(rewrite.rewrite(json.dumps(doc).encode(), ctx, None))
        got = out['links'][0]['href']
    except Exception as exc:
        got = type(exc).__name__
    check(
        'an OPDS 2 link whose type is not a string is rewritten as if it had none', got.startswith('http://proxy'), got
    )

    check('host_key answers a malformed URL rather than raising', _host_key_of('http://[x/y') == '')
    url, jacket = rewrite.decode_parts(encode_token(f'{up}/opds/v1.2/books/7/file', BROKEN_URL))
    check(
        'a token whose cover is malformed keeps its URL and drops the cover', url.endswith('/file') and jacket is None
    )

    for label, bad in (
        ('a malformed host', BROKEN_URL),
        ('a NUL byte', f'{up}/opds/v1.2/a\x00b'),
        ('a scheme other than http', 'ftp://127.0.0.1/x'),
    ):
        for route in ('f', 'p', 'img', 'dl'):
            try:
                r = await c.get(f'/kobo-clara-hd-2e-bw/{route}/{encode_token(bad)}')
                status = r.status_code
            except Exception as exc:
                status = f'{type(exc).__name__} escaped'
            check(f'a token for {label} is refused as 400 on /{route}/', status == 400, f'status={status}')

    for label, path, want in (
        ('a feed with a malformed href', f'/kobo-clara-hd-2e-bw/f/{encode_token(up + "/opds/v1.2/badhref")}', 200),
        ('a malformed search template', f'/kobo-clara-hd-2e-bw/bs/{encode_token(up + "/opds/v1.2/badosd")}?q=x', 502),
    ):
        try:
            r = await c.get(path)
            status, body = r.status_code, r.text
        except Exception as exc:
            status, body = f'{type(exc).__name__} escaped', ''
        check(f'{label} is answered {want}, not 500', status == want, f'status={status}')
        check(f'and {label} leaks nothing upstream', '[broken' not in body and up not in body)


async def check_epub(c) -> None:
    strict, loose = EPUBS['strict'][0], EPUBS['loose'][0]
    for kind, body in EPUBS.items():
        moved = zipfile.ZipFile(
            io.BytesIO(b''.join(cbz.repack_iter(io.BytesIO(body[0]), profiles.PROFILES['kobo-clara-hd-2e-bw'], 1)))
        ).namelist()
        check(f'premise: repacking the {kind} EPUB would move its image', 'OEBPS/images/p1.jpg' not in moved)
    check('premise: only the strict EPUB opens with the mimetype entry', strict[30:38] == b'mimetype' != loose[30:38])

    for how in ('ranged', 'whole'):
        for kind, found in (('strict', 'by its content alone'), ('loose', 'by its type alone')):
            body, media = EPUBS[kind]
            tok = encode_token(f'http://127.0.0.1:8899/opds/v1.2/epub/{how}/{kind}')
            r = await c.get(f'/kobo-clara-hd-2e-bw/dl/{tok}')
            check(
                f'an EPUB found {found} is delivered untouched ({how})',
                r.status_code == 200 and r.content == body,
                f'status={r.status_code} {len(r.content)} bytes of {len(body)}',
            )
            check(
                f'and keeps its own type ({how}, {kind})',
                r.headers.get('content-type', '').startswith(media),
                r.headers.get('content-type', ''),
            )


async def check_fail_closed(c) -> None:
    import logging as _logging
    from inksetter import kavita

    up = 'http://127.0.0.1:8899'
    wide_key = SEAL_KEY.encode('utf-16-le')
    real = kavita.settings
    kavita.settings = dataclasses.replace(settings_mod.settings, upstream_catalog=f'{up}/api/opds/{SEAL_KEY}')
    try:

        def odd(shape: str) -> str:
            return encode_token(f'{up}/api/opds/{SEAL_KEY}/odd/{shape}')

        r = await _get(c, f'/kobo-clara-hd-2e-bw/f/{odd("html")}')
        check(
            'a feed labelled text/html is still rewritten, key and all',
            r.status_code == 200 and SEAL_KEY not in r.text and '/kobo-clara-hd-2e-bw/' in r.text,
            f'{r.status_code} {r.text[:60]}',
        )
        r = await _get(c, f'/kobo-clara-hd-2e-bw/f/{odd("utf16")}')
        body = getattr(r, 'content', b'')
        text = body.decode('utf-8', 'replace')
        check(
            'a UTF-16 feed is rewritten, key and all',
            r.status_code == 200 and SEAL_KEY not in text and wide_key not in body and '/kobo-clara-hd-2e-bw/' in text,
            f'{r.status_code} {text[:60]}',
        )
        r = await _get(c, f'/kobo-clara-hd-2e-bw/f/{odd("broken")}')
        check(
            'a keyed feed that will not parse is refused, not passed through',
            r.status_code == 502 and SEAL_KEY not in r.text,
            f'{r.status_code} {r.text[:60]}',
        )
        r = await _get(c, f'/kobo-clara-hd-2e-bw/dl/{odd("utf16-json")}')
        check(
            'a download of a UTF-16 JSON feed is refused too',
            r.status_code == 502 and wide_key not in getattr(r, 'content', b''),
            f'{r.status_code}',
        )
        r = await _get(c, f'/kobo-clara-hd-2e-bw/f/{odd("very-deep")}')
        check('JSON too deep even to parse is a 502, not a crash', r.status_code == 502, f'{r.status_code}')

        ctx = rewrite.Ctx('kobo-clara-hd-2e-bw', 'http://proxy.test', f'{up}/')
        plain = '<<no key here'.encode('utf-16')
        check('premise: a keyless body that will not parse passes through', rewrite.rewrite(plain, ctx, None) == plain)
        try:
            rewrite.rewrite(f'<<{SEAL_KEY}'.encode('utf-16'), ctx, None)
            got = 'passed through'
        except rewrite.FeedError:
            got = 'FeedError'
        check('a UTF-16 body holding the key that will not parse is refused', got == 'FeedError', got)
    finally:
        kavita.settings = real

    for tpl, want in (
        ('http://u/s{?searchTerms?}', 'http://u/s?searchTerms=a%20b'),
        ('http://u/s{?query*}', 'http://u/s?query=a%20b'),
        ('http://u/s/{searchTerms:40}', 'http://u/s/a%20b'),
    ):
        got = rewrite.fill_search(tpl, 'a b')
        check(f'a search variable written {tpl[tpl.index("{") :]} is filled', got == want, got)
    for tpl, want in (
        ('http://u/s/{searchTerms:3}', 'http://u/s/abc'),
        ('http://u/s{?query:2}', 'http://u/s?query=ab'),
    ):
        got = rewrite.fill_search(tpl, 'abcdef')
        check(f'a prefix modifier {tpl[tpl.index("{") :]} keeps only that many characters', got == want, got)
    for tpl, want in (
        ('http://u/s/{searchTerms:0}', 'http://u/s/abcdef'),
        ('http://u/s/{searchTerms:10000}', 'http://u/s/abcdef'),
        ('http://u/s/{searchTerms,query}', 'http://u/s/abcdef,abcdef'),
    ):
        got = rewrite.fill_search(tpl, 'abcdef')
        check(f'{tpl[tpl.index("{") :]} expands as RFC 6570 says', got == want, got)

    with tempfile.TemporaryDirectory() as d:
        dc = cache_mod.DiskCache(pathlib.Path(d), 150)
        keys = [f'{i:02d}' + 'a' * 62 for i in range(3)]
        for i, k in enumerate(keys):
            dc.put(k, b'x' * 100, 'image/png')
            os.utime(dc._paths(k)[0], (1000 + i, 1000 + i))
        stuck = dc._paths(keys[0])[0]
        real_unlink = pathlib.Path.unlink

        def unlink(self, *a, **kw):
            if self == stuck:
                raise PermissionError('in use')
            return real_unlink(self, *a, **kw)

        try:
            with mock.patch.object(pathlib.Path, 'unlink', unlink):
                dc._trim()
            escaped = ''
        except OSError as exc:
            escaped = type(exc).__name__
        check('a cache trim carries on past a file it cannot delete', not escaped, escaped)
        check('and leaves that entry whole, type and all', dc.get(keys[0]) is not None)
        check('premise: the trim still freed what it could', not dc._paths(keys[1])[0].exists())

    with tempfile.TemporaryDirectory() as d:
        dc = cache_mod.DiskCache(pathlib.Path(d), 150)
        for i in range(3):
            dc.put(f'{i:02d}' + 'b' * 62, b'x' * 100, 'image/png')
        trims = []
        real_trim = dc._trim

        def counted():
            trims.append(1)
            return real_trim()

        mine, real_unlink = pathlib.Path(d), pathlib.Path.unlink

        def refuse(self, *a, **kw):
            if mine in self.parents:
                raise PermissionError('in use')
            return real_unlink(self, *a, **kw)

        with mock.patch.object(dc, '_trim', counted), mock.patch.object(pathlib.Path, 'unlink', refuse):
            await dc.maybe_trim()
            over = dc._size
            await dc.maybe_trim()
        check('premise: a trim that can delete nothing stays over the cap', over is not None and over > dc.max_bytes)
        check('and it is not retried on the very next miss', len(trims) == 1, f'{len(trims)} trims')

    page = (pyvips.Image.black(200, 200) + 255).cast('uchar')
    records = []

    class _Grab(_logging.Handler):
        def emit(self, record):
            records.append(record)

    def boom(*_a, **_k):
        raise RuntimeError('engine fell over')

    grab = _Grab(level=_logging.DEBUG)
    folio.log.addHandler(grab)
    try:
        with (
            mock.patch.object(folio, '_ruled', boom),
            mock.patch.object(folio, '_engine_failed', False),
            mock.patch.object(folio, '_pass_warned', False),
            mock.patch.object(folio.log, 'propagate', False),
        ):
            try:
                outs, escaped = [folio.strip(page, page, 12) for _ in range(2)], ''
            except Exception as exc:
                outs, escaped = [], type(exc).__name__
    finally:
        folio.log.removeHandler(grab)
    check('a folio pass that raises ships the page as it was', not escaped and all(o is page for o in outs), escaped)
    warned = [r for r in records if r.levelno == _logging.WARNING]
    check('and says so once, not on every page', len(warned) == 1, f'{len(warned)} warnings')

    def broken(*_a, **_k):
        raise IndexError('numpy fell over')

    with (
        mock.patch.object(pipeline, '_render_page', broken),
        mock.patch.object(pipeline, '_RENDER_FAILURES', set()),
        mock.patch.object(pipeline.log, 'warning') as warn,
    ):
        for _ in range(2):
            try:
                pipeline.render_page(b'x', profiles.PROFILES['kobo-clara-hd-2e-bw'])
                got = 'returned'
            except pipeline.UnreadableImage:
                got = 'UnreadableImage'
            except Exception as exc:
                got = type(exc).__name__
    check('a render that fails outside libvips is an unreadable page, not a crash', got == 'UnreadableImage', got)
    traced = [bool(call.kwargs.get('exc_info')) for call in warn.call_args_list]
    check('and the same failure again logs a line, not another traceback', traced == [True, False], f'{traced}')

    page_url = encode_token('http://127.0.0.1:8899/opds/v1.2/books/7/pages/7')
    with mock.patch.object(pipeline, '_render_page', broken), mock.patch.object(pipeline.log, 'warning'):
        r = await _get(c, f'/kobo-clara-hd-2e-bw/pf/{page_url}', params={'maxWidth': 613})
    check(
        'and the route blames Inksetter for it, not the upstream',
        r.status_code == 502 and 'upstream' not in r.text and 'Inksetter' in r.text,
        f'{r.status_code} {r.text[:80]}',
    )

    records.clear()
    grab = _Grab(level=_logging.DEBUG)
    cbz.log.addHandler(grab)
    try:
        with mock.patch.object(cbz.log, 'propagate', False):
            for exc in (pipeline.RenderFailed('ours'), RuntimeError('other')):

                def fails(exc=exc):
                    raise exc

                cbz._emit_rendered(zipfile.ZipFile(io.BytesIO(), 'w'), ('page', 1, 'p1.jpg', b'x'), 4, fails)
    finally:
        cbz.log.removeHandler(grab)
    traced = [bool(r.exc_info) for r in records if r.levelno == _logging.WARNING]
    check(
        'a download logs the traceback of a render failure once, not again beside render_page',
        traced == [False, True],
        f'{traced}',
    )

    code = 'from inksetter.settings import settings; print(settings.cache_dir); print(settings.spool_dir)'

    def dirs(**env) -> list[str]:
        e = {k: v for k, v in os.environ.items() if k not in ('CACHE_DIR', 'SPOOL_DIR')} | env
        r = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, cwd=os.getcwd(), env=e)
        return r.stdout.strip().splitlines()

    unset, empty = dirs(), dirs(CACHE_DIR='')
    check(
        'an empty CACHE_DIR is unset, not the working directory',
        len(unset) == 2 and '.' not in unset and empty == unset,
        f'{empty} against {unset}',
    )

    pf = cache_mod.Prefetcher()
    runs = []

    async def job():
        await asyncio.sleep(0)
        runs.append(1)

    pf.spawn(job(), 'k')
    pf.spawn(job(), 'k')
    await asyncio.gather(*pf._tasks)
    await asyncio.sleep(0)
    check('a prefetch in flight is not started twice', len(runs) == 1, f'{len(runs)} runs')
    pf.spawn(job(), 'k')
    await asyncio.gather(*pf._tasks)
    check('a finished prefetch can be asked for again', len(runs) == 2, f'{len(runs)} runs')


def _raw_token(token: str) -> str:
    return base64.urlsafe_b64decode(token + '=' * (-len(token) % 4)).decode('utf-8', 'replace')


async def check_security(c) -> None:
    from inksetter import kavita

    up = 'http://127.0.0.1:8899'
    real = kavita.settings
    kavita.settings = dataclasses.replace(settings_mod.settings, upstream_catalog=f'{up}/api/opds/{SEAL_KEY}')
    try:
        check('premise: the catalog carries a Kavita key', kavita.api_key() == SEAL_KEY)
        start = encode_token(f'{up}/api/opds/{SEAL_KEY}/feed')
        check('a token does not carry the upstream key', SEAL_KEY not in _raw_token(start), _raw_token(start))
        check(
            'and decodes back to the URL it was made from',
            rewrite.decode_token(start) == f'{up}/api/opds/{SEAL_KEY}/feed',
        )
        r = await c.get(f'/kobo-clara-hd-2e-bw/f/{start}')
        check('a feed behind the key is served through a sealed token', r.status_code == 200, f'status={r.status_code}')
        tokens = re.findall(r'/kobo-clara-hd-2e-bw/\w+/([\w-]+)', r.text)
        check('premise: the rewritten feed is full of tokens', len(tokens) >= 5, f'{len(tokens)}')
        leaked = [t for t in tokens if SEAL_KEY in _raw_token(t)]
        check('no link, icon or token in the feed carries the key', SEAL_KEY not in r.text and not leaked, f'{leaked}')
        shelf = next((t for t in tokens if 'shelf=1' in rewrite.decode_token(t)), '')
        r = await c.get(f'/kobo-clara-hd-2e-bw/f/{shelf}')
        check('and following a link still reaches the keyed upstream', r.status_code == 200, f'status={r.status_code}')
        old = base64.urlsafe_b64encode(f'{up}/api/opds/{SEAL_KEY}/feed'.encode()).decode().rstrip('=')
        r = await c.get(f'/kobo-clara-hd-2e-bw/f/{old}')
        check('a token issued before sealing still works', r.status_code == 200, f'status={r.status_code}')

        b = await c.get(f'/kobo-clara-hd-2e-bw/b/{start}')
        crumbs = re.findall(r'[?&](?:amp;)?t=([\w-]+)', b.text)
        inside = [_raw_token(t) for t in re.findall(r'/kobo-clara-hd-2e-bw/\w+/([\w-]+)', b.text)]
        check('premise: the browse page links through tokens', b.status_code == 200 and len(inside) >= 2)
        check(
            'the browse page carries no key, in its text, its links or its trail',
            SEAL_KEY not in b.text
            and not any(SEAL_KEY in x for x in inside)
            and not any(SEAL_KEY in _raw_token(t) for t in crumbs),
        )

        for label, route in (
            ('a download', '/kobo-clara-hd-2e-bw/dl'),
            ('a raw page', '/passthrough/p'),
            ('a raw image', '/passthrough/img'),
        ):
            r = await _get(c, f'{route}/{start}')
            check(f'{label} of the keyed feed is refused, not passed through', r.status_code == 502, f'{r.status_code}')
            check(f'and {label} refused carries no key', SEAL_KEY not in r.text, r.text[:80])
        check('premise: the long JSON feed has no link in its first 4 KB', LONG_JSON.index('"href"') > 4096)
        r = await _get(c, f'/kobo-clara-hd-2e-bw/dl/{encode_token(f"{up}/opds/v1.2/long-json")}')
        check(
            'a download of a JSON feed whose first link lies past its first 4 KB is refused too',
            r.status_code == 502 and SEAL_KEY not in r.text,
            f'{r.status_code} {r.text[:60]}',
        )
        r = await _get(c, f'/kobo-clara-hd-2e-bw/osd/{start}')
        check(
            'the search-description route rewrites a feed it is pointed at, key and all',
            r.status_code == 200 and SEAL_KEY not in r.text and '/kobo-clara-hd-2e-bw/' in r.text,
            f'status={r.status_code} {r.text[:80]}',
        )
    finally:
        kavita.settings = real

    for label, body, want in (
        ('an Atom feed', b'<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"/>', True),
        ('an Atom entry under a prefix', b'<a:entry xmlns:a="http://www.w3.org/2005/Atom"/>', True),
        ('a search description behind a BOM and a comment', b'\xef\xbb\xbf<!-- x --><OpenSearchDescription/>', True),
        ('JSON with links in it', b'{"links": [{"href": "/x"}]}', True),
        ('a JSON array with links in it', b'[{"href": "/x"}]', True),
        ('an XHTML chapter', b'<?xml version="1.0"?><!DOCTYPE html><html><body>one</body></html>', False),
        ('JSON without a link', b'{"positions": [1, 2]}', False),
        ('a JPEG', b'\xff\xd8\xff\xe0', False),
    ):
        check(f'{label} is {"" if want else "not "}taken for a feed', rewrite.looks_like_feed(body) is want)

    tok = encode_token(f'{up}/opds/v1.2/guarded-page')
    alice = {'authorization': 'Basic Zm9vOmJhcg=='}
    r = await c.get(f'/kobo-clara-hd-2e-bw/img/{tok}', headers=alice)
    check('premise: the right credentials fetch the guarded page', r.status_code == 200, f'status={r.status_code}')
    r = await c.get(f'/kobo-clara-hd-2e-bw/img/{tok}')
    check('a reader without credentials is not served the cached page', r.status_code == 401, f'status={r.status_code}')
    r = await c.get(f'/kobo-clara-hd-2e-bw/img/{tok}', headers={'authorization': 'Basic bWFsbG9yeTp4'})
    check('nor is a reader with other credentials', r.status_code == 401, f'status={r.status_code}')
    before = GUARDED_PAGE['hits']
    r = await c.get(f'/kobo-clara-hd-2e-bw/img/{tok}', headers=alice)
    check(
        'while the reader who fetched it is still served from the cache',
        r.status_code == 200 and GUARDED_PAGE['hits'] == before,
        f'status={r.status_code}, {GUARDED_PAGE["hits"] - before} upstream request(s)',
    )

    for shape in ('atom', 'bom', 'json', 'json-array'):
        for kind in ('plain', 'octet', 'none'):
            r = await c.get(f'/kobo-clara-hd-2e-bw/f/{encode_token(f"{up}/opds/v1.2/untyped/{shape}/{kind}")}')
            check(
                f'a {shape} feed served as {kind} is still rewritten',
                r.status_code == 200 and up not in r.text and '/kobo-clara-hd-2e-bw/' in r.text,
                f'status={r.status_code} {r.text[:60]!r}',
            )
    r = await c.get(f'/kobo-clara-hd-2e-bw/f/{encode_token(f"{up}/opds/v1.2/osd-as-xml")}')
    check(
        'an OpenSearch description served as plain XML is rewritten as one',
        '/kobo-clara-hd-2e-bw/s/' in r.text and '/opds/v1.2/search' not in r.text,
        r.text[-160:],
    )

    b = await c.get(f'/kobo-clara-hd-2e-bw/b/{encode_token(f"{up}/opds/v1.2/hostile")}')
    check('premise: the hostile feed is browsed', b.status_code == 200 and 'Vol 1' in b.text, f'status={b.status_code}')
    check(
        'a browse page links only to the proxy, never to a javascript: or data: URL',
        'javascript:' not in b.text and 'data:text' not in b.text,
    )


async def check_http_edges(c) -> None:
    up = 'http://127.0.0.1:8899'
    want = f'{up}/opds/v1.2/moved/r/0/2'
    for route in ('f', 'b'):
        r = await c.get(f'/kobo-clara-hd-2e-bw/{route}/{encode_token(f"{up}/opds/v1.2/moved")}')
        targets = [
            rewrite.decode_token(t)
            for t in re.findall(r'/kobo-clara-hd-2e-bw/\w+/([\w-]+)', r.text)
            if 'r/0/2' in rewrite.decode_token(t)
        ]
        check(
            f'a relative href is resolved against the URL that answered, not the one asked (/{route}/)',
            set(targets) == {want},
            f'{targets}',
        )

    moved_osd = encode_token(f'{up}/opds/v1.2/moved-search')
    r = await c.get(f'/kobo-clara-hd-2e-bw/osd/{moved_osd}')
    found = re.search(r'/kobo-clara-hd-2e-bw/s/([\w-]+)', r.text)
    check(
        'a relative search template is resolved against the URL that answered (/osd/)',
        found is not None and rewrite.decode_token(found.group(1)) == f'{up}/opds/v1.2/moved/found?q={{searchTerms}}',
        f'{rewrite.decode_token(found.group(1)) if found else r.text[:80]}',
    )
    r = await c.get(f'/kobo-clara-hd-2e-bw/bs/{moved_osd}', params={'q': 'moon'})
    check(
        'and browse search follows it to the moved results',
        r.status_code == 200 and 'found:moon' in r.text,
        f'status={r.status_code}',
    )

    r = await c.get(f'/no-such-profile/osd/{encode_token(f"{up}/opds/v1.2/upstream500")}')
    check(
        'an OpenSearch description under an unknown profile is 404, before any fetch',
        r.status_code == 404,
        f'{r.status_code}',
    )

    r = await _get(c, f'/kobo-clara-hd-2e-bw/f/{encode_token(f"{up}/opds/v1.2/proxyauth")}')
    check('an upstream 407 is the proxy failing, answered 502', r.status_code == 502, f'status={r.status_code}')
    check('and the reader is not handed a challenge it cannot answer', 'proxy-authenticate' not in r.headers)

    from inksetter import app as _app

    release = threading.Event()

    def busy_repack():
        release.wait(10.0)
        yield b'x'

    waiting = [asyncio.create_task(_app._repacking(busy_repack(), io.BytesIO()).__anext__()) for _ in range(40)]
    try:
        await asyncio.sleep(0.3)
        try:
            probe = await asyncio.wait_for(asyncio.to_thread(lambda: 'ran'), 3.0)
        except TimeoutError:
            probe = 'starved'
        check(
            'downloads queued for a repack slot hold no worker thread while they wait',
            probe == 'ran',
            f'{len(waiting)} downloads, {_app.settings.repack_workers} slots: {probe}',
        )
    finally:
        release.set()
        for task in waiting:
            task.cancel()
        await asyncio.gather(*waiting, return_exceptions=True)


def _host_key_of(url: str) -> str | None:
    from inksetter.opds.upstream import host_key

    try:
        return host_key(url)
    except Exception:
        return None


async def check_no_stream(c) -> None:
    up = 'http://127.0.0.1:8899'
    strip = profiles.PROFILES['kindle-colorsoft-webtoon']
    check('premise: the webtoon profile is re-cut', strip.reslice)

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
    fixed = encode_token(f'{up}/opds/v1.2/books/7/pages/50')
    divina = encode_token(f'{up}/opds/v2/books/7/manifest')
    webpub = encode_token(f'{up}/opds/v2/books/8/manifest')
    kept = dataclasses.replace(strip, name='smoke-webtoon-kept', reslice=False)
    profiles.PROFILES[kept.name] = kept
    try:
        r = await c.get(f'/{kept.name}/catalog')
        found = re.search(rf'/{kept.name}/p/([\w-]+)\?page=', r.text)
        check('the same profile with the re-cut off streams again', found is not None and 'pse:count="3"' in r.text)
        check(
            'premise: the refusal below uses the token the feed offers', found is not None and found.group(1) == token
        )
        r = await c.get(f'/{kept.name}/p/{token}', params={'page': 0, 'maxWidth': strip.width})
        check('and its stream serves a page', r.status_code == 200 and r.headers['content-type'].startswith('image/'))
        order = (await c.get(f'/{kept.name}/f/{divina}')).json().get('readingOrder', [])
        listed = [e['href'].split('/pf/')[-1] for e in order if '/pf/' in e['href']]
        check('premise: its Divina manifest lists every page through /pf/', len(listed) == 3, f'{len(listed)}')
        check('premise: the refusal below uses a token the manifest offers', fixed in listed)
        r = await c.get(f'/{kept.name}/pf/{fixed}')
        check('and /pf/ serves them', r.status_code == 200 and r.headers['content-type'].startswith('image/'))
        res = (await c.get(f'/{kept.name}/f/{webpub}')).json().get('resources', [])
        check('premise: a webpub routes its images through /pf/ too', any('/pf/' in e['href'] for e in res))
    finally:
        del profiles.PROFILES[kept.name]

    for route, tok in (('p', token), ('pf', fixed)):
        r = await c.get(f'/{strip.name}/{route}/{tok}', params={'page': 0, 'maxWidth': strip.width})
        check(f'/{route}/ refuses a webtoon profile', r.status_code == 404, f'status={r.status_code}')
        said = r.json().get('error', '') if r.headers.get('content-type', '').startswith('application/json') else ''
        check(f'and /{route}/ says to download the book instead', 'download' in said, f'{said!r}')
        r = await c.request('HEAD', f'/{strip.name}/{route}/{tok}', params={'page': 0})
        check(f'HEAD on /{route}/ is refused alike', r.status_code == 404, f'status={r.status_code}')
        r = await c.get(f'/{strip.name}/{route}/' + encode_token(f'{up}/opds/v1.2/upstream500'))
        check(
            f'the /{route}/ refusal comes before the upstream is asked', r.status_code == 404, f'status={r.status_code}'
        )

    man = (await c.get(f'/{strip.name}/f/{divina}')).json()
    check(
        'a webtoon Divina manifest lists no page to stream',
        man.get('readingOrder') == [] and '/pf/' not in json.dumps(man),
        f'{len(man.get("readingOrder", []))} entries',
    )
    book = (await c.get(f'/{strip.name}/f/{webpub}')).json()
    kinds = [e['href'].split('/')[4] for e in book.get('readingOrder', []) + book.get('resources', [])]
    check('a webpub keeps its chapter and stylesheet and loses only the page image', kinds == ['dl', 'dl'], f'{kinds}')

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
    on = json.loads(rewrite.rewrite(body, ctx, 'image/png'))
    off = json.loads(rewrite.rewrite(body, ctx, 'image/png', stream=False))
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

    form = (await c.get(f'/kobo-clara-hd-2e-bw/b/{encode_token(f"{up}/opds/v1.2/form-search")}')).text
    found = re.search(r'<form class="q" action="[^"]*/bs/([\w-]+)"', form)
    check(
        'a form-style search link reaches the browse page as the template it is',
        found is not None and _decoded(found.group(1))[0] == f'{up}/opds/v1.2/series{{?search}}',
        _decoded(found.group(1))[0] if found else 'no search form',
    )
    r = await _get(c, f'/kobo-clara-hd-2e-bw/bs/{found.group(1) if found else "x"}', params={'q': 'dune'})
    check('and searching it fills the template', r.status_code == 200 and 'hits:dune' in r.text, f'{r.status_code}')

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
    narrow = {w: _grid_columns(w) for w in (343, 500, 700)}
    check(
        'a narrow window drops rather than squeezing below the floor',
        narrow == {343: 2, 500: 3, 700: 4},
        f'{narrow}',
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
    check('it counts every page exactly once, in order', seen == [(n, 4) for n in range(5)], f'{seen}')
    check('and the archive it produced is still whole', len(zipfile.ZipFile(io.BytesIO(out)).namelist()) == 5)

    big = _cbz_bytes(8)
    one, many = [], []
    b''.join(
        cbz.repack_iter(
            io.BytesIO(big),
            profiles.PROFILES['kobo-clara-hd-2e-bw'],
            1,
            progress=lambda done, total: one.append((done, total)),
        )
    )
    b''.join(
        cbz.repack_iter(
            io.BytesIO(big),
            profiles.PROFILES['kobo-clara-hd-2e-bw'],
            3,
            progress=lambda done, total: many.append((done, total)),
        )
    )
    check('eight pages are counted one by one', one == [(n, 8) for n in range(9)], f'{one}')
    check('the pooled repack counts the same way', many == one, f'{many}')

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
        {n for n, p in profiles.PROFILES.items() if p.reslice} == set(by_group['Webtoon'])
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
        w, h, _levels = grey_levels(r1.content)
        check('png returned', r1.headers['content-type'] == 'image/png')
        check('fits panel box', w <= 1072 and h <= 1448, f'{w}x{h}')
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

        for _ in range(200):
            if all(task.done() for task in app_mod.prefetcher._tasks):
                break
            await asyncio.sleep(0.05)
        t = time.time()
        r3 = await c.get(f'/kobo-clara-hd-2e-bw/p/{pse}', params={'page': 1, 'maxWidth': 1072})
        pf = time.time() - t
        check('next page prefetched', r3.status_code == 200 and pf < 0.2, f'{pf * 1000:.0f}ms')

        clara = profiles.PROFILES['kobo-clara-hd-2e-bw']
        tpl = rewrite.decode_token(pse)
        key1 = cache_mod.render_key(
            tpl.replace('{pageNumber}', '1').replace('{maxWidth}', str(max(clara.width, 1072))), clara, 1072
        )
        url1 = tpl.replace('{pageNumber}', '1').replace('{maxWidth}', str(max(clara.width, 1072)))
        reads = []
        real_get = app_mod.cache.get

        def get(key):
            reads.append(key)
            return real_get(key)

        with mock.patch.object(app_mod.cache, 'get', get):
            await app_mod._prefetch(url1, clara, 1072, {}, key1)
            skipped = list(reads)
            await app_mod._prefetch(url1, clara, 1072, {}, '0' * 64)
        check('premise: the next page is in the cache', app_mod.cache.has(key1))
        check('premise: a prefetch the cache check lets through reads the cache', key1 in reads, f'{len(reads)} reads')
        check('a page already in the cache is not prefetched again', not skipped, f'{len(skipped)} reads')

        r = await c.get(f'/kobo-clara-hd-2e-bw/p/{pse}', params={'page': 0, 'maxWidth': 600})
        w2, _, _ = grey_levels(r.content)
        check('maxWidth clamps down', w2 <= 600, f'width={w2}')

        print('covers, search, download')
        r = await c.get(f'/kobo-clara-hd-2e-bw/img/{img}')
        check('cover shrunk', r.status_code == 200 and len(r.content) < 30_000, f'{len(r.content)}B')

        r = await c.get(f'/kobo-clara-hd-2e-bw/osd/{osd}')
        check('{searchTerms} survives', '{searchTerms}' in r.text)
        found = re.search(r'/kobo-clara-hd-2e-bw/s/([\w-]+)\?q=', r.text)
        check('and its template goes through the proxy', found is not None, r.text[:80])
        r = await c.get(f'/kobo-clara-hd-2e-bw/s/{found.group(1) if found else "x"}', params={'q': 'sample query'})
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
                r = await _get(c, f'/kobo-clara-hd-2e-bw/{route}/' + encode_token(f'{dead}/x.cbz'))
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
            r = await _get(c, '/kobo-clara-hd-2e-bw/f/' + encode_token(f'http://127.0.0.1:8899/opds/v1.2/{path}'))
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
        whole = _RANGED['whole'] - before['whole']
        plain = await c.get(
            '/kobo-clara-hd-2e-bw/dl/' + encode_token('http://127.0.0.1:8899/opds/v1.2/norange/file.cbz')
        )
        check('a range download succeeds', ranged.status_code == 200, f'status={ranged.status_code}')
        check('so does one that must fall back', plain.status_code == 200, f'status={plain.status_code}')
        check(
            'the range path was actually taken, with no whole-file fetch',
            used > 0 and whole == 0,
            f'{used} range requests, {whole} whole-file fetches',
        )

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
            r = await _get(c, '/kobo-clara-hd-2e-bw/p/' + encode_token(f'http://127.0.0.1:8899/opds/v1.2/{path}'))
            check(
                f'{label} upstream page is 502, not 500',
                r.status_code == 502,
                f'status={r.status_code} {r.text[:60]}',
            )
        for prof in ('kobo-clara-hd-2e-bw', 'kindle-colorsoft'):
            try:
                r = await c.get(f'/{prof}/p/' + encode_token('http://127.0.0.1:8899/opds/v1.2/halfimage'))
                status, said = r.status_code, r.text[:60]
            except Exception as exc:
                status, said = 500, f'{type(exc).__name__} escaped the route'
            check(f'a page that fails mid-decode is 502, not 500 ({prof})', status == 502, f'status={status} {said}')
        for label, route, path in (
            ('a feed', 'f', 'badgzip.xml'),
            ('a page', 'p', 'badgzip.jpg'),
            ('a download', 'dl', 'badgzip.cbz'),
        ):
            try:
                tok = encode_token(f'http://127.0.0.1:8899/opds/v1.2/{path}')
                r = await c.get(f'/kobo-clara-hd-2e-bw/{route}/{tok}')
                status, said = r.status_code, r.text[:70]
            except Exception as exc:
                status, said = 500, f'{type(exc).__name__} escaped the route'
            check(f'{label} the upstream garbles is 502, not 500', status == 502, f'status={status} {said}')
            if route == 'p':
                check('and it is not called unreachable', 'unreachable' not in said and 'badly' in said, said)
        for label, route, path in (
            ('a feed that redirects to a malformed URL', 'f', 'bad-location'),
            ('a download that redirects to a malformed URL', 'dl', 'bad-location'),
            ('a JSON feed nested too deep to rewrite', 'f', 'deep-json'),
        ):
            tok = encode_token(f'http://127.0.0.1:8899/opds/v1.2/{path}')
            r = await _get(c, f'/kobo-clara-hd-2e-bw/{route}/{tok}')
            check(f'{label} is 502, not 500', r.status_code == 502, f'status={r.status_code} {r.text[:70]}')
        tok = encode_token('http://127.0.0.1:8899/opds/v1.2/series?search={searchTerms}')
        r = await _get(c, f'/kobo-clara-hd-2e-bw/s/{tok}?q=' + '+' * 30000)
        check(
            'a search term that fills in past any URL httpx will take is 502, not 500',
            r.status_code == 502,
            f'status={r.status_code} {r.text[:70]}',
        )
        r = await _get(c, '/kobo-clara-hd-2e-bw/p/' + encode_token('http://127.0.0.1:8899/opds/v1.2/upstream500'))
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
        rp = hg
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

        print('malformed input')
        await check_malformed_input(c)

        print('epub downloads')
        await check_epub(c)

        print('security')
        await check_security(c)
        print('failing closed')
        await check_fail_closed(c)

        print('http edges')
        await check_http_edges(c)

        print('browse pages')
        await check_browse(c)
        await check_trail(c)
        await check_logo(c)
        check_grid_columns()
        check_landing_groups()

        print('download progress')
        check_repack_progress()
        check_job_board()
        check_progress_script()

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
        b''.join(cbz.repack_iter(io.BytesIO(bomb.getvalue()), profiles.PROFILES['kobo-clara-hd-2e-bw']))
        check('a highly compressible archive is no longer refused', True)
    except ValueError as exc:
        check('a highly compressible archive is no longer refused', False, str(exc)[:52])

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
    print('seam defects')
    check_seam_defects()
    print('search templates')
    check_search_templates()
    print('rounding and bit depth')
    check_rounding()
    check_bit_depth()
    print('lazy decode')
    check_lazy_decode()
    print('webtoon re-slicing')
    check_reslice()
    check_reslice_edges()
    print('no gutter in reach')
    check_strip_rescue()
    print('split artwork')
    check_strip_plan()
    print('profile config')
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
    print('pipeline version')
    check_pipeline_version()
    print('png compression')
    check_png_compression()
    print('upscale kernel')
    check_upscale_kernel()
    print('comicinfo')
    await check_comicinfo()
    print('png effort')
    check_png_effort()
    print('colour pad ring')
    check_colour_pad_ring()
    print('webtoon pad')
    check_webtoon_pad()
    print('gutter pad')
    check_gutter_pad()
    print('strip width')
    check_strip_width()
    print('pad seam')
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
    check_cpu_budget()
    print('cache default')
    check_cache_default()
    print('settings file')
    check_settings_file()
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
