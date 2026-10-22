"""
Fixed-layout AZW3 (KF8) generator for the Kindle's own reader.
Loosely based on Calibre KF8 writer (GPL v3, Kovid Goyal).
"""

import re
import time
import pyvips
import uuid
import bisect
import struct
import logging
import dataclasses
from xml.etree import ElementTree
from collections.abc import Iterator

from .pipeline import UnreadableImage, open_image
from .profiles import Profile

log = logging.getLogger(__name__)

MIME = 'application/x-mobi8-ebook'
SUFFIX = '.azw3'
KINDLE_PREFIX = 'kindle-'

TEXT_RECORD = 4096
INDEX_LIMIT = 0x10000 - 192 - 1048
CNCX_LIMIT = 0x10000 - 1024
INDEX_HEADER = 192
NAME_PAD = 8192
MAX_RECORDS = 0xFFFF
COPY_BLOCK = 1 << 20
THUMB_SIZE = (900, 1350)
THUMB_QUALITY = 85
COMICINFO_MAX = 4 << 20
AUTHORS_MAX = 20

_BASE32 = '0123456789ABCDEFGHIJKLMNOPQRSTUV'
_PICTURES = (
    (b'\x89PNG\r\n\x1a\n', 'image/png'),
    (b'\xff\xd8\xff', 'image/jpeg'),
    (b'GIF87a', 'image/gif'),
    (b'GIF89a', 'image/gif'),
)

_SKELETON = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    '<html xmlns="http://www.w3.org/1999/xhtml" class="Inksetter">\n'
    '  <head>\n'
    '    <title>Page {n}</title>\n'
    '    <meta name="viewport" content="width={w}, height={h}"/>\n'
    '    <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>\n'
    '  <link rel="stylesheet" type="text/css" href="kindle:flow:0001?mime=text/css"/>\n'
    '</head>\n'
    '  '
)
_BODY = '<body class="Inksetter1" aid="{aid}">'
_CLOSE = '</body>\n</html>\n'
_FRAGMENT = (
    '<div class="Inksetter1" aid="{aid}"><img src="kindle:embed:{embed}?mime={mime}" alt="" class="{cls}"/></div>'
)
_CSS_HEAD = (
    '.Inksetter {\n    margin: 0;\n    padding: 0\n    }\n'
    '.Inksetter1 {\n    display: block;\n    margin: 0;\n    padding: 0\n    }\n'
)
_CSS_IMG = (
    '.{cls} {{\n    display: block;\n    height: {h}px;\n    width: {w}px;\n{margin}'
    '    padding: 0;\n    border: currentColor none 0\n    }}\n'
)

_SKEL_TAGS = ((1, 1, 3), (6, 2, 12))
_CHUNK_TAGS = ((2, 1, 1), (3, 1, 2), (4, 1, 4), (6, 2, 8))
_NCX_TAGS = ((1, 1, 1), (2, 1, 2), (3, 1, 4), (4, 1, 8), (21, 1, 16), (22, 1, 32), (23, 1, 64), (6, 2, 128))

_FIXED_LAYOUT = {
    122: b'true',  # fixed-layout
    123: b'comic',  # book-type
    124: b'portrait',  # orientation-lock
    127: b'true',  # zero-gutter
    128: b'true',  # zero-margin
    132: b'false',  # RegionMagnification
}
_RIGHT_TO_LEFT = frozenset({'yes', 'yesandrighttoleft'})
_FLIS = bytes.fromhex('464c4953000000080041000000000000ffffffff000100030000000300000001ffffffff')
_EOF = b'\xe9\x8e\r\n'
_JFIF = b'\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00'


def profile_for(p: Profile) -> Profile | None:
    # A webtoon volume runs to gigabytes, and a book past 2 GiB never reaches the library.
    if not p.name.startswith(KINDLE_PREFIX) or p.fmt == 'raw' or p.reslice:
        return None
    # KF8 holds PNG, JPEG and GIF pages; a WebP page becomes the palette PNG.
    return dataclasses.replace(p, fmt='pngc') if p.fmt == 'webpc' else p


def _fvwi(n: int) -> bytes:
    out = [n & 0x7F | 0x80]
    n >>= 7
    while n:
        out.append(n & 0x7F)
        n >>= 7
    return bytes(reversed(out))


def _bvwi(n: int) -> bytes:
    out = [n & 0x7F]
    n >>= 7
    while n:
        out.append(n & 0x7F)
        n >>= 7
    out[-1] |= 0x80
    return bytes(reversed(out))


def _base32(n: int, digits: int = 1) -> str:
    out = ''
    while n:
        n, d = divmod(n, 32)
        out = _BASE32[d] + out
    return out.rjust(digits, '0')


def _align(raw: bytes) -> bytes:
    return raw + b'\0' * (-len(raw) % 4)


def _trailing(raw: bytes) -> bytes:
    size = 1
    while len(_bvwi(len(raw) + size)) != size:
        size += 1
    return raw + _bvwi(len(raw) + size)


@dataclasses.dataclass(frozen=True)
class _Picture:
    at: int
    size: int
    mime: str
    width: int
    height: int


def _picture(blob: bytes) -> tuple[bytes, str, int, int] | None:
    try:
        im = pyvips.Image.new_from_buffer(blob, '')
        mime = next((m for sig, m in _PICTURES if blob.startswith(sig)), None)
        if mime is None:
            blob, mime = im.pngsave_buffer(), 'image/png'
    except pyvips.Error:
        return None
    return blob, mime, im.width, im.height


def _comicinfo(raw: bytes | None) -> tuple[list[str], bool]:
    if not raw or len(raw) > COMICINFO_MAX:
        return [], False
    try:
        root = ElementTree.fromstring(raw)
    except ElementTree.ParseError, ValueError:
        return [], False
    fields = {child.tag.rpartition('}')[2]: (child.text or '').strip() for child in root}
    authors: list[str] = []
    for name in (fields.get('Writer') or fields.get('Penciller') or '').split(','):
        name = ' '.join(name.split())
        if name and name not in authors and len(authors) < AUTHORS_MAX:
            authors.append(name)
    return authors, fields.get('Manga', '').lower() in _RIGHT_TO_LEFT


def _thumbnail(blob: bytes) -> tuple[bytes, int, int] | None:
    try:
        im = open_image(blob)
        if im.hasalpha():
            im = im.flatten(background=255)
        im = im.thumbnail_image(THUMB_SIZE[0], height=THUMB_SIZE[1], size='down')
        jpeg = im.jpegsave_buffer(Q=THUMB_QUALITY, keep='none')
    except pyvips.Error, UnreadableImage:
        return None
    if jpeg[2:4] != b'\xff\xe0':
        jpeg = jpeg[:2] + _JFIF + jpeg[2:]
    return jpeg, im.width, im.height


class _Cncx:
    def __init__(self) -> None:
        self.records: list[bytes] = []
        self._buf = bytearray()
        self._at: dict[str, int] = {}

    def __call__(self, text: str) -> int:
        if text not in self._at:
            raw = text.encode('utf-8')
            raw = _fvwi(len(raw)) + raw
            if len(self._buf) + len(raw) > CNCX_LIMIT:
                self.records.append(_align(bytes(self._buf)))
                self._buf.clear()
            self._at[text] = len(self.records) * 0x10000 + len(self._buf)
            self._buf += raw
        return self._at[text]

    def done(self) -> list[bytes]:
        if self._buf:
            self.records.append(_align(bytes(self._buf)))
            self._buf.clear()
        return self.records


def _tagx(tags) -> bytes:
    body = b''.join(bytes((tag, values, mask, 0)) for tag, values, mask in tags) + b'\0\0\0\1'
    return b'TAGX' + struct.pack('>II', 12 + len(body), 1) + body


def _entry(key: bytes, values: dict[int, list[int]], tags) -> bytes:
    control = 0
    for tag, per, mask in tags:
        groups = len(values.get(tag, ())) // per
        control |= (groups << ((mask & -mask).bit_length() - 1)) & mask
    raw = bytes((len(key),)) + key + bytes((control,))
    return raw + b''.join(_fvwi(v) for tag, _, _ in tags for v in values.get(tag, ()))


def _index(entries: list[tuple[bytes, dict[int, list[int]]]], tags, cncx_records: int) -> list[bytes]:
    blocks: list[list[bytes]] = [[]]
    sizes = [0]
    lasts = [b'']
    for key, values in entries:
        raw = _entry(key, values, tags)
        if sizes[-1] + 2 * len(blocks[-1]) + len(raw) + 2 > INDEX_LIMIT:
            blocks.append([])
            sizes.append(0)
            lasts.append(b'')
        blocks[-1].append(raw)
        sizes[-1] += len(raw)
        lasts[-1] = key

    records = []
    for block in blocks:
        at, idxt = INDEX_HEADER, b'IDXT'
        for raw in block:
            idxt += struct.pack('>H', at)
            at += len(raw)
        body = _align(b''.join(block))
        records.append(
            b'INDX'
            + struct.pack('>III', INDEX_HEADER, 0, 1)
            + struct.pack('>III', 0, INDEX_HEADER + len(body), len(block))
            + b'\xff' * 8
            + b'\0' * 156
            + body
            + _align(idxt)
        )

    tagx = _tagx(tags)
    geometry, idxt = b'', b'IDXT'
    for last, block in zip(lasts, blocks, strict=True):
        idxt += struct.pack('>H', INDEX_HEADER + len(tagx) + len(geometry))
        geometry += bytes((len(last),)) + last + struct.pack('>H', len(block))
    geometry = _align(geometry)
    header = (
        b'INDX'
        + struct.pack('>I', INDEX_HEADER)
        + b'\0' * 8
        + struct.pack('>IIIII', 2, INDEX_HEADER + len(tagx) + len(geometry), len(records), 65001, 0xFFFFFFFF)
        + struct.pack('>IIIII', len(entries), 0, 0, 0, cncx_records)
        + b'\0' * 124
        + struct.pack('>I', INDEX_HEADER)
        + b'\0' * 8
    )
    return [header + tagx + geometry + _align(idxt), *records]


def _tbs(entries: list[tuple[int, int]], starts: list[int], start: int, end: int) -> bytes:
    first = max(0, bisect.bisect_right(starts, start) - 1)
    if first < len(entries) and sum(entries[first]) <= start:
        first += 1
    count = bisect.bisect_left(starts, end) - first
    if count <= 0:
        return b''
    last = first + count - 1
    flags = 0b010 | (0b100 if count > 1 else 0)
    spans = entries[last][0] < start and sum(entries[last]) > end
    if spans:
        flags |= 0b001
    out = _fvwi(first << 3 | flags) + _fvwi(8)
    if count > 1:
        out += bytes((count,))
    if spans:
        out += _fvwi(0)
    return out


class Book:
    def __init__(self, spool, width: int, height: int, title: str, ident: str) -> None:
        self._spool = spool
        self._width, self._height = width, height
        self._title = title or 'Untitled'
        self._asin = str(uuid.uuid5(uuid.NAMESPACE_URL, f'inksetter-azw3:{ident}'))
        self._images: list[_Picture] = []
        self._pages: list[int] = []
        self._thumbnail: int | None = None
        self._at = 0
        self._layout = (0, 0, 0)
        self._comicinfo: bytes | None = None
        self._authors: list[str] = []
        self._rtl = False

    def _spooled(self, data: bytes, mime: str, w: int, h: int) -> int:
        self._spool.write(data)
        self._images.append(_Picture(self._at, len(data), mime, w, h))
        self._at += len(data)
        return len(self._images) - 1

    def page(self, name: str, blob: bytes, source: bytes | None = None) -> None:
        picture = _picture(blob)
        if picture is None:
            log.warning('azw3: %s is not a picture the Kindle can show; the book goes without it', name)
            return
        self._pages.append(self._spooled(*picture))
        if len(self._pages) == 1:
            thumbnail = (_thumbnail(source) if source else None) or _thumbnail(picture[0])
            if thumbnail is not None:
                self._thumbnail = self._spooled(thumbnail[0], 'image/jpeg', *thumbnail[1:])

    def copy(self, name: str, data: bytes) -> None:
        if self._comicinfo is None and name.lower().endswith('comicinfo.xml'):
            self._comicinfo = data

    def ready(self) -> bytes:
        return b''

    def finish(self, meta: bytes | None = None) -> Iterator[bytes]:
        if not self._pages:
            raise ValueError('a Kindle book needs at least one page')
        self._authors, self._rtl = _comicinfo(self._comicinfo or meta)
        yield self._head()
        self._spool.seek(0)
        left = self._at
        while left:
            block = self._spool.read(min(COPY_BLOCK, left))
            if not block:
                raise OSError('the page spool ended early')
            left -= len(block)
            yield block
        yield self._tail()

    def close(self) -> None:
        self._spool.close()

    # ----------------------------------------------------------------------

    def _classes(self) -> tuple[list[str], str]:
        names: dict[tuple[int, int, int, int], str] = {}
        css = _CSS_HEAD
        out = []
        for index in self._pages:
            pic = self._images[index]
            scale = min(self._width / pic.width, self._height / pic.height)
            w, h = round(pic.width * scale), round(pic.height * scale)
            left, top = (self._width - w) // 2, (self._height - h) // 2
            box = (w, h, left, top)
            if box not in names:
                names[box] = f'Inksetter{len(names) + 2}'
                margin = '    margin: 0;\n' if not (left or top) else f'    margin: {top}px 0 0 {left}px;\n'
                css += _CSS_IMG.format(cls=names[box], w=w, h=h, margin=margin)
            out.append(names[box])
        return out, css

    def _text(self):
        classes, css = self._classes()
        flow = []
        skel, chunks, toc = [], [], []
        at = 0
        for i, (index, cls) in enumerate(zip(self._pages, classes, strict=True)):
            body_aid, div_aid = _base32(i * 1000000), _base32(i * 1000000 + 1)
            head = _SKELETON.format(n=i + 1, w=self._width, h=self._height) + _BODY.format(aid=body_aid)
            skeleton = head + _CLOSE
            mime = self._images[index].mime
            fragment = _FRAGMENT.format(aid=div_aid, embed=_base32(index + 1, 4), mime=mime, cls=cls)
            skel.append((at, len(skeleton)))
            chunks.append((at + len(head), f"P-//*[@aid='{body_aid}']", len(fragment)))
            toc.append(at + len(head) - len(_BODY.format(aid=body_aid)))
            flow.append(skeleton + fragment)
            at += len(skeleton) + len(fragment)
        html = ''.join(flow).encode('ascii')
        return html, css.encode('ascii'), skel, chunks, toc

    def _head(self) -> bytes:
        html, css, skel, chunks, toc_at = self._text()
        text = html + css
        n = len(self._pages)

        toc = [(at, (toc_at[i + 1] if i + 1 < n else len(html)) - at) for i, at in enumerate(toc_at)]
        records = []
        pad = 0
        for start in range(0, len(text), TEXT_RECORD):
            data = text[start : start + TEXT_RECORD] + b'\0'
            pad += len(data)
            records.append(data + _trailing(_tbs(toc, toc_at, start, start + len(data) - 1)))
        text_records = len(records)
        if pad % 4:
            records.append(b'\0' * (pad % 4))

        cncx = _Cncx()
        chunk_index = _index(
            [
                (b'%010d' % insert, {2: [cncx(selector)], 3: [i], 4: [i], 6: [0, length]})
                for i, (insert, selector, length) in enumerate(chunks)
            ],
            _CHUNK_TAGS,
            len(cncx.done()),
        )
        chunk_cncx = cncx.records
        skel_index = _index(
            [(b'SKEL%010d' % i, {1: [1, 1], 6: [at, length, at, length]}) for i, (at, length) in enumerate(skel)],
            _SKEL_TAGS,
            0,
        )
        labels = _Cncx()
        width = max(2, len(f'{n - 1:X}'))
        toc_entries = [
            (f'{i:0{width}X}'.encode(), {1: [at], 2: [length], 3: [labels(f'Page {i + 1}')], 4: [0], 6: [i, 0]})
            for i, (at, length) in enumerate(toc)
        ]
        ncx_index = _index(toc_entries, _NCX_TAGS, len(labels.done()))

        first_index = 1 + len(records)
        chunk_at = first_index
        skel_at = chunk_at + len(chunk_index) + len(chunk_cncx)
        ncx_at = skel_at + len(skel_index)
        records += chunk_index + chunk_cncx + skel_index + ncx_index + labels.records
        first_image = 1 + len(records)
        fdst_at = first_image + len(self._images)
        total = fdst_at + 4
        if total > MAX_RECORDS:
            raise ValueError(f'{n} pages take {total} records, past the {MAX_RECORDS} a PalmDB holds')
        self._layout = (fdst_at, len(html), len(text))

        record0 = self._record0(len(text), text_records, first_index, first_image, fdst_at, ncx_at, chunk_at, skel_at)
        sizes = [len(record0), *map(len, records), *(p.size for p in self._images), 28, len(_FLIS), 52, len(_EOF)]
        offset = 78 + 8 * total + 2
        table = bytearray()
        for i, size in enumerate(sizes):
            table += struct.pack('>II', offset, 2 * i)
            offset += size
        if offset > 0xFFFFFFFF:
            raise ValueError('the book is past the 4 GiB a PalmDB can address')
        name = re.sub(r'[^-A-Za-z0-9]+', '_', self._title).strip('_')[:31] or 'book'
        now = int(time.time())
        palmdb = (
            name.encode('ascii').ljust(32, b'\0')
            + struct.pack('>HHIII', 0, 0, now, now, 0)
            + struct.pack('>III', 0, 0, 0)
            + b'BOOKMOBI'
            + struct.pack('>IIH', 2 * total - 1, 0, total)
        )
        return palmdb + bytes(table) + b'\0\0' + record0 + b''.join(records)

    def _tail(self) -> bytes:
        _, html_length, text_length = self._layout
        fdst = b'FDST' + struct.pack('>II', 12, 2) + struct.pack('>IIII', 0, html_length, html_length, text_length)
        fcis = (
            b'FCIS'
            + struct.pack('>IIIII', 20, 16, 2, 0, text_length)
            + struct.pack('>IIIII', 0, 40, 0, 40, 8)
            + struct.pack('>HHI', 1, 1, 0)
        )
        return fdst + _FLIS + fcis + _EOF

    def _exth(self) -> bytes:
        number = lambda v: struct.pack('>I', v)  # noqa: E731
        cover = self._pages[0]
        records = [
            (113, self._asin.encode()),
            (501, b'PDOC'),
            (503, self._title.encode('utf-8')),
            (524, b'en'),
            (125, number(len(self._images))),
            (131, number(0)),
            (201, number(cover)),
            *([(202, number(self._thumbnail))] if self._thumbnail is not None else []),
            (203, number(0)),
            (129, f'kindle:embed:{_base32(cover + 1, 4)}'.encode()),
            (204, number(200)),
            (205, number(2)),
            (206, number(9)),
            (207, number(0)),
            (535, b'0730-890adc2'),
            (528, b'true'),
            (126, f'{self._width}x{self._height}'.encode()),
            *_FIXED_LAYOUT.items(),
            (525, b'horizontal-rl' if self._rtl else b'horizontal-lr'),
            (527, b'rtl' if self._rtl else b'ltr'),
            *((100, name.encode('utf-8')) for name in self._authors),
        ]
        body = b''.join(struct.pack('>II', code, 8 + len(value)) + value for code, value in records)
        return b'EXTH' + struct.pack('>II', 12 + len(body), len(records)) + body

    def _record0(self, text_length, text_records, first_index, first_image, fdst_at, ncx_at, chunk_at, skel_at):
        exth = self._exth()
        exth += b'\0' * (4 - len(exth) % 4)
        title = self._title.encode('utf-8')
        uid = uuid.UUID(self._asin).int & 0xFFFFFFFF
        none = 0xFFFFFFFF
        mobi = (
            b'MOBI'
            + struct.pack('>IIII', 264, 2, 65001, uid)
            + struct.pack('>I', 8)
            + b'\xff' * 40
            + struct.pack('>IIIIII', first_index, 16 + 264 + len(exth), len(title), 9, 0, 0)
            + struct.pack('>II', 8, first_image)
            + b'\0' * 16
            + struct.pack('>I', 0x50)
            + b'\0' * 32
            + struct.pack('>IIIII', none, none, 0, 0, 0)
            + b'\0' * 8
            + struct.pack('>IIIIII', fdst_at, 2, fdst_at + 2, 1, fdst_at + 1, 1)
            + b'\0' * 8
            + struct.pack('>II', none, 0)
            + b'\xff' * 8
            + struct.pack('>IIIIII', 3, ncx_at, chunk_at, skel_at, none, none)
            + struct.pack('>IIII', none, 0, none, 0)
        )
        assert len(mobi) == 264, len(mobi)
        palmdoc = struct.pack('>HHIHHHH', 1, 0, text_length, text_records, TEXT_RECORD, 0, 0)
        return palmdoc + mobi + exth + title + b'\0' * NAME_PAD
