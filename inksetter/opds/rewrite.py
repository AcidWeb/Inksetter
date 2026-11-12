"""
Feed rewriting for OPDS 1.x (Atom), OPDS 2.0 (JSON) and Readium manifests.
"""

import re
import copy
import json
import base64
import codecs
from lxml import etree
from dataclasses import dataclass
from collections.abc import Callable
from urllib.parse import quote, urljoin, urlsplit


ATOM = 'http://www.w3.org/2005/Atom'
OSD_NS = 'http://a9.com/-/spec/opensearch/1.1/'

PSE_REL = 'http://vaemendis.net/opds-pse/stream'
PSE_COUNT = '{http://vaemendis.net/opds-pse/ns}count'
IMAGE_RELS = {
    'http://opds-spec.org/image',
    'http://opds-spec.org/image/thumbnail',
    'http://opds-spec.org/cover',
    'http://opds-spec.org/cover/thumbnail',
    'http://opds-spec.org/thumbnail',
}
ACQ_PREFIX = 'http://opds-spec.org/acquisition'
NAV_RELS = {
    'self',
    'start',
    'up',
    'next',
    'previous',
    'prev',
    'first',
    'last',
    'subsection',
    'collection',
    'alternate',
    'related',
    'search',
    'http://opds-spec.org/sort/new',
    'http://opds-spec.org/sort/popular',
    'http://opds-spec.org/facet',
    'http://opds-spec.org/crawlable',
    'http://opds-spec.org/shelf',
    'http://opds-spec.org/subscriptions',
}
JSON_TYPES = (
    'opds+json',
    'webpub+json',
    'divina+json',
    'audiobook+json',
    'opds-publication+json',
    'application/json',
)
PAGE_CONTAINERS = ('readingOrder', 'resources')
MAX_PARTS = 99
SAME_RESOURCE_KEYS = ('alternate',)
SEARCH_VARIABLES = frozenset({'searchTerms', 'query', 'search'})
GENERIC_TYPES = frozenset({'', 'text/plain', 'application/octet-stream', 'binary/octet-stream'})
_SEAL = '~key~'
_KEY_SPOTS = re.compile(r'(?<=/api/opds/)~key~(?=[/?#]|$)|(?<=[?&]apikey=)~key~(?=[&#]|$)', re.I)
EXPRESSION = re.compile(r'\{([?&]?)([^{}]*)\}')


class TokenError(ValueError):
    pass


class FeedError(ValueError):
    pass


def _secret() -> str:
    from .. import kavita

    return kavita.api_key()


def _mirrored(url: str) -> str | None:
    from .. import kavita

    return kavita.mirror_link(url)


def encode_token(url: str, cover: str | None = None) -> str:
    raw = f'{url}\n{cover}' if cover else url
    secret = _secret()
    if secret:
        raw = raw.replace(secret, _SEAL)
    return base64.urlsafe_b64encode(raw.encode('utf-8')).decode('ascii').rstrip('=')


def _fetchable(url: str) -> bool:
    try:
        parts = urlsplit(url)
        whole = parts.scheme.lower() in ('http', 'https') and bool(parts.hostname) and parts.port != 0
    except ValueError:
        return False
    return whole and not any(ord(c) < 0x20 or ord(c) == 0x7F for c in url)


def decode_parts(token: str) -> tuple[str, str | None]:
    pad = '=' * (-len(token) % 4)
    try:
        raw = base64.urlsafe_b64decode(token + pad).decode('utf-8')
    except ValueError as exc:
        raise TokenError(f'malformed token: {token[:24]}') from exc
    secret = _secret()
    url, _, cover = raw.partition('\n')
    url, cover = _unseal(url, secret), _unseal(cover, secret)
    if url is None or not _fetchable(url):
        raise TokenError(f'malformed token: {token[:24]}')
    return url, cover if cover and _fetchable(cover) else None


def _unseal(url: str, secret: str) -> str | None:
    if _SEAL not in url or not secret:
        return url
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    head = f'{parts.scheme}://{parts.netloc}'
    if url[: len(head)].lower() != head.lower():
        return None
    out = url[: len(head)] + _KEY_SPOTS.sub(secret, url[len(head) :])
    return None if _SEAL in out else out


def decode_token(token: str) -> str:
    return decode_parts(token)[0]


def _names(spec: str) -> list[str]:
    return [n.strip() for n in spec.split(',') if n.strip()]


def _bare(name: str) -> str:
    return name.removesuffix('?').partition(':')[0].removesuffix('*')


def _is_search_name(name: str) -> bool:
    return _bare(name) in SEARCH_VARIABLES


def is_search_template(href: str | None) -> bool:
    return any(_is_search_name(n) for _, spec in EXPRESSION.findall(href or '') for n in _names(spec))


def fill_search(template: str, term: str) -> str:
    def value(name: str) -> str:
        size = name.removesuffix('?').partition(':')[2]
        most = int(size) if size.isdecimal() else 0
        return quote(term[:most] if 0 < most < 10000 else term, safe='')

    def expand(match: re.Match[str]) -> str:
        op, spec = match.groups()
        names = _names(spec)
        if op:
            pairs = [f'{_bare(n)}={value(n)}' for n in names if _is_search_name(n)]
            return f'{op}{"&".join(pairs)}' if pairs else ''
        found = [n for n in names if _is_search_name(n)]
        if found:
            return ','.join(value(n) for n in found)
        return '' if spec.strip().endswith('?') else match.group(0)

    return EXPRESSION.sub(expand, template)


def _unfilled(href: str) -> str:
    return EXPRESSION.sub(lambda m: '' if m.group(1) else m.group(0), href)


@dataclass(frozen=True)
class Ctx:
    profile: str
    public_base: str
    base_url: str
    parts: Callable[[str, int], int] | None = None


def parts_of(ctx: Ctx, href: str | None, count: int) -> int:
    if ctx.parts is None or not href:
        return 1
    try:
        n = ctx.parts(urljoin(ctx.base_url, _unfilled(href)), count)
    except Exception:
        return 1
    return min(MAX_PARTS, n) if isinstance(n, int) and n > 1 else 1


def part_href(href: str, part: int, parts: int) -> str:
    return f'{href}{"&" if "?" in href else "?"}part={part}&parts={parts}'


def part_title(title: str, part: int, parts: int) -> str:
    return f'{title} ({part} of {parts})'


def is_acquisition(rel) -> bool:
    return any(r.startswith(ACQ_PREFIX) for r in (rel or '').split())


def _page_count(raw) -> int:
    text = str(raw if raw is not None else '').strip()
    return int(text) if text.isdecimal() and len(text) < 10 else 0


def entry_parts(entry, ctx: Ctx) -> int:
    if ctx.parts is None:
        return 1
    links = [link for link in entry.iter(f'{{{ATOM}}}link') if link.get('href')]
    book = next((link for link in links if is_acquisition(link.get('rel'))), None)
    if book is None:
        return 1
    count = max((_page_count(link.get(PSE_COUNT)) for link in links), default=0)
    return parts_of(ctx, book.get('href'), count)


def classify(rel: str | None, type_: str | None, href: str | None) -> str:
    rels = set((rel or '').split())
    t = (type_ or '').lower()
    h = href or ''
    if PSE_REL in rels or '{pageNumber}' in h:
        return 'p'
    if rels & IMAGE_RELS:
        return 'img'
    if 'opensearchdescription' in t:
        return 'osd'
    if any(r.startswith(ACQ_PREFIX) for r in rels):
        return 'dl'
    if is_search_template(h):
        return 's'
    if 'atom+xml' in t or any(j in t for j in JSON_TYPES):
        return 'f'
    if t.startswith('image/'):
        return 'img'
    if rels & NAV_RELS:
        return 'f'
    return 'dl'


def map_href(
    href: str,
    kind: str,
    ctx: Ctx,
    search_style: str = 'opensearch',
    cover: str | None = None,
    count: int = 0,
) -> str:
    if not href:
        return href
    if kind not in ('p', 's', 'bs'):
        href = _unfilled(href)
    try:
        scheme = urlsplit(href).scheme.lower()
        target = urljoin(ctx.base_url, href)
    except ValueError:
        return ''
    if scheme and scheme not in ('http', 'https'):
        return href
    try:
        jacket = urljoin(ctx.base_url, cover) if cover and kind == 'dl' else None
    except ValueError:
        jacket = None

    token = encode_token(target, jacket)
    prefix = f'{ctx.public_base}/{quote(ctx.profile)}'

    if kind == 'p':
        pages = f'&count={count}' if count > 0 else ''
        stream = _mirrored(target) or f'/p/{token}?page={{pageNumber}}'
        return f'{prefix}{stream}&maxWidth={{maxWidth}}{pages}'
    if kind == 's':
        if search_style == 'opds2':
            return f'{prefix}/s/{token}?query={{query}}'
        return f'{prefix}/s/{token}?q={{searchTerms}}'
    return f'{prefix}/{kind}/{token}'


# --------------------------------------------------------------------------
# OPDS 1.x (Atom) + OPDS-PSE
# --------------------------------------------------------------------------


def _parser() -> etree.XMLParser:
    return etree.XMLParser(recover=True, resolve_entities=False, no_network=True)


def root_or_none(body: bytes):
    try:
        root = etree.fromstring(body, parser=_parser())
    except etree.XMLSyntaxError:
        return None
    return root


def entry_cover(entry) -> str | None:
    for rel in (
        'http://opds-spec.org/image',
        'http://opds-spec.org/cover',
        'http://opds-spec.org/image/thumbnail',
        'http://opds-spec.org/cover/thumbnail',
        'http://opds-spec.org/thumbnail',
    ):
        for link in entry.iter(f'{{{ATOM}}}link'):
            if rel in (link.get('rel') or '').split() and link.get('href'):
                return link.get('href')
    return None


def _rewrite_link(link, ctx: Ctx, page_mime: str | None, cover: str | None) -> None:
    href = link.get('href')
    if not href:
        return
    kind = classify(link.get('rel'), link.get('type'), href)
    link.set('href', map_href(href, kind, ctx, cover=cover, count=_page_count(link.get(PSE_COUNT))))
    if kind == 'p' and page_mime:
        link.set('type', page_mime)


def _split_entry(entry, parts: int) -> None:
    parent = entry.getparent()
    at = parent.index(entry)
    for part in range(1, parts + 1):
        clone = copy.deepcopy(entry)
        for tag in ('title', 'id'):
            el = clone.find(f'{{{ATOM}}}{tag}')
            if el is not None and (el.text or '').strip():
                el.text = part_title(el.text.strip(), part, parts)
        for link in clone.iter(f'{{{ATOM}}}link'):
            if link.get('href') and is_acquisition(link.get('rel')):
                link.set('href', part_href(link.get('href'), part, parts))
        parent.insert(at + part - 1, clone)
    parent.remove(entry)


def rewrite_atom(body: bytes, ctx: Ctx, page_mime: str | None, stream: bool = True) -> bytes:
    root = root_or_none(body)
    if root is None:
        return _unrewritten(body)

    if root.tag == f'{{{OSD_NS}}}OpenSearchDescription':
        return _rewrite_osd(root, ctx)

    for tag in ('icon', 'logo'):
        for el in root.iter(f'{{{ATOM}}}{tag}'):
            href = (el.text or '').strip()
            if href:
                el.text = map_href(href, 'img', ctx)

    if not stream:
        for link in list(root.iter(f'{{{ATOM}}}link')):
            if link.get('href') and classify(link.get('rel'), link.get('type'), link.get('href')) == 'p':
                link.getparent().remove(link)

    done: set = set()
    long: list = []
    for entry in root.iter(f'{{{ATOM}}}entry'):
        parts = entry_parts(entry, ctx)
        cover = entry_cover(entry)
        for link in entry.iter(f'{{{ATOM}}}link'):
            _rewrite_link(link, ctx, page_mime, cover)
            done.add(link)
        if parts > 1:
            long.append((entry, parts))
    for link in root.iter(f'{{{ATOM}}}link'):
        if link not in done:
            _rewrite_link(link, ctx, page_mime, None)
    for entry, parts in long:
        _split_entry(entry, parts)

    return etree.tostring(root, xml_declaration=True, encoding='utf-8')


def search_template(body: bytes) -> str | None:
    root = root_or_none(body)
    if root is None:
        return None
    for url in root.iter(f'{{{OSD_NS}}}Url'):
        tpl = url.get('template')
        if tpl and is_search_template(tpl):
            return tpl
    return None


def _rewrite_osd(root, ctx: Ctx) -> bytes:
    for url in root.iter(f'{{{OSD_NS}}}Url'):
        tpl = url.get('template')
        if tpl:
            url.set('template', map_href(tpl, 's', ctx))
    return etree.tostring(root, xml_declaration=True, encoding='utf-8')


# --------------------------------------------------------------------------
# OPDS 2.0 and Readium manifests (JSON)
# --------------------------------------------------------------------------


def _rel_string(rel) -> str | None:
    if isinstance(rel, str):
        return rel
    if isinstance(rel, list):
        return ' '.join(r for r in rel if isinstance(r, str))
    return None


def _publication_cover(node: dict) -> str | None:
    images = node.get('images')
    if not isinstance(images, list):
        return None
    for image in images:
        if isinstance(image, dict) and isinstance(image.get('href'), str):
            return image['href']
    return None


def _json_kind(node: dict, container: str | None) -> str:
    type_ = node.get('type') if isinstance(node.get('type'), str) else None
    if container == 'images':
        return 'img'
    if container == 'pages':
        return 'pf' if (type_ or '').lower().startswith('image/') else 'dl'
    return classify(_rel_string(node.get('rel')), type_, node['href'])


def _walk_json(
    node,
    ctx: Ctx,
    page_mime: str | None,
    container: str | None = None,
    cover: str | None = None,
    stream: bool = True,
):
    if isinstance(node, list):
        if not stream:
            node[:] = [
                item
                for item in node
                if not (
                    isinstance(item, dict)
                    and isinstance(item.get('href'), str)
                    and _json_kind(item, container) in ('p', 'pf')
                )
            ]
        for item in node:
            _walk_json(item, ctx, page_mime, container, cover, stream)
        return
    if not isinstance(node, dict):
        return

    href = node.get('href')
    if isinstance(href, str):
        kind = _json_kind(node, container)
        node['href'] = map_href(href, kind, ctx, search_style='opds2', cover=cover)
        if kind in ('p', 'pf') and page_mime:
            node['type'] = page_mime
        if kind in ('p', 's'):
            node['templated'] = True
        else:
            node.pop('templated', None)
        if kind == 'pf':
            node.pop('width', None)
            node.pop('height', None)

    own_cover = _publication_cover(node)

    for key, value in node.items():
        if key == 'href':
            continue
        if key == 'images':
            sub: str | None = 'images'
        elif key in PAGE_CONTAINERS:
            sub = 'pages'
        elif key in SAME_RESOURCE_KEYS:
            sub = container
        else:
            sub = None
        _walk_json(value, ctx, page_mime, sub, own_cover if key == 'links' else None, stream)


def _book_links(publication: dict) -> list[dict]:
    links = publication.get('links')
    if not isinstance(links, list):
        return []
    return [
        link
        for link in links
        if isinstance(link, dict) and isinstance(link.get('href'), str) and is_acquisition(_rel_string(link.get('rel')))
    ]


def _long_publications(doc, ctx: Ctx) -> list[tuple[list, dict, int]]:
    found = []
    if ctx.parts is None:
        return found
    stack = [doc]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(node)
        elif isinstance(node, dict):
            stack.extend(node.values())
            listed = node.get('publications')
            for publication in listed if isinstance(listed, list) else ():
                books = _book_links(publication) if isinstance(publication, dict) else []
                if not books:
                    continue
                meta = publication.get('metadata')
                count = _page_count(meta.get('numberOfPages')) if isinstance(meta, dict) else 0
                parts = parts_of(ctx, books[0]['href'], count)
                if parts > 1:
                    found.append((listed, publication, parts))
    return found


def _split_publication(listed: list, publication: dict, parts: int) -> None:
    at = next(i for i, item in enumerate(listed) if item is publication)
    clones = []
    for part in range(1, parts + 1):
        clone = copy.deepcopy(publication)
        meta = clone.get('metadata')
        if isinstance(meta, dict) and isinstance(meta.get('title'), str):
            meta['title'] = part_title(meta['title'], part, parts)
        for link in _book_links(clone):
            link['href'] = part_href(link['href'], part, parts)
        clones.append(clone)
    listed[at : at + 1] = clones


def rewrite_json(body: bytes, ctx: Ctx, page_mime: str | None, stream: bool = True) -> bytes:
    try:
        try:
            doc = json.loads(body)
        except ValueError:
            return _unrewritten(body)
        long = _long_publications(doc, ctx)
        _walk_json(doc, ctx, page_mime, stream=stream)
        for listed, publication, parts in long:
            _split_publication(listed, publication, parts)
        return json.dumps(doc, ensure_ascii=False).encode('utf-8')
    except RecursionError as exc:
        raise FeedError('upstream feed is nested too deeply to rewrite') from exc


# --------------------------------------------------------------------------
# Dispatch
# --------------------------------------------------------------------------


def rewrite(body: bytes, ctx: Ctx, page_mime: str | None, stream: bool = True) -> bytes:
    head = _head(body)
    if head in (b'{', b'['):
        return rewrite_json(body, ctx, page_mime, stream)
    if head == b'<':
        return rewrite_atom(body, ctx, page_mime, stream)
    return _unrewritten(body)


def _unrewritten(body: bytes) -> bytes:
    secret = _secret()
    if secret and secret.encode() in _narrow(body):
        raise FeedError('upstream feed could not be parsed, and it holds the upstream key')
    return body


_LEADING = b' \t\r\n\xef\xbb\xbf'
_WIDE = (
    (codecs.BOM_UTF32_LE, 'utf-32-le'),
    (codecs.BOM_UTF32_BE, 'utf-32-be'),
    (codecs.BOM_UTF16_LE, 'utf-16-le'),
    (codecs.BOM_UTF16_BE, 'utf-16-be'),
)
_WIDE_BOMS = tuple(bom for bom, _ in _WIDE)


def _narrow(body: bytes) -> bytes:
    for bom, codec in _WIDE:
        if body.startswith(bom):
            return body[len(bom) :].decode(codec, 'ignore').encode('utf-8')
    return body


def _head(body: bytes) -> bytes:
    if body.startswith(_WIDE_BOMS):
        body = _narrow(body[:256])
    return body.lstrip(_LEADING)[:1]


def is_feed(content_type: str, body: bytes = b'') -> bool:
    ct = (content_type or '').lower()
    if 'xml' in ct or 'opensearchdescription' in ct or any(j in ct for j in JSON_TYPES):
        return True
    return ct.split(';')[0].strip() in GENERIC_TYPES and (is_json(body) or _head(body) == b'<')


_XML_ROOT = re.compile(rb'(?:\s+|<\?.*?\?>|<!--.*?-->|<!DOCTYPE[^>]*>)*<(?:[\w.-]+:)?([\w.-]+)', re.S)
_FEED_ROOTS = frozenset({b'feed', b'entry', b'OpenSearchDescription'})


def is_json(body: bytes) -> bool:
    return _head(body) in (b'{', b'[')


def looks_like_feed(body: bytes) -> bool:
    body = _narrow(body)
    if is_json(body):
        return b'"href"' in body
    root = _XML_ROOT.match(body.lstrip(_LEADING))
    return root is not None and root.group(1) in _FEED_ROOTS
