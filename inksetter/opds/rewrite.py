"""
Feed rewriting for OPDS 1.x (Atom), OPDS 2.0 (JSON) and Readium manifests.
"""

import re
import json
import base64
import binascii
from lxml import etree
from dataclasses import dataclass
from urllib.parse import quote, urljoin, urlsplit


ATOM = 'http://www.w3.org/2005/Atom'
OSD_NS = 'http://a9.com/-/spec/opensearch/1.1/'

PSE_REL = 'http://vaemendis.net/opds-pse/stream'
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
SAME_RESOURCE_KEYS = ('alternate',)
BARE_SEARCH_PLACEHOLDERS = ('{searchTerms}', '{query}', '{search}')
FORM_STYLE = re.compile(r'\{[?&]([^}]+)\}')


class TokenError(ValueError):
    pass


def encode_token(url: str, cover: str | None = None) -> str:
    raw = f'{url}\n{cover}' if cover else url
    return base64.urlsafe_b64encode(raw.encode('utf-8')).decode('ascii').rstrip('=')


def decode_parts(token: str) -> tuple[str, str | None]:
    pad = '=' * (-len(token) % 4)
    try:
        raw = base64.urlsafe_b64decode(token + pad).decode('utf-8')
    except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
        raise TokenError(f'malformed token: {token[:24]}') from exc
    url, _, cover = raw.partition('\n')
    return url, cover or None


def decode_token(token: str) -> str:
    return decode_parts(token)[0]


def is_search_template(href: str | None) -> bool:
    h = href or ''
    return any(m in h for m in BARE_SEARCH_PLACEHOLDERS) or bool(FORM_STYLE.search(h))


def fill_search(template: str, term: str) -> str:
    value = quote(term, safe='')

    def expand(match: re.Match[str]) -> str:
        names = [n.strip() for n in match.group(1).split(',') if n.strip()]
        if not names:
            return ''
        lead = '?' if match.group(0)[1] == '?' else '&'
        return f'{lead}{names[0]}={value}'

    out = FORM_STYLE.sub(expand, template)
    for placeholder in BARE_SEARCH_PLACEHOLDERS:
        out = out.replace(placeholder, value)
    return out


@dataclass(frozen=True)
class Ctx:
    profile: str
    public_base: str
    base_url: str


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
) -> str:
    if not href:
        return href
    scheme = urlsplit(href).scheme.lower()
    if scheme and scheme not in ('http', 'https'):
        return href

    token = encode_token(
        urljoin(ctx.base_url, href),
        urljoin(ctx.base_url, cover) if cover and kind == 'dl' else None,
    )
    prefix = f'{ctx.public_base}/{quote(ctx.profile)}'

    if kind == 'p':
        return f'{prefix}/p/{token}?page={{pageNumber}}&maxWidth={{maxWidth}}'
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
    link.set('href', map_href(href, kind, ctx, cover=cover))
    if kind == 'p' and page_mime:
        link.set('type', page_mime)


def rewrite_atom(body: bytes, ctx: Ctx, page_mime: str | None, stream: bool = True) -> bytes:
    root = root_or_none(body)
    if root is None:
        return body

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
    for entry in root.iter(f'{{{ATOM}}}entry'):
        cover = entry_cover(entry)
        for link in entry.iter(f'{{{ATOM}}}link'):
            _rewrite_link(link, ctx, page_mime, cover)
            done.add(link)
    for link in root.iter(f'{{{ATOM}}}link'):
        if link not in done:
            _rewrite_link(link, ctx, page_mime, None)

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


def rewrite_opensearch(body: bytes, ctx: Ctx) -> bytes:
    root = root_or_none(body)
    if root is None:
        return body
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
    type_ = node.get('type')
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


def rewrite_json(body: bytes, ctx: Ctx, page_mime: str | None, stream: bool = True) -> bytes:
    try:
        doc = json.loads(body)
    except ValueError:
        return body
    _walk_json(doc, ctx, page_mime, stream=stream)
    return json.dumps(doc, ensure_ascii=False).encode('utf-8')


# --------------------------------------------------------------------------
# Dispatch
# --------------------------------------------------------------------------


def rewrite(body: bytes, content_type: str, ctx: Ctx, page_mime: str | None, stream: bool = True) -> bytes:
    ct = (content_type or '').lower()
    if 'opensearchdescription' in ct:
        return rewrite_opensearch(body, ctx)
    if any(j in ct for j in JSON_TYPES):
        return rewrite_json(body, ctx, page_mime, stream)
    if 'xml' in ct:
        return rewrite_atom(body, ctx, page_mime, stream)
    head = body.lstrip()[:1]
    if head == b'{':
        return rewrite_json(body, ctx, page_mime, stream)
    if head == b'<':
        return rewrite_atom(body, ctx, page_mime, stream)
    return body


def is_feed(content_type: str) -> bool:
    ct = (content_type or '').lower()
    return 'xml' in ct or 'opensearchdescription' in ct or any(j in ct for j in JSON_TYPES)
