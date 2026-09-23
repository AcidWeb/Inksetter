"""
Basic web OPDS client.
"""

import json
import base64
from html import escape
from dataclasses import dataclass
from urllib.parse import quote

from .opds import rewrite
from .opds.rewrite import ATOM, Ctx
from .imaging.profiles import Profile

BROWSE_KIND = {'f': 'b', 's': 'bs', 'osd': 'bs'}

PSE_NS = 'http://vaemendis.net/opds-pse/ns'
SUMMARY_MAX = 140
LOGO = 'logo.png'
LOGO_PX = 50
SOURCE_URL = 'https://github.com/AcidWeb/Inksetter'
FOOTER = (
    f'<footer>Inksetter is free software under the '
    f'<a href="{SOURCE_URL}/blob/master/LICENSE">GNU AGPL v3</a>: '
    f'<a href="{SOURCE_URL}">source code</a>.'
)
TRAIL_PARAM = 't'
TRAIL_MAX = 8
TRAIL_TITLE_MAX = 60

CSS = """
:root{--bg:#f2f1ee;--panel:#fffefb;--ink:#14130f;--dim:#6a675e;--line:#d6d3ca;
--line2:#e7e4dc;--btn:#14130f;--btnink:#fffefb;--cap:#8a8679;--tint:#e8e5dc}
@media (prefers-color-scheme:dark){:root{--bg:#15161a;--panel:#1d1f24;--ink:#eceae4;
--dim:#9b988f;--line:#33363d;--line2:#282b31;--btn:#eceae4;--btnink:#15161a;
--cap:#7d7a72;--tint:#252830}}
*{box-sizing:border-box}
body{margin:0;padding:26px 16px 48px;background:var(--bg);color:var(--ink);
font:15px/1.5 ui-sans-serif,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
.wrap{max-width:1000px;margin:0 auto;background:var(--panel);border:1px solid var(--line);
border-radius:10px;padding:22px 24px 26px}
header{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;margin:0 0 16px}
h1{font-size:19px;margin:0;letter-spacing:-.01em}
.chip{font:11px/1.7 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
border:1px solid var(--line);border-radius:999px;padding:1px 9px;color:var(--dim)}
form.q{display:flex;gap:8px;margin:0 0 16px}
form.q input{flex:1;padding:7px 11px;border:1px solid var(--line);border-radius:6px;
background:var(--bg);color:var(--ink);font-size:14px}
form.q button,a.dl{border:1px solid var(--btn);background:var(--btn);color:var(--btnink);
border-radius:6px;padding:7px 14px;font:500 13px/1.3 inherit;cursor:pointer;
text-decoration:none;display:inline-block;white-space:nowrap}
.crumbs{font-size:13px;color:var(--dim);margin:0 0 14px}
.crumbs a{color:var(--dim)}
.crumbs b{color:var(--ink);font-weight:500}
a{color:var(--ink)}
ul{list-style:none;margin:0;padding:0;border-top:1px solid var(--line2)}
li{display:flex;align-items:center;gap:14px;padding:11px 2px;border-bottom:1px solid var(--line2)}
li .m{flex:1;min-width:0}
li .t{font-weight:500;font-size:14px;display:block}
li .s{font-size:12px;color:var(--dim);margin-top:2px}
li a.t{text-decoration:none}
li a.t:hover{text-decoration:underline}
/* the .grid rule is appended below, computed from GRID_* */
.grid .t{font-size:13px;line-height:1.35;margin:8px 0 1px;font-weight:500;
display:block;text-decoration:none}
.grid .t:hover{text-decoration:underline}
.grid .s{font-size:11.5px;color:var(--dim)}
/* 3/4 is the cover profile's own box, so nothing is cropped or letterboxed. */
.cover{aspect-ratio:3/4;width:100%;object-fit:contain;background:var(--tint);
border:1px solid var(--line);border-radius:3px;display:block}
li .cover{width:44px;flex:none;border-radius:2px}
.pager{display:flex;justify-content:space-between;margin:18px 0 0;font-size:13px}
.pager span{color:var(--cap)}
.empty{color:var(--dim);font-size:14px;padding:18px 0}
.working{font:11px/1.6 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
color:var(--dim);border:1px dashed var(--line);border-radius:6px;padding:5px 10px;
white-space:nowrap;flex:none}
li code{font:11px/1.6 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
color:var(--dim);word-break:break-all;text-align:right}
h2.group{display:flex;justify-content:space-between;align-items:baseline;gap:12px;
font:600 11px/1 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;letter-spacing:.09em;
text-transform:uppercase;color:var(--cap);margin:30px 0 0}
h2.group span{font-weight:400;letter-spacing:0}
h2.group:first-of-type{margin-top:4px}
p.blurb{font-size:12.5px;color:var(--dim);margin:6px 0 10px;max-width:62ch}
header.hero{align-items:center;justify-content:center;gap:16px;margin:0 0 18px}
header.hero h1{font-size:30px;letter-spacing:-.02em}
img.mark{flex:none;display:block}
p.lede{font-size:15px;color:var(--dim);margin:0 0 20px}
footer{max-width:1000px;margin:14px auto 0;font-size:12px;color:var(--cap);text-align:center}
footer a{color:var(--dim)}
"""

GRID_COLUMNS = 5
GRID_GAP = 16
GRID_MIN = 132

CSS += (
    f'.grid{{display:grid;gap:18px {GRID_GAP}px;grid-template-columns:repeat(auto-fill,'
    f'minmax(max({GRID_MIN}px,calc((100% - {(GRID_COLUMNS - 1) * GRID_GAP}px)/{GRID_COLUMNS})),1fr))}}\n'
)


POLL_MS = 700
POLL_GIVE_UP = 40
STAGE_LABELS = {
    'starting': 'Starting',
    'fetching': 'Fetching',
    'repacking': 'Repacking',
    'done': 'Ready',
    'error': 'Failed',
    'lost': 'Lost track',
}

SCRIPT = (
    """
(function(){
var root=document.querySelector('[data-status]');
if(!root||!window.fetch)return;
var api=root.getAttribute('data-status'),say=__LABELS__;
root.querySelectorAll('a.dl').forEach(function(link){
link.addEventListener('click',function(ev){
if(!window.Promise)return;
ev.preventDefault();
var id=(window.crypto&&crypto.randomUUID)?crypto.randomUUID()
:String(Date.now())+'-'+String(Math.random()).slice(2,8);
var href=link.href+(link.href.indexOf('?')<0?'?':'&')+'job='+id;
var note=document.createElement('span');
note.className='working';
note.setAttribute('role','status');
note.textContent=say.starting;
link.replaceWith(note);
var frame=document.createElement('iframe');
frame.hidden=true;
frame.src=href;
document.body.appendChild(frame);
var missed=0,timer=setInterval(poll,__POLL_MS__);
function stop(word){clearInterval(timer);note.textContent=word;
setTimeout(function(){frame.remove();},3000);}
function poll(){
fetch(api+id,{headers:{accept:'application/json'}}).then(function(r){
return r.ok?r.json():null;}).then(function(s){
if(!s){if(++missed>__GIVE_UP__)stop(say.lost);return;}
missed=0;
if(s.stage==='done')stop(say.done);
else if(s.stage==='error')stop(say.error);
else if(s.total)note.textContent=say.repacking+' '+s.done+' / '+s.total+' pages';
else note.textContent=say[s.stage]||say.starting;
}).catch(function(){if(++missed>__GIVE_UP__)stop(say.lost);});}
});
});
})();
""".replace('__POLL_MS__', str(POLL_MS))
    .replace('__GIVE_UP__', str(POLL_GIVE_UP))
    .replace(
        '__LABELS__',
        json.dumps(STAGE_LABELS).replace('<', '\\u003c'),
    )
)


@dataclass(frozen=True)
class Item:
    title: str
    href: str
    cover: str | None
    note: str
    download: bool


@dataclass(frozen=True)
class Feed:
    title: str
    items: tuple[Item, ...]
    search: str | None
    up: str | None
    nxt: str | None
    prev: str | None


def encode_trail(pairs) -> str:
    kept = tuple(pairs)[-TRAIL_MAX:]
    if not kept:
        return ''
    raw = '\n'.join(f'{tok}\t{" ".join(title.split())[:TRAIL_TITLE_MAX]}' for tok, title in kept)
    return base64.urlsafe_b64encode(raw.encode('utf-8')).decode('ascii').rstrip('=')


def decode_trail(value: str) -> tuple[tuple[str, str], ...]:
    if not value:
        return ()
    try:
        raw = base64.urlsafe_b64decode(value + '=' * (-len(value) % 4)).decode('utf-8')
    except ValueError:
        return ()
    out = []
    for line in raw.split('\n')[:TRAIL_MAX]:
        token, _, title = line.partition('\t')
        if token and token.replace('-', '').replace('_', '').isalnum():
            out.append((token, title))
    return tuple(out)


def _extend(seen, token: str, title: str):
    if not token:
        return seen
    for i, (tok, _) in enumerate(seen):
        if tok == token:
            return (*seen[:i], (token, title))
    return (*seen, (token, title))


def _q(href: str, trail: str) -> str:
    if not href or not trail:
        return href
    return f'{href}{"&" if "?" in href else "?"}{TRAIL_PARAM}={trail}'


def _map(href: str | None, kind: str, ctx: Ctx, cover: str | None = None) -> str | None:
    if not href:
        return None
    mapped = rewrite.map_href(href, BROWSE_KIND.get(kind, kind), ctx, cover=cover)
    return mapped if mapped and mapped.startswith(f'{ctx.public_base}/') else None


def _text(el, tag: str) -> str:
    found = el.find(f'{{{ATOM}}}{tag}')
    return ' '.join((found.text or '').split()) if found is not None else ''


def _human_bytes(raw: str | None) -> str:
    try:
        n = float(int(raw or ''))
    except ValueError:
        return ''
    if n <= 0:
        return ''
    for unit in ('B', 'KB', 'MB'):
        if n < 1024:
            return f'{n:.0f} {unit}' if unit == 'B' else f'{n:.1f} {unit}'
        n /= 1024
    return f'{n:.1f} GB'


def _note(entry, acquisition) -> str:
    bits = []
    for link in entry.iter(f'{{{ATOM}}}link'):
        count = link.get(f'{{{PSE_NS}}}count')
        if count:
            bits.append(f'{count} pages')
            break
    if acquisition is not None:
        size = _human_bytes(acquisition.get('length'))
        if size:
            bits.append(f'{size} upstream')
    if bits:
        return ' \u00b7 '.join(bits)
    summary = _text(entry, 'summary') or _text(entry, 'content')
    if len(summary) > SUMMARY_MAX:
        summary = summary[: SUMMARY_MAX - 1].rstrip() + '\u2026'
    return summary


def _entry_item(entry, ctx: Ctx) -> Item | None:
    acquisition = navigation = None
    for link in entry.iter(f'{{{ATOM}}}link'):
        href = link.get('href')
        if not href:
            continue
        kind = rewrite.classify(link.get('rel'), link.get('type'), href)
        if kind == 'dl' and acquisition is None:
            acquisition = link
        elif kind == 'f' and navigation is None:
            navigation = link

    link = acquisition if acquisition is not None else navigation
    if link is None:
        return None
    cover = rewrite.entry_cover(entry)
    href = _map(link.get('href'), 'dl' if acquisition is not None else 'f', ctx, cover=cover)
    if href is None:
        return None
    return Item(
        title=_text(entry, 'title') or 'Untitled',
        href=href,
        cover=_map(cover, 'img', ctx),
        note=_note(entry, acquisition),
        download=acquisition is not None,
    )


def parse(body: bytes, ctx: Ctx) -> Feed | None:
    root = rewrite.root_or_none(body)
    if root is None or not str(root.tag).endswith('}feed'):
        return None

    items = []
    inner: set = set()
    for entry in root.iter(f'{{{ATOM}}}entry'):
        inner.update(entry.iter(f'{{{ATOM}}}link'))
        item = _entry_item(entry, ctx)
        if item is not None:
            items.append(item)

    rels: dict[str, str] = {}
    for link in root.iter(f'{{{ATOM}}}link'):
        href = link.get('href')
        if link in inner or not href:
            continue
        mapped = _map(href, rewrite.classify(link.get('rel'), link.get('type'), href), ctx)
        for rel in (link.get('rel') or '').split():
            rels.setdefault(rel, mapped or '')

    return Feed(
        title=_text(root, 'title') or 'Catalog',
        items=tuple(items),
        search=rels.get('search') or None,
        up=rels.get('up') or None,
        nxt=rels.get('next') or None,
        prev=rels.get('previous') or rels.get('prev') or None,
    )


def _specs(p: Profile) -> str:
    size = f'{p.width}\u00d7{p.height}' if p.width and p.height else 'original'
    return f'{size} \u00b7 {p.fmt}'


def _panel(p: Profile) -> str:
    return f'{p.name} \u00b7 {_specs(p)}'


def _cover_tag(item: Item) -> str:
    if not item.cover:
        return '<div class="cover"></div>'
    return f'<img class="cover" loading="lazy" alt="" src="{escape(item.cover)}">'


def _rows(items, trail: str, status: str) -> str:
    out = []
    for item in items:
        note = f'<div class="s">{escape(item.note)}</div>' if item.note else ''
        if item.download:
            title = f'<span class="t">{escape(item.title)}</span>'
            action = f'<a class="dl" href="{escape(item.href)}">Download CBZ</a>'
        else:
            href = escape(_q(item.href, trail))
            title = f'<a class="t" href="{href}">{escape(item.title)}</a>'
            action = ''
        out.append(f'<li>{_cover_tag(item)}<div class="m">{title}{note}</div>{action}</li>')
    return f'<ul data-status="{escape(status)}">{"".join(out)}</ul>'


def _grid(items, trail: str) -> str:
    out = []
    for item in items:
        note = f'<div class="s">{escape(item.note)}</div>' if item.note else ''
        href = escape(_q(item.href, trail))
        out.append(
            f'<div><a href="{href}">{_cover_tag(item)}</a>'
            f'<a class="t" href="{href}">{escape(item.title)}</a>{note}</div>'
        )
    return f'<div class="grid">{"".join(out)}</div>'


def _shell(title: str, chip: str, body: str, base: str = '', hero: bool = False) -> str:
    tab = title if title == 'Inksetter' else f'{title} \u2014 Inksetter'
    src = f'{escape(base)}/{LOGO}'
    icon = f'<link rel="icon" type="image/png" href="{src}">'
    mark = f'<img class="mark" src="{src}" alt="" width="{LOGO_PX}" height="{LOGO_PX}">' if hero else ''
    return (
        '<!doctype html><html lang="en"><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f'<meta name="robots" content="noindex">{icon}'
        f'<title>{escape(tab)}</title><style>{CSS}</style>'
        f'<div class="wrap"><header{" class='hero'" if hero else ""}>{mark}<h1>{escape(title)}</h1>'
        f'{f"<span class=chip>{escape(chip)}</span>" if chip else ""}</header>{body}</div>{FOOTER}</html>'
    )


def page(feed: Feed, p: Profile, base: str, term: str = '', trail: str = '', here: str = '') -> str:
    seen = decode_trail(trail)
    trail = encode_trail(seen)
    child_pairs = _extend(seen, here, feed.title)
    child = encode_trail(child_pairs)
    ancestors = child_pairs[:-1] if here else seen

    parts = []
    if feed.search:
        parts.append(
            f'<form class="q" action="{escape(feed.search)}">'
            f'<input name="q" value="{escape(term)}" placeholder="Search the library">'
            f'<input type="hidden" name="{TRAIL_PARAM}" value="{escape(child)}">'
            '<button>Search</button></form>'
        )

    crumbs = [f'<a href="{escape(base)}">Profiles</a>']
    prefix = f'{base}/{quote(p.name)}/b/'
    for i, (token, title) in enumerate(ancestors):
        href = _q(f'{prefix}{token}', encode_trail(ancestors[:i]))
        crumbs.append(f'<a href="{escape(href)}">{escape(title or "Up")}</a>')
    if feed.up and not ancestors:
        crumbs.append(f'<a href="{escape(_q(feed.up, trail))}">Up</a>')
    crumbs.append(f'<b>{escape(feed.title)}</b>')
    parts.append(f'<nav class="crumbs">{" \u203a ".join(crumbs)}</nav>')

    if not feed.items:
        parts.append('<div class="empty">Nothing here.</div>')
    elif any(i.download for i in feed.items):
        parts.append(_rows(feed.items, child, f'{base}/{quote(p.name)}/dl-status/'))
        parts.append(f'<script>{SCRIPT}</script>')
    else:
        parts.append(_grid(feed.items, child))

    if feed.nxt or feed.prev:
        prev, nxt = _q(feed.prev or '', trail), _q(feed.nxt or '', trail)
        left = f'<a href="{escape(prev)}">\u2039 Previous</a>' if prev else '<span>\u2039 Previous</span>'
        right = f'<a href="{escape(nxt)}">Next \u203a</a>' if nxt else '<span>Next \u203a</span>'
        parts.append(f'<div class="pager">{left}{right}</div>')

    return _shell(feed.title, _panel(p), ''.join(parts), base)


def unsupported(p: Profile, base: str) -> str:
    return _shell(
        'Not browsable',
        _panel(p),
        f'<nav class="crumbs"><a href="{escape(base)}">Profiles</a></nav>'
        '<div class="empty">This upstream answers OPDS 2.0 JSON, which these pages do '
        'not render. Point a reader at the catalog URL instead.</div>',
        base,
    )


ERROR_TITLE = {
    400: 'Bad request',
    401: 'Sign in required',
    403: 'Not allowed',
    404: 'Not found',
    502: 'Upstream problem',
}


def error(status: int, detail: str, base: str) -> str:
    return _shell(
        ERROR_TITLE.get(status, f'Error {status}'),
        '',
        f'<nav class="crumbs"><a href="{escape(base)}">Profiles</a></nav><div class="empty">{escape(detail)}</div>',
        base,
    )


GROUPS = (
    ('Black and white', 'Carta and other grey panels. Sixteen levels of grey, dithered.'),
    ('Colour', 'Kaleido panels. Colour pages, and the filter array is corrected for.'),
    ('Webtoon', 'Long vertical strips, re-cut into pages that fit the screen.'),
    ('No processing', 'The feed is rewritten, the pages are passed through untouched.'),
)


def group_of(p: Profile) -> str:
    if p.fmt == 'raw':
        return 'No processing'
    if p.reslice:
        return 'Webtoon'
    return 'Colour' if p.panel == 'kaleido' else 'Black and white'


def index(base: str, entries) -> str:
    held: dict[str, list] = {title: [] for title, _ in GROUPS}
    for name, p in entries:
        held[group_of(p)].append((name, p))

    parts = ['<p class="lede">Open a profile to browse and download here, or add its catalog URL to a reader.</p>']
    for title, blurb in GROUPS:
        members = held[title]
        if not members:
            continue
        rows = ''.join(
            f'<li><div class="m">'
            f'<a class="t" href="{escape(base)}/{quote(n)}/browse">{escape(n)}</a>'
            f'<div class="s">{escape(_specs(p))}</div></div>'
            f'<code>{escape(base)}/{quote(n)}/catalog</code></li>'
            for n, p in members
        )
        parts.append(
            f'<h2 class="group">{escape(title)}<span>{len(members)}</span></h2>'
            f'<p class="blurb">{escape(blurb)}</p><ul>{rows}</ul>'
        )
    return _shell('Inksetter', '', ''.join(parts), base, hero=True)
