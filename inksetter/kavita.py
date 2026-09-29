"""
Kavita specific features.
ComicInfo.xml generator and cover parser.
"""

import re
import time
import asyncio
from urllib.parse import urlsplit
from xml.sax.saxutils import escape

from .settings import settings
from .opds.upstream import client

_UNSET = '-100000'
_UNSET_DATE = '0001-01-01T00:00:00'
_IDS = re.compile(r'/series/(\d+)(?:/volume/(\d+))?(?:/chapter/(\d+))?', re.I)
_OPDS_KEY = re.compile(r'/api/opds/([^/?#]+)', re.I)
_AGE = {
    1: 'Rating Pending',
    2: 'Early Childhood',
    3: 'Everyone',
    4: 'G',
    5: 'Everyone 10+',
    6: 'PG',
    7: 'Kids to Adults',
    8: 'Teen',
    9: 'MA15+',
    10: 'Mature 17+',
    11: 'M',
    12: 'R18+',
    13: 'Adults Only 18+',
    14: 'X18+',
}
_MANGA_LIBRARY = 0
_ORDER = (
    'Title',
    'Series',
    'Number',
    'Count',
    'Volume',
    'AlternateSeries',
    'AlternateNumber',
    'AlternateCount',
    'Summary',
    'Notes',
    'Year',
    'Month',
    'Day',
    'Writer',
    'Penciller',
    'Inker',
    'Colorist',
    'Letterer',
    'CoverArtist',
    'Editor',
    'Publisher',
    'Imprint',
    'Genre',
    'Web',
    'PageCount',
    'LanguageISO',
    'Format',
    'BlackAndWhite',
    'Manga',
    'Characters',
    'Teams',
    'Locations',
    'ScanInformation',
    'StoryArc',
    'SeriesGroup',
    'AgeRating',
)


def api_key() -> str:
    m = _OPDS_KEY.search(settings.upstream_catalog or '')
    return m.group(1) if m else ''


def series_id(url: str) -> int | None:
    m = _IDS.search(urlsplit(url).path)
    return int(m.group(1)) if m else None


def volume_id(url: str) -> int | None:
    m = _IDS.search(urlsplit(url).path)
    return int(m.group(2)) if m and m.group(2) else None


def _positive(value):
    return value if isinstance(value, int) and value > 0 else None


def _clean(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return None if text in ('', _UNSET, _UNSET_DATE) else text


def _names(items, key: str = 'name') -> str | None:
    out = [i[key] for i in (items or []) if isinstance(i, dict) and i.get(key)]
    return ', '.join(out) or None


_NEGATIVE_TTL = 300.0
_POSITIVE_TTL = 900.0
_SPECIAL_VOLUME = 100000
_MISS = object()


class _Reauth(Exception):
    pass


class _Kavita:
    def __init__(self) -> None:
        self._token: str | None = None
        self._token_at = 0.0
        self._series: dict[int, tuple[dict | None, float | None]] = {}
        self._locks: dict[int, asyncio.Lock] = {}
        self._auth_lock = asyncio.Lock()

    def _lock(self, sid: int) -> asyncio.Lock:
        lock = self._locks.get(sid)
        if lock is None:
            if len(self._locks) > 512:
                self._locks = {k: v for k, v in self._locks.items() if v.locked()}
            lock = self._locks[sid] = asyncio.Lock()
        return lock

    def base(self) -> str:
        catalog = settings.upstream_catalog or ''
        cut = _OPDS_KEY.search(catalog)
        return catalog[: cut.start()] if cut and urlsplit(catalog).netloc else ''

    async def _auth(self, *, stale: str | None = None) -> str | None:
        async with self._auth_lock:
            if self._token and self._token != stale and time.monotonic() - self._token_at < 3600:
                return self._token
            self._token = None
            resp = await client.raw.post(
                f'{self.base()}/api/Plugin/authenticate',
                params={'apiKey': api_key(), 'pluginName': 'inksetter'},
            )
            if resp.status_code != 200:
                return None
            self._token = resp.json().get('token')
            self._token_at = time.monotonic()
            return self._token

    async def _fetch(self, sid: int, token: str) -> dict | None:
        head = {'Authorization': f'Bearer {token}'}
        meta, ser = await asyncio.gather(
            client.raw.get(f'{self.base()}/api/Series/metadata', params={'seriesId': sid}, headers=head),
            client.raw.get(f'{self.base()}/api/Series/{sid}', headers=head),
        )
        if 401 in (meta.status_code, ser.status_code):
            raise _Reauth
        if meta.status_code != 200 or ser.status_code != 200:
            return None
        series = ser.json()
        fetches = [client.raw.get(f'{self.base()}/api/Series/volumes', params={'seriesId': sid}, headers=head)]
        if isinstance(series.get('libraryId'), int):
            library = {'libraryId': series['libraryId']}
            fetches.append(client.raw.get(f'{self.base()}/api/Library/type', params=library, headers=head))
        vols, *lib = await asyncio.gather(*fetches)
        manga = bool(lib) and lib[0].status_code == 200 and _json_or_none(lib[0]) == _MANGA_LIBRARY
        numbers, covers = {}, {}
        if vols.status_code == 200:
            for v in vols.json():
                if v.get('id') is None:
                    continue
                vid = int(v['id'])
                n = v.get('minNumber')
                if n not in (None, 0, _SPECIAL_VOLUME) and str(n) != _UNSET:
                    numbers[vid] = n
                if _clean(v.get('coverImage')):
                    covers[vid] = v['coverImage']
        return {'meta': meta.json(), 'series': series, 'manga': manga, 'numbers': numbers, 'covers': covers}

    def _cached(self, sid: int):
        hit = self._series.get(sid)
        if hit is None:
            return _MISS
        data, until = hit
        if until is not None and time.monotonic() >= until:
            return _MISS
        return data

    def _remember(self, sid: int, data: dict | None) -> None:
        if len(self._series) > 512:
            self._series.clear()
        self._series[sid] = (data, time.monotonic() + (_NEGATIVE_TTL if data is None else _POSITIVE_TTL))

    async def series_metadata(self, sid: int) -> dict | None:
        hit = self._cached(sid)
        if hit is not _MISS:
            return hit
        async with self._lock(sid):
            hit = self._cached(sid)
            if hit is not _MISS:
                return hit
            out = None
            try:
                token = await self._auth()
                if token:
                    try:
                        out = await self._fetch(sid, token)
                    except _Reauth:
                        token = await self._auth(stale=token)
                        out = await self._fetch(sid, token) if token else None
            except Exception:
                out = None
            self._remember(sid, out)
            return out


def _json_or_none(resp):
    try:
        return resp.json()
    except ValueError:
        return None


_kavita = _Kavita()


def _fields(data: dict, pages: int) -> dict[str, str]:
    meta, ser = data['meta'], data['series']
    out: dict[str, str] = {}

    def put(tag: str, value) -> None:
        cleaned = _clean(value)
        if cleaned:
            out[tag] = cleaned

    put('Series', ser.get('name'))
    put('Count', _positive(meta.get('totalCount')))
    put('Summary', meta.get('summary'))
    put('Notes', 'Generated by Inksetter from Kavita; the source archive had no ComicInfo.xml.')
    put('Year', _positive(meta.get('releaseYear')))
    put('Writer', _names(meta.get('writers')))
    put('Penciller', _names(meta.get('pencillers')))
    put('Inker', _names(meta.get('inkers')))
    put('Colorist', _names(meta.get('colorists')))
    put('Letterer', _names(meta.get('letterers')))
    put('CoverArtist', _names(meta.get('coverArtists')))
    put('Editor', _names(meta.get('editors')))
    put('Publisher', _names(meta.get('publishers')))
    put('Imprint', _names(meta.get('imprints')))
    put('Genre', _names(meta.get('genres'), 'title'))
    put('PageCount', pages)
    put('LanguageISO', meta.get('language'))
    put('Characters', _names(meta.get('characters')))
    put('Teams', _names(meta.get('teams')))
    put('Locations', _names(meta.get('locations')))
    put('AgeRating', _AGE.get(meta.get('ageRating')))
    if data.get('manga'):
        put('Manga', 'Yes')
    if ser.get('aniListId'):
        put('Web', f'https://anilist.co/manga/{ser["aniListId"]}')
    localized = _clean(ser.get('localizedName'))
    if localized and localized != _clean(ser.get('name')):
        put('AlternateSeries', localized)
    return out


def render(data: dict, number: str | None, sizes: list[tuple[int, int] | None]) -> bytes:
    fields = _fields(data, len(sizes))
    if number:
        fields['Number'] = number

    lines = [
        '<?xml version="1.0" encoding="utf-8"?>',
        '<ComicInfo xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
        ' xmlns:xsd="http://www.w3.org/2001/XMLSchema">',
    ]
    for tag in _ORDER:
        if tag in fields:
            lines.append(f'  <{tag}>{escape(fields[tag])}</{tag}>')
    lines.append('  <Pages>')
    for i, size in enumerate(sizes):
        kind = ' Type="FrontCover"' if i == 0 else ''
        dims = f' ImageWidth="{size[0]}" ImageHeight="{size[1]}"' if size else ''
        lines.append(f'    <Page Image="{i}"{kind}{dims} />')
    lines.append('  </Pages>')
    lines.append('</ComicInfo>')
    return ('\n'.join(lines) + '\n').encode('utf-8')


async def cover_url(url: str) -> str | None:
    if not api_key():
        return None
    sid, vid = series_id(url), volume_id(url)
    if sid is None or vid is None:
        return None
    data = await _kavita.series_metadata(sid)
    if not data or vid not in data.get('covers', {}):
        return None
    base = _kavita.base()
    return f'{base}/api/image/volume-cover?volumeId={vid}&apiKey={api_key()}' if base else None


async def for_download(url: str):
    if not api_key():
        return None
    sid = series_id(url)
    if sid is None:
        return None
    data = await _kavita.series_metadata(sid)
    if not data:
        return None
    number = _clean(data.get('numbers', {}).get(volume_id(url)))

    def build(sizes: list[tuple[int, int] | None]) -> bytes:
        return render(data, number, sizes)

    return build
