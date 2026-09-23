"""
Device profiles.
To add or modify device profiles - edit profiles.toml.
"""

import os
import tomllib
import importlib.resources
from dataclasses import dataclass, fields, replace

COLOUR_FORMATS = frozenset({'pngc', 'jpegc'})
FORMATS = frozenset({'png4', 'png8', 'jpeg', 'pngc', 'jpegc', 'raw'})
DITHERS = frozenset({'bayer', 'none'})
DESCREENS = frozenset({'none', 'mono', 'always'})
PANELS = frozenset({'mono', 'kaleido'})
DEFRINGES = frozenset({'diagonal', 'none'})
FITS = frozenset({'box', 'none'})
COLOUR_PADS = frozenset({'border', 'mono'})
UPSCALES = frozenset({'panel', 'none'})
UPSCALE_KERNELS = frozenset({'linear', 'nearest', 'cubic', 'mitchell', 'lanczos3'})
UPSCALE_MAX_CEILING = 8.0

_SHIPPED = 'profiles.toml'
_INTERNAL = frozenset({'cover', 'cover-colour'})


@dataclass(frozen=True)
class Profile:
    name: str
    width: int
    height: int

    # png4  - 4 bpp grey PNG, 16 levels. Matches a mono panel exactly.
    # Best for line art, manga, anything with flat blacks.
    # png8  - 8 bpp grey PNG. Bigger, no banding. For photographic scans.
    # jpeg  - grey JPEG. Smallest for continuous tone, but rings around lettering.
    # pngc  - colour PNG, optionally palettised. Kaleido default.
    # jpegc - colour JPEG at 4:2:0, which is the Kaleido chroma pitch.
    # raw   - no image processing, feed rewriting only.
    fmt: str = 'png4'

    jpeg_quality: int = 82
    png_compression: int = 7

    # What the pixels land on, as opposed to how they are encoded.
    #   mono     plain e-ink, no colour filter array
    #   kaleido  a CFA printed over a mono panel
    panel: str = 'mono'

    # --- geometry ---
    autocrop: bool = True
    # Distance from background, 0-255.
    autocrop_threshold: int = 12
    # Refuse to crop away more than this.
    autocrop_max_frac: float = 0.35
    # Read and erase a page number printed in the bottom margin.
    strip_folio: bool = True
    # Rotate landscape spreads to fill a portrait panel.
    rotate_wide: bool = True
    # Pad the finished page out to EXACTLY width x height.
    #   box    every page is panel-sized, centred on its own margin colour
    #   none   ship whatever size the resize produced
    fit: str = 'box'
    # What the colour path pads with. The mono path always decides as mono does.
    #   border  the median colour of the page border
    #   mono    white or black, decided from the sides the pad touches
    colour_pad: str = 'border'
    # Ignore where the archive happens to cut the strip and re-cut.
    reslice: bool = False
    # Enlarge a source too small to fill the panel.
    #   panel  resize up to the panel box, capped by upscale_max
    #   none   ship it small and let the reader deal with it
    upscale: str = 'panel'
    # Refuse to enlarge beyond this.
    upscale_max: float = 2.0
    upscale_kernel: str = 'linear'

    # --- resampling ---
    # Screentone removal ahead of the downscale.
    #   none    never
    #   mono    only when the page renders through the mono path
    #   always  both paths
    descreen: str = 'mono'
    # Depth of each notch.
    descreen_strength: float = 0.995
    # Width of each stop band, in cycles per pixel.
    descreen_width: float = 0.012
    # How many DISTINCT frequencies to notch.
    descreen_peaks: int = 3
    # Ignore this radius around DC.
    descreen_min_freq: float = 0.06
    # A peak must exceed this multiple of the in-band median to count, so a page with no screentone keeps all
    # of its frequencies.
    descreen_peak_ratio: float = 8.0
    # ...and it must correspond to a modulation of at least this many grey levels.
    descreen_min_amplitude: float = 2.0
    # Discard any per-pixel change smaller than this, in grey levels.
    descreen_deadband: float = 0.5
    # Skip the notch above this size.
    descreen_max_megapixels: float = 32.0
    # Resample in linear light
    linear_light: bool = False

    # --- local contrast ---
    # Luma only on colour profiles.
    usm_sigma: float = 0.70
    usm_amount: float = 0.55

    # --- tone ---
    # Levels are applied before gamma.
    black: int = 8
    white: int = 245
    gamma: float = 1.05

    # --- mono output quantisation ---
    # bayer | none
    dither: str = 'bayer'

    # --- colour (Kaleido) ---
    # chroma_ratio is the panel's colour resolution divided by its mono resolution.
    chroma_ratio: float = 0.5
    chroma_blur: float = 0.0
    saturation: float = 1.0
    # Palettise colour PNG
    palette: bool = True
    palette_colours: int = 256
    # The quantiser's speed/quality dial
    png_effort: int = 7
    # Rainbow fringing related settings
    # Approach adapted from Kindle Comic Converter (ISC).
    #   diagonal  attenuate high diagonal frequencies
    #   none      leave them
    defringe: str = 'none'
    defringe_min_freq: float = 0.20
    defringe_angle: float = 135.0
    defringe_tolerance: float = 10.0
    defringe_strength: float = 0.95
    defringe_deadband: float = 0.4

    # Route grey pages through the mono path even on a colour device.
    auto_mono: bool = True
    auto_mono_fmt: str = 'png4'
    mono_chroma_threshold: float = 3.5

    @property
    def is_colour(self) -> bool:
        return self.fmt in COLOUR_FORMATS

    @property
    def mime(self) -> str:
        return 'image/jpeg' if self.fmt in ('jpeg', 'jpegc') else 'image/png'

    @property
    def mono_fmt(self) -> str:
        if self.fmt == 'jpegc':
            return 'jpeg'
        return self.auto_mono_fmt

    @property
    def chroma_sigma(self) -> float:
        if self.chroma_blur > 0:
            return self.chroma_blur
        if self.chroma_ratio <= 0 or self.chroma_ratio >= 1:
            return 0.0
        return 0.6 / self.chroma_ratio

    @property
    def aspect(self) -> float:
        if self.width <= 0:
            raise ValueError(
                f'profile {self.name!r} has width={self.width}, so it has no '
                'aspect ratio; only fmt=raw profiles may be sizeless'
            )
        return self.height / self.width


def validate(p: Profile, where: str = 'profile') -> Profile:
    problems = []
    if p.fmt not in FORMATS:
        problems.append(f'fmt={p.fmt!r} is not one of {", ".join(sorted(FORMATS))}')
    if p.dither not in DITHERS:
        problems.append(f'dither={p.dither!r} is not one of {", ".join(sorted(DITHERS))}')
    if p.descreen not in DESCREENS:
        problems.append(f'descreen={p.descreen!r} is not one of {", ".join(sorted(DESCREENS))}')
    if not 0.0 <= p.descreen_strength <= 1.0:
        problems.append(f'descreen_strength={p.descreen_strength} must be 0..1')
    if not 0.0 < p.descreen_width < 0.5:
        problems.append(f'descreen_width={p.descreen_width} must be >0 and <0.5')
    if not 0 <= p.descreen_peaks <= 64:
        problems.append(f'descreen_peaks={p.descreen_peaks} must be 0..64')
    if not 0.0 < p.descreen_min_freq < 0.5:
        problems.append(f'descreen_min_freq={p.descreen_min_freq} must be >0 and <0.5')
    if p.descreen_peak_ratio < 1.0:
        problems.append(f'descreen_peak_ratio={p.descreen_peak_ratio} must be >= 1')
    if p.descreen_min_amplitude < 0:
        problems.append(f'descreen_min_amplitude={p.descreen_min_amplitude} must be >= 0')
    if p.descreen_deadband < 0:
        problems.append(f'descreen_deadband={p.descreen_deadband} must be >= 0')
    if p.panel not in PANELS:
        problems.append(f'panel={p.panel!r} is not one of {", ".join(sorted(PANELS))}')
    if p.defringe not in DEFRINGES:
        problems.append(f'defringe={p.defringe!r} is not one of {", ".join(sorted(DEFRINGES))}')
    if not 0.0 < p.defringe_min_freq < 0.5:
        problems.append(f'defringe_min_freq={p.defringe_min_freq} must be >0 and <0.5')
    if not 0.0 <= p.defringe_angle < 360.0:
        problems.append(f'defringe_angle={p.defringe_angle} must be 0..360')
    if not 0.0 < p.defringe_tolerance <= 45.0:
        problems.append(f'defringe_tolerance={p.defringe_tolerance} must be >0 and <=45')
    if not 0.0 <= p.defringe_strength <= 1.0:
        problems.append(f'defringe_strength={p.defringe_strength} must be 0..1')
    if p.defringe_deadband < 0:
        problems.append(f'defringe_deadband={p.defringe_deadband} must be >= 0')
    if p.fit not in FITS:
        problems.append(f'fit={p.fit!r} is not one of {", ".join(sorted(FITS))}')
    if p.colour_pad not in COLOUR_PADS:
        problems.append(f'colour_pad={p.colour_pad!r} is not one of {", ".join(sorted(COLOUR_PADS))}')
    if p.upscale not in UPSCALES:
        problems.append(f'upscale={p.upscale!r} is not one of {", ".join(sorted(UPSCALES))}')
    if p.upscale_kernel not in UPSCALE_KERNELS:
        problems.append(f'upscale_kernel={p.upscale_kernel!r} is not one of {", ".join(sorted(UPSCALE_KERNELS))}')
    if not 1.0 <= p.upscale_max <= UPSCALE_MAX_CEILING:
        problems.append(f'upscale_max={p.upscale_max} must be 1.0..{UPSCALE_MAX_CEILING}')
    if p.defringe != 'none' and p.panel != 'kaleido':
        problems.append(
            f'defringe={p.defringe!r} with panel={p.panel!r}: only a kaleido panel has a filter array to beat with'
        )
    if not 0 < p.descreen_max_megapixels <= 64:
        problems.append(f'descreen_max_megapixels={p.descreen_max_megapixels} must be >0 and <=64')
    if p.usm_sigma <= 0:
        problems.append(f'usm_sigma={p.usm_sigma} must be > 0; set usm_amount=0 to disable sharpening')
    if p.usm_amount < 0:
        problems.append(f'usm_amount={p.usm_amount} must be >= 0; a negative amount blurs')
    if not 0 <= p.autocrop_threshold < 255:
        problems.append(f'autocrop_threshold={p.autocrop_threshold} must be 0..254')
    if not 0.0 <= p.autocrop_max_frac <= 1.0:
        problems.append(f'autocrop_max_frac={p.autocrop_max_frac} must be 0..1')
    if p.mono_chroma_threshold < 0:
        problems.append(f'mono_chroma_threshold={p.mono_chroma_threshold} must be >= 0')
    if p.auto_mono_fmt not in ('png4', 'png8'):
        problems.append(f'auto_mono_fmt={p.auto_mono_fmt!r} must be png4 or png8')
    if p.fmt != 'raw':
        if p.width <= 0 or p.height <= 0:
            problems.append(f'width={p.width} height={p.height} must both be > 0 unless fmt=raw')
        if p.gamma <= 0:
            problems.append(f'gamma={p.gamma} must be > 0')
    if not 0 <= p.black < p.white <= 255:
        problems.append(f'black={p.black} white={p.white} must satisfy 0 <= black < white <= 255')
    if not 1 <= p.jpeg_quality <= 100:
        problems.append(f'jpeg_quality={p.jpeg_quality} must be 1-100')
    if not 1 <= p.png_effort <= 10:
        problems.append(f'png_effort={p.png_effort} must be 1-10')
    if not 2 <= p.palette_colours <= 256:
        problems.append(f'palette_colours={p.palette_colours} must be 2-256')
    elif (p.palette_colours - 1).bit_length() not in (1, 2, 4, 8):
        problems.append(
            f'palette_colours={p.palette_colours} needs a {(p.palette_colours - 1).bit_length()}-bit '
            'index, which PNG has no format for; use 2, 3-4, 9-16 or 129-256'
        )
    if not 0 <= p.png_compression <= 9:
        problems.append(f'png_compression={p.png_compression} must be 0-9')
    if p.saturation < 0:
        problems.append(f'saturation={p.saturation} must be >= 0')
    if problems:
        raise ValueError(f'{where} {p.name!r}: ' + '; '.join(problems))
    return p


def cover_for(p: Profile) -> Profile:
    if p.fmt == 'raw':
        return p
    return COVER_COLOUR if p.is_colour else COVER


def embedded_cover_for(p: Profile) -> Profile:
    return replace(p, upscale_max=UPSCALE_MAX_CEILING, strip_folio=False)


def _section(data: dict, key: str, where: str) -> dict[str, dict]:
    section = data.get(key)
    if section is None:
        return {}
    if not isinstance(section, dict):
        raise ValueError(  # noqa: TRY004
            f'{where}: [{key}] must be a table of tables, not {type(section).__name__}'
        )
    out = {}
    for name, cfg in section.items():
        if not isinstance(cfg, dict):
            raise ValueError(  # noqa: TRY004
                f'{where}: [{key}.{name}] must be a table, not {type(cfg).__name__}'
            )
        out[name] = dict(cfg)
    return out


def _read_tables(text: str, where: str) -> tuple[dict, dict]:
    data = tomllib.loads(text)
    extra = sorted(set(data) - {'profiles', 'mixins'})
    if extra:
        raise ValueError(f'{where}: unknown section(s) {", ".join(extra)}. Expected [profiles.*] or [mixins.*]')
    mixins = _section(data, 'mixins', where)
    profs = _section(data, 'profiles', where)
    both = sorted(set(mixins) & set(profs))
    if both:
        raise ValueError(f'{where}: {", ".join(both)} defined as both a mixin and a profile')
    return mixins, profs


def _flatten(name: str, profs: dict, mixins: dict, where: str, chain: tuple[str, ...] = ()) -> dict:
    if name in chain:
        raise ValueError(f'{where}: profile {chain[0]!r} has a base cycle: {" -> ".join((*chain, name))}')
    cfg = dict(profs.get(name) or mixins.get(name) or {})
    base = cfg.pop('base', None)
    if base is None:
        return cfg
    if base not in profs and base not in mixins:
        known = ', '.join(sorted(set(profs) | set(mixins)))
        raise ValueError(f'{where}: profile {name!r} has unknown base {base!r}. Known: {known}')
    return {**_flatten(base, profs, mixins, where, (*chain, name)), **cfg}


def _build(text: str, where: str) -> dict[str, Profile]:
    mixins, profs = _read_tables(text, where)
    known = {f.name for f in fields(Profile)} - {'name'}
    out: dict[str, Profile] = {}
    for name in profs:
        cfg = _flatten(name, profs, mixins, where)
        unknown = sorted(set(cfg) - known)
        if unknown:
            raise ValueError(
                f'{where}: profile {name!r} has unknown field(s) {", ".join(unknown)}. '
                f'Known: {", ".join(sorted(known))}'
            )
        missing = sorted({'width', 'height'} - set(cfg))
        if missing:
            raise ValueError(f'{where}: profile {name!r} is missing {", ".join(missing)}')
        out[name] = validate(Profile(name=name, **cfg), f'{where}: profile')
    return out


def _folio_enabled() -> bool:
    """
    Whether OCR_ENABLED permits the page-number pass at all.
    """
    return os.environ.get('OCR_ENABLED', '').strip().lower() not in ('false', '0', 'no', 'off')


def _load() -> dict[str, Profile]:
    where = f'{_SHIPPED} (shipped)'
    text = importlib.resources.files(__package__).joinpath(_SHIPPED).read_text(encoding='utf-8')
    out = _build(text, where)
    absent = sorted(_INTERNAL - set(out))
    if absent:
        raise ValueError(f'{where}: reserved profile(s) {", ".join(absent)} missing; cover_for() needs them')
    if not _folio_enabled():
        out = {k: replace(v, strip_folio=False) for k, v in out.items()}
    return out


_ALL = _load()

COVER = _ALL['cover']
COVER_COLOUR = _ALL['cover-colour']
PROFILES: dict[str, Profile] = {k: v for k, v in _ALL.items() if k not in _INTERNAL}


def get(name: str) -> Profile | None:
    return PROFILES.get(name)
