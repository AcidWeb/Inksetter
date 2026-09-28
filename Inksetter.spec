# uv sync --no-dev --group build
# uv run --no-sync pyinstaller --noconfirm Inksetter.spec

import sys
from pathlib import Path
from PyInstaller.utils.hooks import collect_data_files
from PyInstaller.utils.win32 import versioninfo as vi

REPO = Path(SPECPATH)
sys.path.insert(0, str(REPO))

from inksetter import __version__ as version

tuple_version = tuple(int(i) for i in version.split('.')) + (0,)
raw_version = vi.VSVersionInfo(
    ffi=vi.FixedFileInfo(
        filevers=tuple_version,
        prodvers=tuple_version,
        mask=0x3f,
        flags=0x0,
        OS=0x40004,
        fileType=0x1,
        subtype=0x0,
        date=(0, 0)
    ),
    kids=[
        vi.StringFileInfo(
            [
                vi.StringTable(
                    '000004B0',
                    [vi.StringStruct('FileVersion', f'{version}.0'),
                        vi.StringStruct('ProductVersion', f'{version}.0'),
                        vi.StringStruct('OriginalFilename', 'Inksetter.exe'),
                        vi.StringStruct('InternalName', 'Inksetter.exe'),
                        vi.StringStruct('FileDescription', 'Inksetter'),
                        vi.StringStruct('CompanyName', ' '),
                        vi.StringStruct('LegalCopyright', 'Copyright (C) 2026 Paweł Jastrzębski'),
                        vi.StringStruct('ProductName', 'Inksetter')])
            ]),
        vi.VarFileInfo([vi.VarStruct('Translation', [0, 1200])])
    ]
)

a = Analysis(
    [str(REPO / 'inksetter' / '__main__.py')],
    pathex=[str(REPO)],
    datas=collect_data_files('inksetter') + collect_data_files('rapidocr_onnxruntime'),
    excludes=['FixTk', 'tcl', 'tk', '_tkinter', 'tkinter', 'Tkinter', 'lxml.isoschematron'],
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz,
    a.scripts,
    [('X utf8=1', None, 'OPTION')],
    exclude_binaries=True,
    name='Inksetter',
    icon=str(REPO / 'inksetter' / 'logo.png'),
    console=True,
    upx=False,
    version=raw_version
)
coll = COLLECT(exe, a.binaries, a.datas, name='Inksetter', upx=False)
