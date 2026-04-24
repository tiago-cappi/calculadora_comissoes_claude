# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec — Calculadora de Comissões (onedir, janelada).

Build:
    pyinstaller --clean CalcularComissoes.spec

Saída:
    dist/Calcular Comissoes/
        Calcular Comissoes.exe
        _internal/...

Distribuição:
    Zipe a pasta `dist/Calcular Comissoes` e envie para a usuária.
    A usuária extrai em qualquer lugar e dá duplo-clique no .exe.
    A planilha `configuracoes_comissoes.xlsx` e a pasta `dados_entrada/`
    ficam ao lado do .exe (podem ser editadas normalmente).
"""
from PyInstaller.utils.hooks import collect_submodules, collect_data_files

# Coleta recursiva de submódulos para pacotes com imports dinâmicos/condicionais
hiddenimports = []
hiddenimports += collect_submodules("scripts")
hiddenimports += collect_submodules("receita")
hiddenimports += collect_submodules("lean_conductor")
hiddenimports += collect_submodules("customtkinter")

# Módulos top-level importados dinamicamente por rodar_pipeline
hiddenimports += [
    "rodar_pipeline",
    "openpyxl",
    "pandas",
    "pandas.io.formats.style",
]

# customtkinter traz imagens/JSON de tema que precisam ser copiados
datas = []
datas += collect_data_files("customtkinter")

block_cipher = None


a = Analysis(
    ["app_entry.py"],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Descartar libs gigantes/irrelevantes para enxugar a build
        "matplotlib",
        "scipy",
        "PyQt5",
        "PyQt6",
        "PySide2",
        "PySide6",
        "notebook",
        "IPython",
        "jupyter",
        "pytest",
    ],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="Calcular Comissoes",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,           # janelada (sem terminal preto)
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # icon="icone.ico",      # opcional — adicionar arquivo .ico depois se desejar
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name="Calcular Comissoes",
)
