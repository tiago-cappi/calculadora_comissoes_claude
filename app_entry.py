"""
app_entry.py — Ponto de entrada único para o executável empacotado.

Estratégia: o mesmo .exe serve para GUI e pipeline. A dispatcha acontece por argv:

    Calcular Comissões.exe                          → abre a GUI (launcher)
    Calcular Comissões.exe --run-pipeline --mes 10  → roda o pipeline
    Calcular Comissões.exe --gerar-template         → gera o template Excel

A GUI, quando congelada (sys.frozen), chama `sys.executable --run-pipeline ...`
em subprocesso — exatamente o mesmo exe que a usuária clicou.

Rodar como script (sem PyInstaller) também funciona:
    python app_entry.py                 → GUI
    python app_entry.py --run-pipeline  → pipeline
"""
from __future__ import annotations

import io
import os
import sys

# Garante UTF-8 no stdout/stderr do subprocesso. Em exes congelados no Windows,
# o codec padrão é CP1252, que não suporta os emoji usados pelo pipeline (✖ ✓ ⚠).
# Fazemos isso aqui, antes de qualquer import, para cobrir todo o pipeline.
if sys.stdout and hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
    )
if sys.stderr and hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(
        sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True
    )
from pathlib import Path


def _resolver_root() -> Path:
    """Diretório da aplicação (onde o .exe/.py está — NÃO o bundle temporário)."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _main() -> int:
    # Garante que o CWD seja a pasta da aplicação. Isso é crucial porque
    # rodar_pipeline.py usa caminhos relativos (./dados_entrada, ./saida) e
    # excel_config_loader.py procura configuracoes_comissoes.xlsx na raiz.
    root = _resolver_root()
    try:
        os.chdir(str(root))
    except OSError:
        pass

    argv = sys.argv[:]

    # ── Dispatcher ───────────────────────────────────────────────────────
    if len(argv) > 1 and argv[1] == "--run-pipeline":
        # Remove o sentinel; argparse do rodar_pipeline vê --mes/--ano/etc.
        sys.argv = [argv[0]] + argv[2:]
        # Importar executa o pipeline (rodar_pipeline.py roda no topo do módulo).
        import rodar_pipeline  # noqa: F401
        return 0

    if len(argv) > 1 and argv[1] == "--gerar-template":
        sys.argv = [argv[0]] + argv[2:]
        from scripts import gerar_template_excel  # noqa: F401
        # gerar_template_excel.py também pode rodar no topo; se não, chamar main
        if hasattr(gerar_template_excel, "main"):
            gerar_template_excel.main()
        return 0

    # ── Default: GUI ─────────────────────────────────────────────────────
    import launcher_app
    launcher_app.main()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(_main())
    except SystemExit:
        raise
    except BaseException:
        # Loga crashes de inicialização para facilitar suporte.
        import traceback
        log_path = _resolver_root() / "_startup_error.log"
        try:
            log_path.write_text(traceback.format_exc(), encoding="utf-8")
        except OSError:
            pass
        raise
