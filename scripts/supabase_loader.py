"""
supabase_loader.py — Shim de compatibilidade

A fonte de verdade das configurações deixou de ser o Supabase e passou a ser
o arquivo `configuracoes_comissoes.xlsx` na raiz do projeto. Este módulo
mantém a API antiga (`load_json`, `clear_cache`, `diagnose`) para preservar
os 20+ call-sites existentes, delegando para `excel_config_loader`.

Para migrar configurações do Supabase para Excel, use uma única vez:
    python scripts/gerar_template_excel.py

Para editar configurações, abra `configuracoes_comissoes.xlsx` no Excel.

Stubs de compatibilidade
------------------------
Módulos legados (supabase_writer, gerenciar_supabase, MCP server, testes)
importam atributos internos do loader antigo. Mantemos esses nomes como
stubs para evitar ImportError — eles NÃO devem ser usados para novos
desenvolvimentos.
"""

from __future__ import annotations

from typing import Any, Dict, List

from scripts.excel_config_loader import (
    load_json,
    clear_cache,
    diagnose,
    _CACHE,  # re-exportado para compat com testes antigos
)

# Constantes antigas — mantidas como placeholders para imports legados
_SUPABASE_URL: str = ""
_SUPABASE_KEY: str = ""
_SCHEMA: str = "comissoes"
_SUPABASE_UNAVAILABLE: bool = True


def _query_table(table: str) -> List[Dict[str, Any]]:
    """Stub: o projeto migrou para Excel. Esta função não é mais suportada."""
    raise RuntimeError(
        "supabase_loader._query_table foi descontinuado. "
        "A fonte de verdade agora é configuracoes_comissoes.xlsx. "
        "Use scripts.excel_config_loader.load_json() para ler configurações."
    )


__all__ = [
    "load_json",
    "clear_cache",
    "diagnose",
    "_CACHE",
    "_SUPABASE_URL",
    "_SUPABASE_KEY",
    "_SCHEMA",
    "_SUPABASE_UNAVAILABLE",
    "_query_table",
]
