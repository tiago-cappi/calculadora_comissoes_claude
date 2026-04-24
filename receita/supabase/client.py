"""
receita/supabase/client.py — Cliente historico (backend SQLite local).

API publica mantida identica a versao HTTP/Supabase, mas agora delegando
para `receita/storage/sqlite_store.py`. Arquivo unico `historico.db` na
raiz do projeto substitui o Supabase descontinuado em producao local.

API publica
-----------
query_table(table, filtros)           -> List[Dict]  (SELECT com filtros eq)
upsert(table, rows, conflict_cols)    -> Dict[str, int]  (INSERT OR UPDATE)
patch(table, filtro, valores)         -> str  (UPDATE com WHERE)
delete(table, filtro)                 -> str  (DELETE com WHERE)

Tabelas: historico_comissoes, historico_processo_pai, historico_pagamentos_processo_pai.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from receita.storage import sqlite_store


# Tabelas historicas do modulo receita/
TABELA_HISTORICO_COMISSOES = "historico_comissoes"
TABELA_HISTORICO_PARCIAL = TABELA_HISTORICO_COMISSOES
TABELA_HISTORICO_PROCESSO_PAI = "historico_processo_pai"
TABELA_HISTORICO_PAGAMENTOS_PAI = "historico_pagamentos_processo_pai"


def query_table(
    table: str,
    filtros: Optional[Dict[str, Any]] = None,
    select: str = "*",
) -> List[Dict[str, Any]]:
    return sqlite_store.query_table(table, filtros=filtros, select=select)


def upsert(
    table: str,
    rows: List[Dict[str, Any]],
    conflict_cols: List[str],
) -> Dict[str, int]:
    return sqlite_store.upsert(table, rows, conflict_cols)


def patch(
    table: str,
    filtro: Dict[str, Any],
    valores: Dict[str, Any],
) -> str:
    return sqlite_store.patch(table, filtro, valores)


def delete(table: str, filtro: Dict[str, Any]) -> str:
    return sqlite_store.delete(table, filtro)
