"""
receita/storage/ — Persistencia local dos historicos.

Substitui o backend Supabase (descontinuado em producao local) por
um arquivo SQLite unico (historico.db) na raiz do projeto, com UPSERT
nativo, indices e transacoes.

Modulos:
    sqlite_store — Cliente SQLite para historico_comissoes,
                   historico_processo_pai e historico_pagamentos_processo_pai.
"""

from receita.storage import sqlite_store  # noqa: F401
