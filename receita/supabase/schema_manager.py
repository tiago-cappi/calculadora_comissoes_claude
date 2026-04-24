"""
receita/supabase/schema_manager.py - Checagem e bootstrap do schema historico.

Backend SQLite local: as tabelas sao criadas automaticamente pelo
`receita/storage/sqlite_store` na primeira conexao. Este modulo mantem
o comando CLI `print-sql` e a inspecao de estado para compatibilidade.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parent
SCHEMA_SQL_PATH = ROOT / "schema_historico.sql"

TABLES_REQUIRED = (
    "historico_comissoes",
    "historico_processo_pai",
    "historico_pagamentos_processo_pai",
)


def load_schema_sql() -> str:
    """Retorna o DDL versionado (Postgres) do modulo receita/.

    Nota: o backend atual e SQLite; este SQL eh apenas referencia
    historica e pode ser usado para bootstrap em um Postgres externo.
    """
    return SCHEMA_SQL_PATH.read_text(encoding="utf-8")


def _table_exists(table: str) -> bool:
    """Verifica se a tabela existe no SQLite local."""
    from receita.storage import sqlite_store

    conn = sqlite_store._get_conn()
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def check_tables() -> Dict[str, bool]:
    """Verifica a existencia das tabelas historicas no SQLite local."""
    return {table: _table_exists(table) for table in TABLES_REQUIRED}


def missing_required_tables() -> List[str]:
    """Lista as tabelas obrigatorias ausentes no schema."""
    status = check_tables()
    return [table for table, exists in status.items() if not exists]


def ensure_required_tables(strict: bool = False) -> List[str]:
    """Garante que as tabelas historicas existam.

    No backend SQLite, o schema eh criado automaticamente na primeira
    conexao do `sqlite_store`. Esta funcao dispara essa criacao e retorna
    a lista de tabelas que (improvavelmente) ainda estejam ausentes.
    """
    from receita.storage import sqlite_store
    sqlite_store._get_conn()  # inicializa schema se necessario
    missing = missing_required_tables()
    if strict and missing:
        raise RuntimeError(
            "Schema historico SQLite ausente. "
            f"Tabelas faltantes: {', '.join(missing)}."
        )
    return missing


def missing_required_tables() -> List[str]:
    status = check_tables()
    return [table for table, exists in status.items() if not exists]


def render_report() -> str:
    """Retorna um relatorio textual com o estado do schema historico."""
    from receita.storage import sqlite_store

    status = check_tables()
    lines = [
        "=" * 60,
        "SCHEMA HISTORICO - BACKEND SQLITE LOCAL",
        "=" * 60,
        f"Arquivo: {sqlite_store.db_path()}",
        "",
    ]
    for table, exists in status.items():
        lines.append(f"{'OK' if exists else 'MISSING':<8} {table}")
    missing = [table for table, exists in status.items() if not exists]
    lines.append("")
    if missing:
        lines.append("Rode qualquer operacao do storage para criar o schema.")
    else:
        lines.append("Schema historico completo.")
    return "\n".join(lines)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Checa o schema historico do modulo receita (backend SQLite local)."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check", help="Verifica se as tabelas historicas existem no SQLite.")
    sub.add_parser("print-sql", help="Imprime o DDL Postgres versionado (referencia).")

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "check":
        print(render_report())
        sys.exit(1 if missing_required_tables() else 0)

    if args.command == "print-sql":
        print(load_schema_sql())
        return


if __name__ == "__main__":
    main()
