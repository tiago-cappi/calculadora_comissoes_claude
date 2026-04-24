"""
receita/storage/sqlite_store.py — Backend SQLite para historicos de comissao.

Arquivo unico `historico.db` na raiz do projeto. Tres tabelas:
    - historico_comissoes               (granular por GL+processo+doc+periodo)
    - historico_processo_pai            (vinculo mensal Pai <-> filhos)
    - historico_pagamentos_processo_pai (parcelas de AF por Pai)

Substitui a camada HTTP do Supabase. API compatvel com `receita/supabase/client.py`:
    query_table(table, filtros?, select="*") -> List[Dict]
    upsert(table, rows, conflict_cols)       -> Dict[str, int]
    patch(table, filtro, valores)            -> str
    delete(table, filtro)                    -> str

O schema eh criado automaticamente na primeira chamada. Todas as escritas
sao transacionais. Thread-safe via check_same_thread=False + lock por
conexao (uso sincrono do pipeline).
"""

from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Caminho do banco
# ---------------------------------------------------------------------------

def _default_db_path() -> str:
    """Retorna o caminho padrao do banco: <raiz_projeto>/historico.db.

    Permite override via env var `RECEITA_HISTORICO_DB` para testes.
    """
    override = os.environ.get("RECEITA_HISTORICO_DB")
    if override:
        return override
    # receita/storage/sqlite_store.py -> subir 2 niveis para a raiz
    root = Path(__file__).resolve().parents[2]
    return str(root / "historico.db")


_DB_PATH: Optional[str] = None
_CONN: Optional[sqlite3.Connection] = None
_LOCK = threading.RLock()


def _get_conn() -> sqlite3.Connection:
    """Retorna conexao singleton, criando o schema na primeira chamada."""
    global _CONN, _DB_PATH
    with _LOCK:
        if _CONN is None:
            _DB_PATH = _default_db_path()
            Path(_DB_PATH).parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=OFF;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            _init_schema(conn)
            _CONN = conn
        return _CONN


def close_connection() -> None:
    """Fecha a conexao (usado em testes e shutdown)."""
    global _CONN
    with _LOCK:
        if _CONN is not None:
            _CONN.close()
            _CONN = None


def reset_for_tests() -> None:
    """Fecha a conexao e remove o arquivo (apenas se override via env)."""
    global _CONN, _DB_PATH
    with _LOCK:
        if _CONN is not None:
            _CONN.close()
            _CONN = None
        if _DB_PATH and os.environ.get("RECEITA_HISTORICO_DB"):
            try:
                os.remove(_DB_PATH)
            except OSError:
                pass
        _DB_PATH = None


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_TABELAS_CONHECIDAS = {
    "historico_comissoes",
    "historico_processo_pai",
    "historico_pagamentos_processo_pai",
}


def _init_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS historico_comissoes (
        nome                         TEXT    NOT NULL,
        cargo                        TEXT    NOT NULL DEFAULT '',
        processo                     TEXT    NOT NULL,
        numero_pc                    TEXT    NOT NULL DEFAULT '',
        codigo_cliente               TEXT    NOT NULL DEFAULT '',
        tipo                         TEXT    NOT NULL DEFAULT 'recebimento',
        tipo_pagamento               TEXT    NOT NULL DEFAULT 'REGULAR',
        documento                    TEXT    NOT NULL DEFAULT '',
        nf_extraida                  TEXT    NOT NULL DEFAULT '',
        linha_negocio                TEXT    NOT NULL DEFAULT '',
        status_processo              TEXT    NOT NULL DEFAULT '',
        mes_apuracao                 INTEGER NOT NULL,
        ano_apuracao                 INTEGER NOT NULL,
        valor_documento              REAL    NOT NULL DEFAULT 0,
        valor_processo               REAL    NOT NULL DEFAULT 0,
        tcmp                         REAL    NOT NULL DEFAULT 0,
        fcmp_rampa                   REAL    NOT NULL DEFAULT 1,
        fcmp_aplicado                REAL    NOT NULL DEFAULT 1,
        fcmp_considerado             REAL    NOT NULL DEFAULT 1,
        fcmp_modo                    TEXT    NOT NULL DEFAULT 'PROVISORIO',
        comissao_potencial           REAL    NOT NULL DEFAULT 0,
        comissao_adiantada           REAL    NOT NULL DEFAULT 0,
        comissao_total               REAL    NOT NULL DEFAULT 0,
        status_faturamento_completo  INTEGER NOT NULL DEFAULT 0,
        status_pagamento_completo    INTEGER NULL,
        reconciliado                 INTEGER NOT NULL DEFAULT 0,
        ac_snapshot_json             TEXT    NOT NULL DEFAULT '[]',
        af_snapshot_json             TEXT    NOT NULL DEFAULT '[]',
        tcmp_detalhes_json           TEXT    NOT NULL DEFAULT '[]',
        fcmp_detalhes_json           TEXT    NOT NULL DEFAULT '[]',
        created_at                   TEXT    NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (nome, processo, documento, tipo_pagamento, mes_apuracao, ano_apuracao)
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS ix_hist_com_parent ON historico_comissoes (numero_pc, codigo_cliente, reconciliado);")
    cur.execute("CREATE INDEX IF NOT EXISTS ix_hist_com_periodo ON historico_comissoes (ano_apuracao, mes_apuracao);")
    cur.execute("CREATE INDEX IF NOT EXISTS ix_hist_com_proc ON historico_comissoes (processo, nome, documento);")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS historico_processo_pai (
        numero_pc        TEXT    NOT NULL,
        codigo_cliente   TEXT    NOT NULL,
        processo         TEXT    NOT NULL,
        is_processo_pai  INTEGER NOT NULL DEFAULT 0,
        status_faturado  INTEGER NOT NULL DEFAULT 0,
        status_pago      INTEGER NULL,
        mes_referencia   INTEGER NOT NULL,
        ano_referencia   INTEGER NOT NULL,
        created_at       TEXT    NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (numero_pc, codigo_cliente, processo, mes_referencia, ano_referencia)
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS ix_hist_pai_periodo ON historico_processo_pai (ano_referencia, mes_referencia);")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS historico_pagamentos_processo_pai (
        numero_pc        TEXT    NOT NULL,
        codigo_cliente   TEXT    NOT NULL,
        processo         TEXT    NOT NULL,
        numero_nf        TEXT    NOT NULL DEFAULT '',
        documento        TEXT    NOT NULL,
        situacao_codigo  INTEGER NOT NULL DEFAULT -1,
        situacao_texto   TEXT    NOT NULL DEFAULT 'Desconhecido',
        dt_prorrogacao   TEXT    NULL,
        data_baixa       TEXT    NULL,
        valor_documento  REAL    NOT NULL DEFAULT 0,
        mes_referencia   INTEGER NOT NULL,
        ano_referencia   INTEGER NOT NULL,
        created_at       TEXT    NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (numero_pc, codigo_cliente, documento, mes_referencia, ano_referencia)
    );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS ix_hist_pag_periodo ON historico_pagamentos_processo_pai (ano_referencia, mes_referencia);")

    conn.commit()


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

_BOOL_COLS = {
    "historico_comissoes": {
        "status_faturamento_completo",
        "status_pagamento_completo",
        "reconciliado",
    },
    "historico_processo_pai": {
        "is_processo_pai",
        "status_faturado",
        "status_pago",
    },
    "historico_pagamentos_processo_pai": set(),
}


def _to_sqlite_value(table: str, col: str, value: Any) -> Any:
    """Converte valores Python para tipos do SQLite (preserva None)."""
    if value is None:
        return None
    bools = _BOOL_COLS.get(table, set())
    if col in bools:
        return 1 if value else 0
    if isinstance(value, bool):
        return 1 if value else 0
    return value


def _from_sqlite_row(table: str, row: sqlite3.Row) -> Dict[str, Any]:
    """Converte sqlite3.Row em dict, traduzindo booleanos."""
    bools = _BOOL_COLS.get(table, set())
    out: Dict[str, Any] = {}
    for key in row.keys():
        value = row[key]
        if key in bools and value is not None:
            value = bool(value)
        out[key] = value
    return out


def _ensure_known_table(table: str) -> None:
    if table not in _TABELAS_CONHECIDAS:
        raise ValueError(
            f"Tabela '{table}' nao suportada pelo storage local. "
            f"Tabelas conhecidas: {sorted(_TABELAS_CONHECIDAS)}"
        )


def _listar_colunas(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    return [r[1] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# API publica
# ---------------------------------------------------------------------------

def query_table(
    table: str,
    filtros: Optional[Dict[str, Any]] = None,
    select: str = "*",
) -> List[Dict[str, Any]]:
    """SELECT com filtros opcionais de igualdade (AND)."""
    _ensure_known_table(table)
    conn = _get_conn()
    sql = f"SELECT {select} FROM {table}"
    params: List[Any] = []
    if filtros:
        clauses = []
        for col, val in filtros.items():
            clauses.append(f"{col} = ?")
            params.append(_to_sqlite_value(table, col, val))
        sql += " WHERE " + " AND ".join(clauses)
    with _LOCK:
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
    return [_from_sqlite_row(table, r) for r in rows]


def upsert(
    table: str,
    rows: List[Dict[str, Any]],
    conflict_cols: List[str],
) -> Dict[str, int]:
    """INSERT ... ON CONFLICT (...) DO UPDATE SET ... em lote."""
    _ensure_known_table(table)
    if not rows:
        return {"upserted": 0, "batches": 0}

    conn = _get_conn()
    cols_tabela = _listar_colunas(conn, table)
    cols_validas = set(cols_tabela)

    # Uniao das chaves presentes nos rows (apenas colunas validas da tabela)
    all_keys: List[str] = []
    seen: set = set()
    for row in rows:
        for k in row.keys():
            if k in cols_validas and k not in seen:
                all_keys.append(k)
                seen.add(k)

    if not all_keys:
        return {"upserted": 0, "batches": 0}

    cols_str = ", ".join(all_keys)
    placeholders = ", ".join(["?"] * len(all_keys))
    conflict_str = ", ".join(conflict_cols)
    update_cols = [c for c in all_keys if c not in conflict_cols]
    update_str = ", ".join(f"{c} = excluded.{c}" for c in update_cols) if update_cols else ""

    if update_str:
        sql = (
            f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"
        )
    else:
        sql = (
            f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_str}) DO NOTHING"
        )

    values_list = []
    for row in rows:
        values_list.append(tuple(
            _to_sqlite_value(table, k, row.get(k)) for k in all_keys
        ))

    with _LOCK:
        cur = conn.cursor()
        cur.executemany(sql, values_list)
        conn.commit()

    return {"upserted": len(values_list), "batches": 1}


def patch(
    table: str,
    filtro: Dict[str, Any],
    valores: Dict[str, Any],
) -> str:
    """UPDATE table SET ... WHERE <filtro>."""
    _ensure_known_table(table)
    if not filtro:
        raise ValueError("filtro nao pode ser vazio.")
    if not valores:
        raise ValueError("valores nao pode ser vazio.")

    conn = _get_conn()
    cols_validas = set(_listar_colunas(conn, table))

    set_cols = [c for c in valores.keys() if c in cols_validas]
    if not set_cols:
        return f"PATCH sem colunas validas em {table}."
    set_clause = ", ".join(f"{c} = ?" for c in set_cols)
    where_clause = " AND ".join(f"{c} = ?" for c in filtro.keys())

    params = [_to_sqlite_value(table, c, valores[c]) for c in set_cols]
    params.extend(_to_sqlite_value(table, c, v) for c, v in filtro.items())

    with _LOCK:
        cur = conn.cursor()
        cur.execute(f"UPDATE {table} SET {set_clause} WHERE {where_clause}", params)
        affected = cur.rowcount
        conn.commit()

    return f"PATCH {table}: {affected} linha(s) atualizada(s) (filtro={filtro})."


def delete(table: str, filtro: Dict[str, Any]) -> str:
    """DELETE FROM table WHERE <filtro>."""
    _ensure_known_table(table)
    if not filtro:
        raise ValueError("filtro nao pode ser vazio (DELETE sem filtro bloqueado).")

    conn = _get_conn()
    where_clause = " AND ".join(f"{c} = ?" for c in filtro.keys())
    params = [_to_sqlite_value(table, c, v) for c, v in filtro.items()]

    with _LOCK:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table} WHERE {where_clause}", params)
        affected = cur.rowcount
        conn.commit()

    return f"DELETE {table}: {affected} linha(s) removida(s) (filtro={filtro})."


def db_path() -> str:
    """Retorna o caminho do arquivo SQLite em uso (inicializa se necessario)."""
    _get_conn()
    return _DB_PATH or _default_db_path()
