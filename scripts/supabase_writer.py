"""
supabase_writer.py — Escrita de Regras de Negócio no Supabase

Persiste alterações feitas via config_manager.py diretamente nas tabelas
do schema 'comissoes'. Usa a API REST PostgREST com UPSERT e DELETE.

API pública
-----------
persist(filename)          → Persiste o cache de um arquivo no Supabase
persist_all()              → Persiste todos os arquivos em cache
upsert_rows(table, rows, conflict_cols)  → UPSERT direto (uso avançado)
replace_all_rows(table, rows)            → DELETE ALL + INSERT (tabelas sem UNIQUE)
delete_rows(table, filtro)               → DELETE por filtro (uso avançado)

Tabelas com suporte a escrita
------------------------------
config_comissao      → replace (DELETE ALL + INSERT) por falta de UNIQUE CONSTRAINT
pesos_metas          → upsert por (cargo, colaborador, linha)
metas_individuais    → upsert por (colaborador, cargo, tipo_meta)
metas_aplicacao      → upsert por (linha, grupo, subgrupo, tipo_mercadoria, fabricante, aplicacao, tipo_meta)
meta_rentabilidade   → upsert por (linha, grupo, subgrupo, tipo_mercadoria, fabricante, aplicacao)
metas_fornecedores   → upsert por (linha, fornecedor)
monthly_avg_rates    → upsert por (moeda, ano, mes)
colaboradores        → upsert por (id_colaborador)
aliases              → upsert por (entidade, alias)
cross_selling        → upsert por (colaborador)
params               → upsert por chave única (single-row table)
"""

from __future__ import annotations

import json
import urllib.request
import urllib.error
import urllib.parse
from typing import Any, Dict, List, Optional

from scripts import supabase_loader as _sl


# ═══════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO (compartilhada com supabase_loader)
# ═══════════════════════════════════════════════════════════════════════

_SUPABASE_URL = _sl._SUPABASE_URL
_SUPABASE_KEY = _sl._SUPABASE_KEY
_SCHEMA = _sl._SCHEMA


# ═══════════════════════════════════════════════════════════════════════
# MAPEAMENTO: filename → (tabela, conflict_cols, serializer)
# ═══════════════════════════════════════════════════════════════════════

def _serialize_params(data: Dict) -> List[Dict]:
    """params é um dict chave/valor — não tem conflict_cols convencionais.
    Converte para lista de {key, value} para UPSERT por 'key'."""
    return [{"key": k, "value": v} for k, v in data.items()]


def _serialize_fc_escada(data) -> List[Dict]:
    """fc_escada_cargos: pode ser dict (chave=cargo) ou list (ja normalizado).
    
    Formatos suportados:
    - Dict: {cargo: {modo, num_degraus, piso_pct}} → converte para lista
    - List: [{cargo, modo, num_degraus, piso_pct}] → ja normalizado, retorna como-esta
    """
    if isinstance(data, dict):
        # Formato antigo: dict keyed by cargo
        rows = []
        for cargo, vals in data.items():
            row = {"cargo": cargo}
            row.update(vals)
            rows.append(row)
        return rows
    elif isinstance(data, list):
        # Formato novo: lista de dicts com campo cargo
        return data
    else:
        raise TypeError(f"fc_escada_cargos deve ser dict ou list, recebido: {type(data)}")


def _serialize_aliases(data: Dict) -> List[Dict]:
    """aliases: dict {entidade: {alias: nome_padrao}} → lista de rows."""
    rows = []
    for entidade, mapping in data.items():
        for alias, nome_padrao in mapping.items():
            rows.append({"entidade": entidade, "alias": alias, "nome_padrao": nome_padrao})
    return rows


def _serialize_meta_rentabilidade(data) -> List[Dict]:
    """meta_rentabilidade: aceita lista plana ou dict {periodo: [rows]}.

    O loader retorna lista plana; o config_manager trabalha com dict {periodo: [rows]}.
    Ambos os formatos são suportados.
    """
    if isinstance(data, list):
        return data
    # dict {periodo: [rows]}
    rows = []
    for periodo, entries in data.items():
        for e in entries:
            row = {}
            row.update(e)
            rows.append(row)
    return rows


def _serialize_monthly_avg_rates(data: Dict) -> List[Dict]:
    """monthly_avg_rates: dict {moeda: {ano: {mes: taxa}}} → lista de rows."""
    rows = []
    for moeda, anos in data.items():
        for ano, meses in anos.items():
            for mes, taxa in meses.items():
                rows.append({"moeda": moeda, "ano": int(ano), "mes": int(mes), "taxa_media": taxa})
    return rows


def _serialize_config_comissao(data: List[Dict]) -> List[Dict]:
    """config_comissao: normaliza campos antes da escrita.

    - Remove campo calculado taxa_maxima_efetiva (calculado pelo loader, não é coluna Supabase)
    - Converte NULL para string vazia nos campos de identidade
    """
    conflict_cols = {"linha", "grupo", "subgrupo", "tipo_mercadoria",
                     "fabricante", "aplicacao", "cargo", "colaborador"}
    cols = ["linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante",
            "aplicacao", "cargo", "colaborador", "fatia_cargo",
            "taxa_rateio_maximo_pct", "ativo"]
    result = []
    for row in data:
        out = {}
        for k in cols:
            if k not in row:
                continue
            v = row[k]
            # Converte None → "" nas colunas de conflito
            if k in conflict_cols and v is None:
                v = ""
            out[k] = v
        result.append(out)
    return result


# filename → (tabela, conflict_cols, serializer_fn ou None [, strategy])
# serializer_fn=None → data já é List[Dict], usa direto
# strategy (opcional, padrão "upsert"): "upsert" ou "replace" (DELETE ALL + INSERT)
# Apenas config_comissao usa "replace" — tabela sem UNIQUE CONSTRAINT
_WRITE_MAP: Dict[str, tuple] = {
    "config_comissao.json": (
        "config_comissao",
        ["linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante", "aplicacao", "cargo", "colaborador"],
        _serialize_config_comissao,
        "replace",  # sem unique constraint → DELETE ALL + INSERT
    ),
    "pesos_metas.json": (
        "pesos_metas",
        ["cargo", "colaborador", "linha"],
        None,
        "replace",  # Colunas de conflito podem ser NULL → ON CONFLICT falha; DELETE ALL + INSERT é seguro
    ),
    "metas_individuais.json": (
        "metas_individuais",
        ["colaborador", "cargo", "tipo_meta"],
        None,
        "replace",  # sem UNIQUE CONSTRAINT → DELETE ALL + INSERT
    ),
    "metas_aplicacao.json": (
        "metas_aplicacao",
        ["linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante", "aplicacao", "tipo_meta"],
        None,
        "replace",  # sem UNIQUE CONSTRAINT → DELETE ALL + INSERT
    ),
    "meta_rentabilidade.json": (
        "meta_rentabilidade",
        ["linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante", "aplicacao"],
        _serialize_meta_rentabilidade,
        "replace",  # Use DELETE ALL + INSERT para evitar schema cache issues
    ),
    "metas_fornecedores.json": (
        "metas_fornecedores",
        ["linha", "fornecedor"],
        None,
    ),
    "monthly_avg_rates.json": (
        "monthly_avg_rates",
        ["moeda", "ano", "mes"],
        _serialize_monthly_avg_rates,
    ),
    "colaboradores.json": (
        "colaboradores",
        ["id_colaborador"],
        None,
    ),
    "aliases.json": (
        "aliases",
        ["entidade", "alias"],
        _serialize_aliases,
    ),
    "cross_selling.json": (
        "cross_selling",
        ["colaborador"],
        None,
    ),
    "fc_escada_cargos.json": (
        "fc_escada_cargos",
        ["cargo"],
        _serialize_fc_escada,
        "replace",  # Sem UNIQUE CONSTRAINT em cargo → usar DELETE ALL + INSERT
    ),
}


# ═══════════════════════════════════════════════════════════════════════
# ACESSO REST
# ═══════════════════════════════════════════════════════════════════════

def _rest_request(
    method: str,
    path: str,
    body: Any = None,
    extra_headers: Dict[str, str] = None,
) -> Any:
    """Executa uma requisição REST ao Supabase.

    Args:
        method: "GET", "POST", "PATCH", "DELETE"
        path: Path relativo (ex: "/rest/v1/config_comissao")
        body: Dados para POST/PATCH (serializado como JSON)
        extra_headers: Headers adicionais

    Returns:
        Resposta decodificada (list ou dict) ou None para DELETE sem retorno.

    Raises:
        RuntimeError: Em caso de erro HTTP.
    """
    url = f"{_SUPABASE_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None

    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("apikey", _SUPABASE_KEY)
    req.add_header("Authorization", f"Bearer {_SUPABASE_KEY}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("Accept-Profile", _SCHEMA)
    req.add_header("Content-Profile", _SCHEMA)

    if extra_headers:
        for k, v in extra_headers.items():
            req.add_header(k, v)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8")) if raw.strip() else None
    except urllib.error.HTTPError as e:
        body_err = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Erro HTTP {e.code} em {method} {path}: {body_err}"
        ) from e
    except Exception as e:
        raise RuntimeError(f"Erro em {method} {path}: {e}") from e


# ═══════════════════════════════════════════════════════════════════════
# API PÚBLICA
# ═══════════════════════════════════════════════════════════════════════

def upsert_rows(
    table: str,
    rows: List[Dict],
    conflict_cols: List[str],
    batch_size: int = 500,
) -> Dict[str, int]:
    """UPSERT de uma lista de rows em uma tabela do Supabase.

    Usa POST com header Prefer: resolution=merge-duplicates para upsert.
    Envia em lotes para evitar limite de payload.

    Args:
        table: Nome da tabela (sem schema).
        rows: Lista de dicts a inserir/atualizar.
        conflict_cols: Colunas de conflito para o ON CONFLICT.
        batch_size: Tamanho do lote (padrão 500).

    Returns:
        {"upserted": N, "batches": B}
    """
    if not rows:
        return {"upserted": 0, "batches": 0}

    # PostgREST PGRST102: todas as linhas do batch devem ter as mesmas chaves.
    # Normaliza preenchendo com None as chaves ausentes em cada row.
    all_keys: list = []
    seen: set = set()
    for row in rows:
        for k in row.keys():
            if k not in seen:
                all_keys.append(k)
                seen.add(k)
    rows = [{k: row.get(k) for k in all_keys} for row in rows]

    path = f"/rest/v1/{table}"
    conflict_str = ",".join(conflict_cols)

    total = 0
    batches = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        _rest_request(
            "POST",
            path,
            body=batch,
            extra_headers={
                "Prefer": f"resolution=merge-duplicates,return=minimal",
                "On-Conflict": conflict_str,
            },
        )
        total += len(batch)
        batches += 1

    return {"upserted": total, "batches": batches}


def replace_all_rows(
    table: str,
    rows: List[Dict],
    batch_size: int = 500,
) -> Dict[str, int]:
    """Substitui TODOS os dados de uma tabela (DELETE ALL + INSERT).

    Usa esta estratégia quando a tabela NÃO possui UNIQUE CONSTRAINT
    nas colunas de conflito, o que faz o UPSERT duplicar linhas.

    Args:
        table: Nome da tabela (sem schema).
        rows: Lista de dicts a inserir.
        batch_size: Tamanho do lote (padrão 500).

    Returns:
        {"inserted": N, "batches": B}
    """
    # DELETE ALL rows — usa "coluna=not.is.null" para deletar todos de uma vez.
    # Detecta automaticamente a primeira coluna do payload como filtro.
    if rows:
        first_col = next(iter(rows[0].keys()))
        _rest_request(
            "DELETE",
            f"/rest/v1/{table}?{first_col}=not.is.null",
            extra_headers={"Prefer": "return=minimal"},
        )

    if not rows:
        return {"inserted": 0, "batches": 0}

    # PostgREST PGRST102: todas as linhas do batch devem ter as mesmas chaves.
    all_keys: list = []
    seen: set = set()
    for row in rows:
        for k in row.keys():
            if k not in seen:
                all_keys.append(k)
                seen.add(k)
    rows = [{k: row.get(k) for k in all_keys} for row in rows]

    path = f"/rest/v1/{table}"
    total = 0
    batches = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        _rest_request(
            "POST",
            path,
            body=batch,
            extra_headers={"Prefer": "return=minimal"},
        )
        total += len(batch)
        batches += 1

    return {"inserted": total, "batches": batches}


def delete_rows(table: str, filtro: Dict[str, Any]) -> str:
    """DELETE de rows que combinam com o filtro (PostgREST query params).

    Args:
        table: Nome da tabela.
        filtro: Dict {coluna: valor} — todos os pares são combinados com AND e eq.

    Returns:
        Mensagem de confirmação.

    Raises:
        ValueError: Se filtro estiver vazio (segurança contra DELETE ALL).
    """
    if not filtro:
        raise ValueError("filtro não pode ser vazio — DELETE sem filtro não é permitido.")

    # URL-encode dos valores para suportar espaços e caracteres especiais
    params = "&".join(
        f"{urllib.parse.quote(str(k))}=eq.{urllib.parse.quote(str(v))}"
        for k, v in filtro.items()
    )
    path = f"/rest/v1/{table}?{params}"
    _rest_request("DELETE", path, extra_headers={"Prefer": "return=minimal"})
    return f"✓ DELETE em {_SCHEMA}.{table} com filtro {filtro}"


def patch_rows(table: str, filtro: Dict[str, Any], valores: Dict[str, Any]) -> str:
    """PATCH (UPDATE) em rows que combinam com o filtro.

    Usa o método PATCH do PostgREST para atualizar colunas específicas
    nas linhas que satisfazem o filtro.

    Args:
        table: Nome da tabela (sem schema).
        filtro: Dict {coluna: valor} — condição WHERE (todos com AND + eq).
        valores: Dict {coluna: novo_valor} — campos a atualizar.

    Returns:
        Mensagem de confirmação com número de linhas afetadas.

    Raises:
        ValueError: Se filtro ou valores estiverem vazios.
    """
    if not filtro:
        raise ValueError("filtro não pode ser vazio — PATCH sem filtro não é permitido.")
    if not valores:
        raise ValueError("valores não pode ser vazio.")

    params = "&".join(
        f"{urllib.parse.quote(str(k))}=eq.{urllib.parse.quote(str(v))}"
        for k, v in filtro.items()
    )
    path = f"/rest/v1/{table}?{params}"
    _rest_request(
        "PATCH",
        path,
        body=valores,
        extra_headers={"Prefer": "return=minimal"},
    )
    return f"✓ PATCH em {_SCHEMA}.{table} | filtro={filtro} → {valores}"


def persist(filename: str) -> str:
    """Persiste o cache de um filename no Supabase via UPSERT.

    Lê os dados do cache do supabase_loader, serializa se necessário,
    e executa UPSERT na tabela correspondente.

    Args:
        filename: ex. "config_comissao.json"

    Returns:
        Mensagem de resultado.

    Raises:
        KeyError: Se filename não estiver em _WRITE_MAP.
        RuntimeError: Se o UPSERT falhar.
    """
    if filename not in _WRITE_MAP:
        supported = sorted(_WRITE_MAP.keys())
        raise KeyError(
            f"'{filename}' não suportado para escrita.\n"
            f"Suportados: {supported}"
        )

    if filename not in _sl._CACHE:
        return f"⚠ '{filename}' não está em cache — nada para persistir."

    table, conflict_cols, serializer = _WRITE_MAP[filename][:3]
    strategy = _WRITE_MAP[filename][3] if len(_WRITE_MAP[filename]) > 3 else "upsert"
    data = _sl._CACHE[filename]

    rows = serializer(data) if serializer else data

    if not isinstance(rows, list):
        raise RuntimeError(f"Dados serializados de '{filename}' não são lista: {type(rows)}")

    if strategy == "replace":
        result = replace_all_rows(table, rows)
        action = "SUBSTITUÍDO"
        count_key = "inserted"
    else:
        result = upsert_rows(table, rows, conflict_cols)
        action = "PERSISTIDO"
        count_key = "upserted"

    _sl.clear_cache()  # Invalidar cache para forçar re-leitura limpa do banco
    count = result.get(count_key, result.get("inserted", result.get("upserted", 0)))
    return (
        f"✓ {action}: {filename} → {_SCHEMA}.{table}\n"
        f"  {count} linha(s) em {result['batches']} lote(s).\n"
        f"  Cache invalidado — próxima leitura virá do Supabase."
    )


def persist_all() -> str:
    """Persiste todos os arquivos atualmente em cache no Supabase.

    Itera _sl._CACHE e persiste cada filename que estiver em _WRITE_MAP.

    Returns:
        Relatório consolidado.
    """
    cached = list(_sl._CACHE.keys())
    if not cached:
        return "⚠ Cache vazio — nada para persistir."

    lines = ["PERSISTÊNCIA EM LOTE:", ""]
    total_rows = 0
    errors = []

    for filename in cached:
        if filename not in _WRITE_MAP:
            lines.append(f"  ⊘ {filename} — sem suporte a escrita (ignorado)")
            continue
        try:
            msg = persist(filename)
            # Extrair contagem do msg
            for part in msg.split("\n"):
                if "linha(s)" in part:
                    n = int(part.strip().split()[0])
                    total_rows += n
            lines.append(f"  ✓ {filename}")
        except Exception as e:
            errors.append(f"{filename}: {e}")
            lines.append(f"  ✗ {filename} — ERRO: {e}")

    lines.append("")
    lines.append(f"Total: {total_rows} linhas persistidas.")
    if errors:
        lines.append(f"⚠ {len(errors)} erro(s) encontrado(s).")

    return "\n".join(lines)
