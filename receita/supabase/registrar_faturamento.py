"""
Persistencia das tabelas historicas do modulo receita/.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

from receita.schemas.historico import (
    HistoricoComissao,
    HistoricoPagamentoProcessoPai,
    HistoricoProcessoPai,
)
from receita.supabase.client import (
    TABELA_HISTORICO_PAGAMENTOS_PAI,
    TABELA_HISTORICO_PARCIAL,
    TABELA_HISTORICO_PROCESSO_PAI,
    patch,
    upsert,
)


_CONFLICT_COLS_PARCIAL = ["nome", "processo", "documento", "tipo_pagamento", "mes_apuracao", "ano_apuracao"]
_CONFLICT_COLS_PROCESSO_PAI = [
    "numero_pc",
    "codigo_cliente",
    "processo",
    "mes_referencia",
    "ano_referencia",
]
_CONFLICT_COLS_PAGAMENTOS_PAI = [
    "numero_pc",
    "codigo_cliente",
    "documento",
    "mes_referencia",
    "ano_referencia",
]


def registrar_faturamento_parcial(item: HistoricoComissao) -> bool:
    row = _historico_para_row(item)
    result = upsert(TABELA_HISTORICO_PARCIAL, [row], _CONFLICT_COLS_PARCIAL)
    return result.get("upserted", 0) > 0 or result.get("batches", 0) > 0


def registrar_varios(itens: List[HistoricoComissao]) -> Dict:
    return _upsert_dataclasses(
        itens,
        _historico_para_row,
        TABELA_HISTORICO_PARCIAL,
        _CONFLICT_COLS_PARCIAL,
        lambda i: (
            f"processo={i.processo}, gl={i.nome}, documento={i.documento}, "
            f"periodo={i.mes_apuracao:02d}/{i.ano_apuracao}"
        ),
    )


def registrar_historico_processos_pai(itens: List[HistoricoProcessoPai]) -> Dict:
    return _upsert_dataclasses(
        itens,
        _historico_processo_pai_para_row,
        TABELA_HISTORICO_PROCESSO_PAI,
        _CONFLICT_COLS_PROCESSO_PAI,
        lambda i: f"pc={i.numero_pc}, cli={i.codigo_cliente}, processo={i.processo}",
    )


def registrar_historico_pagamentos_pai(itens: List[HistoricoPagamentoProcessoPai]) -> Dict:
    return _upsert_dataclasses(
        itens,
        _historico_pagamento_pai_para_row,
        TABELA_HISTORICO_PAGAMENTOS_PAI,
        _CONFLICT_COLS_PAGAMENTOS_PAI,
        lambda i: f"pc={i.numero_pc}, cli={i.codigo_cliente}, documento={i.documento}",
    )


def marcar_historicos_reconciliados(
    processo: str,
    gl_nome: str,
    numero_pc: str = "",
    codigo_cliente: str = "",
) -> str:
    filtro = {
        "nome": str(gl_nome).strip(),
        "tipo": "recebimento",
        "reconciliado": False,
    }
    if processo:
        filtro["processo"] = str(processo).strip().upper()
    if numero_pc:
        filtro["numero_pc"] = str(numero_pc).strip().upper()
    if codigo_cliente:
        filtro["codigo_cliente"] = str(codigo_cliente).strip().upper()

    return patch(
        TABELA_HISTORICO_PARCIAL,
        filtro=filtro,
        valores={"reconciliado": True},
    )


def _upsert_dataclasses(
    itens: Iterable,
    serializer,
    tabela: str,
    conflict_cols: List[str],
    label_fn,
) -> Dict:
    itens = list(itens)
    if not itens:
        return {"ok": 0, "erros": []}

    rows = []
    erros = []
    for i, item in enumerate(itens):
        try:
            rows.append(serializer(item))
        except Exception as exc:
            erros.append(f"Item {i} ({label_fn(item)}): {exc}")

    if rows:
        try:
            upsert(tabela, rows, conflict_cols)
        except RuntimeError as exc:
            erros.append(f"Erro no UPSERT em lote para {tabela}: {exc}")
            return {"ok": 0, "erros": erros}

    return {"ok": len(rows), "erros": erros}


def _historico_para_row(item: HistoricoComissao) -> Dict:
    return {
        "nome": item.nome,
        "cargo": item.cargo,
        "processo": item.processo,
        "numero_pc": item.numero_pc,
        "codigo_cliente": item.codigo_cliente,
        "tipo": item.tipo,
        "tipo_pagamento": item.tipo_pagamento,
        "documento": item.documento,
        "nf_extraida": item.nf_extraida,
        "linha_negocio": item.linha_negocio,
        "status_processo": item.status_processo,
        "mes_apuracao": item.mes_apuracao,
        "ano_apuracao": item.ano_apuracao,
        "valor_documento": round(item.valor_documento, 2),
        "valor_processo": round(item.valor_processo, 2),
        "tcmp": round(item.tcmp, 8),
        "fcmp_rampa": round(item.fcmp_rampa, 8),
        "fcmp_aplicado": round(item.fcmp_aplicado, 8),
        "fcmp_considerado": round(item.fcmp_considerado, 8),
        "fcmp_modo": item.fcmp_modo,
        "comissao_potencial": round(item.comissao_potencial, 4),
        "comissao_adiantada": round(item.comissao_adiantada, 4),
        "comissao_total": round(item.comissao_total, 4),
        "status_faturamento_completo": bool(item.status_faturamento_completo),
        "status_pagamento_completo": item.status_pagamento_completo,
        "reconciliado": bool(item.reconciliado),
        "ac_snapshot_json": item.ac_snapshot_json,
        "af_snapshot_json": item.af_snapshot_json,
        "tcmp_detalhes_json": item.tcmp_detalhes_json,
        "fcmp_detalhes_json": item.fcmp_detalhes_json,
        "created_at": _serialize_datetime(item.created_at, default_now=True),
    }


def _historico_processo_pai_para_row(item: HistoricoProcessoPai) -> Dict:
    return {
        "numero_pc": item.numero_pc,
        "codigo_cliente": item.codigo_cliente,
        "processo": item.processo,
        "is_processo_pai": bool(item.is_processo_pai),
        "status_faturado": bool(item.status_faturado),
        "status_pago": item.status_pago,
        "mes_referencia": item.mes_referencia,
        "ano_referencia": item.ano_referencia,
        "created_at": _serialize_datetime(item.created_at, default_now=True),
    }


def _historico_pagamento_pai_para_row(item: HistoricoPagamentoProcessoPai) -> Dict:
    return {
        "numero_pc": item.numero_pc,
        "codigo_cliente": item.codigo_cliente,
        "processo": item.processo,
        "numero_nf": item.numero_nf,
        "documento": item.documento,
        "situacao_codigo": int(item.situacao_codigo),
        "situacao_texto": item.situacao_texto,
        "dt_prorrogacao": _serialize_datetime(item.dt_prorrogacao),
        "data_baixa": _serialize_datetime(item.data_baixa),
        "valor_documento": round(item.valor_documento, 2),
        "mes_referencia": item.mes_referencia,
        "ano_referencia": item.ano_referencia,
        "created_at": _serialize_datetime(item.created_at, default_now=True),
    }


def _serialize_datetime(value: Optional[datetime], default_now: bool = False) -> Optional[str]:
    if value is None:
        if default_now:
            return datetime.now(timezone.utc).isoformat()
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc).isoformat()
    return value.isoformat()
