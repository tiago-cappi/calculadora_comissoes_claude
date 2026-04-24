"""
Leitura das tabelas historicas do modulo receita/.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from receita.schemas.historico import (
    HistoricoComissao,
    HistoricoPagamentoProcessoPai,
    HistoricoProcessoPai,
)
from receita.supabase.client import (
    TABELA_HISTORICO_COMISSOES,
    TABELA_HISTORICO_PAGAMENTOS_PAI,
    TABELA_HISTORICO_PROCESSO_PAI,
    query_table,
)


def consultar_historico_processo(
    processo: str,
    gl_nome: str,
    mes: Optional[int] = None,
    ano: Optional[int] = None,
) -> Optional[HistoricoComissao]:
    filtros = {
        "processo": str(processo).strip().upper(),
        "nome": str(gl_nome).strip(),
        "tipo": "recebimento",
    }
    if mes is not None:
        filtros["mes_apuracao"] = int(mes)
    if ano is not None:
        filtros["ano_apuracao"] = int(ano)

    rows = query_table(TABELA_HISTORICO_COMISSOES, filtros=filtros)
    if not rows:
        return None

    row = max(
        rows,
        key=lambda r: (
            int(r.get("ano_apuracao", 0)),
            int(r.get("mes_apuracao", 0)),
            r.get("created_at") or "",
        ),
    )
    return _row_to_historico_comissao(row)


def consultar_todos_historicos_processo(processo: str) -> List[HistoricoComissao]:
    rows = query_table(
        TABELA_HISTORICO_COMISSOES,
        filtros={"processo": str(processo).strip().upper(), "tipo": "recebimento"},
    )
    historicos = [_row_to_historico_comissao(r) for r in rows]
    historicos.sort(key=lambda h: (h.ano_apuracao, h.mes_apuracao, h.created_at or datetime.min))
    return historicos


def consultar_todos_historicos_por_gl(gl_nome: str) -> List[HistoricoComissao]:
    """Retorna todo o historico de comissoes de recebimento da GL, todos os meses."""
    rows = query_table(
        TABELA_HISTORICO_COMISSOES,
        filtros={"nome": str(gl_nome).strip(), "tipo": "recebimento"},
    )
    historicos = [_row_to_historico_comissao(r) for r in rows]
    historicos.sort(
        key=lambda h: (-h.ano_apuracao, -h.mes_apuracao, h.processo, h.documento),
    )
    return historicos


def consultar_historicos_do_pai(
    numero_pc: str,
    codigo_cliente: str,
) -> List[HistoricoComissao]:
    rows = query_table(
        TABELA_HISTORICO_COMISSOES,
        filtros={
            "numero_pc": str(numero_pc).strip().upper(),
            "codigo_cliente": str(codigo_cliente).strip().upper(),
            "tipo": "recebimento",
        },
    )
    historicos = [_row_to_historico_comissao(r) for r in rows]
    historicos.sort(key=lambda h: (h.ano_apuracao, h.mes_apuracao, h.processo, h.nome, h.documento))
    return historicos


def consultar_historicos_pendentes_reconciliacao(
    processo: str,
    gl_nome: Optional[str] = None,
) -> List[HistoricoComissao]:
    historicos = consultar_todos_historicos_processo(processo)
    pendentes = [
        item
        for item in historicos
        if not item.reconciliado
    ]
    if gl_nome is not None:
        gl_normalizado = str(gl_nome).strip()
        pendentes = [item for item in pendentes if item.nome == gl_normalizado]
    return pendentes


def consultar_vinculos_processo_pai(
    numero_pc: str,
    codigo_cliente: str,
    mes: Optional[int] = None,
    ano: Optional[int] = None,
) -> List[HistoricoProcessoPai]:
    filtros = {
        "numero_pc": str(numero_pc).strip().upper(),
        "codigo_cliente": str(codigo_cliente).strip().upper(),
    }
    if mes is not None:
        filtros["mes_referencia"] = int(mes)
    if ano is not None:
        filtros["ano_referencia"] = int(ano)
    rows = query_table(TABELA_HISTORICO_PROCESSO_PAI, filtros=filtros)
    itens = [_row_to_historico_processo_pai(r) for r in rows]
    itens.sort(key=lambda h: (h.ano_referencia, h.mes_referencia, not h.is_processo_pai, h.processo))
    return itens


def consultar_pagamentos_processo_pai(
    numero_pc: str,
    codigo_cliente: str,
    mes: Optional[int] = None,
    ano: Optional[int] = None,
) -> List[HistoricoPagamentoProcessoPai]:
    filtros = {
        "numero_pc": str(numero_pc).strip().upper(),
        "codigo_cliente": str(codigo_cliente).strip().upper(),
    }
    if mes is not None:
        filtros["mes_referencia"] = int(mes)
    if ano is not None:
        filtros["ano_referencia"] = int(ano)
    rows = query_table(TABELA_HISTORICO_PAGAMENTOS_PAI, filtros=filtros)
    itens = [_row_to_historico_pagamento_pai(r) for r in rows]
    itens.sort(key=lambda h: (h.ano_referencia, h.mes_referencia, h.documento))
    return itens


def _parse_datetime(raw_value: object) -> Optional[datetime]:
    if raw_value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _row_to_historico_comissao(row: dict) -> HistoricoComissao:
    return HistoricoComissao(
        nome=str(row.get("nome", row.get("gl_nome", ""))).strip(),
        cargo=str(row.get("cargo", "")).strip(),
        processo=str(row.get("processo", "")).strip().upper(),
        numero_pc=str(row.get("numero_pc", "")).strip().upper(),
        codigo_cliente=str(row.get("codigo_cliente", "")).strip().upper(),
        tipo=str(row.get("tipo", "recebimento")).strip().lower(),
        tipo_pagamento=str(row.get("tipo_pagamento", "")).strip().upper(),
        documento=str(row.get("documento", "")).strip().upper(),
        nf_extraida=str(row.get("nf_extraida", "")).strip().upper(),
        linha_negocio=str(row.get("linha_negocio", "")).strip(),
        status_processo=str(row.get("status_processo", "")).strip(),
        mes_apuracao=int(row.get("mes_apuracao", row.get("mes", 0))),
        ano_apuracao=int(row.get("ano_apuracao", row.get("ano", 0))),
        valor_documento=float(row.get("valor_documento", 0.0)),
        valor_processo=float(row.get("valor_processo", row.get("valor_faturado", 0.0))),
        tcmp=float(row.get("tcmp", 0.0)),
        fcmp_rampa=float(row.get("fcmp_rampa", 0.0)),
        fcmp_aplicado=float(row.get("fcmp_aplicado", 0.0)),
        fcmp_considerado=float(row.get("fcmp_considerado", 1.0)),
        fcmp_modo=str(row.get("fcmp_modo", "")).strip(),
        comissao_potencial=float(row.get("comissao_potencial", 0.0)),
        comissao_adiantada=float(row.get("comissao_adiantada", 0.0)),
        comissao_total=float(row.get("comissao_total", 0.0)),
        status_faturamento_completo=bool(row.get("status_faturamento_completo", False)),
        status_pagamento_completo=row.get("status_pagamento_completo"),
        reconciliado=bool(row.get("reconciliado", False)),
        ac_snapshot_json=str(row.get("ac_snapshot_json", "") or ""),
        af_snapshot_json=str(row.get("af_snapshot_json", "") or ""),
        tcmp_detalhes_json=str(row.get("tcmp_detalhes_json", "") or ""),
        fcmp_detalhes_json=str(row.get("fcmp_detalhes_json", "") or ""),
        created_at=_parse_datetime(row.get("created_at")),
    )


def _row_to_historico_processo_pai(row: dict) -> HistoricoProcessoPai:
    return HistoricoProcessoPai(
        numero_pc=str(row.get("numero_pc", "")).strip().upper(),
        codigo_cliente=str(row.get("codigo_cliente", "")).strip().upper(),
        processo=str(row.get("processo", "")).strip().upper(),
        is_processo_pai=bool(row.get("is_processo_pai", False)),
        status_faturado=bool(row.get("status_faturado", False)),
        status_pago=row.get("status_pago"),
        mes_referencia=int(row.get("mes_referencia", 0)),
        ano_referencia=int(row.get("ano_referencia", 0)),
        created_at=_parse_datetime(row.get("created_at")),
    )


def _row_to_historico_pagamento_pai(row: dict) -> HistoricoPagamentoProcessoPai:
    return HistoricoPagamentoProcessoPai(
        numero_pc=str(row.get("numero_pc", "")).strip().upper(),
        codigo_cliente=str(row.get("codigo_cliente", "")).strip().upper(),
        processo=str(row.get("processo", "")).strip().upper(),
        numero_nf=str(row.get("numero_nf", "")).strip().upper(),
        documento=str(row.get("documento", "")).strip().upper(),
        situacao_codigo=int(row.get("situacao_codigo", -1)),
        situacao_texto=str(row.get("situacao_texto", "")).strip(),
        dt_prorrogacao=_parse_datetime(row.get("dt_prorrogacao")),
        data_baixa=_parse_datetime(row.get("data_baixa")),
        valor_documento=float(row.get("valor_documento", 0.0)),
        mes_referencia=int(row.get("mes_referencia", 0)),
        ano_referencia=int(row.get("ano_referencia", 0)),
        created_at=_parse_datetime(row.get("created_at")),
    )
