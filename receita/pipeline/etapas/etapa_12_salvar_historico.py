"""
Persistencia do historico de recebimentos no Supabase.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from receita.schemas.entrada import ProcessoPedidoItem, ProcessoPedidoTabela
from receita.schemas.historico import (
    HistoricoComissao,
    HistoricoPagamentoProcessoPai,
    HistoricoProcessoPai,
)
from receita.supabase import registrar_faturamento
from receita.supabase.client import TABELA_HISTORICO_COMISSOES, query_table
from receita.supabase.schema_manager import ensure_required_tables


COL_SITUACAO = "Situa\u00e7\u00e3o"
COL_DT_PRORROGACAO = "Dt. Prorroga\u00e7\u00e3o"
COL_VALOR_LIQUIDO = "Valor L\u00edquido"

_PREFIXOS_ADIANTAMENTO = ("COT", "ADT")

_SITUACAO_LABELS = {
    0: "Aberto",
    1: "Recebido",
    2: "Recebido Parcial",
}


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    warnings: List[str] = []
    erros_supabase: List[str] = []
    persistir_historicos = bool(input_data.get("persistir_historicos", True))
    persistir_auxiliares = bool(input_data.get("persistir_auxiliares", True))
    marcar_reconciliados = bool(input_data.get("marcar_reconciliados", True))

    tcmp_por_processo: Dict[str, float] = input_data.get("tcmp_result", {}).get("tcmp_por_processo", {})
    fcmp_por_gl_raw: Dict[str, Dict[str, Any]] = input_data.get("fcmp_por_gl", {})
    comissao_itens_raw: List[Dict[str, Any]] = input_data.get("comissao_result", [])
    reconciliacao_itens_raw: List[Dict[str, Any]] = input_data.get("reconciliacao_result", [])
    status_por_pai: Dict[str, Dict[str, Any]] = input_data.get("status_por_processo_pai", {})
    tabela_pc_json: List[Dict[str, Any]] = input_data.get("tabela_pc_json", [])
    colaboradores_raw: List[Dict[str, Any]] = input_data.get("colaboradores", [])
    mes = int(input_data.get("mes", 0))
    ano = int(input_data.get("ano", 0))

    missing_tables = ensure_required_tables(strict=False)
    if missing_tables:
        warnings.append(
            "etapa_12: schema historico ausente ou incompleto no Supabase. "
            f"Tabelas faltantes: {', '.join(missing_tables)}. "
            "Aplique receita/supabase/schema_historico.sql."
        )
        return {
            "status": "warning",
            "registros_salvos": 0,
            "erros_supabase": [],
            "warnings": warnings,
            "errors": [],
        }

    tabela_pc = ProcessoPedidoTabela(
        registros=[
            ProcessoPedidoItem(
                numero_processo=item["numero_processo"],
                numero_pc=item["numero_pc"],
                codigo_cliente=item["codigo_cliente"],
            )
            for item in tabela_pc_json
            if item.get("numero_processo") and item.get("numero_pc") and item.get("codigo_cliente")
        ]
    )
    df_ac_full = _ensure_dataframe(input_data.get("df_ac_full"))
    df_af_apuracao = _ensure_dataframe(input_data.get("df_af_apuracao"))
    df_af_full = _ensure_dataframe(input_data.get("df_af_full"))
    tcmp_detalhes_raw: Dict[str, List[Dict[str, Any]]] = input_data.get("tcmp_result", {}).get("detalhes", {})

    cargo_por_gl: Dict[str, str] = {
        str(c.get("nome_colaborador", "")).strip(): str(c.get("cargo", "")).strip()
        for c in colaboradores_raw
        if c.get("nome_colaborador")
    }

    status_por_processo: Dict[str, Dict[str, Any]] = {}
    for status in status_por_pai.values():
        faturados = {str(p).strip().upper() for p in status.get("processos_faturados", [])}
        pendentes = {str(p).strip().upper() for p in status.get("processos_pendentes", [])}
        for processo in faturados | pendentes:
            status_por_processo[processo] = status

    registros_salvos = 0
    if persistir_historicos:
        historicos = _montar_historico_comissoes(
            comissao_itens_raw=comissao_itens_raw,
            cargo_por_gl=cargo_por_gl,
            tabela_pc=tabela_pc,
            tcmp_por_processo=tcmp_por_processo,
            tcmp_detalhes_raw=tcmp_detalhes_raw,
            fcmp_por_gl_raw=fcmp_por_gl_raw,
            status_por_processo=status_por_processo,
            mes=mes,
            ano=ano,
            df_ac_full=df_ac_full,
            df_af_apuracao=df_af_apuracao,
            df_af_full=df_af_full,
        )
        if historicos:
            resultado = registrar_faturamento.registrar_varios(historicos)
            registros_salvos = resultado["ok"]
            erros_supabase.extend(resultado["erros"])

    if persistir_auxiliares:
        vinculos_pai = _montar_historico_processos_pai(status_por_pai, tabela_pc, mes, ano)
        if vinculos_pai:
            resultado = registrar_faturamento.registrar_historico_processos_pai(vinculos_pai)
            erros_supabase.extend(resultado["erros"])
            warnings.append(f"etapa_12: {resultado['ok']} registro(s) salvos em historico_processo_pai.")

        pagamentos_pai = _montar_historico_pagamentos_pai(status_por_pai, tabela_pc, df_ac_full, df_af_apuracao, mes, ano)
        if pagamentos_pai:
            resultado = registrar_faturamento.registrar_historico_pagamentos_pai(pagamentos_pai)
            erros_supabase.extend(resultado["erros"])
            warnings.append(
                f"etapa_12: {resultado['ok']} registro(s) salvos em historico_pagamentos_processo_pai."
            )

    reconciliados = 0
    if marcar_reconciliados:
        for item in reconciliacao_itens_raw:
            try:
                registrar_faturamento.marcar_historicos_reconciliados(
                    processo="",
                    gl_nome=str(item.get("gl_nome", "")).strip(),
                    numero_pc=str(item.get("numero_pc", "")).strip().upper(),
                    codigo_cliente=str(item.get("codigo_cliente", "")).strip().upper(),
                )
                reconciliados += 1
            except Exception as exc:
                erros_supabase.append(
                    "Falha ao marcar historico reconciliado "
                    f"({item.get('processo')}/{item.get('gl_nome')}): {exc}"
                )
    if reconciliados:
        warnings.append(f"etapa_12: {reconciliados} grupo(s) historicos marcados como reconciliados.")

    if erros_supabase:
        warnings.append(f"etapa_12: {len(erros_supabase)} erro(s) Supabase.")

    if persistir_historicos:
        warnings.append(f"etapa_12: {registros_salvos} registro(s) salvos em historico_comissoes.")

    return {
        "status": "ok",
        "registros_salvos": registros_salvos,
        "erros_supabase": erros_supabase,
        "warnings": warnings,
        "errors": [],
    }


def _ensure_dataframe(value: Any) -> pd.DataFrame:
    if value is None:
        return pd.DataFrame()
    if isinstance(value, pd.DataFrame):
        return value.copy()
    if isinstance(value, list):
        return pd.DataFrame(value)
    if isinstance(value, dict):
        return pd.DataFrame(value.get("rows", []))
    return pd.DataFrame()


def _consultar_reconciliado_existente(
    processo: str,
    gl_nome: str,
    documento: str,
    tipo_pagamento: str,
    mes: int,
    ano: int,
) -> bool:
    try:
        rows = query_table(
            TABELA_HISTORICO_COMISSOES,
            filtros={
                "processo": str(processo).strip().upper(),
                "nome": str(gl_nome).strip(),
                "documento": str(documento).strip().upper(),
                "tipo_pagamento": str(tipo_pagamento).strip().upper(),
                "tipo": "recebimento",
                "mes_apuracao": int(mes),
                "ano_apuracao": int(ano),
            },
        )
    except Exception:
        return False
    return any(bool(row.get("reconciliado", False)) for row in rows)


def _montar_historico_comissoes(
    comissao_itens_raw: List[Dict[str, Any]],
    cargo_por_gl: Dict[str, str],
    tabela_pc: ProcessoPedidoTabela,
    tcmp_por_processo: Dict[str, float],
    tcmp_detalhes_raw: Dict[str, List[Dict[str, Any]]],
    fcmp_por_gl_raw: Dict[str, Dict[str, Any]],
    status_por_processo: Dict[str, Dict[str, Any]],
    mes: int,
    ano: int,
    df_ac_full: pd.DataFrame,
    df_af_apuracao: pd.DataFrame,
    df_af_full: pd.DataFrame,
) -> List[HistoricoComissao]:
    historicos: List[HistoricoComissao] = []
    ac_cache: Dict[str, str] = {}
    af_cache: Dict[str, str] = {}

    for item in comissao_itens_raw:
        gl_nome = str(item.get("gl_nome", "")).strip()
        processo = str(item.get("processo", "")).strip().upper()
        documento = str(item.get("documento", "")).strip().upper()
        tipo_pagamento = str(item.get("tipo_pagamento", "")).strip().upper() or "REGULAR"
        if not gl_nome or not processo or not documento:
            continue

        numero_pc, codigo_cliente = tabela_pc.get_pai(processo) or ("", "")
        status_processo = status_por_processo.get(processo, {})
        fcmp_proc = (
            fcmp_por_gl_raw.get(gl_nome, {})
            .get("fcmp_por_processo", {})
            .get(processo, {})
        )
        valor_processo = float(fcmp_proc.get("valor_faturado", item.get("valor_documento", 0.0)) or 0.0)
        ac_snapshot_json = ac_cache.setdefault(processo, _serializar_ac_snapshot(df_ac_full, processo))
        af_snapshot_json = af_cache.setdefault(
            documento,
            _serializar_af_snapshot(df_af_apuracao if not df_af_apuracao.empty else df_af_full, documento),
        )
        historicos.append(
            HistoricoComissao(
                nome=gl_nome,
                cargo=cargo_por_gl.get(gl_nome, ""),
                processo=processo,
                numero_pc=numero_pc,
                codigo_cliente=codigo_cliente,
                tipo="recebimento",
                tipo_pagamento=tipo_pagamento,
                documento=documento,
                nf_extraida=str(item.get("nf_extraida", "")).strip().upper(),
                linha_negocio=str(item.get("linha_negocio", "")).strip(),
                status_processo=str(item.get("status_processo", "")).strip(),
                mes_apuracao=mes,
                ano_apuracao=ano,
                valor_documento=float(item.get("valor_documento", 0.0) or 0.0),
                valor_processo=valor_processo,
                tcmp=float(item.get("tcmp", tcmp_por_processo.get(processo, 0.0)) or 0.0),
                fcmp_rampa=float(item.get("fcmp_rampa", fcmp_proc.get("fcmp_rampa", 1.0)) or 1.0),
                fcmp_aplicado=float(item.get("fcmp_aplicado", fcmp_proc.get("fcmp_aplicado", 1.0)) or 1.0),
                fcmp_considerado=float(item.get("fcmp_considerado", 1.0) or 1.0),
                fcmp_modo=str(item.get("fcmp_modo", fcmp_proc.get("modo", "")) or ""),
                comissao_potencial=float(item.get("comissao_potencial", 0.0) or 0.0),
                comissao_adiantada=float(item.get("comissao_base", item.get("comissao_final", 0.0)) or 0.0),
                comissao_total=float(item.get("comissao_final", 0.0) or 0.0),
                status_faturamento_completo=bool(status_processo.get("status_faturamento_completo", False)),
                status_pagamento_completo=status_processo.get("status_pagamento_completo", None),
                reconciliado=_consultar_reconciliado_existente(
                    processo=processo,
                    gl_nome=gl_nome,
                    documento=documento,
                    tipo_pagamento=tipo_pagamento,
                    mes=mes,
                    ano=ano,
                ),
                ac_snapshot_json=ac_snapshot_json,
                af_snapshot_json=af_snapshot_json,
                tcmp_detalhes_json=_serializar_json(tcmp_detalhes_raw.get(processo, [])),
                fcmp_detalhes_json=_serializar_json(
                    fcmp_por_gl_raw.get(gl_nome, {}).get("detalhes", {}).get(processo, [])
                ),
            )
        )

    return historicos


def _serializar_ac_snapshot(df_ac_full: pd.DataFrame, processo: str) -> str:
    if df_ac_full.empty or "Processo" not in df_ac_full.columns:
        return "[]"
    cols = [
        "Processo",
        "Numero NF",
        "Status Processo",
        "Dt Emissão",
        "Linha",
        "Grupo",
        "Subgrupo",
        "Tipo de Mercadoria",
        "Fabricante",
        "Aplicação Mat./Serv.",
        "Valor Realizado",
        "Valor Orçado",
    ]
    df = df_ac_full.loc[
        df_ac_full["Processo"].astype(str).str.strip().str.upper() == str(processo).strip().upper(),
        [col for col in cols if col in df_ac_full.columns],
    ].copy()
    return _serializar_df(df)


def _serializar_af_snapshot(df_af: pd.DataFrame, documento: str) -> str:
    if df_af.empty or "Documento" not in df_af.columns:
        return "[]"
    cols = [
        "Documento",
        "Numero NF",
        "NF",
        "Num. NF",
        "Número NF",
        "Valor Líquido",
        "Situação",
        "Data de Baixa",
        "Dt. Prorrogação",
    ]
    df = df_af.loc[
        df_af["Documento"].astype(str).str.strip().str.upper() == str(documento).strip().upper(),
        [col for col in cols if col in df_af.columns],
    ].copy()
    return _serializar_df(df)


def _serializar_df(df: pd.DataFrame) -> str:
    if df.empty:
        return "[]"
    return _serializar_json(df.where(pd.notna(df), None).to_dict(orient="records"))


def _serializar_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _montar_historico_processos_pai(
    status_por_pai: Dict[str, Dict[str, Any]],
    tabela_pc: ProcessoPedidoTabela,
    mes: int,
    ano: int,
) -> List[HistoricoProcessoPai]:
    itens: List[HistoricoProcessoPai] = []

    for status in status_por_pai.values():
        numero_pc = str(status.get("numero_pc", "")).strip().upper()
        codigo_cliente = str(status.get("codigo_cliente", "")).strip().upper()
        if not numero_pc or not codigo_cliente:
            continue

        filhos_tabela = {
            str(p).strip().upper()
            for p in tabela_pc.get_processos_do_pai(numero_pc, codigo_cliente)
            if str(p).strip()
        }
        faturados = {
            str(p).strip().upper()
            for p in status.get("processos_faturados", [])
            if str(p).strip()
        }
        pendentes = {
            str(p).strip().upper()
            for p in status.get("processos_pendentes", [])
            if str(p).strip()
        }
        processos_relacionados = sorted(filhos_tabela | faturados | pendentes)
        status_pago = status.get("status_pagamento_completo", None)

        itens.append(
            HistoricoProcessoPai(
                numero_pc=numero_pc,
                codigo_cliente=codigo_cliente,
                processo=numero_pc,
                is_processo_pai=True,
                status_faturado=bool(status.get("status_faturamento_completo", False)),
                status_pago=status_pago,
                mes_referencia=mes,
                ano_referencia=ano,
            )
        )

        for processo in processos_relacionados:
            if processo == numero_pc:
                continue
            itens.append(
                HistoricoProcessoPai(
                    numero_pc=numero_pc,
                    codigo_cliente=codigo_cliente,
                    processo=processo,
                    is_processo_pai=False,
                    status_faturado=processo in faturados,
                    status_pago=status_pago,
                    mes_referencia=mes,
                    ano_referencia=ano,
                )
            )

    return itens


def _montar_historico_pagamentos_pai(
    status_por_pai: Dict[str, Dict[str, Any]],
    tabela_pc: ProcessoPedidoTabela,
    df_ac_full: pd.DataFrame,
    df_af_apuracao: pd.DataFrame,
    mes: int,
    ano: int,
) -> List[HistoricoPagamentoProcessoPai]:
    if df_ac_full.empty or df_af_apuracao.empty:
        return []

    if "Processo" not in df_ac_full.columns or "Numero NF" not in df_ac_full.columns:
        return []
    if "Documento" not in df_af_apuracao.columns:
        return []

    itens: List[HistoricoPagamentoProcessoPai] = []

    for status in status_por_pai.values():
        numero_pc = str(status.get("numero_pc", "")).strip().upper()
        codigo_cliente = str(status.get("codigo_cliente", "")).strip().upper()
        if not numero_pc or not codigo_cliente:
            continue

        processos = {
            str(p).strip().upper()
            for p in tabela_pc.get_processos_do_pai(numero_pc, codigo_cliente)
            if str(p).strip()
        }
        if not processos:
            continue

        nf_lookup = _construir_lookup_nf_processo(df_ac_full, processos)
        processo_lookup = _construir_lookup_processo_adiantamento(df_ac_full, processos)
        if not nf_lookup and not processo_lookup:
            continue

        for _, row in df_af_apuracao.iterrows():
            documento = str(row.get("Documento", "") or "").strip()
            if not documento:
                continue

            if _is_adiantamento(documento):
                base_digitos = _normalizar_digitos(documento)
                info_nf = processo_lookup.get(base_digitos)
            else:
                base_digitos = _normalizar_digitos(_extrair_base_documento(documento))
                info_nf = nf_lookup.get(base_digitos)

            if info_nf is None:
                continue

            situacao_codigo = _safe_int(_row_get(row, COL_SITUACAO, "Situacao"), default=-1)
            itens.append(
                HistoricoPagamentoProcessoPai(
                    numero_pc=numero_pc,
                    codigo_cliente=codigo_cliente,
                    processo=info_nf["processo"],
                    numero_nf=info_nf["numero_nf"],
                    documento=documento,
                    situacao_codigo=situacao_codigo,
                    situacao_texto=_SITUACAO_LABELS.get(situacao_codigo, "Desconhecido"),
                    dt_prorrogacao=_coerce_datetime(_row_get(row, COL_DT_PRORROGACAO, "Dt. Prorrogacao")),
                    data_baixa=_coerce_datetime(_row_get(row, "Data de Baixa", "Data Baixa")),
                    valor_documento=_obter_valor_documento(row),
                    mes_referencia=mes,
                    ano_referencia=ano,
                )
            )

    return itens


def _is_adiantamento(documento: str) -> bool:
    doc_upper = str(documento).strip().upper()
    return any(doc_upper.startswith(p) for p in _PREFIXOS_ADIANTAMENTO)


def _construir_lookup_nf_processo(df_ac_full: pd.DataFrame, processos: set) -> Dict[str, Dict[str, str]]:
    mask = df_ac_full["Processo"].astype(str).str.strip().str.upper().isin(processos)
    df_rel = df_ac_full.loc[mask, ["Processo", "Numero NF"]].copy()
    lookup: Dict[str, Dict[str, str]] = {}

    for _, row in df_rel.iterrows():
        processo = str(row.get("Processo", "") or "").strip().upper()
        numero_nf = str(row.get("Numero NF", "") or "").strip().upper()
        nf_digits = _normalizar_digitos(numero_nf)
        if not processo or not numero_nf or not nf_digits:
            continue
        lookup.setdefault(
            nf_digits,
            {
                "processo": processo,
                "numero_nf": numero_nf,
            },
        )

    return lookup


def _construir_lookup_processo_adiantamento(df_ac_full: pd.DataFrame, processos: set) -> Dict[str, Dict[str, str]]:
    """Lookup {processo_digits: {processo, numero_nf}} para match de adiantamentos (COT/ADT).

    Adiantamentos são vinculados ao AC pelo número do Processo, não pela NF.
    O documento AF (ex: 'ADT138047') é comparado via seus dígitos contra os
    dígitos do Processo AC.
    """
    mask = df_ac_full["Processo"].astype(str).str.strip().str.upper().isin(processos)
    df_rel = df_ac_full.loc[mask, ["Processo", "Numero NF"]].copy()
    lookup: Dict[str, Dict[str, str]] = {}

    for _, row in df_rel.iterrows():
        processo = str(row.get("Processo", "") or "").strip().upper()
        numero_nf = str(row.get("Numero NF", "") or "").strip().upper()
        proc_digits = _normalizar_digitos(processo)
        if not processo or not proc_digits:
            continue
        lookup.setdefault(
            proc_digits,
            {
                "processo": processo,
                "numero_nf": numero_nf,
            },
        )

    return lookup


def _extrair_base_documento(documento: str) -> str:
    return re.sub(r"[A-Za-z]+$", "", str(documento).strip())


def _normalizar_digitos(valor: str) -> str:
    digitos = re.sub(r"\D", "", str(valor))
    if not digitos:
        return ""
    return digitos.lstrip("0") or "0"


def _safe_int(valor: Any, default: int = 0) -> int:
    try:
        if pd.isna(valor):
            return default
    except Exception:
        pass
    try:
        return int(valor)
    except Exception:
        return default


def _coerce_datetime(valor: Any) -> Optional[datetime]:
    if valor is None:
        return None
    try:
        if pd.isna(valor):
            return None
    except Exception:
        pass
    if isinstance(valor, datetime):
        return valor
    if isinstance(valor, pd.Timestamp):
        return valor.to_pydatetime()
    try:
        convertido = pd.to_datetime(valor, errors="coerce")
    except Exception:
        return None
    if pd.isna(convertido):
        return None
    if isinstance(convertido, pd.Timestamp):
        return convertido.to_pydatetime()
    return convertido


def _obter_valor_documento(row: pd.Series) -> float:
    valor = _row_get(row, COL_VALOR_LIQUIDO, "Valor Liquido", "Valor", "Valor Documento")
    try:
        if pd.isna(valor):
            return 0.0
    except Exception:
        pass
    try:
        return float(valor)
    except Exception:
        return 0.0


def _row_get(row: pd.Series, *colunas: str) -> Any:
    for coluna in colunas:
        if coluna in row.index:
            return row.get(coluna)
    return None


if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    result = run(payload)
    print(json.dumps(result, ensure_ascii=False, default=str))
