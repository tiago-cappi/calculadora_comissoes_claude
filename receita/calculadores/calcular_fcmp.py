"""
receita/calculadores/calcular_fcmp.py — FCMP por processo por GL.

FCMP = Fator de Comissão Médio Ponderado por processo.

Para cada processo FATURADO (Status Processo == "FATURADO"):
    FCMP_rampa = Σ(FC_item × valor_item) / Σ(valor_item)
    FC_item calculado via scripts.fc_calculator.calcular_fc_item()

Para processos não-FATURADO:
    FCMP = 1.0, modo = "PROVISÓRIO"

Após cálculo do FCMP_rampa, aplica escada (se configurada para o cargo do GL)
usando scripts.fc_calculator.gerar_degraus_escada().

API pública
-----------
executar(df_ac_full, realizados_result, gl, pesos_metas, fc_escada, params) → FCMPResult
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from receita.schemas.calculo import ElegivelGL, FCMPProcesso, FCMPResult

_STATUS_FATURADO = "FATURADO"
_HIERARQUIA_COLS = [
    "Linha", "Grupo", "Subgrupo", "Tipo de Mercadoria", "Fabricante", "Aplicação Mat./Serv."
]


def _hierarquia_row(row: pd.Series) -> Tuple[str, ...]:
    """Extrai tupla de hierarquia de 6 campos de uma linha da AC."""
    return tuple(str(row.get(col, "") or "").strip() for col in _HIERARQUIA_COLS)


def _safe_to_numeric(series: pd.Series) -> pd.Series:
    """Converte números em formato BR/Excel para float."""
    if pd.api.types.is_numeric_dtype(series):
        return series.fillna(0.0)
    s = series.astype(str).str.strip()
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def executar(
    df_ac_full: pd.DataFrame,
    realizados_result: Any,
    gl: ElegivelGL,
    pesos_metas: Dict[Tuple[str, str], Dict],
    fc_escada: Dict[str, Dict],
    params: Dict[str, Any],
) -> FCMPResult:
    """Calcula FCMP para todos os processos do GL informado.

    Reutiliza `scripts.fc_calculator.calcular_fc_item` exatamente como o
    pipeline de faturamento, mas calcula a média ponderada por processo
    em vez de por item individual.

    Args:
        df_ac_full: DataFrame da Análise Comercial SEM filtro de mês.
            Deve conter "Processo", "Status Processo", "Valor Realizado"
            e colunas de hierarquia.
        realizados_result: RealizadosResult do pipeline de faturamento
            (fornece realizados e metas para o FC).
        gl: ElegivelGL com nome, cargo e linha do Gerente de Linha.
        pesos_metas: {(cargo, colaborador): {componente: peso_pct}}.
            Indexado para uso direto por calcular_fc_item.
        fc_escada: {cargo_lower: {modo, num_degraus, piso_pct, degraus_intermediarios}}.
        params: {cap_fc_max, cap_atingimento_max, ...}.

    Returns:
        FCMPResult para o GL, com fcmp_por_processo e detalhes de auditoria.
    """
    from scripts.fc_calculator import _aplicar_escada, calcular_fc_item, gerar_degraus_escada

    warnings: List[str] = []
    fcmp_por_processo: Dict[str, FCMPProcesso] = {}
    detalhes: Dict[str, List[Dict]] = {}

    if df_ac_full is None or df_ac_full.empty:
        return FCMPResult(
            gl_nome=gl.nome,
            fcmp_por_processo={},
            detalhes={},
            warnings=["calcular_fcmp: df_ac_full vazio."],
        )

    # Garantir coluna numérica
    df = df_ac_full.copy()
    if "Valor Realizado" in df.columns:
        df["Valor Realizado"] = _safe_to_numeric(df["Valor Realizado"])
    else:
        df["Valor Realizado"] = 0.0

    # Configuração da escada para o cargo do GL.
    # `fc_escada` é indexado por tupla (cargo_lower, colab_lower) em
    # scripts.fc_calculator._load_config. Tentamos a chave específica do
    # colaborador primeiro e caímos para a regra genérica do cargo.
    # Compatibilidade retroativa: também aceitamos chave string (cargo_lower).
    cargo_lower = gl.cargo.lower().strip()
    colab_lower = gl.nome.lower().strip()
    escada_config = (
        fc_escada.get((cargo_lower, colab_lower))
        or fc_escada.get((cargo_lower, ""))
        or fc_escada.get(cargo_lower)
        or {}
    )

    for processo, grupo in df.groupby("Processo", sort=False):
        processo_str = str(processo)
        # Verificar se o processo está FATURADO
        statuses = grupo["Status Processo"].astype(str).str.strip().str.upper().unique()
        is_faturado = _STATUS_FATURADO in statuses

        if not is_faturado:
            # Processo não-FATURADO → FCMP provisório
            fcmp_por_processo[processo_str] = FCMPProcesso(
                processo=processo_str,
                gl_nome=gl.nome,
                fcmp_rampa=1.0,
                fcmp_aplicado=1.0,
                modo="PROVISÓRIO",
                provisorio=True,
                num_itens=len(grupo),
                valor_faturado=float(grupo["Valor Realizado"].sum()),
            )
            continue

        # Calcular FCMP_rampa via média ponderada de FC_item
        soma_fc_valor = 0.0
        soma_valor = 0.0
        itens_detalhe: List[Dict] = []

        for idx, row in grupo.iterrows():
            valor = float(row.get("Valor Realizado", 0.0) or 0.0)
            if valor == 0.0:
                continue

            hierarquia = _hierarquia_row(row)
            fc_item_detail: Dict[str, Any] = {}

            try:
                fc_result = calcular_fc_item(
                    colaborador=gl.nome,
                    cargo=gl.cargo,
                    hierarquia_item=hierarquia,
                    realizados_result=realizados_result,
                    pesos_indexed=pesos_metas,
                    escada_por_cargo=fc_escada,
                    params=params,
                )
                fc_item = float(fc_result.fc_rampa if hasattr(fc_result, "fc_rampa") else 1.0)
                componentes = []
                for componente in getattr(fc_result, "componentes", []) or []:
                    componentes.append({
                        "nome": str(getattr(componente, "nome", "")),
                        "peso": float(getattr(componente, "peso", 0.0) or 0.0),
                        "realizado": float(getattr(componente, "realizado", 0.0) or 0.0),
                        "meta": float(getattr(componente, "meta", 0.0) or 0.0),
                        "atingimento": float(getattr(componente, "atingimento", 0.0) or 0.0),
                        "atingimento_cap": float(getattr(componente, "atingimento_cap", 0.0) or 0.0),
                        "contribuicao": float(getattr(componente, "contribuicao", 0.0) or 0.0),
                    })
                fc_item_detail = {
                    "colaborador": str(getattr(fc_result, "colaborador", gl.nome)),
                    "cargo": str(getattr(fc_result, "cargo", gl.cargo)),
                    "linha": str(getattr(fc_result, "linha", hierarquia[0] if hierarquia else "")),
                    "hierarquia_key": str(getattr(fc_result, "hierarquia_key", "/".join([h for h in hierarquia if h]))),
                    "fc_rampa": float(getattr(fc_result, "fc_rampa", fc_item) or fc_item),
                    "fc_final": float(getattr(fc_result, "fc_final", fc_item) or fc_item),
                    "modo": str(getattr(fc_result, "modo", "RAMPA")),
                    "escada_num_degraus": getattr(fc_result, "escada_num_degraus", None),
                    "escada_piso": getattr(fc_result, "escada_piso", None),
                    "escada_degrau_indice": getattr(fc_result, "escada_degrau_indice", None),
                    "componentes": componentes,
                }
            except Exception as exc:
                warnings.append(
                    f"calcular_fcmp: erro em calcular_fc_item para processo {processo_str}, "
                    f"GL {gl.nome}: {exc}. FC_item = 1.0."
                )
                fc_item = 1.0
                fc_item_detail = {
                    "colaborador": gl.nome,
                    "cargo": gl.cargo,
                    "linha": hierarquia[0] if hierarquia else "",
                    "hierarquia_key": "/".join([h for h in hierarquia if h]),
                    "fc_rampa": 1.0,
                    "fc_final": 1.0,
                    "modo": "RAMPA",
                    "escada_num_degraus": None,
                    "escada_piso": None,
                    "escada_degrau_indice": None,
                    "componentes": [],
                }

            soma_fc_valor += fc_item * valor
            soma_valor += valor
            itens_detalhe.append({
                "item_idx": idx,
                "hierarquia": hierarquia,
                "fc_item": fc_item,
                "valor_item": valor,
                "contribuicao": fc_item * valor,
                "fc_item_detail": fc_item_detail,
            })

        if soma_valor == 0.0:
            fcmp_rampa = 1.0
        else:
            fcmp_rampa = soma_fc_valor / soma_valor

        # Aplicar escada ao FCMP_rampa — delega para a mesma lógica usada
        # no pipeline de faturamento para garantir paridade (round-up:
        # menor degrau >= perf). Ver scripts.fc_calculator._aplicar_escada.
        fcmp_aplicado, modo, _n, _piso, _idx = _aplicar_escada(
            fcmp_rampa, gl.cargo, fc_escada, gl.nome
        )

        fcmp_por_processo[processo_str] = FCMPProcesso(
            processo=processo_str,
            gl_nome=gl.nome,
            fcmp_rampa=fcmp_rampa,
            fcmp_aplicado=fcmp_aplicado,
            modo=modo,
            provisorio=False,
            num_itens=len(itens_detalhe),
            valor_faturado=soma_valor,
        )
        detalhes[processo_str] = itens_detalhe

    warnings.append(
        f"calcular_fcmp: {gl.nome} — {len(fcmp_por_processo)} processo(s) calculados."
    )

    return FCMPResult(
        gl_nome=gl.nome,
        fcmp_por_processo=fcmp_por_processo,
        detalhes=detalhes,
        warnings=warnings,
    )
