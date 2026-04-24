"""
receita/calculadores/calcular_estornos.py — Estornos proporcionais por devoluções.

Quando um processo tem itens devolvidos, a comissão já calculada deve ser
parcialmente estornada proporcionalmente ao valor devolvido.

Fórmula:
    estorno = comissao_gl_processo × (valor_devolvido / valor_processo) × (-1)

O resultado é sempre negativo (débito).

API pública
-----------
executar(df_devolucoes, df_ac_full, comissao_result) → EstornosResult
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional

import pandas as pd

from receita.schemas.calculo import (
    ComissaoResult,
    EstornoItem,
    EstornosResult,
)


def executar(
    df_devolucoes: Optional[pd.DataFrame],
    df_ac_full: pd.DataFrame,
    comissao_result: ComissaoResult,
) -> EstornosResult:
    """Calcula estornos proporcionais por devoluções.

    Args:
        df_devolucoes: DataFrame de devoluções. Pode ser None ou vazio.
            Deve conter: "Processo" (ou "Num docorigem" para lookup),
            "Valor Realizado" (valor devolvido), "Numero NF".
        df_ac_full: DataFrame da AC completo para obter valor total por processo.
            Deve conter "Processo" e "Valor Realizado".
        comissao_result: Resultado do cálculo de comissões para obter
            comissão total por (processo, gl_nome).

    Returns:
        EstornosResult com itens de estorno e totais por GL.
    """
    warnings: List[str] = []
    itens: List[EstornoItem] = []
    total_por_gl: Dict[str, float] = defaultdict(float)

    if df_devolucoes is None or df_devolucoes.empty:
        return EstornosResult(
            itens=[],
            total_por_gl={},
            warnings=["calcular_estornos: sem devoluções — nenhum estorno calculado."],
        )

    # --- Valor total realizado por processo (da AC) ----------------------------
    valor_por_processo: Dict[str, float] = {}
    if "Processo" in df_ac_full.columns and "Valor Realizado" in df_ac_full.columns:
        ac = df_ac_full.copy()
        ac["Valor Realizado"] = pd.to_numeric(ac["Valor Realizado"], errors="coerce").fillna(0.0)
        valor_por_processo = (
            ac.groupby("Processo")["Valor Realizado"].sum().to_dict()
        )

    # --- Comissão total por (processo, gl_nome) --------------------------------
    comissao_por_processo_gl: Dict[tuple, float] = defaultdict(float)
    for item in comissao_result.itens:
        chave = (item.processo, item.gl_nome)
        comissao_por_processo_gl[chave] += item.comissao_final

    # --- GLs únicos do resultado de comissão ----------------------------------
    gls_ativos = {item.gl_nome for item in comissao_result.itens}

    # --- Processar cada devolução ---------------------------------------------
    # Identificar coluna de processo direto (prioritária) e de NF (fallback).
    # "Num docorigem" é o Numero NF da NF original da devolução — NÃO é processo.
    proc_col: Optional[str] = None
    for candidato in ["Processo", "processo"]:
        if candidato in df_devolucoes.columns:
            proc_col = candidato
            break

    nf_col: Optional[str] = None
    for candidato in ["Num docorigem", "Num Docorigem", "Numero NF", "NF", "Num. NF"]:
        if candidato in df_devolucoes.columns:
            nf_col = candidato
            break

    if proc_col is None and nf_col is None:
        warnings.append(
            "calcular_estornos: sem coluna 'Processo' nem 'Num docorigem'/'Numero NF' nas devoluções — sem estornos."
        )
        return EstornosResult(itens=[], total_por_gl={}, warnings=warnings)

    valor_dev_col: Optional[str] = None
    for candidato in ["Valor Produtos", "Valor Realizado", "Valor Devolvido", "Valor"]:
        if candidato in df_devolucoes.columns:
            valor_dev_col = candidato
            break

    # Mapa NF → Processo (a partir da AC) para resolver quando só há NF na devolução.
    processo_por_nf: Dict[str, str] = {}
    if "Numero NF" in df_ac_full.columns and "Processo" in df_ac_full.columns:
        ac_rel = df_ac_full[["Processo", "Numero NF"]].dropna()
        for _, r in ac_rel.iterrows():
            nf_key = str(r.get("Numero NF", "") or "").strip().upper()
            proc_val = str(r.get("Processo", "") or "").strip()
            if nf_key and nf_key not in {"NAN", "NONE", ""} and proc_val:
                processo_por_nf.setdefault(nf_key, proc_val)

    devs = df_devolucoes.copy()
    if valor_dev_col:
        devs[valor_dev_col] = pd.to_numeric(devs[valor_dev_col], errors="coerce").fillna(0.0)

    for _, row in devs.iterrows():
        processo = str(row.get(proc_col, "") or "").strip() if proc_col else ""
        nf_origem = str(row.get(nf_col, "") or "").strip() if nf_col else ""
        if not processo and nf_origem:
            processo = processo_por_nf.get(nf_origem.upper(), "")

        if not processo:
            if nf_origem:
                warnings.append(
                    f"calcular_estornos: NF '{nf_origem}' sem processo correspondente na AC — estorno ignorado."
                )
            continue

        valor_devolvido = float(row.get(valor_dev_col, 0.0) or 0.0) if valor_dev_col else 0.0
        if valor_devolvido == 0.0:
            continue

        valor_processo = valor_por_processo.get(processo, 0.0)

        if valor_processo == 0.0:
            warnings.append(
                f"calcular_estornos: processo '{processo}' sem valor realizado na AC — estorno ignorado."
            )
            continue

        proporcao = min(abs(valor_devolvido) / valor_processo, 1.0)

        for gl_nome in gls_ativos:
            comissao_base = comissao_por_processo_gl.get((processo, gl_nome), 0.0)
            if comissao_base == 0.0:
                continue

            estorno = comissao_base * proporcao * (-1.0)
            item = EstornoItem(
                gl_nome=gl_nome,
                processo=processo,
                nf_origem=nf_origem,
                valor_devolvido=abs(valor_devolvido),
                valor_processo=valor_processo,
                comissao_base=comissao_base,
                estorno=estorno,
            )
            itens.append(item)
            total_por_gl[gl_nome] += estorno

    warnings.append(
        f"calcular_estornos: {len(itens)} estorno(s) calculados para "
        f"{len(total_por_gl)} GL(s)."
    )

    return EstornosResult(
        itens=itens,
        total_por_gl=dict(total_por_gl),
        warnings=warnings,
    )
