"""
receita/calculadores/calcular_comissao.py — Comissão por documento AF por GL.

Para cada documento mapeado × GL elegível na linha do processo:

    fcmp_considerado = 1.0 até a reconciliação do Processo Pai
    comissao_base    = valor_doc × tcmp
    comissao_final   = valor_doc × tcmp × fcmp_considerado

Documentos sem processo mapeado ou sem TCMP são ignorados com warning.

API pública
-----------
executar(mapeamento_result, atribuicao_result, tcmp_result, fcmp_por_gl) → ComissaoResult
"""

from __future__ import annotations

import unicodedata
from collections import defaultdict
from typing import Dict, List

import pandas as pd

from receita.schemas.calculo import (
    AtribuicaoResult,
    ComissaoItem,
    ComissaoResult,
    FCMPResult,
    MapeamentoResult,
    TCMPResult,
)


def _normalizar(texto: str) -> str:
    return (
        unicodedata.normalize("NFKD", str(texto).upper())
        .encode("ascii", "ignore")
        .decode()
        .strip()
    )


def executar(
    mapeamento_result: MapeamentoResult,
    atribuicao_result: AtribuicaoResult,
    tcmp_result: TCMPResult,
    fcmp_por_gl: Dict[str, FCMPResult],
) -> ComissaoResult:
    """Calcula comissão por documento AF para cada GL elegível.

    Args:
        mapeamento_result: Resultado do mapeamento AF→AC com df_mapeado.
            O DataFrame deve ter: "Documento", "processo_ac", "linha_negocio",
            "tipo_pagamento", "nf_extraida", "Valor Líquido".
        atribuicao_result: GLs elegíveis por linha.
        tcmp_result: TCMP por processo.
        fcmp_por_gl: {gl_nome: FCMPResult} com FCMP por processo por GL.

    Returns:
        ComissaoResult com itens e total_por_gl.
    """
    warnings: List[str] = []
    itens: List[ComissaoItem] = []
    total_por_gl: Dict[str, float] = defaultdict(float)

    df = mapeamento_result.df_mapeado
    if df is None or df.empty:
        return ComissaoResult(itens=[], total_por_gl={}, warnings=["calcular_comissao: df_mapeado vazio."])

    # Identificar coluna de valor
    valor_col = "Valor Líquido" if "Valor Líquido" in df.columns else None

    docs_sem_processo = 0
    docs_sem_tcmp = 0
    docs_sem_gl = 0

    for _, row in df.iterrows():
        documento = str(row.get("Documento", "") or "").strip()
        processo = str(row.get("processo_ac", "") or "").strip()
        linha = str(row.get("linha_negocio", "") or "").strip()
        tipo_pagamento = str(row.get("tipo_pagamento", "REGULAR") or "REGULAR").strip().upper()
        nf_extraida = str(row.get("nf_extraida", "") or "").strip()
        status_processo = str(row.get("Status Processo", "") or "").strip()

        if not processo:
            docs_sem_processo += 1
            continue

        # Valor do documento
        valor = 0.0
        if valor_col:
            try:
                valor = float(row.get(valor_col, 0.0) or 0.0)
            except (TypeError, ValueError):
                valor = 0.0

        # TCMP do processo
        tcmp = tcmp_result.tcmp_por_processo.get(processo)
        if tcmp is None:
            docs_sem_tcmp += 1
            continue

        # GLs elegíveis para a linha do processo
        linha_norm = _normalizar(linha)
        elegiveis = atribuicao_result.por_linha.get(linha_norm, [])
        if not elegiveis:
            docs_sem_gl += 1
            continue

        for gl in elegiveis:
            # FCMP do GL para este processo
            fcmp_result_gl = fcmp_por_gl.get(gl.nome)
            fcmp_processo: object = None
            if fcmp_result_gl:
                fcmp_processo = fcmp_result_gl.fcmp_por_processo.get(processo)

            if fcmp_processo is not None:
                fcmp_rampa = float(fcmp_processo.fcmp_rampa)  # type: ignore[attr-defined]
                fcmp_aplicado = float(fcmp_processo.fcmp_aplicado)  # type: ignore[attr-defined]
                fcmp_modo = str(fcmp_processo.modo)  # type: ignore[attr-defined]
                provisorio = bool(fcmp_processo.provisorio)  # type: ignore[attr-defined]
            else:
                fcmp_rampa = 1.0
                fcmp_aplicado = 1.0
                fcmp_modo = "PROVISÓRIO"
                provisorio = True

            # Nova regra de negócio: FCMP é sempre persistido, mas só entra
            # financeiramente no momento da reconciliação do Processo Pai.
            fcmp_considerado = 1.0
            comissao_potencial = valor * tcmp
            comissao_base = valor * tcmp
            comissao_final = valor * tcmp * fcmp_considerado

            item = ComissaoItem(
                gl_nome=gl.nome,
                processo=processo,
                documento=documento,
                nf_extraida=nf_extraida,
                tipo_pagamento=tipo_pagamento,
                status_processo=status_processo,
                linha_negocio=linha,
                valor_documento=valor,
                tcmp=tcmp,
                fcmp_rampa=fcmp_rampa,
                fcmp_aplicado=fcmp_aplicado,
                fcmp_modo=fcmp_modo,
                provisorio=provisorio,
                comissao_potencial=comissao_potencial,
                comissao_base=comissao_base,
                comissao_final=comissao_final,
                fcmp_considerado=fcmp_considerado,
            )
            itens.append(item)
            total_por_gl[gl.nome] += comissao_final

    if docs_sem_processo:
        warnings.append(f"calcular_comissao: {docs_sem_processo} doc(s) sem processo AC — ignorados.")
    if docs_sem_tcmp:
        warnings.append(f"calcular_comissao: {docs_sem_tcmp} doc(s) sem TCMP — ignorados.")
    if docs_sem_gl:
        warnings.append(f"calcular_comissao: {docs_sem_gl} doc(s) em linhas sem GL elegível — ignorados.")

    warnings.append(
        f"calcular_comissao: {len(itens)} item(ns) calculados para "
        f"{len(total_por_gl)} GL(s)."
    )

    return ComissaoResult(
        itens=itens,
        total_por_gl=dict(total_por_gl),
        warnings=warnings,
    )
