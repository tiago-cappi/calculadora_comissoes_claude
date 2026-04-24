"""
receita/pipeline/etapas/etapa_07_calcular_comissao.py — Cálculo de comissão por doc AF.

Input:
    mapeamento_result  dict  — saída da etapa_04
    atribuicao_result  dict  — saída da etapa_03
    tcmp_result        dict  — saída da etapa_05
    fcmp_por_gl        dict  — saída da etapa_06

Output:
    comissao_result  dict  — {itens, total_por_gl, warnings}
    warnings         list
    errors           list
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List

import pandas as pd

from receita.calculadores import calcular_comissao
from receita.pipeline.etapas.etapa_02_validar_conflitos import _deserializar_atribuicao
from receita.schemas.calculo import (
    FCMPProcesso,
    FCMPResult,
    MapeamentoResult,
    TCMPResult,
)


def _deserializar_fcmp_por_gl(data: Dict) -> Dict[str, FCMPResult]:
    """Reconstrói {gl_nome: FCMPResult} a partir de dict JSON."""
    resultado = {}
    for gl_nome, fcmp_raw in data.items():
        fcmp_por_proc = {}
        for proc, fp_raw in fcmp_raw.get("fcmp_por_processo", {}).items():
            fcmp_por_proc[proc] = FCMPProcesso(
                processo=fp_raw["processo"],
                gl_nome=fp_raw["gl_nome"],
                fcmp_rampa=float(fp_raw.get("fcmp_rampa", 1.0)),
                fcmp_aplicado=float(fp_raw.get("fcmp_aplicado", 1.0)),
                modo=str(fp_raw.get("modo", "PROVISÓRIO")),
                provisorio=bool(fp_raw.get("provisorio", True)),
                num_itens=int(fp_raw.get("num_itens", 0)),
                valor_faturado=float(fp_raw.get("valor_faturado", 0.0)),
            )
        resultado[gl_nome] = FCMPResult(
            gl_nome=gl_nome,
            fcmp_por_processo=fcmp_por_proc,
            warnings=fcmp_raw.get("warnings", []),
        )
    return resultado


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Calcula comissão por documento AF para cada GL."""
    warnings: List[str] = []
    errors: List[str] = []

    mapeamento_raw = input_data.get("mapeamento_result", {})
    atribuicao_raw = input_data.get("atribuicao_result", {})
    tcmp_raw = input_data.get("tcmp_result", {})
    fcmp_raw = input_data.get("fcmp_por_gl", {})

    # Reconstruir objetos
    df_mapeado = pd.DataFrame(mapeamento_raw.get("df_mapeado_json", []))
    mapeamento_result = MapeamentoResult(
        df_mapeado=df_mapeado,
        docs_nao_mapeados=mapeamento_raw.get("docs_nao_mapeados", []),
        warnings=mapeamento_raw.get("warnings", []),
    )
    atribuicao_result = _deserializar_atribuicao(atribuicao_raw)
    tcmp_result = TCMPResult(
        tcmp_por_processo=tcmp_raw.get("tcmp_por_processo", {}),
        warnings=tcmp_raw.get("warnings", []),
    )
    fcmp_por_gl = _deserializar_fcmp_por_gl(fcmp_raw)

    if df_mapeado.empty:
        errors.append("etapa_07: df_mapeado vazio — sem documentos para calcular comissão.")
        return {"status": "error", "errors": errors, "warnings": warnings}

    result = calcular_comissao.executar(
        mapeamento_result, atribuicao_result, tcmp_result, fcmp_por_gl
    )
    warnings.extend(result.warnings)

    # Serializar ComissaoResult
    itens_json = [
        {
            "gl_nome": i.gl_nome,
            "processo": i.processo,
            "documento": i.documento,
            "nf_extraida": i.nf_extraida,
            "tipo_pagamento": i.tipo_pagamento,
            "status_processo": i.status_processo,
            "linha_negocio": i.linha_negocio,
            "valor_documento": i.valor_documento,
            "tcmp": i.tcmp,
            "fcmp_rampa": i.fcmp_rampa,
            "fcmp_aplicado": i.fcmp_aplicado,
            "fcmp_considerado": i.fcmp_considerado,
            "fcmp_modo": i.fcmp_modo,
            "provisorio": i.provisorio,
            "comissao_potencial": i.comissao_potencial,
            "comissao_base": i.comissao_base,
            "comissao_final": i.comissao_final,
        }
        for i in result.itens
    ]

    return {
        "status": "ok",
        "comissao_result": {
            "itens": itens_json,
            "total_por_gl": result.total_por_gl,
            "warnings": result.warnings,
        },
        "warnings": warnings,
        "errors": errors,
    }


if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    result = run(payload)
    print(json.dumps(result, ensure_ascii=False, default=str))
