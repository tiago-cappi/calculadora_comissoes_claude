"""
receita/pipeline/etapas/etapa_10_calcular_reconciliacao.py — Cálculo da reconciliação.

Input:
    comissao_result   dict  — saída da etapa_07
    fcmp_por_gl       dict  — saída da etapa_06
    processos_aptos   list  — saída da etapa_09

Output:
    reconciliacao_result  dict  — {itens, total_por_gl, warnings}
    warnings              list
    errors                list
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List, Set

from receita.calculadores import calcular_reconciliacao
from receita.pipeline.etapas.etapa_07_calcular_comissao import _deserializar_fcmp_por_gl
from receita.schemas.calculo import ComissaoItem, ComissaoResult


def _reconstruir_comissao_result(data: Dict) -> ComissaoResult:
    itens = [
        ComissaoItem(
            gl_nome=i["gl_nome"],
            processo=i["processo"],
            documento=i["documento"],
            nf_extraida=i.get("nf_extraida", ""),
            tipo_pagamento=i.get("tipo_pagamento", "REGULAR"),
            status_processo=i.get("status_processo", ""),
            linha_negocio=i.get("linha_negocio", ""),
            valor_documento=float(i.get("valor_documento", 0.0)),
            tcmp=float(i.get("tcmp", 0.0)),
            fcmp_rampa=float(i.get("fcmp_rampa", 1.0)),
            fcmp_aplicado=float(i.get("fcmp_aplicado", 1.0)),
            fcmp_modo=i.get("fcmp_modo", "PROVISÓRIO"),
            provisorio=bool(i.get("provisorio", True)),
            comissao_potencial=float(i.get("comissao_potencial", 0.0)),
            comissao_final=float(i.get("comissao_final", 0.0)),
            fcmp_considerado=float(i.get("fcmp_considerado", 1.0)),
            comissao_base=float(i.get("comissao_base", i.get("comissao_final", 0.0))),
        )
        for i in data.get("itens", [])
    ]
    return ComissaoResult(
        itens=itens,
        total_por_gl=data.get("total_por_gl", {}),
        warnings=data.get("warnings", []),
    )


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Calcula ajustes de reconciliação para processos aptos."""
    warnings: List[str] = []
    errors: List[str] = []

    comissao_raw = input_data.get("comissao_result", {})
    fcmp_raw = input_data.get("fcmp_por_gl", {})
    processos_aptos_list = input_data.get("processos_aptos", [])
    processos_aptos: Set[str] = set(processos_aptos_list)

    comissao_result = _reconstruir_comissao_result(comissao_raw)
    fcmp_por_gl = _deserializar_fcmp_por_gl(fcmp_raw)

    result = calcular_reconciliacao.executar(comissao_result, fcmp_por_gl, processos_aptos)
    warnings.extend(result.warnings)

    itens_json = [
        {
            "gl_nome": i.gl_nome,
            "numero_pc": i.numero_pc,
            "codigo_cliente": i.codigo_cliente,
            "processo": i.processo,
            "comissao_adiantada": i.comissao_adiantada,
            "fcmp_real": i.fcmp_real,
            "ajuste": i.ajuste,
            "historicos_considerados": i.historicos_considerados,
        }
        for i in result.itens
    ]

    return {
        "status": "ok",
        "reconciliacao_result": {
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
