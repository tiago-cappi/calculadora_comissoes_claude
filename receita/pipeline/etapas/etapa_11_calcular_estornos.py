"""
receita/pipeline/etapas/etapa_11_calcular_estornos.py — Cálculo de estornos.

Input:
    df_devolucoes_json  list  — DataFrame devoluções serializado (pode ser [])
    df_ac_full_json     list  — DataFrame AC completo
    comissao_result     dict  — saída da etapa_07

Output:
    estornos_result  dict  — {itens, total_por_gl, warnings}
    warnings         list
    errors           list
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List

import pandas as pd

from receita.calculadores import calcular_estornos
from receita.pipeline.etapas.etapa_10_calcular_reconciliacao import _reconstruir_comissao_result


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Calcula estornos proporcionais por devoluções."""
    warnings: List[str] = []
    errors: List[str] = []

    df_dev_json = input_data.get("df_devolucoes_json", [])
    df_ac_json = input_data.get("df_ac_full_json", [])
    comissao_raw = input_data.get("comissao_result", {})

    df_devolucoes = pd.DataFrame(df_dev_json) if df_dev_json else None
    df_ac_full = pd.DataFrame(df_ac_json) if df_ac_json else pd.DataFrame()
    comissao_result = _reconstruir_comissao_result(comissao_raw)

    result = calcular_estornos.executar(df_devolucoes, df_ac_full, comissao_result)
    warnings.extend(result.warnings)

    itens_json = [
        {
            "gl_nome": i.gl_nome,
            "processo": i.processo,
            "nf_origem": i.nf_origem,
            "valor_devolvido": i.valor_devolvido,
            "valor_processo": i.valor_processo,
            "comissao_base": i.comissao_base,
            "estorno": i.estorno,
        }
        for i in result.itens
    ]

    return {
        "status": "ok",
        "estornos_result": {
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
