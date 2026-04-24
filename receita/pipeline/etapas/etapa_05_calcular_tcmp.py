"""
receita/pipeline/etapas/etapa_05_calcular_tcmp.py — Cálculo de TCMP por processo.

Input:
    df_ac_full_json    list  — DataFrame AC completo serializado
    atribuicao_result  dict  — saída da etapa_03

Output:
    tcmp_result  dict  — {tcmp_por_processo, warnings}
    warnings     list
    errors       list
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List

import pandas as pd

from receita.calculadores import calcular_tcmp
from receita.pipeline.etapas.etapa_02_validar_conflitos import _deserializar_atribuicao


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Calcula TCMP para todos os processos da AC."""
    warnings: List[str] = []
    errors: List[str] = []

    df_ac_json = input_data.get("df_ac_full_json", [])
    atribuicao_raw = input_data.get("atribuicao_result", {})

    df_ac_full = pd.DataFrame(df_ac_json) if df_ac_json else pd.DataFrame()
    atribuicao_result = _deserializar_atribuicao(atribuicao_raw)

    if df_ac_full.empty:
        errors.append("etapa_05: df_ac_full vazio.")
        return {"status": "error", "errors": errors, "warnings": warnings}

    result = calcular_tcmp.executar(df_ac_full, atribuicao_result)
    warnings.extend(result.warnings)

    return {
        "status": "ok",
        "tcmp_result": {
            "tcmp_por_processo": result.tcmp_por_processo,
            "warnings": result.warnings,
        },
        "warnings": warnings,
        "errors": errors,
    }


if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    result = run(payload)
    print(json.dumps(result, ensure_ascii=False, default=str))
