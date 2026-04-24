"""
receita/pipeline/etapas/etapa_04_mapear_documentos.py — Mapeamento AF → AC.

Input:
    df_af_json      list  — DataFrame AF serializado
    df_ac_full_json list  — DataFrame AC completo (sem filtro de mês)

Output:
    mapeamento_result  dict  — {df_mapeado_json, docs_nao_mapeados, warnings}
    warnings           list
    errors             list
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List

import pandas as pd

from receita.calculadores import mapear_documentos
from receita.schemas.calculo import MapeamentoResult


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Vincula documentos AF a processos AC via NF."""
    warnings: List[str] = []
    errors: List[str] = []

    df_af_json = input_data.get("df_af_json", [])
    df_ac_json = input_data.get("df_ac_full_json", [])

    df_af = pd.DataFrame(df_af_json) if df_af_json else pd.DataFrame()
    df_ac_full = pd.DataFrame(df_ac_json) if df_ac_json else pd.DataFrame()

    if df_af.empty:
        errors.append("etapa_04: df_af vazio — sem documentos a mapear.")
        return {"status": "error", "errors": errors, "warnings": warnings}

    result: MapeamentoResult = mapear_documentos.executar(df_af, df_ac_full)
    warnings.extend(result.warnings)

    df_mapeado_json = (
        result.df_mapeado.to_dict(orient="records")
        if result.df_mapeado is not None and not result.df_mapeado.empty
        else []
    )

    return {
        "status": "ok",
        "mapeamento_result": {
            "df_mapeado_json": df_mapeado_json,
            "docs_nao_mapeados": result.docs_nao_mapeados,
            "warnings": result.warnings,
        },
        "warnings": warnings,
        "errors": errors,
    }


if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    result = run(payload)
    print(json.dumps(result, ensure_ascii=False, default=str))
