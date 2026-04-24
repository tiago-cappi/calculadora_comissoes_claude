"""
receita/pipeline/etapas/etapa_14_exportar_excel.py — Exportação Excel por GL.

Input: (idêntico à etapa_13)
Output:
    arquivos_gerados  list  — caminhos dos .xlsx gerados
    warnings          list
    errors            list
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List

from receita.exportadores import excel_exporter


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Gera arquivos Excel de recebimento por GL."""
    warnings: List[str] = []
    errors: List[str] = []

    saida_dir = str(input_data.get("saida_dir", "saida"))
    mes = int(input_data.get("mes", 0))
    ano = int(input_data.get("ano", 0))

    try:
        arquivos = excel_exporter.gerar_por_gl(
            comissao_result=input_data.get("comissao_result", {}),
            reconciliacao_result=input_data.get("reconciliacao_result", {}),
            estornos_result=input_data.get("estornos_result", {}),
            tcmp_result=input_data.get("tcmp_result", {}),
            fcmp_por_gl=input_data.get("fcmp_por_gl", {}),
            saida_dir=saida_dir,
            mes=mes,
            ano=ano,
            status_por_processo_pai=input_data.get("status_por_processo_pai"),
            processo_para_pai=input_data.get("processo_para_pai"),
            historicos_por_gl=input_data.get("historicos_por_gl"),
            df_ac_full=input_data.get("df_ac_full"),
            df_af_mapeado=input_data.get("df_af_mapeado"),
        )
        warnings.append(f"etapa_14: {len(arquivos)} arquivo(s) Excel gerado(s).")
    except Exception as exc:
        errors.append(f"etapa_14: erro ao exportar Excel: {exc}")
        arquivos = []

    return {
        "status": "error" if errors else "ok",
        "arquivos_gerados": arquivos,
        "warnings": warnings,
        "errors": errors,
    }


if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    result = run(payload)
    print(json.dumps(result, ensure_ascii=False, default=str))
