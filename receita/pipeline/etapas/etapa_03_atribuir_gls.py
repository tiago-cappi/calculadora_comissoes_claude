"""
receita/pipeline/etapas/etapa_03_atribuir_gls.py — Atribuição de GLs elegíveis.

Input:
    config_comissao  list  — regras de comissão do Supabase
    colaboradores    list  — cadastro de colaboradores do Supabase
    cargos           list  — cadastro de cargos do Supabase

Output:
    atribuicao_result  dict  — {elegiveis, por_linha, warnings}
    warnings           list
    errors             list
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List

from receita.calculadores import atribuir_gls


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Atribui GLs elegíveis para comissão de Recebimento."""
    warnings: List[str] = []
    errors: List[str] = []

    config_comissao = input_data.get("config_comissao", [])
    colaboradores = input_data.get("colaboradores", [])
    cargos = input_data.get("cargos", [])

    if not config_comissao:
        errors.append("etapa_03: 'config_comissao' vazia — sem regras de Recebimento.")
        return {"status": "error", "errors": errors, "warnings": warnings}

    result = atribuir_gls.executar(config_comissao, colaboradores, cargos)
    warnings.extend(result.warnings)

    # Serializar AtribuicaoResult
    atribuicao_json = {
        "elegiveis": [
            {
                "nome": g.nome,
                "cargo": g.cargo,
                "linha": g.linha,
                "hierarquia": list(g.hierarquia),
                "taxa_efetiva": g.taxa_efetiva,
                "especificidade": g.especificidade,
            }
            for g in result.elegiveis
        ],
        "por_linha": {
            linha: [
                {
                    "nome": g.nome,
                    "cargo": g.cargo,
                    "linha": g.linha,
                    "hierarquia": list(g.hierarquia),
                    "taxa_efetiva": g.taxa_efetiva,
                    "especificidade": g.especificidade,
                }
                for g in gls
            ]
            for linha, gls in result.por_linha.items()
        },
        "warnings": result.warnings,
    }

    return {
        "status": "ok",
        "atribuicao_result": atribuicao_json,
        "warnings": warnings,
        "errors": errors,
    }


if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    result = run(payload)
    print(json.dumps(result, ensure_ascii=False, default=str))
