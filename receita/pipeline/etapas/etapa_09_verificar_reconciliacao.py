"""
receita/pipeline/etapas/etapa_09_verificar_reconciliacao.py — Decisão de reconciliação.

Input:
    status_por_processo_pai  dict  — saída da etapa_08

Output:
    processos_aptos  list  — processos cujos Pais estão 100% faturados E pagos
    warnings         list
    errors           list
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List, Set

from receita.rastreamento import determinar_reconciliacao


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Determina quais processos estão aptos para reconciliação."""
    warnings: List[str] = []
    errors: List[str] = []

    status_por_pai = input_data.get("status_por_processo_pai", {})

    processos_aptos: Set[str] = set()

    for chave_str, status in status_por_pai.items():
        numero_pc = status.get("numero_pc", "")
        codigo_cliente = status.get("codigo_cliente", "")
        status_fat = bool(status.get("status_faturamento_completo", False))
        status_pag = bool(status.get("status_pagamento_completo", False))

        apto = determinar_reconciliacao.determinar(
            numero_pc, codigo_cliente, status_fat, status_pag
        )

        if apto:
            # Todos os processos faturados do Pai são aptos
            for proc in status.get("processos_faturados", []):
                processos_aptos.add(proc)
            warnings.append(
                f"etapa_09: Pai (PC={numero_pc}, CLI={codigo_cliente}) apto para reconciliação. "
                f"Processos: {status.get('processos_faturados', [])}"
            )
        else:
            motivo = []
            if not status_fat:
                pendentes = status.get("processos_pendentes", [])
                motivo.append(f"faturamento incompleto ({len(pendentes)} pendente(s))")
            if not status_pag:
                motivo.append("pagamentos pendentes (ERP não implementado)")
            warnings.append(
                f"etapa_09: Pai (PC={numero_pc}, CLI={codigo_cliente}) NÃO apto: "
                + "; ".join(motivo)
            )

    if not processos_aptos:
        warnings.append("etapa_09: nenhum processo apto para reconciliação neste ciclo.")

    return {
        "status": "ok",
        "processos_aptos": sorted(processos_aptos),
        "warnings": warnings,
        "errors": errors,
    }


if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    result = run(payload)
    print(json.dumps(result, ensure_ascii=False, default=str))
