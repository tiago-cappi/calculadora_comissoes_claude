"""
receita/pipeline/etapas/etapa_08_rastrear_processo_pai.py — Rastreamento do Processo Pai.

Para cada processo com comissão calculada:
1. Identifica o Processo Pai (numero_pc + codigo_cliente)
2. Verifica se o Pai está completamente faturado
3. Verifica status de pagamento (STUB → sempre False)

Input:
    comissao_result   dict  — saída da etapa_07
    tabela_pc_json    list  — ProcessoPedidoTabela serializada
    df_ac_full_json   list  — DataFrame AC completo
    mes               int
    ano               int

Output:
    status_por_processo_pai  dict  — {(pc, cli): {status_fat, status_pag, processos_...}}
    warnings                 list
    errors                   list
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from receita.rastreamento import (
    identificar_processo_pai,
    verificar_faturamento,
    verificar_pagamentos,
)
from receita.schemas.entrada import ProcessoPedidoItem, ProcessoPedidoTabela


def _reconstruir_tabela_pc(tabela_pc_json: List[Dict]) -> ProcessoPedidoTabela:
    registros = [
        ProcessoPedidoItem(
            numero_processo=r["numero_processo"],
            numero_pc=r["numero_pc"],
            codigo_cliente=r["codigo_cliente"],
        )
        for r in tabela_pc_json
    ]
    return ProcessoPedidoTabela(registros=registros)


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Rastreia o Processo Pai de cada processo com comissão calculada."""
    warnings: List[str] = []
    errors: List[str] = []

    comissao_raw = input_data.get("comissao_result", {})
    tabela_pc_json = input_data.get("tabela_pc_json", [])
    df_ac_json = input_data.get("df_ac_full_json", [])

    tabela_pc = _reconstruir_tabela_pc(tabela_pc_json)
    df_ac_full = pd.DataFrame(df_ac_json) if df_ac_json else pd.DataFrame()
    df_pagamentos = None  # STUB: aguardando ERP

    # Processos únicos com comissão
    processos_com_comissao = list({
        item["processo"]
        for item in comissao_raw.get("itens", [])
        if item.get("processo")
    })

    # Status por Processo Pai: chave = "pc|cli"
    status_por_pai: Dict[str, Dict] = {}
    processo_para_pai: Dict[str, Optional[str]] = {}

    for processo in processos_com_comissao:
        chave_pai = identificar_processo_pai.identificar(processo, tabela_pc)

        if chave_pai is None:
            warnings.append(
                f"etapa_08: processo '{processo}' não encontrado na tabela PC "
                "— sem rastreamento de Processo Pai."
            )
            processo_para_pai[processo] = None
            continue

        numero_pc, codigo_cliente = chave_pai
        chave_str = f"{numero_pc}|{codigo_cliente}"
        processo_para_pai[processo] = chave_str

        if chave_str not in status_por_pai:
            # Verificar faturamento do Pai
            status_fat = verificar_faturamento.verificar(
                numero_pc, codigo_cliente, tabela_pc, df_ac_full
            )
            # Verificar pagamentos (STUB)
            status_pag, w_pag = verificar_pagamentos.verificar(
                numero_pc, codigo_cliente, df_pagamentos
            )
            if w_pag:
                warnings.append(w_pag)

            status_por_pai[chave_str] = {
                "numero_pc": numero_pc,
                "codigo_cliente": codigo_cliente,
                "status_faturamento_completo": status_fat["status_completo"],
                "status_pagamento_completo": status_pag,
                "processos_total": status_fat["processos_total"],
                "processos_faturados": status_fat["processos_faturados"],
                "processos_pendentes": status_fat["processos_pendentes"],
            }

    return {
        "status": "ok",
        "status_por_processo_pai": status_por_pai,
        "processo_para_pai": processo_para_pai,
        "warnings": warnings,
        "errors": errors,
    }


if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    result = run(payload)
    print(json.dumps(result, ensure_ascii=False, default=str))
