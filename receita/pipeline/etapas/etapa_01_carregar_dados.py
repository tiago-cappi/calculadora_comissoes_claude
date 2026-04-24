"""
receita/pipeline/etapas/etapa_01_carregar_dados.py — Carregamento de dados externos.

Input:
    mes                       int    — mês de apuração (1–12)
    ano                       int    — ano de apuração
    caminho_af                str    — caminho para analise-financeira.xlsx
    caminho_processo_pedido   str    — caminho para processo_pedido_compra.xlsx
    caminho_devolucoes        str?   — caminho para devolucoes.xlsx (opcional)

Output:
    df_af_json          list   — DataFrame AF serializado (to_dict "records")
    tabela_pc_json      list   — ProcessoPedidoTabela serializada
    df_devolucoes_json  list?  — DataFrame devoluções (pode ser [])
    warnings            list
    errors              list
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List

import pandas as pd

from receita.loaders import load_analise_financeira, load_pagamentos_pendentes, load_processo_pedido


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Carrega e valida os arquivos de entrada do pipeline de Recebimento.

    Args:
        input_data: Dicionário com chaves 'mes', 'ano', 'caminho_af',
            'caminho_processo_pedido' e opcionalmente 'caminho_devolucoes'.

    Returns:
        Dicionário com status, dados carregados e warnings/errors.
    """
    warnings: List[str] = []
    errors: List[str] = []

    mes = int(input_data.get("mes", 0))
    ano = int(input_data.get("ano", 0))
    caminho_af = str(input_data.get("caminho_af", ""))
    caminho_pc = str(input_data.get("caminho_processo_pedido", ""))
    caminho_dev = input_data.get("caminho_devolucoes", "")

    if not mes or not ano:
        errors.append("etapa_01: 'mes' e 'ano' são obrigatórios.")
        return {"status": "error", "errors": errors, "warnings": warnings}

    # --- Análise Financeira ---------------------------------------------------
    df_af = pd.DataFrame()
    if caminho_af:
        df_af, w_af = load_analise_financeira.load(caminho_af, mes, ano)
        warnings.extend(w_af)
        if df_af.empty:
            errors.append(f"etapa_01: Análise Financeira vazia ou não encontrada: {caminho_af}")
    else:
        errors.append("etapa_01: 'caminho_af' não informado.")

    # --- Processo x Pedido de Compra -----------------------------------------
    tabela_pc, w_pc = load_processo_pedido.load(caminho_pc) if caminho_pc else (None, ["caminho_processo_pedido não informado."])
    warnings.extend(w_pc)

    # --- Devoluções (opcional) ------------------------------------------------
    df_devolucoes = pd.DataFrame()
    if caminho_dev:
        try:
            df_devolucoes = pd.read_excel(caminho_dev, dtype=str)
            warnings.append(f"etapa_01: Devoluções carregadas: {len(df_devolucoes)} linha(s).")
        except Exception as exc:
            warnings.append(f"etapa_01: Devoluções não carregadas ({exc}) — prosseguindo sem estornos.")

    # Serializar para JSON-safe
    df_af_json = df_af.to_dict(orient="records") if not df_af.empty else []
    tabela_pc_json = (
        [
            {
                "numero_processo": item.numero_processo,
                "numero_pc": item.numero_pc,
                "codigo_cliente": item.codigo_cliente,
            }
            for item in tabela_pc.registros
        ]
        if tabela_pc
        else []
    )
    df_dev_json = df_devolucoes.to_dict(orient="records") if not df_devolucoes.empty else []

    status = "error" if errors else "ok"
    return {
        "status": status,
        "mes": mes,
        "ano": ano,
        "df_af_json": df_af_json,
        "tabela_pc_json": tabela_pc_json,
        "df_devolucoes_json": df_dev_json,
        "warnings": warnings,
        "errors": errors,
    }


if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    result = run(payload)
    print(json.dumps(result, ensure_ascii=False, default=str))
