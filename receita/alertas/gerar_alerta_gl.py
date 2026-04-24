"""
receita/alertas/gerar_alerta_gl.py — Geração de arquivo de alertas de GL e vencimentos.

Dois tipos de alertas são gerados em `saida/MM_AAAA/alertas_MM_AAAA.txt`:

1. Conflitos GL: processos com múltiplos GLs distintos, bloqueados para revisão.
2. Vencimentos de parcelas: parcelas da AF com Dt. Prorrogação vencida ou próxima
   do vencimento (janela configurável via JANELA_ALERTA_DIAS) que ainda não foram
   pagas (Situação != 1).

API pública
-----------
gerar(conflitos, saida_dir, mes, ano) → str
gerar_alertas_vencimento(df_af_full, janela_dias) → List[str]
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd

JANELA_ALERTA_DIAS = 7  # Parcelas que vencem nos próximos N dias

_SITUACAO_PAGO = 1
_SITUACAO_COL = "Situação"
_DOCUMENTO_COL = "Documento"
_DT_PRORROGACAO_COL = "Dt. Prorrogação"


def gerar_alertas_vencimento(
    df_af_full: Optional[pd.DataFrame],
    janela_dias: int = JANELA_ALERTA_DIAS,
) -> List[str]:
    """Gera lista de alertas de parcelas vencidas ou a vencer.

    Analisa a AF completa (sem filtro de data) e identifica:
    - Parcelas VENCIDAS: Dt. Prorrogação < hoje e Situação != 1
    - Parcelas A VENCER: Dt. Prorrogação entre hoje e hoje + janela_dias e Situação != 1

    Args:
        df_af_full: DataFrame da AF SEM filtro de data.
            Deve conter colunas "Documento", "Situação" e "Dt. Prorrogação".
        janela_dias: Número de dias à frente para alertar sobre vencimentos.

    Returns:
        Lista de strings descritivas para cada parcela em alerta.
        Lista vazia se não houver alertas ou se df_af_full for None/vazio.
    """
    if df_af_full is None or df_af_full.empty:
        return []

    colunas_necessarias = {_DOCUMENTO_COL, _SITUACAO_COL, _DT_PRORROGACAO_COL}
    if not colunas_necessarias.issubset(df_af_full.columns):
        return []

    hoje = pd.Timestamp(date.today())
    limite_alerta = hoje + timedelta(days=janela_dias)

    df = df_af_full[[_DOCUMENTO_COL, _SITUACAO_COL, _DT_PRORROGACAO_COL]].copy()
    df[_DT_PRORROGACAO_COL] = pd.to_datetime(df[_DT_PRORROGACAO_COL], errors="coerce")

    # Excluir linhas sem data de prorrogação ou já pagas
    mask_valida = df[_DT_PRORROGACAO_COL].notna() & (df[_SITUACAO_COL] != _SITUACAO_PAGO)
    df = df[mask_valida]

    alertas: List[str] = []

    # Parcelas vencidas
    vencidas = df[df[_DT_PRORROGACAO_COL] < hoje]
    for _, row in vencidas.iterrows():
        documento = str(row[_DOCUMENTO_COL])
        dt = row[_DT_PRORROGACAO_COL].strftime("%d/%m/%Y")
        sit = int(row[_SITUACAO_COL]) if pd.notna(row[_SITUACAO_COL]) else -1
        sit_label = {0: "Em Aberto", 2: "Parcial"}.get(sit, f"Sit={sit}")
        alertas.append(f"[VENCIDA]   Doc {documento} — Vencimento: {dt} — {sit_label}")

    # Parcelas a vencer
    a_vencer = df[(df[_DT_PRORROGACAO_COL] >= hoje) & (df[_DT_PRORROGACAO_COL] <= limite_alerta)]
    for _, row in a_vencer.iterrows():
        documento = str(row[_DOCUMENTO_COL])
        dt = row[_DT_PRORROGACAO_COL].strftime("%d/%m/%Y")
        sit = int(row[_SITUACAO_COL]) if pd.notna(row[_SITUACAO_COL]) else -1
        sit_label = {0: "Em Aberto", 2: "Parcial"}.get(sit, f"Sit={sit}")
        dias = (row[_DT_PRORROGACAO_COL] - hoje).days
        alertas.append(f"[A VENCER]  Doc {documento} — Vencimento: {dt} (em {dias}d) — {sit_label}")

    return alertas


def gerar(
    conflitos: List[str],
    saida_dir: str,
    mes: int,
    ano: int,
    alertas_vencimento: Optional[List[str]] = None,
) -> str:
    """Cria alertas_MM_AAAA.txt com conflitos GL e alertas de vencimento.

    Args:
        conflitos: Lista de strings retornada por `validar_conflito_gl.validar`.
        saida_dir: Diretório de saída (ex: "saida/10_2025").
        mes: Mês de apuração (1–12).
        ano: Ano de apuração (ex: 2025).
        alertas_vencimento: Lista de alertas de parcelas vencidas/a vencer,
            retornada por `gerar_alertas_vencimento`. Opcional.

    Returns:
        Caminho absoluto do arquivo criado.

    Raises:
        OSError: Se não for possível criar o diretório ou escrever o arquivo.
    """
    mm = f"{mes:02d}"
    nome_arquivo = f"alertas_{mm}_{ano}.txt"
    caminho = Path(saida_dir) / nome_arquivo

    os.makedirs(saida_dir, exist_ok=True)

    linhas = [
        f"ALERTAS DE COMISSÕES POR RECEBIMENTO — {mm}/{ano}",
        "=" * 60,
        "",
    ]

    # Seção 1: Conflitos GL
    linhas += [
        "1. CONFLITOS DE GL",
        "-" * 60,
        f"Total de processos em conflito: {len(conflitos)}",
        "",
    ]
    if conflitos:
        linhas.append("Processos bloqueados (requerem revisão manual):")
        for i, conflito in enumerate(conflitos, start=1):
            linhas.append(f"{i:3d}. {conflito}")
        linhas += [
            "",
            "AÇÃO NECESSÁRIA: Revisar os processos acima no sistema.",
            "Processos com conflito GL não geram comissão automaticamente.",
        ]
    else:
        linhas.append("Nenhum conflito GL detectado.")

    # Seção 2: Alertas de vencimento
    alertas_vencimento = alertas_vencimento or []
    linhas += [
        "",
        "2. ALERTAS DE VENCIMENTO DE PARCELAS",
        "-" * 60,
        f"Total de parcelas em alerta: {len(alertas_vencimento)}",
        "",
    ]
    if alertas_vencimento:
        vencidas = [a for a in alertas_vencimento if a.startswith("[VENCIDA]")]
        a_vencer = [a for a in alertas_vencimento if a.startswith("[A VENCER]")]
        if vencidas:
            linhas.append(f"Parcelas vencidas ({len(vencidas)}):")
            linhas.extend(f"  {a}" for a in vencidas)
            linhas.append("")
        if a_vencer:
            linhas.append(f"Parcelas a vencer nos próximos {JANELA_ALERTA_DIAS} dias ({len(a_vencer)}):")
            linhas.extend(f"  {a}" for a in a_vencer)
    else:
        linhas.append("Nenhum alerta de vencimento.")

    caminho.write_text("\n".join(linhas), encoding="utf-8")
    return str(caminho)
