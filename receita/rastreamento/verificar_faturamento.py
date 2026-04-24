"""
receita/rastreamento/verificar_faturamento.py — Verifica faturamento do Processo Pai.

Um Processo Pai está "completamente faturado" quando todos os seus processos
filho (listados na tabela PC) possuem `Dt Emissão` preenchida na AC.

Retorno
-------
dict com:
    status_completo       bool   — True se todos faturados
    processos_total       int    — total de processos vinculados ao Pai
    processos_faturados   List[str] — processos com Dt Emissão preenchida
    processos_pendentes   List[str] — processos sem Dt Emissão

API pública
-----------
verificar(numero_pc, codigo_cliente, tabela_pc, df_ac_full) → dict
"""

from __future__ import annotations

from typing import Dict, List

import pandas as pd

from receita.schemas.entrada import ProcessoPedidoTabela

_DT_EMISSAO_COL = "Dt Emissão"
_PROCESSO_COL = "Processo"


def verificar(
    numero_pc: str,
    codigo_cliente: str,
    tabela_pc: ProcessoPedidoTabela,
    df_ac_full: pd.DataFrame,
) -> Dict:
    """Verifica se todos os processos filho de um Pai estão faturados.

    Um processo é considerado faturado quando possui `Dt Emissão` preenchida
    (não nula, não vazia, não "nan") na Análise Comercial.

    Args:
        numero_pc: Número do pedido de compra do Processo Pai.
        codigo_cliente: Código do cliente do Processo Pai.
        tabela_pc: Tabela PC para listar processos vinculados ao Pai.
        df_ac_full: DataFrame da AC SEM filtro de mês.
            Deve conter colunas "Processo" e "Dt Emissão".

    Returns:
        Dicionário com:
            status_completo (bool), processos_total (int),
            processos_faturados (List[str]), processos_pendentes (List[str]).
    """
    # Buscar todos os processos filho vinculados ao Pai
    processos_vinculados = tabela_pc.get_processos_do_pai(numero_pc, codigo_cliente)

    if not processos_vinculados:
        return {
            "status_completo": False,
            "processos_total": 0,
            "processos_faturados": [],
            "processos_pendentes": [],
            "aviso": f"Nenhum processo encontrado para Pai (PC={numero_pc}, CLI={codigo_cliente}).",
        }

    # Indexar processos com Dt Emissão na AC
    processos_faturados_na_ac: set = set()

    if _PROCESSO_COL in df_ac_full.columns and _DT_EMISSAO_COL in df_ac_full.columns:
        df = df_ac_full[[_PROCESSO_COL, _DT_EMISSAO_COL]].copy()
        df[_PROCESSO_COL] = df[_PROCESSO_COL].astype(str).str.strip().str.upper()
        df[_DT_EMISSAO_COL] = df[_DT_EMISSAO_COL].astype(str).str.strip()

        mask_faturado = (
            df[_DT_EMISSAO_COL].notna()
            & (df[_DT_EMISSAO_COL] != "")
            & (df[_DT_EMISSAO_COL].str.upper() != "NAN")
            & (df[_DT_EMISSAO_COL].str.upper() != "NAT")
            & (df[_DT_EMISSAO_COL].str.upper() != "NONE")
        )
        processos_faturados_na_ac = set(df.loc[mask_faturado, _PROCESSO_COL].unique())

    # Classificar processos vinculados
    processos_faturados: List[str] = []
    processos_pendentes: List[str] = []

    for proc in processos_vinculados:
        proc_upper = str(proc).strip().upper()
        if proc_upper in processos_faturados_na_ac:
            processos_faturados.append(proc)
        else:
            processos_pendentes.append(proc)

    status_completo = len(processos_pendentes) == 0 and len(processos_faturados) > 0

    return {
        "status_completo": status_completo,
        "processos_total": len(processos_vinculados),
        "processos_faturados": processos_faturados,
        "processos_pendentes": processos_pendentes,
    }
