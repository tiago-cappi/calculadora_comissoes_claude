"""
receita/validadores/validar_conflito_gl.py — Detecção de conflitos de GL.

Um conflito ocorre quando um mesmo processo AC possui itens pertencentes a
Linhas de negócio distintas, e cada uma dessas Linhas tem um GL diferente
elegível para comissão de Recebimento.

Quando isso acontece, não é possível determinar automaticamente qual GL
deve receber a comissão — o processo deve ser bloqueado e um alerta gerado.

Lógica
------
1. Para cada processo da AC, coletar o conjunto de Linhas dos seus itens.
2. Para cada Linha, verificar se há GL elegível (via AtribuicaoResult.por_linha).
3. Se múltiplas Linhas têm GLs *distintos*, o processo está em conflito.

API pública
-----------
validar(df_ac, atribuicao_result) → List[str]   (lista de mensagens de conflito)
"""

from __future__ import annotations

import unicodedata
from typing import Dict, List, Set

import pandas as pd

from receita.schemas.calculo import AtribuicaoResult


def _normalizar(texto: str) -> str:
    """Normaliza texto para comparação: maiúsculas sem acentos."""
    return unicodedata.normalize("NFKD", str(texto).upper()).encode("ascii", "ignore").decode()


def validar(
    df_ac: pd.DataFrame,
    atribuicao_result: AtribuicaoResult,
) -> List[str]:
    """Detecta processos com múltiplos GLs distintos entre suas Linhas.

    Para cada processo na AC que possua itens em mais de uma Linha de negócio,
    verifica se as Linhas têm GLs diferentes elegíveis. Processos com conflito
    devem ser bloqueados antes do cálculo de comissão.

    Args:
        df_ac: DataFrame da Análise Comercial enriquecido com coluna "Linha".
            Deve conter as colunas "Processo" e "Linha".
        atribuicao_result: Resultado da etapa de atribuição de GLs com
            dicionário `por_linha` {linha_normalizada: [ElegivelGL, ...]}.

    Returns:
        Lista de strings descrevendo cada conflito encontrado.
        Lista vazia significa sem conflitos.
    """
    conflitos: List[str] = []

    if "Processo" not in df_ac.columns or "Linha" not in df_ac.columns:
        return ["validar_conflito_gl: colunas 'Processo' e/ou 'Linha' ausentes no df_ac."]

    # Pré-indexar: linha_normalizada → conjunto de nomes de GL
    gls_por_linha: Dict[str, Set[str]] = {}
    for linha_norm, elegiveis in atribuicao_result.por_linha.items():
        gls_por_linha[linha_norm] = {e.nome for e in elegiveis}

    # Agrupar itens por processo
    grupos = df_ac.groupby("Processo", sort=False)

    for processo, grupo in grupos:
        # Linhas únicas do processo
        linhas_processo = grupo["Linha"].dropna().unique()

        # Para cada Linha, coletar os GLs elegíveis
        gls_por_linha_processo: Dict[str, Set[str]] = {}
        for linha in linhas_processo:
            linha_norm = _normalizar(str(linha))
            gls = gls_por_linha.get(linha_norm)
            if gls:
                gls_por_linha_processo[str(linha)] = gls

        if len(gls_por_linha_processo) <= 1:
            continue  # 0 ou 1 Linha com GL → sem conflito

        # Verificar se os conjuntos de GLs são distintos entre as Linhas
        todos_gls = list(gls_por_linha_processo.values())
        # Conflito real = ao menos dois conjuntos sem interseção total
        # (mesmo que haja sobreposição parcial, GLs distintos = conflito)
        gls_unicos_globais: Set[str] = set()
        for gls in todos_gls:
            gls_unicos_globais |= gls

        # Se todos os GL são os mesmos em todas as linhas → sem conflito
        if all(gls == todos_gls[0] for gls in todos_gls):
            continue

        linhas_desc = sorted(gls_por_linha_processo.keys())
        gls_desc = sorted(gls_unicos_globais)
        conflitos.append(
            f"Processo {processo}: Linhas {linhas_desc} com GLs distintos {gls_desc}"
        )

    return conflitos
