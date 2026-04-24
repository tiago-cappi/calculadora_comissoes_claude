"""
receita/loaders/load_analise_financeira.py — Wrapper para carregamento da AF.

Reutiliza diretamente `scripts.loaders` sem reimplementação.
A função original já faz:
  - Mapeamento de aliases de colunas
  - Filtro Tipo de Baixa != 'A' (mantém 'B', vazio e demais)
  - Filtro por mês/ano da Data de Baixa (apenas no load mensal)
  - Renomeação para nomes padrão
  - Conversão de Situação (int) e Dt. Prorrogação (datetime)

API pública
-----------
load(file_path, mes, ano)         → Tuple[pd.DataFrame, List[str]]  — filtrado por mês/ano
load_bytes(file_bytes, mes, ano)  → Tuple[pd.DataFrame, List[str]]  — filtrado por mês/ano
load_full(file_path)              → Tuple[pd.DataFrame, List[str]]  — sem filtro de data
load_full_bytes(file_bytes)       → Tuple[pd.DataFrame, List[str]]  — sem filtro de data
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import pandas as pd


def load(
    file_path: str,
    mes: int,
    ano: int,
) -> Tuple[pd.DataFrame, List[str]]:
    """Carrega a Análise Financeira a partir de um caminho de arquivo.

    Delega para `scripts.loaders.load_analise_financeira` após ler os bytes
    do arquivo. Filtra automaticamente Tipo de Baixa='B' e mês/ano informados.

    Args:
        file_path: Caminho para o arquivo analise-financeira.xlsx.
        mes: Mês de apuração (1–12).
        ano: Ano de apuração (ex: 2025).

    Returns:
        (DataFrame filtrado com colunas padrão, lista de warnings).
        DataFrame vazio se arquivo não encontrado ou colunas essenciais ausentes.
    """
    path = Path(file_path)
    if not path.exists():
        return pd.DataFrame(), [f"Arquivo não encontrado: {file_path}"]

    try:
        file_bytes = path.read_bytes()
    except OSError as exc:
        return pd.DataFrame(), [f"Erro ao ler {file_path}: {exc}"]

    return load_bytes(file_bytes, mes, ano)


def load_bytes(
    file_bytes: bytes,
    mes: int,
    ano: int,
) -> Tuple[pd.DataFrame, List[str]]:
    """Carrega a Análise Financeira a partir de bytes, filtrada por mês/ano.

    Interface direta com `scripts.loaders.load_analise_financeira`.
    Útil quando os bytes já foram lidos (ex: upload via N8N/web).

    Args:
        file_bytes: Conteúdo binário do arquivo .xlsx.
        mes: Mês de apuração (1–12).
        ano: Ano de apuração (ex: 2025).

    Returns:
        (DataFrame filtrado com colunas padrão, lista de warnings).
    """
    from scripts.loaders import load_analise_financeira  # import tardio: evita ciclo

    return load_analise_financeira(file_bytes, mes, ano)


def load_full(
    file_path: str,
) -> Tuple[pd.DataFrame, List[str]]:
    """Carrega a Análise Financeira completa SEM filtro de data.

    Retorna todos os registros históricos (excluindo apenas Tipo de Baixa='A').
    Necessário para verificar se TODAS as parcelas de um Processo Pai foram
    pagas independente do mês de referência.

    Args:
        file_path: Caminho para o arquivo analise-financeira.xlsx.

    Returns:
        (DataFrame completo sem filtro de data, lista de warnings).
        DataFrame vazio se arquivo não encontrado ou colunas essenciais ausentes.
    """
    path = Path(file_path)
    if not path.exists():
        return pd.DataFrame(), [f"Arquivo não encontrado: {file_path}"]

    try:
        file_bytes = path.read_bytes()
    except OSError as exc:
        return pd.DataFrame(), [f"Erro ao ler {file_path}: {exc}"]

    return load_full_bytes(file_bytes)


def load_full_bytes(
    file_bytes: bytes,
) -> Tuple[pd.DataFrame, List[str]]:
    """Carrega a Análise Financeira completa SEM filtro de data a partir de bytes.

    Interface direta com `scripts.loaders.load_analise_financeira_full`.
    Útil para verificação de pagamentos do Processo Pai e para contexto de
    alertas de vencimento.

    Args:
        file_bytes: Conteúdo binário do arquivo .xlsx.

    Returns:
        (DataFrame completo sem filtro de data, lista de warnings).
    """
    from scripts.loaders import load_analise_financeira_full  # import tardio: evita ciclo

    return load_analise_financeira_full(file_bytes)
