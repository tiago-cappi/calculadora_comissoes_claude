"""
receita/supabase/consultar_pagamentos.py — Verificação de pagamentos via AF.

A Análise Financeira (AF) é a fonte da verdade para status de pagamentos.
Não há tabela Supabase para histórico de pagamentos; a verificação deve ser
feita sempre sobre o df_af_full carregado do arquivo Excel.

As funções aqui são stubs mantidos para compatibilidade com imports existentes.
A lógica real de verificação está em receita/rastreamento/verificar_pagamentos.py.
"""

from __future__ import annotations

from typing import List


def consultar_pagamentos_processo(processo: str) -> List[dict]:
    """Stub — pagamentos são verificados via df_af_full (Excel), não via Supabase.

    Use receita.rastreamento.verificar_pagamentos.verificar() em vez desta função.

    Args:
        processo: Número do processo a consultar.

    Returns:
        Lista vazia (AF é consultada diretamente pelo rastreamento).
    """
    return []


def todas_nfs_pagas(processo: str) -> bool:
    """Stub — pagamentos são verificados via df_af_full (Excel), não via Supabase.

    Use receita.rastreamento.verificar_pagamentos.verificar() em vez desta função.

    Args:
        processo: Número do processo a verificar.

    Returns:
        False (stub seguro — bloqueia reconciliações sem dados reais da AF).
    """
    return False
