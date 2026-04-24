"""
receita/loaders/load_pagamentos_pendentes.py — STUB: Carregamento de pagamentos ERP.

Módulo reservado para futura integração com o relatório ERP de baixa de NFs.
Quando implementado, lerá o relatório de pagamentos exportado do ERP e
retornará um DataFrame com status de pagamento por NF/processo.

Schema esperado do arquivo ERP (reservado)
------------------------------------------
    Processo        text    — Processo vinculado
    NF              text    — Número da nota fiscal
    Status Pago     bool    — True quando baixada no ERP
    Data Vencimento date    — Data de vencimento (opcional)
    Valor NF        float   — Valor da nota fiscal

Por que retornar vazio é seguro
---------------------------------
    Retornar DataFrame vazio faz com que `verificar_pagamentos` retorne False,
    bloqueando reconciliações prematuras. Nenhuma comissão é paga sobre
    processo não confirmado como pago.

API pública
-----------
load(file_path)   → Tuple[pd.DataFrame, List[str]]   [STUB — retorna vazio]
load_bytes(bytes) → Tuple[pd.DataFrame, List[str]]   [STUB — retorna vazio]
"""

from __future__ import annotations

from typing import List, Tuple

import pandas as pd

# Colunas que o DataFrame retornará quando implementado
_COLUNAS_SCHEMA = ["Processo", "NF", "Status Pago", "Data Vencimento", "Valor NF"]

_WARNING_STUB = (
    "load_pagamentos_pendentes: integração ERP não implementada. "
    "Todos os pagamentos serão tratados como pendentes até implementação. "
    "Ver receita/supabase/consultar_pagamentos.py para schema reservado."
)


def load(
    file_path: str,
) -> Tuple[pd.DataFrame, List[str]]:
    """STUB — Carrega relatório de pagamentos ERP a partir de um arquivo.

    Quando implementado, lerá o arquivo ERP de baixa de NFs e retornará
    um DataFrame com status de pagamento por NF/processo.

    Args:
        file_path: Caminho para o arquivo de pagamentos exportado do ERP.

    Returns:
        (DataFrame vazio com schema correto, [warning de stub]).
    """
    return _stub_vazio()


def load_bytes(
    file_bytes: bytes,
) -> Tuple[pd.DataFrame, List[str]]:
    """STUB — Carrega relatório de pagamentos ERP a partir de bytes.

    Args:
        file_bytes: Conteúdo binário do relatório ERP.

    Returns:
        (DataFrame vazio com schema correto, [warning de stub]).
    """
    return _stub_vazio()


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------


def _stub_vazio() -> Tuple[pd.DataFrame, List[str]]:
    """Retorna DataFrame vazio com schema correto + warning de stub."""
    df_vazio = pd.DataFrame(columns=_COLUNAS_SCHEMA)
    return df_vazio, [_WARNING_STUB]
