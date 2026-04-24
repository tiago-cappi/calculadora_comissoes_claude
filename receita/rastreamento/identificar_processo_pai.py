"""
receita/rastreamento/identificar_processo_pai.py — Lookup de Processo Pai.

Dado um número de processo filho, retorna a chave do Processo Pai:
    (numero_pc, codigo_cliente)

Reutiliza o índice O(1) pré-construído em ProcessoPedidoTabela.

API pública
-----------
identificar(processo, tabela_pc) → Tuple[str, str] | None
"""

from __future__ import annotations

from typing import Optional, Tuple

from receita.schemas.entrada import ProcessoPedidoTabela


def identificar(
    processo: str,
    tabela_pc: ProcessoPedidoTabela,
) -> Optional[Tuple[str, str]]:
    """Retorna a chave do Processo Pai de um processo filho.

    Args:
        processo: Número do processo filho (ex: "AC25.1234").
        tabela_pc: Tabela carregada pelo load_processo_pedido.

    Returns:
        (numero_pc, codigo_cliente) se encontrado, None caso contrário.
    """
    if not tabela_pc:
        return None
    return tabela_pc.get_pai(processo)
