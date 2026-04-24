"""
receita/rastreamento/determinar_reconciliacao.py — Decisão de reconciliação.

A reconciliação de um Processo Pai só é devida quando:
    1. STATUS_FATURAMENTO_COMPLETO == True  (todos os filhos têm Dt Emissão)
    2. STATUS_PAGAMENTO_COMPLETO   == True  (todas as NFs foram pagas no ERP)

Ambas as condições são necessárias:
    - Sem faturamento completo: o FCMP médio do Pai ainda pode mudar quando
      os demais processos filho forem faturados em meses futuros.
    - Sem pagamento completo: reconciliação pode gerar comissão sobre valor
      não recebido — risco financeiro para a empresa.

API pública
-----------
determinar(numero_pc, codigo_cliente, status_faturamento, status_pagamento) → bool
"""

from __future__ import annotations


def determinar(
    numero_pc: str,
    codigo_cliente: str,
    status_faturamento: bool,
    status_pagamento: bool,
) -> bool:
    """Decide se a reconciliação é devida para um Processo Pai.

    Args:
        numero_pc: Número do pedido de compra do Processo Pai.
        codigo_cliente: Código do cliente do Processo Pai.
        status_faturamento: True quando todos os processos filho do Pai
            possuem Dt Emissão preenchida na AC.
        status_pagamento: True quando todas as NFs do Pai foram confirmadas
            como pagas no ERP. Atualmente sempre False (STUB).

    Returns:
        True somente se ambos os status são True.
        False em qualquer outro caso — sem reconciliação.
    """
    return status_faturamento is True and status_pagamento is True
