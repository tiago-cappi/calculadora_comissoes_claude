"""
receita/validadores/validar_processo_pai.py — Validação da tabela Processo x PC.

Verifica integridade da tabela "Processo x Pedido de Compra":
1. Unicidade de (Numero_pc, Código_Cliente) como chave do Processo Pai.
   → A mesma chave pode ter múltiplos processos filho, mas não duplicatas
     do mesmo processo filho.
2. Unicidade de processo filho: nenhum processo pode aparecer vinculado
   a dois Pais diferentes.

API pública
-----------
validar(processo_pedido) → List[str]   (lista de mensagens de erro)
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Set, Tuple

from receita.schemas.entrada import ProcessoPedidoTabela


def validar(processo_pedido: ProcessoPedidoTabela) -> List[str]:
    """Valida integridade da tabela Processo x Pedido de Compra.

    Verifica:
    - Nenhum processo filho aparece vinculado a dois Pais diferentes.
    - Nenhum processo filho aparece duplicado dentro do mesmo Pai.

    Args:
        processo_pedido: Tabela carregada pelo load_processo_pedido.

    Returns:
        Lista de strings com erros encontrados.
        Lista vazia = tabela íntegra.
    """
    erros: List[str] = []

    if not processo_pedido:
        return []  # Tabela vazia → sem o que validar

    # --- Verificar unicidade de processo filho (um filho → um pai apenas) ---
    # {numero_processo: set de chaves_pai}
    pais_por_processo: Dict[str, Set[Tuple[str, str]]] = defaultdict(set)
    for item in processo_pedido.registros:
        chave_pai = (item.numero_pc, item.codigo_cliente)
        pais_por_processo[item.numero_processo].add(chave_pai)

    for processo, chaves_pai in pais_por_processo.items():
        if len(chaves_pai) > 1:
            pais_desc = sorted([f"(PC={pc}, CLI={cli})" for pc, cli in chaves_pai])
            erros.append(
                f"Processo filho '{processo}' vinculado a múltiplos Pais: {pais_desc}. "
                "Cada processo filho deve ter exatamente um Processo Pai."
            )

    # --- Verificar duplicatas de processo filho dentro do mesmo Pai ----------
    # {chave_pai: list de processos}
    processos_por_pai: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for item in processo_pedido.registros:
        chave_pai = (item.numero_pc, item.codigo_cliente)
        processos_por_pai[chave_pai].append(item.numero_processo)

    for chave_pai, processos in processos_por_pai.items():
        pc, cli = chave_pai
        vistos: Set[str] = set()
        duplicados: Set[str] = set()
        for p in processos:
            if p in vistos:
                duplicados.add(p)
            vistos.add(p)
        if duplicados:
            erros.append(
                f"Pai (PC={pc}, CLI={cli}): processos filho duplicados: "
                f"{sorted(duplicados)}. Remover linhas duplicadas do Excel."
            )

    return erros
