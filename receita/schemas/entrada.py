"""
receita/schemas/entrada.py — Schemas de dados de entrada.

Define os contratos de dados para:
- ProcessoPedidoItem: uma linha da tabela "Processo x Pedido de Compra"
- ProcessoPedidoTabela: coleção de itens com métodos de consulta
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Dict, List, Optional, Tuple


def _normalizar_processo(valor: str) -> str:
    """Normaliza processo para comparação textual direta."""
    return str(valor).strip().upper()


def _normalizar_processo_digitos(valor: str) -> str:
    """Extrai dígitos e remove zeros à esquerda para lookup tolerante.

    Isso cobre variações comuns entre as fontes, como:
    - `00138047` vs `138047`
    - `138047.0` vs `138047`
    - `AC25.1234` vs `251234` quando ambas as fontes preservam os dígitos
    """
    texto = str(valor).strip()
    texto = re.sub(r"([.,])0+$", "", texto) if re.fullmatch(r"\d+[.,]0+", texto) else texto
    digitos = re.sub(r"\D", "", texto)
    if not digitos:
        return ""
    return digitos.lstrip("0") or "0"


@dataclass
class ProcessoPedidoItem:
    """Representa uma linha da tabela 'Processo x Pedido de Compra'.

    Vincula um Processo Filho ao seu Processo Pai via chave composta
    (numero_pc, codigo_cliente).

    Attributes:
        numero_processo: Número do processo filho (ex: "0141174").
        numero_pc: Número do pedido de compra / identificador do Processo Pai.
        codigo_cliente: Código único do cliente.
    """

    numero_processo: str
    numero_pc: str
    codigo_cliente: str

    def __post_init__(self) -> None:
        """Normaliza strings para maiúsculas e sem espaços extras."""
        self.numero_processo = str(self.numero_processo).strip().upper()
        self.numero_pc = str(self.numero_pc).strip().upper()
        self.codigo_cliente = str(self.codigo_cliente).strip().upper()


@dataclass
class ProcessoPedidoTabela:
    """Coleção de ProcessoPedidoItem com métodos de consulta eficientes.

    Constrói índices internos no momento da criação para buscas O(1).

    Attributes:
        registros: Lista de todos os ProcessoPedidoItem carregados.
    """

    registros: List[ProcessoPedidoItem] = field(default_factory=list)

    # Índices internos (construídos em __post_init__)
    _idx_por_processo: Dict[str, ProcessoPedidoItem] = field(
        default_factory=dict, repr=False, compare=False
    )
    _idx_por_processo_digitos: Dict[str, ProcessoPedidoItem] = field(
        default_factory=dict, repr=False, compare=False
    )
    _idx_por_pai: Dict[Tuple[str, str], List[ProcessoPedidoItem]] = field(
        default_factory=dict, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        """Constrói índices internos a partir dos registros."""
        self._construir_indices()

    def _construir_indices(self) -> None:
        """Reconstrói os índices de busca. Chame após modificar `registros`."""
        self._idx_por_processo = {}
        self._idx_por_processo_digitos = {}
        self._idx_por_pai = {}
        for item in self.registros:
            chave_processo = _normalizar_processo(item.numero_processo)
            self._idx_por_processo[chave_processo] = item

            chave_digitos = _normalizar_processo_digitos(item.numero_processo)
            if chave_digitos:
                self._idx_por_processo_digitos.setdefault(chave_digitos, item)

            chave_pai = (item.numero_pc, item.codigo_cliente)
            self._idx_por_pai.setdefault(chave_pai, []).append(item)

    def get_pai(self, numero_processo: str) -> Optional[Tuple[str, str]]:
        """Retorna (numero_pc, codigo_cliente) do Processo Pai de um filho.

        Args:
            numero_processo: Número do processo filho (normalizado internamente).

        Returns:
            Tupla (numero_pc, codigo_cliente) ou None se não encontrado.
        """
        chave = _normalizar_processo(numero_processo)
        item = self._idx_por_processo.get(chave)
        if item is None:
            chave_digitos = _normalizar_processo_digitos(numero_processo)
            if chave_digitos:
                item = self._idx_por_processo_digitos.get(chave_digitos)
        if item is None:
            return None
        return (item.numero_pc, item.codigo_cliente)

    def get_processos_do_pai(
        self, numero_pc: str, codigo_cliente: str
    ) -> List[str]:
        """Retorna todos os números de processo vinculados a um Processo Pai.

        Args:
            numero_pc: Número do pedido de compra do Pai.
            codigo_cliente: Código do cliente do Pai.

        Returns:
            Lista de números de processo filho (pode ser vazia).
        """
        chave = (
            str(numero_pc).strip().upper(),
            str(codigo_cliente).strip().upper(),
        )
        itens = self._idx_por_pai.get(chave, [])
        return [i.numero_processo for i in itens]

    def __len__(self) -> int:
        """Número de registros na tabela."""
        return len(self.registros)

    def __bool__(self) -> bool:
        """True se a tabela tem ao menos um registro."""
        return len(self.registros) > 0
