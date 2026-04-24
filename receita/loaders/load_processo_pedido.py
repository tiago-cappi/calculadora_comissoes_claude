"""
receita/loaders/load_processo_pedido.py — Carregamento da tabela Processo x PC.

Lê o arquivo "Processo x Pedido de Compra" (Excel), que vincula cada Processo
Filho ao seu Processo Pai via (Numero_pc + Código_Cliente).

Colunas esperadas no arquivo
-----------------------------
    Número           → numero_processo (código do processo filho, ex: "AC25.1234")
    Numero pc        → numero_pc       (pedido de compra / Processo Pai)
    Código do Cliente → codigo_cliente  (código único do cliente)

API pública
-----------
load(file_path)   → Tuple[ProcessoPedidoTabela, List[str]]
load_bytes(bytes) → Tuple[ProcessoPedidoTabela, List[str]]
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from receita.schemas.entrada import ProcessoPedidoItem, ProcessoPedidoTabela

# ---------------------------------------------------------------------------
# Mapeamento de colunas: nome padrão → lista de aliases aceitos no Excel
# ---------------------------------------------------------------------------
_COL_ALIASES: dict[str, list[str]] = {
    "Número": ["Número", "Numero", "número", "numero", "NÚMERO", "NUMERO", "Nº"],
    "Numero pc": [
        "Numero pc",
        "Número pc",
        "numero pc",
        "número pc",
        "NUMERO PC",
        "NÚMERO PC",
        "Numero PC",
        "Número PC",
        "PC",
        "Numero_pc",
        "Número_pc",
    ],
    "Código do Cliente": [
        "Código do Cliente",
        "Codigo do Cliente",
        "CÓDIGO DO CLIENTE",
        "CODIGO DO CLIENTE",
        "Cód. Cliente",
        "Cod. Cliente",
        "codigo_cliente",
        "código_cliente",
    ],
}

_COLUNAS_OBRIGATORIAS = list(_COL_ALIASES.keys())


def load(
    file_path: str,
) -> Tuple[ProcessoPedidoTabela, List[str]]:
    """Carrega a tabela Processo x Pedido de Compra a partir de um arquivo.

    Args:
        file_path: Caminho para o arquivo .xlsx de processos x PC.

    Returns:
        (ProcessoPedidoTabela com índices pré-construídos, lista de warnings).
        ProcessoPedidoTabela vazia se arquivo não encontrado.
    """
    path = Path(file_path)
    if not path.exists():
        return ProcessoPedidoTabela(registros=[]), [
            f"Arquivo não encontrado: {file_path}"
        ]

    try:
        file_bytes = path.read_bytes()
    except OSError as exc:
        return ProcessoPedidoTabela(registros=[]), [f"Erro ao ler {file_path}: {exc}"]

    return load_bytes(file_bytes)


def load_bytes(
    file_bytes: bytes,
) -> Tuple[ProcessoPedidoTabela, List[str]]:
    """Carrega a tabela Processo x Pedido de Compra a partir de bytes.

    Args:
        file_bytes: Conteúdo binário do arquivo .xlsx.

    Returns:
        (ProcessoPedidoTabela com índices pré-construídos, lista de warnings).
    """
    warnings: List[str] = []

    # --- Leitura do Excel ---------------------------------------------------
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
    except Exception as exc:
        return ProcessoPedidoTabela(registros=[]), [
            f"Erro ao ler Excel de processos x PC: {exc}"
        ]

    df.columns = df.columns.str.strip()

    # --- Mapeamento de aliases para nomes padrão ----------------------------
    col_map: dict[str, str] = {}
    for standard_name, aliases in _COL_ALIASES.items():
        for alias in aliases:
            if alias in df.columns:
                col_map[standard_name] = alias
                break

    # --- Validar colunas obrigatórias ---------------------------------------
    ausentes = [c for c in _COLUNAS_OBRIGATORIAS if c not in col_map]
    if ausentes:
        return ProcessoPedidoTabela(registros=[]), [
            f"Colunas obrigatórias ausentes na tabela PC: {ausentes}. "
            f"Colunas encontradas: {list(df.columns)}"
        ]

    # --- Selecionar e renomear colunas relevantes ---------------------------
    rename_map = {v: k for k, v in col_map.items()}
    df = df[[col_map[c] for c in _COLUNAS_OBRIGATORIAS]].copy()
    df = df.rename(columns=rename_map)

    # --- Limpar e normalizar ------------------------------------------------
    for col in _COLUNAS_OBRIGATORIAS:
        df[col] = df[col].astype(str).str.strip().str.upper()

    # Descartar linhas com campos vazios ou "NAN"
    before = len(df)
    mask_valido = (
        (df["Número"] != "") & (df["Número"] != "NAN")
        & (df["Numero pc"] != "") & (df["Numero pc"] != "NAN")
        & (df["Código do Cliente"] != "") & (df["Código do Cliente"] != "NAN")
    )
    df = df[mask_valido]
    descartadas = before - len(df)
    if descartadas:
        warnings.append(
            f"Tabela PC: {descartadas} linha(s) descartada(s) por campos vazios."
        )

    warnings.append(f"Tabela PC carregada: {len(df)} vínculo(s) processo→PC.")

    # --- Construir lista de ProcessoPedidoItem ------------------------------
    registros: List[ProcessoPedidoItem] = [
        ProcessoPedidoItem(
            numero_processo=row["Número"],
            numero_pc=row["Numero pc"],
            codigo_cliente=row["Código do Cliente"],
        )
        for _, row in df.iterrows()
    ]

    return ProcessoPedidoTabela(registros=registros), warnings
