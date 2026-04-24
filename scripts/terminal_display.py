"""scripts/terminal_display.py — Utilitários de exibição tabular no terminal.

Centraliza a renderização de tabelas para todos os scripts do pipeline,
substituindo pandas .to_string() e tabelas manuais por saída rica e legível.
Usa a biblioteca `rich` para bordas, cores no cabeçalho e quebra automática
de células largas.
"""
from __future__ import annotations

from typing import Any

import pandas as pd
from rich.console import Console
from rich.table import Table

_console = Console()

# Largura máxima padrão por coluna (caracteres).
# Valores maiores são quebrados em múltiplas linhas dentro da célula.
_DEFAULT_MAX_COL_WIDTH = 28


def print_df(
    df: pd.DataFrame,
    title: str | None = None,
    max_col_width: int = _DEFAULT_MAX_COL_WIDTH,
    max_rows: int = 500,
) -> None:
    """Exibe um DataFrame pandas em tabela rica no terminal.

    Args:
        df: DataFrame a exibir.
        title: Título opcional exibido acima da tabela.
        max_col_width: Largura máxima de cada coluna em caracteres.
            Conteúdo excedente é quebrado em linhas adicionais dentro
            da mesma célula.
        max_rows: Limite de linhas exibidas.
    """
    if df.empty:
        _console.print("  [dim](sem dados)[/dim]")
        return

    table = _make_table(title)

    for col in df.columns:
        justify = "right" if pd.api.types.is_numeric_dtype(df[col]) else "left"
        table.add_column(
            str(col),
            justify=justify,
            overflow="fold",
            max_width=max_col_width,
            no_wrap=False,
        )

    for _, row in df.head(max_rows).iterrows():
        table.add_row(*[_fmt_cell(v) for v in row])

    if len(df) > max_rows:
        table.caption = f"[dim]Mostrando {max_rows} de {len(df)} linhas[/dim]"

    _console.print(table)


def print_rows(
    rows: list[dict],
    columns: list[str],
    title: str | None = None,
    numbered: bool = False,
    max_col_width: int = _DEFAULT_MAX_COL_WIDTH,
    max_rows: int = 500,
) -> None:
    """Exibe lista de dicts como tabela rica no terminal.

    Args:
        rows: Lista de dicionários com os dados.
        columns: Colunas a exibir (define também a ordem).
        title: Título opcional exibido acima da tabela.
        numbered: Se True, adiciona coluna de índice numérico à esquerda.
        max_col_width: Largura máxima de cada coluna em caracteres.
        max_rows: Limite de linhas exibidas.
    """
    if not rows:
        _console.print("  [dim](nenhum registro encontrado)[/dim]")
        return

    table = _make_table(title)

    if numbered:
        num_width = max(2, len(str(min(len(rows), max_rows))))
        table.add_column("#", style="dim", width=num_width, no_wrap=True, justify="right")

    for col in columns:
        sample_vals = [
            rows[i].get(col)
            for i in range(min(5, len(rows)))
            if rows[i].get(col) is not None
        ]
        is_num = bool(sample_vals) and all(isinstance(v, (int, float)) for v in sample_vals)
        table.add_column(
            col,
            justify="right" if is_num else "left",
            overflow="fold",
            max_width=max_col_width,
            no_wrap=False,
        )

    for i, row in enumerate(rows[:max_rows], 1):
        vals = [_fmt_cell(row.get(col)) for col in columns]
        if numbered:
            table.add_row(str(i), *vals)
        else:
            table.add_row(*vals)

    if len(rows) > max_rows:
        table.caption = f"[dim]Mostrando {max_rows} de {len(rows)} registros[/dim]"

    _console.print(table)


def _make_table(title: str | None) -> Table:
    return Table(
        title=title,
        show_header=True,
        header_style="bold cyan",
        border_style="dim",
        show_lines=True,
        pad_edge=True,
        expand=False,
    )


def _fmt_cell(v: Any) -> str:
    """Converte qualquer valor de célula para string de exibição."""
    if v is None:
        return ""
    if isinstance(v, float):
        if pd.isna(v):
            return ""
        # Remove trailing zeros (e.g. 1.0 → "1", 0.85 → "0.85")
        return f"{v:g}"
    if isinstance(v, bool):
        return "✓" if v else "✗"
    return str(v)
