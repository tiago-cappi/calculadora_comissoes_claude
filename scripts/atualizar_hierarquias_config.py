"""
atualizar_hierarquias_config.py

Prepara o `configuracoes_comissoes.xlsx` para edição manual pelo usuário:

1. Esvazia a aba `config_comissao` (mantém apenas o cabeçalho). O usuário
   preencherá manualmente, linha por linha, com a especificidade desejada
   (regra geral por linha, específica por grupo/subgrupo, etc).

2. Popula a aba `classificacao_produtos` com todas as combinações únicas de
   (Linha, Grupo, Subgrupo, Tipo de Mercadoria, Fabricante) extraídas de
   `dados_entrada/Classificação de Produtos.xlsx`. Serve como catálogo de
   referência para o usuário consultar ao preencher `config_comissao`.

As demais 13 abas do workbook são preservadas sem alteração.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).parent.parent
CLASSIF_PATH = ROOT / "dados_entrada" / "Classificação de Produtos.xlsx"
CONFIG_PATH = ROOT / "configuracoes_comissoes.xlsx"

HIER_COLS_SRC = ["Linha", "Grupo", "Subgrupo", "Tipo de Mercadoria", "Fabricante"]
HIER_COLS_DST = ["linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante"]

CONFIG_COMISSAO_COLUMNS = [
    "linha",
    "grupo",
    "subgrupo",
    "tipo_mercadoria",
    "fabricante",
    "aplicacao",
    "cargo",
    "colaborador",
    "fatia_cargo",
    "taxa_rateio_maximo_pct",
    "ativo",
]


def extrair_hierarquias_unicas() -> pd.DataFrame:
    if not CLASSIF_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {CLASSIF_PATH}")
    df = pd.read_excel(CLASSIF_PATH, engine="openpyxl")
    faltando = [c for c in HIER_COLS_SRC if c not in df.columns]
    if faltando:
        raise RuntimeError(
            f"Colunas ausentes em {CLASSIF_PATH.name}: {faltando}. "
            f"Presentes: {list(df.columns)}"
        )
    uniq = (
        df[HIER_COLS_SRC]
        .drop_duplicates()
        .sort_values(HIER_COLS_SRC, na_position="last")
        .reset_index(drop=True)
    )
    uniq.columns = HIER_COLS_DST
    return uniq


def main() -> None:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {CONFIG_PATH}\n"
            f"Gere-o com: python scripts/gerar_template_excel.py"
        )

    print(f"Lendo hierarquias de: {CLASSIF_PATH.name}")
    hierarquias = extrair_hierarquias_unicas()
    print(f"  -> {len(hierarquias)} combinações únicas")

    print(f"\nLendo workbook: {CONFIG_PATH.name}")
    wb = pd.read_excel(CONFIG_PATH, sheet_name=None, engine="openpyxl")

    cfg_antes = len(wb.get("config_comissao", pd.DataFrame()))
    wb["config_comissao"] = pd.DataFrame(columns=CONFIG_COMISSAO_COLUMNS)
    print(f"\nconfig_comissao: {cfg_antes} -> 0 linhas (esvaziada)")

    cat_antes = len(wb.get("classificacao_produtos", pd.DataFrame()))
    wb["classificacao_produtos"] = hierarquias.copy()
    print(f"classificacao_produtos: {cat_antes} -> {len(hierarquias)} linhas (catálogo)")

    print(f"\nEscrevendo {CONFIG_PATH.name}...")
    with pd.ExcelWriter(CONFIG_PATH, engine="openpyxl") as writer:
        for sheet_name, df in wb.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print("\n[OK] Concluído.")
    print(f"     Abra {CONFIG_PATH.name}:")
    print(f"       - aba 'classificacao_produtos' = catálogo para consulta")
    print(f"       - aba 'config_comissao' = preencha regras com a especificidade desejada")


if __name__ == "__main__":
    main()
