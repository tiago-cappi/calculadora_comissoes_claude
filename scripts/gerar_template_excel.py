"""
gerar_template_excel.py — Gera configuracoes_comissoes.xlsx a partir do Supabase

Script de bootstrap (execução única). Lê todas as 15 tabelas do schema
'comissoes' no Supabase via o loader original (scripts/supabase_loader.py)
e grava um arquivo Excel com uma aba por tabela no formato que o novo
scripts/excel_config_loader.py consome.

Uso
---
    python scripts/gerar_template_excel.py
    python scripts/gerar_template_excel.py --output configuracoes_comissoes.xlsx

Após rodar este script, o Excel passa a ser a fonte de verdade — o
supabase_loader.py é substituído por um shim que delega ao excel_config_loader.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

# Este script é autossuficiente: consulta o Supabase diretamente via
# PostgREST, sem depender do supabase_loader (que virou shim).

_SUPABASE_URL = "https://zkpwdufnvktwoqwbpfdk.supabase.co"
_SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InprcHdkdWZudmt0d29xd2JwZmRrIiwicm9"
    "sZSI6ImFub24iLCJpYXQiOjE3NDc4NTE5NDIsImV4cCI6MjA2MzQyNzk0Mn0"
    ".xQt8-D65XC9DysMMp5Ms-XvwZPjzBh5nju-jh3r6LE0"
)
_SCHEMA = "comissoes"


def _query_table(table: str) -> List[Dict[str, Any]]:
    """Consulta direta ao Supabase via REST (paginada)."""
    all_rows: List[Dict[str, Any]] = []
    offset = 0
    page_size = 1000
    while True:
        url = (
            f"{_SUPABASE_URL}/rest/v1/{table}"
            f"?select=*&limit={page_size}&offset={offset}"
        )
        req = urllib.request.Request(url, method="GET")
        req.add_header("apikey", _SUPABASE_KEY)
        req.add_header("Authorization", f"Bearer {_SUPABASE_KEY}")
        req.add_header("Accept", "application/json")
        req.add_header("Accept-Profile", _SCHEMA)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        if not isinstance(data, list):
            raise RuntimeError(f"Resposta inesperada de {table}: {data}")
        all_rows.extend(data)
        if len(data) < page_size:
            break
        offset += page_size
    return all_rows


def _q(table: str) -> List[Dict[str, Any]]:
    try:
        return _query_table(table)
    except Exception as e:
        print(f"  [AVISO] Falha ao consultar {table}: {e}")
        return []


def _df_from_rows(rows: List[Dict[str, Any]], cols: List[str]) -> pd.DataFrame:
    """Cria DataFrame com colunas fixas (preserva ordem, aceita rows vazias)."""
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows)
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]


def _build_workbook() -> Dict[str, pd.DataFrame]:
    print("Consultando Supabase (schema: comissoes)...")

    sheets: Dict[str, pd.DataFrame] = {}

    # params: 1 linha → transforma em key-value (chave, valor)
    params_rows = _q("params")
    if params_rows:
        pairs = []
        for k, v in params_rows[0].items():
            if v is not None:
                pairs.append({"chave": k, "valor": v})
        sheets["params"] = pd.DataFrame(pairs, columns=["chave", "valor"])
    else:
        sheets["params"] = pd.DataFrame(columns=["chave", "valor"])
    print(f"  params: {len(sheets['params'])} linhas")

    # colaboradores
    sheets["colaboradores"] = _df_from_rows(
        _q("colaboradores"),
        ["id_colaborador", "nome_colaborador", "cargo"],
    )
    print(f"  colaboradores: {len(sheets['colaboradores'])} linhas")

    # cargos
    sheets["cargos"] = _df_from_rows(
        _q("cargos"),
        ["nome_cargo", "tipo_cargo", "tipo_comissao"],
    )
    print(f"  cargos: {len(sheets['cargos'])} linhas")

    # config_comissao
    sheets["config_comissao"] = _df_from_rows(
        _q("config_comissao"),
        [
            "linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante", "aplicacao",
            "cargo", "colaborador",
            "fatia_cargo", "taxa_rateio_maximo_pct", "ativo",
        ],
    )
    print(f"  config_comissao: {len(sheets['config_comissao'])} linhas")

    # pesos_metas
    sheets["pesos_metas"] = _df_from_rows(
        _q("pesos_metas"),
        [
            "cargo", "colaborador", "linha",
            "faturamento_linha", "rentabilidade", "conversao_linha",
            "faturamento_individual", "conversao_individual", "retencao_clientes",
            "meta_fornecedor_1", "meta_fornecedor_2",
        ],
    )
    print(f"  pesos_metas: {len(sheets['pesos_metas'])} linhas")

    # fc_escada_cargos
    sheets["fc_escada_cargos"] = _df_from_rows(
        _q("fc_escada_cargos"),
        ["cargo", "modo", "num_degraus", "piso_pct"],
    )
    print(f"  fc_escada_cargos: {len(sheets['fc_escada_cargos'])} linhas")

    # cross_selling
    sheets["cross_selling"] = _df_from_rows(
        _q("cross_selling"),
        ["colaborador", "taxa_cross_selling_pct"],
    )
    print(f"  cross_selling: {len(sheets['cross_selling'])} linhas")

    # metas_individuais
    sheets["metas_individuais"] = _df_from_rows(
        _q("metas_individuais"),
        ["colaborador", "cargo", "tipo_meta", "valor_meta"],
    )
    print(f"  metas_individuais: {len(sheets['metas_individuais'])} linhas")

    # metas_aplicacao
    sheets["metas_aplicacao"] = _df_from_rows(
        _q("metas_aplicacao"),
        [
            "linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante", "aplicacao",
            "tipo_meta", "valor_meta",
        ],
    )
    print(f"  metas_aplicacao: {len(sheets['metas_aplicacao'])} linhas")

    # meta_rentabilidade
    sheets["meta_rentabilidade"] = _df_from_rows(
        _q("meta_rentabilidade"),
        [
            "linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante", "aplicacao",
            "referencia_media_ponderada_pct", "meta_rentabilidade_alvo_pct",
        ],
    )
    print(f"  meta_rentabilidade: {len(sheets['meta_rentabilidade'])} linhas")

    # metas_fornecedores
    sheets["metas_fornecedores"] = _df_from_rows(
        _q("metas_fornecedores"),
        ["linha", "fornecedor", "moeda", "meta_anual"],
    )
    print(f"  metas_fornecedores: {len(sheets['metas_fornecedores'])} linhas")

    # monthly_avg_rates
    sheets["monthly_avg_rates"] = _df_from_rows(
        _q("monthly_avg_rates"),
        ["moeda", "ano", "mes", "taxa_media"],
    )
    print(f"  monthly_avg_rates: {len(sheets['monthly_avg_rates'])} linhas")

    # aliases
    sheets["aliases"] = _df_from_rows(
        _q("aliases"),
        ["entidade", "alias", "nome_padrao"],
    )
    print(f"  aliases: {len(sheets['aliases'])} linhas")

    # enum_tipo_meta
    sheets["enum_tipo_meta"] = _df_from_rows(
        _q("enum_tipo_meta"),
        ["tipo_meta", "escopo", "descricao"],
    )
    print(f"  enum_tipo_meta: {len(sheets['enum_tipo_meta'])} linhas")

    # classificacao-produtos (nome da tabela tem hífen no Supabase)
    sheets["classificacao_produtos"] = _df_from_rows(
        _q("classificacao-produtos"),
        ["codigo_produto", "linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante", "aplicacao"],
    )
    print(f"  classificacao_produtos: {len(sheets['classificacao_produtos'])} linhas")

    return sheets


def _write_workbook(sheets: Dict[str, pd.DataFrame], output: Path) -> None:
    print(f"\nGravando {output}...")
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)
    print(f"OK: {output}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    if getattr(sys, "frozen", False):
        default_out = Path(sys.executable).parent / "configuracoes_comissoes.xlsx"
    else:
        default_out = Path(__file__).parent.parent / "configuracoes_comissoes.xlsx"
    parser.add_argument("--output", type=Path, default=default_out,
                        help="Caminho do Excel a gerar (padrão: raiz do projeto)")
    args = parser.parse_args()

    sheets = _build_workbook()
    _write_workbook(sheets, args.output)
    print("\nBootstrap concluído. Use 'python scripts/excel_config_loader.py' para verificar.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
