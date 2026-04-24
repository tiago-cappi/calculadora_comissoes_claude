"""
receita/n8n/prep_pipeline.py — Nó N8N: Preparar Dados do Pipeline.

Executa as etapas de preparação do pipeline de faturamento para fornecer
ao pipeline de recebimento os DataFrames e o realizados_result necessários.

Etapas executadas internamente:
  1. scripts/loaders.execute()          → AC, AC_full, AF, devoluções, PC
  2. scripts/atribuicao.execute()       → df_atrib (para realizados)
  3. scripts/realizados.execute()       → realizados_result

Input (stdin JSON):
    project_dir   str  — Caminho absoluto do projeto
    mes           int  — Mês de apuração
    ano           int  — Ano de apuração
    caminho_ac    str  — Caminho relativo de analise-comercial.xlsx
    caminho_cp    str  — Caminho relativo de Classificação de Produtos.xlsx
    caminho_af    str  — Caminho relativo de analise-financeira.xlsx  (opcional)
    caminho_pc    str  — Caminho relativo de processo_pedido_compra.xlsx (opcional)
    caminho_dev   str  — Caminho relativo de devolucoes.xlsx  (opcional)

Output (stdout JSON):
    status              str   — "ok" | "error"
    df_ac_full_json     list  — AC completa (sem filtro de mês), com aliases
    df_ac_json          list  — AC filtrada pelo mês (com aliases)
    df_af_json          list  — Análise financeira filtrada pelo mês
    df_devolucoes_json  list  — Devoluções (pode ser vazio)
    tabela_pc_json      list  — [{numero_processo, numero_pc, codigo_cliente}]
    realizados_result   dict  — dataclasses.asdict(RealizadosResult)
    warnings            list
    errors              list
"""

from __future__ import annotations

import dataclasses
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def _read_bytes(project_dir: str, rel_path: Optional[str]) -> Optional[bytes]:
    """Lê arquivo como bytes; retorna None se não fornecido ou inexistente."""
    if not rel_path:
        return None
    p = Path(project_dir) / rel_path
    if p.exists():
        return p.read_bytes()
    return None


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Prepara todos os dados necessários para o pipeline de recebimento."""
    warnings: List[str] = []
    errors: List[str] = []

    project_dir = input_data.get("project_dir", ".")
    os.chdir(project_dir)
    if project_dir not in sys.path:
        sys.path.insert(0, project_dir)

    mes = int(input_data.get("mes", 10))
    ano = int(input_data.get("ano", 2025))

    _empty_result = {
        "status": "error",
        "df_ac_full_json": [],
        "df_ac_json": [],
        "df_af_json": [],
        "df_devolucoes_json": [],
        "tabela_pc_json": [],
        "realizados_result": None,
        "warnings": warnings,
        "errors": errors,
    }

    try:
        import scripts.loaders as loader
        import scripts.atribuicao as atrib
        import scripts.realizados as reais

        # ── 1. Loader ─────────────────────────────────────────────────────
        loader_result = loader.execute(
            mes=mes,
            ano=ano,
            file_analise_comercial=_read_bytes(project_dir, input_data.get("caminho_ac", "dados_entrada/analise-comercial.xlsx")),
            file_classificacao_produtos=_read_bytes(project_dir, input_data.get("caminho_cp", "dados_entrada/Classificação de Produtos.xlsx")),
            file_analise_financeira=_read_bytes(project_dir, input_data.get("caminho_af", "dados_entrada/analise-financeira.xlsx")),
            file_devolucoes=_read_bytes(project_dir, input_data.get("caminho_dev", "dados_entrada/devolucoes.xlsx")),
            file_rentabilidade=None,
            file_processo_pedido=_read_bytes(project_dir, input_data.get("caminho_pc", "dados_entrada/processo_pedido_compra.xlsx")),
        )
        warnings.extend(loader_result.warnings)

        if not loader_result.ok:
            errors.extend(loader_result.errors)
            _empty_result["errors"] = errors
            _empty_result["warnings"] = warnings
            return _empty_result

        ac = loader_result.analise_comercial
        ac_full = loader_result.analise_comercial_full

        # ── 2. Aplicar aliases ─────────────────────────────────────────────
        try:
            alias_map = atrib._load_config()["alias_map"]
            ac_resolved = atrib.apply_aliases_to_df(ac, alias_map)
            ac_full_resolved = atrib.apply_aliases_to_df(ac_full, alias_map)
        except Exception as exc_alias:
            warnings.append(f"prep_pipeline: apply_aliases falhou ({exc_alias}), usando AC sem aliases.")
            ac_resolved = ac
            ac_full_resolved = ac_full

        # ── 3. Atribuição (billing) ────────────────────────────────────────
        result_atrib = atrib.execute(ac_resolved)
        warnings.extend(getattr(result_atrib, "warnings", []))
        df_atrib = result_atrib.to_dataframe()

        # ── 4. Realizados ──────────────────────────────────────────────────
        result_reais = reais.execute(
            df_analise_comercial=ac_resolved,
            df_atribuicoes=df_atrib,
            df_fat_rent_gpe=None,
            mes=mes,
            ano=ano,
            df_ac_full=ac_full_resolved,
        )
        warnings.extend(result_reais.warnings)
        errors.extend(result_reais.errors)

        # ── 5. Serializar tabela_pc ────────────────────────────────────────
        tabela_pc = loader_result.processo_pedido
        tabela_pc_json = []
        if tabela_pc and hasattr(tabela_pc, "registros"):
            tabela_pc_json = [
                {
                    "numero_processo": r.numero_processo,
                    "numero_pc": r.numero_pc,
                    "codigo_cliente": r.codigo_cliente,
                }
                for r in tabela_pc.registros
            ]

        # ── 6. Serializar AF e devoluções ──────────────────────────────────
        af = loader_result.analise_financeira
        dev = loader_result.devolucoes

        return {
            "status": "ok",
            "df_ac_full_json": ac_full_resolved.to_dict(orient="records"),
            "df_ac_json": ac_resolved.to_dict(orient="records"),
            "df_af_json": af.to_dict(orient="records") if not af.empty else [],
            "df_devolucoes_json": dev.to_dict(orient="records") if not dev.empty else [],
            "tabela_pc_json": tabela_pc_json,
            "realizados_result": dataclasses.asdict(result_reais),
            "warnings": warnings,
            "errors": errors,
        }

    except Exception as exc:
        errors.append(f"prep_pipeline: {exc}")
        _empty_result["errors"] = errors
        _empty_result["warnings"] = warnings
        return _empty_result


if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    result = run(payload)
    print(json.dumps(result, ensure_ascii=False, default=str))
