"""
scenario_runner.py — Comparação de Cenários de Comissão

Executa o pipeline N vezes com overrides de metas/pesos diferentes,
produzindo um JSON de comparação para análise pelo Claude.

Os overrides são aplicados IN-MEMORY (não persistem no Supabase).

Uso:
    python lean_conductor/scenario_runner.py --mes 10 --ano 2025 --scenarios cenarios.json

Formato do cenarios.json:
[
  {
    "nome": "Cenário Atual",
    "overrides": {}
  },
  {
    "nome": "Faturamento Agressivo",
    "overrides": {
      "pesos_metas": [
        {"cargo": "Gerente Comercial", "faturamento_linha": 50, "conversao_linha": 30, "rentabilidade": 20}
      ],
      "metas_aplicacao": [
        {"linha": "Recursos Hídricos", "tipo_meta": "faturamento_linha", "valor_meta": 700000}
      ]
    }
  }
]
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Comparação de Cenários de Comissão"
    )
    p.add_argument("--mes", type=int, required=True, help="Mês de apuração")
    p.add_argument("--ano", type=int, required=True, help="Ano de apuração")
    p.add_argument("--scenarios", type=str, required=True,
                   help="Caminho para o JSON de cenários")
    p.add_argument("--colaborador", type=str, default=None,
                   help="Filtrar por colaborador (opcional)")
    p.add_argument("--cross-selling", type=str, default="B", choices=["A", "B"])
    return p.parse_args()


def _load_scenarios(path: str) -> List[Dict[str, Any]]:
    """Carrega cenários do arquivo JSON."""
    scenarios_path = Path(path)
    if not scenarios_path.is_absolute():
        scenarios_path = ROOT / scenarios_path
    if not scenarios_path.exists():
        print(f"Arquivo de cenários não encontrado: {scenarios_path}")
        sys.exit(1)
    with open(scenarios_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _reset_supabase_cache() -> None:
    """Recarrega o cache do Supabase do disco para resetar overrides."""
    from scripts import supabase_loader as sl
    sl._CACHE.clear()
    sl._LOADED = False


def run_scenarios(
    mes: int,
    ano: int,
    scenarios: List[Dict[str, Any]],
    colaborador: Optional[str] = None,
    cross_selling: str = "B",
) -> Dict[str, Any]:
    """Executa o pipeline para cada cenário e compara resultados.

    Args:
        mes: Mês de apuração.
        ano: Ano de apuração.
        scenarios: Lista de cenários com nome e overrides.
        colaborador: Filtrar por nome.
        cross_selling: Opção A ou B.

    Returns:
        Dict com resultados de comparação.
    """
    from lean_conductor.pipeline_wrapper import run_pipeline

    results: List[Dict[str, Any]] = []

    for i, scenario in enumerate(scenarios):
        nome = scenario.get("nome", f"Cenário {i + 1}")
        overrides = scenario.get("overrides", {})

        print(f"\n{'─' * 50}")
        print(f"  Executando: {nome} ({i + 1}/{len(scenarios)})")
        print(f"{'─' * 50}")

        # Reset cache para começar limpo
        _reset_supabase_cache()

        pesos_ov = overrides.get("pesos_metas")
        metas_ov = overrides.get("metas_aplicacao")

        result = run_pipeline(
            mes=mes,
            ano=ano,
            colaborador=colaborador,
            cross_selling_opcao=cross_selling,
            quiet=True,
            pesos_overrides=pesos_ov,
            metas_overrides=metas_ov,
        )

        results.append({
            "cenario": nome,
            "overrides_aplicados": overrides,
            "status": result.get("status", "error"),
            "total_geral": result.get("total_geral", 0),
            "comissoes": result.get("comissoes", []),
            "erros": len(result.get("erros", [])),
        })

    # Reset final
    _reset_supabase_cache()

    # Montar comparação
    comparison = _build_comparison(results)

    # Salvar
    output_dir = ROOT / "saida" / f"{mes:02d}_{ano}"
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "scenarios_comparison.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(comparison, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n{'=' * 50}")
    print(f"  COMPARAÇÃO DE CENÁRIOS")
    print(f"{'=' * 50}")
    for r in results:
        print(f"  {r['cenario']:30s}  R$ {r['total_geral']:>12,.2f}  ({r['status']})")
    print(f"\n  JSON: {json_path}")

    return comparison


def _build_comparison(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Constrói tabela de comparação entre cenários."""
    if not results:
        return {"cenarios": [], "deltas": [], "por_colaborador": []}

    base = results[0]

    # Comparação de totais
    cenarios_resumo = []
    for r in results:
        delta = r["total_geral"] - base["total_geral"]
        delta_pct = (delta / base["total_geral"] * 100) if base["total_geral"] else 0
        cenarios_resumo.append({
            "cenario": r["cenario"],
            "total": r["total_geral"],
            "delta_vs_base": round(delta, 2),
            "delta_pct": round(delta_pct, 2),
            "status": r["status"],
        })

    # Comparação por colaborador
    all_colabs = set()
    for r in results:
        for c in r.get("comissoes", []):
            all_colabs.add(c.get("colaborador", ""))

    por_colaborador = []
    for colab in sorted(all_colabs):
        entry = {"colaborador": colab, "cenarios": {}}
        for r in results:
            valor = 0
            for c in r.get("comissoes", []):
                if c.get("colaborador") == colab:
                    valor = c.get("total_faturamento", 0)
                    break
            entry["cenarios"][r["cenario"]] = round(valor, 2)
        por_colaborador.append(entry)

    return {
        "cenarios": cenarios_resumo,
        "por_colaborador": por_colaborador,
        "base": base["cenario"],
    }


if __name__ == "__main__":
    args = _parse_args()
    scenarios = _load_scenarios(args.scenarios)
    run_scenarios(
        mes=args.mes,
        ano=args.ano,
        scenarios=scenarios,
        colaborador=args.colaborador,
        cross_selling=getattr(args, "cross_selling", "B"),
    )
