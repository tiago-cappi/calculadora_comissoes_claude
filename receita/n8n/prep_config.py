"""
receita/n8n/prep_config.py — Nó N8N: Carregar Configuração.

Carrega toda a configuração do pipeline de recebimento a partir do
cache do Supabase (supabase_cache.json) e do fc_calculator.

Input (stdin JSON):
    project_dir  str  — Caminho absoluto do projeto
    mes          int  — Mês de apuração
    ano          int  — Ano de apuração

Output (stdout JSON):
    status           str   — "ok" | "error"
    config_comissao  list  — Regras de comissão do Supabase
    colaboradores    list  — Cadastro de colaboradores
    cargos           list  — Cadastro de cargos
    pesos_metas      dict  — {json.dumps([cargo, colab]): {componente: peso_pct}}
    fc_escada        dict  — {cargo_lower: {modo, num_degraus, piso_pct}}
    params           dict  — {cap_fc_max, cap_atingimento_max}
    warnings         list
    errors           list
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Carrega configuração do Supabase e serializa para N8N."""
    warnings: List[str] = []
    errors: List[str] = []

    project_dir = input_data.get("project_dir", ".")
    os.chdir(project_dir)
    if project_dir not in sys.path:
        sys.path.insert(0, project_dir)

    try:
        from scripts import supabase_loader as sl
        import scripts.fc_calculator as fc

        config_comissao = sl.load_json("config_comissao.json") or []
        colaboradores = sl.load_json("colaboradores.json") or []
        cargos = sl.load_json("cargos.json") or []

        pesos_indexed, escada_por_cargo, params = fc._load_config()

        # Serializar pesos_metas: chave tupla → string JSON array
        # Compatível com _converter_pesos_chave() em etapa_06
        pesos_json: Dict[str, Any] = {
            json.dumps([k[0], k[1]]): v
            for k, v in pesos_indexed.items()
        }

        # Serializar fc_escada: usar cargo_lower como chave simples
        # calcular_fcmp.executar() usa fc_escada.get(cargo_lower, {})
        escada_json: Dict[str, Any] = {}
        for (cargo_lower, colab_lower), v in escada_por_cargo.items():
            if not colab_lower:
                # Regra genérica do cargo — é o que calcular_fcmp usa
                escada_json[cargo_lower] = v
            # Ignorar overrides por colaborador (não usados no recebimento)

        return {
            "status": "ok",
            "config_comissao": config_comissao,
            "colaboradores": colaboradores,
            "cargos": cargos,
            "pesos_metas": pesos_json,
            "fc_escada": escada_json,
            "params": params if isinstance(params, dict) else (params[0] if params else {}),
            "warnings": warnings,
            "errors": errors,
        }

    except Exception as exc:
        errors.append(f"prep_config: {exc}")
        return {
            "status": "error",
            "config_comissao": [],
            "colaboradores": [],
            "cargos": [],
            "pesos_metas": {},
            "fc_escada": {},
            "params": {},
            "warnings": warnings,
            "errors": errors,
        }


if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    result = run(payload)
    print(json.dumps(result, ensure_ascii=False, default=str))
