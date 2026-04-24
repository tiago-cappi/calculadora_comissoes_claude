"""
receita/pipeline/etapas/etapa_06_calcular_fcmp.py — Cálculo de FCMP por GL.

Input:
    df_ac_full_json    list  — DataFrame AC completo serializado
    realizados_result  any   — RealizadosResult serializado (passa como-está)
    atribuicao_result  dict  — saída da etapa_03
    pesos_metas        dict  — {(cargo, colaborador): {componente: peso_pct}}
    fc_escada          dict  — {cargo_lower: {modo, num_degraus, piso_pct}}
    params             dict  — {cap_fc_max, cap_atingimento_max}

Output:
    fcmp_por_gl  dict  — {gl_nome: {gl_nome, fcmp_por_processo, warnings}}
    warnings     list
    errors       list
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List, Tuple

import pandas as pd

from receita.calculadores import calcular_fcmp
from receita.pipeline.etapas.etapa_02_validar_conflitos import _deserializar_atribuicao
from receita.schemas.calculo import FCMPProcesso


def _serializar_fcmp_result(result) -> Dict:
    """Serializa FCMPResult para dict JSON-safe."""
    fcmp_por_proc = {}
    for proc, fp in result.fcmp_por_processo.items():
        fcmp_por_proc[proc] = {
            "processo": fp.processo,
            "gl_nome": fp.gl_nome,
            "fcmp_rampa": fp.fcmp_rampa,
            "fcmp_aplicado": fp.fcmp_aplicado,
            "modo": fp.modo,
            "provisorio": fp.provisorio,
            "num_itens": fp.num_itens,
            "valor_faturado": fp.valor_faturado,
        }
    return {
        "gl_nome": result.gl_nome,
        "fcmp_por_processo": fcmp_por_proc,
        "warnings": result.warnings,
    }


def _converter_pesos_chave(pesos_raw: Dict) -> Dict[Tuple[str, str], Dict]:
    """Converte chaves de string '(cargo, colab)' para tupla."""
    resultado: Dict[Tuple[str, str], Dict] = {}
    for chave_str, valor in pesos_raw.items():
        # Suporte a formato '["cargo", "colab"]' ou '(cargo, colab)'
        try:
            parsed = json.loads(chave_str)
            if isinstance(parsed, list) and len(parsed) == 2:
                resultado[(str(parsed[0]), str(parsed[1]))] = valor
        except (json.JSONDecodeError, TypeError):
            # Tentar formato string simples como fallback
            resultado[(chave_str, "")] = valor
    return resultado


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Calcula FCMP para cada GL elegível."""
    warnings: List[str] = []
    errors: List[str] = []

    df_ac_json = input_data.get("df_ac_full_json", [])
    realizados_result = input_data.get("realizados_result")
    # Reconstruir RealizadosResult a partir de dict (desserialização N8N)
    if isinstance(realizados_result, dict):
        try:
            from scripts.realizados import RealizadosResult as _RR
            _valid = set(_RR.__dataclass_fields__.keys())
            realizados_result = _RR(**{k: v for k, v in realizados_result.items() if k in _valid})
        except Exception as _exc_rr:
            warnings.append(f"etapa_06: não foi possível reconstruir RealizadosResult: {_exc_rr}")
    atribuicao_raw = input_data.get("atribuicao_result", {})
    pesos_raw = input_data.get("pesos_metas", {})
    fc_escada = input_data.get("fc_escada", {})
    params = input_data.get("params", {"cap_fc_max": 1.0, "cap_atingimento_max": 1.2})

    df_ac_full = pd.DataFrame(df_ac_json) if df_ac_json else pd.DataFrame()
    atribuicao_result = _deserializar_atribuicao(atribuicao_raw)
    pesos_metas = _converter_pesos_chave(pesos_raw) if pesos_raw else {}

    if df_ac_full.empty:
        errors.append("etapa_06: df_ac_full vazio.")
        return {"status": "error", "errors": errors, "warnings": warnings}

    fcmp_por_gl: Dict[str, Dict] = {}

    for gl in atribuicao_result.elegiveis:
        result = calcular_fcmp.executar(
            df_ac_full=df_ac_full,
            realizados_result=realizados_result,
            gl=gl,
            pesos_metas=pesos_metas,
            fc_escada=fc_escada,
            params=params,
        )
        warnings.extend(result.warnings)
        fcmp_por_gl[gl.nome] = _serializar_fcmp_result(result)

    return {
        "status": "ok",
        "fcmp_por_gl": fcmp_por_gl,
        "warnings": warnings,
        "errors": errors,
    }


if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    result = run(payload)
    print(json.dumps(result, ensure_ascii=False, default=str))
