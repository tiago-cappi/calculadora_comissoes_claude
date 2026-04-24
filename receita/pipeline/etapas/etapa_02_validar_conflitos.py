"""
receita/pipeline/etapas/etapa_02_validar_conflitos.py — Validação de conflitos GL.

Input:
    df_ac_json          list  — DataFrame AC serializado (records)
    atribuicao_result   dict  — saída da etapa_03 (por_linha + elegiveis)
    saida_dir           str   — diretório de saída para alertas_MM_AAAA.txt
    mes                 int
    ano                 int

Output:
    conflitos           list  — descrições de conflito (vazia = sem conflitos)
    processos_bloqueados list — processos bloqueados por conflito
    arquivo_alerta      str?  — caminho do arquivo de alertas gerado
    warnings            list
    errors              list
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, List

import pandas as pd

from receita.alertas import gerar_alerta_gl
from receita.schemas.calculo import AtribuicaoResult, ElegivelGL
from receita.validadores import validar_conflito_gl


def _deserializar_atribuicao(data: Dict) -> AtribuicaoResult:
    """Reconstrói AtribuicaoResult a partir de dict JSON."""
    elegiveis = [
        ElegivelGL(
            nome=e["nome"],
            cargo=e["cargo"],
            linha=e["linha"],
            hierarquia=tuple(e.get("hierarquia", [])),
            taxa_efetiva=float(e.get("taxa_efetiva", 0.0)),
            especificidade=int(e.get("especificidade", 0)),
        )
        for e in data.get("elegiveis", [])
    ]

    por_linha: Dict = {}
    for linha_norm, lista in data.get("por_linha", {}).items():
        por_linha[linha_norm] = [
            ElegivelGL(
                nome=e["nome"],
                cargo=e["cargo"],
                linha=e["linha"],
                hierarquia=tuple(e.get("hierarquia", [])),
                taxa_efetiva=float(e.get("taxa_efetiva", 0.0)),
                especificidade=int(e.get("especificidade", 0)),
            )
            for e in lista
        ]

    return AtribuicaoResult(
        elegiveis=elegiveis,
        por_linha=por_linha,
        warnings=data.get("warnings", []),
    )


def run(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """Valida conflitos GL e gera arquivo de alertas se necessário."""
    warnings: List[str] = []
    errors: List[str] = []

    df_ac_json = input_data.get("df_ac_json", [])
    atribuicao_raw = input_data.get("atribuicao_result", {})
    saida_dir = str(input_data.get("saida_dir", "saida"))
    mes = int(input_data.get("mes", 0))
    ano = int(input_data.get("ano", 0))

    df_ac = pd.DataFrame(df_ac_json) if df_ac_json else pd.DataFrame()
    atribuicao_result = _deserializar_atribuicao(atribuicao_raw)

    conflitos = validar_conflito_gl.validar(df_ac, atribuicao_result)

    arquivo_alerta = None
    if conflitos:
        try:
            arquivo_alerta = gerar_alerta_gl.gerar(conflitos, saida_dir, mes, ano)
            warnings.append(
                f"etapa_02: {len(conflitos)} conflito(s) detectado(s). "
                f"Arquivo de alertas gerado: {arquivo_alerta}"
            )
        except OSError as exc:
            warnings.append(f"etapa_02: erro ao gerar alerta: {exc}")

    # Extrair processos bloqueados das mensagens de conflito
    processos_bloqueados = []
    for msg in conflitos:
        # Formato: "Processo XXXXX: Linhas [...]"
        if msg.startswith("Processo "):
            partes = msg.split(":")
            if partes:
                processo = partes[0].replace("Processo ", "").strip()
                processos_bloqueados.append(processo)

    return {
        "status": "ok",
        "conflitos": conflitos,
        "processos_bloqueados": processos_bloqueados,
        "arquivo_alerta": arquivo_alerta,
        "warnings": warnings,
        "errors": errors,
    }


if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    result = run(payload)
    print(json.dumps(result, ensure_ascii=False, default=str))
