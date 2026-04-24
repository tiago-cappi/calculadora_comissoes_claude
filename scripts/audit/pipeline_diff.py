"""
pipeline_diff.py — Compara divergências entre pipeline de faturamento e recebimento.

Analisa como os mesmos processos foram tratados em ambos os pipelines,
detectando inconsistências de hierarquia, regras e colaboradores.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent.parent


def comparar(resultado_data: dict, mes: int, ano: int) -> dict:
    """Compara como faturamento e recebimento trataram os mesmos processos.

    Args:
        resultado_data: Conteúdo do resultado.json.
        mes: Mês de apuração.
        ano: Ano de apuração.

    Returns:
        Dict com processos analisados, divergências encontradas e recomendações.
    """
    divergencias = []
    processos_analisados = []

    try:
        comissoes_fat = resultado_data.get("comissoes", [])
        colaboradores_fat = {c.get("colaborador") for c in comissoes_fat}

        output_dir = ROOT / "saida" / f"{mes:02d}_{ano}"

        # Tentar ler dados de recebimento do resultado.json se disponível
        recebimento_data = resultado_data.get("recebimento", {})
        colaboradores_rec = set()

        if recebimento_data:
            for nome, dados in recebimento_data.items():
                colaboradores_rec.add(nome)
        else:
            # Tentar inferir de arquivos .md de recebimento
            if output_dir.exists():
                mds_rec = list(output_dir.glob(f"recebimento_*_{mes:02d}_{ano}.md"))
                for md in mds_rec:
                    stem = md.stem
                    nome = stem.replace("recebimento_", "").replace(f"_{mes:02d}_{ano}", "").replace("_", " ").title()
                    colaboradores_rec.add(nome)

        # Detectar divergência 1: Colaboradores em faturamento mas não em recebimento
        # (apenas Gerentes de Linha deveriam estar em recebimento)
        so_faturamento = colaboradores_fat - colaboradores_rec
        so_recebimento = colaboradores_rec - colaboradores_fat

        if so_recebimento:
            divergencias.append({
                "tipo": "colaborador_apenas_recebimento",
                "descricao": f"{len(so_recebimento)} colaborador(es) no recebimento sem comissão de faturamento",
                "colaboradores": list(so_recebimento),
                "recomendacao": "Verifique se esses colaboradores são Gerentes de Linha (esperado) ou se há dados de faturamento ausentes.",
            })

        # Detectar divergência 2: Verificar avisos de atribuição vs recebimento
        avisos_atrib = []
        for etapa in resultado_data.get("etapas", []):
            if etapa.get("nome") == "atribuicao":
                avisos_atrib = etapa.get("avisos", [])
                break

        processos_sem_vinculo = set()
        for aviso in avisos_atrib:
            import re
            m = re.search(r"processo\s+(\w+)", aviso, re.IGNORECASE)
            if m:
                processos_sem_vinculo.add(m.group(1))

        processos_analisados = [
            {"processo": p, "status": "sem_atribuicao_faturamento"}
            for p in list(processos_sem_vinculo)[:20]
        ]

        if processos_sem_vinculo:
            divergencias.append({
                "tipo": "processos_sem_atribuicao_faturamento",
                "descricao": f"{len(processos_sem_vinculo)} processo(s) sem atribuição no faturamento",
                "processos": list(processos_sem_vinculo)[:10],
                "recomendacao": "Esses processos podem ter comissão de recebimento mas não de faturamento. "
                               "Verifique se há regras de comissão faltando em config_comissao.",
            })

        return {
            "status": "ok",
            "mes": mes,
            "ano": ano,
            "colaboradores_faturamento": len(colaboradores_fat),
            "colaboradores_recebimento": len(colaboradores_rec),
            "processos_analisados": len(processos_analisados),
            "total_divergencias": len(divergencias),
            "divergencias": divergencias,
            "recomendacoes": [d["recomendacao"] for d in divergencias if d.get("recomendacao")],
        }

    except Exception as exc:
        return {
            "status": "error",
            "mes": mes,
            "ano": ano,
            "erro": str(exc),
            "divergencias": [],
            "processos_analisados": 0,
            "total_divergencias": 0,
        }
