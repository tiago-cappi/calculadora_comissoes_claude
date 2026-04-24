"""
Reconcilia ajustes quando o Processo Pai fecha.

Busca TODOS os históricos do Processo Pai (por numero_pc + codigo_cliente),
independentemente do flag ``reconciliado``, para garantir que o FCMP Real
seja calculado sobre o universo completo de adiantamentos.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Set

from receita.schemas.calculo import (
    ComissaoResult,
    FCMPResult,
    ReconciliacaoItem,
    ReconciliacaoResult,
)
from receita.supabase.consultar_historico import consultar_historicos_do_pai


def executar(
    comissao_result: ComissaoResult,
    fcmp_por_gl: Dict[str, FCMPResult],
    processos_aptos: Set[str],
    status_por_pai: Dict[str, Dict[str, Any]] | None = None,
) -> ReconciliacaoResult:
    warnings: List[str] = []
    itens: List[ReconciliacaoItem] = []
    total_por_gl: Dict[str, float] = defaultdict(float)

    if not processos_aptos:
        return ReconciliacaoResult(
            itens=[],
            total_por_gl={},
            warnings=["calcular_reconciliacao: nenhum processo apto - sem reconciliacao."],
        )

    # Deduzir os PCs aptos a partir de status_por_pai
    pcs_aptos: Dict[str, Dict[str, Any]] = {}
    if status_por_pai:
        for chave_str, status in status_por_pai.items():
            processos_faturados = {
                str(p).strip().upper()
                for p in status.get("processos_faturados", [])
            }
            if processos_faturados & processos_aptos:
                pc = str(status.get("numero_pc", "")).strip().upper()
                cli = str(status.get("codigo_cliente", "")).strip().upper()
                if pc and cli:
                    pcs_aptos[f"{pc}|{cli}"] = {"numero_pc": pc, "codigo_cliente": cli}

    if not pcs_aptos:
        return ReconciliacaoResult(
            itens=[],
            total_por_gl={},
            warnings=warnings + [
                "calcular_reconciliacao: nenhum PC apto identificado para reconciliacao."
            ],
        )

    historicos_por_gl: Dict[tuple, dict] = {}
    falhas_consulta: List[str] = []

    for chave_pc, info_pc in pcs_aptos.items():
        numero_pc = info_pc["numero_pc"]
        codigo_cliente = info_pc["codigo_cliente"]
        try:
            historicos = consultar_historicos_do_pai(numero_pc, codigo_cliente)
        except Exception as exc:
            falhas_consulta.append(f"PC {numero_pc}/{codigo_cliente}: {exc}")
            continue

        if not historicos:
            warnings.append(
                f"calcular_reconciliacao: PC {numero_pc} sem historicos no Supabase."
            )
            continue

        ja_reconciliado = all(h.reconciliado for h in historicos)
        if ja_reconciliado:
            warnings.append(
                f"calcular_reconciliacao: PC {numero_pc} ja reconciliado anteriormente "
                f"({len(historicos)} historico(s)) - recalculando para exibicao."
            )

        for historico in historicos:
            if not historico.nome:
                continue
            chave = (numero_pc, codigo_cliente, historico.nome)
            bucket = historicos_por_gl.setdefault(
                chave,
                {
                    "numero_pc": numero_pc,
                    "codigo_cliente": codigo_cliente,
                    "comissao_base": 0.0,
                    "fcmp_ponderado": 0.0,
                    "base_fcmp_real": 0.0,
                    "historicos_considerados": 0,
                    "detalhes": [],
                },
            )
            tipo_pag = str(getattr(historico, "tipo_pagamento", "") or "").strip().upper()
            is_adiantamento = tipo_pag == "ADIANTAMENTO"
            contrib_ponderada = historico.comissao_adiantada * historico.fcmp_aplicado
            bucket["comissao_base"] += historico.comissao_adiantada
            # FCMP Real = média ponderada apenas dos FCMPs pós-faturamento.
            # Adiantamentos (FCMP provisório = 1,0) são excluídos para não poluir a média.
            if not is_adiantamento:
                bucket["fcmp_ponderado"] += contrib_ponderada
                bucket["base_fcmp_real"] += historico.comissao_adiantada
            bucket["historicos_considerados"] += 1
            bucket["detalhes"].append({
                "processo": historico.processo,
                "documento": historico.documento,
                "mes_apuracao": historico.mes_apuracao,
                "ano_apuracao": historico.ano_apuracao,
                "comissao_adiantada": historico.comissao_adiantada,
                "fcmp_aplicado": historico.fcmp_aplicado,
                "tipo_pagamento": tipo_pag,
                "contribuicao_ponderada": contrib_ponderada,
                "contribui_fcmp_real": not is_adiantamento,
            })

    if falhas_consulta:
        warnings.append(
            "calcular_reconciliacao: falha ao consultar historico para "
            f"{len(falhas_consulta)} PC(s): {falhas_consulta[:5]}"
        )

    if not historicos_por_gl:
        return ReconciliacaoResult(
            itens=[],
            total_por_gl={},
            warnings=warnings + [
                "calcular_reconciliacao: nenhum historico encontrado para reconciliacao."
            ],
        )

    for (pc, cli, gl_nome), meta in historicos_por_gl.items():
        comissao_base = float(meta["comissao_base"])
        if comissao_base == 0.0:
            continue

        base_fcmp_real = float(meta["base_fcmp_real"])
        if base_fcmp_real > 0.0:
            fcmp_real = float(meta["fcmp_ponderado"]) / base_fcmp_real
        else:
            # Sem rows pós-faturamento: FCMP_real indefinido → mantém 1,0 (sem ajuste).
            fcmp_real = 1.0
        ajuste = comissao_base * (fcmp_real - 1.0)

        # Extrair processos distintos dos detalhes para exibição
        processos_envolvidos = sorted({
            str(d["processo"]) for d in meta["detalhes"] if d.get("processo")
        })

        itens.append(
            ReconciliacaoItem(
                gl_nome=gl_nome,
                numero_pc=pc,
                codigo_cliente=cli,
                processo=", ".join(processos_envolvidos) if processos_envolvidos else pc,
                comissao_adiantada=comissao_base,
                fcmp_real=fcmp_real,
                ajuste=ajuste,
                historicos_considerados=int(meta["historicos_considerados"]),
                detalhes_historicos=meta["detalhes"],
            )
        )
        total_por_gl[gl_nome] += ajuste

    warnings.append(
        f"calcular_reconciliacao: {len(itens)} ajuste(s) calculados para "
        f"{len(total_por_gl)} GL(s)."
    )

    return ReconciliacaoResult(
        itens=itens,
        total_por_gl=dict(total_por_gl),
        warnings=warnings,
    )
