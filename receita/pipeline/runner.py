"""
receita/pipeline/runner.py — Orquestrador do pipeline de comissões por Recebimento.

Executa as etapas 01–14 em sequência. Etapas críticas (01–07) encerram o
pipeline em caso de falha. Etapas não-críticas (08–14) geram warning e
continuam.

API pública
-----------
executar(df_analise_financeira, df_ac_full, realizados_result, tabela_pc,
         df_devolucoes, mes, ano, saida_dir, config, ...) → PipelineRecebimentoResult
"""

from __future__ import annotations

import traceback
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from receita.calculadores import (
    atribuir_gls,
    calcular_comissao,
    calcular_estornos,
    calcular_fcmp,
    calcular_reconciliacao,
    calcular_tcmp,
    mapear_documentos,
)
from receita.rastreamento import (
    determinar_reconciliacao,
    identificar_processo_pai,
    verificar_faturamento,
    verificar_pagamentos,
)
from receita.schemas.calculo import (
    AtribuicaoResult,
    ComissaoResult,
    EstornosResult,
    FCMPResult,
    ReconciliacaoResult,
    TCMPResult,
)
from receita.schemas.entrada import ProcessoPedidoTabela
from receita.schemas.pipeline import PipelineRecebimentoResult
from receita.validadores import validar_conflito_gl, validar_processo_pai


def executar(
    df_analise_financeira: pd.DataFrame,
    df_ac_full: pd.DataFrame,
    realizados_result: Any,
    tabela_pc: ProcessoPedidoTabela,
    df_devolucoes: Optional[pd.DataFrame],
    mes: int,
    ano: int,
    saida_dir: str,
    config_comissao: List[Dict] = None,
    colaboradores: List[Dict] = None,
    cargos: List[Dict] = None,
    pesos_metas: Dict[Tuple, Dict] = None,
    fc_escada: Dict[str, Dict] = None,
    params: Dict[str, Any] = None,
    processos_bloqueados: Optional[List[str]] = None,
    df_af_full: Optional[pd.DataFrame] = None,
) -> PipelineRecebimentoResult:
    """Executa o pipeline completo de comissões por Recebimento.

    Args:
        df_analise_financeira: DataFrame da AF filtrado pelo mês/ano.
        df_ac_full: DataFrame da AC SEM filtro de mês.
        realizados_result: RealizadosResult do pipeline de faturamento.
        tabela_pc: Tabela "Processo x Pedido de Compra".
        df_devolucoes: DataFrame de devoluções (pode ser None).
        mes: Mês de apuração (1–12).
        ano: Ano de apuração.
        saida_dir: Diretório de saída (ex: "saida/10_2025").
        config_comissao: Regras de comissão do Supabase.
        colaboradores: Cadastro de colaboradores do Supabase.
        cargos: Cadastro de cargos do Supabase.
        pesos_metas: {(cargo, colaborador): {componente: peso_pct}}.
        fc_escada: {cargo_lower: {modo, num_degraus, piso_pct}}.
        params: {cap_fc_max, cap_atingimento_max}.
        processos_bloqueados: Processos excluídos por conflito GL.
        df_af_full: DataFrame da AF SEM filtro de data (todos os meses).
            Usado na etapa 8 para verificar pagamentos do Processo Pai.
            Se None, a verificação de pagamentos retorna False com aviso.

    Returns:
        PipelineRecebimentoResult com todos os resultados e status.
    """
    result = PipelineRecebimentoResult(tabela_pc=tabela_pc, df_analise_comercial=df_ac_full, realizados_result=realizados_result)
    warnings = result.warnings
    errors = result.errors

    config_comissao = config_comissao or []
    colaboradores = colaboradores or []
    cargos = cargos or []
    pesos_metas = pesos_metas or {}
    fc_escada = fc_escada or {}
    params = params or {"cap_fc_max": 1.0, "cap_atingimento_max": 1.2}
    processos_bloqueados_set = set(processos_bloqueados or [])

    def _debug_stage(level: str, stage: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        try:
            from lean_conductor.live_debug import log_current_event

            log_current_event(level, "receita.runner", stage, message, details or {})
        except Exception:
            pass

    def _montar_payload_historico(reconciliacao_itens_raw: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        tcmp_json = {
            "tcmp_por_processo": tcmp.tcmp_por_processo,
            "detalhes": tcmp.detalhes,
            "warnings": tcmp.warnings,
        }
        fcmp_json = {}
        for gl_nome, fr in fcmp_por_gl.items():
            fcmp_json[gl_nome] = {
                "gl_nome": fr.gl_nome,
                "fcmp_por_processo": {
                    p: {
                        "processo": fp.processo,
                        "gl_nome": fp.gl_nome,
                        "fcmp_rampa": fp.fcmp_rampa,
                        "fcmp_aplicado": fp.fcmp_aplicado,
                        "modo": fp.modo,
                        "provisorio": fp.provisorio,
                        "num_itens": fp.num_itens,
                        "valor_faturado": fp.valor_faturado,
                    }
                    for p, fp in fr.fcmp_por_processo.items()
                },
                "detalhes": fr.detalhes,
                "warnings": fr.warnings,
            }
        tabela_pc_json = (
            [
                {
                    "numero_processo": item.numero_processo,
                    "numero_pc": item.numero_pc,
                    "codigo_cliente": item.codigo_cliente,
                }
                for item in tabela_pc.registros
            ]
            if tabela_pc
            else []
        )
        comissao_json = [
            {
                "gl_nome": item.gl_nome,
                "processo": item.processo,
                "documento": item.documento,
                "nf_extraida": item.nf_extraida,
                "status_processo": item.status_processo,
                "linha_negocio": item.linha_negocio,
                "valor_documento": item.valor_documento,
                "tcmp": item.tcmp,
                "fcmp_rampa": item.fcmp_rampa,
                "fcmp_aplicado": item.fcmp_aplicado,
                "fcmp_considerado": item.fcmp_considerado,
                "fcmp_modo": item.fcmp_modo,
                "tipo_pagamento": item.tipo_pagamento,
                "comissao_potencial": item.comissao_potencial,
                "comissao_base": item.comissao_base,
                "comissao_final": item.comissao_final,
                "provisorio": item.provisorio,
            }
            for item in comissao.itens
        ]
        return {
            "tcmp_result": tcmp_json,
            "fcmp_por_gl": fcmp_json,
            "comissao_result": comissao_json,
            "reconciliacao_result": reconciliacao_itens_raw or [],
            "status_por_processo_pai": status_por_pai,
            "tabela_pc_json": tabela_pc_json,
            "colaboradores": colaboradores,
            "mes": mes,
            "ano": ano,
            "df_ac_full": df_ac_full,
            "df_af_apuracao": df_analise_financeira,
            "df_af_full": df_af_full,
        }

    # ── Etapa 0: Validar tabela PC ────────────────────────────────────────────
    if tabela_pc:
        erros_pc = validar_processo_pai.validar(tabela_pc)
        if erros_pc:
            warnings.extend(erros_pc)

    # ── Etapa 3: Atribuir GLs (CRÍTICA) ──────────────────────────────────────
    try:
        _debug_stage("info", "receita_03_atribuir_gls", "Iniciando atribuicao de GLs.")
        atrib = atribuir_gls.executar(config_comissao, colaboradores, cargos)
        warnings.extend(atrib.warnings)
        result.atribuicao_result = atrib
        _debug_stage("success", "receita_03_atribuir_gls", "Atribuicao de GLs concluida.", {"elegiveis": len(atrib.elegiveis)})
    except Exception as exc:
        errors.append(f"[CRÍTICO] etapa_03_atribuir_gls: {exc}\n{traceback.format_exc()}")
        _debug_stage("error", "receita_03_atribuir_gls", "Falha na atribuicao de GLs.", {"erro": str(exc)})
        result.step_failed = "etapa_03_atribuir_gls"
        return result

    if not atrib.elegiveis:
        errors.append("[CRÍTICO] Nenhum GL elegível encontrado — pipeline encerrado.")
        result.step_failed = "etapa_03_atribuir_gls"
        return result

    # ── Etapa 2: Validar conflitos GL ─────────────────────────────────────────
    try:
        _debug_stage("info", "receita_02_validar_conflitos", "Validando conflitos de GL.")
        conflitos = validar_conflito_gl.validar(df_ac_full, atrib)
        result.conflitos_gl = conflitos
        from receita.alertas import gerar_alerta_gl
        alertas_venc = gerar_alerta_gl.gerar_alertas_vencimento(df_af_full)
        if conflitos or alertas_venc:
            try:
                arquivo_alerta = gerar_alerta_gl.gerar(
                    conflitos, saida_dir, mes, ano, alertas_vencimento=alertas_venc
                )
                result.arquivo_alertas = arquivo_alerta
                if conflitos:
                    warnings.append(f"{len(conflitos)} conflito(s) GL — alerta: {arquivo_alerta}")
                if alertas_venc:
                    warnings.append(f"{len(alertas_venc)} alerta(s) de vencimento — alerta: {arquivo_alerta}")
            except OSError as exc:
                warnings.append(f"Falha ao gerar alerta GL: {exc}")
    except Exception as exc:
        warnings.append(f"etapa_02_validar_conflitos (não-crítico): {exc}")

    # ── Etapa 4: Mapear documentos (CRÍTICA) ─────────────────────────────────
    # Remover processos bloqueados antes do mapeamento
    df_ac_valido = df_ac_full.copy()
    if processos_bloqueados_set and "Processo" in df_ac_valido.columns:
        mask = ~df_ac_valido["Processo"].astype(str).str.strip().str.upper().isin(
            {p.upper() for p in processos_bloqueados_set}
        )
        bloqueados_count = (~mask).sum()
        df_ac_valido = df_ac_valido[mask]
        if bloqueados_count:
            warnings.append(f"etapa_04: {bloqueados_count} item(ns) de processos bloqueados excluídos.")

    try:
        _debug_stage("info", "receita_04_mapear_documentos", "Iniciando mapeamento de documentos.")
        mapeamento = mapear_documentos.executar(df_analise_financeira, df_ac_valido)
        warnings.extend(mapeamento.warnings)
        result.mapeamento_result = mapeamento
        _debug_stage(
            "success",
            "receita_04_mapear_documentos",
            "Mapeamento concluido.",
            {"linhas_mapeadas": len(mapeamento.df_mapeado) if getattr(mapeamento, "df_mapeado", None) is not None else 0},
        )
    except Exception as exc:
        errors.append(f"[CRÍTICO] etapa_04_mapear_documentos: {exc}\n{traceback.format_exc()}")
        result.step_failed = "etapa_04_mapear_documentos"
        return result

    # ── Etapa 5: Calcular TCMP (CRÍTICA) ─────────────────────────────────────
    try:
        _debug_stage("info", "receita_05_calcular_tcmp", "Iniciando calculo do TCMP.")
        tcmp = calcular_tcmp.executar(df_ac_valido, atrib)
        warnings.extend(tcmp.warnings)
        result.tcmp_result = tcmp
        _debug_stage("success", "receita_05_calcular_tcmp", "TCMP calculado.", {"processos": len(tcmp.tcmp_por_processo)})
    except Exception as exc:
        errors.append(f"[CRÍTICO] etapa_05_calcular_tcmp: {exc}\n{traceback.format_exc()}")
        result.step_failed = "etapa_05_calcular_tcmp"
        return result

    # ── Etapa 6: Calcular FCMP (CRÍTICA) ─────────────────────────────────────
    fcmp_por_gl: Dict[str, FCMPResult] = {}
    try:
        for gl in atrib.elegiveis:
            _debug_stage("info", "receita_06_calcular_fcmp", "Calculando FCMP para GL.", {"gl_nome": gl.nome})
            fcmp_result = calcular_fcmp.executar(
                df_ac_full=df_ac_valido,
                realizados_result=realizados_result,
                gl=gl,
                pesos_metas=pesos_metas,
                fc_escada=fc_escada,
                params=params,
            )
            warnings.extend(fcmp_result.warnings)
            fcmp_por_gl[gl.nome] = fcmp_result
        result.fcmp_por_gl = fcmp_por_gl
        _debug_stage("success", "receita_06_calcular_fcmp", "FCMP calculado.", {"gls": len(fcmp_por_gl)})
    except Exception as exc:
        errors.append(f"[CRÍTICO] etapa_06_calcular_fcmp: {exc}\n{traceback.format_exc()}")
        result.step_failed = "etapa_06_calcular_fcmp"
        return result

    # ── Etapa 7: Calcular comissão (CRÍTICA) ─────────────────────────────────
    try:
        _debug_stage("info", "receita_07_calcular_comissao", "Iniciando calculo de comissoes de recebimento.")
        comissao = calcular_comissao.executar(mapeamento, atrib, tcmp, fcmp_por_gl)
        warnings.extend(comissao.warnings)
        result.comissao_result = comissao
        _debug_stage("success", "receita_07_calcular_comissao", "Comissoes calculadas.", {"itens": len(comissao.itens)})
    except Exception as exc:
        errors.append(f"[CRÍTICO] etapa_07_calcular_comissao: {exc}\n{traceback.format_exc()}")
        result.step_failed = "etapa_07_calcular_comissao"
        return result

    # ── Etapas não-críticas (08–14) ───────────────────────────────────────────

    # Etapa 8: Rastrear Processo Pai
    status_por_pai: Dict[str, Dict] = {}
    processo_para_pai: Dict[str, Optional[str]] = {}
    try:
        _debug_stage("info", "receita_08_rastrear_processo_pai", "Rastreando processos pai.")
        if tabela_pc:
            processos_com_comissao = list({i.processo for i in comissao.itens if i.processo})

            for processo in processos_com_comissao:
                try:
                    chave_pai = identificar_processo_pai.identificar(processo, tabela_pc)
                    if chave_pai is None:
                        processo_para_pai[processo] = None
                        continue
                    numero_pc, codigo_cliente = chave_pai
                    chave_str = f"{numero_pc}|{codigo_cliente}"
                    processo_para_pai[processo] = chave_str
                    if chave_str not in status_por_pai:
                        status_fat = verificar_faturamento.verificar(numero_pc, codigo_cliente, tabela_pc, df_ac_full)
                        status_pag, w_pag = verificar_pagamentos.verificar(
                            numero_pc=numero_pc,
                            codigo_cliente=codigo_cliente,
                            df_af_full=df_af_full,
                            tabela_pc=tabela_pc,
                            df_ac_full=df_ac_full,
                        )
                        if w_pag:
                            warnings.append(w_pag)
                        status_por_pai[chave_str] = {
                            "numero_pc": numero_pc,
                            "codigo_cliente": codigo_cliente,
                            "status_faturamento_completo": status_fat["status_completo"],
                            "status_pagamento_completo": status_pag,
                            "processos_total": status_fat["processos_total"],
                            "processos_faturados": status_fat["processos_faturados"],
                            "processos_pendentes": status_fat["processos_pendentes"],
                        }
                except Exception as exc_proc:
                    warnings.append(f"etapa_08_rastrear_processo_pai: erro no processo {processo} (não-crítico): {exc_proc}")
        result.status_por_processo_pai = status_por_pai
        _debug_stage("success", "receita_08_rastrear_processo_pai", "Rastreamento concluido.", {"processos_pai": len(status_por_pai)})
    except Exception as exc:
        warnings.append(f"etapa_08_rastrear_processo_pai (não-crítico): {exc}")

    # Etapa 8.5: Salvar historico-base antes da reconciliacao
    try:
        from receita.pipeline.etapas.etapa_12_salvar_historico import run as _salvar
        _debug_stage("info", "receita_08_5_salvar_historico_base", "Persistindo historico-base antes da reconciliacao.")
        r12_pre = _salvar({
            **_montar_payload_historico([]),
            "marcar_reconciliados": False,
        })
        warnings.extend(r12_pre.get("warnings", []))
        if r12_pre.get("erros_supabase"):
            warnings.extend(r12_pre["erros_supabase"])
        _debug_stage("success", "receita_08_5_salvar_historico_base", "Historico-base persistido.", {"warnings": len(r12_pre.get("warnings", []))})
    except Exception as exc:
        warnings.append(f"etapa_12_pre_salvar_historico (nao-critico): {exc}")

    # Etapa 9: Verificar reconciliacao
    processos_aptos: List[str] = []
    try:
        _debug_stage("info", "receita_09_verificar_reconciliacao", "Verificando processos aptos para reconciliacao.")
        for chave_str, status in status_por_pai.items():
            apto = determinar_reconciliacao.determinar(
                status.get("numero_pc", ""),
                status.get("codigo_cliente", ""),
                bool(status.get("status_faturamento_completo", False)),
                bool(status.get("status_pagamento_completo", False)),
            )
            if apto:
                processos_aptos.extend(status.get("processos_faturados", []))
        result.processos_aptos_reconciliacao = processos_aptos
        _debug_stage("success", "receita_09_verificar_reconciliacao", "Verificacao concluida.", {"processos_aptos": len(processos_aptos)})
    except Exception as exc:
        warnings.append(f"etapa_09_verificar_reconciliacao (não-crítico): {exc}")

    # Etapa 10: Calcular reconciliação
    try:
        _debug_stage("info", "receita_10_calcular_reconciliacao", "Calculando reconciliacao.")
        reconciliacao = calcular_reconciliacao.executar(
            comissao, fcmp_por_gl, set(processos_aptos), status_por_pai
        )
        warnings.extend(reconciliacao.warnings)
        result.reconciliacao_result = reconciliacao
        _debug_stage("success", "receita_10_calcular_reconciliacao", "Reconciliacao calculada.", {"itens": len(reconciliacao.itens)})
    except Exception as exc:
        warnings.append(f"etapa_10_calcular_reconciliacao (não-crítico): {exc}")

    # Etapa 11: Calcular estornos
    try:
        _debug_stage("info", "receita_11_calcular_estornos", "Calculando estornos.")
        estornos = calcular_estornos.executar(df_devolucoes, df_ac_full, comissao)
        warnings.extend(estornos.warnings)
        result.estornos_result = estornos
        _debug_stage("success", "receita_11_calcular_estornos", "Estornos calculados.", {"itens": len(estornos.itens)})
    except Exception as exc:
        warnings.append(f"etapa_11_calcular_estornos (não-crítico): {exc}")

    # Etapa 12: Marcar reconciliacoes no historico persistido
    try:
        from receita.pipeline.etapas.etapa_12_salvar_historico import run as _salvar
        _debug_stage("info", "receita_12_marcar_reconciliacao", "Marcando reconciliacoes no historico.")
        reconciliacao_json = [
            {
                "gl_nome": item.gl_nome,
                "numero_pc": item.numero_pc,
                "codigo_cliente": item.codigo_cliente,
                "processo": item.processo,
                "comissao_adiantada": item.comissao_adiantada,
                "fcmp_real": item.fcmp_real,
                "ajuste": item.ajuste,
                "historicos_considerados": item.historicos_considerados,
                "detalhes_historicos": item.detalhes_historicos,
            }
            for item in (result.reconciliacao_result.itens if result.reconciliacao_result else [])
        ]
        r12 = _salvar({
            **_montar_payload_historico(reconciliacao_json),
            "persistir_historicos": False,
            "persistir_auxiliares": False,
        })
        warnings.extend(r12.get("warnings", []))
        if r12.get("erros_supabase"):
            warnings.extend(r12["erros_supabase"])
        _debug_stage("success", "receita_12_marcar_reconciliacao", "Marcacao de reconciliacao concluida.", {"warnings": len(r12.get("warnings", []))})
    except Exception as exc:
        warnings.append(f"etapa_12_salvar_historico (não-crítico): {exc}")

    # Etapa 13: Consultar Supabase para abas de auditoria (por GL)
    historicos_por_gl: Dict[str, Dict[str, Any]] = {}
    try:
        _debug_stage("info", "receita_13_consultar_auditoria", "Consultando historico Supabase para auditoria.")
        from receita.supabase import consultar_historico as _consultar

        gls_unicos = sorted({i.gl_nome for i in comissao.itens if getattr(i, "gl_nome", "")})
        for gl_nome in gls_unicos:
            try:
                historicos = _consultar.consultar_todos_historicos_por_gl(gl_nome)
                pares = {
                    (h.numero_pc, h.codigo_cliente)
                    for h in historicos
                    if h.numero_pc and h.codigo_cliente
                }
                vinculos = []
                pagamentos = []
                for pc, cli in sorted(pares):
                    try:
                        vinculos.extend(_consultar.consultar_vinculos_processo_pai(pc, cli))
                    except Exception as exc_v:
                        warnings.append(f"etapa_13_consultar_auditoria: vinculos {pc}|{cli}: {exc_v}")
                    try:
                        pagamentos.extend(_consultar.consultar_pagamentos_processo_pai(pc, cli))
                    except Exception as exc_p:
                        warnings.append(f"etapa_13_consultar_auditoria: pagamentos {pc}|{cli}: {exc_p}")
                historicos_por_gl[gl_nome] = {
                    "historicos": historicos,
                    "vinculos": vinculos,
                    "pagamentos": pagamentos,
                    "erro": None,
                }
            except Exception as exc_gl:
                historicos_por_gl[gl_nome] = {
                    "historicos": None,
                    "vinculos": None,
                    "pagamentos": None,
                    "erro": str(exc_gl),
                }
                warnings.append(f"etapa_13_consultar_auditoria: GL {gl_nome}: {exc_gl}")
        _debug_stage("success", "receita_13_consultar_auditoria", "Auditoria consultada.", {"gls": len(historicos_por_gl)})
    except Exception as exc:
        warnings.append(f"etapa_13_consultar_auditoria (nao-critico): {exc}")

    # Etapa 14: Exportar Excel (MD removido — só Excel é gerado)
    try:
        _debug_stage("info", "receita_14_exportar_excel", "Exportando Excel de recebimento.")
        from receita.exportadores import excel_exporter
        arquivos_excel = excel_exporter.gerar_por_gl(
            comissao_result=comissao,
            reconciliacao_result=result.reconciliacao_result,
            estornos_result=result.estornos_result,
            tcmp_result=tcmp,
            fcmp_por_gl=fcmp_por_gl,
            saida_dir=saida_dir,
            mes=mes,
            ano=ano,
            status_por_processo_pai=status_por_pai,
            processo_para_pai=processo_para_pai,
            historicos_por_gl=historicos_por_gl,
            df_ac_full=df_ac_full,
            df_af_mapeado=getattr(mapeamento, "df_mapeado", None),
        )
        result.arquivos_excel = arquivos_excel
        _debug_stage("success", "receita_14_exportar_excel", "Excel exportado.", {"arquivos": len(arquivos_excel or [])})
    except Exception as exc:
        warnings.append(f"etapa_14_exportar_excel (não-crítico): {exc}")

    return result
