"""
=============================================================================
SKILL: Robô de Comissões — Recebimento Step 08: Pipeline Orquestrador
=============================================================================
Módulo   : pipeline_recebimento
Versão   : 1.0.0

Descrição
---------
Orquestrador que executa os Steps 1→7 do módulo de recebimento na ordem
correta, consolidando todos os resultados em um único
``PipelineRecebimentoResult``.

Fluxo de Execução
------------------
  Step 1: atribuicao_recebimento     → Quem recebe comissão por recebimento
  Step 2: mapeamento_documentos      → Classificar docs AF e vincular a processos
  Step 3: tcmp_calculator            → TCMP por processo
  Step 4: fcmp_calculator            → FCMP por processo (1× por GL)
  Step 5: comissao_recebimento       → Comissão por documento
  Step 6: reconciliacao              → Ajustes de adiantamentos (COT/ADT)
  Step 7: devolucoes                 → Estornos proporcionais

Dependências Externas (Read-Only)
---------------------------------
- scripts.loaders.LoaderResult       (DataFrames: AF já filtrada, AC, devoluções)
- scripts.realizados.RealizadosResult (métricas para FC no FCMP)

Uso
---
    from scripts.recebimento.pipeline_recebimento import execute

    result = execute(
        df_analise_financeira=loader_result.analise_financeira,
        df_analise_comercial=ac,
        df_devolucoes=loader_result.devolucoes,
        realizados_result=result_reais,Where's the movie? Well. Thank you. Damn it, as simple as anything. Synchronous, synchronous. No. Color, but it's not absolute. Stuck in through color. Why did they say million 11 million? No, no. No. Please. Yeah. Hello. No, I don't know myself. For a month. Dame is there, are you? Processing. So you just say. Kind of see. I think. No, there's. A. Is used to imagine going to Java, say. That's the mix. Ture. I. Am available. Jasik Fazil. Are you ashamed? Who needs a lot of men who need to go around? Hello. Search. Said. What? Is. It. Little bit more. Never seen it. It was. No. You. Unfortunately. Abumba, his service of his soldiers. Walker cyber person. But for a normal, so much more. Say this issue. Here. No paper so people know exactly so. Yeah. The web is going to nausea. Proposed. So. Strings is miserly to the. See. My safe quarter. Hi. Yeah. I don't have. Do you like the flight? This is. Yeah. So. It's fresh. Physical. Can I? This is. Interface. Who has set up no? Source. Billions for sure. This. Ivin Sarki Irvine poke my stage. Matthews, Fabio. From the Sea. Hey, John Villas. Alice. Some sex was like big. To me. What's the? 07 days you can. Miss Bridge Fidel. Ity. That was also. Friends. Mega mega me. What exactly? Hey, Cortana, open up. Oh. Hey. OK chad doing quartz again. But blueberry made. For a. No. I. I. I. Start. Decisions. Do you want? No. You watch no. Thank you. Researcher. These IQR is just going to be performing. Yeah, to the main Malcolm, XTA OK Muggle, Erici Fabu materials, Andrea Tony's OK this this services. Cortez, Cortez, I'll keep my hip and GPU. Same kind of ringtone one that can look for a lot of the lobby at Reservoir State for you. Do you know Puerto America? 's to know, I don't know. My. Stuff. Yeah, so. An assembly. America. Do my. Facility, basically that on each firm's porch. Contract. Ors. No. This movie. Building like. I fly my genius. Remind me, remind me my. Menu. My rescue in America, I told me someone if you saying don't talk innovation, you know. It's a. My speech. Will come from. This your point? 5 minutes. I know. You can watch it. Sugar mice, another place. A prison? Ikea. No. My sister. Is. So come out here. I'm a new app kid. If I know that, I can sell my money. For them to tell me do quieter purpose if I want that. What would you buy us to do? I. Forces and also. Adesi. Music. By no way, yeah, you should have more open all. No, no no no. What? 's super good. Yeah. I can search this. Interest. Might as well as Excel make a book. Disabled. Well. No. OK, let me speak. You know. The only thing we deserve you some money. No. Take something. Trace PG to be data. Significant. How? To. No. Direct. Just things are thick and. Thing, no. I. Wichita fridge. Event. The. By. Such a. Official. No. Recompastic. You know. I. Hello, Cortana. New Mexico. Sorry, Linette. Sorry. Hello. I didn't see you. Available now. I ever died every day. Saying yes. Say. So much. In things you Carlos Z. Probably see a little tiger news. Properly. Hello. Hello. Yes. 7:00 AM. Open Dial 5. On top of the premier coming. Someone. Yeah. Back. Yes, that. That and here. Starbucks to Bank Bonjour. But it's still things that it's control pointer Diane Sizer. Transport disappointment correcting size for Brazil? Yeah. Do I need to sassoon for exit don't go I empty this permit? I. Bare feet. OK. Can you, Alexander? No, I need triple duplex. The place of. OK King Time Premier. So I shouldn't poke it. Sufficiently. Come on. Which person which born? What does infinite? System. Sing A. Is it interested in the jobs? I think. No. No. Wait, turn off me this. Chica Adams. And. Ashley direct. Oops. Data changes. Thank you, no. This is. OK. That. Hello. Recycle. Everything is cool. Er, and I know. Your pocket is sitting at both, Senior Sasquatch. OK. Softer money. Softer. Well. Please. I didn't want anything. Thank you. Beijing, Beijing. No. You mute the. Father say, let's just say how the. Day Oman interval. Gas. OK. Before that. I. Could you please remind me to? Say. Hello. You cannot speak, Tiger. Ocean. It doesn't like this. So keep producing. Emergency composite. Patch. It was. Breaking my broker here. I think. No. Review, many say yesterday. Chili and a meter to the traffic. I need break on my stamp. I. If they see if. Open. Hello. Printed with my 6. Don't forget the. In this area. No. Hello, you soon? Season. OK so found the model. Anyone who's talking. Formula. Open. Zara. I don't know. About them. No. Which? Surely the different beds, there's no electricity signatures? Folded. Directors. We want different data. With nice. So hasn't it? Well, awesome. The weekly Neil Chopra was a Daler. And so. Let me see. Yes, I got it. Hi. Sagarvita, Boston. Near. Thank you. I. This. 
    )
    print(result.summary())
=============================================================================
"""

from __future__ import annotations
Mossy. Sorry. Please. Silencing. Three cell phones, and. Once. I. As well as Central. Super resort. That's what. For. Survive syntactic blood. Ever dodge the Windows Phone is? Chicago Jam. IE. 's. Could you have missed? Your I think. The module trace the trace. Which part so we can see now? Shakil Muzammil for sure. I they stop being late. Somehow. Bonjour. Thank you. This is me. How old is the current alarm? So. For. The sentence in Feature versus doors. X. Factor. I. No. Hi simple. DGVG. A. Yeah. Search. Also about refuting a signal, recognition of factors should be released. You want to speak. So from here. So nice. No. Separate the queue. No. No. Minimize this. Oh. Thank you. Tau. Akina. Well, zoom work so. So. So. I. So. So. So. So. See. No. No. Phone. Not so big deal. OK. Change. Skip my. What? Ashur. Yeah, to me. Who followed this? Coming my mind. Would you mind or not? That's basically. My. To be. Can you tell me more? Is. Is. You bring a lot of Y. You show the other thing we don't think whatever one. Where something. Issue. You sure don't talk to me. 
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

# ── Imports dos módulos de recebimento (Steps 1→7) ──────────────────
from scripts.recebimento.atribuicao_recebimento import (
    execute as _exec_atribuicao,
)
from scripts.recebimento.mapeamento_documentos import (
    execute as _exec_mapeamento,
)
from scripts.recebimento.tcmp_calculator import (
    execute as _exec_tcmp,
)
from scripts.recebimento.fcmp_calculator import (
    execute as _exec_fcmp,
)
from scripts.recebimento.comissao_recebimento import (
    execute as _exec_comissao,
)
from scripts.recebimento.reconciliacao import (
    execute as _exec_reconciliacao,
)
from scripts.recebimento.devolucoes import (
    execute as _exec_devolucoes,
)


# ═════════════════════════════════════════════════════════════════════
# DATACLASS DE RESULTADO
# ═════════════════════════════════════════════════════════════════════

@dataclass
class PipelineRecebimentoResult:
    """Resultado consolidado do pipeline completo de recebimento.

    Attributes
    ----------
    atribuicao_result : Any
        AtribuicaoRecebimentoResult — elegíveis e regras por linha.
    mapeamento_result : Any
        MapeamentoResult — docs AF classificados e vinculados.
    tcmp_result : Any
        TCMPResult — TCMP por processo.
    fcmp_por_gl : Dict[str, Any]
        {nome_gl: FCMPResult} — FCMP por GL.
    comissao_result : Any
        ComissaoRecebimentoResult — comissões calculadas.
    reconciliacao_result : Any
        ReconciliacaoResult — ajustes de adiantamentos.
    devolucoes_result : Any
        DevolucoesResult — estornos por devolução.
    warnings : List[str]
        Avisos agregados de todos os steps.
    errors : List[str]
        Erros agregados de todos os steps.
    step_failed : Optional[str]
        Nome do step que falhou (None se todos ok).
    """

    atribuicao_result: Any = None
    mapeamento_result: Any = None
    tcmp_result: Any = None
    tcmp_por_gl: Dict[str, Any] = field(default_factory=dict)
    fcmp_por_gl: Dict[str, Any] = field(default_factory=dict)
    comissao_result: Any = None
    reconciliacao_result: Any = None
    devolucoes_result: Any = None
    df_analise_comercial: Any = None
    realizados_result: Any = None
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    step_failed: Optional[str] = None

    @property
    def ok(self) -> bool:
        """True se nenhum step produziu erros críticos."""
        return len(self.errors) == 0 and self.step_failed is None

    @property
    def total_comissoes(self) -> float:
        """Total de comissões por recebimento (adiantamentos + regulares)."""
        if self.comissao_result is None:
            return 0.0
        return self.comissao_result.total_comissoes

    @property
    def total_reconciliacao(self) -> float:
        """Soma dos ajustes de reconciliação (negativos = débito)."""
        if self.reconciliacao_result is None:
            return 0.0
        return self.reconciliacao_result.total_reconciliacao

    @property
    def total_estornos(self) -> float:
        """Soma dos estornos por devolução (sempre negativos)."""
        if self.devolucoes_result is None:
            return 0.0
        return self.devolucoes_result.total_estornos

    @property
    def total_liquido(self) -> float:
        """Total líquido = comissões + reconciliações + estornos."""
        return self.total_comissoes + self.total_reconciliacao + self.total_estornos

    def consolidar_por_colaborador(self) -> pd.DataFrame:
        """Consolida comissões, reconciliações e estornos por colaborador.

        Returns
        -------
        pd.DataFrame
            Colunas: nome, cargo, comissoes, reconciliacao, estornos, liquido
        """
        registros: Dict[str, Dict[str, Any]] = {}

        # Comissões
        if self.comissao_result is not None:
            for c in self.comissao_result.comissoes:
                nome = c["nome"]
                if nome not in registros:
                    registros[nome] = {
                        "nome": nome, "cargo": c["cargo"],
                        "comissoes": 0.0, "reconciliacao": 0.0,
                        "estornos": 0.0,
                    }
                registros[nome]["comissoes"] += c["comissao_final"]

        # Reconciliações
        if self.reconciliacao_result is not None:
            for aj in self.reconciliacao_result.ajustes:
                nome = aj["nome"]
                if nome not in registros:
                    registros[nome] = {
                        "nome": nome, "cargo": aj["cargo"],
                        "comissoes": 0.0, "reconciliacao": 0.0,
                        "estornos": 0.0,
                    }
                registros[nome]["reconciliacao"] += aj["valor_reconciliacao"]

        # Estornos
        if self.devolucoes_result is not None:
            for e in self.devolucoes_result.estornos:
                nome = e["nome"]
                if nome not in registros:
                    registros[nome] = {
                        "nome": nome, "cargo": e["cargo"],
                        "comissoes": 0.0, "reconciliacao": 0.0,
                        "estornos": 0.0,
                    }
                registros[nome]["estornos"] += e["valor_estorno"]

        if not registros:
            return pd.DataFrame(
                columns=["nome", "cargo", "comissoes", "reconciliacao", "estornos", "liquido"]
            )

        df = pd.DataFrame(list(registros.values()))
        df["liquido"] = df["comissoes"] + df["reconciliacao"] + df["estornos"]
        df.sort_values("liquido", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df[["nome", "cargo", "comissoes", "reconciliacao", "estornos", "liquido"]]

    def summary(self) -> str:
        """Resumo formatado do pipeline completo."""
        sep = "=" * 65
        lines = [
            sep,
            "  PIPELINE RECEBIMENTO — Resumo",
            sep,
        ]

        # Status dos steps
        steps = [
            ("Step 1: Atribuição", self.atribuicao_result),
            ("Step 2: Mapeamento", self.mapeamento_result),
            ("Step 3: TCMP", None if not self.tcmp_por_gl else "ok"),
            ("Step 4: FCMP", None if not self.fcmp_por_gl else "ok"),
            ("Step 5: Comissão", self.comissao_result),
            ("Step 6: Reconciliação", self.reconciliacao_result),
            ("Step 7: Devoluções", self.devolucoes_result),
        ]
        for label, result in steps:
            if result is None:
                status = "⏭ Pulado"
            elif isinstance(result, str):
                n_gls = len(self.tcmp_por_gl) or len(self.fcmp_por_gl)
                status = f"✓ {n_gls} GL(s)"
            elif hasattr(result, "ok"):
                status = "✓ OK" if result.ok else "✗ ERRO"
            else:
                status = "✓ OK"
            lines.append(f"  {label:<30} {status}")

        lines.append("─" * 65)

        # Totais
        if self.comissao_result is not None:
            n_com = len(self.comissao_result.comissoes)
            n_adt = sum(1 for c in self.comissao_result.comissoes
                        if c.get("tipo_pagamento") == "ADIANTAMENTO")
            n_reg = n_com - n_adt
            lines.append(f"  Total de registros          : {n_com:>10}")
            lines.append(f"    Adiantamentos             : {n_adt:>10}")
            lines.append(f"    Regulares                 : {n_reg:>10}")
            lines.append(f"  Total comissões             : R$ {self.total_comissoes:>12,.2f}")

        if self.reconciliacao_result is not None and self.reconciliacao_result.ajustes:
            lines.append(f"  Total reconciliação         : R$ {self.total_reconciliacao:>12,.2f}")

        if self.devolucoes_result is not None and self.devolucoes_result.estornos:
            lines.append(f"  Total estornos              : R$ {self.total_estornos:>12,.2f}")

        lines.append(f"  ─────────────────────────────────────────────")
        lines.append(f"  TOTAL LÍQUIDO               : R$ {self.total_liquido:>12,.2f}")

        # Warnings & Errors
        lines.append("─" * 65)
        if self.warnings:
            lines.append(f"  ⚠ Avisos ({len(self.warnings)}):")
            for w in self.warnings[:10]:
                lines.append(f"    • {w}")
            if len(self.warnings) > 10:
                lines.append(f"    ... e mais {len(self.warnings) - 10}")
        if self.errors:
            lines.append(f"  ✖ Erros ({len(self.errors)}):")
            for e in self.errors[:10]:
                lines.append(f"    • {e}")
        if not self.warnings and not self.errors:
            lines.append("  ✔ Nenhum aviso ou erro.")

        lines.append(sep)
        return "\n".join(lines)


# ═════════════════════════════════════════════════════════════════════
# FUNÇÃO PRINCIPAL
# ═════════════════════════════════════════════════════════════════════

def execute(
    df_analise_financeira: pd.DataFrame,
    df_analise_comercial: pd.DataFrame,
    realizados_result: Any,
    df_devolucoes: Optional[pd.DataFrame] = None,
    mes: int = 0,
    ano: int = 0,
) -> PipelineRecebimentoResult:
    """Executa o pipeline completo de comissões por recebimento.

    Parameters
    ----------
    df_analise_financeira : pd.DataFrame
        AF já filtrada (``Tipo de Baixa='B'``, mês/ano) pelo
        ``scripts.loaders``.
    df_analise_comercial : pd.DataFrame
        AC enriquecida (com hierarquia da CP) do ``LoaderResult``.
    realizados_result : Any
        ``RealizadosResult`` do pipeline de faturamento — necessário
        para calcular o FCMP (Step 4).
    df_devolucoes : pd.DataFrame, optional
        DataFrame de devoluções do ``LoaderResult``. Se ``None`` ou
        vazio, Step 7 é pulado sem erro.

    Returns
    -------
    PipelineRecebimentoResult
        Resultado consolidado com todos os sub-resultados acessíveis.
    """
    result = PipelineRecebimentoResult()
    result.df_analise_comercial = df_analise_comercial
    result.realizados_result = realizados_result

    # ── Step 1: Atribuição ───────────────────────────────────────────
    result.atribuicao_result = _exec_atribuicao()
    _collect(result, result.atribuicao_result, "Step 1 (Atribuição)")

    if not result.atribuicao_result.ok:
        result.step_failed = "atribuicao"
        result.errors.append("Pipeline interrompido: falha na atribuição.")
        return result

    if not result.atribuicao_result.elegiveis:
        result.warnings.append("Nenhum colaborador elegível para recebimento.")
        return result

    # ── Step 2: Mapeamento ───────────────────────────────────────────
    result.mapeamento_result = _exec_mapeamento(
        df_analise_financeira=df_analise_financeira,
        df_analise_comercial=df_analise_comercial,
    )
    _collect(result, result.mapeamento_result, "Step 2 (Mapeamento)")

    if not result.mapeamento_result.ok:
        result.step_failed = "mapeamento"
        result.errors.append("Pipeline interrompido: falha no mapeamento.")
        return result

    # ── Steps 3+4: TCMP e FCMP por GL (com congelamento cross-month) ─
    nomes_gl_vistos: set = set()
    for eleg in result.atribuicao_result.elegiveis:
        nome_gl = eleg["nome"]
        cargo_gl = eleg["cargo"]
        if nome_gl in nomes_gl_vistos:
            continue
        nomes_gl_vistos.add(nome_gl)

        # Buscar TCMP+FCMP congelados para este GL (Fase 2)
        _tcmp_congelados: dict = {}
        _fcmp_congelados: dict = {}
        if mes and ano:
            try:
                from scripts.recebimento import historico_fcmp as _hist_fcmp
                _todos_processos = (
                    set(df_analise_comercial["Processo"].dropna().astype(str).str.strip().unique())
                    if "Processo" in df_analise_comercial.columns else set()
                )
                _congelados = _hist_fcmp.buscar_congelados(nome_gl, _todos_processos)
                _tcmp_congelados = {p: d["tcmp"] for p, d in _congelados.items()}
                _fcmp_congelados = {p: d["fcmp_aplicado"] for p, d in _congelados.items()}
            except Exception as _exc_freeze:
                result.warnings.append(f"historico_fcmp (buscar {nome_gl}): {_exc_freeze}")

        # Step 3: TCMP per-GL
        tcmp_gl = _exec_tcmp(
            df_analise_comercial=df_analise_comercial,
            atribuicao_result=result.atribuicao_result,
            nome_gl=nome_gl,
            tcmp_congelados=_tcmp_congelados or None,
        )
        result.tcmp_por_gl[nome_gl] = tcmp_gl
        _collect(result, tcmp_gl, f"Step 3 (TCMP {nome_gl})")

        if not tcmp_gl.ok:
            result.step_failed = f"tcmp_{nome_gl}"
            result.errors.append(f"Pipeline interrompido: falha no TCMP de '{nome_gl}'.")
            return result

        # Step 4: FCMP per-GL
        fcmp_gl = _exec_fcmp(
            df_analise_comercial=df_analise_comercial,
            realizados_result=realizados_result,
            nome_gl=nome_gl,
            cargo_gl=cargo_gl,
            atribuicao_result=result.atribuicao_result,
            fcmp_congelados=_fcmp_congelados or None,
        )
        result.fcmp_por_gl[nome_gl] = fcmp_gl
        _collect(result, fcmp_gl, f"Step 4 (FCMP {nome_gl})")

        if not fcmp_gl.ok:
            result.step_failed = f"fcmp_{nome_gl}"
            result.errors.append(
                f"Pipeline interrompido: falha no FCMP de '{nome_gl}'."
            )
            return result

        # Salvar novos TCMP+FCMP calculados (não congelados) no Supabase (Fase 2)
        if mes and ano:
            try:
                from scripts.recebimento import historico_fcmp as _hist_fcmp
                _novos = []
                for _det in fcmp_gl.detalhes:
                    if not _det.provisorio and _det.modo != "CONGELADO":
                        _tcmp_val = tcmp_gl.tcmp_por_processo.get(_det.processo, 0.0)
                        _novos.append({
                            "nome": nome_gl, "cargo": cargo_gl,
                            "processo": _det.processo,
                            "tcmp_congelado": _tcmp_val,
                            "fcmp_rampa": _det.fcmp_rampa,
                            "fcmp_aplicado": _det.fcmp_aplicado,
                            "modo": _det.modo,
                        })
                if _novos:
                    result.warnings.extend(
                        _hist_fcmp.salvar_novos(_novos, mes, ano)
                    )
            except Exception as _exc_save_freeze:
                result.warnings.append(
                    f"historico_fcmp (salvar {nome_gl}): {_exc_save_freeze}"
                )

    # Manter tcmp_result para retrocompatibilidade (primeiro GL)
    result.tcmp_result = next(iter(result.tcmp_por_gl.values()), None)

    # ── Step 5: Comissão por Recebimento ─────────────────────────────
    result.comissao_result = _exec_comissao(
        mapeamento_result=result.mapeamento_result,
        atribuicao_result=result.atribuicao_result,
        tcmp_por_gl=result.tcmp_por_gl,
        fcmp_por_gl=result.fcmp_por_gl,
    )
    _collect(result, result.comissao_result, "Step 5 (Comissão)")

    if not result.comissao_result.ok:
        result.step_failed = "comissao"
        result.errors.append("Pipeline interrompido: falha na comissão.")
        return result

    # ── Histórico cross-month: salvar adiantamentos ──────────────────
    if mes and ano:
        try:
            from scripts.recebimento import historico_adiantamentos as _hist
            result.warnings.extend(
                _hist.salvar_adiantamentos(result.comissao_result, mes, ano)
            )
        except Exception as _exc_hist_save:
            result.warnings.append(
                f"historico_adiantamentos (salvar): {_exc_hist_save}"
            )

    # ── Histórico cross-month: salvar comissões de recebimento (Fase 4) ─
    if mes and ano:
        try:
            from scripts.recebimento import historico_comissoes as _hist_com
            result.warnings.extend(
                _hist_com.salvar_comissoes_recebimento(
                    result.comissao_result, df_analise_comercial, mes, ano
                )
            )
        except Exception as _exc_hist_com:
            result.warnings.append(
                f"historico_comissoes (salvar rec): {_exc_hist_com}"
            )

    # ── Histórico cross-month: buscar pendentes de meses anteriores ──
    _adiant_hist: dict = {}
    if mes and ano:
        try:
            from scripts.recebimento import historico_adiantamentos as _hist
            _processos_faturados = {
                det.processo
                for _fcmp_gl in result.fcmp_por_gl.values()
                for det in (_fcmp_gl.detalhes or [])
                if not det.provisorio
            }
            _adiant_hist = _hist.buscar_pendentes_meses_anteriores(
                _processos_faturados, mes, ano
            )
        except Exception as _exc_hist_fetch:
            result.warnings.append(
                f"historico_adiantamentos (buscar): {_exc_hist_fetch}"
            )

    # ── Step 6: Reconciliação ────────────────────────────────────────
    result.reconciliacao_result = _exec_reconciliacao(
        comissao_result=result.comissao_result,
        fcmp_por_gl=result.fcmp_por_gl,
        adiantamentos_historicos=_adiant_hist or None,
    )
    _collect(result, result.reconciliacao_result, "Step 6 (Reconciliação)")

    # ── Histórico cross-month: marcar reconciliados ──────────────────
    if mes and ano and result.reconciliacao_result and result.reconciliacao_result.ajustes:
        try:
            from scripts.recebimento import historico_adiantamentos as _hist
            result.warnings.extend(
                _hist.marcar_reconciliados(
                    result.reconciliacao_result.ajustes, mes, ano
                )
            )
        except Exception as _exc_hist_mark:
            result.warnings.append(
                f"historico_adiantamentos (marcar): {_exc_hist_mark}"
            )

    # Reconciliação não interrompe o pipeline mesmo com erros

    # ── Step 7: Devoluções ───────────────────────────────────────────
    if df_devolucoes is not None and len(df_devolucoes) > 0:
        # Buscar histórico de comissões para estornos cross-month (Fase 4)
        _hist_comissoes: dict = {}
        if mes and ano:
            try:
                from scripts.recebimento import historico_comissoes as _hist_com
                _processos_dev = _extrair_processos_devolucao(
                    df_devolucoes, df_analise_comercial
                )
                if _processos_dev:
                    _hist_comissoes = _hist_com.buscar_historico_por_processos(
                        _processos_dev
                    )
            except Exception as _exc_hist_dev:
                result.warnings.append(
                    f"historico_comissoes (buscar dev): {_exc_hist_dev}"
                )

        result.devolucoes_result = _exec_devolucoes(
            df_devolucoes=df_devolucoes,
            df_ac=df_analise_comercial,
            comissao_result=result.comissao_result,
            historico_comissoes=_hist_comissoes or None,
        )
        _collect(result, result.devolucoes_result, "Step 7 (Devoluções)")
    else:
        result.warnings.append(
            "Step 7 (Devoluções): sem dados de devoluções — step pulado."
        )

    return result


# ═════════════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════════════

def _collect(
    pipeline: PipelineRecebimentoResult,
    step_result: Any,
    prefix: str,
) -> None:
    """Agrega warnings e errors de um step no resultado do pipeline."""
    if hasattr(step_result, "warnings"):
        for w in step_result.warnings:
            pipeline.warnings.append(f"{prefix}: {w}")
    if hasattr(step_result, "errors"):
        for e in step_result.errors:
            pipeline.errors.append(f"{prefix}: {e}")


def _extrair_processos_devolucao(
    df_dev: pd.DataFrame,
    df_ac: pd.DataFrame,
) -> set:
    """Extrai processos vinculados às NFs de devolução."""
    if (
        df_dev is None or df_dev.empty
        or df_ac is None or df_ac.empty
        or "Num docorigem" not in df_dev.columns
        or "Numero NF" not in df_ac.columns
        or "Processo" not in df_ac.columns
    ):
        return set()

    nf_to_proc = dict(zip(
        df_ac["Numero NF"].astype(str).str.strip(),
        df_ac["Processo"].astype(str).str.strip(),
    ))
    processos: set = set()
    for _, row in df_dev.iterrows():
        nf = str(row.get("Num docorigem", "")).strip()
        proc = nf_to_proc.get(nf)
        if proc:
            processos.add(proc)
    return processos
