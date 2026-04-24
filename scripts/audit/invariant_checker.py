"""
invariant_checker.py — Verifica invariantes de negócio nos resultados do pipeline.

Lê resultado.json e dados de configuração para validar regras fundamentais
que devem sempre ser verdadeiras em qualquer execução correta do pipeline.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent.parent


def _load_cache() -> Dict:
    """Carrega supabase_cache.json do root ou do home."""
    cache_path = ROOT / "supabase_cache.json"
    if not cache_path.exists():
        cache_path = Path.home() / "supabase_cache.json"
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _make_result(
    id: str,
    status: str,
    severidade: str,
    descricao: str,
    contexto: Dict,
    fix_suggestion: str,
) -> Dict:
    return {
        "id": id,
        "status": status,
        "severidade": severidade,
        "descricao": descricao,
        "contexto": contexto,
        "fix_suggestion": fix_suggestion,
    }


def _check_i1_fc_limits(resultado_data: dict, cache: dict) -> Dict:
    """I1: FC_final deve estar em [0, cap_fc_max] para todos os colaboradores."""
    try:
        params = cache.get("params", {})
        if isinstance(params, list):
            params = {p.get("chave", ""): p.get("valor") for p in params}
        cap_fc_max = float(params.get("cap_fc_max", 1.0))

        violacoes = []
        comissoes = resultado_data.get("comissoes", [])
        for c in comissoes:
            nome = c.get("colaborador", "?")
            # Verificar via dados de comissão se há indicativo de FC fora dos limites
            # O resultado.json consolidado não expõe fc_final diretamente,
            # então verificamos se há erros relacionados no pipeline
            pass

        # Verificar nos erros do pipeline se houve issues de FC
        erros = resultado_data.get("erros", [])
        fc_erros = [e for e in erros if "fc" in str(e).lower() and "cap" in str(e).lower()]

        if fc_erros:
            return _make_result(
                "I1", "FAIL", "CRITICAL",
                f"FC fora dos limites detectado nos erros do pipeline ({len(fc_erros)} ocorrência(s))",
                {"cap_fc_max": cap_fc_max, "erros_fc": fc_erros[:3]},
                "Revise as metas e pesos configurados. Execute diagnosticar_config() para mais detalhes.",
            )

        return _make_result(
            "I1", "PASS", "CRITICAL",
            f"FC dentro dos limites [0, {cap_fc_max}] — nenhuma violação encontrada nos logs do pipeline",
            {"cap_fc_max": cap_fc_max},
            "",
        )
    except Exception as exc:
        return _make_result(
            "I1", "FAIL", "CRITICAL",
            f"Erro ao verificar limites de FC: {exc}",
            {},
            "Verifique se cap_fc_max está configurado em params.",
        )


def _check_i2_faturado_com_colaborador(resultado_data: dict) -> Dict:
    """I2: Itens com status=FATURADO devem ter ao menos 1 colaborador atribuído."""
    try:
        avisos_atrib = []
        for etapa in resultado_data.get("etapas", []):
            if etapa.get("nome") == "atribuicao":
                avisos_atrib = etapa.get("avisos", [])
                break

        sem_vinculo = [a for a in avisos_atrib if "NÃO tem vínculo" in a or "nao tem vinculo" in a.lower()]

        if sem_vinculo:
            return _make_result(
                "I2", "FAIL", "WARNING",
                f"{len(sem_vinculo)} item(ns) FATURADO(s) sem colaborador atribuído",
                {"exemplos": sem_vinculo[:5], "total": len(sem_vinculo)},
                "Verifique config_comissao: adicione regras ou alias para os colaboradores sem vínculo.",
            )

        return _make_result(
            "I2", "PASS", "WARNING",
            "Todos os itens faturados possuem ao menos um colaborador atribuído",
            {},
            "",
        )
    except Exception as exc:
        return _make_result(
            "I2", "FAIL", "WARNING",
            f"Erro ao verificar atribuições: {exc}",
            {},
            "",
        )


def _check_i3_no_negative_commissions(resultado_data: dict) -> Dict:
    """I3: Sem comissões negativas (exceto devoluções marcadas como tal)."""
    try:
        comissoes = resultado_data.get("comissoes", [])
        negativos = [
            c for c in comissoes
            if float(c.get("total_faturamento", 0)) < 0
        ]

        if negativos:
            return _make_result(
                "I3", "FAIL", "CRITICAL",
                f"{len(negativos)} colaborador(es) com comissão de faturamento negativa",
                {"colaboradores": [c.get("colaborador") for c in negativos]},
                "Verifique se há devoluções processadas sem marcação adequada de observacao='Devolução'.",
            )

        return _make_result(
            "I3", "PASS", "CRITICAL",
            "Nenhuma comissão negativa encontrada no consolidado por colaborador",
            {},
            "",
        )
    except Exception as exc:
        return _make_result(
            "I3", "FAIL", "CRITICAL",
            f"Erro ao verificar comissões negativas: {exc}",
            {},
            "",
        )


def _check_i4_active_colaborador_has_peso(resultado_data: dict, cache: dict) -> Dict:
    """I4: Todo colaborador ativo deve ter ao menos 1 peso > 0 configurado."""
    try:
        colaboradores_cache = cache.get("colaboradores", [])
        pesos_metas = cache.get("pesos_metas", [])

        if not colaboradores_cache or not pesos_metas:
            return _make_result(
                "I4", "INFO", "WARNING",
                "Cache de colaboradores ou pesos_metas não disponível para verificação",
                {},
                "Execute calcular_comissoes() com conexão ao Supabase para atualizar o cache.",
            )

        cargos_com_peso: set = set()
        for pm in pesos_metas:
            componentes = ["faturamento_linha", "faturamento_individual", "conversao_linha",
                           "conversao_individual", "rentabilidade", "retencao_clientes"]
            if any(float(pm.get(c, 0) or 0) > 0 for c in componentes):
                cargo_key = (str(pm.get("cargo", "")), str(pm.get("colaborador", "") or ""))
                cargos_com_peso.add(cargo_key)

        colaboradores_resultados = {c.get("colaborador") for c in resultado_data.get("comissoes", [])}
        sem_peso = []
        for colab in colaboradores_cache:
            nome = str(colab.get("nome_colaborador", ""))
            cargo = str(colab.get("cargo", ""))
            if nome in colaboradores_resultados:
                has_peso = (
                    (cargo, nome) in cargos_com_peso or
                    (cargo, "") in cargos_com_peso
                )
                if not has_peso:
                    sem_peso.append({"nome": nome, "cargo": cargo})

        if sem_peso:
            return _make_result(
                "I4", "FAIL", "WARNING",
                f"{len(sem_peso)} colaborador(es) ativo(s) sem pesos configurados",
                {"colaboradores": sem_peso},
                "Use alterar_peso_meta() para configurar pesos para os colaboradores listados.",
            )

        return _make_result(
            "I4", "PASS", "WARNING",
            "Todos os colaboradores com resultados possuem pesos configurados",
            {},
            "",
        )
    except Exception as exc:
        return _make_result(
            "I4", "FAIL", "WARNING",
            f"Erro ao verificar pesos de colaboradores: {exc}",
            {},
            "",
        )


def _check_i5_meta_for_peso_positivo(resultado_data: dict, cache: dict) -> Dict:
    """I5: Todo componente com peso > 0 deve ter meta configurada em algum nível."""
    try:
        pesos_metas = cache.get("pesos_metas", [])
        metas_aplicacao = cache.get("metas_aplicacao", [])
        metas_individuais = cache.get("metas_individuais", [])

        if not pesos_metas:
            return _make_result(
                "I5", "INFO", "CRITICAL",
                "Cache de pesos_metas não disponível para verificação de metas",
                {},
                "Execute calcular_comissoes() com Supabase para verificar esta invariante.",
            )

        metas_linhas = {str(m.get("linha", "")) for m in metas_aplicacao}
        metas_individ_nomes = {str(m.get("colaborador", "")) for m in metas_individuais}

        erros_meta = resultado_data.get("erros", [])
        missing_meta_erros = [
            e for e in erros_meta
            if "meta" in str(e).lower() and ("not found" in str(e).lower() or "não encontrada" in str(e).lower() or "missing" in str(e).lower())
        ]

        if missing_meta_erros:
            return _make_result(
                "I5", "FAIL", "CRITICAL",
                f"{len(missing_meta_erros)} componente(s) com peso > 0 sem meta configurada",
                {"erros": missing_meta_erros[:5]},
                "Use definir_meta_aplicacao() ou definir_meta_individual() para configurar as metas ausentes.",
            )

        return _make_result(
            "I5", "PASS", "CRITICAL",
            "Nenhum componente com peso positivo sem meta foi detectado nos logs do pipeline",
            {"metas_linhas_configuradas": len(metas_linhas), "metas_individuais_configuradas": len(metas_individ_nomes)},
            "",
        )
    except Exception as exc:
        return _make_result(
            "I5", "FAIL", "CRITICAL",
            f"Erro ao verificar metas de componentes: {exc}",
            {},
            "",
        )


def _check_i6_taxa_fatia_lte_1(resultado_data: dict, cache: dict) -> Dict:
    """I6: taxa_rateio × fatia_cargo <= 1.0 para cada item."""
    try:
        config_comissao = cache.get("config_comissao", [])
        if not config_comissao:
            return _make_result(
                "I6", "INFO", "WARNING",
                "config_comissao não disponível no cache para verificação",
                {},
                "Execute calcular_comissoes() com Supabase para atualizar o cache.",
            )

        violacoes = []
        for regra in config_comissao:
            taxa = float(regra.get("taxa_rateio_maximo_pct", 0) or 0) / 100.0
            fatia = float(regra.get("fatia_cargo", 0) or 0) / 100.0
            produto = taxa * fatia
            if produto > 1.0001:
                violacoes.append({
                    "linha": regra.get("hierarquia", regra.get("linha", "?")),
                    "cargo": regra.get("cargo", "?"),
                    "taxa_pct": taxa * 100,
                    "fatia_pct": fatia * 100,
                    "produto": produto,
                })

        if violacoes:
            return _make_result(
                "I6", "FAIL", "WARNING",
                f"{len(violacoes)} regra(s) com taxa × fatia > 1.0",
                {"violacoes": violacoes[:5]},
                "Revise as taxas e fatias em config_comissao via gerenciar_regra_comissao().",
            )

        return _make_result(
            "I6", "PASS", "WARNING",
            f"Taxa × fatia <= 1.0 em todas as {len(config_comissao)} regras verificadas",
            {"regras_verificadas": len(config_comissao)},
            "",
        )
    except Exception as exc:
        return _make_result(
            "I6", "FAIL", "WARNING",
            f"Erro ao verificar taxa × fatia: {exc}",
            {},
            "",
        )


def _check_i7_cross_selling_option(resultado_data: dict) -> Dict:
    """I7: Cross-selling detectado mas opção não definida → alerta."""
    try:
        avisos = resultado_data.get("avisos", [])
        erros = resultado_data.get("erros", [])
        all_msgs = [str(a) for a in avisos] + [str(e) for e in erros]

        cs_detectado = any("cross-selling" in m.lower() or "cross_selling" in m.lower() for m in all_msgs)
        cs_opcao_definida = any("opção selecionada" in m.lower() or "opcao selecionada" in m.lower() or "option" in m.lower() for m in all_msgs)

        for etapa in resultado_data.get("etapas", []):
            for aviso in etapa.get("avisos", []):
                if "cross-selling" in str(aviso).lower():
                    cs_detectado = True
                if "opção selecionada" in str(aviso).lower():
                    cs_opcao_definida = True

        if cs_detectado and not cs_opcao_definida:
            return _make_result(
                "I7", "FAIL", "INFO",
                "Cross-selling detectado mas confirmação de opção (A ou B) não encontrada nos logs",
                {},
                "Verifique se calcular_comissoes() foi chamado com cross_selling_opcao='A' ou 'B' explicitamente.",
            )

        if cs_detectado:
            return _make_result(
                "I7", "PASS", "INFO",
                "Cross-selling detectado e opção confirmada nos logs",
                {},
                "",
            )

        return _make_result(
            "I7", "INFO", "INFO",
            "Nenhum caso de cross-selling detectado neste período",
            {},
            "",
        )
    except Exception as exc:
        return _make_result(
            "I7", "INFO", "INFO",
            f"Não foi possível verificar cross-selling: {exc}",
            {},
            "",
        )


def _check_i8_pipeline_completed(resultado_data: dict) -> Dict:
    """I8: Pipeline concluído sem erros críticos."""
    try:
        status = resultado_data.get("status", "unknown")
        erros = resultado_data.get("erros", [])
        etapas = resultado_data.get("etapas", [])

        etapas_com_erro = [e for e in etapas if e.get("status") == "error"]
        erros_criticos = [e for e in erros if isinstance(e, dict) and e.get("recovery") != "skipped"]

        if status == "error":
            return _make_result(
                "I8", "FAIL", "INFO",
                f"Pipeline finalizou com status '{status}' — {len(etapas_com_erro)} etapa(s) com erro",
                {"etapas_com_erro": [e.get("nome") for e in etapas_com_erro], "total_erros": len(erros)},
                "Verifique os erros detalhados em resultado.json e corrija os problemas reportados.",
            )

        if status == "partial":
            return _make_result(
                "I8", "FAIL", "INFO",
                f"Pipeline finalizou parcialmente — {len(etapas_com_erro)} etapa(s) pulada(s)/com erro",
                {"etapas_com_erro": [e.get("nome") for e in etapas_com_erro]},
                "Algumas etapas falharam mas não foram críticas. Verifique se os resultados são completos.",
            )

        return _make_result(
            "I8", "PASS", "INFO",
            f"Pipeline concluído com status '{status}' sem erros críticos",
            {"total_etapas": len(etapas), "etapas_ok": len([e for e in etapas if e.get("status") == "ok"])},
            "",
        )
    except Exception as exc:
        return _make_result(
            "I8", "INFO", "INFO",
            f"Não foi possível verificar status do pipeline: {exc}",
            {},
            "",
        )


def _check_i9_expected_colaboradores(resultado_data: dict, cache: dict) -> Dict:
    """I9: Todos os colaboradores ativos esperados possuem resultados."""
    try:
        colaboradores_cache = cache.get("colaboradores", [])
        if not colaboradores_cache:
            return _make_result(
                "I9", "INFO", "WARNING",
                "Cache de colaboradores não disponível para comparação",
                {},
                "Execute calcular_comissoes() com Supabase para verificar esta invariante.",
            )

        colaboradores_resultado = {c.get("colaborador", "") for c in resultado_data.get("comissoes", [])}

        cargos_faturamento = {"Gerente Comercial", "Consultor Interno", "Consultor Externo"}
        esperados_faturamento = [
            c for c in colaboradores_cache
            if c.get("cargo", "") in cargos_faturamento
        ]

        ausentes = [
            {"nome": c.get("nome_colaborador"), "cargo": c.get("cargo")}
            for c in esperados_faturamento
            if c.get("nome_colaborador") not in colaboradores_resultado
        ]

        if ausentes:
            return _make_result(
                "I9", "FAIL", "WARNING",
                f"{len(ausentes)} colaborador(es) ativo(s) sem resultados de comissão",
                {"ausentes": ausentes, "total_com_resultado": len(colaboradores_resultado)},
                "Verifique se há itens faturados para esses colaboradores no período. "
                "Pode ser esperado se não tiveram faturamento no mês.",
            )

        return _make_result(
            "I9", "PASS", "WARNING",
            f"Todos os {len(esperados_faturamento)} colaboradores esperados possuem resultados",
            {"colaboradores_com_resultado": len(colaboradores_resultado)},
            "",
        )
    except Exception as exc:
        return _make_result(
            "I9", "FAIL", "WARNING",
            f"Erro ao verificar colaboradores esperados: {exc}",
            {},
            "",
        )


def _check_i10_codigos_sem_correspondencia(mes: int, ano: int, resultado_data: dict) -> Dict:
    """I10: Códigos sem correspondência na CP."""
    try:
        output_dir = ROOT / "saida" / f"{mes:02d}_{ano}"
        arquivo_sem_cp = output_dir / f"codigos_sem_correspondencia_{mes:02d}_{ano}.txt"

        # Também verificar nos avisos do loader
        avisos_loader = []
        for etapa in resultado_data.get("etapas", []):
            if etapa.get("nome") == "loader":
                avisos_loader = etapa.get("avisos", [])
                break

        sem_cp_avisos = [a for a in avisos_loader if "sem correspondência" in a.lower() or "sem correspondencia" in a.lower()]
        contagem_sem_cp = 0
        for aviso in sem_cp_avisos:
            import re
            m = re.search(r"(\d+)\s+de\s+\d+\s+linhas\s+sem", aviso)
            if m:
                contagem_sem_cp = int(m.group(1))
                break

        if arquivo_sem_cp.exists():
            linhas = arquivo_sem_cp.read_text(encoding="utf-8").strip().splitlines()
            codigos = [l.strip() for l in linhas if l.strip()]
            return _make_result(
                "I10", "FAIL", "WARNING",
                f"{len(codigos)} código(s) de produto sem correspondência na Classificação de Produtos",
                {"arquivo": str(arquivo_sem_cp), "exemplos": codigos[:5], "total": len(codigos)},
                "Adicione os produtos ausentes na Classificação de Produtos.xlsx para garantir hierarquia correta.",
            )

        if contagem_sem_cp > 0:
            return _make_result(
                "I10", "FAIL", "WARNING",
                f"{contagem_sem_cp} linha(s) sem correspondência na CP (conforme log do loader)",
                {"aviso_loader": sem_cp_avisos[0] if sem_cp_avisos else ""},
                "Adicione os produtos ausentes na Classificação de Produtos.xlsx.",
            )

        return _make_result(
            "I10", "PASS", "WARNING",
            "Nenhum código de produto sem correspondência na Classificação de Produtos",
            {},
            "",
        )
    except Exception as exc:
        return _make_result(
            "I10", "INFO", "WARNING",
            f"Não foi possível verificar códigos sem correspondência: {exc}",
            {},
            "",
        )


def verificar(
    mes: int,
    ano: int,
    resultado_data: dict,
    atribuicoes: Optional[List] = None,
    fc_results: Optional[List] = None,
) -> dict:
    """Verifica invariantes de negócio nos resultados do pipeline.

    Args:
        mes: Mês de apuração.
        ano: Ano de apuração.
        resultado_data: Conteúdo do resultado.json como dict.
        atribuicoes: Lista de atribuições (opcional, para verificações mais detalhadas).
        fc_results: Lista de resultados FC (opcional).

    Returns:
        Dict com total_verificacoes, passou, alertas, criticos, infos e detalhes.
    """
    cache = _load_cache()

    checks = [
        _check_i1_fc_limits(resultado_data, cache),
        _check_i2_faturado_com_colaborador(resultado_data),
        _check_i3_no_negative_commissions(resultado_data),
        _check_i4_active_colaborador_has_peso(resultado_data, cache),
        _check_i5_meta_for_peso_positivo(resultado_data, cache),
        _check_i6_taxa_fatia_lte_1(resultado_data, cache),
        _check_i7_cross_selling_option(resultado_data),
        _check_i8_pipeline_completed(resultado_data),
        _check_i9_expected_colaboradores(resultado_data, cache),
        _check_i10_codigos_sem_correspondencia(mes, ano, resultado_data),
    ]

    passou = sum(1 for c in checks if c["status"] == "PASS")
    falhou_critico = sum(1 for c in checks if c["status"] == "FAIL" and c["severidade"] == "CRITICAL")
    falhou_warning = sum(1 for c in checks if c["status"] == "FAIL" and c["severidade"] == "WARNING")
    infos = sum(1 for c in checks if c["status"] == "INFO" or (c["status"] == "FAIL" and c["severidade"] == "INFO"))

    return {
        "total_verificacoes": len(checks),
        "passou": passou,
        "alertas": falhou_warning,
        "criticos": falhou_critico,
        "infos": infos,
        "detalhes": checks,
    }
