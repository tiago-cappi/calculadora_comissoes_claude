"""
pipeline_wrapper.py — Pipeline com Output JSON Estruturado

Wrapper não-invasivo do pipeline original (rodar_pipeline.py).
Executa as mesmas etapas, mas coleta erros estruturados e gera
saida/MM_AAAA/resultado.json para consumo pelo Claude.

Uso:
    python lean_conductor/pipeline_wrapper.py --mes 10 --ano 2025
    python lean_conductor/pipeline_wrapper.py --mes 3 --ano 2026 --colaborador "Dener Martins"
    python lean_conductor/pipeline_wrapper.py --mes 10 --ano 2025 --cross-selling A
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional

# Resolve project root (parent of lean_conductor/)
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

warnings.filterwarnings("ignore")

from lean_conductor.structured_errors import PipelineCollector, safe_execute


# ═════════════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════════════

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Pipeline de Comissões — Lean Conductor (JSON output)"
    )
    p.add_argument("--mes", type=int, required=True, help="Mês de apuração (1-12)")
    p.add_argument("--ano", type=int, required=True, help="Ano de apuração")
    p.add_argument("--colaborador", type=str, default=None,
                   help="Filtrar por colaborador (opcional)")
    p.add_argument("--cross-selling", type=str, default="B", choices=["A", "B"],
                   help="Opção de cross-selling: A (subtrai) ou B (adicional). Padrão: B")
    p.add_argument("--dados-dir", type=str, default=None,
                   help="Diretório com os arquivos de entrada no padrão de produção")
    p.add_argument("--quiet", action="store_true",
                   help="Suprime output no terminal (apenas JSON)")
    return p.parse_args()


# ═════════════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════════════

DADOS_DIR = ROOT / "dados_entrada"

ARQUIVO_AC              = DADOS_DIR / "analise-comercial.xlsx"
ARQUIVO_CLASSIFICACAO   = DADOS_DIR / "Classificação de Produtos.xlsx"
ARQUIVO_FINANCEIRA      = DADOS_DIR / "analise-financeira.xlsx"
ARQUIVO_DEVOLUCOES      = DADOS_DIR / "devolucoes.xlsx"
ARQUIVO_FAT_RENT        = DADOS_DIR / "fat_rent_gpe.csv"
_PROCESSO_PEDIDO_CANDIDATOS = [
    DADOS_DIR / "Processo x Pedido de Compra.xlsx",
    DADOS_DIR / "processo_pedido_compra.xlsx",
]
ARQUIVO_PROCESSO_PEDIDO = next(
    (p for p in _PROCESSO_PEDIDO_CANDIDATOS if p.exists()),
    _PROCESSO_PEDIDO_CANDIDATOS[0],
)


def _ler_bytes(path: Path) -> Optional[bytes]:
    if path.exists():
        return path.read_bytes()
    return None


def _resolve_input_files(dados_dir: Optional[str] = None) -> Dict[str, Path]:
    base_dir = Path(dados_dir) if dados_dir else DADOS_DIR
    if not base_dir.is_absolute():
        base_dir = ROOT / base_dir
    processo_pedido_candidatos = [
        base_dir / "Processo x Pedido de Compra.xlsx",
        base_dir / "processo_pedido_compra.xlsx",
    ]
    return {
        "dados_dir": base_dir,
        "arquivo_ac": base_dir / "analise-comercial.xlsx",
        "arquivo_classificacao": base_dir / "Classificação de Produtos.xlsx",
        "arquivo_financeira": base_dir / "analise-financeira.xlsx",
        "arquivo_devolucoes": base_dir / "devolucoes.xlsx",
        "arquivo_fat_rent": base_dir / "fat_rent_gpe.csv",
        "arquivo_processo_pedido": next(
            (p for p in processo_pedido_candidatos if p.exists()),
            processo_pedido_candidatos[0],
        ),
    }


def _log(msg: str, quiet: bool = False) -> None:
    try:
        from lean_conductor.live_debug import log_current_event

        log_current_event("info", "pipeline_wrapper", "stdout", str(msg))
    except Exception:
        pass
    if not quiet:
        try:
            print(msg, flush=True)
        except UnicodeEncodeError:
            print(msg.encode("ascii", errors="replace").decode("ascii"), flush=True)


def _serialize_recebimento_payment(item: Any) -> Dict[str, Any]:
    payment = {
        "gl_nome": str(getattr(item, "gl_nome", "") or ""),
        "processo": str(getattr(item, "processo", "") or ""),
        "documento": str(getattr(item, "documento", "") or ""),
        "nf_extraida": str(getattr(item, "nf_extraida", "") or ""),
        "tipo_pagamento": str(getattr(item, "tipo_pagamento", "") or ""),
        "status_processo": str(getattr(item, "status_processo", "") or ""),
        "linha_negocio": str(getattr(item, "linha_negocio", "") or ""),
        "valor_documento": float(getattr(item, "valor_documento", 0.0) or 0.0),
        "tcmp": float(getattr(item, "tcmp", 0.0) or 0.0),
        "fcmp_rampa": float(getattr(item, "fcmp_rampa", 1.0) or 1.0),
        "fcmp_aplicado": float(getattr(item, "fcmp_aplicado", 1.0) or 1.0),
        "fcmp_considerado": float(getattr(item, "fcmp_considerado", 1.0) or 1.0),
        "fcmp_modo": str(getattr(item, "fcmp_modo", "") or ""),
        "provisorio": bool(getattr(item, "provisorio", False)),
        "comissao_potencial": float(getattr(item, "comissao_potencial", 0.0) or 0.0),
        "comissao_base": float(getattr(item, "comissao_base", 0.0) or 0.0),
        "comissao_final": float(getattr(item, "comissao_final", 0.0) or 0.0),
    }
    payment["steps"] = [
        (
            f"Comissao potencial = valor do pagamento ({payment['valor_documento']:.2f}) "
            f"x TCMP ({payment['tcmp']:.6f}) = {payment['comissao_potencial']:.6f}."
        ),
        (
            f"Como o tipo do pagamento e {payment['tipo_pagamento'] or 'REGULAR'}, "
            f"o FCMP real salvo para o processo foi {payment['fcmp_aplicado']:.6f}, "
            f"mas o FCMP efetivamente usado nesta competencia foi {payment['fcmp_considerado']:.6f}."
        ),
        (
            f"Comissao final = {payment['valor_documento']:.2f} x {payment['tcmp']:.6f} "
            f"x {payment['fcmp_considerado']:.6f} = {payment['comissao_final']:.6f}."
        ),
    ]
    return payment


def _build_fc_item_steps(detail: Dict[str, Any]) -> List[str]:
    fc_item_detail = detail.get("fc_item_detail", {}) or {}
    componentes = fc_item_detail.get("componentes", []) or []
    steps: List[str] = []
    for componente in componentes:
        steps.append(
            f"{componente.get('nome', 'componente')}: peso {float(componente.get('peso', 0.0)):.4f} "
            f"x atingimento cap {float(componente.get('atingimento_cap', 0.0)):.6f} "
            f"= {float(componente.get('contribuicao', 0.0)):.6f} "
            f"(realizado {float(componente.get('realizado', 0.0)):.2f} / meta {float(componente.get('meta', 0.0)):.2f})."
        )
    steps.append(
        f"FC item rampa = soma das contribuicoes = {float(fc_item_detail.get('fc_rampa', detail.get('fc_item', 0.0))):.6f}."
    )
    if str(fc_item_detail.get("modo", "RAMPA")).upper() == "ESCADA":
        steps.append(
            f"FC item final = {float(fc_item_detail.get('fc_final', detail.get('fc_item', 0.0))):.6f} "
            f"apos aplicacao da escada."
        )
    else:
        steps.append(
            f"FC item final = {float(fc_item_detail.get('fc_final', detail.get('fc_item', 0.0))):.6f} "
            f"(mesmo valor da rampa no modo {fc_item_detail.get('modo', 'RAMPA')})."
        )
    return steps


def _serialize_recebimento_fcmp_auditoria(pipeline_rec_result: Any) -> Dict[str, Any]:
    comissao_result = getattr(pipeline_rec_result, "comissao_result", None)
    if comissao_result is None:
        return {"processes": [], "by_gl": {}, "total_processes": 0, "total_regular_payments": 0}

    payments_by_key: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for item in getattr(comissao_result, "itens", []) or []:
        if str(getattr(item, "tipo_pagamento", "") or "").upper() != "REGULAR":
            continue
        key = (
            str(getattr(item, "gl_nome", "") or ""),
            str(getattr(item, "processo", "") or ""),
        )
        payments_by_key.setdefault(key, []).append(_serialize_recebimento_payment(item))

    by_gl: Dict[str, Any] = {}
    process_map: Dict[str, Dict[str, Any]] = {}

    for gl_nome, fcmp_result in sorted((getattr(pipeline_rec_result, "fcmp_por_gl", {}) or {}).items()):
        detalhes_por_processo = getattr(fcmp_result, "detalhes", {}) or {}
        processos_payload: List[Dict[str, Any]] = []

        for processo, fcmp_proc in sorted((getattr(fcmp_result, "fcmp_por_processo", {}) or {}).items()):
            payments = payments_by_key.get((gl_nome, processo), [])
            if not payments:
                continue

            raw_details = detalhes_por_processo.get(processo, []) or []
            item_audits: List[Dict[str, Any]] = []
            soma_valor = 0.0
            soma_contrib = 0.0
            for pos, detail in enumerate(raw_details, start=1):
                valor_item = float(detail.get("valor_item", 0.0) or 0.0)
                contribuicao = float(detail.get("contribuicao", 0.0) or 0.0)
                fc_item_detail = detail.get("fc_item_detail", {}) or {}
                hierarquia = list(detail.get("hierarquia") or [])
                component_rows = []
                for componente in fc_item_detail.get("componentes", []) or []:
                    component_rows.append({
                        "Componente": str(componente.get("nome", "") or ""),
                        "Peso": float(componente.get("peso", 0.0) or 0.0),
                        "Realizado": float(componente.get("realizado", 0.0) or 0.0),
                        "Meta": float(componente.get("meta", 0.0) or 0.0),
                        "Atingimento": float(componente.get("atingimento", 0.0) or 0.0),
                        "Atingimento Cap": float(componente.get("atingimento_cap", 0.0) or 0.0),
                        "Contribuicao": float(componente.get("contribuicao", 0.0) or 0.0),
                    })
                item_payload = {
                    "item_ordem": pos,
                    "item_idx": detail.get("item_idx"),
                    "hierarquia": hierarquia,
                    "hierarquia_path": " / ".join([str(v) for v in hierarquia if str(v)]),
                    "valor_item": valor_item,
                    "fc_item": float(detail.get("fc_item", 0.0) or 0.0),
                    "contribuicao": contribuicao,
                    "fc_item_detail": fc_item_detail,
                    "component_rows": component_rows,
                }
                item_payload["steps"] = _build_fc_item_steps(item_payload)
                item_audits.append(item_payload)
                soma_valor += valor_item
                soma_contrib += contribuicao

            fcmp_rampa = float(getattr(fcmp_proc, "fcmp_rampa", 1.0) or 1.0)
            fcmp_aplicado = float(getattr(fcmp_proc, "fcmp_aplicado", 1.0) or 1.0)
            fcmp_modo = str(getattr(fcmp_proc, "modo", "RAMPA") or "RAMPA")
            provisorio = bool(getattr(fcmp_proc, "provisorio", False))
            formula_lines = []
            note = ""
            if provisorio:
                note = (
                    "FCMP = 1,0 (PROVISORIO): processo ainda nao FATURADO. "
                    "O FC real sera recalculado quando o Processo Pai fechar."
                )
            else:
                formula_lines.append(
                    f"FCMP Rampa = soma das contribuicoes / soma dos valores = {soma_contrib:.6f} / {soma_valor:.2f} = {fcmp_rampa:.6f}."
                )
                if fcmp_modo.upper() == "ESCADA":
                    formula_lines.append(
                        f"FCMP Aplicado = {fcmp_aplicado:.6f} apos discretizacao do FCMP Rampa na escada do cargo."
                    )
                else:
                    formula_lines.append(
                        f"FCMP Aplicado = FCMP Rampa = {fcmp_aplicado:.6f} porque o modo do cargo neste processo e {fcmp_modo}."
                    )

            process_summary = {
                "processo": processo,
                "status_processo": payments[0].get("status_processo", ""),
                "linha_negocio": payments[0].get("linha_negocio", ""),
                "qtd_itens_ac": int(getattr(fcmp_proc, "num_itens", len(item_audits)) or len(item_audits)),
                "valor_faturado": float(getattr(fcmp_proc, "valor_faturado", soma_valor) or soma_valor),
                "qtd_docs_af": len(payments),
                "valor_recebido": sum(float(p.get("valor_documento", 0.0) or 0.0) for p in payments),
                "tcmp": float(payments[0].get("tcmp", 0.0) or 0.0),
                "fcmp_rampa": fcmp_rampa,
                "fcmp_aplicado": fcmp_aplicado,
                "fcmp_modo": fcmp_modo,
                "provisorio": provisorio,
                "comissao_total": sum(float(p.get("comissao_final", 0.0) or 0.0) for p in payments),
            }
            collaborator_payload = {
                "colaborador": gl_nome,
                "filename": "resultado.json",
                "detail_level": "full",
                "source": "resultado.json",
                "process_summary": process_summary,
                "payments": payments,
                "tcmp_detail": {},
                "fcmp_detail": {
                    "title": "Detalhamento real do FCMP persistido no resultado.json",
                    "headers": ["Hierarquia", "Valor (R$)", "FC Item", "Contribuicao"],
                    "rows": [
                        {
                            "Hierarquia": item.get("hierarquia_path", ""),
                            "Valor (R$)": item.get("valor_item", 0.0),
                            "FC Item": item.get("fc_item", 0.0),
                            "Contribuicao": item.get("contribuicao", 0.0),
                        }
                        for item in item_audits
                    ],
                    "formulas": formula_lines,
                    "note": note,
                },
                "fcmp_item_audits": item_audits,
            }
            processos_payload.append(collaborator_payload)
            process_map.setdefault(processo, {"processo": processo, "collaborators": []})["collaborators"].append(collaborator_payload)

        by_gl[gl_nome] = {
            "colaborador": gl_nome,
            "processes": processos_payload,
            "total_regular_payments": sum(len(item.get("payments", []) or []) for item in processos_payload),
        }

    processes = [process_map[key] for key in sorted(process_map)]
    return {
        "processes": processes,
        "by_gl": by_gl,
        "total_processes": len(processes),
        "total_regular_payments": sum(
            len(collaborator.get("payments", []) or [])
            for process_item in processes
            for collaborator in process_item.get("collaborators", [])
        ),
    }


# ═════════════════════════════════════════════════════════════════════
# PIPELINE
# ═════════════════════════════════════════════════════════════════════

def run_pipeline(
    mes: int,
    ano: int,
    colaborador: Optional[str] = None,
    cross_selling_opcao: str = "B",
    quiet: bool = False,
    usar_cache_local: bool = False,
    dados_dir: Optional[str] = None,
    *,
    pesos_overrides: Optional[List[Dict[str, Any]]] = None,
    metas_overrides: Optional[List[Dict[str, Any]]] = None,
    audit_mode: bool = False,
    audit_item_filter: Optional[str] = None,
    debug_artifacts: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Executa o pipeline completo e retorna resultado estruturado.

    Args:
        mes: Mês de apuração.
        ano: Ano de apuração.
        colaborador: Filtrar por nome (None = todos).
        cross_selling_opcao: "A" ou "B".
        quiet: Suprime output no terminal.
        pesos_overrides: Override de pesos para scenario_runner (in-memory).
        metas_overrides: Override de metas para scenario_runner (in-memory).
        dados_dir: Diretório com arquivos de entrada no padrão de produção.
        audit_mode: Se True, ativa coleta de trilhas de auditoria.
        audit_item_filter: Filtro de item para modo de auditoria (código, NF ou processo).
        debug_artifacts: Dict opcional preenchido in-memory com objetos intermediários.

    Returns:
        Dict serializável como JSON com status, etapas, comissões, erros.
        Em audit_mode=True, inclui chave 'audit_traces' com trilhas completas.
    """
    collector = PipelineCollector(mes, ano)
    input_files = _resolve_input_files(dados_dir)
    arquivo_ac = input_files["arquivo_ac"]
    arquivo_classificacao = input_files["arquivo_classificacao"]
    arquivo_financeira = input_files["arquivo_financeira"]
    arquivo_devolucoes = input_files["arquivo_devolucoes"]
    arquivo_fat_rent = input_files["arquivo_fat_rent"]
    arquivo_processo_pedido = input_files["arquivo_processo_pedido"]

    if audit_mode:
        try:
            from scripts.audit.trace_collector import TraceCollector
            TraceCollector.enable(item_filter=audit_item_filter)
        except Exception:
            pass

    _log(f"\n{'=' * 60}", quiet)
    _log(f"  CALCULADORA DE COMISSÕES — {mes:02d}/{ano}", quiet)
    _log(f"{'=' * 60}", quiet)

    # ── ETAPA 0: Cache ─────────────────────────────────────────────
    from scripts.cache_builder import build_cache_from_supabase_loader, verify_cache
    from scripts import supabase_loader as _sl_cache

    def _stage_cache():
        cache_path = ROOT / "supabase_cache.json"
        if not cache_path.exists():
            cache_path = Path.home() / "supabase_cache.json"

        # Cada nova apuração deve começar sem estado residual em memória.
        _sl_cache.clear_cache()

        if usar_cache_local:
            if not cache_path.exists():
                raise FileNotFoundError(
                    "Cache local não encontrado. Execute: python scripts/cache_builder.py --from-supabase"
                )
            _log("\n[0] Cache: usando cache local anterior (Supabase ignorado a pedido do usuário).", quiet)
            return verify_cache()

        try:
            _log("\n[0] Atualizando cache do Supabase...", quiet)
            build_cache_from_supabase_loader()
            _log("  ✓ Cache atualizado com sucesso.", quiet)
        except Exception as exc:
            raise RuntimeError(
                f"Supabase indisponível: {exc}\n"
                f"Re-execute com usar_cache_local=True para prosseguir com o cache anterior."
            ) from exc

        return verify_cache()

    safe_execute(collector, 0, "cache_check", _stage_cache, "Cache atualizado do Supabase")

    if collector.global_status == "error":
        return _finalize(collector, quiet)

    # ── ETAPA 1: Verificar arquivos ────────────────────────────────
    def _stage_verify():
        missing = []
        for f in [arquivo_ac, arquivo_classificacao]:
            if not f.exists():
                missing.append(str(f))
        if missing:
            raise FileNotFoundError(
                f"Arquivos obrigatórios não encontrados: {', '.join(missing)}"
            )
        return {"obrigatorios": 2, "opcionais_encontrados": sum(
            1 for f in [arquivo_financeira, arquivo_devolucoes, arquivo_fat_rent]
            if f.exists()
        )}

    safe_execute(collector, 1, "verificar_arquivos", _stage_verify, "Arquivos verificados")
    _log(f"[1] Arquivos verificados", quiet)

    if collector.global_status == "error":
        return _finalize(collector, quiet)

    # ── ETAPA 2: Loader ──────────────────────────────────────────
    _log("[1.5] Importando scripts.loaders...", quiet)
    import scripts.loaders as loader
    _log("[1.6] scripts.loaders importado.", quiet)

    def _stage_loader():
        _log("[2.0] Loader: lendo bytes de analise-comercial...", quiet)
        file_analise_comercial = arquivo_ac.read_bytes()
        _log("[2.1] Loader: lendo bytes de classificacao-produtos...", quiet)
        file_classificacao_produtos = arquivo_classificacao.read_bytes()
        _log("[2.2] Loader: lendo bytes de analise-financeira...", quiet)
        file_analise_financeira = _ler_bytes(arquivo_financeira)
        _log("[2.3] Loader: lendo bytes de devolucoes...", quiet)
        file_devolucoes = _ler_bytes(arquivo_devolucoes)
        _log("[2.4] Loader: lendo bytes de processo x pedido...", quiet)
        file_processo_pedido = _ler_bytes(arquivo_processo_pedido)
        _log("[2.5] Loader: chamando loader.execute(...)...", quiet)
        result = loader.execute(
            mes=mes,
            ano=ano,
            file_analise_comercial=file_analise_comercial,
            file_classificacao_produtos=file_classificacao_produtos,
            file_analise_financeira=file_analise_financeira,
            file_devolucoes=file_devolucoes,
            file_rentabilidade=None,
            file_processo_pedido=file_processo_pedido,
        )
        if not result.ok:
            raise RuntimeError(f"Erros no loader: {'; '.join(result.errors)}")
        return result

    loader_result = safe_execute(
        collector, 2, "loader", _stage_loader,
        detalhes_ok="Dados carregados"
    )
    _log(f"[2] Loader concluído", quiet)

    if loader_result is None:
        return _finalize(collector, quiet)
    if debug_artifacts is not None:
        debug_artifacts["loader_result"] = loader_result

    ac = loader_result.analise_comercial
    ac_full = loader_result.analise_comercial_full

    # ── ETAPA 3b: Rentabilidade ──────────────────────────────────
    df_fat_rent = None

    if arquivo_fat_rent.exists():
        from scripts.parse_fat_rent_gpe import execute as parse_fat

        def _stage_rent():
            res = parse_fat(source=arquivo_fat_rent.read_bytes())
            if not res.ok:
                raise RuntimeError(f"Erros no parse_fat_rent_gpe: {'; '.join(res.errors)}")
            return res.to_dataframe()

        df_fat_rent = safe_execute(
            collector, 3, "rentabilidade_parse", _stage_rent,
            detalhes_ok="Rentabilidade parseada", skip_on_error=True
        )
    else:
        collector.skip_stage(3, "rentabilidade_parse", "fat_rent_gpe.csv não fornecido")

    _log(f"[3] Rentabilidade: {'ok' if df_fat_rent is not None else 'pulada'}", quiet)

    # ── ETAPA 4: Atribuição ──────────────────────────────────────
    import scripts.atribuicao as atrib

    def _stage_atrib():
        return atrib.execute(ac)

    result_atrib = safe_execute(
        collector, 4, "atribuicao", _stage_atrib,
        detalhes_ok="Atribuições calculadas"
    )
    _log(f"[4] Atribuição concluída", quiet)

    if result_atrib is None:
        return _finalize(collector, quiet)

    df_atrib = result_atrib.to_dataframe()

    # ── ETAPA 5: Realizados ──────────────────────────────────────
    _alias_map = atrib._load_config()["alias_map"]
    ac_resolved = atrib.apply_aliases_to_df(ac, _alias_map)
    ac_full_resolved = atrib.apply_aliases_to_df(ac_full, _alias_map)

    import scripts.realizados as reais

    def _stage_reais():
        return reais.execute(
            df_analise_comercial=ac_resolved,
            df_atribuicoes=df_atrib,
            df_fat_rent_gpe=df_fat_rent,
            mes=mes,
            ano=ano,
            df_ac_full=ac_full_resolved,
        )

    result_reais = safe_execute(
        collector, 5, "realizados", _stage_reais,
        detalhes_ok="Realizados calculados"
    )
    _log(f"[5] Realizados concluídos", quiet)

    if result_reais is None:
        return _finalize(collector, quiet)
    if debug_artifacts is not None:
        debug_artifacts["result_reais"] = result_reais

    # ── Aplicar overrides (scenario_runner) ──────────────────────
    if pesos_overrides or metas_overrides:
        _apply_overrides(pesos_overrides, metas_overrides)

    # ── ETAPA 6: FC ──────────────────────────────────────────────
    import scripts.fc_calculator as fc

    df_para_fc = df_atrib
    if colaborador:
        nomes_filtro = [colaborador] if isinstance(colaborador, str) else list(colaborador)
        df_para_fc = df_atrib[df_atrib["nome"].isin(nomes_filtro)].copy()

    def _stage_fc():
        return fc.execute(df_para_fc, result_reais)

    result_fc = safe_execute(
        collector, 6, "fc_calculator", _stage_fc,
        detalhes_ok="FC calculado", skip_on_error=True
    )
    _log(f"[6] FC: {'ok' if result_fc else 'falhou (continuando)'}", quiet)

    # ── ETAPA 7: Comissão por Faturamento ────────────────────────
    import scripts.comissao_faturamento as cf

    def _stage_comissao():
        if result_fc is None:
            raise RuntimeError("FC não calculado — impossível calcular comissões")
        return cf.execute(
            atribuicoes=df_para_fc,
            fc_result_set=result_fc,
            cross_selling_cases=result_atrib.cross_selling_cases,
            cross_selling_option=cross_selling_opcao,
        )

    result_fat = safe_execute(
        collector, 7, "comissao_faturamento", _stage_comissao,
        detalhes_ok="Comissões calculadas"
    )
    _log(f"[7] Comissões: {'ok' if result_fat else 'falhou'}", quiet)

    # ── ETAPA 8: Excel Export ────────────────────────────────────
    import scripts.excel_export as excel_export

    output_dir = ROOT / "saida" / f"{mes:02d}_{ano}"

    def _stage_excel():
        if result_fat is None or result_fc is None:
            raise RuntimeError("Dados insuficientes para exportar Excel")
        return excel_export.execute(result_fat, result_fc, mes, ano, output_dir=str(output_dir), df_ac=ac_resolved)

    result_excel = safe_execute(
        collector, 8, "excel_export", _stage_excel,
        detalhes_ok="Excel exportado", skip_on_error=True
    )
    _log(f"[8] Excel: {'ok' if result_excel else 'falhou (não crítico)'}", quiet)

    # ── ETAPA 8b: Raiolanda BA Report ────────────────────────────
    import scripts.raiolanda_ba_report as _raiolanda_report

    def _stage_raiolanda_ba():
        return _raiolanda_report.execute(ac, mes, ano, output_dir=str(output_dir))

    _raiolanda_result = safe_execute(
        collector, 88, "raiolanda_ba_report", _stage_raiolanda_ba,
        detalhes_ok="Relatório BA gerado", skip_on_error=True
    )
    if _raiolanda_result and _raiolanda_result.get("arquivo"):
        _log(f"[8b] Raiolanda BA: {_raiolanda_result['arquivo']}", quiet)
    else:
        _log(f"[8b] Raiolanda BA: sem processos BA ou pulado", quiet)

    # ── Montar payload de comissões ──────────────────────────────
    comissoes_resumo: List[Dict[str, Any]] = []
    total_faturamento_geral = 0.0

    if result_fat is not None:
        df_consol = result_fat.consolidar_por_colaborador()
        total_faturamento_geral = result_fat.total_comissoes

        for _, row in df_consol.iterrows():
            comissoes_resumo.append({
                "colaborador": str(row.get("nome", "")),
                "cargo": str(row.get("cargo", "")),
                "total_faturamento": round(float(row.get("comissao_final", 0)), 2),
                "total_recebimento": 0.0,
                "total_potencial": round(float(row.get("comissao_potencial", 0)), 2),
                "itens": int(row.get("qtd_itens", 0)),
            })

    # ── Enriquecer collector com dados de comissão ───────────────
    result_data = collector.to_dict()
    result_data["comissoes"] = comissoes_resumo
    result_data["total_geral"] = round(total_faturamento_geral, 2)

    if result_excel is not None and hasattr(result_excel, "arquivos_gerados"):
        result_data["arquivos_gerados"] = list(result_excel.arquivos_gerados)
    else:
        result_data["arquivos_gerados"] = []

    if _md_fat_result is not None and getattr(_md_fat_result, "conteudos", None):
        result_data["conteudo_md_faturamento"] = _md_fat_result.conteudos

# ── Pipeline de Recebimento (receita.pipeline.runner) ───────────────
    pipeline_rec_result = None
    af = getattr(loader_result, "analise_financeira", None) if loader_result is not None else None
    tabela_pc = getattr(loader_result, "processo_pedido", None) if loader_result is not None else None
    af_full = None

    if arquivo_financeira.exists():
        try:
            af_full, warnings_af_full = loader.load_analise_financeira_full(arquivo_financeira.read_bytes())
            if warnings_af_full:
                result_data.setdefault("avisos", []).extend(warnings_af_full)
            if debug_artifacts is not None:
                debug_artifacts["af_full"] = af_full
        except Exception as _exc_af_full:
            _msg_af_full = f"load_analise_financeira_full falhou: {_exc_af_full}"
            result_data.setdefault("avisos", []).append(_msg_af_full)
            _log(f"[Rec] {_msg_af_full}", quiet)

    if af is not None and not (hasattr(af, "empty") and af.empty) and result_reais is not None:
        try:
            from receita.pipeline.runner import executar as _exec_receita
            from scripts import supabase_loader as _sl_rec
            _sl_rec.clear_cache()
            _pesos_indexed, _escada_por_cargo, _params_fc = fc._load_config()
            pipeline_rec_result = _exec_receita(
                df_analise_financeira=af,
                df_ac_full=ac_full_resolved,
                realizados_result=result_reais,
                tabela_pc=tabela_pc,
                df_devolucoes=getattr(loader_result, "devolucoes", None),
                mes=mes,
                ano=ano,
                saida_dir=str(output_dir),
                config_comissao=_sl_rec.load_json("config_comissao.json"),
                colaboradores=_sl_rec.load_json("colaboradores.json"),
                cargos=_sl_rec.load_json("cargos.json"),
                pesos_metas=_pesos_indexed,
                fc_escada=_escada_por_cargo,
                params=_params_fc,
                df_af_full=af_full,
            )
            _log(f"[Rec] Pipeline recebimento executado", quiet)
            for _w in (pipeline_rec_result.warnings or []):
                _log(f"  ⚠ [Rec] {_w}", quiet)
            result_data["recebimento"] = {
                "status": "ok" if pipeline_rec_result.ok else "error",
                "step_failed": pipeline_rec_result.step_failed,
                "warnings": list(pipeline_rec_result.warnings or []),
                "errors": list(pipeline_rec_result.errors or []),
                "totais_por_gl": dict(
                    getattr(getattr(pipeline_rec_result, "comissao_result", None), "total_por_gl", {}) or {}
                ),
                "processos_aptos_reconciliacao": list(
                    getattr(pipeline_rec_result, "processos_aptos_reconciliacao", []) or []
                ),
                "auditoria_fcmp": _serialize_recebimento_fcmp_auditoria(pipeline_rec_result),
            }
            if debug_artifacts is not None:
                debug_artifacts["pipeline_rec_result"] = pipeline_rec_result
        except Exception as _exc_rec:
            _log(f"[Rec] Pipeline recebimento falhou (não crítico): {_exc_rec}", quiet)
            result_data["recebimento"] = {
                "status": "error",
                "step_failed": "pipeline_wrapper_recebimento",
                "warnings": [],
                "errors": [str(_exc_rec)],
                "totais_por_gl": {},
                "processos_aptos_reconciliacao": [],
            }

    # ── Enriquecer comissões com dados de recebimento ─────────────
    if pipeline_rec_result is not None and getattr(pipeline_rec_result, "comissao_result", None) is not None:
        try:
            comissao_result_rec = pipeline_rec_result.comissao_result
            total_recebimento_geral = sum(comissao_result_rec.total_por_gl.values())
            existing_map = {c["colaborador"]: i for i, c in enumerate(comissoes_resumo)}

            for gl_nome, total_rec in comissao_result_rec.total_por_gl.items():
                potencial_rec = sum(
                    i.comissao_potencial
                    for i in comissao_result_rec.itens
                    if i.gl_nome == gl_nome
                )
                num_docs = len([i for i in comissao_result_rec.itens if i.gl_nome == gl_nome])
                if gl_nome in existing_map:
                    comissoes_resumo[existing_map[gl_nome]]["total_recebimento"] = round(total_rec, 2)
                else:
                    comissoes_resumo.append({
                        "colaborador": gl_nome,
                        "cargo": "Gerente de Linha",
                        "total_faturamento": 0.0,
                        "total_recebimento": round(total_rec, 2),
                        "total_potencial": round(potencial_rec, 2),
                        "itens": num_docs,
                    })

            result_data["comissoes"] = comissoes_resumo
            result_data["total_geral"] = round(
                result_data.get("total_geral", 0) + total_recebimento_geral, 2
            )
        except Exception as _exc_enrich:
            _log(f"[Rec] Enriquecimento de resumo com recebimento falhou (não crítico): {_exc_enrich}", quiet)

    if audit_mode:
        try:
            from scripts.audit.trace_collector import TraceCollector
            result_data["audit_traces"] = TraceCollector.export()
            TraceCollector.disable()
        except Exception:
            pass

    return _finalize_with_data(result_data, mes, ano, quiet)


# ═════════════════════════════════════════════════════════════════════
# OVERRIDE HELPERS (para scenario_runner)
# ═════════════════════════════════════════════════════════════════════

def _apply_overrides(
    pesos_overrides: Optional[List[Dict[str, Any]]],
    metas_overrides: Optional[List[Dict[str, Any]]],
) -> None:
    """Aplica overrides in-memory no cache JSON (sem persistir)."""
    import scripts.config_manager as cm

    if pesos_overrides:
        for ov in pesos_overrides:
            cargo = ov.pop("cargo", None)
            colab = ov.pop("colaborador", "")
            if cargo:
                cm.set_pesos_metas(cargo, ov, colaborador=colab)

    if metas_overrides:
        for ov in metas_overrides:
            tipo = ov.get("tipo_meta")
            valor = ov.get("valor_meta")
            linha = ov.get("linha")
            if tipo and valor and linha:
                cm.set_meta_aplicacao(
                    linha=linha,
                    tipo_meta=tipo,
                    valor_meta=valor,
                    grupo=ov.get("grupo"),
                    subgrupo=ov.get("subgrupo"),
                    tipo_mercadoria=ov.get("tipo_mercadoria"),
                    fabricante=ov.get("fabricante"),
                )


# ═════════════════════════════════════════════════════════════════════
# FINALIZERS
# ═════════════════════════════════════════════════════════════════════

def _finalize(collector: PipelineCollector, quiet: bool) -> Dict[str, Any]:
    """Finaliza e salva JSON apenas do collector."""
    data = collector.to_dict()
    data["comissoes"] = []
    data["total_geral"] = 0.0
    data["arquivos_gerados"] = []
    return _finalize_with_data(data, collector.mes, collector.ano, quiet)


def _finalize_with_data(
    data: Dict[str, Any], mes: int, ano: int, quiet: bool
) -> Dict[str, Any]:
    """Salva resultado.json e imprime sumário."""
    import time

    output_dir = ROOT / "saida" / f"{mes:02d}_{ano}"
    _mkdir_ok = False
    _last_exc: Optional[Exception] = None
    for _attempt in range(3):
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            _mkdir_ok = True
            break
        except (PermissionError, OSError) as exc:
            _last_exc = exc
            if _attempt < 2:
                time.sleep(0.5)

    if not _mkdir_ok:
        _log(f"  ⚠ Não foi possível criar '{output_dir}' após 3 tentativas: {_last_exc}", quiet)
        fallback_path = ROOT / f"resultado_{mes:02d}_{ano}.json"
        try:
            with open(fallback_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            _log(f"  ⚠ resultado.json salvo em fallback: {fallback_path}", quiet)
        except Exception as fb_exc:
            _log(f"  ✖ Não foi possível salvar resultado.json em nenhum local: {fb_exc}", quiet)
        return data

    json_path = output_dir / "resultado.json"

    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    except (PermissionError, OSError) as _json_exc:
        _log(f"  ⚠ Não foi possível salvar resultado.json em '{json_path}': {_json_exc}", quiet)
        fallback_path = ROOT / f"resultado_{mes:02d}_{ano}.json"
        try:
            with open(fallback_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            _log(f"  ⚠ resultado.json salvo em fallback: {fallback_path}", quiet)
            json_path = fallback_path
        except Exception as _fb2_exc:
            _log(f"  ✖ Não foi possível salvar resultado.json em nenhum local: {_fb2_exc}", quiet)

    _log(f"\n{'=' * 60}", quiet)
    _log(f"  STATUS: {data['status'].upper()}", quiet)
    _log(f"  Total Comissões: R$ {data.get('total_geral', 0):,.2f}", quiet)
    _log(f"  Erros: {len(data.get('erros', []))}", quiet)
    _log(f"  Avisos: {len(data.get('avisos', []))}", quiet)
    _log(f"  JSON: {json_path}", quiet)
    _log(f"{'=' * 60}", quiet)

    return data


# ═════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    args = _parse_args()
    result = run_pipeline(
        mes=args.mes,
        ano=args.ano,
        colaborador=args.colaborador,
        cross_selling_opcao=getattr(args, "cross_selling", "B"),
        quiet=args.quiet,
        dados_dir=args.dados_dir,
    )
    sys.exit(0 if result.get("status") == "ok" else 1)
