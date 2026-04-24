"""
config_cli.py — CLI para Gestão de Configuração de Comissões

Traduz comandos estruturados em chamadas ao config_manager.py existente.
O Claude mapeia linguagem natural → argumentos CLI → config_manager.

Uso:
    python lean_conductor/config_cli.py summary
    python lean_conductor/config_cli.py get-pesos --cargo "Gerente Comercial"
    python lean_conductor/config_cli.py set-peso --cargo "Gerente Comercial" --componente faturamento_linha --valor 45
    python lean_conductor/config_cli.py persist
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import scripts.config_manager as cm


# ═════════════════════════════════════════════════════════════════════
# SUBCOMMAND HANDLERS
# ═════════════════════════════════════════════════════════════════════

def _cmd_summary(args: argparse.Namespace) -> None:
    print(cm.summary())


def _cmd_get_params(args: argparse.Namespace) -> None:
    print(cm.get_params())


def _cmd_list_colaboradores(args: argparse.Namespace) -> None:
    print(cm.list_colaboradores())


def _cmd_get_regras(args: argparse.Namespace) -> None:
    print(cm.query_regras(
        linha=args.linha,
        grupo=args.grupo,
        subgrupo=args.subgrupo,
        cargo=args.cargo,
        somente_ativos=not args.incluir_inativos,
        limite=args.limite,
    ))


def _cmd_get_pesos(args: argparse.Namespace) -> None:
    print(cm.get_pesos_metas(cargo=args.cargo, colaborador=args.colaborador))


def _cmd_get_meta(args: argparse.Namespace) -> None:
    print(cm.get_metas(tipo=args.tipo, colaborador=args.colaborador, linha=args.hierarquia))


def _cmd_get_meta_rentabilidade(args: argparse.Namespace) -> None:
    print(cm.get_meta_rentabilidade(
        mes=args.mes, ano=args.ano, linha=args.linha, limite=args.limite
    ))


def _cmd_get_fc_escada(args: argparse.Namespace) -> None:
    print(cm.get_fc_escada())


def _cmd_get_cross_selling(args: argparse.Namespace) -> None:
    print(cm.get_cross_selling())


def _cmd_get_aliases(args: argparse.Namespace) -> None:
    print(cm.get_aliases())


def _cmd_diagnose(args: argparse.Namespace) -> None:
    print(cm.diagnose())


# ── Escrita ──────────────────────────────────────────────────────

def _cmd_set_peso(args: argparse.Namespace) -> None:
    """Define um componente de peso. Para atualizar apenas um componente,
    primeiro lê os pesos atuais e faz merge."""
    # Ler pesos atuais para fazer merge inteligente
    pesos_atuais = _get_current_pesos(args.cargo, args.colaborador or "")

    # Atualizar o componente solicitado
    pesos_atuais[args.componente] = args.valor

    # Validar soma = 100
    soma = sum(pesos_atuais.values())
    if abs(soma - 100) > 0.01:
        print(f"⚠ Soma dos pesos após alteração: {soma:.1f}% (precisa ser 100%)")
        print(f"  Pesos atuais: {json.dumps(pesos_atuais, indent=2)}")
        print(f"  Ajuste outros componentes para totalizar 100%.")
        return

    result = cm.set_pesos_metas(args.cargo, pesos_atuais, colaborador=args.colaborador or "")
    print(result)


def _get_current_pesos(cargo: str, colaborador: str = "") -> dict:
    """Lê os pesos atuais de um cargo/colaborador do cache."""
    try:
        from scripts import supabase_loader as sl
        pesos_list = sl.load_json("pesos_metas.json")
        for p in pesos_list:
            if (str(p.get("cargo", "")).lower() == cargo.lower()
                    and str(p.get("colaborador", "")).lower() == colaborador.lower()):
                componentes = {
                    "faturamento_linha", "rentabilidade", "conversao_linha",
                    "faturamento_individual", "conversao_individual",
                    "retencao_clientes", "meta_fornecedor_1", "meta_fornecedor_2",
                }
                return {k: p.get(k, 0) for k in componentes if p.get(k, 0) > 0}
    except Exception:
        pass
    return {}


def _cmd_set_meta(args: argparse.Namespace) -> None:
    result = cm.set_meta_aplicacao(
        linha=args.hierarquia,
        tipo_meta=args.tipo,
        valor_meta=args.valor,
        grupo=args.grupo,
        subgrupo=args.subgrupo,
        tipo_mercadoria=args.tipo_mercadoria,
        fabricante=args.fabricante,
    )
    print(result)


def _cmd_set_meta_individual(args: argparse.Namespace) -> None:
    result = cm.set_meta_individual(
        colaborador=args.colaborador,
        cargo=args.cargo or "",
        tipo_meta=args.tipo,
        valor_meta=args.valor,
    )
    print(result)


def _cmd_set_meta_rentabilidade(args: argparse.Namespace) -> None:
    result = cm.set_meta_rentabilidade(
        linha=args.linha,
        grupo=args.grupo,
        subgrupo=args.subgrupo,
        referencia_pct=args.referencia,
        meta_alvo_pct=args.alvo,
        periodo=args.periodo,
    )
    print(result)


def _cmd_set_fc_escada(args: argparse.Namespace) -> None:
    result = cm.set_fc_escada(
        cargo=args.cargo,
        modo=args.modo,
        num_degraus=args.degraus,
        piso_pct=args.piso,
    )
    print(result)


def _cmd_persist(args: argparse.Namespace) -> None:
    result = cm.persist(filename=args.filename)
    print(result)


# ═════════════════════════════════════════════════════════════════════
# ARGUMENT PARSER
# ═════════════════════════════════════════════════════════════════════

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="CLI para gestão de configuração de comissões"
    )
    sub = p.add_subparsers(dest="command", help="Comando a executar")

    # ── Consultas ────────────────────────────────────────────────

    sub.add_parser("summary", help="Visão geral de todas as regras")
    sub.add_parser("get-params", help="Parâmetros operacionais")
    sub.add_parser("list-colaboradores", help="Lista de colaboradores")
    sub.add_parser("diagnose", help="Diagnóstico do ambiente")
    sub.add_parser("get-fc-escada", help="Configuração FC escada por cargo")
    sub.add_parser("get-cross-selling", help="Elegibilidade cross-selling")
    sub.add_parser("get-aliases", help="Mapeamento de aliases")

    # get-regras
    p_regras = sub.add_parser("get-regras", help="Consultar regras de comissão")
    p_regras.add_argument("--linha", type=str, default=None)
    p_regras.add_argument("--grupo", type=str, default=None)
    p_regras.add_argument("--subgrupo", type=str, default=None)
    p_regras.add_argument("--cargo", type=str, default=None)
    p_regras.add_argument("--incluir-inativos", action="store_true")
    p_regras.add_argument("--limite", type=int, default=20)

    # get-pesos
    p_pesos = sub.add_parser("get-pesos", help="Pesos do FC por cargo")
    p_pesos.add_argument("--cargo", type=str, default=None)
    p_pesos.add_argument("--colaborador", type=str, default=None)

    # get-meta
    p_meta = sub.add_parser("get-meta", help="Metas individuais ou de aplicação")
    p_meta.add_argument("--tipo", type=str, default=None,
                        help="faturamento_linha, conversao_linha, etc.")
    p_meta.add_argument("--colaborador", type=str, default=None)
    p_meta.add_argument("--hierarquia", type=str, default=None,
                        help="Linha de negócio (ex: 'Recursos Hídricos')")

    # get-meta-rentabilidade
    p_mr = sub.add_parser("get-meta-rentabilidade", help="Metas de rentabilidade")
    p_mr.add_argument("--mes", type=int, default=None)
    p_mr.add_argument("--ano", type=int, default=None)
    p_mr.add_argument("--linha", type=str, default=None)
    p_mr.add_argument("--limite", type=int, default=20)

    # ── Modificações ─────────────────────────────────────────────

    # set-peso
    p_sp = sub.add_parser("set-peso", help="Alterar peso de um componente do FC")
    p_sp.add_argument("--cargo", required=True, help="Cargo (ex: 'Gerente Comercial')")
    p_sp.add_argument("--componente", required=True,
                      help="faturamento_linha|conversao_linha|rentabilidade|etc.")
    p_sp.add_argument("--valor", required=True, type=float, help="Peso em %% (ex: 45)")
    p_sp.add_argument("--colaborador", type=str, default=None,
                      help="Colaborador específico (opcional)")

    # set-meta
    p_sm = sub.add_parser("set-meta", help="Definir meta de aplicação (hierarquia)")
    p_sm.add_argument("--hierarquia", required=True, help="Linha de negócio")
    p_sm.add_argument("--tipo", required=True,
                      help="faturamento_linha|conversao_linha")
    p_sm.add_argument("--valor", required=True, type=float, help="Valor da meta")
    p_sm.add_argument("--grupo", type=str, default=None)
    p_sm.add_argument("--subgrupo", type=str, default=None)
    p_sm.add_argument("--tipo-mercadoria", type=str, default=None)
    p_sm.add_argument("--fabricante", type=str, default=None)

    # set-meta-individual
    p_smi = sub.add_parser("set-meta-individual", help="Definir meta individual")
    p_smi.add_argument("--colaborador", required=True)
    p_smi.add_argument("--tipo", required=True,
                       help="faturamento_individual|conversao_individual")
    p_smi.add_argument("--valor", required=True, type=float)
    p_smi.add_argument("--cargo", type=str, default=None)

    # set-meta-rentabilidade
    p_smr = sub.add_parser("set-meta-rentabilidade",
                           help="Definir meta de rentabilidade")
    p_smr.add_argument("--linha", required=True)
    p_smr.add_argument("--referencia", type=float, default=None,
                       help="Referência %% (ex: 15.0)")
    p_smr.add_argument("--alvo", type=float, default=None,
                       help="Meta alvo %% (ex: 12.0)")
    p_smr.add_argument("--grupo", type=str, default=None)
    p_smr.add_argument("--subgrupo", type=str, default=None)
    p_smr.add_argument("--periodo", type=str, default=None,
                       help="YYYY ou YYYY-MM")

    # set-fc-escada
    p_sfe = sub.add_parser("set-fc-escada", help="Configurar FC escada para cargo")
    p_sfe.add_argument("--cargo", required=True)
    p_sfe.add_argument("--modo", default="ESCADA", choices=["ESCADA", "RAMPA"])
    p_sfe.add_argument("--degraus", type=int, default=None)
    p_sfe.add_argument("--piso", type=float, default=None, help="Piso em %% (ex: 60)")

    # persist
    p_per = sub.add_parser("persist", help="Salvar alterações no Supabase")
    p_per.add_argument("--filename", type=str, default=None,
                       help="Arquivo específico (ex: pesos_metas.json)")

    return p


# ═════════════════════════════════════════════════════════════════════
# DISPATCH
# ═════════════════════════════════════════════════════════════════════

COMMANDS = {
    "summary": _cmd_summary,
    "get-params": _cmd_get_params,
    "list-colaboradores": _cmd_list_colaboradores,
    "diagnose": _cmd_diagnose,
    "get-regras": _cmd_get_regras,
    "get-pesos": _cmd_get_pesos,
    "get-meta": _cmd_get_meta,
    "get-meta-rentabilidade": _cmd_get_meta_rentabilidade,
    "get-fc-escada": _cmd_get_fc_escada,
    "get-cross-selling": _cmd_get_cross_selling,
    "get-aliases": _cmd_get_aliases,
    "set-peso": _cmd_set_peso,
    "set-meta": _cmd_set_meta,
    "set-meta-individual": _cmd_set_meta_individual,
    "set-meta-rentabilidade": _cmd_set_meta_rentabilidade,
    "set-fc-escada": _cmd_set_fc_escada,
    "persist": _cmd_persist,
}


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    handler = COMMANDS.get(args.command)
    if handler is None:
        print(f"Comando desconhecido: {args.command}")
        parser.print_help()
        sys.exit(1)

    handler(args)


if __name__ == "__main__":
    main()
