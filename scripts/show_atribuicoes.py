"""
=============================================================================
show_atribuicoes.py — Visualizador Comprimido de Atribuições por Hierarquia
=============================================================================
Exibe todas as combinações hierárquicas que possuem atribuições distintas
(colaborador + cargo + fatia + taxa), comprimindo automaticamente os níveis:

  - Se toda uma linha de negócio tem a mesma configuração em TODAS as suas
    sub-hierarquias, exibe apenas "[LINHA] Nome da Linha".
  - Se apenas um grupo específico difere, exibe "[LINHA > GRUPO] ..." separado.
  - Continua aprofundando (subgrupo, tipo_mercadoria, fabricante, aplicacao) conforme
    necessário até encontrar o nível mínimo que descreve sem ambiguidade.

USO:
    python scripts/show_atribuicoes.py
    python scripts/show_atribuicoes.py --linha "Recursos Hídricos"
    python scripts/show_atribuicoes.py --somente-linhas

    Ou como módulo:
        from scripts.show_atribuicoes import run
        print(run())

ALGORITMO DE COMPRESSÃO:
    Para cada nível de profundidade d (1=linha … 6=aplicacao):
      - Agrupa as hierarquias AINDA NÃO COBERTAS pelo prefixo de profundidade d.
      - Se todas as hierarquias dentro do grupo têm o mesmo fingerprint
        (mesmo conjunto de cargo+colaborador+fatia+taxa), emite uma entrada no
        nível d e marca todas como cobertas.
      - Caso contrário, deixa para o próximo nível tentar.
    Hierárquias restantes são emitidas individualmente no nível 6.
=============================================================================
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

from scripts import supabase_loader as _sl


# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTES
# ═══════════════════════════════════════════════════════════════════════════

HIERARCHY_FIELDS: List[str] = [
    "linha",
    "grupo",
    "subgrupo",
    "tipo_mercadoria",
    "fabricante",
    "aplicacao",
]

_LEVEL_LABELS: List[str] = [
    "LINHA",
    "LINHA > GRUPO",
    "LINHA > GRUPO > SUBGRUPO",
    "LINHA > GRUPO > SUBGRUPO > TIPO",
    "LINHA > GRUPO > SUBGRUPO > TIPO > FABRICANTE",
    "LINHA > GRUPO > SUBGRUPO > TIPO > FABRICANTE > APLICAÇÃO",
]


# ═══════════════════════════════════════════════════════════════════════════
# TIPOS
# ═══════════════════════════════════════════════════════════════════════════

# Um único "nó" de atribuição: (cargo, colaborador, fatia_pct, taxa_pct)
AssignmentTuple = Tuple[str, str, float, float]

# Fingerprint: conjunto imutável de AssignmentTuples representando uma config
Fingerprint = FrozenSet[AssignmentTuple]

# Entrada comprimida: (prefixo 1–6 campos, fingerprint)
CompressedEntry = Tuple[Tuple[str, ...], Fingerprint]


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _nv(v: Any) -> str:
    """Normaliza valor para string vazia quando None."""
    return str(v).strip() if v is not None else ""


def _full_key(row: Dict) -> Tuple[str, ...]:
    """Extrai a chave completa de 6 campos de uma row."""
    return tuple(_nv(row.get(f)) for f in HIERARCHY_FIELDS)


def _fingerprint(rows: List[Dict]) -> Fingerprint:
    """Calcula o fingerprint de um grupo de rows.

    Fingerprint = frozenset de (cargo, colaborador, fatia_cargo, taxa_rateio).
    Representa de forma imutável e comparável a configuração de atribuição.
    """
    return frozenset(
        (
            _nv(row.get("cargo")),
            _nv(row.get("colaborador")),
            float(row.get("fatia_cargo") or 0),
            float(row.get("taxa_rateio_maximo_pct") or 0),
        )
        for row in rows
    )


# ═══════════════════════════════════════════════════════════════════════════
# CARREGAMENTO
# ═══════════════════════════════════════════════════════════════════════════

def load_atribuicoes(filtro_linha: Optional[str] = None) -> List[Dict]:
    """Carrega todas as atribuições ativas (cargo + colaborador definidos).

    Args:
        filtro_linha: Se fornecido, filtra apenas a linha especificada.

    Returns:
        Lista de dicts do config_comissao com cargo e colaborador preenchidos.
    """
    config = _sl.load_json("config_comissao.json")
    rows = [
        r for r in config
        if r.get("ativo", True)
        and r.get("cargo")
        and r.get("colaborador")
    ]
    if filtro_linha:
        rows = [
            r for r in rows
            if _nv(r.get("linha")).lower() == filtro_linha.lower()
        ]
    return rows


# ═══════════════════════════════════════════════════════════════════════════
# COMPRESSÃO HIERÁRQUICA
# ═══════════════════════════════════════════════════════════════════════════

def compress_hierarchies(rows: List[Dict]) -> List[CompressedEntry]:
    """Encontra a representação mínima de hierarquias com configs distintas.

    Aplica compressão gulosa do nível mais genérico (linha) ao mais específico
    (fabricante, aplicacao). Um grupo de hierarquias compartilhando o mesmo prefixo é
    emitido no menor nível que ainda descreva uniformemente todas elas.

    Args:
        rows: Lista de dicts de atribuições (com cargo e colaborador).

    Returns:
        Lista de (prefix_tuple, fingerprint) ordenada por profundidade e nome.
        prefix_tuple tem entre 1 e 6 elementos conforme a compressão obtida.
    """
    if not rows:
        return []

    # Agrupa rows por chave completa de 5 campos
    full_map: Dict[Tuple, List[Dict]] = defaultdict(list)
    for r in rows:
        full_map[_full_key(r)].append(r)

    # Calcula fingerprint por chave completa
    fp_map: Dict[Tuple, Fingerprint] = {
        k: _fingerprint(v) for k, v in full_map.items()
    }

    covered: set = set()
    result: List[CompressedEntry] = []

    for depth in range(1, len(HIERARCHY_FIELDS) + 1):
        # Agrupa chaves NÃO cobertas pelo prefixo de profundidade atual
        by_prefix: Dict[Tuple, List[Tuple]] = defaultdict(list)
        for full_key in full_map:
            if full_key not in covered:
                by_prefix[full_key[:depth]].append(full_key)

        for prefix in sorted(by_prefix):
            full_keys_in_group = by_prefix[prefix]

            # Verifica uniformidade: todos os filhos têm o mesmo fingerprint?
            fps = {fp_map[fk] for fk in full_keys_in_group}
            if len(fps) == 1:
                result.append((prefix, fps.pop()))
                covered.update(full_keys_in_group)

    # Qualquer remanescente não coberto (não deveria ocorrer, mas segurança)
    for full_key in sorted(fp_map):
        if full_key not in covered:
            result.append((full_key, fp_map[full_key]))

    return result


# ═══════════════════════════════════════════════════════════════════════════
# FORMATAÇÃO
# ═══════════════════════════════════════════════════════════════════════════

def _format_prefix(prefix: Tuple[str, ...]) -> str:
    """Formata o prefixo hierárquico para exibição."""
    depth = len(prefix)
    label = _LEVEL_LABELS[depth - 1]
    parts = " > ".join(p for p in prefix if p)
    return f"[{label}]  {parts}"


def _format_entry(prefix: Tuple[str, ...], fp: Fingerprint) -> List[str]:
    """Formata uma entrada comprimida em linhas de texto."""
    lines = [_format_prefix(prefix)]

    # Ordena os colaboradores: primeiro por cargo, depois por nome
    sorted_assignments = sorted(fp, key=lambda x: (x[0], x[1]))
    total = len(sorted_assignments)

    for i, (cargo, colaborador, fatia, taxa) in enumerate(sorted_assignments):
        connector = "`--" if i == total - 1 else "|--"
        taxa_str = f"{taxa:.1f}%" if taxa else "—"
        lines.append(
            f"  {connector} {colaborador:<28}  {cargo:<25}  "
            f"fatia={fatia:.1f}%  taxa={taxa_str}"
        )

    return lines


def format_output(
    entries: List[CompressedEntry],
    titulo: str = "ATRIBUIÇÕES POR HIERARQUIA",
) -> str:
    """Formata todas as entradas comprimidas em texto legível.

    Args:
        entries:  Lista de (prefix, fingerprint) gerada por compress_hierarchies.
        titulo:   Título do relatório.

    Returns:
        String com o relatório completo.
    """
    sep = "=" * 72
    lines = [sep, f"  {titulo}", sep, ""]

    if not entries:
        lines.append("  Nenhuma atribuição encontrada.")
        lines.append(sep)
        return "\n".join(lines)

    # Conta estatísticas
    n_entries = len(entries)
    depths = [len(p) for p, _ in entries]
    at_linha = sum(1 for d in depths if d == 1)
    below_linha = n_entries - at_linha

    lines.append(
        f"  {n_entries} bloco(s) de configuração encontrado(s)  "
        f"({at_linha} ao nível LINHA, {below_linha} em nível mais específico)"
    )
    lines.append("")

    current_linha: Optional[str] = None
    for prefix, fp in entries:
        # Separador visual entre linhas de negócio
        linha_atual = prefix[0] if prefix else ""
        if linha_atual != current_linha:
            if current_linha is not None:
                lines.append("")
            current_linha = linha_atual

        lines.extend(_format_entry(prefix, fp))
        lines.append("")

    lines.append(sep)
    return "\n".join(lines)


# ===========================================================================
# PONTO DE ENTRADA
# ═══════════════════════════════════════════════════════════════════════════

def run(
    filtro_linha: Optional[str] = None,
    somente_linhas: bool = False,
) -> str:
    """Executa a visualização completa e retorna o texto formatado.

    Args:
        filtro_linha:   Filtra por linha de negócio específica (opcional).
        somente_linhas: Se True, exibe apenas entradas comprimidas ao nível
                        LINHA (oculta blocos mais específicos).

    Returns:
        Texto formatado do relatório.
    """
    rows = load_atribuicoes(filtro_linha=filtro_linha)
    if not rows:
        return "Nenhuma atribuição ativa encontrada."

    entries = compress_hierarchies(rows)

    if somente_linhas:
        entries = [(p, fp) for p, fp in entries if len(p) == 1]

    titulo = "ATRIBUIÇÕES POR HIERARQUIA"
    if filtro_linha:
        titulo += f" — {filtro_linha}"
    if somente_linhas:
        titulo += " (somente nível LINHA)"

    return format_output(entries, titulo=titulo)


def main() -> None:
    """Ponto de entrada CLI."""
    # Garante UTF-8 no stdout (Windows pode usar cp1252 por padrão)
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Visualiza atribuições de comissão por hierarquia de produto."
    )
    parser.add_argument(
        "--linha",
        metavar="NOME",
        default=None,
        help='Filtra por linha de negócio (ex: "Recursos Hídricos").',
    )
    parser.add_argument(
        "--somente-linhas",
        action="store_true",
        default=False,
        help="Exibe apenas blocos comprimidos ao nível LINHA.",
    )
    args = parser.parse_args()

    print(run(filtro_linha=args.linha, somente_linhas=args.somente_linhas))


if __name__ == "__main__":
    main()
