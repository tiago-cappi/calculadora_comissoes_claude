"""
=============================================================================
SKILL: Robô de Comissões — Script 03a: Atribuição de Comissões
=============================================================================
Módulo   : 03a_atribuicao
Versão   : 1.0.0
Autor    : Claude Commission Skill

Descrição
---------
Determina **quem** ganha comissão por cada item da Análise Comercial,
aplicando as regras de negócio carregadas pelo config_manager.

Para cada item:
1. Resolve aliases dos nomes nas colunas operacionais
2. Identifica colaboradores OPERACIONAIS (colunas AC)
3. Identifica colaboradores de GESTÃO (regras config_comissao)
4. Faz match da regra mais específica (especificidade 0–5)
5. Detecta casos de cross-selling

Saída:
  AtribuicaoResult com DataFrame enriquecido + cross_selling_cases

Dependências
------------
- pandas
- scripts.config_manager (funções internas: _load_json)
=============================================================================
"""

from __future__ import annotations

import json
import os
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

# --- Colunas da Análise Comercial ---
COL_PROCESSO = "Processo"
COL_STATUS = "Status Processo"
COL_NF = "Numero NF"
COL_DT_EMISSAO = "Dt Emissão"
COL_VALOR_REALIZADO = "Valor Realizado"
COL_VALOR_ORCADO = "Valor Orçado"
COL_DATA_ACEITE = "Data Aceite"
COL_CONSULTOR_INTERNO = "Consultor Interno"
COL_REPRESENTANTE = "Representante-pedido"
COL_GERENTE_COMERCIAL = "Gerente Comercial-Pedido"
COL_LINHA = "Linha"
COL_GRUPO = "Grupo"
COL_SUBGRUPO = "Subgrupo"
COL_TIPO_MERCADORIA = "Tipo de Mercadoria"
COL_FABRICANTE = "Fabricante"
COL_APLICACAO = "Aplicação Mat./Serv."
COL_CLIENTE = "Cliente"
COL_NOME_CLIENTE = "Nome Cliente"
COL_CODIGO_PRODUTO = "Código Produto"
COL_OPERACAO = "Operação"

# --- Campos hierárquicos (ordem de especificidade) ---
HIERARCHY_FIELDS = ["linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante", "aplicacao"]
AC_HIERARCHY_COLS = [COL_LINHA, COL_GRUPO, COL_SUBGRUPO, COL_TIPO_MERCADORIA, COL_FABRICANTE, COL_APLICACAO]

# --- Tipos de cargo ---
TIPO_OPERACIONAL = "Operacional"
TIPO_GESTAO = "Gestão"

# --- Nomes descartáveis nas colunas operacionais ---
_SKIP_NAMES = {"", "nan", "none", "nenhum", "null", "-"}


# ═══════════════════════════════════════════════════════════════════════════════
# CARREGAMENTO DE REGRAS — via Supabase (substituiu references/*.json)
# ═══════════════════════════════════════════════════════════════════════════════

from scripts import supabase_loader as _sl


def _load_json(filename: str) -> Any:
    """Carrega regras de negócio do Supabase (equivalente ao JSON original)."""
    return _sl.load_json(filename)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _normalize(text: Any) -> str:
    """Remove acentos e converte para minúsculo para comparação."""
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    s = str(text).strip()
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def _is_skip_name(name: Any) -> bool:
    """Retorna True se o nome deve ser ignorado (vazio, nan, etc)."""
    return _normalize(name) in _SKIP_NAMES


# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING — carrega regras necessárias para atribuição
# ═══════════════════════════════════════════════════════════════════════════════

def _load_config() -> Dict[str, Any]:
    """Carrega todas as configurações necessárias para atribuição.

    Returns:
        Dict com chaves: colaboradores, cargos, config_comissao,
        aliases, cross_selling.
    """
    colaboradores = _load_json("colaboradores.json")
    cargos_raw = _load_json("cargos.json")
    config_comissao = _load_json("config_comissao.json")
    aliases_raw = _load_json("aliases.json")
    cross_selling = _load_json("cross_selling.json")

    # Indexar colaboradores por nome_normalizado → {nome, cargo, ...}
    colab_map: Dict[str, Dict] = {}
    for c in colaboradores:
        key = _normalize(c["nome_colaborador"])
        colab_map[key] = c

    # Indexar cargos: nome_cargo → {tipo_cargo, tipo_comissao}
    cargo_map: Dict[str, Dict] = {}
    for cg in cargos_raw:
        cargo_map[cg["nome_cargo"]] = cg

    # Indexar aliases: nome_normalizado → nome_padrão
    alias_map: Dict[str, str] = {}
    colab_aliases = aliases_raw.get("colaborador", {})
    for alias_key, padrao in colab_aliases.items():
        alias_map[_normalize(alias_key)] = padrao

    # Indexar cross-selling: nome_normalizado → {taxa_pct, ...}
    cs_map: Dict[str, Dict] = {}
    for cs in cross_selling:
        key = _normalize(cs["colaborador"])
        cs_map[key] = cs

    return {
        "colab_map": colab_map,
        "cargo_map": cargo_map,
        "config_comissao": [r for r in config_comissao if r.get("ativo", True)],
        "alias_map": alias_map,
        "cs_map": cs_map,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ALIAS RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════════

def resolve_alias(name: Any, alias_map: Dict[str, str]) -> str:
    """Resolve um nome através do mapa de aliases.

    Retorna o nome padrão se encontrar alias, ou o nome original (stripped).
    """
    if _is_skip_name(name):
        return ""
    name_str = str(name).strip()
    key = _normalize(name_str)
    return alias_map.get(key, name_str)


def apply_aliases_to_df(
    df: pd.DataFrame, alias_map: Dict[str, str]
) -> pd.DataFrame:
    """Aplica resolução de aliases nas colunas operacionais do DataFrame.

    Colunas afetadas: Consultor Interno, Representante-pedido,
    Gerente Comercial-Pedido.
    """
    df = df.copy()
    for col in [COL_CONSULTOR_INTERNO, COL_REPRESENTANTE, COL_GERENTE_COMERCIAL]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: resolve_alias(x, alias_map))
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# RULE MATCHING — match hierárquico com especificidade
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_hierarchy(item: pd.Series) -> Dict[str, Optional[str]]:
    """Extrai a hierarquia de produto de uma linha da AC.

    Returns:
        {"linha": str|None, "grupo": str|None, ...}
    """
    result = {}
    for json_key, ac_col in zip(HIERARCHY_FIELDS, AC_HIERARCHY_COLS):
        val = item.get(ac_col)
        if pd.isna(val) or str(val).strip() == "":
            result[json_key] = None
        else:
            result[json_key] = str(val).strip()
    return result


def _calc_specificity(regra: Dict) -> int:
    """Calcula a especificidade de uma regra (0–6).

    Conta quantos campos hierárquicos estão definidos (não-None).
    """
    count = 0
    for field_name in HIERARCHY_FIELDS:
        val = regra.get(field_name)
        if val is not None and str(val).strip() != "":
            count += 1
    return count


def _rule_matches_hierarchy(regra: Dict, hierarchy: Dict[str, Optional[str]]) -> bool:
    """Verifica se uma regra combina com a hierarquia de um item.

    Para cada campo hierárquico da regra:
    - None/vazio na regra → wildcard (aceita qualquer valor)
    - Valor definido → deve ser igual ao do item (case-insensitive)
    """
    for field_name in HIERARCHY_FIELDS:
        rule_val = regra.get(field_name)
        if rule_val is None or str(rule_val).strip() == "":
            continue  # wildcard
        item_val = hierarchy.get(field_name)
        if item_val is None:
            return False
        if _normalize(rule_val) != _normalize(item_val):
            return False
    return True


def _find_matching_rules(
    hierarchy: Dict[str, Optional[str]],
    config_comissao: List[Dict],
) -> List[Dict]:
    """Retorna todas as regras ativas que combinam com a hierarquia."""
    return [
        regra
        for regra in config_comissao
        if regra.get("ativo", True) and _rule_matches_hierarchy(regra, hierarchy)
    ]


def find_best_rule(
    hierarchy: Dict[str, Optional[str]],
    config_comissao: List[Dict],
) -> List[Dict]:
    """Encontra todas as linhas de regra com maior especificidade para a hierarquia.

    No schema flat, cada linha representa uma hierarquia+cargo.
    Retorna todas as linhas que têm a melhor especificidade (pode ser N cargos).
    """
    best_spec = -1
    candidates: List[Dict] = []

    for regra in _find_matching_rules(hierarchy, config_comissao):
        spec = _calc_specificity(regra)
        if spec > best_spec:
            best_spec = spec
            candidates = [regra]
        elif spec == best_spec:
            candidates.append(regra)

    return candidates


def _find_best_rules_for_colaborador_cargo(
    hierarchy: Dict[str, Optional[str]],
    config_comissao: List[Dict],
    cargo_nome: str,
    nome_colaborador: str,
    *,
    include_generic: bool,
) -> List[Dict]:
    """Seleciona a melhor regra da hierarquia para um colaborador+cargo."""
    nome_norm = _normalize(nome_colaborador)
    candidates: List[Dict] = []

    for regra in _find_matching_rules(hierarchy, config_comissao):
        if regra.get("cargo") != cargo_nome:
            continue

        colab_regra = regra.get("colaborador")
        if colab_regra and _normalize(colab_regra) == nome_norm:
            candidates.append(regra)
            continue

        if include_generic and _is_skip_name(colab_regra):
            candidates.append(regra)

    if not candidates:
        return []

    best_spec = max(_calc_specificity(regra) for regra in candidates)
    best_rules = [regra for regra in candidates if _calc_specificity(regra) == best_spec]
    nominative_rules = [
        regra
        for regra in best_rules
        if regra.get("colaborador") and _normalize(regra["colaborador"]) == nome_norm
    ]
    resultado_final = nominative_rules or best_rules
    try:
        from scripts.audit.trace_collector import TraceCollector
        if TraceCollector.is_enabled():
            item_key = f"{hierarchy.get('linha', '')}/{nome_colaborador}"
            TraceCollector.record(item_key, "atribuicao", {
                "regras_candidatas": [{"hierarquia": r.get("hierarquia", ""), "especificidade": _calc_specificity(r)} for r in candidates],
                "regra_selecionada": resultado_final[0] if resultado_final else None,
                "motivo": f"Especificidade {best_spec}" if candidates else "Sem candidatos",
            })
    except Exception:
        pass
    return resultado_final


def _build_item_base(
    item: pd.Series,
    hierarchy: Dict[str, Optional[str]],
    spec: int,
) -> Dict[str, Any]:
    """Monta o registro-base do item truncado na especificidade da regra."""
    hier_fields_for_base = {}
    for i, (json_key, ac_col) in enumerate(zip(HIERARCHY_FIELDS, AC_HIERARCHY_COLS)):
        if i < spec:
            hier_fields_for_base[ac_col] = hierarchy.get(json_key, "")
        else:
            hier_fields_for_base[ac_col] = ""

    return {
        "idx_ac": item.name,
        COL_PROCESSO: item.get(COL_PROCESSO, ""),
        COL_NF: item.get(COL_NF, ""),
        COL_DT_EMISSAO: item.get(COL_DT_EMISSAO, ""),
        COL_VALOR_REALIZADO: item.get(COL_VALOR_REALIZADO, 0),
        COL_VALOR_ORCADO: item.get(COL_VALOR_ORCADO, 0),
        COL_LINHA: hier_fields_for_base[COL_LINHA],
        COL_GRUPO: hier_fields_for_base[COL_GRUPO],
        COL_SUBGRUPO: hier_fields_for_base[COL_SUBGRUPO],
        COL_TIPO_MERCADORIA: hier_fields_for_base[COL_TIPO_MERCADORIA],
        COL_FABRICANTE: hier_fields_for_base[COL_FABRICANTE],
        COL_APLICACAO: hier_fields_for_base[COL_APLICACAO],
        COL_STATUS: item.get(COL_STATUS, ""),
        COL_CLIENTE: item.get(COL_CLIENTE, ""),
        COL_NOME_CLIENTE: item.get(COL_NOME_CLIENTE, ""),
        COL_OPERACAO: item.get(COL_OPERACAO, ""),
        COL_CODIGO_PRODUTO: item.get(COL_CODIGO_PRODUTO, ""),
        "Descrição Produto": item.get("Descrição Produto", ""),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ATTRIBUTION — determina quem ganha comissão por cada item
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class ColaboradorAtribuido:
    """Um colaborador com direito a comissão por um item."""
    nome: str
    cargo: str
    tipo_cargo: str  # "Operacional" ou "Gestão"
    tipo_comissao: str  # "Faturamento" ou "Recebimento"
    fatia_cargo_pct: float  # PE — percentual da taxa que o cargo recebe
    taxa_rateio_pct: float  # taxa máxima da hierarquia (ou customizada)
    fonte: str  # "AC" ou "CONFIG"
    regra_especificidade: int  # 0–6


@dataclass
class CrossSellingCase:
    """Um caso de cross-selling detectado."""
    processo: str
    consultor: str
    cargo: str
    linha_item: str
    taxa_cross_selling_pct: float
    itens_afetados: int = 0


@dataclass
class AtribuicaoResult:
    """Resultado completo da atribuição."""
    atribuicoes: List[Dict[str, Any]] = field(default_factory=list)
    cross_selling_cases: List[CrossSellingCase] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0

    def summary(self) -> str:
        """Resumo textual da atribuição."""
        lines = [
            f"{'='*60}",
            f"  ATRIBUIÇÃO — Resumo",
            f"{'='*60}",
            f"  Total de atribuições      : {len(self.atribuicoes):>7,}",
            f"  Casos de cross-selling     : {len(self.cross_selling_cases):>7}",
        ]
        # Contagem por tipo
        by_tipo = {}
        for a in self.atribuicoes:
            t = a.get("tipo_cargo", "?")
            by_tipo[t] = by_tipo.get(t, 0) + 1
        for t, n in sorted(by_tipo.items()):
            lines.append(f"    {t:<25}: {n:>7,}")

        # Contagem por cargo
        lines.append(f"{'─'*60}")
        by_cargo = {}
        for a in self.atribuicoes:
            c = a.get("cargo", "?")
            by_cargo[c] = by_cargo.get(c, 0) + 1
        for c, n in sorted(by_cargo.items()):
            lines.append(f"    {c:<25}: {n:>7,}")

        if self.warnings:
            lines.append(f"{'─'*60}")
            lines.append(f"  ⚠ Avisos ({len(self.warnings)}):")
            for w in self.warnings[:20]:
                lines.append(f"    • {w}")
            if len(self.warnings) > 20:
                lines.append(f"    ... e mais {len(self.warnings) - 20}")
        if self.errors:
            lines.append(f"  ✖ Erros ({len(self.errors)}):")
            for e in self.errors[:10]:
                lines.append(f"    • {e}")
        if not self.warnings and not self.errors:
            lines.append(f"  ✔ Nenhum aviso ou erro.")
        lines.append(f"{'='*60}")
        return "\n".join(lines)

    def to_dataframe(self) -> pd.DataFrame:
        """Converte atribuições para DataFrame."""
        if not self.atribuicoes:
            return pd.DataFrame()
        return pd.DataFrame(self.atribuicoes)


def _get_operacional_names(item: pd.Series) -> List[Tuple[str, str]]:
    """Extrai nomes de colaboradores operacionais das colunas da AC.

    Suporta múltiplos nomes separados por ';'.

    Returns:
        Lista de (nome, coluna_origem)
    """
    results = []
    for col in [COL_CONSULTOR_INTERNO, COL_REPRESENTANTE]:
        val = item.get(col, "")
        if _is_skip_name(val):
            continue
        # Suporta múltiplos nomes via ";"
        names = str(val).split(";")
        for name in names:
            name = name.strip()
            if not _is_skip_name(name):
                results.append((name, col))
    return results


def _resolve_cargo(
    nome: str,
    coluna_origem: str,
    colab_map: Dict[str, Dict],
    cargo_map: Dict[str, Dict],
) -> Tuple[str, str, str]:
    """Resolve cargo, tipo_cargo e tipo_comissao de um colaborador.

    Returns:
        (cargo, tipo_cargo, tipo_comissao)
    """
    key = _normalize(nome)
    colab = colab_map.get(key)

    if colab:
        cargo_nome = colab["cargo"]
    else:
        # Fallback: inferir cargo pela coluna de origem
        if coluna_origem == COL_CONSULTOR_INTERNO:
            cargo_nome = "Consultor Interno"
        elif coluna_origem == COL_REPRESENTANTE:
            cargo_nome = "Representante Comercial"
        else:
            cargo_nome = "Consultor Externo"

    cargo_info = cargo_map.get(cargo_nome, {})
    tipo_cargo = cargo_info.get("tipo_cargo", TIPO_OPERACIONAL)
    tipo_comissao = cargo_info.get("tipo_comissao", "Faturamento")

    return cargo_nome, tipo_cargo, tipo_comissao


def _find_gestao_for_hierarchy(
    hierarchy: Dict[str, Optional[str]],
    config_comissao: List[Dict],
    cargo_map: Dict[str, Dict],
    colab_map: Dict[str, Dict],
) -> List[Dict]:
    """Encontra colaboradores de GESTÃO atribuídos à hierarquia.

    Cada elemento de `regras` é uma linha de config_comissao com campos:
    cargo, colaborador, fatia_cargo, taxa_rateio_maximo_pct.

    Para cargos de Gestão:
    - Se `colaborador` estiver preenchido: atribuição direta (nominativa).
    - Se `colaborador` for NULL/vazio: regra genérica — expande para TODOS
      os colaboradores cadastrados com aquele cargo (ex: uma regra de
      Coordenador sem colaborador se aplica a todos os Coordenadores).

    Apenas cargos com tipo_comissao = "Faturamento" são processados aqui.
    Cargos com tipo_comissao = "Recebimento" (ex: Gerente Linha) são ignorados
    neste fluxo — serão tratados no módulo de comissões por recebimento.

    Returns:
        Lista de dicts {nome, cargo, tipo_cargo, tipo_comissao,
        fatia_cargo_pct, taxa_rateio_pct}
    """
    results = []

    candidatos_por_cargo: Dict[str, set[str]] = {}

    for regra in _find_matching_rules(hierarchy, config_comissao):
        cargo_nome = regra.get("cargo")
        if not cargo_nome:
            continue

        cargo_info = cargo_map.get(cargo_nome, {})
        if cargo_info.get("tipo_cargo") != TIPO_GESTAO:
            continue

        # Ignorar cargos de Recebimento (ex: Gerente Linha) — fora do escopo atual
        if cargo_info.get("tipo_comissao", "Faturamento") != "Faturamento":
            continue

        colaborador = regra.get("colaborador")
        if colaborador and not _is_skip_name(colaborador):
            candidatos_por_cargo.setdefault(cargo_nome, set()).add(str(colaborador).strip())
        else:
            for colab in colab_map.values():
                if colab.get("cargo") == cargo_nome:
                    candidatos_por_cargo.setdefault(cargo_nome, set()).add(colab["nome_colaborador"])

    for cargo_nome, colaboradores in candidatos_por_cargo.items():
        for colaborador in sorted(colaboradores):
            regras_colaborador = _find_best_rules_for_colaborador_cargo(
                hierarchy,
                config_comissao,
                cargo_nome,
                colaborador,
                include_generic=True,
            )
            if not regras_colaborador:
                continue

            regra = regras_colaborador[0]
            fatia = float(regra.get("fatia_cargo") or 0)
            if fatia <= 0:
                continue

            results.append({
                "nome": colaborador,
                "cargo": cargo_nome,
                "tipo_cargo": TIPO_GESTAO,
                "tipo_comissao": "Faturamento",
                "fatia_cargo_pct": fatia,
                "taxa_rateio_pct": float(regra.get("taxa_rateio_maximo_pct") or 0),
                "regra_especificidade": _calc_specificity(regra),
            })

    return results


def _check_operacional_vinculo(
    nome_canonico: str,
    cargo_nome: str,
    regras: List[Dict],
) -> bool:
    """Verifica se um colaborador operacional tem vínculo na regra da hierarquia.

    Para ser comissionado, o colaborador Operacional (CI, CE, RC) precisa ter
    seu nome canônico cadastrado como `colaborador` em alguma das regras que
    fazem match com a hierarquia do item, para o seu cargo específico.

    Args:
        nome_canonico : nome canônico do colaborador (já resolvido via alias)
        cargo_nome    : cargo do colaborador (ex: "Consultor Interno")
        regras        : regras de config_comissao que fizeram match com o item

    Returns:
        True se vínculo encontrado, False caso contrário.
    """
    nome_norm = _normalize(nome_canonico)
    for regra in regras:
        if regra.get("cargo") != cargo_nome:
            continue
        colab_regra = regra.get("colaborador")
        if colab_regra and _normalize(colab_regra) == nome_norm:
            return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# CROSS-SELLING DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_cross_selling_for_item(
    item: pd.Series,
    hierarchy: Dict[str, Optional[str]],
    regras: List[Dict],
    config: Dict[str, Any],
) -> Optional[CrossSellingCase]:
    """Detecta cross-selling para um item individual.

    Condições (todas devem ser verdadeiras):
    1. Gerente Comercial-Pedido está preenchido
    2. O nome corresponde a um Consultor Externo
    3. O consultor NÃO tem atribuição para a linha do item
    4. O consultor está na lista CROSS_SELLING
    """
    gerente_raw = item.get(COL_GERENTE_COMERCIAL, "")
    if _is_skip_name(gerente_raw):
        return None

    gerente_nome = str(gerente_raw).strip()
    gerente_key = _normalize(gerente_nome)

    # Cond 2: deve ser Consultor Externo
    colab = config["colab_map"].get(gerente_key)
    if not colab:
        return None
    cargo = colab["cargo"]
    cargo_info = config["cargo_map"].get(cargo, {})
    if cargo_info.get("tipo_cargo") != TIPO_OPERACIONAL:
        return None
    if "externo" not in _normalize(cargo):
        return None

    # Cond 3: consultor NÃO tem vínculo nominativo para a linha do item
    linha_item = hierarchy.get("linha")
    if linha_item is None:
        return None

    gerente_key_norm = _normalize(gerente_nome)
    has_attribution = any(
        r.get("cargo") == cargo
        and r.get("linha") and _normalize(r["linha"]) == _normalize(linha_item)
        and r.get("colaborador") and _normalize(r["colaborador"]) == gerente_key_norm
        and float(r.get("fatia_cargo") or 0) > 0
        for r in config["config_comissao"]
    )
    if has_attribution:
        return None

    # Cond 4: consultor está na lista CROSS_SELLING
    cs_entry = config["cs_map"].get(gerente_key)
    if not cs_entry:
        return None

    processo = str(item.get(COL_PROCESSO, ""))
    return CrossSellingCase(
        processo=processo,
        consultor=gerente_nome,
        cargo=cargo,
        linha_item=linha_item or "",
        taxa_cross_selling_pct=cs_entry.get("taxa_cross_selling_pct", 0),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def execute(
    df_analise_comercial: pd.DataFrame,
) -> AtribuicaoResult:
    """Executa a atribuição completa de comissões.

    Para cada item da Análise Comercial:
    1. Resolve aliases dos nomes
    2. Encontra a regra mais específica para a hierarquia do item
    3. Atribui colaboradores operacionais (das colunas AC)
    4. Atribui colaboradores de gestão (das regras)
    5. Calcula fator split
    6. Detecta cross-selling

    Args:
        df_analise_comercial: DataFrame da AC já enriquecido (output do loader)

    Returns:
        AtribuicaoResult com lista de atribuições e casos de cross-selling
    """
    config = _load_config()
    result = AtribuicaoResult()

    # Aplicar aliases no DataFrame
    df = apply_aliases_to_df(df_analise_comercial, config["alias_map"])

    # Rastrear cross-selling por processo (evitar duplicatas)
    cs_by_processo: Dict[str, CrossSellingCase] = {}

    for idx, item in df.iterrows():
        hierarchy = _extract_hierarchy(item)
        regras_match = _find_matching_rules(hierarchy, config["config_comissao"])

        if not regras_match:
            result.warnings.append(
                f"Item idx={idx}: sem regra para hierarquia "
                f"{hierarchy.get('linha')}/{hierarchy.get('grupo')}/"
                f"{hierarchy.get('subgrupo')}/{hierarchy.get('tipo_mercadoria')}"
            )
            continue

        # --- Atribuir OPERACIONAIS ---
        # Nomes já com alias resolvido (feito pelo apply_aliases_to_df no início)
        operacionais_raw = _get_operacional_names(item)
        operacionais: List[Dict] = []

        for nome, coluna in operacionais_raw:
            cargo_nome, tipo_cargo, tipo_comissao = _resolve_cargo(
                nome, coluna, config["colab_map"], config["cargo_map"]
            )

            # Ignorar colaboradores de Gestão na coluna operacional da AC
            # (ex: Coordenador aparecendo na coluna "Consultor Interno")
            if tipo_cargo == TIPO_GESTAO:
                continue

            regras_colaborador = _find_best_rules_for_colaborador_cargo(
                hierarchy,
                config["config_comissao"],
                cargo_nome,
                nome,
                include_generic=False,
            )
            if not regras_colaborador:
                result.warnings.append(
                    f"⚠ Item idx={idx} (processo {item.get(COL_PROCESSO, '?')}): "
                    f"'{nome}' ({cargo_nome}) está na AC mas NÃO tem vínculo "
                    f"com a hierarquia "
                    f"{hierarchy.get('linha')}/{hierarchy.get('grupo')}/"
                    f"{hierarchy.get('subgrupo')} — não comissionado."
                )
                continue

            regra_cargo = regras_colaborador[0]
            spec = _calc_specificity(regra_cargo)
            item_base = _build_item_base(item, hierarchy, spec)

            operacionais.append({
                **item_base,
                "nome": nome,
                "cargo": cargo_nome,
                "tipo_cargo": tipo_cargo,
                "tipo_comissao": tipo_comissao,
                "fatia_cargo_pct": float(regra_cargo.get("fatia_cargo") or 0),
                "taxa_rateio_pct": float(regra_cargo.get("taxa_rateio_maximo_pct") or 0),
                "fonte": "AC",
                "regra_especificidade": spec,
            })

        # --- Atribuir GESTÃO ---
        # Cargos de Gestão são atribuídos pelo nome cadastrado na regra (nominativo)
        # Cargos de Recebimento (ex: Gerente Linha) são ignorados aqui
        gestao_bruta = _find_gestao_for_hierarchy(
            hierarchy,
            config["config_comissao"],
            config["cargo_map"],
            config["colab_map"],
        )
        gestao: List[Dict] = []
        for g in gestao_bruta:
            item_base = _build_item_base(item, hierarchy, g["regra_especificidade"])
            gestao.append({
                **item_base,
                **g,
                "fonte": "CONFIG",
            })

        # --- Combinar e aplicar split ---
        todos = operacionais + gestao

        # Deduplicar: mesmo (nome_lower, cargo) → manter apenas 1
        seen = set()
        deduped = []
        for a in todos:
            key = (_normalize(a["nome"]), a["cargo"])
            if key not in seen:
                seen.add(key)
                deduped.append(a)

        # --- Registrar atribuições ---
        for a in deduped:
            result.atribuicoes.append(a)

        # --- Detectar cross-selling ---
        cs = _detect_cross_selling_for_item(item, hierarchy, regras_match, config)
        if cs is not None:
            processo = cs.processo
            if processo not in cs_by_processo:
                cs_by_processo[processo] = cs
                cs.itens_afetados = 1
            else:
                cs_by_processo[processo].itens_afetados += 1

    result.cross_selling_cases = list(cs_by_processo.values())

    if result.cross_selling_cases:
        result.warnings.append(
            f"Detectados {len(result.cross_selling_cases)} caso(s) de cross-selling."
        )

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

def _run_tests():
    """Testes embutidos — execute com: python scripts/atribuicao.py --test"""
    import sys
    import tempfile

    passed = 0
    failed = 0
    total = 0

    def _assert(condition, msg=""):
        nonlocal passed, failed, total
        total += 1
        if condition:
            passed += 1
            print(f"  ✓ Test {total}: {msg}")
        else:
            failed += 1
            print(f"  ✗ Test {total}: {msg}")

    print("=" * 60)
    print("  ATRIBUICAO.PY — Testes Embutidos")
    print("=" * 60)

    # ── Test helpers ──
    print("\n── Helpers ──")

    _assert(_normalize("André Caramello") == "andre caramello", "_normalize acentos")
    _assert(_normalize(None) == "", "_normalize None")
    _assert(_normalize(float("nan")) == "", "_normalize nan")
    _assert(_is_skip_name(""), "skip empty")
    _assert(_is_skip_name("nan"), "skip nan")
    _assert(_is_skip_name("Nenhum"), "skip Nenhum")
    _assert(not _is_skip_name("João Silva"), "not skip valid name")

    # ── Test hierarchy extraction ──
    print("\n── Hierarchy extraction ──")
    item_series = pd.Series({
        COL_LINHA: "Hidrologia",
        COL_GRUPO: "Equipamentos",
        COL_SUBGRUPO: "Bombas",
        COL_TIPO_MERCADORIA: "Produto",
        COL_FABRICANTE: "QED",
    })
    h = _extract_hierarchy(item_series)
    _assert(h["linha"] == "Hidrologia", "hierarchy linha")
    _assert(h["fabricante"] == "QED", "hierarchy fabricante")

    # ── Test specificity ──
    print("\n── Specificity ──")
    r_all = {"linha": "A", "grupo": "B", "subgrupo": "C", "tipo_mercadoria": "D", "fabricante": "E"}
    _assert(_calc_specificity(r_all) == 5, "spec 5")

    r_partial = {"linha": "A", "grupo": None, "subgrupo": None, "tipo_mercadoria": "D"}
    _assert(_calc_specificity(r_partial) == 2, "spec 2")

    r_empty = {}
    _assert(_calc_specificity(r_empty) == 0, "spec 0")

    # ── Test rule matching ──
    print("\n── Rule matching (flat schema) ──")
    regra_specific = {
        "linha": "Hidrologia", "grupo": "Equipamentos", "subgrupo": "Bombas",
        "tipo_mercadoria": "Produto", "fabricante": None,
        "cargo": "Consultor Interno", "fatia_cargo": 25.0,
        "taxa_rateio_maximo_pct": 8.0, "taxa_maxima_efetiva": 2.0, "ativo": True,
    }
    regra_wildcard = {
        "linha": "Hidrologia", "grupo": None, "subgrupo": None,
        "tipo_mercadoria": None, "fabricante": None,
        "cargo": "Consultor Interno", "fatia_cargo": 25.0,
        "taxa_rateio_maximo_pct": 5.0, "taxa_maxima_efetiva": 1.25, "ativo": True,
    }
    regra_wrong = {
        "linha": "Ambiental", "grupo": None, "subgrupo": None,
        "tipo_mercadoria": None, "fabricante": None,
        "cargo": "Consultor Interno", "fatia_cargo": 20.0,
        "taxa_rateio_maximo_pct": 3.0, "taxa_maxima_efetiva": 0.6, "ativo": True,
    }

    h_test = {"linha": "Hidrologia", "grupo": "Equipamentos", "subgrupo": "Bombas",
              "tipo_mercadoria": "Produto", "fabricante": "QED"}

    _assert(_rule_matches_hierarchy(regra_specific, h_test), "specific matches")
    _assert(_rule_matches_hierarchy(regra_wildcard, h_test), "wildcard matches")
    _assert(not _rule_matches_hierarchy(regra_wrong, h_test), "wrong linha no match")

    best_list = find_best_rule(h_test, [regra_wildcard, regra_specific, regra_wrong])
    _assert(len(best_list) == 1, "best rule returns 1 item")
    _assert(best_list[0]["taxa_rateio_maximo_pct"] == 8.0, "best rule taxa = 8.0")

    # ── Test best rule when only wildcard exists ──
    h_generic = {"linha": "Hidrologia", "grupo": "Novo", "subgrupo": "Novo",
                 "tipo_mercadoria": "Serviço", "fabricante": None}
    best2 = find_best_rule(h_generic, [regra_specific, regra_wildcard, regra_wrong])
    _assert(len(best2) == 1 and best2[0]["taxa_rateio_maximo_pct"] == 5.0, "fallback to wildcard rule")

    # ── Test no match ──
    h_no = {"linha": "Inexistente", "grupo": None, "subgrupo": None,
            "tipo_mercadoria": None, "fabricante": None}
    _assert(find_best_rule(h_no, [regra_specific, regra_wildcard, regra_wrong]) == [],
            "no match returns empty list")

    # ── Test alias resolution ──
    print("\n── Alias resolution ──")
    alias_map = {"andrey.andrade": "Andrey Andrade", "valmir cardoso flor": "Valmir"}
    _assert(resolve_alias("ANDREY.ANDRADE", alias_map) == "Andrey Andrade", "alias resolve")
    _assert(resolve_alias("João Novo", alias_map) == "João Novo", "unknown keeps original")
    _assert(resolve_alias("", alias_map) == "", "empty → empty")
    _assert(resolve_alias(None, alias_map) == "", "None → empty")

    # ── Test apply_aliases_to_df ──
    print("\n── Apply aliases to DF ──")
    df_test = pd.DataFrame({
        COL_CONSULTOR_INTERNO: ["ANDREY.ANDRADE", "João"],
        COL_REPRESENTANTE: ["VALMIR CARDOSO FLOR", ""],
        COL_GERENTE_COMERCIAL: ["", "NaN"],
    })
    df_resolved = apply_aliases_to_df(df_test, alias_map)
    _assert(df_resolved[COL_CONSULTOR_INTERNO].iloc[0] == "Andrey Andrade", "df alias CI")
    _assert(df_resolved[COL_REPRESENTANTE].iloc[0] == "Valmir", "df alias Rep")

    # ── Test operacional name extraction ──
    print("\n── Operacional names ──")
    item_op = pd.Series({
        COL_CONSULTOR_INTERNO: "Dener Martins; Samanta",
        COL_REPRESENTANTE: "nan",
    })
    names = _get_operacional_names(item_op)
    _assert(len(names) == 2, "2 operacional names via ;")
    _assert(names[0][0] == "Dener Martins", "first name")
    _assert(names[1][0] == "Samanta", "second name")

    item_empty = pd.Series({COL_CONSULTOR_INTERNO: "", COL_REPRESENTANTE: "Nenhum"})
    _assert(len(_get_operacional_names(item_empty)) == 0, "skip empty/Nenhum")

    # ── Test cross-selling detection (flat schema) ──
    print("\n── Cross-selling detection ──")
    mock_config = {
        "colab_map": {
            "andre camargo": {"nome_colaborador": "André Camargo", "cargo": "Consultor Externo"},
            "joao silva": {"nome_colaborador": "João Silva", "cargo": "Consultor Interno"},
        },
        "cargo_map": {
            "Consultor Externo": {"tipo_cargo": "Operacional", "tipo_comissao": "Faturamento"},
            "Consultor Interno": {"tipo_cargo": "Operacional", "tipo_comissao": "Faturamento"},
        },
        "config_comissao": [
            {"linha": "Hidrologia", "grupo": None, "subgrupo": None,
             "tipo_mercadoria": None, "fabricante": None,
             "cargo": "Consultor Externo", "colaborador": "André Camargo",
             "fatia_cargo": 15.0, "taxa_rateio_maximo_pct": 8.0,
             "taxa_maxima_efetiva": 1.2, "ativo": True},
        ],
        "cs_map": {
            "andre camargo": {"colaborador": "André Camargo", "taxa_cross_selling_pct": 1.0},
        },
        "alias_map": {},
        "params": {"cross_selling_default_option": "B"},
    }

    # Caso: CE em linha diferente → cross-selling
    item_cs = pd.Series({
        COL_GERENTE_COMERCIAL: "André Camargo",
        COL_PROCESSO: "P-001",
        COL_LINHA: "Ambiental",
    })
    h_cs = {"linha": "Ambiental", "grupo": None, "subgrupo": None,
            "tipo_mercadoria": None, "fabricante": None}
    cs_result = _detect_cross_selling_for_item(item_cs, h_cs, [], mock_config)
    _assert(cs_result is not None, "cross-selling detected for CE in foreign line")
    _assert(cs_result.consultor == "André Camargo", "cs consultor name")
    _assert(cs_result.taxa_cross_selling_pct == 1.0, "cs taxa")

    # Caso: CE na sua própria linha → sem cross-selling
    item_own = pd.Series({
        COL_GERENTE_COMERCIAL: "André Camargo",
        COL_PROCESSO: "P-002",
        COL_LINHA: "Hidrologia",
    })
    h_own = {"linha": "Hidrologia", "grupo": None, "subgrupo": None,
             "tipo_mercadoria": None, "fabricante": None}
    _assert(
        _detect_cross_selling_for_item(item_own, h_own, [], mock_config) is None,
        "no cross-selling when CE has own line attribution"
    )

    # Caso: CI (não externo) → sem cross-selling
    item_ci = pd.Series({
        COL_GERENTE_COMERCIAL: "João Silva",
        COL_PROCESSO: "P-003",
        COL_LINHA: "Ambiental",
    })
    h_ci = {"linha": "Ambiental", "grupo": None, "subgrupo": None,
            "tipo_mercadoria": None, "fabricante": None}
    _assert(
        _detect_cross_selling_for_item(item_ci, h_ci, [], mock_config) is None,
        "no cross-selling for CI"
    )

    # Caso: campo vazio → sem cross-selling
    item_empty_gc = pd.Series({COL_GERENTE_COMERCIAL: "", COL_PROCESSO: "P-004"})
    _assert(
        _detect_cross_selling_for_item(
            item_empty_gc, {"linha": None}, [], mock_config
        ) is None,
        "no cross-selling when Gerente Comercial empty"
    )

    # ── Test full execute (mini integration) — flat schema ──
    print("\n── Full execute (mini integration) ──")

    _sl.clear_cache()
    _sl._CACHE["colaboradores.json"] = [
        {"id_colaborador": "C001", "nome_colaborador": "Paulo Negrão", "cargo": "Diretor"},
        {"id_colaborador": "C002", "nome_colaborador": "Dener Martins", "cargo": "Consultor Interno"},
        {"id_colaborador": "C003", "nome_colaborador": "André Camargo", "cargo": "Consultor Externo"},
    ]
    _sl._CACHE["cargos.json"] = [
        {"nome_cargo": "Diretor", "tipo_cargo": "Gestão", "tipo_comissao": "Faturamento", "TIPO_COMISSAO": "Faturamento"},
        {"nome_cargo": "Consultor Interno", "tipo_cargo": "Operacional", "tipo_comissao": "Faturamento", "TIPO_COMISSAO": "Faturamento"},
        {"nome_cargo": "Consultor Externo", "tipo_cargo": "Operacional", "tipo_comissao": "Faturamento", "TIPO_COMISSAO": "Faturamento"},
    ]
    # Flat schema: 3 linhas para a mesma hierarquia (uma por cargo+colaborador)
    # CI e Diretor têm colaborador definido (vínculo nominativo)
    _sl._CACHE["config_comissao.json"] = [
        {"linha": "Hidrologia", "grupo": None, "subgrupo": None, "tipo_mercadoria": None, "fabricante": None,
         "cargo": "Consultor Interno", "colaborador": "Dener Martins",
         "fatia_cargo": 25.0, "taxa_rateio_maximo_pct": 8.0, "taxa_maxima_efetiva": 2.0, "ativo": True},
        {"linha": "Hidrologia", "grupo": None, "subgrupo": None, "tipo_mercadoria": None, "fabricante": None,
         "cargo": "Consultor Externo", "colaborador": "André Camargo",
         "fatia_cargo": 15.0, "taxa_rateio_maximo_pct": 8.0, "taxa_maxima_efetiva": 1.2, "ativo": True},
        {"linha": "Hidrologia", "grupo": None, "subgrupo": None, "tipo_mercadoria": None, "fabricante": None,
         "cargo": "Diretor", "colaborador": "Paulo Negrão",
         "fatia_cargo": 10.0, "taxa_rateio_maximo_pct": 8.0, "taxa_maxima_efetiva": 0.8, "ativo": True},
    ]
    _sl._CACHE["aliases.json"] = {"colaborador": {"DENER.MARTINS": "Dener Martins"}}
    _sl._CACHE["cross_selling.json"] = [
        {"colaborador": "André Camargo", "taxa_cross_selling_pct": 1.0},
    ]

    df_ac = pd.DataFrame([
        {
            COL_PROCESSO: "P-100",
            COL_NF: "NF001",
            COL_STATUS: "FATURADO",
            COL_VALOR_REALIZADO: 10000.0,
            COL_VALOR_ORCADO: 12000.0,
            COL_LINHA: "Hidrologia",
            COL_GRUPO: "Equipamentos",
            COL_SUBGRUPO: "Bombas",
            COL_TIPO_MERCADORIA: "Produto",
            COL_FABRICANTE: "QED",
            COL_CONSULTOR_INTERNO: "DENER.MARTINS",
            COL_REPRESENTANTE: "",
            COL_GERENTE_COMERCIAL: "",
            COL_CLIENTE: "CLI001",
            COL_NOME_CLIENTE: "Client Test",
            COL_OPERACAO: "PVEN",
        },
    ])

    res = execute(df_ac)
    _assert(res.ok, "execute ok (no errors)")

    df_atrib = res.to_dataframe()
    _assert(len(df_atrib) >= 2, f"at least 2 atribuições (got {len(df_atrib)})")

    nomes = set(df_atrib["nome"].tolist())
    _assert("Dener Martins" in nomes, "Dener (CI-Operacional) atribuído")
    _assert("Paulo Negrão" in nomes, "Paulo (Diretor-Gestão) atribuído")

    dener_row = df_atrib[df_atrib["nome"] == "Dener Martins"].iloc[0]
    _assert(dener_row["fatia_cargo_pct"] == 25.0, "Dener fatia = 25%")
    _assert(dener_row["taxa_rateio_pct"] == 8.0, "Dener taxa = 8%")

    paulo_row = df_atrib[df_atrib["nome"] == "Paulo Negrão"].iloc[0]
    _assert(paulo_row["fatia_cargo_pct"] == 10.0, "Paulo fatia = 10%")
    _assert(paulo_row["tipo_cargo"] == "Gestão", "Paulo tipo = Gestão")

    _assert(len(res.cross_selling_cases) == 0, "no cross-selling (gerente vazio)")

    # ── Test CI sem vínculo → warning, não atribuído ──
    print("\n── CI sem vínculo na hierarquia → não comissionado + warning ──")

    # Adicionar item com CI que NÃO está vinculado à hierarquia Hidrologia
    _sl._CACHE["aliases.json"] = {"colaborador": {"DENER.MARTINS": "Dener Martins", "JOAO.SILVA": "João Silva"}}

    df_ac_sem_vinculo = pd.DataFrame([
        {
            COL_PROCESSO: "P-200",
            COL_NF: "NF002",
            COL_STATUS: "FATURADO",
            COL_VALOR_REALIZADO: 5000.0,
            COL_VALOR_ORCADO: 6000.0,
            COL_LINHA: "Hidrologia",
            COL_GRUPO: "Equipamentos",
            COL_SUBGRUPO: "Bombas",
            COL_TIPO_MERCADORIA: "Produto",
            COL_FABRICANTE: "QED",
            COL_CONSULTOR_INTERNO: "JOAO.SILVA",  # João não tem vínculo com Hidrologia
            COL_REPRESENTANTE: "",
            COL_GERENTE_COMERCIAL: "",
            COL_CLIENTE: "CLI002",
            COL_NOME_CLIENTE: "Client 2",
            COL_OPERACAO: "PVEN",
        },
    ])

    res_sv = execute(df_ac_sem_vinculo)
    df_sv = res_sv.to_dataframe()

    nomes_sv = set(df_sv["nome"].tolist()) if not df_sv.empty else set()
    _assert("João Silva" not in nomes_sv, "João (CI sem vínculo) NÃO atribuído")
    _assert("Paulo Negrão" in nomes_sv, "Paulo (Diretor com vínculo) ainda atribuído")
    _assert(
        any("João Silva" in w or "Joao Silva" in w or "JOAO.SILVA" in w.upper() for w in res_sv.warnings),
        "warning emitido para CI sem vínculo"
    )

    # Limpar cache de teste
    _sl.clear_cache()

    # ── Resultado final ──
    print(f"\n{'='*60}")
    print(f"  RESULTADO: {passed}/{total} testes passaram")
    if failed:
        print(f"  ✗ {failed} teste(s) falharam")
    print(f"{'='*60}")

    sys.exit(0 if failed == 0 else 1)


def _write_json(directory: str, filename: str, data: Any):
    """Escreve JSON em diretório (helper para testes)."""
    path = os.path.join(directory, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    import sys
    if "--test" in sys.argv:
        _run_tests()
    else:
        print("Usage: python scripts/atribuicao.py --test")
        print("  Para uso na skill: import scripts.atribuicao as atrib")
