"""
=============================================================================
SKILL: Robô de Comissões — Script 03b: Cálculo de Realizados e Métricas
=============================================================================
Módulo   : 03b_realizados
Versão   : 1.0.0
Autor    : Claude Commission Skill

Descrição
---------
Calcula os **valores realizados** de cada componente de meta para cada
colaborador/linha, que serão usados pelo fc_calculator para calcular
o Fator de Correção (FC).

Componentes calculados:
1. Faturamento por Linha     — Σ(Valor Realizado) status=FATURADO por Negócio
2. Faturamento Individual    — Σ(Valor Realizado) por colaborador
3. Conversão por Linha       — Σ(Valor Orçado) filtrado por Data Aceite no mês
4. Conversão Individual      — Σ(Valor Orçado) por colaborador (Data Aceite)
5. Rentabilidade             — cruzamento rentabilidade.xlsx × meta_rentabilidade.json
6. Retenção de Clientes      — janelas 24 meses (clientes únicos atual/anterior)
7. Metas de Fornecedores     — faturamento YTD × taxa câmbio

Dependências
------------
- pandas
- references/*.json (via _load_json)
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

COL_PROCESSO = "Processo"
COL_STATUS = "Status Processo"
COL_NF = "Numero NF"
COL_DT_EMISSAO = "Dt Emissão"
COL_VALOR_REALIZADO = "Valor Realizado"
COL_VALOR_ORCADO = "Valor Orçado"
COL_DATA_ACEITE = "Data Aceite"
COL_CONSULTOR_INTERNO = "Consultor Interno"
COL_REPRESENTANTE = "Representante-pedido"
COL_LINHA = "Linha"
COL_GRUPO = "Grupo"
COL_SUBGRUPO = "Subgrupo"
COL_TIPO_MERCADORIA = "Tipo de Mercadoria"
COL_FABRICANTE = "Fabricante"
COL_APLICACAO = "Aplicação Mat./Serv."
COL_CLIENTE = "Cliente"
COL_NOME_CLIENTE = "Nome Cliente"

STATUS_FATURADO = "FATURADO"

# Hierarquia de 6 níveis (do mais genérico ao mais específico)
HIERARCHY_COLS = [COL_LINHA, COL_GRUPO, COL_SUBGRUPO, COL_TIPO_MERCADORIA, COL_FABRICANTE, COL_APLICACAO]


# ═══════════════════════════════════════════════════════════════════════════════
# PATH RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════════

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
    """Remove acentos e converte para minúsculo."""
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    s = str(text).strip()
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def _safe_float(val: Any, default: float = 0.0) -> float:
    """Converte para float de forma segura."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _mask_comissionavel(df: pd.DataFrame) -> pd.Series:
    """Máscara booleana para linhas comissionáveis: Dt Emissão preenchida.

    Substitui o antigo filtro por Status Processo = FATURADO. O loader
    (``scripts.loaders.load_analise_comercial``) já restringe a AC a
    ``Dt Emissão`` ∈ (mês, ano) selecionados, então a existência de data
    não-nula aqui basta para caracterizar uma venda do período.
    """
    if COL_DT_EMISSAO in df.columns:
        dt = pd.to_datetime(df[COL_DT_EMISSAO], errors="coerce", dayfirst=True)
        return dt.notna()
    return pd.Series(False, index=df.index)


def _build_hierarchy_key(*parts: str) -> str:
    """Constrói chave hierárquica a partir dos componentes não-vazios.

    Apenas inclui partes consecutivas a partir do início.
    Ex: ("RH", "Equip", "", "REVENDA") → "RH/Equip" (para em parte vazia)

    Para chaves de meta onde intermediários podem ser nulos, todas as partes
    são incluídas: usar _build_meta_key() em vez desta.
    """
    result_parts = []
    for p in parts:
        s = str(p).strip() if p is not None else ""
        if not s or s.lower() in ("nan", "none", "null"):
            break
        result_parts.append(s)
    return "/".join(result_parts) if result_parts else ""


def _build_meta_key(entry: dict, fields: tuple = ("linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante", "aplicacao")) -> str:
    """Constrói chave de meta a partir de um dict, contando campos preenchidos.

    Retorna a chave composta com TODOS os campos preenchidos (não-nulos).
    Ex: {"linha": "RH", "grupo": None, "subgrupo": None, ...} → "RH"
    Ex: {"linha": "RH", "grupo": "Equip", "subgrupo": "Bombas", ...} → "RH/Equip/Bombas"
    """
    parts = []
    for f in fields:
        val = entry.get(f)
        s = str(val).strip() if val is not None else ""
        if not s or s.lower() in ("nan", "none", "null"):
            break
        parts.append(s)
    return "/".join(parts) if parts else ""


def _count_filled_fields(key: str) -> int:
    """Conta o nível de especificidade de uma chave hierárquica."""
    return len(key.split("/")) if key else 0


def _aggregate_by_all_hierarchy_levels(
    df: pd.DataFrame,
    value_col: str,
    hierarchy_cols: list = None,
) -> Dict[str, float]:
    """Agrega valor por TODOS os níveis de hierarquia (do mais específico ao mais genérico).

    Para cada combinação única de hierarquia no DataFrame, gera chaves
    em todos os níveis de agregação:
      nivel 1: "RH" (soma tudo de RH)
      nivel 2: "RH/Equipamentos" (soma tudo de RH/Equip)
      ...
      nivel 6: "RH/Equipamentos/Bombas/REVENDA/YSI/Monitoramento"

    Returns:
        Dict com todas as chaves hierárquicas e seus valores agregados.
    """
    if hierarchy_cols is None:
        hierarchy_cols = HIERARCHY_COLS

    result: Dict[str, float] = {}
    if df is None or df.empty:
        return result

    # Garantir que as colunas existam
    available_cols = [c for c in hierarchy_cols if c in df.columns]
    if not available_cols:
        return result

    # Preparar: limpar NaN para string vazia
    df_work = df[available_cols + [value_col]].copy()
    for col in available_cols:
        df_work[col] = df_work[col].fillna("").astype(str).str.strip()

    # Agregar para cada nível (de 1 campo até N campos)
    for level in range(1, len(available_cols) + 1):
        group_cols = available_cols[:level]
        grouped = df_work.groupby(group_cols, dropna=False)[value_col].sum()

        for idx, val in grouped.items():
            if not isinstance(idx, tuple):
                idx = (idx,)
            # Construir chave apenas se todos os campos consecutivos estão preenchidos
            parts = []
            valid = True
            for p in idx:
                s = str(p).strip()
                if not s or s.lower() in ("nan", "none", "null"):
                    valid = False
                    break
                parts.append(s)
            if valid and parts:
                key = "/".join(parts)
                result[key] = float(val)

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# RESULT DATACLASS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class RealizadosResult:
    """Resultado completo dos valores realizados."""

    # Realizados por dimensão → {chave_hierarquica: valor_realizado}
    faturamento_hierarquia: Dict[str, float] = field(default_factory=dict)
    faturamento_individual: Dict[str, float] = field(default_factory=dict)
    conversao_hierarquia: Dict[str, float] = field(default_factory=dict)
    conversao_individual: Dict[str, float] = field(default_factory=dict)
    rentabilidade: Dict[str, float] = field(default_factory=dict)
    retencao_clientes: Dict[str, float] = field(default_factory=dict)
    fornecedor_ytd: Dict[str, float] = field(default_factory=dict)

    # Metas carregadas (para referência)
    metas_faturamento_hierarquia: Dict[str, float] = field(default_factory=dict)
    metas_faturamento_individual: Dict[str, float] = field(default_factory=dict)
    metas_conversao_hierarquia: Dict[str, float] = field(default_factory=dict)
    metas_conversao_individual: Dict[str, float] = field(default_factory=dict)
    metas_rentabilidade: Dict[str, float] = field(default_factory=dict)
    metas_fornecedor: Dict[str, Dict] = field(default_factory=dict)

    # Aliases para retrocompatibilidade (usados por componentes não migrados)
    @property
    def faturamento_linha(self) -> Dict[str, float]:
        """Retrocompat: filtra faturamento_hierarquia para chaves de 1 nível (linha)."""
        return {k: v for k, v in self.faturamento_hierarquia.items() if "/" not in k}

    @property
    def metas_faturamento_linha(self) -> Dict[str, float]:
        """Retrocompat: filtra metas_faturamento_hierarquia para chaves de 1 nível."""
        return {k: v for k, v in self.metas_faturamento_hierarquia.items() if "/" not in k}

    @property
    def conversao_linha(self) -> Dict[str, float]:
        """Retrocompat: filtra conversao_hierarquia para chaves de 1 nível."""
        return {k: v for k, v in self.conversao_hierarquia.items() if "/" not in k}

    @property
    def metas_conversao_linha(self) -> Dict[str, float]:
        """Retrocompat: filtra metas_conversao_hierarquia para chaves de 1 nível."""
        return {k: v for k, v in self.metas_conversao_hierarquia.items() if "/" not in k}

    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0

    def get_atingimento(
        self, componente: str, chave: str, cap: float = 1.0
    ) -> float:
        """Calcula atingimento = realizado / meta, limitado pelo cap.

        Args:
            componente: nome do componente (ex: "faturamento_linha")
            chave: chave de lookup (ex: nome da linha)
            cap: limite superior (default 1.0 = cap_atingimento_max)

        Returns:
            Atingimento entre 0.0 e cap
        """
        realizado_map = getattr(self, componente, {})
        meta_map_name = f"metas_{componente}"
        meta_map = getattr(self, meta_map_name, {})

        realizado = realizado_map.get(chave, 0.0)
        meta = meta_map.get(chave, 0.0)

        if meta <= 0:
            return 1.0 if realizado > 0 else 0.0
        atingimento = realizado / meta
        return min(atingimento, cap)

    def summary(self) -> str:
        """Resumo textual dos realizados."""
        lines = [
            f"{'='*60}",
            f"  REALIZADOS — Resumo de Métricas",
            f"{'='*60}",
        ]

        def _section(title, real_dict, meta_dict):
            lines.append(f"\n  📊 {title}:")
            if not real_dict:
                lines.append("    (nenhum dado)")
                return
            for key in sorted(real_dict.keys()):
                r = real_dict[key]
                m = meta_dict.get(key, 0)
                ating = f"{r/m*100:.1f}%" if m > 0 else "N/A"
                lines.append(f"    {key:<30} R$ {r:>12,.2f}  meta: {m:>12,.2f}  ating: {ating}")

        _section("Faturamento por Hierarquia", self.faturamento_hierarquia, self.metas_faturamento_hierarquia)
        _section("Faturamento Individual", self.faturamento_individual, self.metas_faturamento_individual)
        _section("Conversão por Hierarquia", self.conversao_hierarquia, self.metas_conversao_hierarquia)
        _section("Conversão Individual", self.conversao_individual, self.metas_conversao_individual)

        if self.rentabilidade:
            lines.append(f"\n  📊 Rentabilidade:")
            for key in sorted(self.rentabilidade.keys()):
                r = self.rentabilidade[key]
                m = self.metas_rentabilidade.get(key, 0)
                ating = f"{r/m*100:.1f}%" if m > 0 else "N/A"
                lines.append(f"    {key:<30} {r:>8.2f}%  meta: {m:>8.2f}%  ating: {ating}")

        if self.retencao_clientes:
            lines.append(f"\n  📊 Retenção de Clientes:")
            for key, val in sorted(self.retencao_clientes.items()):
                lines.append(f"    {key:<30} {val:.2%}")

        if self.fornecedor_ytd:
            lines.append(f"\n  📊 Fornecedores (YTD em moeda estrangeira):")
            for key, val in sorted(self.fornecedor_ytd.items()):
                meta_info = self.metas_fornecedor.get(key, {})
                moeda = meta_info.get("moeda", "?")
                meta_ytd = meta_info.get("meta_ytd", 0)
                ating = f"{val/meta_ytd*100:.1f}%" if meta_ytd > 0 else "N/A"
                lines.append(f"    {key:<30} {moeda} {val:>12,.2f}  meta_ytd: {meta_ytd:>12,.2f}  ating: {ating}")

        if self.warnings:
            lines.append(f"\n{'─'*60}")
            lines.append(f"  ⚠ Avisos ({len(self.warnings)}):")
            for w in self.warnings[:15]:
                lines.append(f"    • {w}")
        if self.errors:
            lines.append(f"  ✖ Erros ({len(self.errors)}):")
            for e in self.errors[:10]:
                lines.append(f"    • {e}")

        lines.append(f"{'='*60}")
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. FATURAMENTO POR HIERARQUIA
# ═══════════════════════════════════════════════════════════════════════════════

def _calc_faturamento_hierarquia(
    df_ac: pd.DataFrame,
) -> Dict[str, float]:
    """Σ(Valor Realizado) agrupado por TODOS os níveis de hierarquia.

    Considera toda linha com ``Dt Emissão`` preenchida no período
    selecionado (o loader já restringe a AC ao mês/ano).

    Gera chaves em todos os níveis de agregação:
      "RH" → soma total da linha
      "RH/Equipamentos" → soma do grupo dentro da linha
      "RH/Equipamentos/Bombas/REVENDA/YSI/Monitoramento" → nível mais granular

    Returns:
        {"RH": 1600000, "RH/Equipamentos": 900000, ...}
    """
    faturados = df_ac[_mask_comissionavel(df_ac)]
    if faturados.empty:
        return {}
    return _aggregate_by_all_hierarchy_levels(faturados, COL_VALOR_REALIZADO)


# ═══════════════════════════════════════════════════════════════════════════════
# 2. FATURAMENTO INDIVIDUAL
# ═══════════════════════════════════════════════════════════════════════════════

def _load_alias_map() -> Dict[str, str]:
    """Carrega mapa de aliases: nome_normalizado → nome_canônico."""
    try:
        aliases_raw = _load_json("aliases.json")
    except Exception:
        return {}
    return {
        _normalize(k): v
        for k, v in aliases_raw.get("colaborador", {}).items()
    }


def _resolve_name(name: str, alias_map: Dict[str, str]) -> str:
    """Resolve um nome via alias map, retornando o canônico ou o original."""
    s = str(name).strip()
    return alias_map.get(_normalize(s), s)


_SKIP_NAMES = {"", "nan", "none", "nenhum", "null", "-"}


def _calc_faturamento_individual(
    df_ac: pd.DataFrame,
) -> Dict[str, float]:
    """Σ(Valor Realizado) por colaborador.

    Considera toda linha com ``Dt Emissão`` preenchida no período
    selecionado. Extrai colaboradores das colunas Consultor Interno e
    Representante-pedido da AC, resolve aliases para nomes canônicos, e
    soma o valor total. Mede o desempenho pessoal de vendas
    (independente de regras de comissão).

    Returns:
        {"Dener Martins": 86990.24, "Paulo Negrão": 150000.0, ...}
    """
    if df_ac.empty:
        return {}
    faturados = df_ac[_mask_comissionavel(df_ac)]
    if faturados.empty:
        return {}

    alias_map = _load_alias_map()
    result: Dict[str, float] = {}

    for col in [COL_CONSULTOR_INTERNO, COL_REPRESENTANTE]:
        if col not in faturados.columns:
            continue
        sub = faturados[[col, COL_VALOR_REALIZADO]].copy()
        sub.columns = ["_nome_raw", "_valor"]
        sub["_valor"] = pd.to_numeric(sub["_valor"], errors="coerce").fillna(0)
        sub = sub[sub["_valor"] > 0]
        sub["_nome_raw"] = sub["_nome_raw"].fillna("").astype(str)
        # Semicolon-separated names
        sub = sub.assign(_nome_raw=sub["_nome_raw"].str.split(";")).explode("_nome_raw")
        sub["_nome_raw"] = sub["_nome_raw"].str.strip()
        sub = sub[~sub["_nome_raw"].apply(_normalize).isin(_SKIP_NAMES)]
        if sub.empty:
            continue
        sub["_nome"] = sub["_nome_raw"].apply(lambda n: _resolve_name(n, alias_map))
        grouped = sub.groupby("_nome")["_valor"].sum()
        for nome, val in grouped.items():
            result[nome] = result.get(nome, 0) + val

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 3. CONVERSÃO POR HIERARQUIA
# ═══════════════════════════════════════════════════════════════════════════════

def _calc_conversao_hierarquia(
    df_ac: pd.DataFrame, mes: int, ano: int,
) -> Dict[str, float]:
    """Σ(Valor Orçado) agrupado por TODOS os níveis de hierarquia, filtrado por Data Aceite no mês.

    Conversão = vendas confirmadas (Data Aceite) no mês, usando Valor Orçado.
    Gera chaves em todos os níveis de agregação (mesmo padrão de faturamento_hierarquia).
    """
    if COL_DATA_ACEITE not in df_ac.columns or COL_VALOR_ORCADO not in df_ac.columns:
        return {}

    df = df_ac.copy()
    df[COL_DATA_ACEITE] = pd.to_datetime(df[COL_DATA_ACEITE], errors="coerce", dayfirst=True)
    mask = (
        (df[COL_DATA_ACEITE].dt.month == mes)
        & (df[COL_DATA_ACEITE].dt.year == ano)
    )
    convertidos = df[mask]
    if convertidos.empty:
        return {}
    return _aggregate_by_all_hierarchy_levels(convertidos, COL_VALOR_ORCADO)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. CONVERSÃO INDIVIDUAL
# ═══════════════════════════════════════════════════════════════════════════════

def _calc_conversao_individual(
    df_ac: pd.DataFrame, mes: int, ano: int,
) -> Dict[str, float]:
    """Σ(Valor Orçado) por colaborador operacional, filtrado por Data Aceite.

    Extrai colaboradores de Consultor Interno e Representante-pedido.
    """
    if COL_DATA_ACEITE not in df_ac.columns or COL_VALOR_ORCADO not in df_ac.columns:
        return {}

    df = df_ac.copy()
    df[COL_DATA_ACEITE] = pd.to_datetime(df[COL_DATA_ACEITE], errors="coerce", dayfirst=True)
    mask = (
        (df[COL_DATA_ACEITE].dt.month == mes)
        & (df[COL_DATA_ACEITE].dt.year == ano)
    )
    convertidos = df[mask]
    if convertidos.empty:
        return {}

    alias_map = _load_alias_map()
    result: Dict[str, float] = {}

    for col in [COL_CONSULTOR_INTERNO, COL_REPRESENTANTE]:
        if col not in convertidos.columns:
            continue
        sub = convertidos[[col, COL_VALOR_ORCADO]].copy()
        sub.columns = ["_nome_raw", "_valor"]
        sub["_valor"] = pd.to_numeric(sub["_valor"], errors="coerce").fillna(0)
        sub = sub[sub["_valor"] > 0]
        sub["_nome_raw"] = sub["_nome_raw"].fillna("").astype(str)
        sub = sub.assign(_nome_raw=sub["_nome_raw"].str.split(";")).explode("_nome_raw")
        sub["_nome_raw"] = sub["_nome_raw"].str.strip()
        sub = sub[~sub["_nome_raw"].apply(_normalize).isin(_SKIP_NAMES)]
        if sub.empty:
            continue
        sub["_nome"] = sub["_nome_raw"].apply(lambda n: _resolve_name(n, alias_map))
        grouped = sub.groupby("_nome")["_valor"].sum()
        for nome, val in grouped.items():
            result[nome] = result.get(nome, 0) + val

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 5. RENTABILIDADE
# ═══════════════════════════════════════════════════════════════════════════════

COL_CODIGO_PRODUTO = "Código Produto"


def _aggregate_weighted_avg_by_hierarchy(
    df: pd.DataFrame,
    value_col: str,
    weight_col: str,
    hierarchy_cols: list = None,
) -> Dict[str, float]:
    """Média ponderada por TODOS os níveis de hierarquia (1 a N).

    Para cada nível de agregação, calcula:
        média = Σ(value × weight) / Σ(weight)

    Segue o mesmo padrão de ``_aggregate_by_all_hierarchy_levels``, mas
    produz uma média ponderada em vez de soma.

    Args:
        df: DataFrame com colunas de hierarquia, *value_col* e *weight_col*.
        value_col: coluna com o valor a ponderar (ex: rentabilidade %).
        weight_col: coluna com o peso (ex: Valor Realizado R$).
        hierarchy_cols: lista ordenada de colunas de hierarquia (padrão: HIERARCHY_COLS).

    Returns:
        Dict com chaves hierárquicas e médias ponderadas (como fração, ex: 0.3084).
    """
    if hierarchy_cols is None:
        hierarchy_cols = HIERARCHY_COLS

    result: Dict[str, float] = {}
    if df is None or df.empty:
        return result

    available_cols = [c for c in hierarchy_cols if c in df.columns]
    if not available_cols:
        return result

    df_work = df[available_cols + [value_col, weight_col]].copy()
    for col in available_cols:
        df_work[col] = df_work[col].fillna("").astype(str).str.strip()

    df_work["_vxw"] = df_work[value_col] * df_work[weight_col]

    for level in range(1, len(available_cols) + 1):
        group_cols = available_cols[:level]
        grouped = df_work.groupby(group_cols, dropna=False).agg(
            _sum_vxw=("_vxw", "sum"),
            _sum_w=(weight_col, "sum"),
        )

        for idx, row in grouped.iterrows():
            if not isinstance(idx, tuple):
                idx = (idx,)
            parts = []
            valid = True
            for p in idx:
                s = str(p).strip()
                if not s or s.lower() in ("nan", "none", "null"):
                    valid = False
                    break
                parts.append(s)
            if valid and parts and row["_sum_w"] != 0:
                key = "/".join(parts)
                avg = row["_sum_vxw"] / row["_sum_w"]
                result[key] = float(avg)

    return result


def _calc_rentabilidade(
    df_ac: pd.DataFrame,
    df_fat_rent_gpe: Optional[pd.DataFrame],
    mes: int,
    ano: int,
) -> Tuple[Dict[str, float], Dict[str, float]]:
    """Calcula rentabilidade realizada e carrega metas.

    Cruza a AC enriquecida (FATURADOS do mês) com a tabela de rentabilidade
    por produto (fat_rent_gpe) via ``Código Produto`` = ``codigo_produto``.
    Em seguida agrega a média ponderada de rentabilidade em todos os 6 níveis
    hierárquicos, usando ``Valor Realizado`` como peso.

    Fórmula por nível K:
        rent_realizada(K) = Σ(rent_pct_i × Valor_Realizado_i) / Σ(Valor_Realizado_i)

    Metas vêm de ``meta_rentabilidade.json`` filtrado pelo período.

    Args:
        df_ac: AC enriquecida (pós-JOIN com Classificação de Produtos).
               Já com coluna ``Negócio`` renomeada e filtrada pelo mês.
        df_fat_rent_gpe: DataFrame do parse_fat_rent_gpe com colunas
                         ``codigo_produto``, ``venda_liq``, ``rentabilidade``.
                         Pode ser None se o arquivo não foi fornecido.
        mes: mês de apuração.
        ano: ano de apuração.

    Returns:
        (realizados_dict, metas_dict) — chave: hierarquia (ex: "RH/Equip")
        Valores de rentabilidade como fração (ex: 0.3084 = 30,84%).
    """
    realizados: Dict[str, float] = {}
    metas: Dict[str, float] = {}

    # ── Realizados: AC × fat_rent_gpe ──
    if (df_fat_rent_gpe is not None and not df_fat_rent_gpe.empty
            and df_ac is not None and not df_ac.empty):

        # Filtrar itens comissionáveis (Dt Emissão preenchida no período)
        faturados = df_ac[_mask_comissionavel(df_ac)].copy()

        if not faturados.empty and COL_CODIGO_PRODUTO in faturados.columns:
            # Preparar lookup: codigo_produto → rentabilidade (%)
            df_rent = df_fat_rent_gpe[["codigo_produto", "rentabilidade"]].copy()
            df_rent["codigo_produto"] = df_rent["codigo_produto"].astype(str).str.strip()
            df_rent = df_rent.drop_duplicates(subset="codigo_produto")

            faturados[COL_CODIGO_PRODUTO] = (
                faturados[COL_CODIGO_PRODUTO].astype(str).str.strip()
            )

            # JOIN: AC × fat_rent_gpe em Código Produto
            df_merged = faturados.merge(
                df_rent,
                how="inner",
                left_on=COL_CODIGO_PRODUTO,
                right_on="codigo_produto",
            )

            if not df_merged.empty:
                # Converter % para fração (ex: 30.84 → 0.3084)
                df_merged["_rent_frac"] = df_merged["rentabilidade"] / 100.0

                realizados = _aggregate_weighted_avg_by_hierarchy(
                    df_merged,
                    value_col="_rent_frac",
                    weight_col=COL_VALOR_REALIZADO,
                )

    # ── Metas do JSON ──
    try:
        meta_rent = _load_json("meta_rentabilidade.json")
        if isinstance(meta_rent, dict):
            chave = f"{ano}-{mes:02d}"
            entries = meta_rent.get(chave, [])
        else:
            entries = meta_rent

        for entry in entries:
            if isinstance(entry, str):
                continue
            periodo_ini = entry.get("periodo_inicio", "")
            periodo_fim = entry.get("periodo_fim", "")
            if periodo_ini and periodo_fim:
                try:
                    ini = pd.to_datetime(periodo_ini, dayfirst=True)
                    fim = pd.to_datetime(periodo_fim, dayfirst=True)
                    ref = pd.Timestamp(year=ano, month=mes, day=15)
                    if not (ini <= ref <= fim):
                        continue
                except Exception:
                    pass

            key = _build_meta_key(entry, ("linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante", "aplicacao"))
            if not key:
                continue
            metas[key] = _safe_float(entry.get("meta_rentabilidade_pct", entry.get("meta_rentabilidade_alvo_pct", 0))) / 100.0
    except FileNotFoundError:
        pass

    return realizados, metas


# ═══════════════════════════════════════════════════════════════════════════════
# 6. RETENÇÃO DE CLIENTES
# ═══════════════════════════════════════════════════════════════════════════════

def _calc_retencao_clientes(
    df_ac_full: pd.DataFrame,
    mes: int,
    ano: int,
) -> Dict[str, float]:
    """Calcula taxa de retenção de clientes por Linha de Negócio.

    Compara clientes únicos em duas janelas móveis de 24 meses:
    - Janela Atual: [mes/ano - 24 meses, mes/ano]
    - Janela Anterior: [mes/ano - 25 meses, mes/ano - 1 mês]

    Apenas processos FATURADOS são considerados.
    Retenção = clientes_janela_atual / clientes_janela_anterior.

    Args:
        df_ac_full: AC completa (NÃO filtrada por mês — precisa do histórico)
        mes: mês de apuração
        ano: ano de apuração

    Returns:
        {"Hidrologia": 0.95, "Ambiental": 1.02, ...}
    """
    if df_ac_full is None or df_ac_full.empty:
        return {}
    if COL_DT_EMISSAO not in df_ac_full.columns:
        return {}

    df = df_ac_full.copy()
    df[COL_DT_EMISSAO] = pd.to_datetime(df[COL_DT_EMISSAO], errors="coerce", dayfirst=True)

    # Apenas linhas com Dt Emissão preenchida
    df = df.dropna(subset=[COL_DT_EMISSAO])

    if df.empty:
        return {}

    # Definir janelas
    ref_date = pd.Timestamp(year=ano, month=mes, day=1)
    # Último dia do mês de referência
    ref_end = ref_date + pd.offsets.MonthEnd(0)

    # Janela Atual: 24 meses retroativos terminando no mês atual
    atual_end = ref_end
    atual_start = ref_date - pd.DateOffset(months=24)

    # Janela Anterior: mesmos 24 meses, mas deslocada 1 mês antes
    anterior_end = ref_date - pd.DateOffset(months=1) + pd.offsets.MonthEnd(0)
    anterior_start = ref_date - pd.DateOffset(months=25)

    # Coluna de cliente (usar Cliente ou Nome Cliente)
    col_cli = COL_CLIENTE if COL_CLIENTE in df.columns else COL_NOME_CLIENTE
    if col_cli not in df.columns:
        return {}

    # Filtrar janelas
    mask_atual = (df[COL_DT_EMISSAO] >= atual_start) & (df[COL_DT_EMISSAO] <= atual_end)
    mask_anterior = (df[COL_DT_EMISSAO] >= anterior_start) & (df[COL_DT_EMISSAO] <= anterior_end)

    df_atual = df[mask_atual]
    df_anterior = df[mask_anterior]

    # Agrupar por Negócio
    result: Dict[str, float] = {}
    linhas = set()
    if COL_LINHA in df.columns:
        linhas = set(df[COL_LINHA].dropna().unique())

    for linha in linhas:
        cli_atual = set(
            df_atual[df_atual[COL_LINHA] == linha][col_cli].dropna().unique()
        )
        cli_anterior = set(
            df_anterior[df_anterior[COL_LINHA] == linha][col_cli].dropna().unique()
        )
        n_atual = len(cli_atual)
        n_anterior = len(cli_anterior)

        if n_anterior == 0:
            result[str(linha)] = 1.0 if n_atual > 0 else 0.0
        else:
            result[str(linha)] = n_atual / n_anterior

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 7. METAS DE FORNECEDORES (YTD com câmbio)
# ═══════════════════════════════════════════════════════════════════════════════

def _calc_fornecedores_ytd(
    df_ac: pd.DataFrame,
    mes: int,
    ano: int,
) -> Tuple[Dict[str, float], Dict[str, Dict]]:
    """Calcula faturamento YTD por fornecedor convertido em moeda estrangeira.

    Meta: METAS_FORNECEDORES define meta_anual por fornecedor+linha
          meta_ytd = meta_anual / 12 × mês_apuração

    Realizado: Soma faturamento FATURADO YTD por Fabricante,
              filtrado por linha, convertido pela taxa de câmbio mensal.

    Returns:
        (realizados_ytd, metas_info)
        realizados_ytd: {"Hidrologia/YSI": 25.5, ...} em moeda estrangeira
        metas_info: {"Hidrologia/YSI": {"moeda": "USD", "meta_ytd": 25.0}, ...}
    """
    realizados: Dict[str, float] = {}
    metas_info: Dict[str, Dict] = {}

    try:
        forn_metas = _load_json("metas_fornecedores.json")
    except FileNotFoundError:
        return realizados, metas_info

    # Carregar taxas de câmbio
    try:
        rates_data = _load_json("monthly_avg_rates.json")
        taxas = rates_data.get("taxas", {})
    except FileNotFoundError:
        taxas = {}

    for meta in forn_metas:
        linha = meta.get("linha", "")
        fornecedor = meta.get("fornecedor", "")
        moeda = meta.get("moeda", "USD")
        meta_anual = _safe_float(meta.get("meta_anual", 0))
        meta_ytd = meta_anual / 12.0 * mes
        key = f"{linha}/{fornecedor}"

        metas_info[key] = {
            "moeda": moeda,
            "meta_anual": meta_anual,
            "meta_ytd": meta_ytd,
            "linha": linha,
            "fornecedor": fornecedor,
        }

        # Calcular realizado YTD: somar faturamento por mês e converter
        if df_ac is None or df_ac.empty:
            realizados[key] = 0.0
            continue

        df = df_ac.copy()
        df[COL_DT_EMISSAO] = pd.to_datetime(df[COL_DT_EMISSAO], errors="coerce", dayfirst=True)

        # Filtros: Dt Emissão preenchida, ano correto, mês <= mes_apuração,
        # fabricante, linha
        mask = (
            df[COL_DT_EMISSAO].notna()
            & (df[COL_DT_EMISSAO].dt.year == ano)
            & (df[COL_DT_EMISSAO].dt.month <= mes)
        )

        if COL_FABRICANTE in df.columns:
            mask = mask & (df[COL_FABRICANTE].str.upper() == fornecedor.upper())
        if COL_LINHA in df.columns:
            mask = mask & (df[COL_LINHA].str.upper() == linha.upper())

        df_filt = df[mask]

        # Somar por mês e converter
        total_foreign = 0.0
        if not df_filt.empty:
            df_filt = df_filt.copy()
            df_filt["_mes"] = df_filt[COL_DT_EMISSAO].dt.month
            monthly_sums = df_filt.groupby("_mes")[COL_VALOR_REALIZADO].sum()

            ano_taxas = taxas.get(str(ano), {})
            moeda_taxas = ano_taxas.get(moeda, {})

            for m, val_brl in monthly_sums.items():
                taxa_info = moeda_taxas.get(str(int(m)), {})
                taxa_media = _safe_float(taxa_info.get("taxa_media", 0))
                if taxa_media > 0:
                    total_foreign += val_brl * taxa_media
                else:
                    # Sem taxa → avisar e usar valor BRL como fallback
                    total_foreign += val_brl

        realizados[key] = total_foreign

    return realizados, metas_info


# ═══════════════════════════════════════════════════════════════════════════════
# METAS LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def _load_metas() -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float], Dict[str, float]]:
    """Carrega metas de faturamento e conversão dos JSONs.

    Metas por Hierarquia: de metas_aplicacao.json (tipo_meta="faturamento"/"conversao")
        — preserva chave hierárquica completa (ex: "RH/Equipamentos/Bombas")
    Metas Individuais: de metas_individuais.json

    Returns:
        (metas_fat_hierarquia, metas_fat_individual, metas_conv_hierarquia, metas_conv_individual)
        Chaves: chave hierárquica ou nome do colaborador
    """
    metas_fat_hierarquia: Dict[str, float] = {}
    metas_conv_hierarquia: Dict[str, float] = {}
    metas_fat_individual: Dict[str, float] = {}
    metas_conv_individual: Dict[str, float] = {}

    try:
        metas_app = _load_json("metas_aplicacao.json")
        for entry in metas_app:
            key = _build_meta_key(entry, ("linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante", "aplicacao"))
            tipo = entry.get("tipo_meta", "")
            valor = _safe_float(entry.get("valor_meta", 0))
            if not key:
                continue
            if tipo == "faturamento":
                metas_fat_hierarquia[key] = metas_fat_hierarquia.get(key, 0) + valor
            elif tipo == "conversao":
                metas_conv_hierarquia[key] = metas_conv_hierarquia.get(key, 0) + valor
    except FileNotFoundError:
        pass

    try:
        metas_ind = _load_json("metas_individuais.json")
        for entry in metas_ind:
            colab = str(entry.get("colaborador", "")).strip()
            tipo = entry.get("tipo_meta", "")
            valor = _safe_float(entry.get("valor_meta", 0))
            if not colab:
                continue
            if tipo == "faturamento":
                metas_fat_individual[colab] = metas_fat_individual.get(colab, 0) + valor
            elif tipo == "conversao":
                metas_conv_individual[colab] = metas_conv_individual.get(colab, 0) + valor
    except FileNotFoundError:
        pass

    return metas_fat_hierarquia, metas_fat_individual, metas_conv_hierarquia, metas_conv_individual


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def execute(
    df_analise_comercial: pd.DataFrame,
    df_atribuicoes: pd.DataFrame,
    df_fat_rent_gpe: Optional[pd.DataFrame],
    mes: int,
    ano: int,
    df_ac_full: Optional[pd.DataFrame] = None,
) -> RealizadosResult:
    """Calcula todos os valores realizados para o mês de apuração.

    Args:
        df_analise_comercial: AC filtrada pelo mês (output do loader, enriquecida
                              com Classificação de Produtos)
        df_atribuicoes: DataFrame de atribuições (output do atribuicao.py)
        df_fat_rent_gpe: DataFrame do parse_fat_rent_gpe com colunas
                         [codigo_produto, venda_liq, rentabilidade].
                         Pode ser None se fat_rent_gpe.csv não foi fornecido.
        mes: mês de apuração
        ano: ano de apuração
        df_ac_full: AC completa sem filtro de mês (para retenção 24 meses
                    e conversão por Data Aceite).
                    Se None, retenção não será calculada e conversão usará AC filtrada.

    Returns:
        RealizadosResult
    """
    result = RealizadosResult()

    # 1. Faturamento por Hierarquia
    result.faturamento_hierarquia = _calc_faturamento_hierarquia(df_analise_comercial)

    # 2. Faturamento Individual (baseado na AC — desempenho pessoal de vendas)
    result.faturamento_individual = _calc_faturamento_individual(df_analise_comercial)

    # 3. Conversão por Hierarquia (usa AC full — filtra por Data Aceite internamente)
    df_conv = df_ac_full if (df_ac_full is not None and not df_ac_full.empty) else df_analise_comercial
    result.conversao_hierarquia = _calc_conversao_hierarquia(df_conv, mes, ano)

    # 4. Conversão Individual (usa AC full — filtra por Data Aceite internamente)
    result.conversao_individual = _calc_conversao_individual(df_conv, mes, ano)

    # 5. Rentabilidade (AC enriquecida × fat_rent_gpe, agregação multi-nível)
    rent_real, rent_meta = _calc_rentabilidade(
        df_analise_comercial, df_fat_rent_gpe, mes, ano,
    )
    result.rentabilidade = rent_real
    result.metas_rentabilidade = rent_meta

    # 6. Retenção de Clientes
    if df_ac_full is not None and not df_ac_full.empty:
        result.retencao_clientes = _calc_retencao_clientes(df_ac_full, mes, ano)
    else:
        result.warnings.append(
            "AC completa (histórico 24 meses) não fornecida — "
            "retenção de clientes não calculada."
        )

    # 7. Fornecedores YTD
    forn_real, forn_meta = _calc_fornecedores_ytd(df_analise_comercial, mes, ano)
    result.fornecedor_ytd = forn_real
    result.metas_fornecedor = forn_meta

    # Carregar metas
    mfh, mfi, mch, mci = _load_metas()
    result.metas_faturamento_hierarquia = mfh
    result.metas_faturamento_individual = mfi
    result.metas_conversao_hierarquia = mch
    result.metas_conversao_individual = mci

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

def _run_tests():
    """Testes embutidos — execute com: python scripts/realizados.py --test"""
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
    print("  REALIZADOS.PY — Testes Embutidos")
    print("=" * 60)

    # ── Helpers ──
    print("\n── Helpers ──")
    _assert(_normalize("André") == "andre", "normalize accent")
    _assert(_safe_float(None) == 0.0, "safe_float None")
    _assert(_safe_float("123.45") == 123.45, "safe_float string")
    _assert(_safe_float("abc") == 0.0, "safe_float invalid")

    # ── Faturamento por Hierarquia ──
    print("\n── Faturamento por Hierarquia ──")
    df_ac = pd.DataFrame([
        {COL_STATUS: "FATURADO", COL_LINHA: "Hidrologia", COL_GRUPO: "Equipamentos",
         COL_SUBGRUPO: "", COL_TIPO_MERCADORIA: "", COL_FABRICANTE: "", COL_APLICACAO: "",
         COL_VALOR_REALIZADO: 100},
        {COL_STATUS: "FATURADO", COL_LINHA: "Hidrologia", COL_GRUPO: "Equipamentos",
         COL_SUBGRUPO: "", COL_TIPO_MERCADORIA: "", COL_FABRICANTE: "", COL_APLICACAO: "",
         COL_VALOR_REALIZADO: 200},
        {COL_STATUS: "FATURADO", COL_LINHA: "Ambiental", COL_GRUPO: "Reagentes",
         COL_SUBGRUPO: "", COL_TIPO_MERCADORIA: "", COL_FABRICANTE: "", COL_APLICACAO: "",
         COL_VALOR_REALIZADO: 50},
        {COL_STATUS: "Em Andamento", COL_LINHA: "Hidrologia", COL_GRUPO: "Equipamentos",
         COL_SUBGRUPO: "", COL_TIPO_MERCADORIA: "", COL_FABRICANTE: "", COL_APLICACAO: "",
         COL_VALOR_REALIZADO: 999},
    ])
    fat_hier = _calc_faturamento_hierarquia(df_ac)
    _assert(fat_hier.get("Hidrologia") == 300, "fat Hidrologia = 300")
    _assert(fat_hier.get("Hidrologia/Equipamentos") == 300, "fat Hidrologia/Equip = 300")
    _assert(fat_hier.get("Ambiental") == 50, "fat Ambiental = 50")

    # ── Faturamento Individual ──
    print("\n── Faturamento Individual ──")
    _sl._CACHE["aliases.json"] = {"colaborador": {"DENER.MARTINS": "Dener Martins"}}
    df_ac_fi = pd.DataFrame([
        {COL_STATUS: "FATURADO", COL_CONSULTOR_INTERNO: "DENER.MARTINS", COL_REPRESENTANTE: "",
         COL_VALOR_REALIZADO: 100},
        {COL_STATUS: "FATURADO", COL_CONSULTOR_INTERNO: "DENER.MARTINS", COL_REPRESENTANTE: "",
         COL_VALOR_REALIZADO: 200},
        {COL_STATUS: "FATURADO", COL_CONSULTOR_INTERNO: "Paulo", COL_REPRESENTANTE: "",
         COL_VALOR_REALIZADO: 100},
        {COL_STATUS: "Em Andamento", COL_CONSULTOR_INTERNO: "DENER.MARTINS", COL_REPRESENTANTE: "",
         COL_VALOR_REALIZADO: 999},
    ])
    fat_ind = _calc_faturamento_individual(df_ac_fi)
    _assert(fat_ind.get("Dener Martins") == 300, "individual Dener = 300 (alias resolved)")
    _assert(fat_ind.get("Paulo") == 100, "individual Paulo = 100 (full valor, no split)")
    _sl._CACHE.pop("aliases.json", None)

    # ── Conversão por Hierarquia ──
    print("\n── Conversão por Hierarquia ──")
    df_conv = pd.DataFrame([
        {COL_LINHA: "Hidrologia", COL_GRUPO: "Equipamentos", COL_SUBGRUPO: "",
         COL_TIPO_MERCADORIA: "", COL_FABRICANTE: "", COL_APLICACAO: "",
         COL_VALOR_ORCADO: 500, COL_DATA_ACEITE: "15/10/2025"},
        {COL_LINHA: "Hidrologia", COL_GRUPO: "Equipamentos", COL_SUBGRUPO: "",
         COL_TIPO_MERCADORIA: "", COL_FABRICANTE: "", COL_APLICACAO: "",
         COL_VALOR_ORCADO: 300, COL_DATA_ACEITE: "20/10/2025"},
        {COL_LINHA: "Ambiental", COL_GRUPO: "Reagentes", COL_SUBGRUPO: "",
         COL_TIPO_MERCADORIA: "", COL_FABRICANTE: "", COL_APLICACAO: "",
         COL_VALOR_ORCADO: 100, COL_DATA_ACEITE: "05/11/2025"},
    ])
    conv_hier = _calc_conversao_hierarquia(df_conv, 10, 2025)
    _assert(conv_hier.get("Hidrologia") == 800, "conv Hidrologia = 800")
    _assert(conv_hier.get("Hidrologia/Equipamentos") == 800, "conv Hidrologia/Equip = 800")
    _assert("Ambiental" not in conv_hier, "Ambiental (nov) excluído de outubro")

    # ── Conversão Individual ──
    print("\n── Conversão Individual ──")
    _sl._CACHE["aliases.json"] = {"colaborador": {}}
    df_conv2 = pd.DataFrame([
        {COL_CONSULTOR_INTERNO: "Dener", COL_REPRESENTANTE: "", COL_VALOR_ORCADO: 500, COL_DATA_ACEITE: "15/10/2025"},
        {COL_CONSULTOR_INTERNO: "Dener; João", COL_REPRESENTANTE: "", COL_VALOR_ORCADO: 200, COL_DATA_ACEITE: "20/10/2025"},
    ])
    conv_ind = _calc_conversao_individual(df_conv2, 10, 2025)
    _assert(conv_ind.get("Dener") == 700, "conv ind Dener = 700")
    _assert(conv_ind.get("João") == 200, "conv ind João = 200 (via ;)")
    _sl._CACHE.pop("aliases.json", None)

    # ── Retenção de Clientes ──
    print("\n── Retenção de Clientes ──")
    # Criar AC com histórico de 26 meses
    import datetime
    rows = []
    for m_offset in range(26):
        dt = datetime.date(2025, 10, 15) - datetime.timedelta(days=30 * m_offset)
        rows.append({
            COL_STATUS: "FATURADO",
            COL_DT_EMISSAO: dt.strftime("%d/%m/%Y"),
            COL_LINHA: "Hidrologia",
            COL_CLIENTE: f"CLI{m_offset % 5}",
        })
    # Add um cliente extra apenas no período atual
    rows.append({
        COL_STATUS: "FATURADO",
        COL_DT_EMISSAO: "01/10/2025",
        COL_LINHA: "Hidrologia",
        COL_CLIENTE: "CLI_NOVO",
    })
    df_hist = pd.DataFrame(rows)
    ret = _calc_retencao_clientes(df_hist, 10, 2025)
    _assert("Hidrologia" in ret, "retenção Hidrologia calculada")
    _assert(ret["Hidrologia"] >= 1.0, f"retenção >= 1.0 (cliente novo) got {ret.get('Hidrologia', 0):.2f}")

    # empty case
    _assert(_calc_retencao_clientes(pd.DataFrame(), 10, 2025) == {}, "empty df → empty retencao")

    # ── Metas loading (com dados mock via supabase_loader cache) ──
    print("\n── Metas loading ──")
    _sl.clear_cache()
    _sl._CACHE["metas_aplicacao.json"] = [
        {"linha": "Hidrologia", "grupo": None, "subgrupo": None,
         "tipo_mercadoria": None, "fabricante": None, "aplicacao": None,
         "tipo_meta": "faturamento", "valor_meta": 5000},
        {"linha": "Hidrologia", "grupo": "Equipamentos", "subgrupo": None,
         "tipo_mercadoria": None, "fabricante": None, "aplicacao": None,
         "tipo_meta": "faturamento", "valor_meta": 3000},
        {"linha": "Ambiental", "grupo": None, "subgrupo": None,
         "tipo_mercadoria": None, "fabricante": None, "aplicacao": None,
         "tipo_meta": "conversao", "valor_meta": 2000},
    ]
    _sl._CACHE["metas_individuais.json"] = [
        {"colaborador": "Dener Martins", "cargo": "CI", "tipo_meta": "faturamento", "valor_meta": 1000},
        {"colaborador": "Dener Martins", "cargo": "CI", "tipo_meta": "conversao", "valor_meta": 800},
    ]
    _sl._CACHE["meta_rentabilidade.json"] = [
        {"linha": "Hidrologia", "grupo": "", "subgrupo": "", "tipo_mercadoria": "",
         "meta_rentabilidade_pct": 15.0, "periodo_inicio": "01/01/2025", "periodo_fim": "31/12/2025"},
    ]
    _sl._CACHE["metas_fornecedores.json"] = [
        {"linha": "Hidrologia", "fornecedor": "YSI", "moeda": "USD", "meta_anual": 30},
    ]
    _sl._CACHE["monthly_avg_rates.json"] = {
        "USD": {"2025": {"1": 0.17, "2": 0.17, "3": 0.17, "4": 0.17, "5": 0.17,
                         "6": 0.17, "7": 0.17, "8": 0.17, "9": 0.17, "10": 0.17}},
    }
    _sl._CACHE["params.json"] = {"cap_fc_max": 1.0}
    _sl._CACHE["aliases.json"] = {"colaborador": {}}

    mfh, mfi, mch, mci = _load_metas()
    _assert(mfh.get("Hidrologia") == 5000, f"meta fat Hidrologia = 5000 (got {mfh.get('Hidrologia')})")
    _assert(mfh.get("Hidrologia/Equipamentos") == 3000, f"meta fat Hidro/Equip = 3000 (got {mfh.get('Hidrologia/Equipamentos')})")
    _assert(mfi.get("Dener Martins") == 1000, "meta fat individual Dener = 1000")
    _assert(mch.get("Ambiental") == 2000, "meta conv Ambiental = 2000")
    _assert(mci.get("Dener Martins") == 800, "meta conv individual Dener = 800")

    # ── Fornecedores YTD ──
    print("\n── Fornecedores YTD ──")
    df_forn = pd.DataFrame([
        {COL_STATUS: "FATURADO", COL_DT_EMISSAO: "15/03/2025",
         COL_LINHA: "Hidrologia", COL_FABRICANTE: "YSI", COL_VALOR_REALIZADO: 10000},
        {COL_STATUS: "FATURADO", COL_DT_EMISSAO: "20/06/2025",
         COL_LINHA: "Hidrologia", COL_FABRICANTE: "YSI", COL_VALOR_REALIZADO: 20000},
        {COL_STATUS: "FATURADO", COL_DT_EMISSAO: "10/10/2025",
         COL_LINHA: "Hidrologia", COL_FABRICANTE: "YSI", COL_VALOR_REALIZADO: 5000},
    ])
    forn_real, forn_meta = _calc_fornecedores_ytd(df_forn, 10, 2025)
    key_ysi = "Hidrologia/YSI"
    _assert(key_ysi in forn_real, "YSI in realizados")
    expected_ytd = (10000 * 0.17) + (20000 * 0.17) + (5000 * 0.17)
    _assert(abs(forn_real[key_ysi] - expected_ytd) < 0.01,
            f"YSI YTD = {expected_ytd} (got {forn_real.get(key_ysi, 0):.2f})")
    _assert(forn_meta[key_ysi]["meta_ytd"] == 30 / 12 * 10,
            f"YSI meta_ytd = {30/12*10}")

    # ── Full execute ──
    print("\n── Full execute ──")
    df_ac_test = pd.DataFrame([
        {COL_STATUS: "FATURADO", COL_LINHA: "Hidrologia", COL_GRUPO: "Equipamentos",
         COL_SUBGRUPO: "", COL_TIPO_MERCADORIA: "", COL_FABRICANTE: "YSI", COL_APLICACAO: "",
         COL_VALOR_REALIZADO: 5000, COL_VALOR_ORCADO: 6000,
         COL_DT_EMISSAO: "15/10/2025", COL_DATA_ACEITE: "10/10/2025",
         COL_CONSULTOR_INTERNO: "Dener",
         COL_REPRESENTANTE: "", COL_CLIENTE: "C1"},
    ])
    df_atrib_test = pd.DataFrame([
        {COL_STATUS: "FATURADO", "nome": "Dener", COL_VALOR_REALIZADO: 5000},
    ])
    df_rent_test = pd.DataFrame([
        {"Linha": "Hidrologia", COL_GRUPO: "", COL_SUBGRUPO: "",
         COL_TIPO_MERCADORIA: "", COL_FABRICANTE: "", COL_APLICACAO: "",
         "rentabilidade_realizada_pct": 18.0},
    ])

    res = execute(df_ac_test, df_atrib_test, df_rent_test, 10, 2025)
    _assert(res.ok, "execute ok")
    _assert(res.faturamento_hierarquia.get("Hidrologia") == 5000, "fat hierarquia Hidro = 5000")
    _assert(res.faturamento_linha.get("Hidrologia") == 5000, "fat linha (retrocompat) Hidro = 5000")
    _assert(res.faturamento_hierarquia.get("Hidrologia/Equipamentos") == 5000, "fat hierarquia Hidro/Equip = 5000")
    _assert(res.conversao_hierarquia.get("Hidrologia") == 6000, "conv hierarquia Hidro = 6000")
    _assert(res.faturamento_individual.get("Dener") == 5000, "fat ind Dener = 5000")

    # Test get_atingimento — usando hierarquia
    ating = res.get_atingimento("faturamento_hierarquia", "Hidrologia", cap=1.0)
    expected_ating = min(5000 / 5000, 1.0)
    _assert(abs(ating - expected_ating) < 0.001,
            f"atingimento fat_hierarquia Hidro = {expected_ating:.4f} (got {ating:.4f})")

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
        print("Usage: python scripts/realizados.py --test")
        print("  Para uso na skill: import scripts.realizados as reais")