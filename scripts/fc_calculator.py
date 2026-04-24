"""
=============================================================================
SKILL: Robô de Comissões — Script 04: Fator de Correção (FC)
=============================================================================
Módulo   : 04_fc_calculator
Versão   : 1.0.0
Autor    : Claude Commission Skill

Descrição
---------
Calcula o **Fator de Correção (FC)** para cada combinação
(colaborador, cargo, linha_do_item).

O FC reflete o desempenho do colaborador em relação às suas metas e é
aplicado como multiplicador na fórmula da comissão.

Dois modos de cálculo:
1. **RAMPA** — FC contínuo (valor proporcional ao atingimento)
2. **ESCADA** — FC discretizado em degraus (piso → topo)

Dependências
------------
- references/pesos_metas.json
- references/fc_escada_cargos.json
- references/params.json
- references/metas_fornecedores.json
- RealizadosResult (output do realizados.py)
=============================================================================
"""

from __future__ import annotations

import math
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

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


def _normalize_hierarchy_part(value: Any) -> str:
    """Normaliza uma parte de hierarquia para comparação interna."""
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in ("", "nan", "none", "null"):
        return ""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch)).lower()


def _normalize_hierarchy_key(value: Any) -> str:
    """Normaliza uma chave hierárquica completa sem alterar sua exibição."""
    if value is None:
        return ""
    parts = str(value).split("/")
    normalized_parts = [_normalize_hierarchy_part(part) for part in parts]
    return "/".join(part for part in normalized_parts if part)


def _get_dict_value_normalized(data: Dict[str, Any], key: str, default: Any = 0.0) -> Any:
    """Busca em dict aceitando variações de acentuação na chave hierárquica."""
    if key in data:
        return data[key]

    normalized_key = _normalize_hierarchy_key(key)
    if not normalized_key:
        return default

    for existing_key, value in data.items():
        if _normalize_hierarchy_key(existing_key) == normalized_key:
            return value
    return default


# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS — Componentes do FC
# ═══════════════════════════════════════════════════════════════════════════════

# Componentes "padrão" (5 base + retenção)
# Nomes correspondem às colunas de pesos_metas no Supabase
COMPONENTES_BASE = [
    "faturamento_linha",       # realizado em faturamento_hierarquia
    "faturamento_individual",
    "conversao_linha",         # realizado em conversao_hierarquia
    "conversao_individual",
    "rentabilidade",
    "retencao_clientes",
]

# Componentes de fornecedores (até 2)
COMPONENTES_FORNECEDOR = [
    "meta_fornecedor_1",
    "meta_fornecedor_2",
]

ALL_COMPONENTES = COMPONENTES_BASE + COMPONENTES_FORNECEDOR

# Mapping: componente (nome DB) → atributo no RealizadosResult
# Componentes _linha agora lêem de dicts _hierarquia
_COMP_REALIZADO_ATTR = {
    "faturamento_linha": "faturamento_hierarquia",
    "conversao_linha": "conversao_hierarquia",
}
_COMP_META_ATTR = {
    "faturamento_linha": "metas_faturamento_hierarquia",
    "conversao_linha": "metas_conversao_hierarquia",
}

# Componentes cujo atingimento depende da hierarquia do item
_COMPONENTES_HIERARQUIA = {"faturamento_linha", "conversao_linha", "rentabilidade"}


# ═══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class ComponenteFC:
    """Detalhe de um componente no cálculo do FC."""
    nome: str
    peso: float           # 0.0–1.0 (já convertido de %)
    realizado: float
    meta: float
    atingimento: float     # realizado / meta
    atingimento_cap: float  # min(atingimento, cap)
    contribuicao: float    # atingimento_cap × peso


@dataclass
class FCResult:
    """Resultado do cálculo de FC para um (colaborador, cargo, hierarquia_item)."""
    colaborador: str
    cargo: str
    linha: str                # primeiro nível da hierarquia (retrocompat)
    hierarquia_key: str = ""  # chave hierárquica completa (ex: "RH/Equip/Bombas")
    fc_rampa: float = 0.0
    fc_final: float = 0.0    # após escada (se aplicável) e cap
    modo: str = "RAMPA"       # "RAMPA" ou "ESCADA"
    componentes: List[ComponenteFC] = field(default_factory=list)

    # Detalhes escada (preenchidos quando modo=ESCADA)
    escada_num_degraus: Optional[int] = None
    escada_piso: Optional[float] = None
    escada_degrau_indice: Optional[int] = None


@dataclass
class FCResultSet:
    """Conjunto de resultados FC para todos os colaboradores."""
    resultados: List[FCResult] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0

    def get_fc(self, colaborador: str, hierarquia_key: str) -> Optional[float]:
        """Retorna o FC final para um (colaborador, hierarquia_key).

        Busca por hierarquia_key exato. Se não encontrado, tenta por linha (retrocompat).
        Retorna None se não encontrado.
        """
        hierarquia_key_norm = _normalize_hierarchy_key(hierarquia_key)
        for r in self.resultados:
            if (
                r.colaborador == colaborador
                and _normalize_hierarchy_key(r.hierarquia_key) == hierarquia_key_norm
            ):
                return r.fc_final
        # Fallback retrocompat: buscar por linha
        for r in self.resultados:
            if (
                r.colaborador == colaborador
                and _normalize_hierarchy_key(r.linha) == hierarquia_key_norm
            ):
                return r.fc_final
        return None

    def get_result(self, colaborador: str, hierarquia_key: str) -> Optional[FCResult]:
        """Retorna o FCResult completo para um (colaborador, hierarquia_key)."""
        hierarquia_key_norm = _normalize_hierarchy_key(hierarquia_key)
        for r in self.resultados:
            if (
                r.colaborador == colaborador
                and _normalize_hierarchy_key(r.hierarquia_key) == hierarquia_key_norm
            ):
                return r
        # Fallback retrocompat: buscar por linha
        for r in self.resultados:
            if (
                r.colaborador == colaborador
                and _normalize_hierarchy_key(r.linha) == hierarquia_key_norm
            ):
                return r
        return None

    def summary(self) -> str:
        """Resumo textual dos FCs calculados."""
        lines = [
            f"{'='*65}",
            f"  FATOR DE CORREÇÃO — Resumo",
            f"{'='*65}",
            f"  Total de FCs calculados: {len(self.resultados)}",
        ]

        # Agrupar por cargo
        by_cargo: Dict[str, List[FCResult]] = {}
        for r in self.resultados:
            by_cargo.setdefault(r.cargo, []).append(r)

        for cargo in sorted(by_cargo.keys()):
            results = by_cargo[cargo]
            lines.append(f"\n  📊 {cargo} ({len(results)} entradas):")
            for r in sorted(results, key=lambda x: x.colaborador):
                modo_tag = f"[{r.modo}]"
                hier_display = r.hierarquia_key or r.linha
                lines.append(
                    f"    {r.colaborador:<25} {hier_display:<30} "
                    f"rampa={r.fc_rampa:.4f}  final={r.fc_final:.4f}  {modo_tag}"
                )
                if r.componentes:
                    for c in r.componentes:
                        if c.peso > 0:
                            lines.append(
                                f"      ├─ {c.nome:<25} peso={c.peso:.0%}  "
                                f"real={c.realizado:>12,.2f}  meta={c.meta:>12,.2f}  "
                                f"ating={c.atingimento_cap:.4f}  cont={c.contribuicao:.4f}"
                            )

        if self.warnings:
            lines.append(f"\n{'─'*65}")
            lines.append(f"  ⚠ Avisos ({len(self.warnings)}):")
            for w in self.warnings[:10]:
                lines.append(f"    • {w}")
        if self.errors:
            lines.append(f"  ✖ Erros ({len(self.errors)}):")
            for e in self.errors[:10]:
                lines.append(f"    • {e}")

        lines.append(f"{'='*65}")
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# CORE LOGIC
# ═══════════════════════════════════════════════════════════════════════════════

def _load_config() -> Tuple[Dict[Tuple[str, str, str], Dict], Dict[str, Dict], Dict]:
    """Carrega pesos, escada e params.

    Returns:
        (pesos_indexed, escada_por_cargo, params)
        pesos_indexed: {(cargo, colaborador, linha): {componente: peso_pct, ...}}
            — colaborador="" para regra genérica do cargo
            — linha="" para regra que se aplica a todas as linhas
        escada_por_cargo: {"consultor interno": {modo, num_degraus, piso_pct}, ...}
        params: {"cap_fc_max": 1.0, "cap_atingimento_max": 1.0, ...}
    """
    pesos_list = _load_json("pesos_metas.json")
    pesos_indexed: Dict[Tuple[str, str, str], Dict] = {}
    for entry in pesos_list:
        cargo = entry.get("cargo", "")
        colaborador = str(entry.get("colaborador", "") or "").strip()
        linha = str(entry.get("linha", "") or "").strip()
        key = (cargo, colaborador, linha)
        pesos_indexed[key] = {
            k: v for k, v in entry.items()
            if k not in ("cargo", "colaborador", "linha")
        }

    escada_list = _load_json("fc_escada_cargos.json")
    escada_por_cargo: Dict[Tuple[str, str], Dict] = {}
    for entry in escada_list:
        cargo = entry.get("cargo", "")
        colaborador = str(entry.get("colaborador", "") or "").strip()
        key = (cargo.strip().lower(), colaborador.lower())
        escada_por_cargo[key] = entry

    return pesos_indexed, escada_por_cargo


def _get_pesos(
    pesos_indexed: Dict[Tuple[str, str, str], Dict],
    cargo: str,
    colaborador: str,
    linha: str = "",
) -> Dict:
    """Busca pesos para (cargo, colaborador, linha) com fallbacks progressivos.

    Ordem: (cargo, colaborador, linha) → (cargo, colaborador, "") → (cargo, "", "")
    """
    if linha:
        pesos = pesos_indexed.get((cargo, colaborador, linha))
        if pesos is not None:
            return pesos
    pesos = pesos_indexed.get((cargo, colaborador, ""))
    if pesos is not None:
        return pesos
    return pesos_indexed.get((cargo, "", ""), {})


def _calcular_atingimento(realizado: float, meta: float) -> float:
    """Calcula atingimento = realizado / meta.

    Versão simplificada para a Skill (sem raise em realizado=0).
    - Se realizado <= 0: retorna 0.0
    - Se meta <= 0 e realizado > 0: retorna 1.0
    - Caso normal: realizado / meta
    """
    if realizado <= 0:
        return 0.0
    if meta <= 0:
        return 1.0
    return realizado / meta


def _aplicar_escada(
    fc_rampa: float,
    cargo: str,
    escada_por_cargo: Dict[Tuple[str, str], Dict],
    colaborador: str = "",
) -> Tuple[float, str, Optional[int], Optional[float], Optional[int]]:
    """Aplica regra de escada (ou rampa) e retorna multiplicador + detalhes.

    Busca a configuração por (cargo, colaborador). Se não houver regra
    específica para o colaborador, usa fallback para (cargo, "").

    Returns:
        (multiplicador, modo, num_degraus, piso, degrau_indice)
    """
    cargo_norm = cargo.strip().lower()
    colab_norm = colaborador.strip().lower()
    cfg = escada_por_cargo.get((cargo_norm, colab_norm))
    if cfg is None:
        cfg = escada_por_cargo.get((cargo_norm, ""))

    perf = max(0.0, fc_rampa)

    if cfg is None or cfg.get("modo", "RAMPA").upper() == "RAMPA":
        return perf, "RAMPA", None, None, None

    # ESCADA
    piso_pct = cfg.get("piso_pct", 0)

    # Normalizar piso: pode vir como 0-100 ou 0-1
    if isinstance(piso_pct, (int, float)) and piso_pct > 1.0:
        piso = piso_pct / 100.0
    else:
        piso = float(piso_pct)
    piso = max(0.0, min(1.0, piso))

    # Gerar todos os degraus
    intermediarios = cfg.get("degraus_intermediarios") or []
    if intermediarios:
        steps = [piso] + [float(v) for v in intermediarios] + [1.0]
        n = len(steps)
    else:
        n = max(2, int(cfg.get("num_degraus", 2)))
        steps = [piso + k * (1.0 - piso) / (n - 1) for k in range(n)]

    # Calcular degrau: arredondamento PARA CIMA.
    # Escolhe o menor degrau cujo valor seja >= perf. Ex.: com degraus
    # [0.30, 0.5333, 0.7667, 1.0], perf=0.31 → degrau 1 (0.5333).
    # perf acima de 1.0 cai no topo; perf abaixo do piso fica no piso.
    if perf >= 1.0:
        i = n - 1
    else:
        i = n - 1
        for k in range(n):
            if steps[k] >= perf:
                i = k
                break

    multiplicador = steps[i]
    multiplicador = max(0.0, min(1.0, multiplicador))

    return multiplicador, "ESCADA", n, piso, i


def gerar_degraus_escada(
    num_degraus: int,
    piso: float,
    degraus_intermediarios: Optional[List[float]] = None,
) -> List[float]:
    """Gera a lista de valores de todos os degraus da escada.

    Args:
        num_degraus: Número total de degraus (usado apenas quando degraus_intermediarios é None/vazio).
        piso: Valor do primeiro degrau (fração, ex: 0.5).
        degraus_intermediarios: Lista explícita de degraus intermediários (ex: [0.65, 0.80]).
            Quando fornecida e não vazia, retorna [piso] + intermediarios + [1.0].

    Returns:
        Lista ordenada de valores (ex: [0.5, 0.75, 1.0] para n=3, piso=0.5).
    """
    if degraus_intermediarios:
        return [piso] + [float(v) for v in degraus_intermediarios] + [1.0]
    if num_degraus < 2:
        return [piso]
    return [piso + k * (1.0 - piso) / (num_degraus - 1) for k in range(num_degraus)]


def _encontrar_meta_mais_especifica(
    hierarquia_item: Tuple[str, ...],
    metas_dict: Dict[str, float],
) -> Tuple[str, float]:
    """Encontra a meta mais específica que corresponde à hierarquia do item.

    Tenta do mais específico (6 campos) ao mais genérico (1 campo).
    Cada nível é uma chave consecutiva: "RH/Equip/Bombas/REVENDA/YSI/Monit".

    Args:
        hierarquia_item: Tupla de 6 strings (linha, grupo, subgrupo, tipo, fab, aplic)
        metas_dict: {chave_hierarquica: valor_meta}

    Returns:
        (chave_meta, valor_meta) ou ("", 0.0) se não encontrou
    """
    # Montar as chaves possíveis (do mais específico ao mais genérico)
    for level in range(len(hierarquia_item), 0, -1):
        parts = []
        valid = True
        for i in range(level):
            p = str(hierarquia_item[i]).strip() if hierarquia_item[i] else ""
            if not p or p.lower() in ("nan", "none", "null"):
                valid = False
                break
            parts.append(p)
        if valid and parts:
            key = "/".join(parts)
            if key in metas_dict:
                return key, metas_dict[key]

            normalized_key = _normalize_hierarchy_key(key)
            for existing_key, meta_value in metas_dict.items():
                if _normalize_hierarchy_key(existing_key) == normalized_key:
                    return existing_key, meta_value
    return "", 0.0


def _get_fornecedores_por_linha(
    metas_fornecedores: List[Dict],
) -> Dict[str, List[Dict]]:
    """Agrupa metas de fornecedores por linha.

    Returns:
        {"Hidrologia": [{"fornecedor": "YSI", "moeda": "USD", ...}, ...], ...}
    """
    result: Dict[str, List[Dict]] = {}
    for entry in metas_fornecedores:
        linha = entry.get("linha", "")
        result.setdefault(linha, []).append(entry)
    return result


def calcular_fc_item(
    colaborador: str,
    cargo: str,
    hierarquia_item: Tuple[str, ...],
    realizados_result: Any,
    pesos_indexed: Dict[Tuple[str, str], Dict],
    escada_por_cargo: Dict[str, Dict],
    params: Optional[Dict] = None,
    fornecedores_por_linha: Optional[Dict[str, List[Dict]]] = None,
) -> FCResult:
    """Calcula o FC para um (colaborador, cargo, hierarquia_item).

    Este é o cálculo central:
    1. Para cada componente com peso > 0, calcula atingimento
       — componentes _hierarquia: usa _encontrar_meta_mais_especifica
       — componentes _individual: chave = colaborador
    2. Aplica cap no atingimento
    3. Soma ponderada → fc_rampa
    4. Aplica cap_fc_max
    5. Aplica escada (se configurada)

    Args:
        colaborador: Nome do colaborador
        cargo: Cargo do colaborador
        hierarquia_item: Tupla de 6 str (linha, grupo, subgrupo, tipo, fabricante, aplicacao)
        realizados_result: RealizadosResult (do realizados.py)
        pesos_indexed: {(cargo, colaborador): {componente: peso_pct, ...}}
        escada_por_cargo: {cargo_lower: {modo, num_degraus, piso_pct}}
        params: {cap_fc_max, cap_atingimento_max, ...}
        fornecedores_por_linha: {linha: [{fornecedor, moeda, meta_anual}, ...]}

    Returns:
        FCResult
    """
    from scripts.realizados import _build_hierarchy_key

    linha_item = hierarquia_item[0] if hierarquia_item else ""
    pesos = _get_pesos(pesos_indexed, cargo, colaborador, linha_item)
    _p = params or {}
    cap_atingimento = float(_p.get("cap_atingimento_max", 1.0))
    cap_fc = float(_p.get("cap_fc_max", 1.0))
    hierarquia_key = _build_hierarchy_key(*hierarquia_item)

    componentes: List[ComponenteFC] = []
    fc_total = 0.0

    # ── Componentes base ──
    for comp_name in COMPONENTES_BASE:
        peso_pct = float(pesos.get(comp_name, 0))
        peso = peso_pct / 100.0
        if peso <= 0:
            continue

        realizado = 0.0
        meta = 0.0

        if comp_name in _COMPONENTES_HIERARQUIA:
            # Componentes que dependem da hierarquia do item
            if comp_name == "rentabilidade":
                real_dict = getattr(realizados_result, "rentabilidade", {})
                meta_dict = getattr(realizados_result, "metas_rentabilidade", {})
            else:
                real_attr = _COMP_REALIZADO_ATTR.get(comp_name, comp_name)
                meta_attr = _COMP_META_ATTR.get(comp_name, f"metas_{comp_name}")
                real_dict = getattr(realizados_result, real_attr, {})
                meta_dict = getattr(realizados_result, meta_attr, {})

            # Encontrar a meta mais específica para este item
            chave_meta, meta = _encontrar_meta_mais_especifica(hierarquia_item, meta_dict)
            # Usar o realizado no MESMO nível de agregação
            if chave_meta:
                realizado = _get_dict_value_normalized(real_dict, chave_meta, 0.0)
            try:
                from scripts.audit.trace_collector import TraceCollector
                if TraceCollector.is_enabled():
                    item_key_trace = f"{hierarquia_key}/{colaborador}"
                    TraceCollector.record(item_key_trace, "fc_meta", {
                        "componente": comp_name,
                        "hierarquia_buscada": "/".join(str(h) for h in hierarquia_item if h),
                        "meta_encontrada_em": f"L{len(chave_meta.split('/'))}" if chave_meta else "não encontrada",
                        "valor_meta": meta,
                        "niveis_tentados": [f"L{i}" for i in range(len(hierarquia_item), 0, -1)] if not chave_meta else [],
                    })
            except Exception:
                pass

        elif comp_name == "retencao_clientes":
            realizado = _get_dict_value_normalized(
                getattr(realizados_result, "retencao_clientes", {}),
                linha_item,
                0.0,
            )
            meta = 1.0  # Retenção: meta implícita = 1.0

        else:
            # _individual → chave = nome do colaborador
            real_dict = getattr(realizados_result, comp_name, {})
            meta_dict = getattr(realizados_result, f"metas_{comp_name}", {})
            realizado = real_dict.get(colaborador, 0.0)
            meta = meta_dict.get(colaborador, 0.0)

        atingimento = _calcular_atingimento(realizado, meta)
        atingimento_cap = min(atingimento, cap_atingimento)
        contribuicao = atingimento_cap * peso
        fc_total += contribuicao

        componentes.append(ComponenteFC(
            nome=comp_name,
            peso=peso,
            realizado=realizado,
            meta=meta,
            atingimento=atingimento,
            atingimento_cap=atingimento_cap,
            contribuicao=contribuicao,
        ))

    # ── Componentes de fornecedores ──
    if fornecedores_por_linha:
        fornecedores = fornecedores_por_linha.get(linha_item, [])
        for idx, forn in enumerate(fornecedores[:2], start=1):
            comp_name = f"meta_fornecedor_{idx}"
            peso_pct = float(pesos.get(comp_name, 0))
            peso = peso_pct / 100.0
            if peso <= 0:
                continue

            forn_key = f"{linha_item}/{forn.get('fornecedor', '')}"
            realizado = _get_dict_value_normalized(
                getattr(realizados_result, "fornecedor_ytd", {}),
                forn_key,
                0.0,
            )
            meta_info = _get_dict_value_normalized(
                getattr(realizados_result, "metas_fornecedor", {}),
                forn_key,
                {},
            )
            meta = meta_info.get("meta_ytd", 0.0) if isinstance(meta_info, dict) else 0.0

            atingimento = _calcular_atingimento(realizado, meta)
            atingimento_cap = min(atingimento, cap_atingimento)
            contribuicao = atingimento_cap * peso
            fc_total += contribuicao

            componentes.append(ComponenteFC(
                nome=comp_name,
                peso=peso,
                realizado=realizado,
                meta=meta,
                atingimento=atingimento,
                atingimento_cap=atingimento_cap,
                contribuicao=contribuicao,
            ))

    # ── Cap do FC rampa ──
    fc_rampa = min(fc_total, cap_fc)

    # ── Escada ──
    fc_final, modo, n_degraus, piso, degrau_idx = _aplicar_escada(
        fc_rampa, cargo, escada_por_cargo, colaborador,
    )

    fc_result_obj = FCResult(
        colaborador=colaborador,
        cargo=cargo,
        linha=linha_item,
        hierarquia_key=hierarquia_key,
        fc_rampa=fc_rampa,
        fc_final=fc_final,
        modo=modo,
        componentes=componentes,
        escada_num_degraus=n_degraus,
        escada_piso=piso,
        escada_degrau_indice=degrau_idx,
    )
    try:
        from scripts.audit.trace_collector import TraceCollector
        if TraceCollector.is_enabled():
            item_key_trace = f"{hierarquia_key}/{colaborador}"
            TraceCollector.record(item_key_trace, "fc_result", {
                "fc_rampa": fc_rampa,
                "fc_final": fc_final,
            })
    except Exception:
        pass
    return fc_result_obj


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def execute(
    atribuicoes: List[Dict],
    realizados_result: Any,
) -> FCResultSet:
    """Calcula o FC para todos os colaboradores atribuídos — FC por item.

    Extrai combinações únicas (colaborador, cargo, hierarquia_6) dos itens
    de atribuição e calcula um FC para cada combinação.

    Args:
        atribuicoes: Lista de dicts com keys: nome, cargo, linha (e opcionalmente
                     Grupo, Subgrupo, Tipo de Mercadoria, Fabricante, Aplicação Mat./Serv.)
                     (ou DataFrame com essas colunas)
        realizados_result: RealizadosResult do realizados.py

    Returns:
        FCResultSet
    """
    from scripts.realizados import _build_hierarchy_key, HIERARCHY_COLS

    result_set = FCResultSet()

    # Carregar configs
    pesos_indexed, escada_por_cargo = _load_config()

    # Carregar metas de fornecedores
    try:
        metas_forn = _load_json("metas_fornecedores.json")
        fornecedores_por_linha = _get_fornecedores_por_linha(metas_forn)
    except FileNotFoundError:
        fornecedores_por_linha = {}

    # Normalizar input: aceitar lista de dicts ou DataFrame
    records: List[Dict] = []
    if hasattr(atribuicoes, "to_dict"):
        # DataFrame
        records = atribuicoes.to_dict("records")  # type: ignore
    elif isinstance(atribuicoes, list):
        records = atribuicoes
    else:
        result_set.errors.append(f"Tipo de atribuições inválido: {type(atribuicoes)}")
        return result_set

    # Colunas de hierarquia (nomes usados no DataFrame de atribuições)
    hier_col_names = ["Linha", "Grupo", "Subgrupo", "Tipo de Mercadoria", "Fabricante", "Aplicação Mat./Serv."]
    # Fallback: colunas do realizados
    hier_col_alt = ["linha", "grupo", "subgrupo", "tipo_mercadoria", "fabricante", "aplicacao"]

    # Determinar combinações únicas (colaborador, cargo, hierarquia_key)
    seen: set = set()
    combos: List[Tuple[str, str, Tuple[str, ...]]] = []
    for rec in records:
        nome = rec.get("nome", "")
        cargo = rec.get("cargo", "")
        if not nome or not cargo:
            continue

        # Extrair 6 campos de hierarquia
        hier_parts: List[str] = []
        for col, alt in zip(hier_col_names, hier_col_alt):
            val = rec.get(col, rec.get(alt, ""))
            s = str(val).strip() if val is not None else ""
            if s.lower() in ("nan", "none", "null"):
                s = ""
            hier_parts.append(s)

        # Linha é obrigatória
        if not hier_parts[0]:
            continue

        hierarquia = tuple(hier_parts)
        hier_key = _build_hierarchy_key(*hierarquia)
        combo_key = (nome, cargo, _normalize_hierarchy_key(hier_key))
        if combo_key not in seen:
            seen.add(combo_key)
            combos.append((nome, cargo, hierarquia))

    if not combos:
        result_set.warnings.append("Nenhuma combinação (colaborador, cargo, hierarquia) encontrada.")
        return result_set

    # Calcular FC para cada combinação
    for nome, cargo, hierarquia in combos:
        # Verificar se existe pesos para este (cargo, colaborador, linha)
        pesos = _get_pesos(pesos_indexed, cargo, nome, hierarquia[0] if hierarquia else "")
        if not pesos:
            result_set.warnings.append(
                f"Cargo '{cargo}' não encontrado em pesos_metas — FC=0 para {nome}"
            )
            result_set.resultados.append(FCResult(
                colaborador=nome, cargo=cargo,
                linha=hierarquia[0],
                hierarquia_key=_build_hierarchy_key(*hierarquia),
                fc_rampa=0.0, fc_final=0.0, modo="RAMPA",
            ))
            continue

        fc_result = calcular_fc_item(
            colaborador=nome,
            cargo=cargo,
            hierarquia_item=hierarquia,
            realizados_result=realizados_result,
            pesos_indexed=pesos_indexed,
            escada_por_cargo=escada_por_cargo,
            fornecedores_por_linha=fornecedores_por_linha,
        )
        result_set.resultados.append(fc_result)

    return result_set


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

def _run_tests():
    """Testes embutidos — execute com: python scripts/fc_calculator.py --test"""
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
    print("  FC_CALCULATOR.PY — Testes Embutidos")
    print("=" * 60)

    # ── Helpers ──
    print("\n── _calcular_atingimento ──")
    _assert(_calcular_atingimento(90000, 100000) == 0.9, "ating 90%")
    _assert(_calcular_atingimento(100000, 100000) == 1.0, "ating 100%")
    _assert(_calcular_atingimento(120000, 100000) == 1.2, "ating 120%")
    _assert(_calcular_atingimento(50000, 0) == 1.0, "meta=0 → 1.0")
    _assert(_calcular_atingimento(0, 100000) == 0.0, "realizado=0 → 0.0")
    _assert(_calcular_atingimento(-100, 100000) == 0.0, "realizado negativo → 0.0")

    # ── Escada ──
    print("\n── _aplicar_escada ──")
    esc_config = {
        "consultor interno": {"modo": "ESCADA", "num_degraus": 2, "piso_pct": 50},
        "gerente linha": {"modo": "ESCADA", "num_degraus": 3, "piso_pct": 40},
    }

    # Escada 2 degraus, piso=50%
    mult, modo, n, piso, idx = _aplicar_escada(0.3, "Consultor Interno", esc_config)
    _assert(modo == "ESCADA", "escada 2d → modo ESCADA")
    _assert(abs(mult - 0.5) < 0.001, f"escada 2d perf=0.3 → mult=0.5 (piso) got {mult}")
    _assert(idx == 0, "escada 2d perf=0.3 → degrau 0")

    mult, modo, _, _, idx = _aplicar_escada(1.0, "Consultor Interno", esc_config)
    _assert(abs(mult - 1.0) < 0.001, "escada 2d perf=1.0 → mult=1.0 (topo)")
    _assert(idx == 1, "escada 2d perf=1.0 → degrau 1")

    # Escada 3 degraus, piso=40%
    mult, modo, n, piso, idx = _aplicar_escada(0.0, "Gerente Linha", esc_config)
    _assert(abs(mult - 0.4) < 0.001, f"escada 3d perf=0.0 → mult=0.4 (piso) got {mult}")
    _assert(idx == 0, "escada 3d perf=0.0 → degrau 0")

    mult, _, _, _, idx = _aplicar_escada(0.5, "Gerente Linha", esc_config)
    expected = 0.4 + (1 * (1.0 - 0.4) / 2)  # 0.7
    _assert(abs(mult - expected) < 0.001, f"escada 3d perf=0.5 → mult={expected} got {mult}")
    _assert(idx == 1, "escada 3d perf=0.5 → degrau 1")

    mult, _, _, _, idx = _aplicar_escada(1.0, "Gerente Linha", esc_config)
    _assert(abs(mult - 1.0) < 0.001, "escada 3d perf=1.0 → mult=1.0 (topo)")
    _assert(idx == 2, "escada 3d perf=1.0 → degrau 2")

    # Cargo sem config → RAMPA
    mult, modo, _, _, _ = _aplicar_escada(0.75, "Cargo Desconhecido", esc_config)
    _assert(modo == "RAMPA", "cargo desconhecido → RAMPA")
    _assert(abs(mult - 0.75) < 0.001, "rampa: mult = perf")

    # ── Mock RealizadosResult ──
    print("\n── calcular_fc_item ──")

    class MockRealizados:
        def __init__(self):
            self.faturamento_hierarquia = {"Hidrologia": 80000, "Hidrologia/Equipamentos": 60000}
            self.metas_faturamento_hierarquia = {"Hidrologia": 100000, "Hidrologia/Equipamentos": 70000}
            self.faturamento_individual = {"Dener Martins": 50000}
            self.metas_faturamento_individual = {"Dener Martins": 60000}
            self.conversao_hierarquia = {"Hidrologia": 70000}
            self.metas_conversao_hierarquia = {"Hidrologia": 80000}
            self.conversao_individual = {"Dener Martins": 30000}
            self.metas_conversao_individual = {"Dener Martins": 40000}
            self.rentabilidade = {"Hidrologia": 0.15}
            self.metas_rentabilidade = {"Hidrologia": 0.12}
            self.retencao_clientes = {"Hidrologia": 0.95}
            self.fornecedor_ytd = {"Hidrologia/YSI": 20.0}
            self.metas_fornecedor = {"Hidrologia/YSI": {"meta_ytd": 25.0, "moeda": "USD"}}
            # Retrocompat
            self.faturamento_linha = {"Hidrologia": 80000}
            self.metas_faturamento_linha = {"Hidrologia": 100000}
            self.conversao_linha = {"Hidrologia": 70000}
            self.metas_conversao_linha = {"Hidrologia": 80000}

    mock = MockRealizados()

    # Test 1: Cargo com 100% faturamento_linha → hierarquia "Hidrologia"
    pesos_100fat = {("Consultor Interno", ""): {"faturamento_linha": 100, "rentabilidade": 0,
                     "conversao_linha": 0, "faturamento_individual": 0,
                     "conversao_individual": 0, "retencao_clientes": 0,
                     "meta_fornecedor_1": 0, "meta_fornecedor_2": 0}}
    esc_real = {"consultor interno": {"modo": "ESCADA", "num_degraus": 2, "piso_pct": 50}}
    params_real = {"cap_atingimento_max": 1.0, "cap_fc_max": 1.0}

    fc = calcular_fc_item(
        "Dener Martins", "Consultor Interno",
        ("Hidrologia", "", "", "", "", ""),
        mock, pesos_100fat, esc_real, params_real, {},
    )
    # atingimento = 80000/100000 = 0.8, cap=0.8, peso=1.0 → rampa=0.8
    # escada 2d piso=50% (steps=[0.5, 1.0]): arredonda p/ cima → degrau 1 (1.0)
    _assert(abs(fc.fc_rampa - 0.8) < 0.001, f"rampa=0.8 got {fc.fc_rampa}")
    _assert(fc.modo == "ESCADA", "modo ESCADA")
    _assert(abs(fc.fc_final - 1.0) < 0.001, f"escada final=1.0 got {fc.fc_final}")
    _assert(fc.escada_degrau_indice == 1, "degrau=1")
    _assert(fc.hierarquia_key == "Hidrologia", "hierarquia_key=Hidrologia")

    # Test 1b: Same collaborator, more specific hierarchy → different meta match
    fc1b = calcular_fc_item(
        "Dener Martins", "Consultor Interno",
        ("Hidrologia", "Equipamentos", "", "", "", ""),
        mock, pesos_100fat, esc_real, params_real, {},
    )
    # meta mais específica: "Hidrologia/Equipamentos" = 70000
    # realizado no mesmo nível: 60000
    # atingimento = 60000/70000 ≈ 0.857
    _assert(abs(fc1b.fc_rampa - 60000/70000) < 0.001,
            f"hierarquia Equip rampa={60000/70000:.4f} got {fc1b.fc_rampa:.4f}")
    _assert(fc1b.hierarquia_key == "Hidrologia/Equipamentos", "hierarquia_key=Hidrologia/Equipamentos")

    # Test 2: atingimento=1.0 → topo da escada
    mock2 = MockRealizados()
    mock2.faturamento_hierarquia = {"Hidrologia": 100000}
    fc2 = calcular_fc_item(
        "Dener Martins", "Consultor Interno",
        ("Hidrologia", "", "", "", "", ""),
        mock2, pesos_100fat, esc_real, params_real, {},
    )
    _assert(abs(fc2.fc_rampa - 1.0) < 0.001, f"rampa=1.0 got {fc2.fc_rampa}")
    _assert(abs(fc2.fc_final - 1.0) < 0.001, f"escada topo=1.0 got {fc2.fc_final}")
    _assert(fc2.escada_degrau_indice == 1, "degrau=1 (topo)")

    # Test 3: Múltiplos componentes — pesos diversos
    pesos_multi = {("Gerente Linha", ""): {
        "faturamento_linha": 40, "faturamento_individual": 20,
        "conversao_linha": 20, "retencao_clientes": 10,
        "meta_fornecedor_1": 10,
        "conversao_individual": 0, "rentabilidade": 0,
        "meta_fornecedor_2": 0,
    }}
    forn_por_linha = {"Hidrologia": [{"fornecedor": "YSI", "moeda": "USD", "meta_anual": 30}]}

    fc3 = calcular_fc_item(
        "Dener Martins", "Gerente Linha",
        ("Hidrologia", "", "", "", "", ""),
        mock, pesos_multi, {}, params_real, forn_por_linha,
    )
    # faturamento_linha: 80000/100000=0.8 × 0.4 = 0.32
    # faturamento_individual: 50000/60000=0.833 × 0.2 = 0.1667
    # conversao_linha: 70000/80000=0.875 × 0.2 = 0.175
    # retencao_clientes: 0.95 / 1.0 = 0.95 × 0.1 = 0.095
    # meta_fornecedor_1: 20/25=0.8 × 0.1 = 0.08
    expected_rampa = 0.32 + 50000/60000*0.2 + 0.875*0.2 + 0.95*0.1 + 0.8*0.1
    _assert(abs(fc3.fc_rampa - expected_rampa) < 0.002,
            f"rampa multi={expected_rampa:.4f} got {fc3.fc_rampa:.4f}")
    _assert(fc3.modo == "RAMPA", "cargo sem escada → RAMPA")
    _assert(abs(fc3.fc_final - fc3.fc_rampa) < 0.001, "rampa final=rampa")
    _assert(len(fc3.componentes) == 5, f"5 componentes com peso>0 got {len(fc3.componentes)}")

    # Test 4: cap_atingimento limita a 1.0
    mock4 = MockRealizados()
    mock4.faturamento_hierarquia = {"Hidrologia": 150000}
    fc4 = calcular_fc_item(
        "Dener", "Consultor Interno",
        ("Hidrologia", "", "", "", "", ""),
        mock4, pesos_100fat, {}, params_real, {},
    )
    _assert(abs(fc4.fc_rampa - 1.0) < 0.001,
            f"cap ating 1.0 → rampa=1.0 got {fc4.fc_rampa}")

    # Test 5: cap_fc_max limita rampa
    params_cap = {"cap_atingimento_max": 2.0, "cap_fc_max": 0.8}
    fc5 = calcular_fc_item(
        "Dener", "Consultor Interno",
        ("Hidrologia", "", "", "", "", ""),
        mock4, pesos_100fat, {}, params_cap, {},
    )
    # ating = 1.5, cap_ating=2.0 → ating_cap=1.5, rampa=1.5, cap_fc=0.8 → rampa=0.8
    _assert(abs(fc5.fc_rampa - 0.8) < 0.001,
            f"cap_fc_max=0.8 → rampa=0.8 got {fc5.fc_rampa}")

    # Test 6: _load_config with (cargo, colaborador) indexing
    print("\n── _load_config (cargo, colaborador) ──")
    _sl.clear_cache()
    _sl._CACHE["pesos_metas.json"] = [
        {"cargo": "CI", "colaborador": "", "faturamento_linha": 100, "conversao_linha": 0},
        {"cargo": "CI", "colaborador": "Dener", "faturamento_linha": 60, "conversao_linha": 40},
    ]
    _sl._CACHE["fc_escada_cargos.json"] = []
    pi, _ = _load_config()
    pesos_generic = _get_pesos(pi, "CI", "João")  # fallback to (CI, "")
    _assert(float(pesos_generic.get("faturamento_linha", 0)) == 100,
            "fallback (CI, '') → faturamento_linha=100")
    pesos_specific = _get_pesos(pi, "CI", "Dener")  # specific
    _assert(float(pesos_specific.get("faturamento_linha", 0)) == 60,
            "specific (CI, Dener) → faturamento_linha=60")
    _assert(float(pesos_specific.get("conversao_linha", 0)) == 40,
            "specific (CI, Dener) → conversao_linha=40")
    _sl.clear_cache()

    # ── Full execute with mock JSONs ──
    print("\n── execute (com mock JSONs) ──")
    _sl.clear_cache()
    _sl._CACHE["pesos_metas.json"] = [
        {"cargo": "Consultor Interno", "colaborador": "",
         "faturamento_linha": 100, "conversao_linha": 0,
         "faturamento_individual": 0, "conversao_individual": 0,
         "rentabilidade": 0, "retencao_clientes": 0,
         "meta_fornecedor_1": 0, "meta_fornecedor_2": 0},
        {"cargo": "Gerente Linha", "colaborador": "",
         "faturamento_linha": 50, "conversao_linha": 0,
         "faturamento_individual": 50, "conversao_individual": 0,
         "rentabilidade": 0, "retencao_clientes": 0,
         "meta_fornecedor_1": 0, "meta_fornecedor_2": 0},
    ]
    _sl._CACHE["fc_escada_cargos.json"] = []
    _sl._CACHE["params.json"] = {"cap_atingimento_max": 1.0, "cap_fc_max": 1.0}
    _sl._CACHE["metas_fornecedores.json"] = []

    atribuicoes_test = [
        {"nome": "Dener Martins", "cargo": "Consultor Interno",
         "Linha": "Hidrologia", "Grupo": "", "Subgrupo": "",
         "Tipo de Mercadoria": "", "Fabricante": "", "Aplicação Mat./Serv.": ""},
        {"nome": "André Caramello", "cargo": "Gerente Linha",
         "Linha": "Hidrologia", "Grupo": "", "Subgrupo": "",
         "Tipo de Mercadoria": "", "Fabricante": "", "Aplicação Mat./Serv.": ""},
        # Duplicata — deve aparecer só 1 vez
        {"nome": "Dener Martins", "cargo": "Consultor Interno",
         "Linha": "Hidrologia", "Grupo": "", "Subgrupo": "",
         "Tipo de Mercadoria": "", "Fabricante": "", "Aplicação Mat./Serv.": ""},
    ]
    result = execute(atribuicoes_test, mock)
    _assert(result.ok, "execute ok")
    _assert(len(result.resultados) == 2, f"2 combinações únicas got {len(result.resultados)}")

    fc_dener = result.get_fc("Dener Martins", "Hidrologia")
    _assert(fc_dener is not None, "Dener FC encontrado")

    fc_andre = result.get_fc("André Caramello", "Hidrologia")
    _assert(fc_andre is not None, "André FC encontrado")

    # Verificar que summary funciona
    s = result.summary()
    _assert("FATOR DE CORREÇÃO" in s, "summary gerado")

    # ── Edge: cargo desconhecido ──
    print("\n── Edge cases ──")
    atrib_edge = [{"nome": "Teste", "cargo": "Cargo Inexistente",
                   "Linha": "X", "Grupo": "", "Subgrupo": "",
                   "Tipo de Mercadoria": "", "Fabricante": "", "Aplicação Mat./Serv.": ""}]
    result_edge = execute(atrib_edge, mock)
    _assert(len(result_edge.warnings) > 0, "warning para cargo inexistente")
    _assert(result_edge.resultados[0].fc_final == 0.0, "FC=0 para cargo inexistente")

    # ── Edge: lista vazia ──
    result_empty = execute([], mock)
    _assert(result_empty.ok, "lista vazia ok (sem erros)")
    _assert(len(result_empty.resultados) == 0, "nenhum resultado")

    _sl.clear_cache()

    # ── _get_fornecedores_por_linha ──
    print("\n── _get_fornecedores_por_linha ──")
    forn = _get_fornecedores_por_linha([
        {"linha": "Hidrologia", "fornecedor": "YSI"},
        {"linha": "Hidrologia", "fornecedor": "Hach"},
        {"linha": "Ambiental", "fornecedor": "Env"},
    ])
    _assert(len(forn["Hidrologia"]) == 2, "2 fornecedores Hidrologia")
    _assert(len(forn["Ambiental"]) == 1, "1 fornecedor Ambiental")

    # ── Resultado final ──
    print(f"\n{'='*60}")
    print(f"  RESULTADO: {passed}/{total} testes passaram")
    if failed:
        print(f"  ✗ {failed} teste(s) falharam")
    print(f"{'='*60}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    import sys
    if "--test" in sys.argv:
        _run_tests()
    else:
        print("Usage: python scripts/fc_calculator.py --test")
        print("  Para uso na skill: import scripts.fc_calculator as fc")
