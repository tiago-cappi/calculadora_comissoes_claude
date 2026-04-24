"""
=============================================================================
SKILL: Robô de Comissões — Script 05: Comissão por Faturamento
=============================================================================
Módulo   : 05_comissao_faturamento
Versão   : 1.0.0
Autor    : Claude Commission Skill

Descrição
---------
Calcula a **comissão por faturamento** para cada colaborador em cada
item faturado no período de apuração.

Fórmula principal:
    Comissão_Potencial = Valor_Realizado × Taxa_Rateio × Fatia_Cargo × Fator_Split
    Comissão_Final     = Comissão_Potencial × FC_Aplicado

Trata cross-selling conforme opção A (subtrair taxa) ou B (taxa adicional).

Dependências
------------
- AtribuicaoResult (output do atribuicao.py)
- FCResultSet       (output do fc_calculator.py)
- references/params.json  (para arredondamento)
=============================================================================
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

STATUS_FATURADO = "FATURADO"
TIPO_COMISSAO_FATURAMENTO = "Faturamento"

# Colunas herdadas do atribuicao.py (nomes das colunas da AC)
COL_PROCESSO = "Processo"
COL_STATUS = "Status Processo"
COL_NF = "Numero NF"
COL_DT_EMISSAO = "Dt Emissão"
COL_VALOR_REALIZADO = "Valor Realizado"
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

# Campos da atribuição
FIELD_NOME = "nome"
FIELD_CARGO = "cargo"
FIELD_TIPO_CARGO = "tipo_cargo"
FIELD_TIPO_COMISSAO = "tipo_comissao"
FIELD_FATIA_CARGO_PCT = "fatia_cargo_pct"
FIELD_TAXA_RATEIO_PCT = "taxa_rateio_pct"

# Observações
OBS_CROSS_SELLING = "CROSS_SELLING"
OBS_NORMAL = ""


# ═══════════════════════════════════════════════════════════════════════════════
# PATH RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# CARREGAMENTO DE REGRAS — via Supabase (substituiu references/*.json)
# ═══════════════════════════════════════════════════════════════════════════════

from scripts import supabase_loader as _sl
from scripts.realizados import _build_hierarchy_key


def _load_json(filename: str) -> Any:
    """Carrega regras de negócio do Supabase (equivalente ao JSON original)."""
    return _sl.load_json(filename)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_float(value: Any, default: float = 0.0) -> float:
    """Converte para float de forma segura."""
    if value is None:
        return default
    try:
        v = float(value)
        if v != v:  # NaN check
            return default
        return v
    except (ValueError, TypeError):
        return default


def _safe_str(value: Any, default: str = "") -> str:
    """Converte para string de forma segura."""
    if value is None:
        return default
    s = str(value).strip()
    if s.lower() in ("nan", "none", "null"):
        return default
    return s


def _normalize(text: str) -> str:
    """Normaliza string para comparação (lowercased, stripped)."""
    return str(text).strip().lower()


def _has_dt_emissao(atrib: Dict[str, Any]) -> bool:
    """True se a atribuição tem Dt Emissão preenchida.

    O loader já filtra a AC por Dt Emissão ∈ (mês, ano), então a simples
    existência de uma data não-nula aqui implica que ela pertence ao
    período selecionado pelo usuário.
    """
    v = atrib.get(COL_DT_EMISSAO)
    if v is None:
        return False
    try:
        if pd.isna(v):
            return False
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "null", "nat"):
        return False
    return True


def _round_currency(value: float, decimais: int = 2) -> float:
    """Arredonda valor monetário."""
    return round(value, decimais)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class ComissaoFaturamentoResult:
    """Resultado completo do cálculo de comissões por faturamento."""
    comissoes: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0

    @property
    def total_comissoes(self) -> float:
        """Total geral de comissões calculadas (comissao_final)."""
        return sum(_safe_float(c.get("comissao_final", 0)) for c in self.comissoes)

    @property
    def total_potencial(self) -> float:
        """Total de comissões potenciais (sem FC)."""
        return sum(_safe_float(c.get("comissao_potencial", 0)) for c in self.comissoes)

    def get_comissoes_colaborador(self, nome: str) -> List[Dict]:
        """Retorna todas as comissões de um colaborador."""
        nome_norm = _normalize(nome)
        return [c for c in self.comissoes if _normalize(c.get("nome", "")) == nome_norm]

    def consolidar_por_colaborador(self) -> pd.DataFrame:
        """Consolida comissões por colaborador.

        Returns:
            DataFrame com colunas: nome, cargo, comissao_potencial, comissao_final,
            num_itens, fc_medio_ponderado
        """
        if not self.comissoes:
            return pd.DataFrame(columns=[
                "nome", "cargo", "comissao_potencial", "comissao_final",
                "num_itens", "fc_medio_ponderado",
            ])

        records: Dict[str, Dict] = {}
        for c in self.comissoes:
            nome = c.get("nome", "")
            cargo = c.get("cargo", "")
            key = f"{nome}|{cargo}"
            if key not in records:
                records[key] = {
                    "nome": nome,
                    "cargo": cargo,
                    "comissao_potencial": 0.0,
                    "comissao_final": 0.0,
                    "num_itens": 0,
                    "_soma_fc_x_valor": 0.0,
                    "_soma_valor": 0.0,
                }
            r = records[key]
            potencial = _safe_float(c.get("comissao_potencial", 0))
            final = _safe_float(c.get("comissao_final", 0))
            valor = _safe_float(c.get("valor_item", 0))
            fc = _safe_float(c.get("fc_final", 0))

            r["comissao_potencial"] += potencial
            r["comissao_final"] += final
            r["num_itens"] += 1
            r["_soma_fc_x_valor"] += fc * abs(valor)
            r["_soma_valor"] += abs(valor)

        rows = []
        for r in records.values():
            soma_val = r.pop("_soma_valor")
            soma_fc_val = r.pop("_soma_fc_x_valor")
            r["fc_medio_ponderado"] = (
                soma_fc_val / soma_val if soma_val > 0 else 0.0
            )
            rows.append(r)

        df = pd.DataFrame(rows)
        df = df.sort_values("comissao_final", ascending=False).reset_index(drop=True)
        return df

    def to_dataframe(self) -> pd.DataFrame:
        """Converte comissões para DataFrame completo."""
        if not self.comissoes:
            return pd.DataFrame()
        return pd.DataFrame(self.comissoes)

    def summary(self) -> str:
        """Resumo textual das comissões por faturamento."""
        lines = [
            f"{'='*65}",
            f"  COMISSÃO POR FATURAMENTO — Resumo",
            f"{'='*65}",
            f"  Total de linhas de comissão : {len(self.comissoes):>10,}",
            f"  Total potencial (sem FC)    : R$ {self.total_potencial:>14,.2f}",
            f"  Total final (com FC)        : R$ {self.total_comissoes:>14,.2f}",
        ]

        # Itens normais vs cross-selling
        normais = [c for c in self.comissoes if c.get("observacao") != OBS_CROSS_SELLING]
        cs_lines = [c for c in self.comissoes if c.get("observacao") == OBS_CROSS_SELLING]
        if cs_lines:
            total_cs = sum(_safe_float(c.get("comissao_final", 0)) for c in cs_lines)
            lines.append(f"    Normal                    : {len(normais):>10,} linhas")
            lines.append(f"    Cross-selling             : {len(cs_lines):>10,} linhas  (R$ {total_cs:>12,.2f})")

        # Consolidado por colaborador (top 10)
        df_consol = self.consolidar_por_colaborador()
        if not df_consol.empty:
            lines.append(f"\n{'─'*65}")
            lines.append(f"  Consolidado por colaborador (top 10):")
            for _, row in df_consol.head(10).iterrows():
                lines.append(
                    f"    {row['nome']:<25} {row['cargo']:<20} "
                    f"R$ {row['comissao_final']:>12,.2f}  "
                    f"({row['num_itens']} itens)"
                )

        if self.warnings:
            lines.append(f"\n{'─'*65}")
            lines.append(f"  ⚠ Avisos ({len(self.warnings)}):")
            for w in self.warnings[:15]:
                lines.append(f"    • {w}")
            if len(self.warnings) > 15:
                lines.append(f"    ... e mais {len(self.warnings) - 15}")
        if self.errors:
            lines.append(f"  ✖ Erros ({len(self.errors)}):")
            for e in self.errors[:10]:
                lines.append(f"    • {e}")
        if not self.warnings and not self.errors:
            lines.append(f"  ✔ Nenhum aviso ou erro.")

        lines.append(f"{'='*65}")
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# CORE LOGIC
# ═══════════════════════════════════════════════════════════════════════════════


def _build_cs_lookup(
    cross_selling_cases: Optional[List[Any]],
) -> Dict[str, Dict]:
    """Constrói lookup de cross-selling por processo.

    Cada CrossSellingCase tem: processo, consultor, cargo, linha_item,
    taxa_cross_selling_pct, itens_afetados.

    Returns:
        {processo: {consultor, taxa_pct, cargo, linha_item}}
    """
    if not cross_selling_cases:
        return {}
    lookup: Dict[str, Dict] = {}
    for cs in cross_selling_cases:
        # Suporta tanto dataclass quanto dict
        if hasattr(cs, "processo"):
            processo = str(cs.processo).strip()
            lookup[processo] = {
                "consultor": cs.consultor,
                "taxa_pct": float(cs.taxa_cross_selling_pct),
                "cargo": getattr(cs, "cargo", "Consultor Externo"),
                "linha_item": getattr(cs, "linha_item", ""),
            }
        elif isinstance(cs, dict):
            processo = str(cs.get("processo", "")).strip()
            lookup[processo] = {
                "consultor": cs.get("consultor", ""),
                "taxa_pct": float(cs.get("taxa_cross_selling_pct", 0)),
                "cargo": cs.get("cargo", "Consultor Externo"),
                "linha_item": cs.get("linha_item", ""),
            }
    return lookup


def _is_cs_consultor(nome: str, cs_info: Dict) -> bool:
    """Verifica se o nome é o consultor externo do cross-selling."""
    return _normalize(nome) == _normalize(cs_info.get("consultor", ""))


def calcular_comissao_item(
    atrib: Dict[str, Any],
    fc_result_set: Any,
    cs_lookup: Dict[str, Dict],
    decimais: int,
) -> Optional[Dict[str, Any]]:
    """Calcula comissão de um item de atribuição.

    Args:
        atrib: dict de atribuição (do atribuicao.py)
        fc_result_set: FCResultSet
        cs_lookup: lookup de cross-selling por processo (cada entrada já contém
                   a chave "option" com "A" ou "B" resolvida por execute())
        decimais: casas decimais para arredondamento

    Returns:
        dict com dados da comissão ou None se item excluído
    """
    nome = _safe_str(atrib.get(FIELD_NOME))
    cargo = _safe_str(atrib.get(FIELD_CARGO))
    processo = _safe_str(atrib.get(COL_PROCESSO))
    linha = _safe_str(atrib.get(COL_LINHA))
    grupo = _safe_str(atrib.get(COL_GRUPO))
    subgrupo = _safe_str(atrib.get(COL_SUBGRUPO))
    tipo_mercadoria = _safe_str(atrib.get(COL_TIPO_MERCADORIA))
    fabricante = _safe_str(atrib.get(COL_FABRICANTE))
    aplicacao = _safe_str(atrib.get(COL_APLICACAO))

    # Chave hierárquica para busca do FC por item
    hierarquia_key = _build_hierarchy_key(
        linha, grupo, subgrupo, tipo_mercadoria, fabricante, aplicacao,
    )

    # Cross-selling: pular o consultor externo (comissão tratada separadamente)
    cs_info = cs_lookup.get(processo)
    if cs_info and _is_cs_consultor(nome, cs_info):
        return None

    op_raw = _safe_str(atrib.get(COL_OPERACAO, ""))
    op_code = op_raw.split(" - ", 1)[0].strip().upper() if op_raw else ""
    is_pdir = op_code.startswith("PDIR")
    valor_item = _safe_float(atrib.get("Valor Orçado", 0)) if is_pdir else _safe_float(atrib.get(COL_VALOR_REALIZADO, 0))
    taxa_rateio_pct = _safe_float(atrib.get(FIELD_TAXA_RATEIO_PCT, 0))
    fatia_cargo_pct = _safe_float(atrib.get(FIELD_FATIA_CARGO_PCT, 0))

    # Converter de % para decimal
    taxa_rateio = taxa_rateio_pct / 100.0
    fatia_cargo = fatia_cargo_pct / 100.0

    # Ajuste cross-selling Opção A: subtrair taxa CS da taxa de rateio
    cs_decision = None
    if cs_info:
        item_cs_option = cs_info.get("option", "B")
        cs_decision = item_cs_option
        if item_cs_option == "A":
            taxa_cs_decimal = cs_info["taxa_pct"] / 100.0
            taxa_rateio = max(0.0, taxa_rateio - taxa_cs_decimal)

    # Obter FC — busca por hierarquia_key (com fallback interno para linha)
    fc_val = None
    fc_rampa = 0.0
    fc_final = 0.0
    fc_modo = "N/A"

    if fc_result_set is not None:
        fc_result = None
        # Tentar get_result (retorna FCResult com detalhes)
        if hasattr(fc_result_set, "get_result"):
            fc_result = fc_result_set.get_result(nome, hierarquia_key)
        if fc_result is not None:
            fc_rampa = getattr(fc_result, "fc_rampa", 0.0)
            fc_final = getattr(fc_result, "fc_final", 0.0)
            fc_modo = getattr(fc_result, "modo", "N/A")
        elif hasattr(fc_result_set, "get_fc"):
            fc_val = fc_result_set.get_fc(nome, hierarquia_key)
            if fc_val is not None:
                fc_rampa = fc_val
                fc_final = fc_val
                fc_modo = "N/A"

    # Cálculos
    comissao_potencial = _round_currency(
        valor_item * taxa_rateio * fatia_cargo, decimais
    )
    comissao_final = _round_currency(comissao_potencial * fc_final, decimais)

    try:
        from scripts.audit.trace_collector import TraceCollector
        if TraceCollector.is_enabled():
            item_key_trace = f"{hierarquia_key}/{nome}"
            TraceCollector.record(item_key_trace, "comissao", {
                "formula": f"{valor_item} × {round(taxa_rateio * 100, 4)}% × {fatia_cargo_pct}% × FC{fc_final}",
                "comissao_potencial": comissao_potencial,
                "comissao_final": comissao_final,
            })
    except Exception:
        pass

    return {
        "nome": nome,
        "cargo": cargo,
        "tipo_cargo": _safe_str(atrib.get(FIELD_TIPO_CARGO)),
        "tipo_comissao": _safe_str(atrib.get(FIELD_TIPO_COMISSAO)),
        "processo": processo,
        "numero_nf": _safe_str(atrib.get(COL_NF)),
        "linha": linha,
        "grupo": _safe_str(atrib.get(COL_GRUPO)),
        "subgrupo": _safe_str(atrib.get(COL_SUBGRUPO)),
        "tipo_mercadoria": _safe_str(atrib.get(COL_TIPO_MERCADORIA)),
        "fabricante": _safe_str(atrib.get(COL_FABRICANTE)),
        "aplicacao": aplicacao,
        "hierarquia_key": hierarquia_key,
        "codigo_produto": _safe_str(atrib.get(COL_CODIGO_PRODUTO)),
        "descricao_produto": _safe_str(atrib.get("Descrição Produto")),
        "cliente": _safe_str(atrib.get(COL_CLIENTE)),
        "nome_cliente": _safe_str(atrib.get(COL_NOME_CLIENTE)),
        "valor_item": valor_item,
        "taxa_rateio_pct": _round_currency(taxa_rateio * 100.0, 4),
        "fatia_cargo_pct": fatia_cargo_pct,
        "fc_rampa": fc_rampa,
        "fc_final": fc_final,
        "fc_modo": fc_modo,
        "comissao_potencial": comissao_potencial,
        "comissao_final": comissao_final,
        "observacao": OBS_NORMAL,
        "cross_selling_decision": cs_decision,
        "is_pdir": is_pdir,
    }


def _gerar_linhas_cross_selling(
    atribuicoes: List[Dict],
    cs_lookup: Dict[str, Dict],
    decimais: int,
) -> List[Dict]:
    """Gera linhas de comissão de cross-selling para consultores externos.

    Para cada processo com CS, para cada item FATURADO do processo:
    - Comissão = Valor_Realizado × (taxa_cross_selling_pct / 100)
    - FC = 1.0 (CE não tem FC aplicado)

    Returns:
        Lista de dicts de comissão CE
    """
    if not cs_lookup:
        return []

    # Agrupar itens comissionáveis (Dt Emissão no período) por processo
    # (un-deduplicated: 1 item per unique AC row)
    itens_por_processo: Dict[str, List[Dict]] = {}
    seen: set = set()
    for atrib in atribuicoes:
        if not _has_dt_emissao(atrib):
            continue
        processo = _safe_str(atrib.get(COL_PROCESSO))
        if processo not in cs_lookup:
            continue
        # Deduplicar por (processo, idx_ac) para não contar o item múltiplas vezes
        # (cada item aparece N vezes na atribuição, uma por colaborador)
        idx_ac = atrib.get("idx_ac", "")
        dedup_key = f"{processo}|{idx_ac}"
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        itens_por_processo.setdefault(processo, []).append(atrib)

    result = []
    for processo, itens in itens_por_processo.items():
        cs_info = cs_lookup[processo]
        consultor = cs_info["consultor"]
        cargo_ce = cs_info.get("cargo", "Consultor Externo")
        taxa_pct = cs_info["taxa_pct"]
        taxa_decimal = taxa_pct / 100.0

        for item in itens:
            op_raw_cs = _safe_str(item.get(COL_OPERACAO, ""))
            op_code_cs = op_raw_cs.split(" - ", 1)[0].strip().upper() if op_raw_cs else ""
            is_pdir_cs = op_code_cs.startswith("PDIR")
            valor = _safe_float(item.get("Valor Orçado", 0)) if is_pdir_cs else _safe_float(item.get(COL_VALOR_REALIZADO, 0))
            comissao = _round_currency(valor * taxa_decimal, decimais)

            item_linha = _safe_str(item.get(COL_LINHA))
            item_grupo = _safe_str(item.get(COL_GRUPO))
            item_subgrupo = _safe_str(item.get(COL_SUBGRUPO))
            item_tipo_merc = _safe_str(item.get(COL_TIPO_MERCADORIA))
            item_fabricante = _safe_str(item.get(COL_FABRICANTE))
            item_aplicacao = _safe_str(item.get(COL_APLICACAO))
            item_hierarquia_key = _build_hierarchy_key(
                item_linha, item_grupo, item_subgrupo,
                item_tipo_merc, item_fabricante, item_aplicacao,
            )

            result.append({
                "nome": consultor,
                "cargo": cargo_ce,
                "tipo_cargo": "Operacional",
                "tipo_comissao": TIPO_COMISSAO_FATURAMENTO,
                "processo": processo,
                "numero_nf": _safe_str(item.get(COL_NF)),
                "linha": item_linha,
                "grupo": item_grupo,
                "subgrupo": item_subgrupo,
                "tipo_mercadoria": item_tipo_merc,
                "fabricante": item_fabricante,
                "aplicacao": item_aplicacao,
                "hierarquia_key": item_hierarquia_key,
                "codigo_produto": _safe_str(item.get(COL_CODIGO_PRODUTO)),
                "descricao_produto": _safe_str(item.get("Descrição Produto")),
                "cliente": _safe_str(item.get(COL_CLIENTE)),
                "nome_cliente": _safe_str(item.get(COL_NOME_CLIENTE)),
                "valor_item": valor,
                "taxa_rateio_pct": taxa_pct,
                "fatia_cargo_pct": 100.0,
                "fc_rampa": 1.0,
                "fc_final": 1.0,
                "fc_modo": "N/A",
                "comissao_potencial": comissao,
                "comissao_final": comissao,
                "observacao": OBS_CROSS_SELLING,
                "cross_selling_decision": None,
                "is_pdir": is_pdir_cs,
            })

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════


def execute(
    atribuicoes: Union[List[Dict], "pd.DataFrame"],
    fc_result_set: Any,
    cross_selling_cases: Optional[List[Any]] = None,
    cross_selling_option: Union[str, Dict[str, str]] = "B",
) -> ComissaoFaturamentoResult:
    """Calcula comissões por faturamento.

    Para cada item faturado, aplica a fórmula:
      Comissão = Valor × Taxa_Rateio × Fatia_Cargo × Split × FC

    Cross-selling:
    - Opção A: Taxa de rateio é reduzida pela taxa_cross_selling para os
      colaboradores normais. CE recebe comissão = Valor × taxa_cs.
    - Opção B: Taxa de rateio inalterada. CE recebe comissão adicional
      = Valor × taxa_cs. CE é excluído do rateio normal em ambas as opções.

    Args:
        atribuicoes: Lista de dicts ou DataFrame do atribuicao.py.
        fc_result_set: FCResultSet do fc_calculator.py.
        cross_selling_cases: Lista de CrossSellingCase do atribuicao.py.
        cross_selling_option: "A"/"B" global, OU dict {nome_consultor: "A"/"B"}
            com opção individual por consultor. Default: "B".

    Returns:
        ComissaoFaturamentoResult
    """
    result = ComissaoFaturamentoResult()

    decimais = 2

    # Converter DataFrame para lista de dicts
    if isinstance(atribuicoes, pd.DataFrame):
        atribs_list = atribuicoes.to_dict("records")
    else:
        atribs_list = list(atribuicoes) if atribuicoes else []

    if not atribs_list:
        result.warnings.append("Nenhuma atribuição recebida.")
        return result

    # Build cross-selling lookup e enriquecer com a opção por consultor
    cs_lookup = _build_cs_lookup(cross_selling_cases)
    if cs_lookup:
        if isinstance(cross_selling_option, dict):
            # Opção individual por consultor
            for proc_info in cs_lookup.values():
                consultor = proc_info.get("consultor", "")
                raw = cross_selling_option.get(consultor, "B")
                opt = str(raw).strip().upper()
                proc_info["option"] = opt if opt in ("A", "B") else "B"
            opcoes_resumo = ", ".join(
                f"{info['consultor']}={info['option']}"
                for info in cs_lookup.values()
            )
            result.warnings.append(
                f"Cross-selling detectado em {len(cs_lookup)} processo(s). "
                f"Opções por consultor: {opcoes_resumo}."
            )
        else:
            # Opção global (string)
            cs_option_global = str(cross_selling_option).strip().upper()
            if cs_option_global not in ("A", "B"):
                result.warnings.append(
                    f"cross_selling_option inválida '{cross_selling_option}', usando 'B'."
                )
                cs_option_global = "B"
            for proc_info in cs_lookup.values():
                proc_info["option"] = cs_option_global
            result.warnings.append(
                f"Cross-selling detectado em {len(cs_lookup)} processo(s). "
                f"Opção selecionada: {cs_option_global}."
            )

    # Filtrar atribuições: Dt Emissão preenchida (período já garantido pelo loader)
    # + tipo de comissão = Faturamento.
    faturados = []
    skipped_dt = 0
    skipped_tipo = 0
    for atrib in atribs_list:
        tipo = _safe_str(atrib.get(FIELD_TIPO_COMISSAO))

        if not _has_dt_emissao(atrib):
            skipped_dt += 1
            continue
        if _normalize(tipo) != _normalize(TIPO_COMISSAO_FATURAMENTO):
            skipped_tipo += 1
            continue

        faturados.append(atrib)

    if skipped_dt > 0:
        result.warnings.append(
            f"{skipped_dt} atribuição(ões) ignorada(s): Dt Emissão vazia."
        )
    if skipped_tipo > 0:
        result.warnings.append(
            f"{skipped_tipo} atribuição(ões) ignorada(s): tipo_comissao ≠ Faturamento."
        )

    if not faturados:
        result.warnings.append(
            "Nenhuma atribuição com Dt Emissão preenchida e tipo Faturamento."
        )
        return result

    # Processar cada atribuição
    fc_not_found = set()
    for atrib in faturados:
        comissao = calcular_comissao_item(
            atrib, fc_result_set, cs_lookup, decimais,
        )

        if comissao is None:
            # Skipped (CE em cross-selling)
            continue

        # Verificar se FC foi encontrado
        nome = comissao.get("nome", "")
        hierarquia_key = comissao.get("hierarquia_key", "")
        if comissao["fc_final"] == 0.0 and comissao["fc_modo"] == "N/A":
            fc_key = f"{nome}|{hierarquia_key}"
            if fc_key not in fc_not_found:
                fc_not_found.add(fc_key)
                result.warnings.append(
                    f"FC não encontrado para '{nome}' na hierarquia "
                    f"'{hierarquia_key}'. Usando FC=0.0 (comissão será zero)."
                )

        result.comissoes.append(comissao)

    # Gerar linhas de cross-selling para CE
    cs_lines = _gerar_linhas_cross_selling(atribs_list, cs_lookup, decimais)
    result.comissoes.extend(cs_lines)

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

def _run_tests() -> None:
    """Suite de testes integrada."""
    import sys

    passed = 0
    failed = 0
    total = 0

    def _assert(cond: bool, msg: str):
        nonlocal passed, failed, total
        total += 1
        if cond:
            passed += 1
            print(f"  ✓ Test {total}: {msg}")
        else:
            failed += 1
            print(f"  ✗ Test {total}: FAILED — {msg}")

    print("=" * 60)
    print("  COMISSÃO POR FATURAMENTO — Auto-testes")
    print("=" * 60)

    # ── Helpers ──
    print("\n── Helpers ──")
    _assert(_safe_float(None) == 0.0, "safe_float None → 0")
    _assert(_safe_float("abc") == 0.0, "safe_float invalid → 0")
    _assert(_safe_float(100.5) == 100.5, "safe_float normal")
    _assert(_safe_float(float("nan")) == 0.0, "safe_float NaN → 0")

    _assert(_safe_str(None) == "", "safe_str None → empty")
    _assert(_safe_str("nan") == "", "safe_str nan → empty")
    _assert(_safe_str(" Hello ") == "Hello", "safe_str strip")

    _assert(_normalize("  Hidrologia  ") == "hidrologia", "normalize")
    _assert(_round_currency(1.23456, 2) == 1.23, "round 2 dec")

    # ── CS Lookup ──
    print("\n── CS Lookup ──")
    _assert(_build_cs_lookup(None) == {}, "cs_lookup None → empty")
    _assert(_build_cs_lookup([]) == {}, "cs_lookup empty → empty")

    # Usando dicts
    cases_dict = [
        {"processo": "P001", "consultor": "Mateus", "taxa_cross_selling_pct": 1.5,
         "cargo": "CE", "linha_item": "Hidro"},
    ]
    lookup = _build_cs_lookup(cases_dict)
    _assert("P001" in lookup, "cs_lookup dict → found")
    _assert(lookup["P001"]["taxa_pct"] == 1.5, "cs_lookup dict → taxa")

    # Usando dataclass-like
    from types import SimpleNamespace
    cs_obj = SimpleNamespace(
        processo="P002", consultor="André", taxa_cross_selling_pct=1.0,
        cargo="Consultor Externo", linha_item="Ambiental",
    )
    lookup2 = _build_cs_lookup([cs_obj])
    _assert("P002" in lookup2, "cs_lookup obj → found")
    _assert(lookup2["P002"]["consultor"] == "André", "cs_lookup obj → consultor")

    _assert(_is_cs_consultor("Mateus", {"consultor": "Mateus"}), "is_cs_consultor match")
    _assert(_is_cs_consultor("MATEUS", {"consultor": "mateus"}), "is_cs_consultor case insensitive")
    _assert(not _is_cs_consultor("João", {"consultor": "Mateus"}), "is_cs_consultor no match")

    # ── Mock FC ResultSet ──
    print("\n── Mock FC ──")

    class MockFCResult:
        def __init__(self, rampa, final, modo):
            self.fc_rampa = rampa
            self.fc_final = final
            self.modo = modo

    class MockFCResultSet:
        def __init__(self, data):
            self._data = data  # {(nome, hierarquia_key): MockFCResult}

        def get_fc(self, nome, hierarquia_key):
            r = self._data.get((nome, hierarquia_key))
            return r.fc_final if r else None

        def get_result(self, nome, hierarquia_key):
            return self._data.get((nome, hierarquia_key))

    # Hierarquia keys para teste (simula _build_hierarchy_key)
    hk_hidro = _build_hierarchy_key("Hidrologia", "Equipamentos", "Bombas", "Produto", "QED", "")
    hk_ambiental = _build_hierarchy_key("Ambiental", "Equipamentos", "Bombas", "Produto", "QED", "")
    hk_inexistente = _build_hierarchy_key("Inexistente", "", "", "", "", "")

    fc_data = {
        ("Dener Martins", hk_hidro): MockFCResult(0.8, 0.5, "ESCADA"),
        ("Ana Silva", hk_hidro): MockFCResult(0.9, 0.5, "ESCADA"),
        ("Dener Martins", hk_ambiental): MockFCResult(1.0, 1.0, "ESCADA"),
    }
    mock_fc = MockFCResultSet(fc_data)
    _assert(mock_fc.get_fc("Dener Martins", hk_hidro) == 0.5, "mock fc found")
    _assert(mock_fc.get_fc("Inexistente", "X") is None, "mock fc not found")

    # ── calcular_comissao_item ──
    print("\n── calcular_comissao_item ──")

    atrib_normal = {
        FIELD_NOME: "Dener Martins",
        FIELD_CARGO: "Consultor Interno",
        FIELD_TIPO_CARGO: "Operacional",
        FIELD_TIPO_COMISSAO: "Faturamento",
        FIELD_FATIA_CARGO_PCT: 25.0,
        FIELD_TAXA_RATEIO_PCT: 8.0,
        COL_PROCESSO: "P100",
        COL_NF: "NF001",
        COL_VALOR_REALIZADO: 100000.0,
        COL_LINHA: "Hidrologia",
        COL_GRUPO: "Equipamentos",
        COL_SUBGRUPO: "Bombas",
        COL_TIPO_MERCADORIA: "Produto",
        COL_FABRICANTE: "QED",
        COL_APLICACAO: "",
        COL_STATUS: "FATURADO",
        COL_CLIENTE: "C001",
        COL_NOME_CLIENTE: "Cliente Teste",
    }

    # Sem cross-selling
    c1 = calcular_comissao_item(atrib_normal, mock_fc, {}, 2)
    _assert(c1 is not None, "item normal retornado")
    # Potencial = 100000 × 0.08 × 0.25 × 1.0 = 2000
    _assert(c1["comissao_potencial"] == 2000.0, "potencial = 2000")
    # Final = 2000 × 0.5 (FC escada) = 1000
    _assert(c1["comissao_final"] == 1000.0, "final = 1000")
    _assert(c1["fc_final"] == 0.5, "fc_final = 0.5")
    _assert(c1["fc_rampa"] == 0.8, "fc_rampa = 0.8")
    _assert(c1["fc_modo"] == "ESCADA", "fc_modo = ESCADA")
    _assert(c1["observacao"] == "", "observacao vazia")
    _assert(c1["cross_selling_decision"] is None, "no CS decision")
    _assert(c1["taxa_rateio_pct"] == 8.0, "taxa_rateio_pct = 8.0")
    _assert(c1["hierarquia_key"] == hk_hidro, "hierarquia_key correta")
    _assert(c1["aplicacao"] == "", "aplicacao vazia")

    # Sem FC (resultado None → FC=0)
    atrib_no_fc = {**atrib_normal, COL_LINHA: "Inexistente"}
    c3 = calcular_comissao_item(atrib_no_fc, mock_fc, {}, 2)
    _assert(c3["fc_final"] == 0.0, "FC not found → 0.0")
    _assert(c3["comissao_final"] == 0.0, "no FC → comissao 0")

    # Sem fc_result_set (None)
    c4 = calcular_comissao_item(atrib_normal, None, {}, 2)
    _assert(c4["fc_final"] == 0.0, "fc_result_set None → FC 0")
    _assert(c4["comissao_potencial"] == 2000.0, "potencial calculado mesmo sem FC")

    # ── Cross-selling Opção A ──
    print("\n── Cross-selling Opção A ──")
    # cs_lookup com opção embutida (nova assinatura — option por entrada)
    cs_lookup_a = {
        "P100": {"consultor": "Mateus Machado", "taxa_pct": 1.5, "cargo": "CE",
                 "linha_item": "Hidro", "option": "A"},
    }
    cs_lookup_b = {
        "P100": {"consultor": "Mateus Machado", "taxa_pct": 1.5, "cargo": "CE",
                 "linha_item": "Hidro", "option": "B"},
    }
    # Colaborador normal com CS Opção A: taxa reduzida
    c5 = calcular_comissao_item(atrib_normal, mock_fc, cs_lookup_a, 2)
    _assert(c5 is not None, "normal com CS-A retornado")
    # Taxa = 8.0 - 1.5 = 6.5 → 0.065
    _assert(abs(c5["taxa_rateio_pct"] - 6.5) < 0.001, "CS-A taxa reduzida a 6.5%")
    # Potencial = 100000 × 0.065 × 0.25 × 1.0 = 1625
    _assert(c5["comissao_potencial"] == 1625.0, "CS-A potencial = 1625")
    _assert(c5["cross_selling_decision"] == "A", "CS decision = A")

    # CE deve ser excluído
    atrib_ce = {**atrib_normal, FIELD_NOME: "Mateus Machado"}
    c6 = calcular_comissao_item(atrib_ce, mock_fc, cs_lookup_a, 2)
    _assert(c6 is None, "CE excluído do rateio normal (Opção A)")

    # ── Cross-selling Opção B ──
    print("\n── Cross-selling Opção B ──")
    c7 = calcular_comissao_item(atrib_normal, mock_fc, cs_lookup_b, 2)
    _assert(c7 is not None, "normal com CS-B retornado")
    _assert(c7["taxa_rateio_pct"] == 8.0, "CS-B taxa inalterada")
    _assert(c7["comissao_potencial"] == 2000.0, "CS-B potencial = 2000")
    _assert(c7["cross_selling_decision"] == "B", "CS decision = B")

    c8 = calcular_comissao_item(atrib_ce, mock_fc, cs_lookup_b, 2)
    _assert(c8 is None, "CE excluído do rateio normal (Opção B)")

    # ── Valor zero ──
    print("\n── Valor zero ──")
    atrib_zero = {**atrib_normal, COL_VALOR_REALIZADO: 0.0}
    c9 = calcular_comissao_item(atrib_zero, mock_fc, {}, 2)
    _assert(c9["comissao_potencial"] == 0.0, "valor=0 → potencial=0")
    _assert(c9["comissao_final"] == 0.0, "valor=0 → final=0")

    # ── _gerar_linhas_cross_selling ──
    print("\n── _gerar_linhas_cross_selling ──")

    atribs_cs_test = [
        {**atrib_normal, "idx_ac": 0, COL_PROCESSO: "P100"},
        {**atrib_normal, "idx_ac": 0, COL_PROCESSO: "P100",
         FIELD_NOME: "Ana Silva"},  # Mesmo item, outro colaborador
        {**atrib_normal, "idx_ac": 1, COL_PROCESSO: "P100",
         COL_VALOR_REALIZADO: 50000.0},  # Segundo item
        {**atrib_normal, "idx_ac": 2, COL_PROCESSO: "P200",
         COL_STATUS: "Em Andamento"},  # Não faturado
    ]

    cs_lines = _gerar_linhas_cross_selling(atribs_cs_test, cs_lookup_a, 2)
    _assert(len(cs_lines) == 2, "CS: 2 itens faturados no P100")
    _assert(cs_lines[0]["nome"] == "Mateus Machado", "CS: nome consultor")
    _assert(cs_lines[0]["observacao"] == OBS_CROSS_SELLING, "CS: observacao")
    _assert(cs_lines[0]["fc_final"] == 1.0, "CS: FC=1.0")
    _assert(cs_lines[0]["fatia_cargo_pct"] == 100.0, "CS: fatia=100%")
    # Item 1: 100000 × 0.015 = 1500
    _assert(cs_lines[0]["comissao_final"] == 1500.0, "CS item1: 100000×1.5% = 1500")
    # Item 2: 50000 × 0.015 = 750
    _assert(cs_lines[1]["comissao_final"] == 750.0, "CS item2: 50000×1.5% = 750")

    # Deduplicação: mesmo idx_ac não gera linha duplicada
    _assert(cs_lines[0]["valor_item"] == 100000.0, "CS dedup: primeiro item valor")

    # Sem CS
    _assert(_gerar_linhas_cross_selling(atribs_cs_test, {}, 2) == [], "sem CS → vazio")

    # ── execute() completo ──
    print("\n── execute() ──")

    # Cenário básico: 3 atribuições faturadas + 1 não faturada + 1 recebimento
    atribs_exec = [
        # Faturado, Faturamento
        {**atrib_normal, "idx_ac": 0, COL_PROCESSO: "P100",
         FIELD_NOME: "Dener Martins", COL_VALOR_REALIZADO: 100000.0},
        # Mesmo item, gestão
        {**atrib_normal, "idx_ac": 0, COL_PROCESSO: "P100",
         FIELD_NOME: "Ana Silva", FIELD_CARGO: "Coordenador",
         FIELD_TIPO_CARGO: "Gestão", FIELD_FATIA_CARGO_PCT: 15.0,
         COL_VALOR_REALIZADO: 100000.0},
        # Outro item faturado
        {**atrib_normal, "idx_ac": 1, COL_PROCESSO: "P100",
         FIELD_NOME: "Dener Martins", COL_LINHA: "Ambiental",
         COL_VALOR_REALIZADO: 50000.0},
        # Não faturado → ignorado
        {**atrib_normal, "idx_ac": 2, COL_PROCESSO: "P200",
         COL_STATUS: "Em Andamento"},
        # Tipo Recebimento → ignorado
        {**atrib_normal, "idx_ac": 3, COL_PROCESSO: "P300",
         FIELD_TIPO_COMISSAO: "Recebimento"},
    ]

    res = execute(atribs_exec, mock_fc)
    _assert(res.ok, "execute ok")
    _assert(len(res.comissoes) == 3, "3 comissões geradas")

    # Dener Hidrologia: 100000 × 0.08 × 0.25 × 1.0 × 0.5 = 1000
    dener_hidro = [c for c in res.comissoes
                   if c["nome"] == "Dener Martins" and c["linha"] == "Hidrologia"]
    _assert(len(dener_hidro) == 1, "Dener Hidrologia encontrado")
    _assert(dener_hidro[0]["comissao_final"] == 1000.0, "Dener Hidro: final=1000")

    # Ana Hidrologia: 100000 × 0.08 × 0.15 × 1.0 × 0.5 = 600
    ana_hidro = [c for c in res.comissoes
                 if c["nome"] == "Ana Silva" and c["linha"] == "Hidrologia"]
    _assert(len(ana_hidro) == 1, "Ana Hidrologia encontrada")
    _assert(ana_hidro[0]["comissao_final"] == 600.0, "Ana Hidro: final=600")

    # Dener Ambiental: 50000 × 0.08 × 0.25 × 1.0 × 1.0 = 1000
    dener_amb = [c for c in res.comissoes
                 if c["nome"] == "Dener Martins" and c["linha"] == "Ambiental"]
    _assert(len(dener_amb) == 1, "Dener Ambiental encontrado")
    _assert(dener_amb[0]["comissao_final"] == 1000.0, "Dener Amb: final=1000")
    _assert(dener_amb[0]["fc_final"] == 1.0, "Dener Amb: fc=1.0")

    # Total
    _assert(res.total_comissoes == 2600.0, "total final = 2600")

    # ── execute() com cross-selling ──
    print("\n── execute() com cross-selling ──")

    cs_cases_exec = [
        SimpleNamespace(
            processo="P100", consultor="Mateus Machado",
            taxa_cross_selling_pct=1.5, cargo="Consultor Externo",
            linha_item="Hidrologia", itens_afetados=2,
        ),
    ]

    # Opção A
    res_a = execute(atribs_exec, mock_fc, cs_cases_exec, "A")
    _assert(res_a.ok, "execute CS-A ok")
    cs_a_lines = [c for c in res_a.comissoes if c["observacao"] == OBS_CROSS_SELLING]
    normal_a_lines = [c for c in res_a.comissoes if c["observacao"] != OBS_CROSS_SELLING]
    # 2 itens faturados no P100 → 2 linhas CE
    _assert(len(cs_a_lines) == 2, "CS-A: 2 linhas CE")
    # CE item 1 (100000 × 1.5%) = 1500
    _assert(cs_a_lines[0]["comissao_final"] == 1500.0, "CS-A CE item1 = 1500")
    # Normal: taxa reduzida de 8% para 6.5%
    dener_a = [c for c in normal_a_lines
               if c["nome"] == "Dener Martins" and c["linha"] == "Hidrologia"]
    _assert(len(dener_a) == 1, "CS-A Dener Hidro found")
    _assert(abs(dener_a[0]["taxa_rateio_pct"] - 6.5) < 0.001, "CS-A taxa Dener = 6.5%")
    # Potencial = 100000 × 0.065 × 0.25 × 1.0 = 1625
    _assert(dener_a[0]["comissao_potencial"] == 1625.0, "CS-A Dener potencial = 1625")

    # Opção B
    res_b = execute(atribs_exec, mock_fc, cs_cases_exec, "B")
    cs_b_lines = [c for c in res_b.comissoes if c["observacao"] == OBS_CROSS_SELLING]
    normal_b_lines = [c for c in res_b.comissoes if c["observacao"] != OBS_CROSS_SELLING]
    _assert(len(cs_b_lines) == 2, "CS-B: 2 linhas CE")
    dener_b = [c for c in normal_b_lines
               if c["nome"] == "Dener Martins" and c["linha"] == "Hidrologia"]
    _assert(dener_b[0]["taxa_rateio_pct"] == 8.0, "CS-B taxa inalterada")
    _assert(dener_b[0]["comissao_potencial"] == 2000.0, "CS-B Dener potencial = 2000")

    # ── ComissaoFaturamentoResult methods ──
    print("\n── ComissaoFaturamentoResult ──")

    # to_dataframe
    df_res = res.to_dataframe()
    _assert(isinstance(df_res, pd.DataFrame), "to_dataframe retorna DF")
    _assert(len(df_res) == 3, "to_dataframe 3 rows")

    # consolidar_por_colaborador
    df_consol = res.consolidar_por_colaborador()
    _assert(isinstance(df_consol, pd.DataFrame), "consolidar retorna DF")
    _assert(len(df_consol) == 2, "2 colaboradores consolidados")
    # Dener total: 1000 + 1000 = 2000
    dener_row = df_consol[df_consol["nome"] == "Dener Martins"].iloc[0]
    _assert(dener_row["comissao_final"] == 2000.0, "Dener total = 2000")
    _assert(dener_row["num_itens"] == 2, "Dener 2 itens")

    # get_comissoes_colaborador
    dener_comissoes = res.get_comissoes_colaborador("Dener Martins")
    _assert(len(dener_comissoes) == 2, "get_comissoes Dener: 2 itens")

    # summary
    s = res.summary()
    _assert("COMISSÃO POR FATURAMENTO" in s, "summary tem título")
    _assert("2.600,00" in s or "2,600.00" in s, "summary tem total")

    # Empty result
    empty_res = ComissaoFaturamentoResult()
    _assert(empty_res.ok, "empty ok")
    _assert(empty_res.total_comissoes == 0.0, "empty total = 0")
    df_empty = empty_res.to_dataframe()
    _assert(len(df_empty) == 0, "empty DF vazio")
    df_consol_empty = empty_res.consolidar_por_colaborador()
    _assert(len(df_consol_empty) == 0, "empty consolidado vazio")

    # ── execute com lista vazia ──
    print("\n── Edge cases ──")
    res_empty = execute([], mock_fc)
    _assert(res_empty.ok, "execute vazio ok")
    _assert(len(res_empty.comissoes) == 0, "execute vazio → 0 comissões")

    # execute com DataFrame
    df_input = pd.DataFrame(atribs_exec)
    res_df = execute(df_input, mock_fc)
    _assert(res_df.ok, "execute com DF ok")
    _assert(len(res_df.comissoes) == 3, "execute DF → 3 comissões")

    # execute com opção inválida
    res_inv = execute(atribs_exec, mock_fc, cross_selling_option="X")
    _assert(res_inv.ok, "opção inválida → ok (fallback B)")
    _assert(any("inválida" in w for w in res_inv.warnings), "aviso opção inválida")

    # Todos os itens não faturados
    atribs_nenhum = [{**atrib_normal, COL_STATUS: "Em Andamento"}]
    res_nenhum = execute(atribs_nenhum, mock_fc)
    _assert(len(res_nenhum.comissoes) == 0, "nenhum faturado → 0")

    # ── CS com taxa maior que taxa_rateio (edge case) ──
    print("\n── CS taxa > taxa_rateio ──")
    cs_high = {"P100": {"consultor": "CE", "taxa_pct": 10.0, "cargo": "CE", "linha_item": "H", "option": "A"}}
    atrib_low_taxa = {**atrib_normal, FIELD_TAXA_RATEIO_PCT: 5.0}
    c_high = calcular_comissao_item(atrib_low_taxa, mock_fc, cs_high, 2)
    # max(0, 5-10) = 0 → taxa = 0
    _assert(c_high["taxa_rateio_pct"] == 0.0, "CS taxa > rateio → taxa=0")
    _assert(c_high["comissao_potencial"] == 0.0, "CS taxa > rateio → potencial=0")

    # ── Arredondamento ──
    print("\n── Arredondamento ──")
    atrib_round = {**atrib_normal, COL_VALOR_REALIZADO: 33333.33,
                   FIELD_TAXA_RATEIO_PCT: 7.5, FIELD_FATIA_CARGO_PCT: 12.5}
    c_round = calcular_comissao_item(atrib_round, mock_fc, {}, 2)
    # 33333.33 × 0.075 × 0.125 × 1.0 = 312.4999...
    _assert(c_round["comissao_potencial"] == 312.50, "arredondamento potencial")
    # × 0.5 = 156.25
    _assert(c_round["comissao_final"] == 156.25, "arredondamento final")

    # ═════════════════════════
    print("\n" + "=" * 60)
    print(f"  RESULTADO: {passed}/{total} testes passaram")
    if failed:
        print(f"  ✗ {failed} teste(s) FALHARAM")
    print("=" * 60)
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    import sys
    if "--test" in sys.argv:
        _run_tests()
    else:
        print("Uso: python comissao_faturamento.py --test")
        print("Ou importe como módulo: import scripts.comissao_faturamento as cf")
