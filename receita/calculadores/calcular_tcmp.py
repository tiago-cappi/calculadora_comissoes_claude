"""
receita/calculadores/calcular_tcmp.py — TCMP por processo (Taxa de Comissão Média Ponderada).

TCMP = Σ(taxa_item × valor_item) / Σ(valor_item)

A taxa de cada item é determinada pela regra de comissão mais específica
que cobre a hierarquia do item (busca do nível 6 ao nível 1).

API pública
-----------
executar(df_ac_full, atribuicao_result) → TCMPResult
"""

from __future__ import annotations

import unicodedata
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from receita.schemas.calculo import AtribuicaoResult, ElegivelGL, TCMPResult

_HIERARQUIA_COLS = ["Linha", "Grupo", "Subgrupo", "Tipo de Mercadoria", "Fabricante", "Aplicação Mat./Serv."]


def _normalizar(texto: str) -> str:
    """Normaliza texto: maiúsculas sem acentos."""
    return (
        unicodedata.normalize("NFKD", str(texto).upper())
        .encode("ascii", "ignore")
        .decode()
        .strip()
    )


def _safe_to_numeric(series: pd.Series) -> pd.Series:
    """Converte números em formato BR/Excel para float."""
    if pd.api.types.is_numeric_dtype(series):
        return series.fillna(0.0)
    s = series.astype(str).str.strip()
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _hierarquia_item(row: pd.Series) -> Tuple[str, ...]:
    """Extrai a tupla de hierarquia de uma linha da AC (até 6 campos)."""
    return tuple(
        str(row.get(col, "") or "").strip()
        for col in _HIERARQUIA_COLS
    )


def _find_best_rule(
    hierarquia: Tuple[str, ...],
    elegiveis: List[ElegivelGL],
) -> Optional[ElegivelGL]:
    """Retorna o ElegivelGL com a regra mais específica para a hierarquia.

    Compara a hierarquia do item com a hierarquia de cada ElegivelGL.
    A especificidade é o número de campos preenchidos na regra.
    Prioriza a regra com mais campos preenchidos que faz match completo.

    Args:
        hierarquia: Tupla de 6 strings do item da AC.
        elegiveis: Lista de GLs elegíveis para a linha do item.

    Returns:
        ElegivelGL mais específico que faz match, ou o menos específico
        (catch-all) se nenhum match parcial for encontrado.
    """
    candidatos_com_match: List[ElegivelGL] = []
    catch_all: Optional[ElegivelGL] = None

    for gl in elegiveis:
        espec = gl.especificidade
        if espec == 0:
            # Regra catch-all (sem restrição de hierarquia)
            if catch_all is None or gl.taxa_efetiva > catch_all.taxa_efetiva:
                catch_all = gl
            continue
        # Verificar se os primeiros `espec` campos batem
        match = True
        for i in range(espec):
            h_regra = _normalizar(gl.hierarquia[i]) if i < len(gl.hierarquia) else ""
            h_item = _normalizar(hierarquia[i]) if i < len(hierarquia) else ""
            if h_regra and h_item != h_regra:
                match = False
                break
        if match:
            candidatos_com_match.append(gl)

    if candidatos_com_match:
        # Retornar o de maior especificidade (mais restrito)
        return max(candidatos_com_match, key=lambda g: g.especificidade)

    return catch_all


def executar(
    df_ac_full: pd.DataFrame,
    atribuicao_result: AtribuicaoResult,
) -> TCMPResult:
    """Calcula a TCMP para cada processo da AC.

    Para cada processo, itera sobre todos os seus itens e acumula
    soma ponderada de taxa × valor, dividindo pelo valor total.

    Args:
        df_ac_full: DataFrame da Análise Comercial SEM filtro de mês.
            Deve conter "Processo", "Valor Realizado" e colunas de hierarquia.
        atribuicao_result: Resultado de atribuir_gls com `por_linha`.

    Returns:
        TCMPResult com tcmp_por_processo {processo: float}.
    """
    warnings: List[str] = []
    tcmp_por_processo: Dict[str, float] = {}
    detalhes: Dict[str, List[Dict]] = {}

    if df_ac_full is None or df_ac_full.empty:
        return TCMPResult(tcmp_por_processo={}, warnings=["calcular_tcmp: df_ac_full vazio."])

    if "Processo" not in df_ac_full.columns:
        return TCMPResult(
            tcmp_por_processo={},
            warnings=["calcular_tcmp: coluna 'Processo' ausente na AC."],
        )

    # Garantir pesos numéricos. Para processos ainda não faturados, o
    # adiantamento ocorre antes do Valor Realizado existir; nesse caso o
    # TCMP deve usar o Valor Orçado como peso de fallback.
    df = df_ac_full.copy()
    if "Valor Realizado" in df.columns:
        df["Valor Realizado"] = _safe_to_numeric(df["Valor Realizado"])
    else:
        df["Valor Realizado"] = 0.0
        warnings.append("calcular_tcmp: coluna 'Valor Realizado' ausente — TCMP será 0.")
    if "Valor Orçado" in df.columns:
        df["Valor Orçado"] = _safe_to_numeric(df["Valor Orçado"])
    else:
        df["Valor Orçado"] = 0.0

    processos_sem_taxa = set()

    for processo, grupo in df.groupby("Processo", sort=False):
        soma_taxa_valor = 0.0
        soma_valor = 0.0
        itens_detalhe: List[Dict] = []

        for _, row in grupo.iterrows():
            valor_realizado = float(row.get("Valor Realizado", 0.0) or 0.0)
            valor_orcado = float(row.get("Valor Orçado", 0.0) or 0.0)
            valor = valor_realizado if valor_realizado > 0 else valor_orcado
            if valor == 0.0:
                continue

            linha = str(row.get("Linha", "") or "").strip()
            linha_norm = _normalizar(linha)
            elegiveis = atribuicao_result.por_linha.get(linha_norm, [])

            hierarquia = _hierarquia_item(row)
            gl_melhor = _find_best_rule(hierarquia, elegiveis)

            if gl_melhor is None:
                processos_sem_taxa.add(str(processo))
                continue

            taxa = gl_melhor.taxa_efetiva
            contribuicao = taxa * valor
            soma_taxa_valor += contribuicao
            soma_valor += valor
            itens_detalhe.append({
                "hierarquia": hierarquia,
                "taxa": taxa,
                "valor_item": valor,
                "contribuicao": contribuicao,
                "gl_nome": gl_melhor.nome,
                "gl_cargo": gl_melhor.cargo,
                "gl_hierarquia": gl_melhor.hierarquia,
                "fatia_cargo_pct": gl_melhor.fatia_cargo_pct,
                "taxa_rateio_maximo_pct": gl_melhor.taxa_rateio_maximo_pct,
            })

        tcmp = soma_taxa_valor / soma_valor if soma_valor > 0 else 0.0
        tcmp_por_processo[str(processo)] = tcmp
        if itens_detalhe:
            detalhes[str(processo)] = itens_detalhe

    if processos_sem_taxa:
        warnings.append(
            f"calcular_tcmp: {len(processos_sem_taxa)} processo(s) com itens sem taxa GL: "
            f"{sorted(processos_sem_taxa)[:5]}{'...' if len(processos_sem_taxa) > 5 else ''}"
        )

    warnings.append(f"calcular_tcmp: TCMP calculado para {len(tcmp_por_processo)} processo(s).")

    return TCMPResult(tcmp_por_processo=tcmp_por_processo, detalhes=detalhes, warnings=warnings)
