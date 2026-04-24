"""
receita/calculadores/mapear_documentos.py — Vínculo docs AF → processos AC.

Regras de vínculo:
- documentos regulares: AF → AC via `Numero NF`
- adiantamentos (`COT`/`ADT`): AF → AC via `Processo`

Em ambos os casos, o identificador é normalizado antes do match:
- comparação exata em caixa alta
- fallback por dígitos sem zeros à esquerda

ATENÇÃO: usar df_ac_full (sem filtro de mês), pois processos históricos
podem ter sido faturados em meses anteriores.

API pública
-----------
executar(df_af, df_ac_full) → MapeamentoResult
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from receita.schemas.calculo import MapeamentoResult

_PREFIXOS_ADIANTAMENTO = ("COT", "ADT")


def _extrair_digitos(texto: str) -> str:
    """Extrai apenas dígitos de uma string."""
    return re.sub(r"\D", "", str(texto))


def _normalizar_digitos(texto: str) -> str:
    """Extrai dígitos e remove zeros à esquerda para comparação estável."""
    digitos = _extrair_digitos(texto)
    if not digitos:
        return ""
    return digitos.lstrip("0") or "0"


def _classificar_tipo(documento: str) -> str:
    """Retorna 'ADIANTAMENTO' ou 'REGULAR' baseado no prefixo do documento."""
    doc_upper = str(documento).strip().upper()
    for prefixo in _PREFIXOS_ADIANTAMENTO:
        if doc_upper.startswith(prefixo):
            return "ADIANTAMENTO"
    return "REGULAR"


def executar(
    df_af: pd.DataFrame,
    df_ac_full: pd.DataFrame,
) -> MapeamentoResult:
    """Vincula documentos AF a processos AC via NF ou Processo.

    Args:
        df_af: DataFrame da Análise Financeira filtrado pelo mês/ano.
            Deve conter "Documento" e alguma coluna de NF (tentativas abaixo).
        df_ac_full: DataFrame da Análise Comercial SEM filtro de mês.
            Deve conter "Processo", "Numero NF" e "Linha".

    Returns:
        MapeamentoResult com df_mapeado enriquecido e lista de não-mapeados.
    """
    warnings: List[str] = []
    docs_nao_mapeados: List[str] = []

    if df_af is None or df_af.empty:
        return MapeamentoResult(
            df_mapeado=pd.DataFrame(),
            docs_nao_mapeados=[],
            warnings=["mapear_documentos: df_af vazio — nenhum documento a mapear."],
        )

    # --- Identificar coluna NF no df_af -----------------------------------------
    nf_col_af: Optional[str] = None
    for candidato in ["Numero NF", "NF", "Num. NF", "Número NF", "NumeroNF", "Documento"]:
        if candidato in df_af.columns:
            nf_col_af = candidato
            break

    # Se o documento AF não tem coluna de NF separada, tentamos extrair do próprio Documento
    usar_documento_como_nf = nf_col_af is None or nf_col_af == "Documento"

    if "Processo" not in df_ac_full.columns:
        warnings.append("mapear_documentos: coluna 'Processo' ausente na AC — mapeamento impossível.")
        return MapeamentoResult(
            df_mapeado=df_af.copy(),
            docs_nao_mapeados=list(df_af.get("Documento", pd.Series(dtype=str)).astype(str)),
            warnings=warnings,
        )

    # --- Construir lookups da AC completa ---------------------------------------
    lookup_nf_exato: Dict[str, Dict[str, str]] = {}
    lookup_nf_digitos: Dict[str, Dict[str, str]] = {}
    lookup_processo_exato: Dict[str, Dict[str, str]] = {}
    lookup_processo_digitos: Dict[str, Dict[str, str]] = {}

    for _, row in df_ac_full.iterrows():
        processo = str(row.get("Processo", "") or "").strip()
        linha = str(row.get("Linha", "") or "").strip()
        if not processo:
            continue

        info = {"processo": processo, "linha": linha}

        processo_upper = processo.upper()
        processo_digitos = _normalizar_digitos(processo)
        lookup_processo_exato.setdefault(processo_upper, info)
        if processo_digitos:
            lookup_processo_digitos.setdefault(processo_digitos, info)

        nf_raw = str(row.get("Numero NF", "") or "").strip()
        if not nf_raw:
            continue
        nf_upper = nf_raw.upper()
        nf_digitos = _normalizar_digitos(nf_raw)
        lookup_nf_exato.setdefault(nf_upper, info)
        if nf_digitos:
            lookup_nf_digitos.setdefault(nf_digitos, info)

    # --- Mapear cada documento AF -----------------------------------------------
    df_resultado = df_af.copy()
    processo_mapeado: List[str] = []
    linha_mapeada: List[str] = []
    tipo_pagamento: List[str] = []
    nf_extraida_lista: List[str] = []

    doc_col = "Documento" if "Documento" in df_af.columns else df_af.columns[0]

    for _, row in df_af.iterrows():
        documento = str(row.get(doc_col, "") or "").strip()
        tipo = _classificar_tipo(documento)

        # Determinar NF para lookup
        if usar_documento_como_nf:
            nf_para_lookup = documento
        else:
            nf_para_lookup = str(row.get(nf_col_af, "") or "").strip()

        chave_upper = nf_para_lookup.upper()
        chave_digitos = _normalizar_digitos(nf_para_lookup)

        if tipo == "ADIANTAMENTO":
            info = lookup_processo_exato.get(chave_upper)
            if info is None and chave_digitos:
                info = lookup_processo_digitos.get(chave_digitos)
        else:
            info = lookup_nf_exato.get(chave_upper)
            if info is None and chave_digitos:
                info = lookup_nf_digitos.get(chave_digitos)

        if info:
            processo_mapeado.append(info["processo"])
            linha_mapeada.append(info["linha"])
            nf_extraida_lista.append(nf_para_lookup)
        else:
            processo_mapeado.append("")
            linha_mapeada.append("")
            nf_extraida_lista.append(nf_para_lookup)
            docs_nao_mapeados.append(documento)

        tipo_pagamento.append(tipo)

    df_resultado["processo_ac"] = processo_mapeado
    df_resultado["linha_negocio"] = linha_mapeada
    df_resultado["tipo_pagamento"] = tipo_pagamento
    df_resultado["nf_extraida"] = nf_extraida_lista

    total = len(df_resultado)
    mapeados = total - len(docs_nao_mapeados)
    warnings.append(
        f"mapear_documentos: {mapeados}/{total} documentos mapeados a processos AC."
    )
    if docs_nao_mapeados:
        warnings.append(
            f"mapear_documentos: {len(docs_nao_mapeados)} documento(s) sem processo AC: "
            f"{docs_nao_mapeados[:5]}{'...' if len(docs_nao_mapeados) > 5 else ''}"
        )

    return MapeamentoResult(
        df_mapeado=df_resultado,
        docs_nao_mapeados=docs_nao_mapeados,
        warnings=warnings,
    )
