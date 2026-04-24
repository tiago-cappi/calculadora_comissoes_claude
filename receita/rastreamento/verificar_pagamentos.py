"""
receita/rastreamento/verificar_pagamentos.py — Verifica pagamentos do Processo Pai.

Um Processo Pai está "completamente pago" quando TODAS as parcelas de TODAS
as NFs vinculadas aos seus processos filho possuem Situação == 1
(Pagamento Recebido) na Análise Financeira.

Regras de parcela
-----------------
A coluna "Documento" da AF pode ter duas formas:
  - "123456"           → pagamento integral (uma única linha)
  - "123456A", "123456B"  → parcelas (letras A, B, C... ao final)

A base numérica é extraída removendo letras do final. Todas as parcelas de
uma mesma base devem ter Situação == 1 para o documento ser considerado pago.

Valores de Situação
-------------------
  1 → Pagamento Recebido (pago)
  0 → Pagamento em Aberto
  2 → Recebimento Parcial

Observação importante sobre df_af_full
---------------------------------------
Para verificar os pagamentos do Processo Pai é necessário usar a AF completa
(sem filtro de Data de Baixa), pois parcelas podem ter sido recebidas em meses
diferentes do mês de apuração. O parâmetro df_af_full deve ser carregado via
`load_analise_financeira_full`.

API pública
-----------
verificar(numero_pc, codigo_cliente, df_af_full, tabela_pc, df_ac_full)
    → Tuple[bool, str]
"""

from __future__ import annotations

import re
from typing import Optional, Set, Tuple

import pandas as pd

from receita.schemas.entrada import ProcessoPedidoTabela

_SITUACAO_PAGO = 1
_SITUACAO_COL = "Situação"
_DOCUMENTO_COL = "Documento"
_NF_COL_AC = "Numero NF"
_PROCESSO_COL_AC = "Processo"


def _extrair_base_numerica(documento: str) -> str:
    """Extrai a parte numérica base de um documento, removendo letra final.

    Exemplos:
        "123456"   → "123456"
        "123456A"  → "123456"
        "123456B"  → "123456"
        "0012345C" → "0012345"
    """
    return re.sub(r"[A-Za-z]+$", "", str(documento).strip())


def _obter_nfs_dos_processos(
    processos: list,
    df_ac_full: pd.DataFrame,
) -> Set[str]:
    """Retorna o conjunto de NFs (Numero NF) vinculadas a uma lista de processos.

    Args:
        processos: Lista de números de processo filho.
        df_ac_full: DataFrame da AC SEM filtro de mês.

    Returns:
        Conjunto de NFs normalizadas (upper, strip).
    """
    if _NF_COL_AC not in df_ac_full.columns or _PROCESSO_COL_AC not in df_ac_full.columns:
        return set()

    processos_upper = {str(p).strip().upper() for p in processos}
    mask = df_ac_full[_PROCESSO_COL_AC].astype(str).str.strip().str.upper().isin(processos_upper)
    nfs_raw = (
        df_ac_full.loc[mask, _NF_COL_AC]
        .dropna()
        .astype(str)
        .str.strip()
        .str.upper()
    )
    return {nf for nf in nfs_raw if nf and nf not in ("NAN", "NONE", "")}


def _obter_bases_documentos_por_nf(
    nfs: Set[str],
    df_af_full: pd.DataFrame,
) -> dict:
    """Retorna mapa {base_numerica: [Situação...]} para todas as NFs fornecidas.

    Vincula documentos da AF às NFs da AC via comparação de dígitos.

    Args:
        nfs: Conjunto de NFs a verificar (já normalizadas, upper).
        df_af_full: DataFrame da AF completo (sem filtro de data).

    Returns:
        Dicionário {base_numerica_documento: [situacao_int, ...]}.
        Cada base agrupa todas as parcelas de um mesmo documento.
    """
    if df_af_full.empty or _DOCUMENTO_COL not in df_af_full.columns:
        return {}

    # Normalizar NFs da AC: extrair apenas dígitos e remover zeros à esquerda
    nfs_digitos = {re.sub(r"\D", "", str(nf)).lstrip("0") for nf in nfs if re.sub(r"\D", "", str(nf))}

    tem_situacao = _SITUACAO_COL in df_af_full.columns

    resultado: dict = {}

    for _, row in df_af_full.iterrows():
        doc_raw = row.get(_DOCUMENTO_COL, "")
        try:
            if pd.isna(doc_raw):
                continue
        except (TypeError, ValueError):
            pass
        documento = str(doc_raw).strip()
        if not documento or documento.upper() in ("NAN", "NONE", ""):
            continue

        base = _extrair_base_numerica(documento)
        base_digitos = re.sub(r"\D", "", base).lstrip("0")

        # Verificar se esta base pertence a alguma NF dos processos do Pai
        if base_digitos not in nfs_digitos:
            continue

        sit_raw = row.get(_SITUACAO_COL, -1) if tem_situacao else -1
        try:
            situacao = -1 if pd.isna(sit_raw) else int(sit_raw)
        except (TypeError, ValueError):
            situacao = -1
        resultado.setdefault(base, []).append(situacao)

    return resultado


def verificar(
    numero_pc: str,
    codigo_cliente: str,
    df_af_full: Optional[pd.DataFrame],
    tabela_pc: Optional[ProcessoPedidoTabela],
    df_ac_full: Optional[pd.DataFrame],
) -> Tuple[bool, str]:
    """Verifica se todas as parcelas de todas as NFs do Processo Pai estão pagas.

    Um Processo Pai está completamente pago quando:
    - Todos os processos filho estão identificados via tabela_pc
    - Todas as NFs dos processos filho existem na AC (Numero NF)
    - Para cada NF, todos os Documentos correspondentes na AF têm Situação == 1

    Args:
        numero_pc: Número do pedido de compra do Processo Pai.
        codigo_cliente: Código do cliente do Processo Pai.
        df_af_full: DataFrame da AF SEM filtro de data (load_analise_financeira_full).
            Se None ou vazio, retorna False com aviso.
        tabela_pc: Tabela "Processo x Pedido de Compra".
            Se None, retorna False com aviso.
        df_ac_full: DataFrame da AC SEM filtro de mês.
            Necessário para obter NFs dos processos filho.

    Returns:
        (bool: todos_pagos, str: mensagem de status ou aviso).
    """
    # Validações de entrada
    if tabela_pc is None:
        return False, "verificar_pagamentos: tabela_pc não disponível — status de pagamento indeterminado."

    if df_af_full is None or df_af_full.empty:
        return False, "verificar_pagamentos: df_af_full vazio — status de pagamento indeterminado."

    if df_ac_full is None or df_ac_full.empty:
        return False, "verificar_pagamentos: df_ac_full vazio — não é possível obter NFs dos processos."

    if _SITUACAO_COL not in df_af_full.columns:
        return False, (
            "verificar_pagamentos: coluna 'Situação' ausente na AF — "
            "não é possível verificar status de pagamento."
        )

    # 1. Obter processos filho do Pai
    processos_filho = tabela_pc.get_processos_do_pai(numero_pc, codigo_cliente)
    if not processos_filho:
        return False, (
            f"verificar_pagamentos: nenhum processo filho encontrado para "
            f"Pai (PC={numero_pc}, CLI={codigo_cliente})."
        )

    # 2. Obter NFs dos processos filho na AC
    nfs = _obter_nfs_dos_processos(processos_filho, df_ac_full)

    # Todo filho precisa ter pelo menos uma NF; sem NF = ainda não faturado
    # → o Pai não pode estar completamente pago.
    if _NF_COL_AC in df_ac_full.columns and _PROCESSO_COL_AC in df_ac_full.columns:
        proc_col = df_ac_full[_PROCESSO_COL_AC].astype(str).str.strip().str.upper()
        nf_col = df_ac_full[_NF_COL_AC].astype(str).str.strip()
        sem_nf = [
            str(p) for p in processos_filho
            if nf_col[proc_col == str(p).strip().upper()]
               .pipe(lambda s: s[~s.str.upper().isin(["NAN", "NONE", ""])])
               .empty
        ]
        if sem_nf:
            return False, (
                f"verificar_pagamentos: processo(s) filho(s) sem NF (não faturados): "
                f"{sem_nf[:5]} — pagamento do Pai incompleto."
            )

    if not nfs:
        return False, (
            f"verificar_pagamentos: nenhuma NF encontrada na AC para os processos "
            f"filho de (PC={numero_pc}, CLI={codigo_cliente})."
        )

    # 3. Obter parcelas da AF por NF e verificar Situação
    bases = _obter_bases_documentos_por_nf(nfs, df_af_full)
    if not bases:
        return False, (
            f"verificar_pagamentos: nenhum documento AF encontrado para as NFs "
            f"{list(nfs)[:3]} — pagamento não confirmado."
        )

    # 4. Verificar se TODAS as parcelas têm Situação == 1
    parcelas_nao_pagas = []
    for base, situacoes in bases.items():
        for sit in situacoes:
            if sit != _SITUACAO_PAGO:
                parcelas_nao_pagas.append(f"{base}(Sit={sit})")

    if parcelas_nao_pagas:
        amostra = parcelas_nao_pagas[:5]
        sufixo = "..." if len(parcelas_nao_pagas) > 5 else ""
        return False, (
            f"verificar_pagamentos: {len(parcelas_nao_pagas)} parcela(s) não pagas — "
            f"{amostra}{sufixo}"
        )

    total_parcelas = sum(len(v) for v in bases.values())
    return True, (
        f"verificar_pagamentos: {total_parcelas} parcela(s) de {len(bases)} documento(s) "
        f"confirmadas como pagas (Situação=1)."
    )
