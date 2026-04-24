"""
receita/calculadores/atribuir_gls.py — Atribuição de GLs elegíveis por Linha.

Determina quais Gerentes de Linha são elegíveis para comissão de Recebimento,
com base nas regras ativas da `config_comissao` (tipo_comissao == "Recebimento").

Para cada regra ativa:
  taxa_efetiva = fatia_cargo × taxa_rateio_maximo_pct / 100

API pública
-----------
executar(config_comissao, colaboradores, cargos) → AtribuicaoResult
"""

from __future__ import annotations

import unicodedata
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from receita.schemas.calculo import AtribuicaoResult, ElegivelGL


def _normalizar(texto: str) -> str:
    """Normaliza texto: maiúsculas sem acentos para comparações."""
    return (
        unicodedata.normalize("NFKD", str(texto).upper())
        .encode("ascii", "ignore")
        .decode()
        .strip()
    )


def executar(
    config_comissao: List[Dict[str, Any]],
    colaboradores: List[Dict[str, Any]],
    cargos: List[Dict[str, Any]],
) -> AtribuicaoResult:
    """Atribui GLs elegíveis para comissão de Recebimento por Linha.

    Filtra `config_comissao` por `tipo_comissao == "Recebimento"` e regras
    ativas (`ativo == True`). Para cada regra, localiza o colaborador e
    calcula a taxa efetiva.

    Args:
        config_comissao: Lista de dicionários com regras de comissão do Supabase.
            Campos relevantes: nome_colaborador, cargo, linha, hierarquia_1..6,
            tipo_comissao, ativo, fatia_cargo_pct, taxa_rateio_maximo_pct.
        colaboradores: Lista de dicionários de colaboradores do Supabase.
            Campos: nome, cargo.
        cargos: Lista de dicionários de cargos do Supabase.
            (Reservado para uso futuro — validação de cargos ativos.)

    Returns:
        AtribuicaoResult com:
            - elegiveis: todos os ElegivelGL encontrados
            - por_linha: {linha_normalizada: [ElegivelGL, ...]}
            - warnings: avisos não-críticos
    """
    warnings: List[str] = []
    elegiveis: List[ElegivelGL] = []

    tipo_comissao_por_cargo: Dict[str, str] = {}
    for cargo_cfg in cargos:
        nome_cargo = str(cargo_cfg.get("nome_cargo", "")).strip()
        tipo_comissao = str(
            cargo_cfg.get("tipo_comissao", cargo_cfg.get("TIPO_COMISSAO", ""))
        ).strip()
        if nome_cargo and tipo_comissao:
            tipo_comissao_por_cargo[_normalizar(nome_cargo)] = tipo_comissao

    # Indexar colaboradores por nome normalizado → cargo
    cargo_por_colaborador: Dict[str, str] = {}
    for col in colaboradores:
        nome = str(col.get("nome", col.get("nome_colaborador", ""))).strip()
        cargo = str(col.get("cargo", "")).strip()
        if nome and cargo:
            cargo_por_colaborador[_normalizar(nome)] = cargo

    # Filtrar regras de Recebimento ativas
    regras_recebimento = []
    for regra in config_comissao:
        if not bool(regra.get("ativo", True)):
            continue
        cargo_regra = str(regra.get("cargo", "")).strip()
        tipo_regra = str(regra.get("tipo_comissao", "")).strip()
        if not tipo_regra and cargo_regra:
            tipo_regra = tipo_comissao_por_cargo.get(_normalizar(cargo_regra), "")
        if tipo_regra.upper() == "RECEBIMENTO":
            regras_recebimento.append(regra)

    if not regras_recebimento:
        warnings.append("atribuir_gls: nenhuma regra de Recebimento ativa encontrada.")
        return AtribuicaoResult(elegiveis=[], por_linha={}, warnings=warnings)

    for regra in regras_recebimento:
        nome = str(regra.get("nome_colaborador", regra.get("colaborador", ""))).strip()
        cargo = str(regra.get("cargo", "")).strip()
        linha = str(regra.get("linha", "")).strip()

        if not nome or not linha:
            warnings.append(
                f"atribuir_gls: regra sem nome_colaborador ou linha ignorada: {regra}"
            )
            continue

        # Resolver cargo: da regra ou do cadastro do colaborador
        if not cargo:
            cargo = cargo_por_colaborador.get(_normalizar(nome), "")

        # Calcular taxa efetiva
        try:
            fatia_pct = float(regra.get("fatia_cargo_pct", regra.get("fatia_cargo", 100)))
            taxa_max = float(regra.get("taxa_rateio_maximo_pct", 0))
        except (TypeError, ValueError):
            warnings.append(f"atribuir_gls: falha ao parsear taxas para '{nome}' — ignorando.")
            continue

        taxa_efetiva = (fatia_pct / 100.0) * taxa_max / 100.0

        # Construir hierarquia (até 6 níveis)
        hierarquia_campos = [
            str(regra.get(f"hierarquia_{i}", "") or "").strip()
            for i in range(1, 7)
        ]
        if not any(hierarquia_campos):
            hierarquia_campos = [
                str(regra.get("linha", "") or "").strip(),
                str(regra.get("grupo", "") or "").strip(),
                str(regra.get("subgrupo", "") or "").strip(),
                str(regra.get("tipo_mercadoria", "") or "").strip(),
                str(regra.get("fabricante", "") or "").strip(),
                str(regra.get("aplicacao", "") or "").strip(),
            ]
        hierarquia: Tuple[str, ...] = tuple(hierarquia_campos)
        especificidade = sum(1 for h in hierarquia if h)

        gl = ElegivelGL(
            nome=nome,
            cargo=cargo,
            linha=linha,
            hierarquia=hierarquia,
            taxa_efetiva=taxa_efetiva,
            especificidade=especificidade,
            fatia_cargo_pct=fatia_pct,
            taxa_rateio_maximo_pct=taxa_max,
        )
        elegiveis.append(gl)

    # Agrupar por linha normalizada
    por_linha: Dict[str, List[ElegivelGL]] = defaultdict(list)
    for gl in elegiveis:
        por_linha[_normalizar(gl.linha)].append(gl)

    warnings.append(
        f"atribuir_gls: {len(elegiveis)} GL(s) elegíveis em "
        f"{len(por_linha)} linha(s) de Recebimento."
    )

    return AtribuicaoResult(
        elegiveis=elegiveis,
        por_linha=dict(por_linha),
        warnings=warnings,
    )
