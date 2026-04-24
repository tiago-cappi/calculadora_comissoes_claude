"""
receita/schemas/calculo.py — Schemas de resultados de cálculo.

Define contratos de dados produzidos por cada calculador:
- ElegivelGL / AtribuicaoResult — saída de atribuir_gls.py
- MapeamentoResult            — saída de mapear_documentos.py
- TCMPResult                  — saída de calcular_tcmp.py
- FCMPProcesso / FCMPResult   — saída de calcular_fcmp.py
- ComissaoItem / ComissaoResult — saída de calcular_comissao.py
- ReconciliacaoItem / ReconciliacaoResult — saída de calcular_reconciliacao.py
- EstornoItem / EstornosResult — saída de calcular_estornos.py
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# ---------------------------------------------------------------------------
# Atribuição de GLs
# ---------------------------------------------------------------------------


@dataclass
class ElegivelGL:
    """Gerente de Linha elegível para comissionar uma linha de negócio.

    Representa uma regra ativa da config_comissao onde
    tipo_comissao == "Recebimento".

    Attributes:
        nome: Nome canônico do colaborador.
        cargo: Cargo (ex: "Gerente Linha").
        linha: Linha de negócio (ex: "Recursos Hídricos").
        hierarquia: Tupla de até 6 níveis (linha, grupo, subgrupo,
            tipo_mercadoria, fabricante, aplicacao).  Campos ausentes = "".
        taxa_efetiva: fatia_cargo × taxa_rateio_maximo_pct / 100.
        especificidade: Número de campos de hierarquia preenchidos (0–6).
        fatia_cargo_pct: Fatia do cargo aplicada (0–100, crua da config).
        taxa_rateio_maximo_pct: Taxa máxima de rateio do cargo (0–100, crua).
    """

    nome: str
    cargo: str
    linha: str
    hierarquia: Tuple[str, ...]
    taxa_efetiva: float
    especificidade: int
    fatia_cargo_pct: float = 0.0
    taxa_rateio_maximo_pct: float = 0.0


@dataclass
class AtribuicaoResult:
    """Resultado da etapa de atribuição de GLs.

    Attributes:
        elegiveis: Lista de todos os GLs elegíveis encontrados.
        por_linha: Dicionário {linha_normalizada: [ElegivelGL, ...]}.
        warnings: Avisos não-críticos (ex: GL sem regra para determinada linha).
    """

    elegiveis: List[ElegivelGL] = field(default_factory=list)
    por_linha: Dict[str, List[ElegivelGL]] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Mapeamento de Documentos AF → AC
# ---------------------------------------------------------------------------


@dataclass
class MapeamentoResult:
    """Resultado do mapeamento de documentos AF para processos AC.

    Attributes:
        df_mapeado: DataFrame com colunas adicionais `processo_ac`,
            `tipo_pagamento` ("ADIANTAMENTO" ou "REGULAR") e `linha_negocio`.
        docs_nao_mapeados: Lista de documentos AF que não foram vinculados
            a nenhum processo AC.
        warnings: Avisos sobre documentos não mapeados ou ambíguos.
    """

    df_mapeado: Any = None  # pd.DataFrame
    docs_nao_mapeados: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Cálculo TCMP
# ---------------------------------------------------------------------------


@dataclass
class TCMPResult:
    """Resultado do cálculo de TCMP por processo.

    Attributes:
        tcmp_por_processo: Dicionário {numero_processo: float} com o TCMP
            calculado para cada processo da AC.
        detalhes: Dados item a item para auditoria do TCMP.
            Estrutura: {numero_processo: [{hierarquia, taxa, valor_item, contribuicao}, ...]}.
        warnings: Avisos sobre processos sem itens válidos para TCMP.
    """

    tcmp_por_processo: Dict[str, float] = field(default_factory=dict)
    detalhes: Dict[str, List[Dict]] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Cálculo FCMP
# ---------------------------------------------------------------------------


@dataclass
class FCMPProcesso:
    """FCMP calculado para um processo específico de um GL.

    Attributes:
        processo: Número do processo.
        gl_nome: Nome do Gerente de Linha.
        fcmp_rampa: FCMP contínuo: Σ(FC_item × valor_item) / Σ(valor_item).
        fcmp_aplicado: FCMP após aplicação da escada (ou igual ao rampa se
            modo=RAMPA).
        modo: "RAMPA", "ESCADA" ou "PROVISÓRIO" (processo não-FATURADO).
        provisorio: True se o processo não está FATURADO e FCMP = 1.0.
        num_itens: Quantidade de itens da AC usados no cálculo.
        valor_faturado: Σ valor_item do processo.
    """

    processo: str
    gl_nome: str
    fcmp_rampa: float
    fcmp_aplicado: float
    modo: str  # "RAMPA" | "ESCADA" | "PROVISÓRIO"
    provisorio: bool
    num_itens: int = 0
    valor_faturado: float = 0.0


@dataclass
class FCMPResult:
    """Resultado do cálculo de FCMP para todos os processos de um GL.

    Attributes:
        gl_nome: Nome do GL ao qual este resultado pertence.
        fcmp_por_processo: Dicionário {numero_processo: FCMPProcesso}.
        detalhes: Dados adicionais de auditoria (ex: contribuição por item).
            Estrutura: {numero_processo: [{item_idx, fc_item, valor_item, contribuicao}, ...]}.
        warnings: Avisos sobre processos sem itens ou sem regra.
    """

    gl_nome: str = ""
    fcmp_por_processo: Dict[str, FCMPProcesso] = field(default_factory=dict)
    detalhes: Dict[str, List[Dict]] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Comissão por Documento
# ---------------------------------------------------------------------------


@dataclass
class ComissaoItem:
    """Comissão calculada para um documento AF de um GL.

    Attributes:
        gl_nome: Nome do Gerente de Linha.
        processo: Número do processo AC vinculado.
        documento: Código do documento AF.
        nf_extraida: NF usada no mapeamento.
        tipo_pagamento: "ADIANTAMENTO" ou "REGULAR".
        status_processo: Status do processo na AC.
        linha_negocio: Linha de negócio do processo.
        valor_documento: Valor financeiro do documento AF.
        tcmp: TCMP do processo.
        fcmp_rampa: FCMP contínuo do processo para este GL.
        fcmp_aplicado: FCMP real do processo após escada.
        fcmp_considerado: FCMP efetivamente aplicado na competência.
            Na nova regra de recebimento, permanece 1.0 até a reconciliação.
        fcmp_modo: "RAMPA", "ESCADA" ou "PROVISÓRIO".
        provisorio: True se FCMP = 1.0 por status não-faturado.
        comissao_potencial: valor_documento × tcmp (sem FC).
        comissao_base: valor_documento × tcmp.
            Base monetária usada como peso na reconciliação.
        comissao_final: valor_documento × tcmp × fcmp_considerado.
    """

    gl_nome: str
    processo: str
    documento: str
    nf_extraida: str
    tipo_pagamento: str  # "ADIANTAMENTO" | "REGULAR"
    status_processo: str
    linha_negocio: str
    valor_documento: float
    tcmp: float
    fcmp_rampa: float
    fcmp_aplicado: float
    fcmp_modo: str
    provisorio: bool
    comissao_potencial: float
    comissao_final: float
    fcmp_considerado: float = 1.0
    comissao_base: float = 0.0


@dataclass
class ComissaoResult:
    """Resultado do cálculo de comissões por documento.

    Attributes:
        itens: Lista de todos os ComissaoItem calculados.
        total_por_gl: Dicionário {gl_nome: comissao_final_total}.
        warnings: Avisos sobre documentos sem TCMP ou FCMP.
    """

    itens: List[ComissaoItem] = field(default_factory=list)
    total_por_gl: Dict[str, float] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Reconciliação
# ---------------------------------------------------------------------------


@dataclass
class ReconciliacaoItem:
    """Ajuste de reconciliação quando o Processo Pai fecha.

    A reconciliação corrige a diferença entre comissões adiantadas
    (calculadas com FCMP provisório = 1.0) e o FCMP real do processo.

    Attributes:
        gl_nome: Nome do Gerente de Linha.
        numero_pc: Número do pedido de compra do Processo Pai.
        codigo_cliente: Código do cliente.
        processo: Processo filho que gerou a reconciliação.
        comissao_adiantada: Total de comissões pagas com FCMP provisório.
        fcmp_real: FCMP real calculado após faturamento completo.
        ajuste: comissao_adiantada × (fcmp_real - 1.0).
            Positivo = crédito; negativo = débito.
        detalhes_historicos: Lista de dicts com breakdown por histórico
            contribuinte. Cada dict contém: processo, documento,
            mes_apuracao, ano_apuracao, comissao_adiantada, fcmp_aplicado,
            contribuicao_ponderada.
    """

    gl_nome: str
    numero_pc: str
    codigo_cliente: str
    processo: str
    comissao_adiantada: float
    fcmp_real: float
    ajuste: float
    historicos_considerados: int = 0
    detalhes_historicos: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ReconciliacaoResult:
    """Resultado do cálculo de reconciliações.

    Attributes:
        itens: Lista de todos os ReconciliacaoItem.
        total_por_gl: Dicionário {gl_nome: ajuste_total}.
        warnings: Avisos sobre processos sem histórico de adiantamentos.
    """

    itens: List[ReconciliacaoItem] = field(default_factory=list)
    total_por_gl: Dict[str, float] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Estornos por Devolução
# ---------------------------------------------------------------------------


@dataclass
class EstornoItem:
    """Estorno proporcional gerado por uma devolução.

    Attributes:
        gl_nome: Nome do Gerente de Linha.
        processo: Processo AC vinculado à devolução.
        nf_origem: NF da devolução.
        valor_devolvido: Valor financeiro da devolução.
        valor_processo: Valor total realizado do processo.
        comissao_base: Comissão total do GL no processo (base do cálculo).
        estorno: comissao_base × (valor_devolvido / valor_processo) × (-1).
    """

    gl_nome: str
    processo: str
    nf_origem: str
    valor_devolvido: float
    valor_processo: float
    comissao_base: float
    estorno: float


@dataclass
class EstornosResult:
    """Resultado do cálculo de estornos por devolução.

    Attributes:
        itens: Lista de todos os EstornoItem.
        total_por_gl: Dicionário {gl_nome: estorno_total} (valores negativos).
        warnings: Avisos sobre devoluções sem processo correspondente.
    """

    itens: List[EstornoItem] = field(default_factory=list)
    total_por_gl: Dict[str, float] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
