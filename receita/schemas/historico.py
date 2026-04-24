"""
Schemas historicos persistidos no Supabase para o modulo receita/.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class HistoricoComissao:
    """Fonte única da verdade para recebimento, reconciliação e devoluções."""

    nome: str
    cargo: str
    processo: str
    numero_pc: str
    codigo_cliente: str
    tipo: str
    tipo_pagamento: str
    documento: str
    nf_extraida: str
    linha_negocio: str
    status_processo: str
    mes_apuracao: int
    ano_apuracao: int
    valor_documento: float
    valor_processo: float
    tcmp: float
    fcmp_rampa: float
    fcmp_aplicado: float
    fcmp_considerado: float
    fcmp_modo: str
    comissao_potencial: float
    comissao_adiantada: float
    comissao_total: float
    status_faturamento_completo: bool = False
    status_pagamento_completo: Optional[bool] = None
    reconciliado: bool = False
    ac_snapshot_json: str = ""
    af_snapshot_json: str = ""
    tcmp_detalhes_json: str = ""
    fcmp_detalhes_json: str = ""
    created_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        self.nome = str(self.nome).strip()
        self.cargo = str(self.cargo).strip()
        self.processo = str(self.processo).strip().upper()
        self.numero_pc = str(self.numero_pc).strip().upper()
        self.codigo_cliente = str(self.codigo_cliente).strip().upper()
        self.tipo = str(self.tipo).strip().lower() or "recebimento"
        self.tipo_pagamento = str(self.tipo_pagamento).strip().upper()
        self.documento = str(self.documento).strip().upper()
        self.nf_extraida = str(self.nf_extraida).strip().upper()
        self.linha_negocio = str(self.linha_negocio).strip()
        self.status_processo = str(self.status_processo).strip()
        self.mes_apuracao = int(self.mes_apuracao)
        self.ano_apuracao = int(self.ano_apuracao)


@dataclass
class HistoricoProcessoPai:
    """Vinculo mensal entre um Processo Pai e seus processos relacionados."""

    numero_pc: str
    codigo_cliente: str
    processo: str
    is_processo_pai: bool
    status_faturado: bool
    status_pago: Optional[bool]
    mes_referencia: int
    ano_referencia: int
    created_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        self.numero_pc = str(self.numero_pc).strip().upper()
        self.codigo_cliente = str(self.codigo_cliente).strip().upper()
        self.processo = str(self.processo).strip().upper()
        self.mes_referencia = int(self.mes_referencia)
        self.ano_referencia = int(self.ano_referencia)


@dataclass
class HistoricoPagamentoProcessoPai:
    """Parcela/documento da AF associado a um Processo Pai em um periodo."""

    numero_pc: str
    codigo_cliente: str
    processo: str
    numero_nf: str
    documento: str
    situacao_codigo: int
    situacao_texto: str
    dt_prorrogacao: Optional[datetime]
    data_baixa: Optional[datetime]
    valor_documento: float
    mes_referencia: int
    ano_referencia: int
    created_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        self.numero_pc = str(self.numero_pc).strip().upper()
        self.codigo_cliente = str(self.codigo_cliente).strip().upper()
        self.processo = str(self.processo).strip().upper()
        self.numero_nf = str(self.numero_nf).strip().upper()
        self.documento = str(self.documento).strip().upper()
        self.situacao_codigo = int(self.situacao_codigo)
        self.situacao_texto = str(self.situacao_texto).strip()
        self.mes_referencia = int(self.mes_referencia)
        self.ano_referencia = int(self.ano_referencia)


# Alias de compatibilidade temporária para pontos ainda nomeados pelo modelo antigo.
HistoricoFaturamentoParcial = HistoricoComissao
