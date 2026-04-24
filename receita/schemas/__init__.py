"""
receita/schemas/ — Contratos de dados entre módulos.

Imports convenientes para uso externo:
    from receita.schemas import ProcessoPedidoItem, ProcessoPedidoTabela
    from receita.schemas import HistoricoFaturamentoParcial
    from receita.schemas import ElegívelGL, AtribuicaoResult, ...
    from receita.schemas import PipelineRecebimentoResult
"""

from receita.schemas.entrada import ProcessoPedidoItem, ProcessoPedidoTabela
from receita.schemas.historico import HistoricoFaturamentoParcial
from receita.schemas.calculo import (
    ElegivelGL,
    AtribuicaoResult,
    MapeamentoResult,
    TCMPResult,
    FCMPProcesso,
    FCMPResult,
    ComissaoItem,
    ComissaoResult,
    ReconciliacaoItem,
    ReconciliacaoResult,
    EstornoItem,
    EstornosResult,
)
from receita.schemas.pipeline import PipelineRecebimentoResult

__all__ = [
    "ProcessoPedidoItem",
    "ProcessoPedidoTabela",
    "HistoricoFaturamentoParcial",
    "ElegivelGL",
    "AtribuicaoResult",
    "MapeamentoResult",
    "TCMPResult",
    "FCMPProcesso",
    "FCMPResult",
    "ComissaoItem",
    "ComissaoResult",
    "ReconciliacaoItem",
    "ReconciliacaoResult",
    "EstornoItem",
    "EstornosResult",
    "PipelineRecebimentoResult",
]
