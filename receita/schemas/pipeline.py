"""
receita/schemas/pipeline.py — Schema de resultado consolidado do pipeline.

PipelineRecebimentoResult agrega todos os resultados parciais das etapas
01–14 do pipeline de comissões por recebimento.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from receita.schemas.calculo import (
    AtribuicaoResult,
    ComissaoResult,
    EstornosResult,
    FCMPResult,
    MapeamentoResult,
    ReconciliacaoResult,
    TCMPResult,
)
from receita.schemas.entrada import ProcessoPedidoTabela


@dataclass
class PipelineRecebimentoResult:
    """Resultado consolidado do pipeline de comissões por recebimento.

    Produzido por `receita.pipeline.runner.executar()`.
    Cada campo corresponde ao resultado de uma etapa do pipeline.

    Attributes:
        atribuicao_result: GLs elegíveis por linha (etapa 03).
        mapeamento_result: Documentos AF vinculados a processos AC (etapa 04).
        tcmp_result: TCMP por processo (etapa 05).
        fcmp_por_gl: Dicionário {gl_nome: FCMPResult} (etapa 06).
        comissao_result: Comissões por documento AF (etapa 07).
        status_por_processo_pai: Status de faturamento/pagamento por Pai
            (etapa 08). Chave: (numero_pc, codigo_cliente).
        processos_aptos_reconciliacao: Conjunto de processos cujos Pais estão
            100% faturados E pagos (etapa 09).
        reconciliacao_result: Ajustes de adiantamentos (etapa 10).
        estornos_result: Estornos por devoluções (etapa 11).
        tabela_pc: Tabela "Processo x Pedido de Compra" usada no pipeline.
        df_analise_comercial: DataFrame AC sem filtro de mês (para exportadores).
        realizados_result: RealizadosResult do pipeline de faturamento
            (necessário para recalcular FC por item nos exportadores).
        arquivos_md: Caminhos dos arquivos .md gerados (etapa 13).
        arquivos_excel: Caminhos dos arquivos .xlsx gerados (etapa 14).
        conflitos_gl: Lista de conflitos de GL detectados na validação.
        arquivo_alertas: Caminho do arquivo alertas_MM_AAAA.txt, se gerado.
        warnings: Avisos não-críticos coletados de todas as etapas.
        errors: Erros críticos que interromperam o pipeline.
        step_failed: Nome da etapa que falhou criticamente, se aplicável.
    """

    atribuicao_result: Optional[AtribuicaoResult] = None
    mapeamento_result: Optional[MapeamentoResult] = None
    tcmp_result: Optional[TCMPResult] = None
    fcmp_por_gl: Dict[str, FCMPResult] = field(default_factory=dict)
    comissao_result: Optional[ComissaoResult] = None
    status_por_processo_pai: Dict[Any, Any] = field(default_factory=dict)
    processos_aptos_reconciliacao: List[str] = field(default_factory=list)
    reconciliacao_result: Optional[ReconciliacaoResult] = None
    estornos_result: Optional[EstornosResult] = None
    tabela_pc: Optional[ProcessoPedidoTabela] = None
    df_analise_comercial: Any = None  # pd.DataFrame
    realizados_result: Any = None
    arquivos_md: List[str] = field(default_factory=list)
    arquivos_excel: List[str] = field(default_factory=list)
    conflitos_gl: List[str] = field(default_factory=list)
    arquivo_alertas: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    step_failed: Optional[str] = None

    @property
    def ok(self) -> bool:
        """True se o pipeline não produziu erros críticos."""
        return len(self.errors) == 0 and self.step_failed is None

    @property
    def tem_comissoes(self) -> bool:
        """True se há ao menos um ComissaoItem calculado."""
        return (
            self.comissao_result is not None
            and len(self.comissao_result.itens) > 0
        )
