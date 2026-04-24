"""
receita/calculadores/ — Cálculos puros do pipeline de recebimento (sem I/O).

Módulos:
    atribuir_gls          → GLs elegíveis por Linha (da config_comissao)
    mapear_documentos     → Vincula docs AF a processos AC via NF
    calcular_tcmp         → TCMP por processo (média ponderada taxa × valor)
    calcular_fcmp         → FCMP por processo por GL (reutiliza calcular_fc_item)
    calcular_comissao     → Comissão por documento AF
    calcular_reconciliacao → Reconciliação quando Processo Pai fecha
    calcular_estornos     → Estornos proporcionais por devoluções
"""
