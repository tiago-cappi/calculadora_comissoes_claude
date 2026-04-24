"""
receita/pipeline/etapas/ — Etapas individuais do pipeline de Recebimento.

Cada etapa segue o padrão N8N:
    - Importável como função: run(input_data: dict) -> dict
    - Executável como nó N8N: stdin JSON → stdout JSON
"""
