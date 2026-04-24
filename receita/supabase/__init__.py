"""
receita/supabase/ — Interface com o Supabase para dados históricos.

Reutiliza a infraestrutura existente em scripts/supabase_loader.py e
scripts/supabase_writer.py sem duplicar código.

Tabelas novas (schema comissoes):
  - historico_comissoes            (fonte unificada da verdade)
  - historico_processo_pai
  - historico_pagamentos_processo_pai
"""
