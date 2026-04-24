---
description: "Use when editing, debugging, or extending commission-by-receipt logic in receita/. Covers pipeline stages, TCMP/FCMP formulas, reconciliation, estornos, Supabase persistence, and the N8N wrapper pattern."
applyTo: "receita/**"
---

# Comissões por Recebimento — `receita/` Module Guide

> Full business rules documentation: [docs/documentacao_comissoes_recebimento.md](../../docs/documentacao_comissoes_recebimento.md)

## Architecture at a Glance

```
receita/
├── schemas/          # Dataclass contracts (entrada, calculo, historico, pipeline)
├── loaders/          # Data ingestion (AF filtered + full, PC table, stubs)
├── calculadores/     # Pure calculation logic (no I/O)
├── rastreamento/     # Process-father tracking & reconciliation eligibility
├── validadores/      # PC integrity + GL conflict checks
├── alertas/          # Expiry & conflict alert generation
├── exportadores/     # Markdown + Excel per GL
├── supabase/         # Persistence client, historico queries, schema DDL
├── n8n/              # JSON stdin/stdout wrappers for external workflow
└── pipeline/
    ├── runner.py     # Orchestrator: 14 sequential stages
    └── etapas/       # N8N-compatible wrappers per stage
```

## Pipeline Stage Map

| Stage | Module | Critical? | Fails → |
|-------|--------|:---------:|---------|
| 00 | `validadores/validar_processo_pai` | No | Warning |
| 03 | `calculadores/atribuir_gls` | **Yes** | Pipeline stops |
| 02 | `validadores/validar_conflito_gl` | No | Warning |
| 04 | `calculadores/mapear_documentos` | **Yes** | Pipeline stops |
| 05 | `calculadores/calcular_tcmp` | **Yes** | Pipeline stops |
| 06 | `calculadores/calcular_fcmp` | **Yes** | Pipeline stops |
| 07 | `calculadores/calcular_comissao` | **Yes** | Pipeline stops |
| 08 | `rastreamento/*` | No | Warning |
| 08.5 | `pipeline/etapas/etapa_12_salvar_historico` | No | Warning |
| 09 | `rastreamento/determinar_reconciliacao` | No | Warning |
| 10 | `calculadores/calcular_reconciliacao` | No | Warning |
| 11 | `calculadores/calcular_estornos` | No | Warning |
| 12 | `pipeline/etapas/etapa_12_salvar_historico` | No | Warning |
| 13–14 | `exportadores/md_exporter, excel_exporter` | No | Warning |

**Rule:** Critical stages raise and halt the pipeline. Non-critical stages catch exceptions, append to `warnings`, and continue.

## Core Formulas

### TCMP — Taxa de Comissão Média Ponderada (Stage 05)
```
TCMP(processo) = Σ(taxa_efetiva_item × valor_item) / Σ(valor_item)
```
Where `taxa_efetiva_item` comes from the GL rule with **highest specificity** matching the item hierarchy (6-level: Linha → Grupo → Subgrupo → Tipo Mercadoria → Fabricante → Aplicação).

### FCMP — Fator de Comissão Médio Ponderado (Stage 06)
```
If FATURADO:  FCMP_rampa = Σ(FC_item × valor_item) / Σ(valor_item)
              FCMP_aplicado = apply_escada(FCMP_rampa)  # if configured
If NOT FATURADO: FCMP = 1.0 (PROVISÓRIO)
```
**Dependency:** `from scripts.fc_calculator import calcular_fc_item, gerar_degraus_escada`

### Commission (Stage 07)
```
comissao_final = valor_documento × TCMP × fcmp_considerado × fator_split
```
**Critical rule:** `fcmp_considerado` is **always 1.0** at payment time (adiantamento). Real FCMP is stored for audit but only applied at reconciliation.

### Reconciliation (Stage 10)
```
fcmp_real = Σ(comissao_adiantada_i × fcmp_aplicado_i) / Σ(comissao_adiantada_i)
ajuste = Σ(comissao_adiantada) × (fcmp_real - 1.0)
```
Triggered only when **both** parent faturamento and pagamento are 100% complete.

### Estornos (Stage 11)
```
proporção = min(|valor_devolvido| / valor_processo_ac, 1.0)
estorno = comissao_gl_processo × proporção × (-1)
```

## Key Data Contracts (schemas/)

| Dataclass | Module | Purpose |
|-----------|--------|---------|
| `ProcessoPedidoTabela` | `schemas/entrada` | O(1) lookup: process → parent (numero_pc, codigo_cliente) |
| `ElegivelGL` | `schemas/calculo` | GL with taxa_efetiva, hierarquia, especificidade, fator_split |
| `TCMPResult` | `schemas/calculo` | `{processo: float}` + audit `detalhes` |
| `FCMPProcesso` | `schemas/calculo` | Per-GL per-process: fcmp_rampa, fcmp_aplicado, modo, provisorio |
| `ComissaoItem` | `schemas/calculo` | One commission line: doc × GL with all factors |
| `ReconciliacaoItem` | `schemas/calculo` | Adjustment when parent process closes |
| `EstornoItem` | `schemas/calculo` | Reversal from devolução |
| `PipelineRecebimentoResult` | `schemas/pipeline` | Aggregate of all stages + `ok`, `tem_comissoes` |

## Conventions & Patterns

### Result object pattern
Every calculator returns a typed result with `.warnings: List[str]`. The pipeline accumulates all warnings. Check `result.ok` (no errors, no `step_failed`).

### Dual DataFrame pattern
- `df_analise_financeira` — filtered by mês/ano (current period commissions)
- `df_af_full` — unfiltered (historical payment verification for reconciliation)
- `df_ac_full` — always unfiltered (processes may have been billed months ago)

### Document classification
```python
if documento.startswith("COT") or documento.startswith("ADT"):
    tipo = "ADIANTAMENTO"  # match via Processo field
else:
    tipo = "REGULAR"       # match via NF field
```

### NF/Processo matching (mapear_documentos)
1. Exact upper-case match
2. Fallback: digits-only comparison (strips non-digits, removes leading zeros)

### Parcel base extraction (verificar_pagamentos)
```python
base = re.sub(r"[A-Za-z]+$", "", documento)  # "123456A" → "123456"
```
All parcels sharing the same base must have `Situação == 1` (paid).

### Hierarchy specificity matching (_find_best_rule)
Rules are scored 0–6 by number of filled hierarchy fields. Best match = highest specificity where all filled fields match the item's hierarchy. Catch-all (specificity 0) is the last resort.

### Fator split
When multiple GLs cover the same line: `fator_split_i = taxa_efetiva_i / Σ(taxa_efetiva)`.

## Supabase Persistence

| Table | PK | Purpose |
|-------|-----|---------|
| `historico_comissoes` | (nome, processo, documento, tipo_pagamento, mes, ano) | Commission facts + reconciliation flag |
| `historico_processo_pai` | (numero_pc, codigo_cliente, processo, mes, ano) | Parent-child status per period |
| `historico_pagamentos_processo_pai` | (numero_pc, codigo_cliente, documento, mes, ano) | Payment parcels per parent |

**Flow:** Stage 08.5 persists with `reconciliado=False` → Stage 12 updates `reconciliado=True` for reconciled processes.

## N8N Etapa Wrapper Pattern
Each `etapa_XX_*.py` reads JSON from stdin, calls the calculator, and prints JSON to stdout:
```python
def run(input_data: dict) -> dict:
    # deserialize → call calculador → serialize
if __name__ == "__main__":
    payload = json.loads(sys.stdin.read())
    print(json.dumps(run(payload), ensure_ascii=False, default=str))
```

## Testing

```bash
# Full faturamento pipeline test (synthetic 7-item data)
python testes_comissao/rodar_teste.py

# Receipt-specific test (stages 1–5 + reconciliation)
python testes_comissao/teste_recebimento/rodar_teste_recebimento.py

# Multi-month audit with snapshots
python testes_comissao/rodar_auditoria_recebimento.py --manifest manifesto.json --offline
```

## Gotchas

- **iterrows() usage:** Calculators use `.iterrows()` (not vectorized) due to per-row rule matching by specificity. This is intentional — vectorization is impractical for hierarchical rule lookups.
- **FCMP dependency on scripts/:** `calcular_fcmp.py` imports `calcular_fc_item` from `scripts.fc_calculator`. Changes to faturamento FC logic ripple into recebimento.
- **Reconciliation requires Supabase:** `calcular_reconciliacao` queries `historico_comissoes` directly. In offline/test mode, ensure `SUPABASE_UNAVAILABLE=True` or mock the client.
- **PC table sync:** The `ProcessoPedidoTabela` must reflect the current ERP state. Stale data = wrong parent identification.
- **Month filtering asymmetry:** AF is dual (filtered + full), AC is always full, PC is static. Never filter AC by month.
