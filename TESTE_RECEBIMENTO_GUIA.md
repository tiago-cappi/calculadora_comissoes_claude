# Guia de Teste — Pipeline de Comissões por Recebimento

> Objetivo: exercitar manualmente, mês a mês, o fluxo completo de
> recebimento — adiantamento, pagamento regular, rastreamento de
> Processo Pai, reconciliação, estorno — e validar os resultados
> na saída Excel **e** nas três tabelas do banco SQLite.

---

## 0. Contexto e GL sob teste

A única regra ativa em `configuracoes_comissoes.xlsx` (aba `config_comissao`)
com `cargo == "Gerente Linha"` (único cargo marcado como `Recebimento` em
`cargos`) é:

| Campo                 | Valor                              |
|-----------------------|------------------------------------|
| `nome_colaborador`    | **Alessandro Cappi**               |
| `cargo`               | Gerente Linha                      |
| `linha`               | **Detecção e Processos de Gases**  |
| `fatia_cargo`         | 20                                 |
| `taxa_rateio_maximo`  | 50                                 |

**Taxa efetiva** aplicada a todo item da linha:
`0,20 × 0,50 = 0,10` → **10%**.

**Escada de FCMP (aba `fc_escada_cargos`, Gerente Linha):**
modo `ESCADA`, 4 degraus, piso 30%.
Degraus = `[0,3000; 0,5333; 0,7667; 1,0000]`.
Regra round-up: degrau aplicado = menor degrau ≥ FCMP_rampa.

**Pesos (aba `pesos_metas`, Alessandro Cappi):**
`faturamento_linha=40 | rentabilidade=20 | conversao_linha=40`.

**Metas (abas `metas_aplicacao` e `meta_rentabilidade`, linha Detecção):**
`faturamento=300 | conversao=300 | meta_rentabilidade_alvo_pct=40`.

Todos os números abaixo são calculáveis a partir dessas regras
— não dependem de dados ocultos da empresa.

---

## 1. Preparação (antes de começar)

1. **Backup** de arquivos e banco (pasta `backup_pre_teste/`):
   - `dados_entrada/analise-comercial.xlsx`
   - `dados_entrada/analise-financeira.xlsx`
   - `dados_entrada/devolucoes.xlsx`
   - `dados_entrada/Processo x Pedido de Compra.xlsx`
   - `historico.db`
   - `saida/` (arquivo por arquivo — basta renomear `saida/` para `saida_pre_teste/`)

2. **Zerar o banco** para partir de estado conhecido:
   ```
   python -c "import sqlite3; c=sqlite3.connect('historico.db'); [c.execute(f'DELETE FROM {t}') for t in ('historico_comissoes','historico_processo_pai','historico_pagamentos_processo_pai')]; c.commit(); c.close()"
   ```

3. **Confira** que `configuracoes_comissoes.xlsx` NÃO é modificado
   durante o teste. Ele é apenas lido.

> **ATENÇÃO — regra crítica para todas as modificações na AC:**
> Nunca apague ou substitua as linhas existentes. Sempre **acrescente**
> as novas linhas ao final. O pipeline usa os >135 000 registros
> originais para montar a Análise Comercial completa; se os dados reais
> forem apagados, `df_ac_full` ficará vazio e o pipeline não encontrará
> nenhuma Linha/GL.

---

## 2. Cenário-teste

Criaremos **1 Processo Pai** com **2 processos filho**, todos no
cliente fictício `7001`:

| Campo     | Pai       | Filho A    | Filho B    |
|-----------|-----------|------------|------------|
| Processo  | `999001`  | `999002`   | `999003`   |
| `numero_pc` | `999001` | `999001`  | `999001`   |
| `codigo_cliente` | `7001` | `7001` | `7001` |
| Linha     | Detecção e Processos de Gases | (idem) | (idem) |
| Valor total (Valor Realizado quando faturado) | — | R$ 50.000 | R$ 30.000 |

Produto usado em todos os itens: **`BM5K0000-000`** ("ANALISADOR
PORTÁTIL BIOGAS 5000"), que está na CP como Linha "Detecção e
Processos de Gases".

**Linha do tempo:**

| Mês apurado | Filho A  | Filho B  | AF              | Devolução |
|-------------|----------|----------|-----------------|-----------|
| **11/2025** | pedido aberto | pedido aberto | adiantamento `COT-999002` R$ 5.000 (Sit=1) | — |
| **12/2025** | faturado, NF 90001 | pedido aberto | parcela regular `90001A` R$ 25.000 (Sit=1) | — |
| **01/2026** | já faturado | faturado, NF 90002 | parcelas `90001B` R$ 25.000 (Sit=1) + `90002A` R$ 30.000 (Sit=1) | devolução de R$ 6.000 da NF 90001 |

No final do mês 3, o Pai está **100% faturado e 100% pago** →
dispara reconciliação.

---

## 3. Mês 1 — Novembro/2025 (apenas adiantamento)

### 3.1 Modificações nos arquivos de entrada

#### `dados_entrada/Processo x Pedido de Compra.xlsx` — aba Dados
Adicionar 2 linhas no fim (mantenha as existentes):

| Número  | Código do Cliente | Numero pc |
|---------|-------------------|-----------|
| 999002  | 7001              | 999001    |
| 999003  | 7001              | 999001    |

#### `dados_entrada/analise-comercial.xlsx` — aba Dados
Acrescentar 2 linhas **ao final** do arquivo (NÃO apague nem substitua
as linhas existentes). Preencher exatamente as colunas listadas abaixo;
as demais colunas podem ficar vazias:

| Código Produto | Operação | Processo | Numero NF | Status Processo | Dt Emissão | Valor Orçado | Valor Realizado |
|----------------|----------|----------|-----------|-----------------|------------|--------------|-----------------|
| BM5K0000-000   | PSEM     | 999002   | (vazio)   | EM ANDAMENTO    | (vazio)    | 50000        | 0               |
| BM5K0000-000   | PSEM     | 999003   | (vazio)   | EM ANDAMENTO    | (vazio)    | 30000        | 0               |

> **Motivo do campo `Operação`:** o pipeline filtra a AC por operações
> válidas antes de qualquer cálculo. Linhas com `Operação` vazia são
> descartadas. Use `PSEM` (ou qualquer valor da lista válida: `COS`,
> `COT`, `FLOC`, `PSEM`, `PSER`, `PVEN`, etc.).
>
> **`Status Processo` ≠ "FATURADO"** e **`Dt Emissão` vazio** garantem
> que o Pai ainda **não** está 100% faturado.

#### `dados_entrada/analise-financeira.xlsx` — aba Dados
Adicionar 1 linha:

| Documento     | Cliente | Dt. Vencimento | Situação | Valor Líquido | Data de Baixa | Tipo de Baixa |
|---------------|---------|----------------|----------|---------------|---------------|---------------|
| COT-999002    | 007001  | 25/11/2025     | 1        | 5000          | 25/11/2025    | B             |

> Prefixo `COT` ⇒ adiantamento; vínculo AF→AC é pelo **Processo**
> (não pela NF). Situação=1 ⇒ Recebido.

### 3.2 Rodar o cálculo

```
python rodar_pipeline.py --mes 11 --ano 2025
```

### 3.3 O que esperar

**Excel `saida/11_2025/recebimento_Alessandro Cappi_11_2025.xlsx`:**

- Aba **Adiantamentos**: 1 linha
  - Processo `999002`, Documento `COT-999002`, Valor R$ 5.000
  - TCMP = `0,10` (10%)
  - FCMP Modo = `PROVISÓRIO`
  - **Comissão Adiantada = R$ 500** (5000 × 0,10 × 1,0)
- Aba **Pagamentos Regulares**: vazia (0 linhas)
- Aba **Reconciliação**: vazia
- Aba **FCMP Processos**: `999002` e `999003` presentes, modo
  `PROVISÓRIO`, `fcmp_aplicado = 1,0`
- Aba **Resumo** → bloco "Conciliação do Líquido":
  - Adiantamentos: R$ 500,00
  - Regulares: R$ 0
  - Ajustes: R$ 0
  - Estornos: R$ 0
  - **Total Líquido: R$ 500,00**

**Banco `historico.db` — `historico_comissoes`:**

| nome             | processo | documento   | tipo_pagamento | mes/ano | valor_documento | tcmp | fcmp_rampa | fcmp_aplicado | fcmp_considerado | fcmp_modo   | comissao_adiantada | comissao_total | status_fat | status_pag | reconciliado |
|------------------|----------|-------------|----------------|---------|-----------------|------|------------|---------------|------------------|-------------|--------------------|----------------|------------|------------|--------------|
| Alessandro Cappi | 999002   | COT-999002  | ADIANTAMENTO   | 11/2025 | 5000            | 0,10 | 1,0        | 1,0           | 1,0              | PROVISÓRIO  | 500                | 500            | 0 (False)  | NULL ou 0  | 0 (False)    |

Checagem (SQL):
```
sqlite3 historico.db "SELECT nome,processo,documento,tipo_pagamento,valor_documento,tcmp,fcmp_considerado,comissao_total,reconciliado FROM historico_comissoes WHERE mes_apuracao=11 AND ano_apuracao=2025"
```

**Banco — `historico_processo_pai`:**
Devem existir 3 linhas com `mes_referencia=11, ano_referencia=2025`:
- `(999001, 7001, 999001, is_processo_pai=1, status_faturado=0, status_pago=0)`
- `(999001, 7001, 999002, is_processo_pai=0, status_faturado=0, status_pago=0)`
- `(999001, 7001, 999003, is_processo_pai=0, status_faturado=0, status_pago=0)`

**Banco — `historico_pagamentos_processo_pai`:**
- 1 linha para `COT-999002`, situacao_codigo=1, valor_documento=5000,
  mes_referencia=11, ano_referencia=2025.

### 3.4 Checklist Mês 1

- [ ] Comissão adiantada registrada = R$ 500
- [ ] `reconciliado = 0` (adiantamento é sempre provisório)
- [ ] `status_faturamento_completo = 0` no histórico do Pai
- [ ] Nenhuma linha aparece em "Pagamentos Regulares" nem "Reconciliação"
- [ ] FCMP modo = PROVISÓRIO e FCMP aplicado = 1,0

---

## 4. Mês 2 — Dezembro/2025 (primeiro faturamento + pagamento parcial)

### 4.1 Modificações

#### `analise-comercial.xlsx`
**Atualizar** a linha do processo `999002`:

| Processo | Numero NF | Status Processo | Dt Emissão  | Valor Realizado |
|----------|-----------|-----------------|-------------|-----------------|
| 999002   | 90001     | FATURADO        | 15/12/2025  | 50000           |

Manter `999003` como estava (aberto, sem NF).

#### `analise-financeira.xlsx`
Adicionar 1 linha (primeira parcela da NF 90001):

| Documento | Cliente | Dt. Vencimento | Situação | Valor Líquido | Data de Baixa |
|-----------|---------|----------------|----------|---------------|---------------|
| 90001A    | 007001  | 10/12/2025     | 1        | 25000         | 10/12/2025    |

> O COT-999002 do Mês 1 NÃO precisa ser removido — ele permanece na
> AF. Como foi `Data de Baixa` em 11/2025, não aparece no filtro de
> dezembro (o pipeline filtra a AF pelo mês da Data de Baixa).

### 4.2 Rodar

```
python rodar_pipeline.py --mes 12 --ano 2025
```

### 4.3 O que esperar

**Excel do Mês 2:**

- Aba **Adiantamentos**: vazia (o adiantamento de Nov não reaparece).
- Aba **Pagamentos Regulares**: 1 linha
  - Processo `999002`, Documento `90001A`, NF 90001, Valor R$ 25.000
  - TCMP = 0,10
  - **FCMP Rampa** e **FCMP Aplicado** — valores calculados de verdade
    com base nos metas da linha Detecção (≠ 1,0 em geral)
  - **Aguarda Reconciliação = SIM** (Pai ainda não fechado: `999003` pendente)
  - **Comissão a Pagar = R$ 2.500** (25000 × 0,10 × **1,0**,
    porque na competência o FCMP considerado é 1,0 até o Pai fechar)
- Aba **Reconciliação**: vazia (Pai não está 100% faturado)
- Aba **FCMP Processos**:
  - `999002` → modo `RAMPA` ou `ESCADA`, `provisorio=False`
  - `999003` → modo `PROVISÓRIO`, `provisorio=True` (ainda não faturou)
- Aba **Resumo**: Total Líquido = R$ 2.500 (só o regular deste mês)

> **Anote** o `fcmp_aplicado` mostrado para `999002`. Chamaremos esse
> número de **FCMP_A**. Ele será usado no Mês 3 para validar a
> reconciliação.

**Banco — `historico_comissoes`** (acumulado, ordenar por `ano,mes`):

| processo | documento   | tipo_pagamento | mes/ano | comissao_adiantada | fcmp_aplicado | fcmp_considerado | status_fat   | status_pag | reconciliado |
|----------|-------------|----------------|---------|--------------------|---------------|------------------|--------------|------------|--------------|
| 999002   | COT-999002  | ADIANTAMENTO   | 11/2025 | 500                | 1,0           | 1,0              | 0            | 0          | 0            |
| 999002   | 90001A      | REGULAR        | 12/2025 | 2500               | **FCMP_A**    | 1,0              | 0 (ainda!)   | 0          | 0            |

> **Invariante chave:** `fcmp_aplicado` persistido **reflete o FCMP
> real do momento** (escada já aplicada quando o processo foi faturado),
> mas `fcmp_considerado` = 1,0 → `comissao_total = 2500` (sem FCMP).

**Banco — `historico_processo_pai`:**
3 linhas novas com `mes_referencia=12, ano_referencia=2025`:
- `999001` (Pai) → `status_faturado=0, status_pago=0`
- `999002` → `status_faturado=1, status_pago=0` (AF parcial: só 90001A, falta B)
- `999003` → `status_faturado=0`

**Banco — `historico_pagamentos_processo_pai`:**
Linha nova para `90001A`, `situacao_codigo=1`, `mes_referencia=12`.

### 4.4 Checklist Mês 2

- [ ] `999002` aparece em "Pagamentos Regulares", **não** em "Reconciliação"
- [ ] `fcmp_considerado=1,0` e `comissao_total=2500` no banco
- [ ] `fcmp_aplicado` persistido ≠ 1,0 (se a escada entrar)
- [ ] Histórico do Mês 1 permanece intacto (não foi sobrescrito)
- [ ] `historico_processo_pai` do Mês 2 tem `999001.status_faturado=0`
      (porque `999003` ainda não faturou)

---

## 5. Mês 3 — Janeiro/2026 (segundo faturamento + pagamento final + reconciliação + devolução)

### 5.1 Modificações

#### `analise-comercial.xlsx`
Manter `999002` FATURADO. **Atualizar** `999003`:

| Processo | Numero NF | Status Processo | Dt Emissão  | Valor Realizado |
|----------|-----------|-----------------|-------------|-----------------|
| 999003   | 90002     | FATURADO        | 10/01/2026  | 30000           |

#### `analise-financeira.xlsx`
Adicionar 2 linhas:

| Documento | Cliente | Dt. Vencimento | Situação | Valor Líquido | Data de Baixa |
|-----------|---------|----------------|----------|---------------|---------------|
| 90001B    | 007001  | 10/01/2026     | 1        | 25000         | 10/01/2026    |
| 90002A    | 007001  | 15/01/2026     | 1        | 30000         | 15/01/2026    |

> Agora **todas** as parcelas de todas as NFs dos filhos do Pai têm
> Situação=1. O Pai fica 100% faturado e 100% pago.

#### `devolucoes.xlsx`
Adicionar 1 linha:

| Código Operação | Data de Entrada | Valor Produtos | Num docorigem |
|-----------------|-----------------|----------------|---------------|
| 5128            | 20/01/2026      | 6000           | 90001         |

> Devolução de R$ 6.000 vinculada à NF 90001 (processo 999002).

### 5.2 Rodar

```
python rodar_pipeline.py --mes 01 --ano 2026
```

### 5.3 O que esperar

**Excel do Mês 3:**

- Aba **Adiantamentos**: vazia
- Aba **Pagamentos Regulares**: 2 linhas (90001B e 90002A), mas
  **roteadas** para a aba **Reconciliação** — por isso devem estar
  **ausentes** de Regulares neste mês (regra: processo reconciliado
  suprime o doc das abas de competência).
- Aba **Reconciliação**: 1 bloco consolidado do Pai `999001 / 7001`
  - Total adiantado por GL (somado de todos os meses):
    `500 + 2500 + 2500 + 3000 = R$ 8.500`
    (COT Nov + 90001A Dez + 90001B Jan + 90002A Jan)
  - FCMP real: `sum(comissao_adiantada × fcmp_aplicado) / sum(comissao_adiantada)`.
    Com os números persistidos:
    `(500×1,0 + 2500×FCMP_A + 2500×FCMP_B + 3000×FCMP_B) / 8500`
    onde **FCMP_A** é o FCMP do 999002 que você anotou no Mês 2 e
    **FCMP_B** é o FCMP do 999003 neste mês (veja aba FCMP Processos).
  - **Ajuste = 8500 × (fcmp_real − 1,0)** (negativo se fcmp_real<1)
  - **Detalhes históricos**: 4 linhas internas (uma por documento
    contribuinte), cada uma com `mes_apuracao/ano_apuracao` do período
    em que foi adiantada.
- Aba **Reconciliação — sub-bloco Estornos**: 1 linha
  - Processo `999002`, NF 90001, valor_devolvido R$ 6.000
  - `valor_processo = 50.000`, proporção = `6000/50000 = 0,12`
  - `comissao_base` do GL no processo **no mês atual** = soma dos itens
    de `comissao_final` de 999002 no resultado de `comissao_result`
    (neste mês, itens de 999002 entraram na reconciliação, portanto
    `comissao_base` do estorno vem do resultado corrente de cálculo,
    que inclui 90001B de 25.000 × 0,10 × 1,0 = 2.500)
  - **Estorno ≈ −R$ 300** (= 2.500 × 0,12 × −1)
    > Nota: a fórmula exata do estorno depende do `comissao_final`
    > corrente do GL no processo, não do total histórico. Anote o valor
    > efetivo na célula para validar.
- Aba **Resumo** — bloco "Conciliação do Líquido":
  - Adiantamentos: R$ 0 (não há adiantamento neste mês)
  - Regulares: R$ 0 (os dois docs foram absorvidos pela reconciliação)
  - Ajustes: valor calculado acima (negativo se FCMP_real<1)
  - Estornos: ≈ −R$ 300
  - **Total Líquido** = Ajustes + Estornos

**Banco — `historico_comissoes`:**

Após o mês 3 devem existir **4 linhas** para Alessandro Cappi:

| processo | documento   | mes/ano  | comissao_adiantada | fcmp_aplicado | reconciliado |
|----------|-------------|----------|--------------------|---------------|--------------|
| 999002   | COT-999002  | 11/2025  | 500                | 1,0           | **1**        |
| 999002   | 90001A      | 12/2025  | 2500               | FCMP_A        | **1**        |
| 999002   | 90001B      | 01/2026  | 2500               | FCMP_A'       | **1**        |
| 999003   | 90002A      | 01/2026  | 3000               | FCMP_B        | **1**        |

> Todas as 4 linhas devem ficar com `reconciliado = 1` após a etapa 12
> (a etapa marca pelas chaves `numero_pc + codigo_cliente + nome`).
> O `fcmp_considerado` permanece 1,0 em todas — o ajuste **não** altera
> linhas anteriores, apenas é derivado delas na reconciliação.

**Banco — `historico_processo_pai`:**
3 linhas novas com `mes_referencia=01, ano_referencia=2026`:
- `999001` → `status_faturado=1, status_pago=1`
- `999002` → `status_faturado=1, status_pago=1`
- `999003` → `status_faturado=1, status_pago=1`

**Banco — `historico_pagamentos_processo_pai`:**
2 linhas novas: `90001B` e `90002A` ambas `situacao_codigo=1`,
`mes_referencia=01, ano_referencia=2026`.

### 5.4 Checklist Mês 3

- [ ] Pai `999001` aparece em `historico_processo_pai` com `status_faturado=1 AND status_pago=1`
- [ ] Nenhum processo de `999001` aparece em "Pagamentos Regulares" (todos absorvidos pela reconciliação)
- [ ] **Não existe** documento em "Pagamentos Regulares" E simultaneamente em "Reconciliação"
- [ ] Todas as 4 linhas anteriores em `historico_comissoes` para o Pai `999001` têm `reconciliado=1`
- [ ] Estorno calculado = comissão base × (valor_devolvido / valor_processo) × (-1)
- [ ] Resumo "Total Líquido" = Ajuste + Estorno (sem adiantamentos nem regulares neste ciclo)
- [ ] `fcmp_real` mostrado no bloco de reconciliação bate com
      `sum(comissao_adiantada × fcmp_aplicado) / sum(comissao_adiantada)` sobre as 4 linhas

---

## 6. Validações globais (ao final dos 3 meses)

Rodar estas queries no `historico.db` e comparar com o esperado:

```sql
-- Total de comissão adiantada registrada para o Pai 999001
SELECT SUM(comissao_adiantada)
FROM historico_comissoes
WHERE numero_pc='999001' AND codigo_cliente='7001';
-- Esperado: 8500

-- FCMP real recalculado
SELECT
  SUM(comissao_adiantada * fcmp_aplicado) / SUM(comissao_adiantada) AS fcmp_real
FROM historico_comissoes
WHERE numero_pc='999001' AND codigo_cliente='7001';
-- Deve bater com o valor mostrado na aba "Reconciliação"

-- Ajuste estimado
SELECT SUM(comissao_adiantada) * (
  (SUM(comissao_adiantada * fcmp_aplicado) / SUM(comissao_adiantada)) - 1.0
) AS ajuste
FROM historico_comissoes
WHERE numero_pc='999001' AND codigo_cliente='7001';
-- Deve bater com o Ajuste do bloco de reconciliação

-- Todos reconciliados
SELECT COUNT(*) FROM historico_comissoes
WHERE numero_pc='999001' AND reconciliado=0;
-- Esperado: 0
```

Invariantes que o pipeline deve respeitar em qualquer cenário:

1. **Adiantamento vs Regular**: prefixo `COT`/`ADT` no Documento AF
   sempre vira ADIANTAMENTO; demais viram REGULAR.
2. **FCMP na competência**: `comissao_total = valor × tcmp × 1,0`.
   O FCMP real só entra financeiramente pelo **ajuste de reconciliação**.
3. **Roteamento exclusivo**: um `(numero_pc, codigo_cliente, processo)`
   reconciliado no mês é suprimido das abas Adiantamentos e Regulares
   do Excel desse mês.
4. **Persistência incremental**: rodar o mesmo mês 2× não deve
   duplicar linhas no SQLite (PK composta garante upsert).
5. **Status do Pai monotônico**: `status_faturamento_completo` só vira
   True quando **todos** os filhos têm `Dt Emissão` preenchida em
   algum mês; `status_pagamento_completo` só vira True quando **todas**
   as parcelas de **todas** as NFs dos filhos estão com `Situação=1` na
   AF (considerando a AF **completa**, não apenas do mês).

---

## 7. Restaurar ambiente pós-teste

```
copy /Y backup_pre_teste\*.xlsx dados_entrada\
copy /Y backup_pre_teste\historico.db historico.db
rmdir /s /q saida
rename saida_pre_teste saida
```
