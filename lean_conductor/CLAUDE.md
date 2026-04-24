# Calculadora de Comissões — Operator Manifest

> **Princípio:** Você é o condutor inteligente. O Python calcula rigidamente.
> Você interpreta, orquestra, repara e analisa. NÃO reimplemente fórmulas — rode os scripts.

## Comandos Disponíveis

### 1. Pipeline Completo (JSON Estruturado)
```bash
python lean_conductor/pipeline_wrapper.py --mes MM --ano AAAA [--colaborador "Nome"] [--cross-selling A|B]
```
Gera `saida/MM_AAAA/resultado.json` com status, etapas, comissões e erros estruturados.
Leia o JSON para interpretar resultados — NÃO parse logs do terminal.

### 2. Gestão de Configuração (Linguagem Natural → CLI)
```bash
# Consultas
python lean_conductor/config_cli.py get-pesos [--cargo X] [--colaborador Y]
python lean_conductor/config_cli.py get-meta --tipo faturamento_linha|conversao_linha|rentabilidade [--hierarquia "RH/Equip"] [--colaborador Y]
python lean_conductor/config_cli.py get-params
python lean_conductor/config_cli.py list-colaboradores
python lean_conductor/config_cli.py get-regras [--linha X] [--cargo Y]
python lean_conductor/config_cli.py get-fc-escada
python lean_conductor/config_cli.py summary

# Modificações
python lean_conductor/config_cli.py set-peso --cargo X --componente faturamento_linha --valor 0.45 [--colaborador Y]
python lean_conductor/config_cli.py set-meta --tipo faturamento_linha --valor 600000 --hierarquia "RH" [--grupo "Equip"]
python lean_conductor/config_cli.py set-meta-individual --colaborador "Nome" --tipo faturamento_individual --valor 200000
python lean_conductor/config_cli.py set-meta-rentabilidade --linha "RH" --referencia 0.15 --alvo 0.12
python lean_conductor/config_cli.py set-fc-escada --cargo "Gerente Comercial" --modo ESCADA --degraus 5 --piso 0.6
python lean_conductor/config_cli.py persist [--filename cache.json]
```

### 3. Comparação de Cenários
```bash
python lean_conductor/scenario_runner.py --mes MM --ano AAAA --scenarios cenarios.json
```
Gera `saida/MM_AAAA/scenarios_comparison.json`. Use para gerar gráficos Plotly comparativos.

**Formato do cenarios.json:**
```json
[
  {"nome": "Atual", "overrides": {}},
  {"nome": "Agressive", "overrides": {"pesos_metas": [{"cargo": "Gerente Comercial", "faturamento_linha": 50}], "metas_aplicacao": [{"linha": "RH", "tipo_meta": "faturamento_linha", "valor_meta": 700000}]}}
]
```

## Estrutura do resultado.json
```json
{
  "status": "ok | partial | error",
  "mes": 10, "ano": 2025,
  "etapas": [{"id": 1, "nome": "Loader", "status": "ok", "detalhes": "342 itens"}],
  "comissoes": [{"colaborador": "Nome", "cargo": "X", "total_faturamento": 4200.00, "itens": 15}],
  "total_geral": 25000.00,
  "erros": [{"stage": "fc_calculator", "tipo": "missing_meta", "contexto": {...}, "impacto": "...", "recovery": "continued", "fix_suggestion": "..."}],
  "avisos": ["..."]
}
```

## Quando Ler Documentação Profunda
Leia `references/` SOMENTE nestes casos:
- Bug que o erro estruturado não explica → leia `references/regras-negocio.md`
- Dúvida sobre colunas de arquivo Excel → leia `references/schema-excel.md`
- Termo desconhecido → leia `references/glossario.md`
- Lógica de rentabilidade → leia `references/rentabilidade-calculo.md`

## Mapa de Erros → Script Responsável
| Erro | Script | Ação |
|------|--------|------|
| Arquivo não encontrado | `scripts/loaders.py` | Verificar `dados_entrada/` |
| Coluna faltando | `scripts/loaders.py` | Verificar schema do Excel |
| Atribuição sem regra | `scripts/atribuicao.py` | Adicionar via config_cli |
| Meta não encontrada | `scripts/fc_calculator.py` | Adicionar meta via config_cli |
| FC = 0 inesperado | `scripts/fc_calculator.py` | Verificar pesos/metas |
| Cross-selling | `scripts/atribuicao.py` | Confirmar opção A ou B |
| Excel export falhou | `scripts/excel_export.py` | Verificar permissões |

## Arquivos de Entrada (em `dados_entrada/`)
| Arquivo | Obrigatório | Conteúdo |
|---------|-------------|----------|
| `analise-comercial.xlsx` | Sim | Processos e faturamento |
| `Classificação de Produtos.xlsx` | Sim | Hierarquia de produtos |
| `analise-financeira.xlsx` | Não | Pagamentos recebidos |
| `devolucoes.xlsx` | Não | Notas de devolução |
| `fat_rent_gpe.csv` | Não | Rentabilidade por produto |

## Saída (em `saida/MM_AAAA/`)
- `resultado.json` — Resultado estruturado para Claude
- `comissao_<nome>_MM_AAAA.xlsx` — Excel faturamento por colaborador (3 abas: Resumo, Detalhe, FC)
- `comissao_<nome>_MM_AAAA.md` — Markdown faturamento por colaborador (lido por relatorio_faturamento)
- `recebimento_<nome>_MM_AAAA.md` — Markdown recebimento por colaborador (lido por relatorio_recebimento)
- `relatorio_comissoes_MM_AAAA.html` — HTML standalone completo (faturamento + recebimento)
- `scenarios_comparison.json` — Comparação de cenários (se gerado)

**Nota:** Os arquivos .md e o resultado.json também são mantidos em memória (cache inline) durante a
sessão MCP. relatorio_faturamento() e relatorio_recebimento() usam o cache automaticamente quando
os arquivos em disco não existem (ex.: PermissionError na pasta saida/).
