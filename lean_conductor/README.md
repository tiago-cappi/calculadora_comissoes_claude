# Lean Conductor — Guia de Integração com Claude Code

## O que é isto?

A pasta `lean_conductor/` é uma camada de orquestração que permite ao Claude Code operar o pipeline de comissões consumindo ~90% menos tokens do que o uso direto do `SKILL.md`/`claude.md`.

**Princípio:** O Python calcula rigidamente. O Claude interpreta, orquestra, repara e analisa.

## Estrutura

```
lean_conductor/
├── CLAUDE.md              ← Manifesto compacto (~80 linhas vs ~700 do original)
├── pipeline_wrapper.py    ← Pipeline com JSON output + error collection
├── config_cli.py          ← CLI para gestão de configuração (NL → comando)
├── scenario_runner.py     ← Comparação N cenários com overrides in-memory
├── structured_errors.py   ← Framework de erros auto-explicativos
└── README.md              ← Este arquivo
```

## Como Integrar ao Claude Code

### Passo 1 — Copiar o CLAUDE.md para a raiz do projeto

O Claude Code (e o Claude Desktop com Projects) lê automaticamente o arquivo `CLAUDE.md` na **raiz** do projeto como instruções de sistema. Você precisa substituir (ou renomear) o `claude.md` atual pelo novo manifesto compacto.

```powershell
# Na raiz do projeto (calculadora-comissoes/)
# Backup do original
Rename-Item -Path "claude.md" -NewName "claude_original_backup.md"

# Copiar o novo manifesto
Copy-Item -Path "lean_conductor\CLAUDE.md" -Destination "CLAUDE.md"
```

> **Importante:** O nome do arquivo DEVE ser `CLAUDE.md` (maiúsculo) para que o Claude Code o reconheça automaticamente.

### Passo 2 — Testar os comandos

```powershell
# Testar o pipeline wrapper
python lean_conductor/pipeline_wrapper.py --mes 10 --ano 2025

# Testar a CLI de configuração
python lean_conductor/config_cli.py summary
python lean_conductor/config_cli.py get-pesos
python lean_conductor/config_cli.py list-colaboradores
```

### Passo 3 — Abrir no Claude Code

```powershell
# Abrir o projeto no Claude Code
claude code calculadora-comissoes/
```

Ou, se estiver usando VS Code + extensão Claude:
1. Abra a pasta `calculadora-comissoes/` no VS Code
2. Claude lerá o `CLAUDE.md` automaticamente

## Como Funciona na Prática

### Antes (token-heavy)
```
Usuário: "Calcule outubro 2025"
Claude: [carrega ~700 linhas de regras de negócio na janela de contexto]
Claude: [reimplementa mentalmente cada fórmula]
Claude: [executa scripts com conhecimento completo das regras]
→ ~10.000+ tokens de contexto
```

### Depois (lean)
```
Usuário: "Calcule outubro 2025"
Claude: [lê ~80 linhas do CLAUDE.md — apenas sintaxe de comandos]
Claude: python lean_conductor/pipeline_wrapper.py --mes 10 --ano 2025
Claude: [lê resultado.json — ~50 linhas com status, comissões, erros]
→ ~500 tokens de contexto
```

### Auto-healing de bugs
```
Usuário: "Calcule março 2026"
Claude: python lean_conductor/pipeline_wrapper.py --mes 3 --ano 2026
Claude: [lê resultado.json → status: "partial", 1 erro em fc_calculator]
Claude: [o erro diz: missing_meta para "RH/Equip", fix_suggestion: "Adicionar meta via config_cli"]
Claude: python lean_conductor/config_cli.py set-meta --hierarquia "Recursos Hídricos" --grupo "Equipamentos" --tipo faturamento_linha --valor 500000
Claude: python lean_conductor/pipeline_wrapper.py --mes 3 --ano 2026  ← re-executa
→ Resolvido sem ler regras-negocio.md (~800 tokens total)
```

### Comparação de cenários
```
Usuário: "Compare 3 cenários: atual, faturamento agressivo, e conservador"
Claude: [gera cenarios.json com overrides]
Claude: python lean_conductor/scenario_runner.py --mes 10 --ano 2025 --scenarios cenarios.json
Claude: [lê scenarios_comparison.json]
Claude: [gera script Plotly com Chart interativo]
→ Análise completa sem carregar regras de negócio
```

### Gestão de configuração por linguagem natural
```
Usuário: "Mude o peso de faturamento do Gerente pra 45%"
Claude: python lean_conductor/config_cli.py set-peso --cargo "Gerente Comercial" --componente faturamento_linha --valor 45
```

## Arquitetura de 4 Camadas

| Camada | Quando carrega | Tokens |
|--------|---------------|--------|
| **L1 — CLAUDE.md** | Sempre (startup) | ~500 |
| **L2 — resultado.json** | Após cada execução | ~200-500 |
| **L3 — references/** | Somente se erro não é auto-explicativo | ~500-2000 |
| **L4 — Scripts Python** | Somente para debug cirúrgico | ~300-800 |

**Total médio por sessão: ~700 tokens** vs ~10.000+ do modelo anterior.

## Notas Importantes

- **NÃO** modifica nenhum arquivo existente do projeto
- O pipeline original (`rodar_pipeline.py`) continua funcionando normalmente
- Os overrides do `scenario_runner.py` são **in-memory** — não persistem no Supabase
- Para persistir alterações feitas via `config_cli.py`, use `python lean_conductor/config_cli.py persist`
