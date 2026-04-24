"""
rodar_pipeline.py — Pipeline da Calculadora de Comissões
Clean Environment Brasil

Uso
---
    python rodar_pipeline.py --mes 10 --ano 2025
    python rodar_pipeline.py --mes 3  --ano 2026
    python rodar_pipeline.py --mes 10 --ano 2025 --colaborador "Dener Martins"

Requisitos
----------
    pip install pandas openpyxl

    Antes de rodar, gere o cache do Supabase:
    python scripts/cache_builder.py --from-supabase
    (ou copie um supabase_cache.json gerado previamente para ~/supabase_cache.json)

Arquivos esperados em ./dados_entrada/
    - analise-comercial.xlsx
    - Classificação de Produtos.xlsx
    - fat_rent_gpe.csv           (opcional — rentabilidade)
    - analise-financeira.xlsx    (opcional)
    - devolucoes.xlsx            (opcional)

Relatórios gerados em ./saida/MM_AAAA/
    - comissao_<colaborador>_MM_AAAA.xlsx  (um por colaborador)
"""

import sys
import os
import argparse
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────
# ARGUMENTOS DE LINHA DE COMANDO
# ─────────────────────────────────────────────────────────────────────

_parser = argparse.ArgumentParser(description="Pipeline de Comissões — Clean Environment Brasil")
_parser.add_argument("--mes",          type=int, default=10,   help="Mês de apuração (1-12). Padrão: 10")
_parser.add_argument("--ano",          type=int, default=2025, help="Ano de apuração. Padrão: 2025")
_parser.add_argument("--colaborador",  type=str, default=None, help="Filtrar por nome de colaborador (opcional; padrão: todos)")
_parser.add_argument("--dados-dir",    type=str, default="./dados_entrada", help="Diretório com arquivos de entrada no padrão de produção")
_args = _parser.parse_args()

# ─────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────────

MES  = _args.mes
ANO  = _args.ano

# Colaborador(es) a filtrar para FC + Comissão (None = todos)
FILTRAR_COLABORADOR = _args.colaborador

# Diretório dos arquivos de entrada
DADOS_DIR = Path(_args.dados_dir)

# Nomes dos arquivos
ARQUIVO_AC              = DADOS_DIR / "analise-comercial.xlsx"
ARQUIVO_CLASSIFICACAO   = DADOS_DIR / "Classificação de Produtos.xlsx"
ARQUIVO_FINANCEIRA      = DADOS_DIR / "analise-financeira.xlsx"         # opcional
ARQUIVO_DEVOLUCOES      = DADOS_DIR / "devolucoes.xlsx"                 # opcional
ARQUIVO_FAT_RENT        = DADOS_DIR / "fat_rent_gpe.csv"                # opcional
_PROCESSO_PEDIDO_CANDIDATOS = [
    DADOS_DIR / "Processo x Pedido de Compra.xlsx",
    DADOS_DIR / "processo_pedido_compra.xlsx",
]
ARQUIVO_PROCESSO_PEDIDO = next(
    (p for p in _PROCESSO_PEDIDO_CANDIDATOS if p.exists()),
    _PROCESSO_PEDIDO_CANDIDATOS[0],
)


# ─────────────────────────────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────────────────────────────

# Garante que os scripts da skill estão no path.
# Em modo congelado (PyInstaller), __file__ fica em _internal/ — usamos a
# pasta do .exe para localizar dados_entrada/, saida/ e configuracoes.xlsx.
if getattr(sys, "frozen", False):
    ROOT = Path(sys.executable).parent
else:
    ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

import warnings
warnings.filterwarnings("ignore")


def _ler_bytes(path: Path):
    """Lê arquivo como bytes, retorna None se não existir."""
    if path.exists():
        return path.read_bytes()
    print(f"  ⚠ Arquivo não encontrado (opcional): {path}")
    return None


# ─────────────────────────────────────────────────────────────────────
# ETAPA 0: Verificar arquivo de configuração (Excel)
# ─────────────────────────────────────────────────────────────────────

print("=" * 60)
print(f"  CALCULADORA DE COMISSOES - {MES:02d}/{ANO}")
print("=" * 60)

CONFIG_EXCEL = ROOT / "configuracoes_comissoes.xlsx"

print("\n[0] Verificando arquivo de configuracao...")
if not CONFIG_EXCEL.exists():
    print(f"  [X] Arquivo nao encontrado: {CONFIG_EXCEL}")
    print("      Gere-o executando: python scripts/gerar_template_excel.py")
    sys.exit(1)
print(f"  [OK] {CONFIG_EXCEL.name}")

from scripts.excel_config_loader import diagnose as _excel_diagnose
print(_excel_diagnose())

# ─────────────────────────────────────────────────────────────────────
# ETAPA 1: Verificar arquivos de entrada
# ─────────────────────────────────────────────────────────────────────

print("\n[1] Verificando arquivos de entrada...")
for f in [ARQUIVO_AC, ARQUIVO_CLASSIFICACAO]:
    if not f.exists():
        print(f"  ✖ OBRIGATÓRIO não encontrado: {f}")
        sys.exit(1)
    print(f"  ✓ {f.name}")
for f in [ARQUIVO_FINANCEIRA, ARQUIVO_DEVOLUCOES, ARQUIVO_FAT_RENT]:
    status = "✓" if f.exists() else "⚠ (opcional, pulado)"
    print(f"  {status} {f.name}")

# ─────────────────────────────────────────────────────────────────────
# ETAPA 2: Carregar arquivos
# ─────────────────────────────────────────────────────────────────────

print(f"\n[2] Carregando arquivos — {MES:02d}/{ANO}...")
import scripts.loaders as loader

result = loader.execute(
    mes=MES,
    ano=ANO,
    file_analise_comercial=ARQUIVO_AC.read_bytes(),
    file_classificacao_produtos=ARQUIVO_CLASSIFICACAO.read_bytes(),
    file_analise_financeira=_ler_bytes(ARQUIVO_FINANCEIRA),
    file_devolucoes=_ler_bytes(ARQUIVO_DEVOLUCOES),
    file_rentabilidade=None,
    file_processo_pedido=_ler_bytes(ARQUIVO_PROCESSO_PEDIDO),
)
print(result.summary())

if not result.ok:
    print("\n✖ Erros no carregamento:")
    for e in result.errors:
        print(f"  • {e}")
    sys.exit(1)

ac = result.analise_comercial
ac_full = result.analise_comercial_full

# Relatório de Código Produto sem correspondência na CP
if result.codigos_sem_correspondencia:
    _saida_dir = Path(f"./saida/{MES:02d}_{ANO}")
    try:
        _saida_dir.mkdir(parents=True, exist_ok=True)
    except (PermissionError, OSError) as _exc:
        print(f"  ⚠ Não foi possível criar pasta de saída: {_exc}")
        _saida_dir = None
if result.codigos_sem_correspondencia and _saida_dir is not None:
    _relatorio_path = _saida_dir / f"codigos_sem_correspondencia_{MES:02d}_{ANO}.txt"
    with open(_relatorio_path, "w", encoding="utf-8") as _f:
        _f.write(f"RELATÓRIO: Código Produto sem correspondência na Classificação de Produtos\n")
        _f.write(f"Período: {MES:02d}/{ANO}\n")
        _f.write(f"Filtros aplicados: Operação (válidas) + Dt Emissão (mês/ano)\n")
        _f.write(f"Total de códigos sem correspondência: {len(result.codigos_sem_correspondencia)}\n")
        _f.write("=" * 80 + "\n\n")
        _f.write(f"{'Código Produto':<25} {'Processo':<15} {'Descrição'}\n")
        _f.write("-" * 80 + "\n")
        for item in result.codigos_sem_correspondencia:
            _f.write(f"{item['codigo_produto']:<25} {item['processo']:<15} {item['descricao']}\n")
    print(f"  ⚠ {len(result.codigos_sem_correspondencia)} código(s) sem correspondência na CP.")
    print(f"    Relatório salvo em: {_relatorio_path}")

# ─────────────────────────────────────────────────────────────────────
# ETAPA 3b: Rentabilidade (opcional)
# ─────────────────────────────────────────────────────────────────────

df_fat_rent = None
if ARQUIVO_FAT_RENT.exists():
    print("\n[3b] Parseando fat_rent_gpe.csv (rentabilidade por produto)...")
    from scripts.parse_fat_rent_gpe import execute as parse_fat

    parse_res = parse_fat(source=ARQUIVO_FAT_RENT.read_bytes())
    print(parse_res.summary())

    if parse_res.ok:
        df_fat_rent = parse_res.to_dataframe()
else:
    print("\n[3b] fat_rent_gpe.csv não fornecido — componente rentabilidade será 0.")

# ─────────────────────────────────────────────────────────────────────
# ETAPA 4: Atribuição
# ─────────────────────────────────────────────────────────────────────

print("\n[4] Atribuindo comissões...")
import scripts.atribuicao as atrib

result_atrib = atrib.execute(ac)
print(result_atrib.summary())

if result_atrib.warnings:
    print(f"\n  ⚠ {len(result_atrib.warnings)} aviso(s) de atribuição:")
    for w in result_atrib.warnings[:20]:
        print(f"    • {w}")

if not result_atrib.ok:
    print("\n✖ Erros na atribuição:")
    for e in result_atrib.errors:
        print(f"  • {e}")
    sys.exit(1)

df_atrib = result_atrib.to_dataframe()

# Cross-selling — abre modal para o usuário escolher A ou B por consultor
from scripts.cross_selling_modal import ask_cross_selling_options
cs_opcoes: dict = {}
if result_atrib.cross_selling_cases:
    n_cs = len(result_atrib.cross_selling_cases)
    print(f"\n  ⚠ {n_cs} caso(s) de cross-selling detectado(s). Abrindo seleção...")
    cs_opcoes = ask_cross_selling_options(result_atrib.cross_selling_cases)
    for consultor, opcao in cs_opcoes.items():
        print(f"    {consultor}: Opção {opcao}")

# ─────────────────────────────────────────────────────────────────────
# ETAPA 5: Realizados
# ─────────────────────────────────────────────────────────────────────

# Resolver aliases na AC para que os nomes dos colaboradores
# sejam canônicos (ex: "DENER.MARTINS" → "Dener Martins")
_alias_map = atrib._load_config()["alias_map"]
ac_resolved = atrib.apply_aliases_to_df(ac, _alias_map)
ac_full_resolved = atrib.apply_aliases_to_df(ac_full, _alias_map)

print("\n[5] Calculando realizados...")
import scripts.realizados as reais

result_reais = reais.execute(
    df_analise_comercial=ac_resolved,
    df_atribuicoes=df_atrib,
    df_fat_rent_gpe=df_fat_rent,
    mes=MES,
    ano=ANO,
    df_ac_full=ac_full_resolved,
)
print(result_reais.summary())

# ─────────────────────────────────────────────────────────────────────
# ETAPA 6: FC
# ─────────────────────────────────────────────────────────────────────

print("\n[6] Calculando Fator de Correção (FC)...")
import scripts.fc_calculator as fc

# Filtrar por colaborador(es) se configurado
if FILTRAR_COLABORADOR:
    if isinstance(FILTRAR_COLABORADOR, str):
        nomes_filtro = [FILTRAR_COLABORADOR]
    else:
        nomes_filtro = list(FILTRAR_COLABORADOR)
    df_para_fc = df_atrib[df_atrib["nome"].isin(nomes_filtro)].copy()
    print(f"  Filtrando por: {nomes_filtro} ({len(df_para_fc)} itens)")
else:
    df_para_fc = df_atrib

result_fc = fc.execute(df_para_fc, result_reais)
print(result_fc.summary())

# Detalhe do FC por hierarquia
print("\n  ── FC Detalhado por colaborador/hierarquia ──")
for fcr in result_fc.resultados:
    nome = fcr.colaborador
    hierarquia_label = getattr(fcr, "hierarquia_key", "") or fcr.linha
    print(f"\n  {nome} / {hierarquia_label}")
    print(f"    Modo: {fcr.modo} | FC_rampa: {fcr.fc_rampa:.4f} | FC_final: {fcr.fc_final:.4f}")
    for comp in fcr.componentes:
        peso = comp.peso
        if peso > 0:
            real  = comp.realizado
            meta  = comp.meta
            ating = comp.atingimento
            ating_cap = comp.atingimento_cap
            contrib   = comp.contribuicao
            nome_comp = comp.nome
            real_fmt = f"R$ {real:,.2f}" if isinstance(real, (int, float)) else str(real)
            meta_fmt = f"R$ {meta:,.2f}" if isinstance(meta, (int, float)) else str(meta)
            print(f"    • {nome_comp:<30} real={real_fmt}  meta={meta_fmt}  "
                  f"ating={ating:.1%}  cap={ating_cap:.1%}  peso={peso}%  contrib={contrib:.4f}")

# ─────────────────────────────────────────────────────────────────────
# ETAPA 7: Comissão por Faturamento
# ─────────────────────────────────────────────────────────────────────

print("\n[7] Calculando comissões por faturamento...")
import scripts.comissao_faturamento as cf
from scripts.terminal_display import print_df

result_fat = cf.execute(
    atribuicoes=df_para_fc,
    fc_result_set=result_fc,
    cross_selling_cases=result_atrib.cross_selling_cases,
    cross_selling_option=cs_opcoes if cs_opcoes else "B",
)
print(result_fat.summary())

df_com = result_fat.to_dataframe()

# ─────────────────────────────────────────────────────────────────────
# RESULTADO FINAL
# ─────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("  RESULTADO FINAL — COMISSÕES POR FATURAMENTO")
print("=" * 60)

# Detalhe por item
cols_detalhe = ["Processo", "nome", "linha", "Grupo", "Valor Realizado",
                "taxa_rateio_pct", "fatia_cargo_pct",
                "fc_aplicado", "comissao_final"]
cols_existentes = [c for c in cols_detalhe if c in df_com.columns]

print("\n  Detalhe por item:")
print_df(df_com[cols_existentes], title="Detalhe por Item")

# Consolidado por colaborador
print("\n  Consolidado por colaborador:")
df_consol = result_fat.consolidar_por_colaborador()
print_df(df_consol, title="Consolidado por Colaborador")

print("\n" + "=" * 60)
total = df_com["comissao_final"].sum() if "comissao_final" in df_com.columns else 0
print(f"  TOTAL COMISSÃO: R$ {total:,.2f}")
print("=" * 60)

# ─────────────────────────────────────────────────────────────────────
# ETAPA 8: Exportar Excel por Colaborador
# ─────────────────────────────────────────────────────────────────────

print("\n[8] Exportando Excel por colaborador...")
import scripts.excel_export as excel_export

try:
    result_excel = excel_export.execute(result_fat, result_fc, MES, ANO, df_ac=ac_resolved)
    print(result_excel.summary())
except (PermissionError, OSError) as _exc_excel:
    print(f"  ⚠ Excel não exportado (não crítico): {_exc_excel}")
    print("    Verifique se algum arquivo .xlsx está aberto no Excel e tente novamente.")

# ─────────────────────────────────────────────────────────────────────
# ETAPA 9: Pipeline de Recebimento (receita.pipeline.runner)
# ─────────────────────────────────────────────────────────────────────

af = result.analise_financeira
tabela_pc = getattr(result, "processo_pedido", None)
pipeline_rec = None
af_full = None

if ARQUIVO_FINANCEIRA.exists():
    try:
        af_full, _warnings_af_full = loader.load_analise_financeira_full(ARQUIVO_FINANCEIRA.read_bytes())
        for _w in _warnings_af_full:
            print(f"  ⚠ {_w}")
    except Exception as _exc_af_full:
        print(f"  ⚠ load_analise_financeira_full falhou: {_exc_af_full}")

if af is not None and not af.empty:
    print("\n" + "=" * 60)
    print("  PIPELINE DE RECEBIMENTO")
    print("=" * 60)
    try:
        from receita.pipeline.runner import executar as exec_receita
        from scripts import supabase_loader as _sl_rec
        _pesos_indexed, _escada_por_cargo = fc._load_config()
        _params_fc = None  # runner usa default {cap_fc_max: 1.0, cap_atingimento_max: 1.2}
        pipeline_rec = exec_receita(
            df_analise_financeira=af,
            df_ac_full=ac_full_resolved,
            realizados_result=result_reais,
            tabela_pc=tabela_pc,
            df_devolucoes=result.devolucoes,
            mes=MES,
            ano=ANO,
            saida_dir=f"saida/{MES:02d}_{ANO}",
            config_comissao=_sl_rec.load_json("config_comissao.json"),
            colaboradores=_sl_rec.load_json("colaboradores.json"),
            cargos=_sl_rec.load_json("cargos.json"),
            pesos_metas=_pesos_indexed,
            fc_escada=_escada_por_cargo,
            params=_params_fc,
            df_af_full=af_full,
        )
        print(f"  ✓ Pipeline recebimento executado.")
        if pipeline_rec.comissao_result:
            total_rec = sum(pipeline_rec.comissao_result.total_por_gl.values())
            print(f"  ✓ Total comissão recebimento: R$ {total_rec:,.2f}")
        for _w in (pipeline_rec.warnings or []):
            print(f"  ⚠ {_w}")
        for _e in (pipeline_rec.errors or []):
            print(f"  ✖ {_e}")
    except Exception as _exc_rec:
        print(f"  ⚠ Pipeline recebimento falhou (não crítico): {_exc_rec}")
else:
    print("\n⚠ analise-financeira.xlsx não disponível — pipeline de recebimento pulado.")
