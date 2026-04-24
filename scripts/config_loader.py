"""
=============================================================================
SKILL: Robô de Comissões — Script 02: Loader de Regras de Negócio
=============================================================================
Módulo   : 02_config_loader
Versão   : 1.0.0
Autor    : Claude Commission Skill

Descrição
---------
Carrega, valida e normaliza todas as abas do arquivo REGRAS_COMISSOES.xlsx,
retornando um ConfigResult estruturado com todas as regras de negócio
prontas para uso nos scripts de cálculo subsequentes.

Arquivo de Entrada
------------------
REGRAS_COMISSOES.xlsx com as seguintes abas:
  - PARAMS               : Parâmetros operacionais (cap_fc_max, etc.)
  - CARGOS               : Tipos de cargo e elegibilidade
  - COLABORADORES        : Mapa colaborador → cargo
  - CONFIG_COMISSAO      : Taxa de rateio e fatia por hierarquia+cargo
  - PESOS_METAS          : Pesos do FC por cargo
  - METAS_INDIVIDUAIS    : Metas por colaborador
  - METAS_APLICACAO      : Metas por hierarquia (linha)
  - META_RENTABILIDADE   : Metas de rentabilidade por hierarquia+período
  - METAS_FORNECEDORES   : Metas anuais por fornecedor+moeda
  - CROSS_SELLING        : Elegibilidade cross-selling
  - FC_ESCADA_CARGOS     : Configuração rampa/escada por cargo
  - ALIASES              : Normalização de nomes de colaboradores
  - HIERARQUIA           : Catálogo de hierarquia de produtos

Saída
-----
ConfigResult com:
  - params            : dict de parâmetros operacionais
  - cargos            : DataFrame de cargos
  - colaboradores     : DataFrame de colaboradores
  - config_comissao   : DataFrame principal (hierarquia+cargo → taxa+fatia)
  - pesos_metas       : DataFrame de pesos FC
  - metas_individuais : DataFrame de metas individuais
  - metas_aplicacao   : DataFrame de metas por hierarquia
  - meta_rentabilidade: DataFrame de metas de rentabilidade
  - metas_fornecedores: DataFrame de metas de fornecedores
  - cross_selling     : DataFrame de elegibilidade cross-selling
  - fc_escada_cargos  : DataFrame de configuração escada
  - aliases           : dict alias → nome padrão
  - colaboradores_recebimento : set de nomes que recebem por recebimento
  - warnings / errors / ok

Dependências Externas
---------------------
- pandas  (leitura Excel / manipulação de dados)
- openpyxl (engine para .xlsx)
=============================================================================
"""

from __future__ import annotations

import io
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

import pandas as pd


# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTES
# ═══════════════════════════════════════════════════════════════════════════

REQUIRED_SHEETS: List[str] = [
    "PARAMS",
    "CARGOS",
    "COLABORADORES",
    "CONFIG_COMISSAO",
    "PESOS_METAS",
    "FC_ESCADA_CARGOS",
]

OPTIONAL_SHEETS: List[str] = [
    "METAS_INDIVIDUAIS",
    "METAS_APLICACAO",
    "META_RENTABILIDADE",
    "METAS_FORNECEDORES",
    "CROSS_SELLING",
    "ALIASES",
    "HIERARQUIA",
    "ATRIBUICOES",
    "ENUM_TIPO_META",
    "DICIONARIO",
    "README",
]

PARAMS_EXPECTED_KEYS: List[str] = [
    "cap_fc_max",
    "cap_atingimento_max",
]

CARGOS_EXPECTED_COLS: List[str] = [
    "nome_cargo",
    "tipo_cargo",
    "TIPO_COMISSAO",
]

COLABORADORES_EXPECTED_COLS: List[str] = [
    "id_colaborador",
    "nome_colaborador",
    "cargo",
]

CONFIG_COMISSAO_EXPECTED_COLS: List[str] = [
    "linha",
    "grupo",
    "subgrupo",
    "tipo_mercadoria",
    "cargo",
    "taxa_rateio_maximo_pct",
    "fatia_cargo_pct",
    "ativo",
]

PESOS_METAS_EXPECTED_COLS: List[str] = [
    "cargo",
    "faturamento_linha",
]

FC_ESCADA_EXPECTED_COLS: List[str] = [
    "cargo",
    "modo",
    "num_degraus",
    "piso_pct",
]

CROSS_SELLING_EXPECTED_COLS: List[str] = [
    "colaborador",
    "taxa_cross_selling_pct",
]

ALIASES_EXPECTED_COLS: List[str] = [
    "entidade",
    "alias",
    "padrao",
]

# ═══════════════════════════════════════════════════════════════════════════
# DATACLASS DE RESULTADO
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ConfigResult:
    """Resultado estruturado do carregamento de configurações."""

    params: Dict[str, Any] = field(default_factory=dict)
    cargos: pd.DataFrame = field(default_factory=pd.DataFrame)
    colaboradores: pd.DataFrame = field(default_factory=pd.DataFrame)
    config_comissao: pd.DataFrame = field(default_factory=pd.DataFrame)
    pesos_metas: pd.DataFrame = field(default_factory=pd.DataFrame)
    metas_individuais: pd.DataFrame = field(default_factory=pd.DataFrame)
    metas_aplicacao: pd.DataFrame = field(default_factory=pd.DataFrame)
    meta_rentabilidade: pd.DataFrame = field(default_factory=pd.DataFrame)
    metas_fornecedores: pd.DataFrame = field(default_factory=pd.DataFrame)
    cross_selling: pd.DataFrame = field(default_factory=pd.DataFrame)
    fc_escada_cargos: pd.DataFrame = field(default_factory=pd.DataFrame)
    aliases: Dict[str, str] = field(default_factory=dict)
    colaboradores_recebimento: Set[str] = field(default_factory=set)
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """True se não houve erros críticos."""
        return len(self.errors) == 0

    def summary(self) -> str:
        """Retorna resumo do carregamento."""
        lines = [
            "=" * 60,
            "CONFIG LOADER — RESUMO DO CARREGAMENTO",
            "=" * 60,
            "",
            f"  Status............: {'OK' if self.ok else 'ERRO'}",
            f"  Parâmetros........: {len(self.params)} chaves",
            f"  Cargos............: {len(self.cargos)} registros",
            f"  Colaboradores.....: {len(self.colaboradores)} registros",
            f"  Config Comissão...: {len(self.config_comissao)} registros",
            f"  Pesos Metas.......: {len(self.pesos_metas)} cargos",
            f"  Metas Individuais.: {len(self.metas_individuais)} registros",
            f"  Metas Aplicação...: {len(self.metas_aplicacao)} registros",
            f"  Meta Rentabilid...: {len(self.meta_rentabilidade)} registros",
            f"  Metas Fornecedores: {len(self.metas_fornecedores)} registros",
            f"  Cross-Selling.....: {len(self.cross_selling)} colaboradores",
            f"  FC Escada Cargos..: {len(self.fc_escada_cargos)} cargos",
            f"  Aliases...........: {len(self.aliases)} mapeamentos",
            f"  Colab. Recebimento: {len(self.colaboradores_recebimento)} colaboradores",
            "",
        ]

        if self.colaboradores_recebimento:
            lines.append("  Recebem por Recebimento:")
            for name in sorted(self.colaboradores_recebimento):
                lines.append(f"    - {name}")
            lines.append("")

        if self.warnings:
            lines.append(f"  ⚠ Avisos ({len(self.warnings)}):")
            for w in self.warnings:
                lines.append(f"    - {w}")
            lines.append("")

        if self.errors:
            lines.append(f"  ✖ Erros ({len(self.errors)}):")
            for e in self.errors:
                lines.append(f"    - {e}")
            lines.append("")

        lines.append("=" * 60)
        return "\n".join(lines)

    def sample_config_comissao(self, n: int = 10) -> str:
        """Amostra da tabela CONFIG_COMISSAO."""
        if self.config_comissao.empty:
            return "CONFIG_COMISSAO: Vazio"
        cols = [
            "linha", "grupo", "subgrupo", "tipo_mercadoria",
            "cargo", "taxa_rateio_maximo_pct", "fatia_cargo_pct",
        ]
        display_cols = [c for c in cols if c in self.config_comissao.columns]
        return self.config_comissao[display_cols].head(n).to_string(index=False)

    def sample_colaboradores(self) -> str:
        """Amostra completa dos colaboradores."""
        if self.colaboradores.empty:
            return "COLABORADORES: Vazio"
        return self.colaboradores.to_string(index=False)

    def params_display(self) -> str:
        """Exibe parâmetros formatados."""
        lines = ["PARÂMETROS OPERACIONAIS:"]
        for k, v in self.params.items():
            lines.append(f"  {k}: {v}")
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# NORMALIZAÇÃO
# ═══════════════════════════════════════════════════════════════════════════

def _strip_strings_in_df(df: pd.DataFrame) -> pd.DataFrame:
    """Strip de espaços em colunas string e nos nomes de colunas."""
    df = df.copy()
    df.columns = df.columns.astype(str).str.strip()
    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].map(lambda v: v.strip() if isinstance(v, str) else v)
    return df


def _build_aliases(df_aliases: pd.DataFrame) -> Dict[str, str]:
    """Constrói dicionário alias → nome padrão a partir da aba ALIASES."""
    aliases: Dict[str, str] = {}
    if df_aliases.empty:
        return aliases

    for _, row in df_aliases.iterrows():
        entidade = str(row.get("entidade", "")).strip().lower()
        alias_val = row.get("alias")
        padrao_val = row.get("padrao")

        if entidade != "colaborador":
            continue
        if pd.isna(alias_val) or pd.isna(padrao_val):
            continue

        alias_key = str(alias_val).strip().upper()
        padrao_name = str(padrao_val).strip()
        if alias_key and padrao_name:
            aliases[alias_key] = padrao_name

    return aliases


def resolve_alias(name: Any, aliases: Dict[str, str]) -> str:
    """Resolve um nome de colaborador, aplicando aliases se encontrado."""
    if pd.isna(name):
        return ""
    name_str = str(name).strip()
    name_upper = name_str.upper()
    return aliases.get(name_upper, name_str)


# ═══════════════════════════════════════════════════════════════════════════
# VALIDAÇÃO DE COLUNAS
# ═══════════════════════════════════════════════════════════════════════════

def _check_columns(
    df: pd.DataFrame,
    expected: List[str],
    sheet_name: str,
    result: ConfigResult,
) -> bool:
    """Verifica se as colunas esperadas existem no DataFrame."""
    actual = set(df.columns)
    expected_set = set(expected)
    missing = expected_set - actual
    if missing:
        result.warnings.append(
            f"Aba '{sheet_name}': colunas ausentes: {sorted(missing)}"
        )
        return False
    return True


# ═══════════════════════════════════════════════════════════════════════════
# LOADERS INDIVIDUAIS (UM POR ABA)
# ═══════════════════════════════════════════════════════════════════════════

def _load_params(raw: Dict[str, pd.DataFrame], result: ConfigResult) -> None:
    """Carrega e processa a aba PARAMS."""
    df = raw.get("PARAMS")
    if df is None or df.empty:
        result.errors.append("Aba PARAMS não encontrada ou vazia.")
        return

    df = _strip_strings_in_df(df)

    if "chave" not in df.columns or "valor" not in df.columns:
        result.errors.append("Aba PARAMS: colunas 'chave' e 'valor' obrigatórias.")
        return

    params: Dict[str, Any] = {}
    for _, row in df.iterrows():
        key = str(row["chave"]).strip()
        val = row["valor"]
        # Tentar converter para float se possível
        if isinstance(val, str):
            try:
                val = float(val)
            except ValueError:
                pass
        params[key] = val

    # Validar parâmetros obrigatórios
    for key in PARAMS_EXPECTED_KEYS:
        if key not in params:
            result.warnings.append(f"PARAMS: chave '{key}' não encontrada. Usando default.")
            if key == "cap_fc_max":
                params[key] = 1.0
            elif key == "cap_atingimento_max":
                params[key] = 1.0

    # Garantir cross_selling_default_option
    params["cross_selling_default_option"] = str(
        params.get("cross_selling_default_option", "B")
    ).upper()

    result.params = params


def _load_cargos(raw: Dict[str, pd.DataFrame], result: ConfigResult) -> None:
    """Carrega e processa a aba CARGOS."""
    df = raw.get("CARGOS")
    if df is None or df.empty:
        result.errors.append("Aba CARGOS não encontrada ou vazia.")
        return

    df = _strip_strings_in_df(df)
    _check_columns(df, CARGOS_EXPECTED_COLS, "CARGOS", result)
    result.cargos = df


def _load_colaboradores(raw: Dict[str, pd.DataFrame], result: ConfigResult) -> None:
    """Carrega e processa a aba COLABORADORES."""
    df = raw.get("COLABORADORES")
    if df is None or df.empty:
        result.errors.append("Aba COLABORADORES não encontrada ou vazia.")
        return

    df = _strip_strings_in_df(df)
    _check_columns(df, COLABORADORES_EXPECTED_COLS, "COLABORADORES", result)

    # Validar nomes duplicados
    if "nome_colaborador" in df.columns:
        names = df["nome_colaborador"].dropna().str.strip()
        dupes = names[names.duplicated(keep=False)]
        if not dupes.empty:
            result.warnings.append(
                f"COLABORADORES: nomes duplicados detectados: {sorted(dupes.unique().tolist())}"
            )

    result.colaboradores = df


def _load_config_comissao(raw: Dict[str, pd.DataFrame], result: ConfigResult) -> None:
    """Carrega e processa a aba CONFIG_COMISSAO."""
    df = raw.get("CONFIG_COMISSAO")
    if df is None or df.empty:
        result.errors.append("Aba CONFIG_COMISSAO não encontrada ou vazia.")
        return

    df = _strip_strings_in_df(df)
    _check_columns(df, CONFIG_COMISSAO_EXPECTED_COLS, "CONFIG_COMISSAO", result)

    # Filtrar somente registros ativos
    if "ativo" in df.columns:
        total_before = len(df)
        df = df[df["ativo"].astype(str).str.strip().str.upper().isin(["TRUE", "1", "SIM", "S"])]
        inactive_count = total_before - len(df)
        if inactive_count > 0:
            result.warnings.append(
                f"CONFIG_COMISSAO: {inactive_count} registros inativos filtrados."
            )

    # Converter taxas e fatias para float (Excel pode ler inteiros)
    for col in ["taxa_rateio_maximo_pct", "fatia_cargo_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

    result.config_comissao = df.reset_index(drop=True)


def _load_pesos_metas(raw: Dict[str, pd.DataFrame], result: ConfigResult) -> None:
    """Carrega e processa a aba PESOS_METAS."""
    df = raw.get("PESOS_METAS")
    if df is None or df.empty:
        result.errors.append("Aba PESOS_METAS não encontrada ou vazia.")
        return

    df = _strip_strings_in_df(df)
    _check_columns(df, PESOS_METAS_EXPECTED_COLS, "PESOS_METAS", result)

    # Converter colunas de peso para float
    peso_cols = [c for c in df.columns if c != "cargo" and c != "Soma dos pesos"]
    for col in peso_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Validar que pesos somam ~100 por cargo
    if "cargo" in df.columns:
        for _, row in df.iterrows():
            cargo = row.get("cargo", "?")
            total = sum(row.get(c, 0) for c in peso_cols if c in df.columns)
            if abs(total - 100) > 0.01 and total > 0:
                result.warnings.append(
                    f"PESOS_METAS: cargo '{cargo}' soma {total:.1f}% (esperado ~100%)."
                )

    result.pesos_metas = df


def _load_metas_individuais(raw: Dict[str, pd.DataFrame], result: ConfigResult) -> None:
    """Carrega e processa a aba METAS_INDIVIDUAIS."""
    df = raw.get("METAS_INDIVIDUAIS")
    if df is None or df.empty:
        result.warnings.append("Aba METAS_INDIVIDUAIS não encontrada ou vazia.")
        return

    df = _strip_strings_in_df(df)
    if "valor_meta" in df.columns:
        df["valor_meta"] = pd.to_numeric(df["valor_meta"], errors="coerce").fillna(0.0)

    result.metas_individuais = df


def _load_metas_aplicacao(raw: Dict[str, pd.DataFrame], result: ConfigResult) -> None:
    """Carrega e processa a aba METAS_APLICACAO."""
    df = raw.get("METAS_APLICACAO")
    if df is None or df.empty:
        result.warnings.append("Aba METAS_APLICACAO não encontrada ou vazia.")
        return

    df = _strip_strings_in_df(df)
    if "valor_meta" in df.columns:
        df["valor_meta"] = pd.to_numeric(df["valor_meta"], errors="coerce").fillna(0.0)

    result.metas_aplicacao = df


def _load_meta_rentabilidade(raw: Dict[str, pd.DataFrame], result: ConfigResult) -> None:
    """Carrega e processa a aba META_RENTABILIDADE."""
    df = raw.get("META_RENTABILIDADE")
    if df is None or df.empty:
        result.warnings.append("Aba META_RENTABILIDADE não encontrada ou vazia.")
        return

    df = _strip_strings_in_df(df)

    for col in ["referencia_media_ponderada_pct", "meta_rentabilidade_alvo_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    result.meta_rentabilidade = df


def _load_metas_fornecedores(raw: Dict[str, pd.DataFrame], result: ConfigResult) -> None:
    """Carrega e processa a aba METAS_FORNECEDORES."""
    df = raw.get("METAS_FORNECEDORES")
    if df is None or df.empty:
        result.warnings.append("Aba METAS_FORNECEDORES não encontrada ou vazia.")
        return

    df = _strip_strings_in_df(df)

    # Normalizar fabricante → fornecedor (se existir)
    if "fabricante" in df.columns and "fornecedor" not in df.columns:
        df = df.rename(columns={"fabricante": "fornecedor"})

    if "meta_anual" in df.columns:
        df["meta_anual"] = pd.to_numeric(df["meta_anual"], errors="coerce").fillna(0.0)

    result.metas_fornecedores = df


def _load_cross_selling(raw: Dict[str, pd.DataFrame], result: ConfigResult) -> None:
    """Carrega e processa a aba CROSS_SELLING."""
    df = raw.get("CROSS_SELLING")
    if df is None or df.empty:
        result.warnings.append("Aba CROSS_SELLING não encontrada ou vazia.")
        return

    df = _strip_strings_in_df(df)
    _check_columns(df, CROSS_SELLING_EXPECTED_COLS, "CROSS_SELLING", result)

    if "taxa_cross_selling_pct" in df.columns:
        df["taxa_cross_selling_pct"] = pd.to_numeric(
            df["taxa_cross_selling_pct"], errors="coerce"
        ).fillna(0.0)

    result.cross_selling = df


def _load_fc_escada_cargos(raw: Dict[str, pd.DataFrame], result: ConfigResult) -> None:
    """Carrega e processa a aba FC_ESCADA_CARGOS."""
    df = raw.get("FC_ESCADA_CARGOS")
    if df is None or df.empty:
        result.errors.append("Aba FC_ESCADA_CARGOS não encontrada ou vazia.")
        return

    df = _strip_strings_in_df(df)
    _check_columns(df, FC_ESCADA_EXPECTED_COLS, "FC_ESCADA_CARGOS", result)

    if "modo" in df.columns:
        df["modo"] = df["modo"].astype(str).str.strip().str.upper()

    for col in ["num_degraus", "piso_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "num_degraus" in df.columns:
        df["num_degraus"] = df["num_degraus"].astype(int)

    result.fc_escada_cargos = df


def _load_aliases(raw: Dict[str, pd.DataFrame], result: ConfigResult) -> None:
    """Carrega e processa a aba ALIASES."""
    df = raw.get("ALIASES")
    if df is None or df.empty:
        result.warnings.append("Aba ALIASES não encontrada ou vazia.")
        return

    df = _strip_strings_in_df(df)
    _check_columns(df, ALIASES_EXPECTED_COLS, "ALIASES", result)
    result.aliases = _build_aliases(df)


def _detect_recebimento(result: ConfigResult) -> None:
    """Detecta colaboradores que recebem por recebimento via aba CARGOS."""
    receb_set: Set[str] = set()

    # Estratégia 1: Coluna TIPO_COMISSAO em CARGOS
    if not result.cargos.empty and "TIPO_COMISSAO" in result.cargos.columns:
        cargos_receb = result.cargos[
            result.cargos["TIPO_COMISSAO"]
            .astype(str).str.strip().str.lower() == "recebimento"
        ]
        if "nome_cargo" in cargos_receb.columns:
            cargos_receb_names = cargos_receb["nome_cargo"].dropna().str.strip().tolist()

            if cargos_receb_names and not result.colaboradores.empty:
                if "cargo" in result.colaboradores.columns and "nome_colaborador" in result.colaboradores.columns:
                    mask = result.colaboradores["cargo"].isin(cargos_receb_names)
                    colab_names = (
                        result.colaboradores.loc[mask, "nome_colaborador"]
                        .dropna().str.strip().tolist()
                    )
                    receb_set.update(colab_names)

    result.colaboradores_recebimento = receb_set


# ═══════════════════════════════════════════════════════════════════════════
# FUNÇÃO PRINCIPAL — execute()
# ═══════════════════════════════════════════════════════════════════════════

def execute(
    file_regras_comissoes: bytes,
    mes: Optional[int] = None,
    ano: Optional[int] = None,
) -> ConfigResult:
    """
    Carrega e processa o arquivo REGRAS_COMISSOES.xlsx.

    Parameters
    ----------
    file_regras_comissoes : bytes
        Conteúdo binário do arquivo REGRAS_COMISSOES.xlsx.
    mes : int, opcional
        Mês de apuração (usado para filtrar META_RENTABILIDADE).
    ano : int, opcional
        Ano de apuração (usado para filtrar META_RENTABILIDADE).

    Returns
    -------
    ConfigResult
        Resultado estruturado com todas as regras de negócio.
    """
    result = ConfigResult()

    # ── 1. Ler todas as abas do Excel ──────────────────────────────────
    try:
        raw: Dict[str, pd.DataFrame] = pd.read_excel(
            io.BytesIO(file_regras_comissoes),
            sheet_name=None,
            engine="openpyxl",
        )
    except Exception as exc:
        result.errors.append(f"Falha ao ler REGRAS_COMISSOES.xlsx: {exc}")
        return result

    # ── 2. Verificar abas obrigatórias ─────────────────────────────────
    available_sheets = set(raw.keys())
    for sheet in REQUIRED_SHEETS:
        if sheet not in available_sheets:
            result.errors.append(f"Aba obrigatória '{sheet}' não encontrada.")

    if result.errors:
        return result

    # ── 3. Carregar cada aba ───────────────────────────────────────────
    _load_params(raw, result)
    _load_cargos(raw, result)
    _load_colaboradores(raw, result)
    _load_aliases(raw, result)
    _load_config_comissao(raw, result)
    _load_pesos_metas(raw, result)
    _load_metas_individuais(raw, result)
    _load_metas_aplicacao(raw, result)
    _load_meta_rentabilidade(raw, result)
    _load_metas_fornecedores(raw, result)
    _load_cross_selling(raw, result)
    _load_fc_escada_cargos(raw, result)

    # ── 4. Filtrar META_RENTABILIDADE por mês/ano se fornecido ─────────
    if mes and ano and not result.meta_rentabilidade.empty:
        if "mes_ano" in result.meta_rentabilidade.columns:
            target = f"{ano}-{mes:02d}"
            before = len(result.meta_rentabilidade)
            result.meta_rentabilidade = result.meta_rentabilidade[
                result.meta_rentabilidade["mes_ano"].astype(str).str.strip() == target
            ].reset_index(drop=True)
            after = len(result.meta_rentabilidade)
            if after == 0:
                result.warnings.append(
                    f"META_RENTABILIDADE: nenhum registro para {target}. "
                    f"({before} registros disponíveis no total.)"
                )

    # ── 5. Detectar colaboradores que recebem por recebimento ──────────
    _detect_recebimento(result)

    return result


# ═══════════════════════════════════════════════════════════════════════════
# SELF-TESTS
# ═══════════════════════════════════════════════════════════════════════════

def _run_self_tests() -> None:
    """
    Bateria de testes internos para validar o script.
    Pode ser executada com: python config_loader.py --test
    """
    import traceback

    passed = 0
    failed = 0
    total = 0

    def _assert(condition: bool, msg: str) -> None:
        nonlocal passed, failed, total
        total += 1
        if condition:
            passed += 1
            print(f"  ✓ Test {total}: {msg}")
        else:
            failed += 1
            print(f"  ✗ Test {total}: {msg}")

    print("=" * 60)
    print("CONFIG LOADER — SELF-TESTS")
    print("=" * 60)

    # ── Helper: criar Excel fake em memória ────────────────────────────
    def _create_test_excel(**sheet_dfs) -> bytes:
        """Cria um arquivo Excel em memória com as abas fornecidas."""
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            for name, df in sheet_dfs.items():
                df.to_excel(writer, sheet_name=name, index=False)
        return buf.getvalue()

    # ── Dados de teste ─────────────────────────────────────────────────
    df_params = pd.DataFrame({
        "chave": ["cap_fc_max", "cap_atingimento_max", "arred_moeda_decimais",
                   "cross_selling_default_option"],
        "valor": [1.0, 1.0, 2, "B"],
        "observacao": ["Teto FC", "Teto atingimento", "Decimais", "Opção CS"],
    })

    df_cargos = pd.DataFrame({
        "nome_cargo": ["Consultor Interno", "Gerente Linha", "Diretor"],
        "tipo_cargo": ["Operacional", "Gestão", "Gestão"],
        "TIPO_COMISSAO": ["Faturamento", "Recebimento", "Faturamento"],
    })

    df_colaboradores = pd.DataFrame({
        "id_colaborador": ["C001", "C002", "C003"],
        "nome_colaborador": ["Alice", "Bob", "Carlos"],
        "cargo": ["Consultor Interno", "Gerente Linha", "Diretor"],
    })

    df_config = pd.DataFrame({
        "linha": ["Hidrologia", "Hidrologia", "Diversos"],
        "grupo": ["Equip", "Equip", "Geral"],
        "subgrupo": ["Bombas", "Bombas", "Outros"],
        "tipo_mercadoria": ["Produto", "Produto", "Insumo"],
        "cargo": ["Consultor Interno", "Gerente Linha", "Diretor"],
        "taxa_rateio_maximo_pct": [5.0, 5.0, 3.0],
        "fatia_cargo_pct": [30.0, 40.0, 10.0],
        "ativo": [True, True, True],
        "fabricante": [None, None, None],
        "aplicacao": [None, None, None],
        "colaborador": [None, None, None],
    })

    df_pesos = pd.DataFrame({
        "cargo": ["Consultor Interno", "Gerente Linha", "Diretor"],
        "faturamento_linha": [100, 100, 100],
        "rentabilidade": [0, 0, 0],
        "conversao_linha": [0, 0, 0],
        "faturamento_individual": [0, 0, 0],
        "conversao_individual": [0, 0, 0],
        "retencao_clientes": [0, 0, 0],
        "meta_fornecedor_1": [0, 0, 0],
        "meta_fornecedor_2": [0, 0, 0],
    })

    df_fc_escada = pd.DataFrame({
        "cargo": ["Consultor Interno", "Gerente Linha", "Diretor"],
        "modo": ["ESCADA", "ESCADA", "RAMPA"],
        "num_degraus": [2, 2, 0],
        "piso_pct": [50, 50, 0],
    })

    df_metas_ind = pd.DataFrame({
        "colaborador": ["Alice", "Alice"],
        "cargo": ["Consultor Interno", "Consultor Interno"],
        "tipo_meta": ["faturamento", "conversao"],
        "valor_meta": [100, 100],
    })

    df_metas_app = pd.DataFrame({
        "linha": ["Hidrologia", "Hidrologia"],
        "grupo": [None, None],
        "subgrupo": [None, None],
        "tipo_mercadoria": ["Produto", "Produto"],
        "tipo_meta": ["faturamento", "conversao"],
        "valor_meta": [100, 100],
    })

    df_meta_rent = pd.DataFrame({
        "mes_ano": ["2025-10", "2025-10", "2025-09"],
        "tipo_meta": ["rentabilidade", "rentabilidade", "rentabilidade"],
        "linha": ["Hidrologia", "Diversos", "Hidrologia"],
        "grupo": ["Equip", "Geral", "Equip"],
        "subgrupo": ["Bombas", "Outros", "Bombas"],
        "tipo_mercadoria": ["Produto", "Insumo", "Produto"],
        "referencia_media_ponderada_pct": [0.44, 0.55, 0.30],
        "meta_rentabilidade_alvo_pct": [0.50, 0.50, 0.50],
    })

    df_metas_forn = pd.DataFrame({
        "linha": ["Hidrologia"],
        "fabricante": ["YSI"],
        "moeda": ["USD"],
        "meta_anual": [30],
    })

    df_cross = pd.DataFrame({
        "colaborador": ["Carlos"],
        "taxa_cross_selling_pct": [1.5],
    })

    df_aliases = pd.DataFrame({
        "entidade": ["colaborador", "colaborador"],
        "alias": ["CARLOS.SILVA", "BOB.JUNIOR"],
        "padrao": ["Carlos", "Bob"],
    })

    # ── Test 1: Carregamento completo bem-sucedido ─────────────────────
    print("\n--- Teste: Carregamento Completo ---")
    excel_bytes = _create_test_excel(
        PARAMS=df_params,
        CARGOS=df_cargos,
        COLABORADORES=df_colaboradores,
        CONFIG_COMISSAO=df_config,
        PESOS_METAS=df_pesos,
        FC_ESCADA_CARGOS=df_fc_escada,
        METAS_INDIVIDUAIS=df_metas_ind,
        METAS_APLICACAO=df_metas_app,
        META_RENTABILIDADE=df_meta_rent,
        METAS_FORNECEDORES=df_metas_forn,
        CROSS_SELLING=df_cross,
        ALIASES=df_aliases,
    )

    r = execute(excel_bytes, mes=10, ano=2025)

    _assert(r.ok, "Resultado ok = True")
    _assert(len(r.errors) == 0, "Sem erros críticos")

    # ── Test 2: Parâmetros ─────────────────────────────────────────────
    print("\n--- Teste: Parâmetros ---")
    _assert(r.params.get("cap_fc_max") == 1.0, "cap_fc_max = 1.0")
    _assert(r.params.get("cap_atingimento_max") == 1.0, "cap_atingimento_max = 1.0")
    _assert(r.params.get("cross_selling_default_option") == "B", "cross_selling_default = B")

    # ── Test 3: Cargos ─────────────────────────────────────────────────
    print("\n--- Teste: Cargos ---")
    _assert(len(r.cargos) == 3, "3 cargos carregados")
    _assert("Gerente Linha" in r.cargos["nome_cargo"].values, "Gerente Linha presente")

    # ── Test 4: Colaboradores ──────────────────────────────────────────
    print("\n--- Teste: Colaboradores ---")
    _assert(len(r.colaboradores) == 3, "3 colaboradores carregados")
    _assert("Alice" in r.colaboradores["nome_colaborador"].values, "Alice presente")

    # ── Test 5: CONFIG_COMISSAO ────────────────────────────────────────
    print("\n--- Teste: CONFIG_COMISSAO ---")
    _assert(len(r.config_comissao) == 3, "3 registros em CONFIG_COMISSAO")
    _assert(pd.api.types.is_float_dtype(r.config_comissao["taxa_rateio_maximo_pct"]), "taxa_rateio é float")
    _assert(pd.api.types.is_float_dtype(r.config_comissao["fatia_cargo_pct"]), "fatia_cargo é float")

    # ── Test 6: Pesos Metas ────────────────────────────────────────────
    print("\n--- Teste: Pesos Metas ---")
    _assert(len(r.pesos_metas) == 3, "3 cargos em PESOS_METAS")

    # ── Test 7: FC Escada ──────────────────────────────────────────────
    print("\n--- Teste: FC Escada ---")
    _assert(len(r.fc_escada_cargos) == 3, "3 cargos em FC_ESCADA_CARGOS")
    escada_row = r.fc_escada_cargos[r.fc_escada_cargos["cargo"] == "Consultor Interno"].iloc[0]
    _assert(escada_row["modo"] == "ESCADA", "Consultor Interno modo=ESCADA")
    _assert(escada_row["num_degraus"] == 2, "num_degraus = 2")
    _assert(escada_row["piso_pct"] == 50, "piso_pct = 50")

    # ── Test 8: Metas individuais ──────────────────────────────────────
    print("\n--- Teste: Metas Individuais ---")
    _assert(len(r.metas_individuais) == 2, "2 registros em METAS_INDIVIDUAIS")

    # ── Test 9: Metas aplicação ────────────────────────────────────────
    print("\n--- Teste: Metas Aplicação ---")
    _assert(len(r.metas_aplicacao) == 2, "2 registros em METAS_APLICACAO")

    # ── Test 10: Meta rentabilidade filtrada ────────────────────────────
    print("\n--- Teste: Meta Rentabilidade (filtro 10/2025) ---")
    _assert(len(r.meta_rentabilidade) == 2, "2 registros para 2025-10")

    # ── Test 11: Metas fornecedores ────────────────────────────────────
    print("\n--- Teste: Metas Fornecedores ---")
    _assert(len(r.metas_fornecedores) == 1, "1 registro em METAS_FORNECEDORES")
    _assert("fornecedor" in r.metas_fornecedores.columns, "Coluna renomeada para 'fornecedor'")

    # ── Test 12: Cross-Selling ─────────────────────────────────────────
    print("\n--- Teste: Cross-Selling ---")
    _assert(len(r.cross_selling) == 1, "1 colaborador em CROSS_SELLING")

    # ── Test 13: Aliases ───────────────────────────────────────────────
    print("\n--- Teste: Aliases ---")
    _assert(len(r.aliases) == 2, "2 aliases carregados")
    _assert(r.aliases.get("CARLOS.SILVA") == "Carlos", "Alias CARLOS.SILVA → Carlos")
    _assert(resolve_alias("BOB.JUNIOR", r.aliases) == "Bob", "resolve_alias BOB.JUNIOR → Bob")
    _assert(resolve_alias("Alice", r.aliases) == "Alice", "resolve_alias sem match mantém original")

    # ── Test 14: Colaboradores Recebimento ─────────────────────────────
    print("\n--- Teste: Colaboradores Recebimento ---")
    _assert("Bob" in r.colaboradores_recebimento, "Bob detectado como recebimento")
    _assert("Alice" not in r.colaboradores_recebimento, "Alice NÃO é recebimento")
    _assert(len(r.colaboradores_recebimento) == 1, "Apenas 1 colaborador recebimento")

    # ── Test 15: Summary e display ─────────────────────────────────────
    print("\n--- Teste: Summary ---")
    summary = r.summary()
    _assert("CONFIG LOADER" in summary, "Summary contém título")
    _assert("OK" in summary, "Summary mostra OK")

    # ── Test 16: Aba obrigatória ausente ──────────────────────────────
    print("\n--- Teste: Aba Obrigatória Ausente ---")
    bad_excel = _create_test_excel(
        PARAMS=df_params,
        CARGOS=df_cargos,
        # COLABORADORES missing!
        CONFIG_COMISSAO=df_config,
        PESOS_METAS=df_pesos,
        FC_ESCADA_CARGOS=df_fc_escada,
    )
    r2 = execute(bad_excel)
    _assert(not r2.ok, "ok=False quando aba obrigatória falta")
    _assert(any("COLABORADORES" in e for e in r2.errors), "Erro menciona COLABORADORES")

    # ── Test 17: Filtro ativo em CONFIG_COMISSAO ───────────────────────
    print("\n--- Teste: Filtro ativo CONFIG_COMISSAO ---")
    df_config_inactive = df_config.copy()
    df_config_inactive.loc[2, "ativo"] = False  # Terceiro registro inativo
    excel_inactive = _create_test_excel(
        PARAMS=df_params,
        CARGOS=df_cargos,
        COLABORADORES=df_colaboradores,
        CONFIG_COMISSAO=df_config_inactive,
        PESOS_METAS=df_pesos,
        FC_ESCADA_CARGOS=df_fc_escada,
    )
    r3 = execute(excel_inactive)
    _assert(len(r3.config_comissao) == 2, "CONFIG_COMISSAO: 2 ativos após filtro")

    # ── Test 18: Bytes inválidos ───────────────────────────────────────
    print("\n--- Teste: Bytes Inválidos ---")
    r4 = execute(b"not an excel file")
    _assert(not r4.ok, "ok=False para bytes inválidos")
    _assert(len(r4.errors) > 0, "Erro registrado para bytes inválidos")

    # ── Test 19: Meta rentabilidade sem filtro ─────────────────────────
    print("\n--- Teste: Meta Rentabilidade Sem Filtro ---")
    excel_no_filter = _create_test_excel(
        PARAMS=df_params,
        CARGOS=df_cargos,
        COLABORADORES=df_colaboradores,
        CONFIG_COMISSAO=df_config,
        PESOS_METAS=df_pesos,
        FC_ESCADA_CARGOS=df_fc_escada,
        META_RENTABILIDADE=df_meta_rent,
    )
    r5 = execute(excel_no_filter)  # Sem mes/ano
    _assert(len(r5.meta_rentabilidade) == 3, "Sem filtro: 3 registros mantidos")

    # ── Test 20: Params display ────────────────────────────────────────
    print("\n--- Teste: Params Display ---")
    display = r.params_display()
    _assert("cap_fc_max" in display, "params_display mostra cap_fc_max")

    # ── Test 21: Sample CONFIG_COMISSAO ────────────────────────────────
    print("\n--- Teste: Sample CONFIG_COMISSAO ---")
    sample = r.sample_config_comissao(2)
    _assert("Hidrologia" in sample, "Sample mostra Hidrologia")

    # ── Resumo ─────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"RESULTADO: {passed}/{total} testes passaram")
    if failed > 0:
        print(f"  ⚠ {failed} teste(s) falharam!")
        sys.exit(1)
    else:
        print("  ✓ Todos os testes passaram!")
    print("=" * 60)


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if "--test" in sys.argv:
        _run_self_tests()
    else:
        print("Uso: python config_loader.py --test")
        print("  Executa a bateria de testes internos.")
