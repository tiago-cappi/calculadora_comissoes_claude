"""
=============================================================================
SKILL: Robô de Comissões — Script 01: Loader de Arquivos de Entrada
=============================================================================
Módulo   : 01_loaders
Versão   : 1.0.0
Autor    : Claude Commission Skill

Descrição
---------
Carrega, valida, filtra e enriquece os 5 arquivos de entrada necessários
para o cálculo de comissões, retornando DataFrames prontos para uso
nos scripts subsequentes da Skill.

Arquivos de Entrada Esperados
-----------------------------
1. analise-comercial.xlsx          — Faturamento / processos comerciais
2. Classificação de Produtos.xlsx  — Hierarquia de produtos (Linha/Grupo/Subgrupo)
3. analise-financeira.xlsx         — Recebimentos / baixas financeiras
4. devolucoes.xlsx                 — Notas de devolução
5. rentabilidade_MM_AAAA_agrupada.xlsx — Rentabilidade mensal por hierarquia

Saída
-----
Dicionário com 5 DataFrames filtrados pelo mês/ano de apuração:
  - "analise_comercial"  (enriquecida com Classificação de Produtos)
  - "classificacao_produtos"
  - "analise_financeira"
  - "devolucoes"
  - "rentabilidade"

Dependências Externas
---------------------
- pandas  (leitura Excel / CSV e manipulação de dados)
- openpyxl (engine para .xlsx — instalado junto com pandas)
=============================================================================
"""

from __future__ import annotations

import io
import sys
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


def _debug_loader_boot(message: str) -> None:
    try:
        print(f"[pipeline-debug import::loaders] {message}", flush=True)
    except Exception:
        pass


_debug_loader_boot("Modulo scripts.loaders iniciado.")
import pandas as pd
_debug_loader_boot("Pandas importado em scripts.loaders.")


def _debug_loader(message: str, **details: Any) -> None:
    try:
        from lean_conductor.live_debug import log_current_event

        log_current_event("info", "loader", "execute", message, details)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

# ---------- Análise Comercial ----------
AC_DATE_COL = "Dt Emissão"
AC_STATUS_COL = "Status Processo"
AC_JOIN_KEY = "Código Produto"

# Colunas que esperamos encontrar na Análise Comercial
# NOTA: Colunas de hierarquia (Linha, Grupo, Subgrupo, Tipo de Mercadoria,
# Fabricante) NÃO existem nativamente na AC — vêm exclusivamente do
# LEFT JOIN com a Classificação de Produtos (CP).
AC_EXPECTED_COLS: List[str] = [
    "Processo",
    "Status Processo",
    "Numero NF",
    "Dt Emissão",
    "Valor Realizado",
    "Valor Orçado",
    "Consultor Interno",
    "Representante-pedido",
    "Gerente Comercial-Pedido",
    "Aplicação Mat./Serv.",
    "Cliente",
    "Nome Cliente",
    "Cidade",
    "UF",
    "Código Produto",
    "Descrição Produto",
    "Qtde Atendida",
    "Operação",
    "Centro Custo-pedido",
]

# ---------- Operações válidas da AC (filtro pré-processamento) ----------
AC_VALID_OPERATIONS = {
    "COS", "COT", "FLOC", "IMO2", "IMO3", "OR19",
    "P205", "P804", "PSEM", "PSER", "PVEN", "PVMA", "SERV",
}
AC_VALID_OPERATION_PREFIXES = {"PDIR"}
AC_OPERACAO_COL = "Operação"

# ---------- Classificação de Produtos ----------
CP_JOIN_KEY = "Código Produto"
CP_EXPECTED_COLS: List[str] = [
    "Código Produto",
    "Descrição Produto",
    "Linha",
    "Grupo",
    "Subgrupo",
    "Tipo de Mercadoria",
    "Fabricante",
]

# Sufixo para colunas duplicadas após LEFT JOIN
# Apenas colunas que existem em AMBOS os arquivos recebem sufixo (ex: Descrição Produto).
# Colunas de hierarquia (Linha, Grupo, etc.) existem SOMENTE na CP, portanto
# entram no merge SEM sufixo e são a fonte autoritativa da hierarquia.
CP_SUFFIX = "_cp"

# ---------- Análise Financeira ----------
AF_DATE_COL = "Data de Baixa"
AF_TIPO_BAIXA_COL = "Tipo de Baixa"
AF_TIPO_BAIXA_EXCLUIR = "A"  # Excluir somente 'A'; manter 'B', vazio e demais valores

AF_COL_ALIASES: Dict[str, List[str]] = {
    "Documento": ["Documento", "documento", "DOCUMENTO"],
    "Valor Líquido": [
        "Valor Líquido",
        "Valor Liquido",
        "valor líquido",
        "VALOR LIQUIDO",
    ],
    "Data de Baixa": [
        "Data de Baixa",
        "Data Baixa",
        "data de baixa",
        "DATA BAIXA",
    ],
    "Tipo de Baixa": [
        "Tipo de Baixa",
        "Tipo Baixa",
        "tipo de baixa",
        "TIPO BAIXA",
    ],
    "Situação": [
        "Situação",
        "Situacao",
        "situação",
        "SITUACAO",
        "SITUAÇÃO",
        "Situação Pagamento",
        "situacao pagamento",
    ],
    "Dt. Prorrogação": [
        "Dt. Prorrogação",
        "Dt Prorrogação",
        "Dt. Prorrogacao",
        "dt. prorrogação",
        "DT. PRORROGAÇÃO",
        "DT PRORROGACAO",
        "Data Prorrogação",
        "Data Prorrogacao",
    ],
}

# ---------- Devoluções ----------
DEV_DATE_COL = "Data de Entrada"
DEV_COL_ALIASES: Dict[str, List[str]] = {
    "Num docorigem": [
        "Num docorigem",
        "num docorigem",
        "NUM DOCORIGEM",
        "Numero Doc Origem",
    ],
    "Data de Entrada": [
        "Data de Entrada",
        "data de entrada",
        "DATA DE ENTRADA",
        "Data Entrada",
    ],
    "Valor Produtos": [
        "Valor Produtos",
        "valor produtos",
        "VALOR PRODUTOS",
        "Valor",
    ],
    "Código Operação": [
        "Código Operação",
        "codigo operacao",
        "CODIGO OPERACAO",
        "Cod Operacao",
    ],
}

# ---------- Rentabilidade ----------
RENT_DATE_COL = None  # Sem coluna de data; filtro é pelo nome do arquivo.
RENT_EXPECTED_COLS: List[str] = [
    "Linha",
    "Grupo",
    "Subgrupo",
    "Tipo de Mercadoria",
    "rentabilidade_realizada_pct",
]

# Alias para primeira coluna (padrão Linha)
RENT_LINHA_ALIASES: List[str] = ["Linha", "linha", "Negócio", "Negocio", "NEGOCIO", "LINHA"]


# ═══════════════════════════════════════════════════════════════════════════════
# DATA CLASSES — Resultado Estruturado
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class LoaderResult:
    """Resultado completo do carregamento de dados de entrada."""

    analise_comercial: pd.DataFrame = field(default_factory=pd.DataFrame)
    analise_comercial_full: pd.DataFrame = field(default_factory=pd.DataFrame)
    classificacao_produtos: pd.DataFrame = field(default_factory=pd.DataFrame)
    analise_financeira: pd.DataFrame = field(default_factory=pd.DataFrame)
    devolucoes: pd.DataFrame = field(default_factory=pd.DataFrame)
    rentabilidade: pd.DataFrame = field(default_factory=pd.DataFrame)

    # Tabela Processo x Pedido de Compra (opcional — pipeline de Recebimento)
    processo_pedido: Any = None  # ProcessoPedidoTabela | None

    # Metadados de validação
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    # Código Produto sem correspondência na CP (após filtro Operação + Dt Emissão)
    codigos_sem_correspondencia: List[Dict[str, str]] = field(default_factory=list)

    # Parâmetros utilizados
    mes: int = 0
    ano: int = 0

    @property
    def ok(self) -> bool:
        """True se não houve erros críticos."""
        return len(self.errors) == 0

    def summary(self) -> str:
        """Resumo textual legível do carregamento."""
        lines = [
            f"{'='*60}",
            f"  LOADER — Resumo do Carregamento  (Mês/Ano: {self.mes:02d}/{self.ano})",
            f"{'='*60}",
            f"  Análise Comercial (enriquecida) : {len(self.analise_comercial):>7,} linhas",
            f"  Classificação de Produtos       : {len(self.classificacao_produtos):>7,} linhas",
            f"  Análise Financeira              : {len(self.analise_financeira):>7,} linhas",
            f"  Devoluções                      : {len(self.devolucoes):>7,} linhas",
            f"  Rentabilidade                   : {len(self.rentabilidade):>7,} linhas",
            f"{'─'*60}",
        ]
        if self.warnings:
            lines.append(f"  ⚠ Avisos ({len(self.warnings)}):")
            for w in self.warnings:
                lines.append(f"    • {w}")
        if self.errors:
            lines.append(f"  ✖ Erros ({len(self.errors)}):")
            for e in self.errors:
                lines.append(f"    • {e}")
        if not self.warnings and not self.errors:
            lines.append("  ✔ Nenhum aviso ou erro.")
        lines.append(f"{'='*60}")
        return "\n".join(lines)

    def sample(self, n: int = 10) -> str:
        """Retorna amostra textual de N linhas de cada DataFrame."""
        sep = "\n" + "─" * 60 + "\n"
        parts: List[str] = []
        datasets = [
            ("Análise Comercial (enriquecida)", self.analise_comercial),
            ("Classificação de Produtos", self.classificacao_produtos),
            ("Análise Financeira", self.analise_financeira),
            ("Devoluções", self.devolucoes),
            ("Rentabilidade", self.rentabilidade),
        ]
        for name, df in datasets:
            header = f"📄 {name}  ({len(df)} linhas totais — amostra de {min(n, len(df))})"
            if df.empty:
                parts.append(f"{header}\n  (vazio)")
            else:
                parts.append(f"{header}\n{df.head(n).to_string(index=False)}")
        return sep.join(parts)


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS (funções puras e utilitárias)
# ═══════════════════════════════════════════════════════════════════════════════


def _normalize_text(text: Any) -> str:
    """Remove acentos, BOM e converte para MAIÚSCULO.

    Reproduz a função ``normalize_text`` do robô original
    (src/utils/normalization.py) de forma independente.
    """
    if text is None:
        return ""
    s = str(text).strip()
    # Remover BOM
    s = s.lstrip("\ufeff")
    # Remover acentos via NFKD
    nfkd = unicodedata.normalize("NFKD", s)
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    return sem_acento.upper()


def _find_column(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    """Encontra a primeira coluna do DataFrame que bate com a lista de aliases."""
    cols_strip = {c.strip(): c for c in df.columns}
    for alias in aliases:
        if alias in cols_strip:
            return cols_strip[alias]
        # Tentar sem acento
        alias_norm = _normalize_text(alias)
        for raw, original in cols_strip.items():
            if _normalize_text(raw) == alias_norm:
                return original
    return None


def _safe_to_datetime(series: pd.Series) -> pd.Series:
    """Converte Series para datetime, tratando formatos ISO e BR.

    Quando o arquivo é lido com dtype=str, datas em formato ISO
    (YYYY-MM-DD HH:MM:SS) seriam corrompidas por dayfirst=True
    (dia e mês trocados). Esta função detecta o formato e aplica
    dayfirst apenas em datas não-ISO (ex: DD/MM/YYYY).
    """
    s = series.astype(str).str.strip()
    is_iso = s.str.match(r"^\d{4}-", na=False)
    result = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    if is_iso.any():
        result[is_iso] = pd.to_datetime(s[is_iso], errors="coerce")
    non_iso = ~is_iso & s.ne("") & s.str.lower().ne("nan") & series.notna()
    if non_iso.any():
        result[non_iso] = pd.to_datetime(
            s[non_iso], errors="coerce", dayfirst=True
        )
    return result


def _safe_to_numeric(series: pd.Series) -> pd.Series:
    """Converte Series para numérico, tratando formato BR (vírgula decimal)."""
    if series.dtype == object:
        s = series.astype(str)
        # Formato BR detectado pela presença de vírgula: "14.167,73"
        has_comma = s.str.contains(",", na=False)
        if has_comma.any():
            br = s[has_comma]
            br = br.str.replace(".", "", regex=False)   # remove milhar "."
            br = br.str.replace(",", ".", regex=False)   # vírgula → ponto decimal
            s = s.copy()
            s[has_comma] = br
        series = s
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _filter_by_month_year(
    df: pd.DataFrame,
    date_col: str,
    mes: int,
    ano: int,
) -> pd.DataFrame:
    """Filtra DataFrame por mês e ano de uma coluna de data."""
    if date_col not in df.columns:
        return df
    dt = _safe_to_datetime(df[date_col])
    mask = (dt.dt.month == mes) & (dt.dt.year == ano)
    return df.loc[mask].copy()


# ═══════════════════════════════════════════════════════════════════════════════
# LOADERS INDIVIDUAIS
# ═══════════════════════════════════════════════════════════════════════════════


def load_analise_comercial(
    file_bytes: bytes,
    mes: int,
    ano: int,
) -> Tuple[pd.DataFrame, List[str]]:
    """Carrega a Análise Comercial e filtra pelo mês/ano de emissão.

    Args:
        file_bytes: Conteúdo binário do arquivo .xlsx
        mes: Mês de apuração (1-12)
        ano: Ano de apuração (ex.: 2025)

    Returns:
        (DataFrame filtrado, lista de warnings)
    """
    warnings: List[str] = []
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
    except Exception as exc:
        return pd.DataFrame(), [f"Erro ao ler analise-comercial.xlsx: {exc}"]

    df.columns = df.columns.str.strip()

    # Normalizar chave de junção para evitar falhas de match
    if AC_JOIN_KEY in df.columns:
        df[AC_JOIN_KEY] = df[AC_JOIN_KEY].astype(str).str.strip().str.upper()

    # Validar colunas essenciais
    missing = [c for c in AC_EXPECTED_COLS if c not in df.columns]
    if missing:
        warnings.append(
            f"Colunas ausentes na Análise Comercial: {missing}. "
            "O cálculo pode ser afetado."
        )

    # Filtrar por operações válidas (ANTES de qualquer outro filtro)
    if AC_OPERACAO_COL in df.columns:
        before_op = len(df)
        op_code = df[AC_OPERACAO_COL].astype(str).str.strip().str.split(" - ", n=1).str[0].str.strip()
        mask_exact = op_code.isin(AC_VALID_OPERATIONS)
        mask_prefix = op_code.str.startswith(tuple(AC_VALID_OPERATION_PREFIXES))
        df = df[mask_exact | mask_prefix].copy()
        removed_op = before_op - len(df)
        if removed_op > 0:
            warnings.append(
                f"Análise Comercial: {removed_op} linhas removidas por operação inválida "
                f"({before_op} → {len(df)})."
            )
    else:
        warnings.append(
            f"Coluna '{AC_OPERACAO_COL}' não encontrada — sem filtro de operação aplicado."
        )

    # Converter data de emissão e filtrar mês/ano
    if AC_DATE_COL in df.columns:
        df[AC_DATE_COL] = _safe_to_datetime(df[AC_DATE_COL])
        before = len(df)
        df_full = df.copy()
        df = _filter_by_month_year(df, AC_DATE_COL, mes, ano)
        warnings.append(
            f"Análise Comercial: {before} linhas totais → {len(df)} no mês {mes:02d}/{ano}."
        )
    else:
        df_full = df.copy()
        warnings.append(
            f"Coluna '{AC_DATE_COL}' não encontrada — sem filtro de data aplicado."
        )

    # Converter colunas numéricas (em ambos os DataFrames)
    for col in ["Valor Realizado", "Valor Orçado"]:
        if col in df.columns:
            df[col] = _safe_to_numeric(df[col])
        if col in df_full.columns:
            df_full[col] = _safe_to_numeric(df_full[col])

    return df, df_full, warnings


def load_classificacao_produtos(
    file_bytes: bytes,
) -> Tuple[pd.DataFrame, List[str]]:
    """Carrega a Classificação de Produtos.

    Args:
        file_bytes: Conteúdo binário do arquivo .xlsx

    Returns:
        (DataFrame, lista de warnings)
    """
    warnings: List[str] = []
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), dtype=str)
    except Exception as exc:
        return pd.DataFrame(), [f"Erro ao ler Classificação de Produtos.xlsx: {exc}"]

    df.columns = df.columns.str.strip()

    missing = [c for c in CP_EXPECTED_COLS if c not in df.columns]
    if missing:
        warnings.append(f"Colunas ausentes na Classificação de Produtos: {missing}")

    # Normalizar chave de junção para evitar falhas de match
    if CP_JOIN_KEY in df.columns:
        df[CP_JOIN_KEY] = df[CP_JOIN_KEY].astype(str).str.strip().str.upper()

    warnings.append(f"Classificação de Produtos: {len(df)} produtos carregados.")
    return df, warnings


def enrich_analise_comercial(
    df_ac: pd.DataFrame,
    df_cp: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[str], List[Dict[str, str]]]:
    """Executa LEFT JOIN da Análise Comercial com Classificação de Produtos.

    A junção usa a coluna 'Código Produto' como chave.
    Colunas duplicadas da Classificação de Produtos recebem sufixo '_cp'.

    Args:
        df_ac: DataFrame da Análise Comercial (já filtrada por mês/ano)
        df_cp: DataFrame da Classificação de Produtos

    Returns:
        (DataFrame enriquecido, lista de warnings, lista de códigos sem correspondência)
    """
    warnings: List[str] = []

    if df_ac.empty:
        warnings.append("Análise Comercial vazia — JOIN não realizado.")
        return df_ac, warnings, []

    if df_cp.empty:
        warnings.append(
            "Classificação de Produtos vazia — Análise Comercial não enriquecida."
        )
        return df_ac, warnings, []

    # Garantir que a chave está normalizada (str + strip + upper) em ambos
    if AC_JOIN_KEY in df_ac.columns:
        df_ac[AC_JOIN_KEY] = df_ac[AC_JOIN_KEY].astype(str).str.strip().str.upper()
    else:
        warnings.append(
            f"Coluna '{AC_JOIN_KEY}' ausente na Análise Comercial — JOIN impossível."
        )
        return df_ac, warnings, []

    if CP_JOIN_KEY not in df_cp.columns:
        warnings.append(
            f"Coluna '{CP_JOIN_KEY}' ausente na Classificação de Produtos — JOIN impossível."
        )
        return df_ac, warnings, []

    before = len(df_ac)

    # Identificar colunas que colidiriam
    cols_ac = set(df_ac.columns)
    cols_cp = set(df_cp.columns) - {CP_JOIN_KEY}
    overlapping = cols_ac & cols_cp
    if overlapping:
        warnings.append(
            f"Colunas em comum (receberão sufixo '{CP_SUFFIX}'): {sorted(overlapping)}"
        )

    # LEFT JOIN
    df_merged = df_ac.merge(
        df_cp,
        how="left",
        on=AC_JOIN_KEY,
        suffixes=("", CP_SUFFIX),
    )

    # Identificar produtos sem classificação (NaN em coluna 'Linha' da CP)
    linha_col = "Linha" if "Linha" in df_merged.columns else f"Linha{CP_SUFFIX}"
    codigos_sem_match: List[Dict[str, str]] = []
    if linha_col in df_merged.columns:
        mask_sem = df_merged[linha_col].isna()
        sem_match = mask_sem.sum()
        if sem_match > 0:
            warnings.append(
                f"{sem_match} de {len(df_merged)} linhas sem correspondência "
                f"na Classificação de Produtos (Código Produto não encontrado)."
            )
            # Coletar detalhes dos códigos sem correspondência
            df_sem = df_merged.loc[mask_sem, [AC_JOIN_KEY]].copy()
            if "Descrição Produto" in df_merged.columns:
                df_sem["Descrição Produto"] = df_merged.loc[mask_sem, "Descrição Produto"]
            if "Processo" in df_merged.columns:
                df_sem["Processo"] = df_merged.loc[mask_sem, "Processo"]
            for _, row in df_sem.drop_duplicates(subset=[AC_JOIN_KEY]).iterrows():
                codigos_sem_match.append({
                    "codigo_produto": str(row.get(AC_JOIN_KEY, "")),
                    "descricao": str(row.get("Descrição Produto", "")),
                    "processo": str(row.get("Processo", "")),
                })

    warnings.append(
        f"JOIN concluído: {before} linhas AC → {len(df_merged)} linhas enriquecidas."
    )
    return df_merged, warnings, codigos_sem_match


def _load_analise_financeira_base(
    file_bytes: bytes,
) -> Tuple[pd.DataFrame, Dict[str, str], List[str]]:
    """Carrega e normaliza a AF, aplica filtro Tipo de Baixa != 'A'.

    Função interna reutilizada por load_analise_financeira e
    load_analise_financeira_full.

    Returns:
        (DataFrame com filtro de Tipo de Baixa, col_map, warnings)
    """
    warnings: List[str] = []
    try:
        df = pd.read_excel(io.BytesIO(file_bytes))
    except Exception as exc:
        return pd.DataFrame(), {}, [f"Erro ao ler analise-financeira.xlsx: {exc}"]

    df.columns = df.columns.str.strip()

    # Mapear colunas via aliases
    col_map: Dict[str, str] = {}
    for standard_name, aliases in AF_COL_ALIASES.items():
        found = _find_column(df, aliases)
        if found:
            col_map[standard_name] = found
        else:
            if standard_name not in ("Situação", "Dt. Prorrogação"):
                warnings.append(f"Coluna '{standard_name}' não encontrada na Análise Financeira.")

    if not col_map.get("Documento") or not col_map.get("Tipo de Baixa"):
        warnings.append("Colunas essenciais ausentes — retornando DataFrame vazio.")
        return pd.DataFrame(columns=list(AF_COL_ALIASES.keys())), {}, warnings

    # Filtrar: excluir Tipo de Baixa == 'A'; manter 'B', vazio e demais valores
    tipo_col = col_map["Tipo de Baixa"]
    before_tipo = len(df)
    df = df[df[tipo_col].astype(str).str.strip().str.upper() != AF_TIPO_BAIXA_EXCLUIR].copy()
    warnings.append(
        f"Filtro Tipo de Baixa != 'A': {before_tipo} → {len(df)} linhas."
    )

    return df, col_map, warnings


def _finalizar_af(
    df: pd.DataFrame,
    col_map: Dict[str, str],
) -> pd.DataFrame:
    """Renomeia colunas, converte tipos e normaliza strings no DataFrame da AF.

    Aplicado após os filtros de data (ou sem filtro, no caso full).
    """
    # Renomear colunas para nomes padrão
    rename_map = {v: k for k, v in col_map.items()}
    df = df.rename(columns=rename_map)

    # Converter Valor Líquido para numérico
    if "Valor Líquido" in df.columns:
        df["Valor Líquido"] = _safe_to_numeric(df["Valor Líquido"])

    # Garantir Documento como string (preservar zeros à esquerda)
    if "Documento" in df.columns:
        df["Documento"] = df["Documento"].astype(str).str.strip()

    # Converter Situação para inteiro (0=Aberto, 1=Recebido, 2=Parcial)
    if "Situação" in df.columns:
        df["Situação"] = pd.to_numeric(df["Situação"], errors="coerce").fillna(-1).astype(int)

    # Converter Dt. Prorrogação para datetime
    if "Dt. Prorrogação" in df.columns:
        df["Dt. Prorrogação"] = _safe_to_datetime(df["Dt. Prorrogação"])

    return df


def load_analise_financeira(
    file_bytes: bytes,
    mes: int,
    ano: int,
) -> Tuple[pd.DataFrame, List[str]]:
    """Carrega a Análise Financeira, exclui Tipo de Baixa='A' e filtra por mês/ano.

    Mantém registros com Tipo de Baixa = 'B', vazio ou outros valores,
    excluindo apenas 'A'. Inclui colunas Situação e Dt. Prorrogação se presentes.

    Args:
        file_bytes: Conteúdo binário do arquivo .xlsx
        mes: Mês de apuração
        ano: Ano de apuração

    Returns:
        (DataFrame filtrado, lista de warnings)
    """
    df, col_map, warnings = _load_analise_financeira_base(file_bytes)

    if df.empty or not col_map:
        return df, warnings

    # Filtrar por mês/ano da Data de Baixa
    if "Data de Baixa" in col_map:
        data_col = col_map["Data de Baixa"]
        df[data_col] = _safe_to_datetime(df[data_col])
        before_data = len(df)
        mask = (df[data_col].dt.month == mes) & (df[data_col].dt.year == ano)
        df = df[mask].copy()
        warnings.append(
            f"Filtro mês/ano ({mes:02d}/{ano}): {before_data} → {len(df)} linhas."
        )

    return _finalizar_af(df, col_map), warnings


def load_analise_financeira_full(
    file_bytes: bytes,
) -> Tuple[pd.DataFrame, List[str]]:
    """Carrega a Análise Financeira completa sem filtro de data.

    Exclui apenas Tipo de Baixa='A'. Retorna todos os registros históricos,
    necessário para verificar se TODAS as parcelas de um Processo Pai foram pagas
    independente do mês de referência.

    Args:
        file_bytes: Conteúdo binário do arquivo .xlsx

    Returns:
        (DataFrame completo sem filtro de data, lista de warnings)
    """
    df, col_map, warnings = _load_analise_financeira_base(file_bytes)

    if df.empty or not col_map:
        return df, warnings

    # Converter Data de Baixa para datetime sem filtrar
    if "Data de Baixa" in col_map:
        data_col = col_map["Data de Baixa"]
        df[data_col] = _safe_to_datetime(df[data_col])

    warnings.append(f"load_analise_financeira_full: {len(df)} linhas carregadas (sem filtro de data).")

    return _finalizar_af(df, col_map), warnings


def load_devolucoes(
    file_bytes: bytes,
    mes: int,
    ano: int,
) -> Tuple[pd.DataFrame, List[str]]:
    """Carrega o arquivo de Devoluções e filtra pelo mês/ano.

    Args:
        file_bytes: Conteúdo binário do arquivo .xlsx
        mes: Mês de apuração
        ano: Ano de apuração

    Returns:
        (DataFrame filtrado, lista de warnings)
    """
    warnings: List[str] = []
    try:
        df = pd.read_excel(io.BytesIO(file_bytes))
    except Exception as exc:
        return pd.DataFrame(), [f"Erro ao ler devolucoes.xlsx: {exc}"]

    df.columns = df.columns.str.strip()

    # Mapear colunas
    col_map: Dict[str, str] = {}
    for standard_name, aliases in DEV_COL_ALIASES.items():
        found = _find_column(df, aliases)
        if found:
            col_map[standard_name] = found
        else:
            if standard_name != "Código Operação":
                warnings.append(f"Coluna obrigatória '{standard_name}' não encontrada nas Devoluções.")

    # Remove linhas sem Num docorigem
    if "Num docorigem" in col_map:
        doc_col = col_map["Num docorigem"]
        before = len(df)
        df = df[df[doc_col].notna() & (df[doc_col].astype(str).str.strip() != "")]
        warnings.append(f"Devoluções sem Num docorigem removidas: {before} → {len(df)} linhas.")

    # Filtrar por mês/ano
    if "Data de Entrada" in col_map:
        data_col = col_map["Data de Entrada"]
        df[data_col] = _safe_to_datetime(df[data_col])
        before_data = len(df)
        mask = (df[data_col].dt.month == mes) & (df[data_col].dt.year == ano)
        df = df[mask].copy()
        warnings.append(f"Filtro mês/ano ({mes:02d}/{ano}): {before_data} → {len(df)} linhas.")

    # Renomear colunas para nomes padrão
    rename_map = {v: k for k, v in col_map.items()}
    cols_to_keep = [v for v in col_map.values() if v in df.columns]
    df = df[cols_to_keep].rename(columns=rename_map)

    # Converter Valor Produtos para numérico
    if "Valor Produtos" in df.columns:
        df["Valor Produtos"] = _safe_to_numeric(df["Valor Produtos"])

    # Remover devoluções com valor zero
    if "Valor Produtos" in df.columns:
        before_zero = len(df)
        df = df[df["Valor Produtos"] != 0.0]
        if before_zero != len(df):
            warnings.append(f"Devoluções com valor zero removidas: {before_zero} → {len(df)}.")

    return df, warnings


def load_rentabilidade(
    file_bytes: bytes,
) -> Tuple[pd.DataFrame, List[str]]:
    """Carrega o arquivo de Rentabilidade mensal.

    O arquivo de rentabilidade não requer filtro de mês/ano pois é
    selecionado pelo nome (ex.: rentabilidade_10_2025_agrupada.xlsx).

    Args:
        file_bytes: Conteúdo binário do arquivo .xlsx

    Returns:
        (DataFrame, lista de warnings)
    """
    warnings: List[str] = []
    try:
        df = pd.read_excel(io.BytesIO(file_bytes))
    except Exception as exc:
        return pd.DataFrame(columns=RENT_EXPECTED_COLS), [
            f"Erro ao ler arquivo de rentabilidade: {exc}"
        ]

    df.columns = df.columns.str.strip()

    # Normalizar a coluna de linha (padroniza para "Linha")
    linha_col = _find_column(df, RENT_LINHA_ALIASES)
    if linha_col and linha_col != "Linha":
        df = df.rename(columns={linha_col: "Linha"})
        warnings.append(f"Coluna '{linha_col}' renomeada para 'Linha' (padronização).")

    # Converter rentabilidade para numérico
    if "rentabilidade_realizada_pct" in df.columns:
        df["rentabilidade_realizada_pct"] = _safe_to_numeric(
            df["rentabilidade_realizada_pct"]
        )

    warnings.append(f"Rentabilidade: {len(df)} linhas carregadas.")
    return df, warnings


# ═══════════════════════════════════════════════════════════════════════════════
# FUNÇÃO PRINCIPAL — execute()
# ═══════════════════════════════════════════════════════════════════════════════


def load_processo_pedido_bytes(
    file_bytes: Optional[bytes],
) -> Tuple[Any, List[str]]:
    """Carrega a tabela Processo x Pedido de Compra a partir de bytes.

    Delega para ``receita.loaders.load_processo_pedido.load_bytes``.
    Importação lazy para evitar dependência circular.

    Args:
        file_bytes: Conteúdo binário do .xlsx de processo x PC, ou None.

    Returns:
        (ProcessoPedidoTabela | None, lista de warnings).
    """
    if not file_bytes:
        return None, []
    try:
        from receita.loaders.load_processo_pedido import load_bytes as _lb
        tabela, warnings = _lb(file_bytes)
        return tabela, warnings
    except Exception as exc:
        return None, [f"load_processo_pedido_bytes: {exc}"]


def execute(
    mes: int,
    ano: int,
    file_analise_comercial: bytes,
    file_classificacao_produtos: bytes,
    file_analise_financeira: bytes,
    file_devolucoes: bytes,
    file_rentabilidade: bytes,
    file_processo_pedido: Optional[bytes] = None,
) -> LoaderResult:
    """Função principal do script: carrega todos os arquivos e retorna resultado.

    Esta é a única função que deve ser chamada externamente pela Skill.

    Args:
        mes: Mês de apuração (1-12)
        ano: Ano de apuração (ex.: 2025)
        file_analise_comercial: Bytes do arquivo analise-comercial.xlsx
        file_classificacao_produtos: Bytes do arquivo Classificação de Produtos.xlsx
        file_analise_financeira: Bytes do arquivo analise-financeira.xlsx
        file_devolucoes: Bytes do arquivo devolucoes.xlsx
        file_rentabilidade: Bytes do arquivo rentabilidade_MM_AAAA_agrupada.xlsx

    Returns:
        LoaderResult com todos os DataFrames e metadados de validação.

    Example:
        >>> result = execute(
        ...     mes=10, ano=2025,
        ...     file_analise_comercial=open("analise-comercial.xlsx", "rb").read(),
        ...     file_classificacao_produtos=open("Classificação de Produtos.xlsx", "rb").read(),
        ...     file_analise_financeira=open("analise-financeira.xlsx", "rb").read(),
        ...     file_devolucoes=open("devolucoes.xlsx", "rb").read(),
        ...     file_rentabilidade=open("rentabilidade_10_2025_agrupada.xlsx", "rb").read(),
        ... )
        >>> print(result.summary())
        >>> print(result.sample(10))
    """
    # Validar parâmetros
    if not (1 <= mes <= 12):
        return LoaderResult(
            mes=mes, ano=ano, errors=[f"Mês inválido: {mes}. Deve ser entre 1 e 12."]
        )
    if ano < 2000 or ano > 2100:
        return LoaderResult(
            mes=mes, ano=ano, errors=[f"Ano inválido: {ano}. Deve ser entre 2000 e 2100."]
        )

    result = LoaderResult(mes=mes, ano=ano)

    # ── 1. Análise Comercial ──────────────────────────────────────────────
    _debug_loader("Carregando Análise Comercial.", tamanho_bytes=len(file_analise_comercial or b""))
    df_ac, df_ac_full, w_ac = load_analise_comercial(file_analise_comercial, mes, ano)
    result.warnings.extend(w_ac)
    _debug_loader("Análise Comercial carregada.", linhas_filtradas=len(df_ac), linhas_full=len(df_ac_full), warnings=len(w_ac))

    # ── 2. Classificação de Produtos ──────────────────────────────────────
    _debug_loader("Carregando Classificação de Produtos.", tamanho_bytes=len(file_classificacao_produtos or b""))
    df_cp, w_cp = load_classificacao_produtos(file_classificacao_produtos)
    result.classificacao_produtos = df_cp
    result.warnings.extend(w_cp)
    _debug_loader("Classificação de Produtos carregada.", linhas=len(df_cp), warnings=len(w_cp))

    # ── 3. Enriquecer AC com CP (LEFT JOIN) ───────────────────────────────
    _debug_loader("Enriquecendo AC com Classificação de Produtos.")
    df_enriched, w_join, codigos_sem = enrich_analise_comercial(df_ac, df_cp)
    result.analise_comercial = df_enriched
    result.warnings.extend(w_join)
    result.codigos_sem_correspondencia = codigos_sem
    _debug_loader("Enriquecimento AC+CP concluído.", linhas=len(df_enriched), codigos_sem_correspondencia=len(codigos_sem), warnings=len(w_join))

    # Enriquecer AC full (sem filtro de mês) para conversão
    df_enriched_full, _, _ = enrich_analise_comercial(df_ac_full, df_cp)
    result.analise_comercial_full = df_enriched_full
    _debug_loader("Enriquecimento da AC full concluído.", linhas=len(df_enriched_full))

    # ── 4. Análise Financeira ─────────────────────────────────────────────
    _debug_loader("Carregando Análise Financeira.", tamanho_bytes=len(file_analise_financeira or b""))
    df_af, w_af = load_analise_financeira(file_analise_financeira, mes, ano)
    result.analise_financeira = df_af
    result.warnings.extend(w_af)
    _debug_loader("Análise Financeira carregada.", linhas=len(df_af), warnings=len(w_af))

    # ── 5. Devoluções ─────────────────────────────────────────────────────
    _debug_loader("Carregando Devoluções.", tamanho_bytes=len(file_devolucoes or b""))
    df_dev, w_dev = load_devolucoes(file_devolucoes, mes, ano)
    result.devolucoes = df_dev
    result.warnings.extend(w_dev)
    _debug_loader("Devoluções carregadas.", linhas=len(df_dev), warnings=len(w_dev))

    # ── 6. Rentabilidade ──────────────────────────────────────────────────
    _debug_loader("Carregando Rentabilidade.", possui_bytes=bool(file_rentabilidade))
    df_rent, w_rent = load_rentabilidade(file_rentabilidade)
    result.rentabilidade = df_rent
    result.warnings.extend(w_rent)
    _debug_loader("Rentabilidade carregada.", linhas=len(df_rent), warnings=len(w_rent))

    # ── 7. Processo x Pedido de Compra (opcional) ─────────────────────────
    _debug_loader("Carregando Processo x Pedido de Compra.", possui_bytes=bool(file_processo_pedido))
    tabela_pc, w_pc = load_processo_pedido_bytes(file_processo_pedido)
    result.processo_pedido = tabela_pc
    result.warnings.extend(w_pc)
    _debug_loader("Processo x Pedido de Compra carregado.", possui_tabela=bool(tabela_pc), warnings=len(w_pc))

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# SELF-TEST  (execute with: python 01_loader_script.py --test)
# ═══════════════════════════════════════════════════════════════════════════════


def _create_test_excel_bytes(df: pd.DataFrame) -> bytes:
    """Helper: converte um DataFrame em bytes de .xlsx para testes."""
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


def _test():
    """Testes unitários embarcados."""
    import traceback

    passed = 0
    failed = 0
    total = 0

    def _assert(name: str, condition: bool, detail: str = ""):
        nonlocal passed, failed, total
        total += 1
        if condition:
            passed += 1
            print(f"  ✔ {name}")
        else:
            failed += 1
            print(f"  ✖ {name}  — {detail}")

    print("\n" + "=" * 60)
    print("  SELF-TEST: 01_loader_script.py")
    print("=" * 60)

    # ── Test _normalize_text ──────────────────────────────────────────
    print("\n▸ normalize_text")
    _assert("acentos", _normalize_text("Aplicação") == "APLICACAO")
    _assert("BOM", _normalize_text("\ufeffNegócio") == "NEGOCIO")
    _assert("None", _normalize_text(None) == "")
    _assert("vazio", _normalize_text("") == "")
    _assert("idempotente", _normalize_text("HIDROLOGIA") == "HIDROLOGIA")

    # ── Test load_analise_comercial ───────────────────────────────────
    print("\n▸ load_analise_comercial")
    df_ac_test = pd.DataFrame(
        {
            "Processo": ["P001", "P002", "P003"],
            "Status Processo": ["FATURADO", "FATURADO", "CANCELADO"],
            "Numero NF": ["1001", "1002", "1003"],
            "Dt Emissão": pd.to_datetime(["2025-10-15", "2025-10-20", "2025-11-01"]),
            "Valor Realizado": [50000, 30000, 10000],
            "Valor Orçado": [55000, 35000, 12000],
            "Consultor Interno": ["JOAO", "MARIA", "JOSE"],
            "Código Produto": ["PRD001", "PRD002", "PRD003"],
            "Descrição Produto": ["Bomba X", "Motor Y", "CLP Z"],
            "Operação": ["PVEN", "PVEN", "SERV"],
            "Representante-pedido": ["REP1", "REP2", "REP3"],
            "Gerente Comercial-Pedido": ["GER1", "GER2", "GER3"],
            "Aplicação Mat./Serv.": ["INDUSTRIAL", "INDUSTRIAL", "INDUSTRIAL"],
            "Cliente": ["C001", "C002", "C003"],
            "Nome Cliente": ["CLI A", "CLI B", "CLI C"],
            "Cidade": ["SAO PAULO", "RIO", "BH"],
            "UF": ["SP", "RJ", "MG"],
            "Qtde Atendida": [10, 5, 2],
            "Centro Custo-pedido": ["CC1", "CC2", "CC3"],
        }
    )
    ac_bytes = _create_test_excel_bytes(df_ac_test)
    df_loaded, df_loaded_full, w = load_analise_comercial(ac_bytes, mes=10, ano=2025)
    _assert("filtra mês correto", len(df_loaded) == 2, f"Esperado 2, obteve {len(df_loaded)}")
    _assert("P003 excluído (nov)", "P003" not in df_loaded["Processo"].values)
    _assert("full AC mantém todas", len(df_loaded_full) == 3, f"Esperado 3, obteve {len(df_loaded_full)}")

    # ── Test load_classificacao_produtos ──────────────────────────────
    print("\n▸ load_classificacao_produtos")
    df_cp_test = pd.DataFrame(
        {
            "Código Produto": ["PRD001", "PRD002", "PRD003"],
            "Descrição Produto": ["Bomba X", "Motor Y", "CLP Z"],
            "Linha": ["HIDROLOGIA", "ELETRICA", "AUTOMACAO"],
            "Grupo": ["QED", "WEG", "SIEMENS"],
            "Subgrupo": ["BOMBAS", "MOTORES", "CLPs"],
            "Tipo de Mercadoria": ["REVENDA", "REVENDA", "SERVICO"],
            "Fabricante": ["KSB", "WEG", "SIEMENS"],
        }
    )
    cp_bytes = _create_test_excel_bytes(df_cp_test)
    df_cp_loaded, w_cp = load_classificacao_produtos(cp_bytes)
    _assert("carrega 3 produtos", len(df_cp_loaded) == 3)
    _assert("tem coluna Linha", "Linha" in df_cp_loaded.columns)

    # ── Test enrich_analise_comercial (LEFT JOIN) ─────────────────────
    print("\n▸ enrich_analise_comercial (LEFT JOIN)")
    df_enriched, w_j, codigos_sem = enrich_analise_comercial(df_loaded, df_cp_loaded)
    _assert("mesma qtd linhas", len(df_enriched) == len(df_loaded))
    _assert(
        "tem coluna Linha (da CP)",
        "Linha" in df_enriched.columns or "Linha_cp" in df_enriched.columns,
    )

    # ── Test load_analise_financeira ──────────────────────────────────
    print("\n▸ load_analise_financeira")
    df_af_test = pd.DataFrame(
        {
            "Documento": ["D001", "D002", "D003", "D004"],
            "Valor Líquido": [10000, 20000, 15000, 5000],
            "Data de Baixa": pd.to_datetime(
                ["2025-10-05", "2025-10-15", "2025-11-01", "2025-10-20"]
            ),
            "Tipo de Baixa": ["B", "B", "B", "A"],
        }
    )
    af_bytes = _create_test_excel_bytes(df_af_test)
    df_af_loaded, w_af = load_analise_financeira(af_bytes, mes=10, ano=2025)
    _assert(
        "filtra tipo B + mês 10",
        len(df_af_loaded) == 2,
        f"Esperado 2, obteve {len(df_af_loaded)}",
    )

    # ── Test load_devolucoes ──────────────────────────────────────────
    print("\n▸ load_devolucoes")
    df_dev_test = pd.DataFrame(
        {
            "Num docorigem": ["NF001", "NF002", "", "NF003"],
            "Data de Entrada": pd.to_datetime(
                ["2025-10-10", "2025-10-20", "2025-10-15", "2025-11-05"]
            ),
            "Valor Produtos": [5000, 3000, 1000, 2000],
            "Código Operação": ["DEV", "DEV", "DEV", "DEV"],
        }
    )
    dev_bytes = _create_test_excel_bytes(df_dev_test)
    df_dev_loaded, w_dev = load_devolucoes(dev_bytes, mes=10, ano=2025)
    _assert(
        "filtra mês 10 + remove vazio",
        len(df_dev_loaded) == 2,
        f"Esperado 2, obteve {len(df_dev_loaded)}",
    )

    # ── Test load_rentabilidade ───────────────────────────────────────
    print("\n▸ load_rentabilidade")
    df_rent_test = pd.DataFrame(
        {
            "Negócio": ["HIDROLOGIA", "ELETRICA"],
            "Grupo": ["QED", "WEG"],
            "Subgrupo": ["BOMBAS", "MOTORES"],
            "Tipo de Mercadoria": ["REVENDA", "REVENDA"],
            "rentabilidade_realizada_pct": [0.15, 0.22],
        }
    )
    rent_bytes = _create_test_excel_bytes(df_rent_test)
    df_rent_loaded, w_rent = load_rentabilidade(rent_bytes)
    _assert("carrega 2 linhas", len(df_rent_loaded) == 2)
    _assert(
        "renomeia 'Negócio' para 'Linha'",
        "Linha" in df_rent_loaded.columns,
        f"Colunas: {list(df_rent_loaded.columns)}",
    )

    # ── Test execute() (pipeline completo) ────────────────────────────
    print("\n▸ execute() — pipeline completo")
    result = execute(
        mes=10,
        ano=2025,
        file_analise_comercial=ac_bytes,
        file_classificacao_produtos=cp_bytes,
        file_analise_financeira=af_bytes,
        file_devolucoes=dev_bytes,
        file_rentabilidade=rent_bytes,
    )
    _assert("resultado OK", result.ok, f"Erros: {result.errors}")
    _assert("AC enriquecida não vazia", len(result.analise_comercial) > 0)
    _assert("AF carregada", len(result.analise_financeira) > 0)
    _assert("DEV carregada", len(result.devolucoes) > 0)
    _assert("RENT carregada", len(result.rentabilidade) > 0)
    _assert("summary executa", "LOADER" in result.summary())
    _assert("sample executa", "Análise Comercial" in result.sample(5))

    # ── Test validação de parâmetros ──────────────────────────────────
    print("\n▸ Validação de parâmetros")
    result_bad_mes = execute(
        mes=13, ano=2025,
        file_analise_comercial=ac_bytes,
        file_classificacao_produtos=cp_bytes,
        file_analise_financeira=af_bytes,
        file_devolucoes=dev_bytes,
        file_rentabilidade=rent_bytes,
    )
    _assert("mês inválido gera erro", not result_bad_mes.ok)

    # ── Sumário ───────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  Resultado: {passed}/{total} testes passaram")
    if failed:
        print(f"  ⚠ {failed} teste(s) FALHARAM")
    else:
        print("  ✔ Todos os testes passaram!")
    print(f"{'='*60}\n")

    return failed == 0


if __name__ == "__main__":
    if "--test" in sys.argv:
        success = _test()
        sys.exit(0 if success else 1)
    else:
        print("Uso: python 01_loader_script.py --test")
        print("Este script é um módulo da Skill de Comissões do Claude.")
        print("Use a função execute() para carregamento programático.")
