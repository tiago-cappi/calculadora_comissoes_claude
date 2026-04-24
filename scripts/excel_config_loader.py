"""
excel_config_loader.py — Loader de Regras de Negócio via Excel

Substitui o supabase_loader.py: lê todas as tabelas de configuração de um
único arquivo Excel (configuracoes_comissoes.xlsx, na raiz do projeto) e
devolve as MESMAS estruturas (dicts/listas) que o loader antigo devolvia
do Supabase, preservando a lógica já validada em atribuicao.py,
fc_calculator.py, config_manager.py, realizados.py e comissao_faturamento.py.

Abas esperadas (15)
-------------------
params, colaboradores, cargos, config_comissao, pesos_metas,
fc_escada_cargos, cross_selling, metas_individuais, metas_aplicacao,
meta_rentabilidade, metas_fornecedores, monthly_avg_rates, aliases,
enum_tipo_meta, classificacao_produtos

Localização
-----------
Padrão: ./configuracoes_comissoes.xlsx (raiz do projeto).
Override: variável de ambiente CONFIG_EXCEL_PATH.

API pública
-----------
    load_json(filename) -> Any   # mesmo contrato do antigo supabase_loader
    clear_cache() -> None
    diagnose() -> str
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


# Em modo congelado (PyInstaller), __file__ fica dentro de _internal/ e não
# pode ser usado para localizar a raiz. O xlsx vive ao lado do .exe.
if getattr(sys, "frozen", False):
    _DEFAULT_EXCEL_PATH = Path(sys.executable).parent / "configuracoes_comissoes.xlsx"
else:
    _DEFAULT_EXCEL_PATH = Path(__file__).parent.parent / "configuracoes_comissoes.xlsx"


def _get_excel_path() -> Path:
    env_path = os.environ.get("CONFIG_EXCEL_PATH")
    if env_path:
        return Path(env_path)
    return _DEFAULT_EXCEL_PATH


_CACHE: Dict[str, Any] = {}
_WORKBOOK_CACHE: Optional[Dict[str, pd.DataFrame]] = None


def _read_workbook() -> Dict[str, pd.DataFrame]:
    global _WORKBOOK_CACHE
    if _WORKBOOK_CACHE is not None:
        return _WORKBOOK_CACHE
    path = _get_excel_path()
    if not path.exists():
        raise RuntimeError(
            f"Arquivo de configuração não encontrado: {path}\n"
            f"Gere-o com: python scripts/gerar_template_excel.py\n"
            f"Ou defina a variável de ambiente CONFIG_EXCEL_PATH."
        )
    try:
        sheets = pd.read_excel(path, sheet_name=None, engine="openpyxl")
    except Exception as e:
        raise RuntimeError(f"Falha ao ler {path}: {e}") from e

    # Normaliza nomes das abas (case-insensitive, remove espaços extras)
    _WORKBOOK_CACHE = {str(name).strip().lower(): df for name, df in sheets.items()}
    return _WORKBOOK_CACHE


def _get_sheet(sheet_name: str) -> pd.DataFrame:
    wb = _read_workbook()
    key = sheet_name.strip().lower()
    if key not in wb:
        raise RuntimeError(
            f"Aba '{sheet_name}' não encontrada no Excel {_get_excel_path()}. "
            f"Abas presentes: {sorted(wb.keys())}"
        )
    df = wb[key].copy()
    # Normaliza nomes de colunas: lowercase, strip
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def _none_if_nan(v: Any) -> Any:
    """Converte NaN/strings vazias em None (para wildcards NULL)."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, str):
        s = v.strip()
        if s == "" or s.lower() in ("nan", "none", "null"):
            return None
        return s
    return v


def _to_bool(v: Any) -> bool:
    """Aceita True, 1, 'SIM', 'S', 'TRUE', 'VERDADEIRO' como True."""
    if v is None:
        return False
    try:
        if pd.isna(v):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().upper() in ("SIM", "S", "TRUE", "VERDADEIRO", "1", "YES", "Y")
    return False


def _to_float(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        if pd.isna(v):
            return default
    except (TypeError, ValueError):
        pass
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _rows(sheet_name: str) -> List[Dict[str, Any]]:
    """Retorna linhas da aba como lista de dicts, com NaN → None."""
    df = _get_sheet(sheet_name)
    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        entry = {col: _none_if_nan(row[col]) for col in df.columns}
        # Ignora linhas totalmente vazias
        if all(v is None for v in entry.values()):
            continue
        records.append(entry)
    return records


# ═══════════════════════════════════════════════════════════════════════
# TRANSFORMAÇÕES — produzem exatamente o mesmo formato do supabase_loader
# ═══════════════════════════════════════════════════════════════════════

def _load_cargos() -> List[Dict[str, Any]]:
    result = []
    for r in _rows("cargos"):
        tipo_com = r.get("tipo_comissao") or ""
        result.append({
            "nome_cargo": r.get("nome_cargo") or "",
            "tipo_cargo": r.get("tipo_cargo") or "",
            "tipo_comissao": tipo_com,
            "TIPO_COMISSAO": tipo_com,
        })
    return result


def _load_colaboradores() -> List[Dict[str, Any]]:
    return [
        {
            "nome_colaborador": r.get("nome_colaborador") or "",
            "cargo": r.get("cargo") or "",
        }
        for r in _rows("colaboradores")
    ]


def _load_config_comissao() -> List[Dict[str, Any]]:
    result = []
    for r in _rows("config_comissao"):
        taxa_raw = r.get("taxa_rateio_maximo_pct")
        fatia_raw = r.get("fatia_cargo")
        # Regra sem taxa OU sem fatia preenchidas = template vazio, não é considerada
        if taxa_raw is None or fatia_raw is None:
            continue
        taxa = _to_float(taxa_raw)
        fatia = _to_float(fatia_raw)
        taxa_efetiva = round(fatia * taxa / 100.0, 6)
        result.append({
            "linha": r.get("linha"),
            "grupo": r.get("grupo"),
            "subgrupo": r.get("subgrupo"),
            "tipo_mercadoria": r.get("tipo_mercadoria"),
            "fabricante": r.get("fabricante"),
            "aplicacao": r.get("aplicacao"),
            "cargo": r.get("cargo"),
            "colaborador": r.get("colaborador"),
            "fatia_cargo": fatia,
            "taxa_rateio_maximo_pct": taxa,
            "taxa_maxima_efetiva": taxa_efetiva,
        })
    return result


def _load_pesos_metas() -> List[Dict[str, Any]]:
    result = []
    for r in _rows("pesos_metas"):
        entry: Dict[str, Any] = {"cargo": r.get("cargo") or ""}
        for k, v in r.items():
            if k == "cargo":
                continue
            if v is not None:
                entry[k] = v
        result.append(entry)
    return result


def _load_fc_escada_cargos() -> List[Dict[str, Any]]:
    result = []
    for r in _rows("fc_escada_cargos"):
        entry = {k: v for k, v in r.items() if v is not None}
        result.append(entry)
    return result


def _load_cross_selling() -> List[Dict[str, Any]]:
    return [
        {
            "colaborador": r.get("colaborador") or "",
            "taxa_cross_selling_pct": _to_float(r.get("taxa_cross_selling_pct")),
        }
        for r in _rows("cross_selling")
    ]


def _load_metas_individuais() -> List[Dict[str, Any]]:
    return [
        {
            "colaborador": r.get("colaborador") or "",
            "cargo": r.get("cargo") or "",
            "tipo_meta": r.get("tipo_meta") or "",
            "valor_meta": _to_float(r.get("valor_meta")),
        }
        for r in _rows("metas_individuais")
    ]


def _load_metas_aplicacao() -> List[Dict[str, Any]]:
    return [dict(r) for r in _rows("metas_aplicacao")]


def _load_meta_rentabilidade() -> List[Dict[str, Any]]:
    return [
        {
            "linha": r.get("linha"),
            "grupo": r.get("grupo"),
            "subgrupo": r.get("subgrupo"),
            "tipo_mercadoria": r.get("tipo_mercadoria"),
            "fabricante": r.get("fabricante"),
            "aplicacao": r.get("aplicacao"),
            "referencia_media_ponderada_pct": r.get("referencia_media_ponderada_pct"),
            "meta_rentabilidade_alvo_pct": _to_float(r.get("meta_rentabilidade_alvo_pct")),
        }
        for r in _rows("meta_rentabilidade")
    ]


def _load_metas_fornecedores() -> List[Dict[str, Any]]:
    return [dict(r) for r in _rows("metas_fornecedores")]


def _load_monthly_avg_rates() -> Dict[str, Dict[str, Dict[str, float]]]:
    result: Dict[str, Dict[str, Dict[str, float]]] = {}
    for r in _rows("monthly_avg_rates"):
        moeda = str(r.get("moeda") or "").strip()
        ano_raw = r.get("ano")
        mes_raw = r.get("mes")
        taxa = r.get("taxa_media")
        if not moeda or ano_raw is None or mes_raw is None:
            continue
        try:
            ano = str(int(float(ano_raw)))
            mes = str(int(float(mes_raw)))
        except (TypeError, ValueError):
            continue
        result.setdefault(moeda, {}).setdefault(ano, {})[mes] = _to_float(taxa)
    return result


def _load_aliases() -> Dict[str, Dict[str, str]]:
    result: Dict[str, Dict[str, str]] = {}
    for r in _rows("nome no ti9"):
        entidade = r.get("entidade") or ""
        alias = r.get("nome no ti9") or ""
        nome_padrao = r.get("nome_padrao") or ""
        if entidade and alias:
            result.setdefault(entidade, {})[alias] = nome_padrao
    return result


def _load_classificacao_produtos() -> List[Dict[str, Any]]:
    result = []
    for r in _rows("classificacao_produtos"):
        result.append({
            "codigo_produto": r.get("codigo_produto") or r.get("código produto") or "",
            "linha": r.get("linha"),
            "grupo": r.get("grupo"),
            "subgrupo": r.get("subgrupo"),
            "tipo_mercadoria": r.get("tipo_mercadoria"),
            "fabricante": r.get("fabricante"),
        })
    return result


_LOADERS = {
    "cargos.json":                 _load_cargos,
    "colaboradores.json":          _load_colaboradores,
    "config_comissao.json":        _load_config_comissao,
    "pesos_metas.json":            _load_pesos_metas,
    "fc_escada_cargos.json":       _load_fc_escada_cargos,
    "cross_selling.json":          _load_cross_selling,
    "metas_individuais.json":      _load_metas_individuais,
    "metas_aplicacao.json":        _load_metas_aplicacao,
    "meta_rentabilidade.json":     _load_meta_rentabilidade,
    "metas_fornecedores.json":     _load_metas_fornecedores,
    "monthly_avg_rates.json":      _load_monthly_avg_rates,
    "aliases.json":                _load_aliases,
    "classificacao_produtos.json": _load_classificacao_produtos,
}


# ═══════════════════════════════════════════════════════════════════════
# API PÚBLICA (contrato idêntico ao antigo supabase_loader)
# ═══════════════════════════════════════════════════════════════════════

def load_json(filename: str) -> Any:
    if filename in _CACHE:
        return _CACHE[filename]
    if filename not in _LOADERS:
        raise FileNotFoundError(
            f"Arquivo '{filename}' não mapeado no excel_config_loader.\n"
            f"Disponíveis: {sorted(_LOADERS.keys())}"
        )
    data = _LOADERS[filename]()
    _CACHE[filename] = data
    return data


def clear_cache() -> None:
    global _WORKBOOK_CACHE
    _CACHE.clear()
    _WORKBOOK_CACHE = None


def diagnose() -> str:
    path = _get_excel_path()
    lines = [
        "=" * 56,
        "DIAGNÓSTICO DO EXCEL CONFIG LOADER",
        "=" * 56,
        "",
        f"Arquivo: {path}",
        f"Existe : {path.exists()}",
        f"CWD    : {os.getcwd()}",
        "",
    ]
    if not path.exists():
        lines.append("[X] Arquivo nao encontrado. Gere com: python scripts/gerar_template_excel.py")
        return "\n".join(lines)

    try:
        wb = _read_workbook()
        lines.append(f"[OK] Workbook carregado ({len(wb)} abas):")
        for name in sorted(wb.keys()):
            lines.append(f"    - {name} ({len(wb[name])} linhas)")
    except Exception as e:
        lines.append(f"[X] Falha ao ler workbook: {e}")
        return "\n".join(lines)

    lines.append("")
    for filename in sorted(_LOADERS.keys()):
        try:
            data = _LOADERS[filename]()
            n = len(data) if hasattr(data, "__len__") else "n/a"
            lines.append(f"  [OK] {filename:<35} ({n} registros)")
        except Exception as e:
            lines.append(f"  [X]  {filename:<35} FALHOU: {e}")
    return "\n".join(lines)


if __name__ == "__main__":
    print(diagnose())
