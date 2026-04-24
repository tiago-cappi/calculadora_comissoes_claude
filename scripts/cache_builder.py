"""
cache_builder.py — Gerador do arquivo de cache local do Supabase

Uso
---
Este script é executado pelo Robô de Comissões no início de cada sessão
de cálculo, quando o ambiente não tem acesso direto ao Supabase.

Ele recebe os dados de todas as tabelas do schema 'comissoes' já
transformados (via parâmetro Python ou dicionário pré-carregado) e os
salva em ~/supabase_cache.json para uso pelo supabase_loader.py.

Fluxo típico
------------
1. Claude busca os dados via MCP do Supabase (fora do container)
2. Claude chama `build_cache(dados)` passando um dict com todos os dados
3. O arquivo ~/supabase_cache.json é gerado
4. O pipeline roda normalmente — supabase_loader.py usa o cache

API
---
    from scripts.cache_builder import build_cache, verify_cache

    # Construir cache a partir de um dict de dados já transformados
    build_cache({
        "params.json": {"cap_fc_max": 1.0, ...},
        "cargos.json": [...],
        "colaboradores.json": [...],
        ...
    })

    # Verificar integridade do cache gerado
    report = verify_cache()
    print(report)

Linha de comando
----------------
    python scripts/cache_builder.py --verify
    python scripts/cache_builder.py --output /caminho/custom/cache.json
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional


# Tabelas obrigatórias — o cache deve conter todas para o pipeline rodar
_REQUIRED_TABLES = [
    "cargos.json",
    "colaboradores.json",
    "config_comissao.json",
    "pesos_metas.json",
    "fc_escada_cargos.json",
    "cross_selling.json",
    "aliases.json",
    "metas_individuais.json",
    "metas_aplicacao.json",
    "meta_rentabilidade.json",
    "metas_fornecedores.json",
    "monthly_avg_rates.json",
    "classificacao_produtos.json",
]


def _get_cache_path(output: Optional[str] = None) -> Path:
    """Resolve o caminho de saída do arquivo de cache."""
    if output:
        return Path(output)
    env_path = os.environ.get("SUPABASE_CACHE_PATH")
    if env_path:
        return Path(env_path)
    return Path.home() / "supabase_cache.json"


def build_cache(
    data: Dict[str, Any],
    output: Optional[str] = None,
    overwrite: bool = True,
) -> Path:
    """Salva os dados transformados do Supabase em um arquivo JSON local.

    Args:
        data     : Dict mapeando filename (ex: "params.json") → dados já
                   transformados pelo supabase_loader (dict ou lista de dicts).
        output   : Caminho de saída. Se None, usa ~/supabase_cache.json
                   ou a env var SUPABASE_CACHE_PATH.
        overwrite: Se False, não sobrescreve arquivo existente.

    Returns:
        Path do arquivo gerado.

    Raises:
        FileExistsError: Se overwrite=False e o arquivo já existir.
        ValueError: Se `data` não for um dict.
    """
    if not isinstance(data, dict):
        raise ValueError(f"'data' deve ser um dict, recebeu: {type(data)}")

    cache_path = _get_cache_path(output)

    if not overwrite and cache_path.exists():
        raise FileExistsError(
            f"Cache já existe em '{cache_path}'. "
            f"Use overwrite=True para sobrescrever."
        )

    cache_path.parent.mkdir(parents=True, exist_ok=True)

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    return cache_path


def verify_cache(cache_path: Optional[str] = None) -> str:
    """Verifica a integridade do arquivo de cache local.

    Checa se o arquivo existe, é JSON válido, e contém todas as tabelas
    obrigatórias.

    Args:
        cache_path: Caminho do arquivo. Se None, usa o padrão.

    Returns:
        String com relatório de verificação.
    """
    path = _get_cache_path(cache_path)
    lines = [
        "=" * 56,
        "VERIFICAÇÃO DO CACHE LOCAL",
        "=" * 56,
        f"Arquivo : {path}",
        "",
    ]

    if not path.exists():
        lines.append("✗ Arquivo não encontrado.")
        lines.append("")
        lines.append("Execute build_cache() para gerar o cache.")
        return "\n".join(lines)

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        lines.append(f"✗ JSON inválido: {e}")
        return "\n".join(lines)

    file_size = path.stat().st_size
    lines.append(f"✓ Arquivo encontrado ({file_size / 1024:.1f} KB)")
    lines.append("")

    ok_count = 0
    for table in _REQUIRED_TABLES:
        if table in data:
            val = data[table]
            count = len(val) if isinstance(val, (list, dict)) else "—"
            lines.append(f"  ✓ {table:<35} ({count} registros)")
            ok_count += 1
        else:
            lines.append(f"  ✗ {table:<35} AUSENTE")

    lines.append("")
    lines.append(f"Resultado: {ok_count}/{len(_REQUIRED_TABLES)} tabelas presentes")

    if ok_count == len(_REQUIRED_TABLES):
        lines.append("✓ Cache completo — pipeline pode ser executado.")
    else:
        lines.append("⚠ Cache incompleto — regenere com build_cache().")

    return "\n".join(lines)


def build_cache_from_supabase_loader(output: Optional[str] = None) -> Path:
    """Constrói o cache consultando diretamente o Supabase via supabase_loader.

    Útil quando o ambiente TEM acesso ao Supabase — gera o cache para uso
    posterior em ambientes sem acesso.

    Args:
        output: Caminho de saída. Se None, usa o padrão.

    Returns:
        Path do arquivo gerado.

    Raises:
        RuntimeError: Se qualquer tabela do Supabase falhar.
    """
    # Importação local para evitar dependência circular
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from scripts import supabase_loader as sl

    data: Dict[str, Any] = {}
    errors = []

    print("Carregando dados do Supabase...", file=sys.stderr)
    for filename in _REQUIRED_TABLES:
        try:
            # Força recarga do Supabase ignorando cache em memória
            if filename in sl._CACHE:
                del sl._CACHE[filename]
            sl._SUPABASE_UNAVAILABLE = False
            result = sl.load_json(filename)
            data[filename] = result
            count = len(result) if isinstance(result, (list, dict)) else "—"
            print(f"  [ok] {filename:<35} ({count} registros)", file=sys.stderr)
        except Exception as e:
            errors.append(f"  [erro] {filename}: {e}")
            print(f"  [erro] {filename}: {e}", file=sys.stderr)

    if errors:
        raise RuntimeError(
            f"Falha ao carregar {len(errors)} tabela(s) do Supabase:\n"
            + "\n".join(errors)
        )

    cache_path = build_cache(data, output)
    print(f"\n[ok] Cache salvo em: {cache_path}", file=sys.stderr)
    return cache_path


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Gera ou verifica o cache local do Supabase para o pipeline de comissões."
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Caminho de saída do arquivo de cache (padrão: ~/supabase_cache.json)",
    )
    parser.add_argument(
        "--verify", "-v",
        action="store_true",
        help="Apenas verifica o cache existente, sem regenerar",
    )
    parser.add_argument(
        "--from-supabase",
        action="store_true",
        help="Gera o cache consultando o Supabase diretamente (requer acesso à rede)",
    )
    args = parser.parse_args()

    if args.verify:
        print(verify_cache(args.output))
        sys.exit(0)

    if args.from_supabase:
        try:
            path = build_cache_from_supabase_loader(args.output)
            print(verify_cache(str(path)))
        except RuntimeError as e:
            print(f"ERRO: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("Use --from-supabase para gerar o cache ou --verify para verificar.")
        print(f"Cache path padrão: {_get_cache_path(args.output)}")
        sys.exit(0)
