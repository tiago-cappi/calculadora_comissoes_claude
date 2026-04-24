"""
config_manager.py — Gerenciador de Regras de Comissão (JSON)

Lê, consulta e modifica as regras de negócio armazenadas em arquivos JSON.
Usado como script da Skill do Claude para evitar que o modelo precise ler
os JSONs inteiros — todas as operações retornam dados filtrados/resumidos.

USO:
    import scripts.config_manager as cm

    # Leitura
    print(cm.summary())
    print(cm.query_regras(linha="Hidrologia"))
    print(cm.get_params())
    print(cm.list_colaboradores())
    print(cm.get_pesos_metas())
    print(cm.get_metas(tipo="faturamento"))
    print(cm.get_meta_rentabilidade(mes=10, ano=2025))

    # Escrita — alterar regras existentes em massa
    print(cm.set_taxa_fatia(linha="Recursos Hídricos", grupo="Sonda Portátil",
                            cargo="Consultor Interno", taxa_rateio_maximo_pct=30, fatia_cargo=10))

    # Escrita — criar hierarquia+cargo nova
    print(cm.add_regra(linha="Recursos Hídricos", cargo="Gerente Linha",
                       taxa_rateio_maximo_pct=10, fatia_cargo=20))
    print(cm.update_regra(filtro={"linha": "Hidrologia"},
                          fatia_cargo=15))
    print(cm.remove_regra(filtro={"linha": "Inativo"}))

    # Persistir no Supabase
    cm.persist("config_comissao.json")
"""

from __future__ import annotations

import json
import os
import sys
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

# ═══════════════════════════════════════════════════════════════════════
# CARREGAMENTO DE REGRAS — via Supabase (substituiu references/*.json)
# ═══════════════════════════════════════════════════════════════════════

from scripts import supabase_loader as _sl


def _load_json(filename: str) -> Any:
    """Carrega regras de negócio do Supabase (equivalente ao JSON original)."""
    return _sl.load_json(filename)


def _save_json(filename: str, data: Any) -> str:
    """Atualiza o cache em memória. Use persist() para salvar no Supabase."""
    _sl._CACHE[filename] = data
    return f"[cache] {filename} atualizado"


# ═══════════════════════════════════════════════════════════════════════
# PERSISTÊNCIA — salva alterações no Supabase
# ═══════════════════════════════════════════════════════════════════════

def persist(filename: str = None) -> str:
    """Persiste um ou todos os arquivos em cache no Supabase.

    Args:
        filename: ex. "config_comissao.json". Se None, persiste tudo.

    Returns:
        Mensagem de resultado.
    """
    from scripts import supabase_writer as _sw
    if filename:
        return _sw.persist(filename)
    return _sw.persist_all()


# ═══════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _match_filter(entry: dict, filtro: dict) -> bool:
    """Verifica se uma entrada da config_comissao combina com o filtro.

    Chaves suportadas: linha, grupo, subgrupo, tipo_mercadoria,
                       fabricante, aplicacao, colaborador.
    Regra: se o filtro exige um valor para uma chave, a entrada só combina
    se também tiver aquela chave com o mesmo valor. Entradas sem a chave
    são tratadas como None e só combinam quando o filtro pede None ou não
    especifica aquela chave.
    """
    for key, val in filtro.items():
        if val is None:
            # Filtro pede ausência: só combina se entrada não tem a chave
            if entry.get(key) is not None:
                return False
            continue
        entry_val = entry.get(key)
        if entry_val is None:
            return False
        if str(entry_val).lower() != str(val).lower():
            return False
    return True


# ═══════════════════════════════════════════════════════════════════════
# DIAGNÓSTICO — verificação de ambiente
# ═══════════════════════════════════════════════════════════════════════

def diagnose() -> str:
    """Diagnóstico do ambiente de execução da skill.

    Exibe onde os dados JSON foram encontrados e quais arquivos estão
    disponíveis. Use esta função primeiro se houver qualquer dúvida sobre
    o carregamento das regras.

    Returns:
        Texto formatado com informações de diagnóstico.
    """
    lines = ["=" * 56, "DIAGNÓSTICO DO AMBIENTE config_manager", "=" * 56, ""]

    lines.append("Fonte de dados : Supabase (schema comissoes)")
    lines.append(f"CWD atual      : {os.getcwd()}")
    lines.append("")

    # Quick test: try loading one file from Supabase
    try:
        _load_json("params.json")
        lines.append("✓ Leitura de params.json: OK (Supabase)")
    except Exception as e:
        lines.append(f"✗ Leitura de params.json: FALHOU — {e}")

    # Full connectivity test via supabase_loader.diagnose
    try:
        lines.append("")
        lines.append(_sl.diagnose())
    except Exception as e:
        lines.append(f"✗ Diagnóstico Supabase: {e}")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════
# LEITURA — FUNÇÕES QUERY
# ═══════════════════════════════════════════════════════════════════════

def summary() -> str:
    """Resumo geral de todas as regras de negócio.

    Retorna texto curto com contagens e visão geral.
    """
    lines = ["=" * 56, "RESUMO DAS REGRAS DE NEGÓCIO", "=" * 56, ""]

    # Params
    params = _load_json("params.json")
    lines.append(f"Parâmetros: {len(params)} definidos")
    for k, v in params.items():
        lines.append(f"  {k}: {v}")
    lines.append("")

    # Cargos
    cargos = _load_json("cargos.json")
    lines.append(f"Cargos: {len(cargos)}")
    for c in cargos:
        lines.append(f"  {c['nome_cargo']} ({c.get('tipo_comissao', c.get('TIPO_COMISSAO', '?'))})")
    lines.append("")

    # Colaboradores
    colabs = _load_json("colaboradores.json")
    lines.append(f"Colaboradores: {len(colabs)}")
    lines.append("")

    # Config Comissão
    config = _load_json("config_comissao.json")
    ativos = [e for e in config if e.get("ativo", True)]
    linhas = set(e["linha"] for e in ativos)
    lines.append(f"Regras de Comissão: {len(ativos)} hierarquias ativas")
    lines.append(f"  Linhas de negócio: {', '.join(sorted(linhas))}")
    for lin in sorted(linhas):
        count = sum(1 for e in ativos if e["linha"] == lin)
        lines.append(f"    {lin}: {count} hierarquias")
    lines.append("")

    # Metas
    mi = _load_json("metas_individuais.json")
    ma = _load_json("metas_aplicacao.json")
    mr = _load_json("meta_rentabilidade.json")
    mf = _load_json("metas_fornecedores.json")
    lines.append(f"Metas Individuais: {len(mi)} registros")
    lines.append(f"Metas Aplicação: {len(ma)} registros")
    periodos = list(mr.keys())
    total_rent = sum(len(v) for v in mr.values())
    lines.append(f"Meta Rentabilidade: {total_rent} registros, períodos: {', '.join(periodos)}")
    lines.append(f"Metas Fornecedores: {len(mf)} registros")
    lines.append("")

    # FC Escada
    fc = _load_json("fc_escada_cargos.json")
    lines.append(f"FC Escada: {len(fc)} cargos configurados")
    lines.append("")

    # Cross-Selling
    cs = _load_json("cross_selling.json")
    lines.append(f"Cross-Selling: {len(cs)} colaboradores elegíveis")
    lines.append("")

    lines.append("=" * 56)
    return "\n".join(lines)


def get_params() -> str:
    """Exibe todos os parâmetros operacionais."""
    params = _load_json("params.json")
    lines = ["PARÂMETROS OPERACIONAIS:", ""]
    for k, v in params.items():
        lines.append(f"  {k}: {v}")
    return "\n".join(lines)


def list_colaboradores() -> str:
    """Lista todos os colaboradores com cargo e tipo de comissão."""
    colabs = _load_json("colaboradores.json")
    cargos = _load_json("cargos.json")

    # Mapa cargo → tipo_comissao
    cargo_map = {}
    for c in cargos:
        nome = c.get("nome_cargo", "")
        tipo = c.get("tipo_comissao", c.get("TIPO_COMISSAO", ""))
        cargo_map[nome] = tipo

    lines = [f"COLABORADORES ({len(colabs)}):", ""]
    lines.append(f"{'Nome':<25} {'Cargo':<25} {'Tipo Comissão':<15}")
    lines.append("-" * 65)
    for co in sorted(colabs, key=lambda x: x.get("nome_colaborador", "")):
        nome = co.get("nome_colaborador", "?")
        cargo = co.get("cargo", "?")
        tipo = cargo_map.get(cargo, "?")
        lines.append(f"{nome:<25} {cargo:<25} {tipo:<15}")
    return "\n".join(lines)


def query_regras(
    linha: str = None,
    grupo: str = None,
    subgrupo: str = None,
    tipo_mercadoria: str = None,
    cargo: str = None,
    somente_ativos: bool = True,
    limite: int = 20,
) -> str:
    """Consulta regras de CONFIG_COMISSAO com filtros opcionais.

    Retorna tabela formatada com as hierarquias encontradas (schema flat).
    Se `cargo` for fornecido, filtra apenas as linhas daquele cargo.
    """
    config = _load_json("config_comissao.json")

    filtro = {}
    if linha:
        filtro["linha"] = linha
    if grupo:
        filtro["grupo"] = grupo
    if subgrupo:
        filtro["subgrupo"] = subgrupo
    if tipo_mercadoria:
        filtro["tipo_mercadoria"] = tipo_mercadoria
    if cargo:
        filtro["cargo"] = cargo

    resultados = []
    for entry in config:
        if somente_ativos and not entry.get("ativo", True):
            continue
        if not _match_filter(entry, filtro):
            continue
        resultados.append(entry)

    if not resultados:
        return f"Nenhuma regra encontrada para o filtro: {filtro or 'sem filtro'}"

    total = len(resultados)
    truncated = total > limite
    resultados = resultados[:limite]

    lines = [f"REGRAS DE COMISSÃO ({total} linhas encontradas)", ""]
    lines.append(f"{'Linha':<16} {'Grupo':<16} {'Subgrupo':<14} {'TM':<10} {'Cargo':<25} {'Fatia':>7} {'Taxa':>6} {'EfetMax':>8}")
    lines.append("-" * 105)

    for e in resultados:
        lines.append(
            f"{str(e.get('linha') or ''):<16} "
            f"{str(e.get('grupo') or '*'):<16} "
            f"{str(e.get('subgrupo') or '*'):<14} "
            f"{str(e.get('tipo_mercadoria') or '*'):<10} "
            f"{str(e.get('cargo') or ''):<25} "
            f"{e.get('fatia_cargo', 0):>6.1f}% "
            f"{e.get('taxa_rateio_maximo_pct', 0):>5.1f}% "
            f"{e.get('taxa_maxima_efetiva', 0):>7.2f}%"
        )

    if truncated:
        lines.append(f"\n... e mais {total - limite} linhas. Use filtros mais específicos.")

    return "\n".join(lines)


def get_pesos_metas(cargo: str = None, colaborador: str = None) -> str:
    """Exibe pesos das metas para o cálculo do FC."""
    pesos = _load_json("pesos_metas.json")

    if cargo:
        pesos = [p for p in pesos if str(p.get("cargo", "")).lower() == cargo.lower()]
    if colaborador:
        pesos = [p for p in pesos if str(p.get("colaborador", "")).lower() == colaborador.lower()]

    if not pesos:
        filtro = f"cargo: {cargo}" if cargo else ""
        if colaborador:
            filtro += f", colaborador: {colaborador}" if filtro else f"colaborador: {colaborador}"
        return f"Nenhum peso encontrado para {filtro}"

    lines = ["PESOS DAS METAS (FC):", ""]

    componentes = [
        "faturamento_linha", "rentabilidade", "conversao_linha",
        "faturamento_individual", "conversao_individual",
        "retencao_clientes", "meta_fornecedor_1", "meta_fornecedor_2",
    ]

    for p in pesos:
        colab = p.get("colaborador", "")
        label = f"  {p['cargo']}"
        if colab:
            label += f" ({colab})"
        lines.append(f"{label}:")
        for comp in componentes:
            val = p.get(comp, 0)
            if val and val > 0:
                lines.append(f"    {comp}: {val}%")
        lines.append("")

    return "\n".join(lines)


def get_metas(
    tipo: str = None,
    colaborador: str = None,
    linha: str = None,
) -> str:
    """Exibe metas (individuais e/ou por aplicação)."""
    lines = []

    # Metas Individuais
    mi = _load_json("metas_individuais.json")
    if tipo:
        mi = [m for m in mi if str(m.get("tipo_meta", "")).lower() == tipo.lower()]
    if colaborador:
        mi = [m for m in mi if str(m.get("colaborador", "")).lower() == colaborador.lower()]

    if mi:
        lines.append(f"METAS INDIVIDUAIS ({len(mi)} registros):")
        lines.append(f"{'Colaborador':<25} {'Cargo':<22} {'Tipo Meta':<18} {'Valor':>12}")
        lines.append("-" * 80)
        for m in mi:
            lines.append(
                f"{str(m.get('colaborador') or '?'):<25} "
                f"{str(m.get('cargo') or '?'):<22} "
                f"{str(m.get('tipo_meta') or '?'):<18} "
                f"{m.get('valor_meta',0):>12,.0f}"
            )
        lines.append("")

    # Metas Aplicação
    ma = _load_json("metas_aplicacao.json")
    if tipo:
        ma = [m for m in ma if str(m.get("tipo_meta", "")).lower() == tipo.lower()]
    if linha:
        ma = [m for m in ma if str(m.get("linha", "")).lower() == linha.lower()]

    if ma:
        lines.append(f"METAS POR APLICAÇÃO ({len(ma)} registros):")
        lines.append(f"{'Linha':<18} {'Grupo':<18} {'Tipo Meta':<18} {'Valor':>12}")
        lines.append("-" * 70)
        for m in ma:
            g = str(m.get('grupo') or '*')
            lines.append(
                f"{str(m.get('linha') or '?'):<18} "
                f"{g:<18} "
                f"{str(m.get('tipo_meta') or '?'):<18} "
                f"{m.get('valor_meta',0):>12,.0f}"
            )
        lines.append("")

    # Metas Fornecedores
    mf = _load_json("metas_fornecedores.json")
    if linha:
        mf = [m for m in mf if str(m.get("linha", "")).lower() == linha.lower()]

    if mf:
        lines.append(f"METAS FORNECEDORES ({len(mf)} registros):")
        for m in mf:
            lines.append(
                f"  {m.get('linha','?')} | {m.get('fornecedor','?')} | "
                f"Meta anual: {m.get('meta_anual',0):,.0f} {m.get('moeda','?')}"
            )
        lines.append("")

    return "\n".join(lines) if lines else "Nenhuma meta encontrada para os filtros informados."


def get_meta_rentabilidade(mes: int = None, ano: int = None, linha: str = None, limite: int = 20) -> str:
    """Exibe metas de rentabilidade para um período específico (ou todas se período for None)."""
    mr = _load_json("meta_rentabilidade.json")
    
    # Normalizar formato: se for dict {periodo: [...]}, manter como está
    # Se for lista plana, usar como está (período não é salvo em Supabase atualmente)
    
    entries = []
    if isinstance(mr, dict):
        # Formato legado: dict com períodos
        if mes is not None and ano is not None:
            key = f"{ano}-{mes:02d}"
            if key not in mr:
                available = ', '.join(mr.keys()) if mr else "(nenhum)"
                return f"Nenhuma meta de rentabilidade para o período {key}. Períodos disponíveis: {available}"
            entries = mr[key]
        else:
            # Se não especificou período, pega tudo
            for period_entries in mr.values():
                entries.extend(period_entries)
    else:
        # Formato novo: lista plana (sem período em Supabase)
        entries = mr if isinstance(mr, list) else []
    
    if linha:
        entries = [e for e in entries if str(e.get("linha", "")).lower() == linha.lower()]

    if not entries:
        period_str = f"{ano}-{mes:02d}  " if mes and ano else ""
        return f"Nenhuma meta de rentabilidade {period_str}para linha '{linha}'." if linha else "Nenhuma meta de rentabilidade."

    total = len(entries)
    truncated = total > limite
    entries = entries[:limite]

    period_str = f" {ano}-{mes:02d}" if mes and ano else ""
    lines = [f"META RENTABILIDADE{period_str} ({total} registros)", ""]
    lines.append(f"{'Linha':<16} {'Grupo':<20} {'Subgrupo':<18} {'TM':<10} {'Fab':<12} {'Aplic':<12} {'Ref %':>8} {'Meta %':>8}")
    lines.append("-" * 110)
    for e in entries:
        lines.append(
            f"{str(e.get('linha') or '?'):<16} "
            f"{str(e.get('grupo') or '?'):<20} "
            f"{str(e.get('subgrupo') or '?'):<18} "
            f"{str(e.get('tipo_mercadoria') or '?'):<10} "
            f"{str(e.get('fabricante') or '?'):<12} "
            f"{str(e.get('aplicacao') or '?'):<12} "
            f"{float(e.get('referencia_media_ponderada_pct') or 0):>7.2f}% "
            f"{float(e.get('meta_rentabilidade_alvo_pct') or 0):>7.2f}%"
        )

    if truncated:
        lines.append(f"\n... e mais {total - limite} registros.")

    return "\n".join(lines)


def get_fc_escada() -> str:
    """Exibe configuração de FC Escada por cargo."""
    fc = _load_json("fc_escada_cargos.json")
    lines = ["FC ESCADA POR CARGO:", ""]
    lines.append(f"{'Cargo':<25} {'Modo':<10} {'Degraus':>8} {'Piso %':>8}")
    lines.append("-" * 55)

    # Suporta tanto dict {cargo: {modo,..}} quanto list [{cargo, modo,..}]
    if isinstance(fc, dict):
        items = [{"cargo": cargo, **vals} for cargo, vals in fc.items()]
    else:
        items = fc

    for f in items:
        lines.append(
            f"{f.get('cargo','?'):<25} "
            f"{f.get('modo','?'):<10} "
            f"{f.get('num_degraus',0):>8} "
            f"{f.get('piso_pct',0):>7.0f}%"
        )
    return "\n".join(lines)


def get_cross_selling() -> str:
    """Exibe colaboradores elegíveis a cross-selling."""
    cs = _load_json("cross_selling.json")
    lines = ["CROSS-SELLING:", ""]
    for c in cs:
        lines.append(f"  {c.get('colaborador','?')}: taxa={c.get('taxa_cross_selling_pct',0)}%")
    return "\n".join(lines)


def get_aliases() -> str:
    """Exibe mapeamento de aliases."""
    aliases = _load_json("aliases.json")
    lines = ["ALIASES:", ""]
    for entidade, mapping in aliases.items():
        lines.append(f"  [{entidade}]")
        for alias, padrao in mapping.items():
            lines.append(f"    {alias} → {padrao}")
    return "\n".join(lines)


def resolve_alias(nome: str) -> str:
    """Resolve um alias para o nome padrão."""
    aliases = _load_json("aliases.json")
    nome_upper = nome.strip().upper()
    for mapping in aliases.values():
        if nome_upper in mapping:
            return mapping[nome_upper]
    return nome


# ═══════════════════════════════════════════════════════════════════════
# ESCRITA — FUNÇÕES DE MODIFICAÇÃO
# ═══════════════════════════════════════════════════════════════════════

def add_regra(
    linha: str,
    cargo: str,
    taxa_rateio_maximo_pct: float,
    fatia_cargo: float,
    grupo: str = None,
    subgrupo: str = None,
    tipo_mercadoria: str = None,
    fabricante: str = None,
    aplicacao: str = None,
    colaborador: str = None,
) -> str:
    """Cria UMA ÚNICA linha nova em config_comissao (ou atualiza se já existir match exato).

    ATENÇÃO: esta função cria/atualiza uma regra com combinação EXATA de todos os campos.
    Se você quer atualizar TODAS as linhas existentes que pertencem a uma hierarquia
    (ex: mudar taxa de todo Consultor Interno em RH/Sonda Portátil), use set_taxa_fatia().

    Uso correto de add_regra: criar uma hierarquia+cargo que ainda NÃO existe na config.
    """
    config = _load_json("config_comissao.json")

    taxa_efetiva = round(fatia_cargo * taxa_rateio_maximo_pct / 100.0, 6)

    # Chave de identidade da linha
    def _matches(e):
        return (
            str(e.get("linha") or "").lower() == (linha or "").lower()
            and str(e.get("cargo") or "").lower() == (cargo or "").lower()
            and str(e.get("grupo") or "").lower() == (grupo or "").lower()
            and str(e.get("subgrupo") or "").lower() == (subgrupo or "").lower()
            and str(e.get("tipo_mercadoria") or "").lower() == (tipo_mercadoria or "").lower()
            and str(e.get("fabricante") or "").lower() == (fabricante or "").lower()
            and str(e.get("aplicacao") or "").lower() == (aplicacao or "").lower()
            and str(e.get("colaborador") or "").lower() == (colaborador or "").lower()
        )

    updated = False
    for e in config:
        if _matches(e):
            e["taxa_rateio_maximo_pct"] = taxa_rateio_maximo_pct
            e["fatia_cargo"] = fatia_cargo
            e["taxa_maxima_efetiva"] = taxa_efetiva
            e["ativo"] = True
            updated = True
            break

    if not updated:
        config.append({
            "linha": linha,
            "grupo": grupo,
            "subgrupo": subgrupo,
            "tipo_mercadoria": tipo_mercadoria,
            "fabricante": fabricante,
            "aplicacao": aplicacao,
            "cargo": cargo,
            "colaborador": colaborador,
            "fatia_cargo": fatia_cargo,
            "taxa_rateio_maximo_pct": taxa_rateio_maximo_pct,
            "taxa_maxima_efetiva": taxa_efetiva,
            "ativo": True,
        })

    _save_json("config_comissao.json", config)
    action = "atualizada" if updated else "criada"
    return (
        f"✓ REGRA {action.upper()}\n"
        f"  {linha} / {grupo or '*'} / {subgrupo or '*'} / {tipo_mercadoria or '*'} — {cargo}\n"
        f"  taxa={taxa_rateio_maximo_pct}%  fatia={fatia_cargo}%  efetiva={taxa_efetiva:.4f}%"
    )


def update_regra(
    filtro: dict,
    taxa_rateio_maximo_pct: float = None,
    fatia_cargo: float = None,
    ativo: bool = None,
) -> str:
    """Atualiza campos em TODAS as linhas de config_comissao que combinam com o filtro.

    Modifica em massa — qualquer campo de hierarquia, cargo ou colaborador
    pode ser incluído no filtro. Apenas os campos presentes no filtro são
    verificados; os ausentes são ignorados (match parcial).

    Exemplo: filtro={"linha": "RH", "grupo": "SP", "cargo": "CI"} atualiza
    TODAS as linhas de CI em RH/SP, independente de subgrupo, TM ou fabricante.

    Para uso mais ergonômico, prefira a função set_taxa_fatia().
    """
    config = _load_json("config_comissao.json")

    matched = 0
    for e in config:
        if not _match_filter(e, filtro):
            continue
        matched += 1
        if ativo is not None:
            e["ativo"] = ativo
        if taxa_rateio_maximo_pct is not None:
            e["taxa_rateio_maximo_pct"] = taxa_rateio_maximo_pct
        if fatia_cargo is not None:
            e["fatia_cargo"] = fatia_cargo
        # Recalcular taxa_maxima_efetiva
        e["taxa_maxima_efetiva"] = round(
            float(e.get("fatia_cargo") or 0) * float(e.get("taxa_rateio_maximo_pct") or 0) / 100.0, 6
        )

    if matched == 0:
        return f"⚠ Nenhuma linha encontrada para o filtro: {filtro}"

    _save_json("config_comissao.json", config)
    return f"✓ ATUALIZAÇÃO CONCLUÍDA — {matched} linha(s) modificada(s). Filtro: {filtro}"


def set_taxa_fatia(
    linha: str,
    taxa_rateio_maximo_pct: float = None,
    fatia_cargo: float = None,
    grupo: str = None,
    subgrupo: str = None,
    tipo_mercadoria: str = None,
    fabricante: str = None,
    aplicacao: str = None,
    cargo: str = None,
    colaborador: str = None,
) -> str:
    """Atualiza taxa e/ou fatia em TODAS as regras existentes que combinam com a hierarquia.

    USO: Quando o usuário pede "defina taxa X% e fatia Y% para [Cargo/Colaborador]
    em [Hierarquia]". Atualiza TODAS as linhas existentes que combinam com os filtros
    informados. Não cria linhas novas.

    DIFERENÇA vs add_regra:
    - set_taxa_fatia: atualiza em massa regras JÁ EXISTENTES
      → "defina taxa 30% para CI em RH/Sonda Portátil" (atualiza N linhas)
    - add_regra: cria/atualiza UMA ÚNICA regra com match exato de todos os campos
      → criar nova hierarquia+cargo que não existe

    RESOLUÇÃO DE CARGO vs COLABORADOR:
    - cargo="Consultor Interno" (sem colaborador):
      atualiza TODOS os Consultores Internos naquela hierarquia
    - colaborador="Dener Martins" (sem cargo):
      atualiza SOMENTE as linhas do Dener naquela hierarquia
    - cargo="Consultor Interno" + colaborador="Dener Martins":
      atualiza somente Dener, desde que seja CI

    Args:
        linha: Linha de negócio (obrigatório).
        taxa_rateio_maximo_pct: Nova taxa de rateio (None = não alterar).
        fatia_cargo: Nova fatia do cargo (None = não alterar).
        grupo, subgrupo, tipo_mercadoria, fabricante: Filtros hierárquicos adicionais.
        cargo: Filtrar por cargo (ex: "Consultor Interno").
        colaborador: Filtrar por colaborador específico (ex: "Dener Martins").

    Returns:
        Mensagem com quantidade de linhas modificadas.
    """
    filtro: Dict[str, str] = {"linha": linha}
    if grupo:
        filtro["grupo"] = grupo
    if subgrupo:
        filtro["subgrupo"] = subgrupo
    if tipo_mercadoria:
        filtro["tipo_mercadoria"] = tipo_mercadoria
    if fabricante:
        filtro["fabricante"] = fabricante
    if aplicacao:
        filtro["aplicacao"] = aplicacao
    if cargo:
        filtro["cargo"] = cargo
    if colaborador:
        filtro["colaborador"] = colaborador

    return update_regra(filtro, taxa_rateio_maximo_pct, fatia_cargo)


def remove_regra(
    filtro: dict,
    desativar: bool = True,
) -> str:
    """Remove (desativa ou exclui) linhas de config_comissao que combinam com o filtro.

    filtro pode incluir: linha, grupo, subgrupo, tipo_mercadoria, fabricante, cargo.
    desativar=True → marca ativo=False (reversível).
    desativar=False → exclui permanentemente.
    """
    config = _load_json("config_comissao.json")

    matched = 0
    if desativar:
        for e in config:
            if _match_filter(e, filtro) and e.get("ativo", True):
                e["ativo"] = False
                matched += 1
        new_config = config
    else:
        new_config = []
        for e in config:
            if _match_filter(e, filtro):
                matched += 1
            else:
                new_config.append(e)

    if matched == 0:
        return f"⚠ Nenhuma linha encontrada para o filtro: {filtro}"

    _save_json("config_comissao.json", new_config)
    action = "desativadas" if desativar else "excluídas"
    return f"✓ REMOÇÃO CONCLUÍDA — {matched} linha(s) {action}. Filtro: {filtro}"


def add_colaborador(
    id_colaborador: str,
    nome_colaborador: str,
    cargo: str,
) -> str:
    """Adiciona um colaborador ao cadastro."""
    colabs = _load_json("colaboradores.json")

    # Verificar duplicata
    for c in colabs:
        if c.get("nome_colaborador", "").lower() == nome_colaborador.lower():
            return f"⚠ Colaborador '{nome_colaborador}' já existe no cadastro."

    colabs.append({
        "id_colaborador": id_colaborador,
        "nome_colaborador": nome_colaborador,
        "cargo": cargo,
    })

    _save_json("colaboradores.json", colabs)
    return f"✓ Colaborador '{nome_colaborador}' ({cargo}) adicionado com sucesso."


def add_alias(entidade: str, alias: str, padrao: str) -> str:
    """Adiciona um alias ao mapeamento."""
    aliases = _load_json("aliases.json")
    if entidade not in aliases:
        aliases[entidade] = {}
    aliases[entidade][alias.upper()] = padrao
    _save_json("aliases.json", aliases)
    return f"✓ Alias '{alias}' → '{padrao}' adicionado para entidade '{entidade}'."


def set_pesos_metas(cargo: str, pesos: Dict[str, float], colaborador: str = "") -> str:
    """Define ou atualiza os pesos do FC para um cargo (opcionalmente por colaborador).

    Args:
        cargo: Nome do cargo (ex: "Gerente Linha").
        pesos: Dict com componentes e pesos (ex: {"faturamento_linha": 60, "rentabilidade": 40}).
               A soma deve ser 100. Componentes omitidos são zerados.
        colaborador: Nome do colaborador (opcional). Se vazio, aplica ao cargo genérico.

    Returns:
        Mensagem de resultado.
    """
    soma = sum(pesos.values())
    if abs(soma - 100) > 0.01:
        return f"⚠ Soma dos pesos = {soma:.1f}% (deve ser 100%). Corrija antes de salvar."

    pesos_list = _load_json("pesos_metas.json")

    componentes_validos = {
        "faturamento_linha", "rentabilidade", "conversao_linha",
        "faturamento_individual", "conversao_individual",
        "retencao_clientes", "meta_fornecedor_1", "meta_fornecedor_2",
    }
    invalidos = set(pesos.keys()) - componentes_validos
    if invalidos:
        return f"⚠ Componentes inválidos: {invalidos}. Válidos: {componentes_validos}"

    novo = {"cargo": cargo, "colaborador": colaborador}
    for comp in componentes_validos:
        novo[comp] = pesos.get(comp, 0)

    found = False
    for i, p in enumerate(pesos_list):
        if (str(p.get("cargo", "")).lower() == cargo.lower()
                and str(p.get("colaborador", "")).lower() == colaborador.lower()):
            pesos_list[i] = novo
            found = True
            break
    if not found:
        pesos_list.append(novo)

    _save_json("pesos_metas.json", pesos_list)
    label = f"'{cargo}'"
    if colaborador:
        label += f" / '{colaborador}'"
    return f"✓ Pesos do FC para {label} atualizados: {pesos}"


def set_meta_individual(
    colaborador: str,
    cargo: str,
    tipo_meta: str,
    valor_meta: float,
) -> str:
    """Define ou atualiza uma meta individual de um colaborador.

    Args:
        colaborador: Nome do colaborador.
        cargo: Cargo do colaborador.
        tipo_meta: Tipo de meta (ex: "faturamento_linha", "conversao_linha").
        valor_meta: Valor da meta.
    """
    metas = _load_json("metas_individuais.json")

    found = False
    for m in metas:
        if (str(m.get("colaborador", "")).lower() == colaborador.lower()
                and str(m.get("tipo_meta", "")).lower() == tipo_meta.lower()):
            m["valor_meta"] = valor_meta
            m["cargo"] = cargo
            found = True
            break
    if not found:
        metas.append({
            "colaborador": colaborador,
            "cargo": cargo,
            "tipo_meta": tipo_meta,
            "valor_meta": valor_meta,
        })

    _save_json("metas_individuais.json", metas)
    return f"✓ Meta individual '{tipo_meta}' de '{colaborador}': {valor_meta:,.0f}"


def set_meta_rentabilidade(
    linha: str,
    grupo: str = None,
    subgrupo: str = None,
    tipo_mercadoria: str = None,
    fabricante: str = None,
    aplicacao: str = None,
    referencia_pct: float = None,
    meta_alvo_pct: float = None,
    periodo: str = None,
) -> str:
    """Define ou atualiza uma meta de rentabilidade.

    Args:
        linha: Linha de negócio.
        grupo / subgrupo / tipo_mercadoria / fabricante / aplicacao: Hierarquia (None = wildcard).
        referencia_pct: Percentual de referência (média ponderada histórica).
        meta_alvo_pct: Percentual alvo.
        periodo: "YYYY" ou "YYYY-MM". Se None, usa o ano corrente.
    """
    import datetime
    if periodo is None:
        periodo = str(datetime.date.today().year)

    mr = _load_json("meta_rentabilidade.json")
    
    # Normalizar formato: se for lista (loader retorna lista plana), converter para dict
    # mantendo as entradas existentes no periodo atual para nao perder dados
    if isinstance(mr, list):
        mr = {periodo: mr}
    
    if periodo not in mr:
        mr[periodo] = []

    entries = mr[periodo]
    found = False
    for e in entries:
        if (str(e.get("linha") or "").lower() == linha.lower()
                and str(e.get("grupo") or "").lower() == (grupo or "").lower()
                and str(e.get("subgrupo") or "").lower() == (subgrupo or "").lower()
                and str(e.get("tipo_mercadoria") or "").lower() == (tipo_mercadoria or "").lower()
                and str(e.get("fabricante") or "").lower() == (fabricante or "").lower()
                and str(e.get("aplicacao") or "").lower() == (aplicacao or "").lower()):
            if referencia_pct is not None:
                e["referencia_media_ponderada_pct"] = referencia_pct
            if meta_alvo_pct is not None:
                e["meta_rentabilidade_alvo_pct"] = meta_alvo_pct
            found = True
            break

    if not found:
        entry: Dict = {
            "linha": linha, "grupo": grupo, "subgrupo": subgrupo,
            "tipo_mercadoria": tipo_mercadoria, "fabricante": fabricante,
            "aplicacao": aplicacao,
        }
        if referencia_pct is not None:
            entry["referencia_media_ponderada_pct"] = referencia_pct
        if meta_alvo_pct is not None:
            entry["meta_rentabilidade_alvo_pct"] = meta_alvo_pct
        entries.append(entry)

    _save_json("meta_rentabilidade.json", mr)
    hier_parts = [linha, grupo or '*', subgrupo or '*', tipo_mercadoria or '*', fabricante or '*', aplicacao or '*']
    hier_str = '/'.join(hier_parts)
    return (
        f"✓ Meta rentabilidade '{periodo}' — {hier_str}: "
        f"ref={referencia_pct}%  alvo={meta_alvo_pct}%"
    )


def set_meta_aplicacao(
    linha: str,
    tipo_meta: str,
    valor_meta: float,
    grupo: str = None,
    subgrupo: str = None,
    tipo_mercadoria: str = None,
    fabricante: str = None,
    aplicacao: str = None,
) -> str:
    """Define ou atualiza uma meta de aplicação (hierarquia de produto)."""
    metas = _load_json("metas_aplicacao.json")

    found = False
    for m in metas:
        if (str(m.get("linha") or "").lower() == linha.lower()
                and str(m.get("tipo_meta") or "").lower() == tipo_meta.lower()
                and str(m.get("grupo") or "").lower() == (grupo or "").lower()
                and str(m.get("subgrupo") or "").lower() == (subgrupo or "").lower()
                and str(m.get("tipo_mercadoria") or "").lower() == (tipo_mercadoria or "").lower()
                and str(m.get("fabricante") or "").lower() == (fabricante or "").lower()
                and str(m.get("aplicacao") or "").lower() == (aplicacao or "").lower()):
            m["valor_meta"] = valor_meta
            found = True
            break

    if not found:
        metas.append({
            "linha": linha, "grupo": grupo, "subgrupo": subgrupo,
            "tipo_mercadoria": tipo_mercadoria, "fabricante": fabricante,
            "aplicacao": aplicacao, "tipo_meta": tipo_meta, "valor_meta": valor_meta,
        })

    _save_json("metas_aplicacao.json", metas)
    return f"✓ Meta aplicação '{tipo_meta}' para {linha}/{grupo or '*'}: {valor_meta:,.0f}"


def set_fc_escada(
    cargo: str,
    modo: str = "ESCADA",
    num_degraus: int = None,
    piso_pct: float = None,
) -> str:
    """Define ou atualiza configuracao de FC Escada para um cargo.

    Args:
        cargo: Nome do cargo (ex: "Consultor Interno").
        modo: "ESCADA" ou "RAMPA".
        num_degraus: Quantidade de degraus (ex: 4).
        piso_pct: Piso minimo em percentual (ex: 30.0 para 30%).

    Returns:
        Mensagem de sucesso.
    """
    fc_escada = _load_json("fc_escada_cargos.json")

    # Suporta tanto dict {cargo: {...}} quanto list [{cargo, ...}]
    if isinstance(fc_escada, dict):
        if cargo not in fc_escada:
            fc_escada[cargo] = {}
        fc_escada[cargo]["modo"] = modo
        if num_degraus is not None:
            fc_escada[cargo]["num_degraus"] = num_degraus
        if piso_pct is not None:
            fc_escada[cargo]["piso_pct"] = piso_pct
    else:
        # Se for list, atualizar ou adicionar
        found = False
        for item in fc_escada:
            if item.get("cargo") == cargo:
                item["modo"] = modo
                if num_degraus is not None:
                    item["num_degraus"] = num_degraus
                if piso_pct is not None:
                    item["piso_pct"] = piso_pct
                found = True
                break
        if not found:
            novo = {"cargo": cargo, "modo": modo}
            if num_degraus is not None:
                novo["num_degraus"] = num_degraus
            if piso_pct is not None:
                novo["piso_pct"] = piso_pct
            fc_escada.append(novo)

    _save_json("fc_escada_cargos.json", fc_escada)
    piso_str = f", piso {piso_pct:.0f}%" if piso_pct is not None else ""
    degraus_str = f", {num_degraus} degraus" if num_degraus is not None else ""
    return f"[OK] FC Escada '{cargo}': {modo}{degraus_str}{piso_str}"


# ═══════════════════════════════════════════════════════════════════════
# UTILITÁRIOS
# ═══════════════════════════════════════════════════════════════════════

def export_json(file_key: str, output_dir: str = None) -> str:
    """Exporta um JSON do cache para arquivo local (para inspeção/backup)."""
    filename = f"{file_key}.json"
    data = _load_json(filename)
    out_path = os.path.join(output_dir or os.getcwd(), filename)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    size = os.path.getsize(out_path)
    return f"✓ Exportado: {out_path} ({size:,} bytes)"


def list_modified_files() -> str:
    """Lista os arquivos de configuração no cache desta sessão."""
    lines = ["ARQUIVOS EM CACHE:", ""]
    cached = sorted(_sl._CACHE.keys())
    if cached:
        for key in cached:
            lines.append(f"  {key}")
    else:
        lines.append("  (nenhum dado em cache ainda)")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════
# SELF-TESTS
# ═══════════════════════════════════════════════════════════════════════

def _run_tests():
    """Bateria de testes internos."""
    import shutil
    import tempfile

    print("=" * 60)
    print("CONFIG MANAGER — SELF-TESTS")
    print("=" * 60)

    passed = 0
    failed = 0
    test_num = 0

    def _assert(cond: bool, label: str):
        nonlocal passed, failed, test_num
        test_num += 1
        if cond:
            print(f"  ✓ Test {test_num}: {label}")
            passed += 1
        else:
            print(f"  ✗ Test {test_num}: {label}")
            failed += 1

    # Injetar dados mock via cache do supabase_loader
    _sl.clear_cache()

    # Params
    _sl._CACHE["params.json"] = {"cap_fc_max": 1.0, "cap_atingimento_max": 1.0}

    # Cargos
    _sl._CACHE["cargos.json"] = [
        {"nome_cargo": "Gerente Linha", "tipo_cargo": "Linha", "tipo_comissao": "Recebimento", "TIPO_COMISSAO": "Recebimento"},
        {"nome_cargo": "Consultor Interno", "tipo_cargo": "Operacional", "tipo_comissao": "Faturamento", "TIPO_COMISSAO": "Faturamento"},
        {"nome_cargo": "Diretor", "tipo_cargo": "Diretoria", "tipo_comissao": "Faturamento", "TIPO_COMISSAO": "Faturamento"},
    ]

    # Colaboradores
    _sl._CACHE["colaboradores.json"] = [
        {"id_colaborador": "C001", "nome_colaborador": "André Caramello", "cargo": "Gerente Linha"},
        {"id_colaborador": "C002", "nome_colaborador": "Alice Silva", "cargo": "Consultor Interno"},
    ]

    # Config Comissao — schema flat (1 linha por hierarquia+cargo)
    _sl._CACHE["config_comissao.json"] = [
        {"linha": "Hidrologia", "grupo": "Equipamentos", "subgrupo": "Bombas",
         "tipo_mercadoria": "Produto", "fabricante": None,
         "cargo": "Consultor Interno", "fatia_cargo": 25.0,
         "taxa_rateio_maximo_pct": 8.0, "taxa_maxima_efetiva": 2.0, "ativo": True},
        {"linha": "Hidrologia", "grupo": "Equipamentos", "subgrupo": "Bombas",
         "tipo_mercadoria": "Produto", "fabricante": None,
         "cargo": "Gerente Linha", "fatia_cargo": 40.0,
         "taxa_rateio_maximo_pct": 8.0, "taxa_maxima_efetiva": 3.2, "ativo": True},
        {"linha": "Hidrologia", "grupo": "Equipamentos", "subgrupo": "Bombas",
         "tipo_mercadoria": "Produto", "fabricante": None,
         "cargo": "Diretor", "fatia_cargo": 10.0,
         "taxa_rateio_maximo_pct": 8.0, "taxa_maxima_efetiva": 0.8, "ativo": True},
        {"linha": "Hidrologia", "grupo": "Equipamentos", "subgrupo": "Medidores",
         "tipo_mercadoria": "Produto", "fabricante": None,
         "cargo": "Consultor Interno", "fatia_cargo": 30.0,
         "taxa_rateio_maximo_pct": 6.0, "taxa_maxima_efetiva": 1.8, "ativo": True},
        {"linha": "Saneamento", "grupo": "Reagentes", "subgrupo": "Cloro",
         "tipo_mercadoria": "Insumo", "fabricante": None,
         "cargo": "Consultor Interno", "fatia_cargo": 20.0,
         "taxa_rateio_maximo_pct": 5.0, "taxa_maxima_efetiva": 1.0, "ativo": True},
        {"linha": "Inativo", "grupo": "Teste", "subgrupo": "Teste",
         "tipo_mercadoria": "Produto", "fabricante": None,
         "cargo": "Diretor", "fatia_cargo": 100.0,
         "taxa_rateio_maximo_pct": 1.0, "taxa_maxima_efetiva": 1.0, "ativo": False},
    ]

    # Pesos metas
    _sl._CACHE["pesos_metas.json"] = [
        {"cargo": "Gerente Linha", "faturamento_linha": 100, "rentabilidade": 0},
        {"cargo": "Consultor Interno", "faturamento_linha": 100, "rentabilidade": 0},
    ]

    # FC Escada
    _sl._CACHE["fc_escada_cargos.json"] = {
        "Gerente Linha": {"modo": "ESCADA", "num_degraus": 2, "piso_pct": 50},
    }

    # Cross-selling
    _sl._CACHE["cross_selling.json"] = [
        {"colaborador": "Mateus Machado", "taxa_cross_selling_pct": 1.5},
    ]

    # Aliases
    _sl._CACHE["aliases.json"] = {"colaborador": {"CARLOS.SILVA": "Carlos"}}

    # Metas individuais
    _sl._CACHE["metas_individuais.json"] = [
        {"colaborador": "Alice Silva", "cargo": "Consultor Interno", "tipo_meta": "faturamento", "valor_meta": 100000},
    ]

    # Metas aplicação
    _sl._CACHE["metas_aplicacao.json"] = [
        {"linha": "Hidrologia", "grupo": None, "subgrupo": None, "tipo_mercadoria": None, "tipo_meta": "faturamento", "valor_meta": 500000},
    ]

    # Meta rentabilidade
    _sl._CACHE["meta_rentabilidade.json"] = {
        "2025-10": [
            {"linha": "Hidrologia", "grupo": "Equipamentos", "subgrupo": "Bombas", "tipo_mercadoria": "Produto",
             "referencia_media_ponderada_pct": 0.15, "meta_rentabilidade_alvo_pct": 0.20},
        ],
    }

    # Metas fornecedores
    _sl._CACHE["metas_fornecedores.json"] = [
        {"linha": "Hidrologia", "fornecedor": "YSI", "moeda": "USD", "meta_anual": 30},
    ]

    # Enum tipo meta
    _sl._CACHE["enum_tipo_meta.json"] = [
        {"tipo_meta": "faturamento_linha", "escopo": "linha", "descricao": "Faturamento da linha"},
    ]

    # Cache de dados de teste já injetado via _sl._CACHE acima

    try:
        # --- TESTES DE LEITURA ---
        print("\n--- Testes de Leitura ---")

        result = diagnose()
        _assert("DIAGNÓSTICO" in result, "diagnose() retorna diagnóstico")
        _assert("Supabase" in result, "diagnose() menciona Supabase")
        _assert("params.json: OK" in result, "diagnose() valida leitura de params.json")

        result = summary()
        _assert("RESUMO DAS REGRAS" in result, "summary() retorna resumo")
        _assert("Hidrologia" in result, "summary() contém Hidrologia")

        result = get_params()
        _assert("cap_fc_max: 1.0" in result, "get_params() mostra cap_fc_max")

        result = list_colaboradores()
        _assert("André Caramello" in result, "list_colaboradores() mostra André")
        _assert("Recebimento" in result, "list_colaboradores() mostra tipo comissão")

        result = query_regras(linha="Hidrologia")
        _assert("linhas encontradas" in result, "query_regras filtra por linha")
        _assert("Bombas" in result, "query_regras mostra Bombas")
        _assert("Medidores" in result, "query_regras mostra Medidores")

        result = query_regras(linha="Hidrologia", cargo="Gerente Linha")
        _assert("40.0%" in result, "query_regras filtra por cargo mostra fatia")

        result = query_regras(somente_ativos=True)
        _assert("Inativo" not in result.split("REGRAS")[1], "query_regras omite inativos")

        result = get_pesos_metas(cargo="Gerente Linha")
        _assert("faturamento_linha: 100" in result, "get_pesos_metas filtra cargo")

        result = get_metas(tipo="faturamento")
        _assert("Alice Silva" in result, "get_metas mostra meta individual")
        _assert("500,000" in result or "500000" in result, "get_metas mostra meta aplicação")

        result = get_meta_rentabilidade(mes=10, ano=2025)
        _assert("Bombas" in result, "get_meta_rentabilidade filtra período")

        result = get_meta_rentabilidade(mes=1, ano=2024)
        _assert("Nenhuma meta" in result, "get_meta_rentabilidade período inexistente")

        result = get_fc_escada()
        _assert("ESCADA" in result, "get_fc_escada mostra modo")

        result = get_cross_selling()
        _assert("Mateus Machado" in result, "get_cross_selling mostra colaborador")

        result = get_aliases()
        _assert("CARLOS.SILVA" in result, "get_aliases mostra alias")

        result = resolve_alias("CARLOS.SILVA")
        _assert(result == "Carlos", "resolve_alias funciona")

        result = resolve_alias("nome.desconhecido")
        _assert(result == "nome.desconhecido", "resolve_alias mantém desconhecido")

        # --- TESTES DE ESCRITA ---
        print("\n--- Testes de Escrita ---")

        # add_regra: nova linha (não existe ainda)
        result = add_regra(
            linha="Hidrologia",
            cargo="Supervisor",
            taxa_rateio_maximo_pct=8.0,
            fatia_cargo=5.0,
            grupo="Equipamentos",
            subgrupo="Bombas",
            tipo_mercadoria="Produto",
        )
        _assert("CRIADA" in result, "add_regra cria nova linha")
        _assert("efetiva=0.4000%" in result, "add_regra calcula taxa_maxima_efetiva")

        # add_regra: atualiza linha existente
        result = add_regra(
            linha="Hidrologia",
            cargo="Consultor Interno",
            taxa_rateio_maximo_pct=8.0,
            fatia_cargo=30.0,
            grupo="Equipamentos",
            subgrupo="Bombas",
            tipo_mercadoria="Produto",
        )
        _assert("ATUALIZADA" in result, "add_regra atualiza linha existente")

        # Verificar persistência via query
        result = query_regras(linha="Hidrologia", subgrupo="Bombas", cargo="Consultor Interno")
        _assert("30.0%" in result, "add_regra persistiu fatia=30%")

        # update_regra
        result = update_regra(
            filtro={"linha": "Hidrologia", "subgrupo": "Bombas", "cargo": "Gerente Linha"},
            fatia_cargo=45.0,
        )
        _assert("ATUALIZAÇÃO CONCLUÍDA" in result, "update_regra sucesso")
        _assert("1 linha(s)" in result, "update_regra: 1 modificada")

        result = query_regras(linha="Hidrologia", subgrupo="Bombas", cargo="Gerente Linha")
        _assert("45.0%" in result, "update_regra persistiu fatia=45%")

        # remove_regra (desativar)
        result = remove_regra(filtro={"linha": "Saneamento"}, desativar=True)
        _assert("REMOÇÃO CONCLUÍDA" in result, "remove_regra desativar sucesso")
        result = query_regras(linha="Saneamento", somente_ativos=True)
        _assert("Nenhuma regra" in result, "remove_regra desativou linha Saneamento")

        # remove_regra (excluir permanente)
        result = remove_regra(
            filtro={"linha": "Hidrologia", "subgrupo": "Bombas", "cargo": "Supervisor"},
            desativar=False,
        )
        _assert("REMOÇÃO CONCLUÍDA" in result, "remove_regra exclusão permanente")

        # set_pesos_metas
        result = set_pesos_metas("Gerente Linha", {"faturamento_linha": 60, "rentabilidade": 40})
        _assert("Pesos do FC" in result, "set_pesos_metas sucesso")
        result = get_pesos_metas(cargo="Gerente Linha")
        _assert("faturamento_linha: 60" in result, "set_pesos_metas persistiu")

        result = set_pesos_metas("Gerente Linha", {"faturamento_linha": 60, "rentabilidade": 30})
        _assert("Soma dos pesos = 90.0%" in result, "set_pesos_metas rejeita soma != 100")

        # set_meta_individual
        result = set_meta_individual("Alice Silva", "Consultor Interno", "faturamento_linha", 200000)
        _assert("Meta individual" in result, "set_meta_individual sucesso")

        # set_meta_rentabilidade
        result = set_meta_rentabilidade(
            linha="Remediação", grupo="Skimmer",
            referencia_pct=0.20, meta_alvo_pct=0.30, periodo="2025"
        )
        _assert("Meta rentabilidade" in result, "set_meta_rentabilidade sucesso")

        # set_meta_aplicacao
        result = set_meta_aplicacao("Remediação", "faturamento_linha", 1000000)
        _assert("Meta aplicação" in result, "set_meta_aplicacao sucesso")

        # add_colaborador
        result = add_colaborador("C003", "Novo Colaborador", "Diretor")
        _assert("adicionado com sucesso" in result, "add_colaborador sucesso")
        result = list_colaboradores()
        _assert("Novo Colaborador" in result, "add_colaborador persistiu")

        result = add_colaborador("C004", "André Caramello", "Gerente Linha")
        _assert("já existe" in result, "add_colaborador bloqueia duplicata")

        # add_alias
        result = add_alias("colaborador", "NOVO.ALIAS", "Novo Nome")
        _assert("adicionado" in result, "add_alias sucesso")
        result = resolve_alias("NOVO.ALIAS")
        _assert(result == "Novo Nome", "add_alias resolve corretamente")

        # list_modified_files
        result = list_modified_files()
        _assert("config_comissao.json" in result, "list_modified_files lista arquivos")

    finally:
        # Limpar cache de teste
        _sl.clear_cache()

    # --- RESULTADO ---
    print()
    print("=" * 60)
    print(f"RESULTADO: {passed}/{passed + failed} testes passaram")
    if failed:
        print(f"  ⚠ {failed} teste(s) falharam!")
    else:
        print("  ✓ Todos os testes passaram!")
    print("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    if "--test" in sys.argv:
        sys.exit(_run_tests())
    else:
        print("Uso: python config_manager.py --test")
        print("  Executa a bateria de testes internos.")
