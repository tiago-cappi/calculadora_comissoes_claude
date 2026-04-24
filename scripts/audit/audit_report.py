"""
audit_report.py — Gera relatório consolidado de auditoria do pipeline de comissões.

Combina resultados de invariantes, dados do pipeline e análise de qualidade
em um relatório legível em Markdown ou HTML.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

ROOT = Path(__file__).resolve().parent.parent.parent


def _fmt_brl(v: float) -> str:
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _status_icon(status: str, severidade: str) -> str:
    if status == "PASS":
        return "✓"
    if status == "FAIL":
        if severidade == "CRITICAL":
            return "✗"
        if severidade == "WARNING":
            return "⚠"
        return "ℹ"
    return "ℹ"


def _gerar_markdown(
    mes: int,
    ano: int,
    resultado_data: dict,
    invariantes_result: Optional[dict],
    incluir_detalhes: bool,
) -> str:
    lines = []

    status_pipeline = resultado_data.get("status", "unknown")
    total_geral = resultado_data.get("total_geral", 0.0)
    comissoes = resultado_data.get("comissoes", [])
    etapas = resultado_data.get("etapas", [])
    erros = resultado_data.get("erros", [])
    avisos = resultado_data.get("avisos", [])

    lines.append(f"# Relatório de Auditoria — {mes:02d}/{ano}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Sumário executivo
    lines.append("## Sumário Executivo")
    lines.append("")
    lines.append(f"| Campo | Valor |")
    lines.append(f"|---|---|")
    lines.append(f"| Status do Pipeline | `{status_pipeline.upper()}` |")
    lines.append(f"| Total de Comissões | {_fmt_brl(total_geral)} |")
    lines.append(f"| Colaboradores com Resultado | {len(comissoes)} |")
    lines.append(f"| Etapas Executadas | {len(etapas)} |")
    lines.append(f"| Erros no Pipeline | {len(erros)} |")
    lines.append(f"| Avisos no Pipeline | {len(avisos)} |")
    lines.append("")

    if invariantes_result:
        passou = invariantes_result.get("passou", 0)
        alertas = invariantes_result.get("alertas", 0)
        criticos = invariantes_result.get("criticos", 0)
        total_v = invariantes_result.get("total_verificacoes", 0)
        lines.append(f"| Invariantes Verificadas | {total_v} |")
        lines.append(f"| Invariantes OK | {passou} |")
        lines.append(f"| Alertas (WARNING) | {alertas} |")
        lines.append(f"| Críticos (CRITICAL) | {criticos} |")
        lines.append("")

    # Comissões por colaborador
    lines.append("## Comissões por Colaborador")
    lines.append("")
    if comissoes:
        lines.append("| Colaborador | Cargo | Total | Potencial | Itens |")
        lines.append("|---|---|---:|---:|---:|")
        for c in sorted(comissoes, key=lambda x: x.get("total_faturamento", 0), reverse=True):
            lines.append(
                f"| {c.get('colaborador', '?')} "
                f"| {c.get('cargo', '?')} "
                f"| {_fmt_brl(float(c.get('total_faturamento', 0)))} "
                f"| {_fmt_brl(float(c.get('total_potencial', 0)))} "
                f"| {c.get('itens', 0)} |"
            )
        lines.append(f"| **TOTAL** | | **{_fmt_brl(total_geral)}** | | |")
    else:
        lines.append("_Nenhuma comissão calculada._")
    lines.append("")

    # Invariantes
    if invariantes_result and invariantes_result.get("detalhes"):
        lines.append("## Validação de Invariantes")
        lines.append("")

        detalhes = invariantes_result["detalhes"]

        # Críticos primeiro
        criticos = [d for d in detalhes if d.get("status") == "FAIL" and d.get("severidade") == "CRITICAL"]
        warnings = [d for d in detalhes if d.get("status") == "FAIL" and d.get("severidade") == "WARNING"]
        passes = [d for d in detalhes if d.get("status") == "PASS"]
        infos = [d for d in detalhes if d.get("status") == "INFO" or (d.get("status") == "FAIL" and d.get("severidade") == "INFO")]

        if criticos:
            lines.append("### Erros Críticos")
            lines.append("")
            for d in criticos:
                icon = _status_icon(d["status"], d["severidade"])
                lines.append(f"**{icon} {d['id']}: {d['descricao']}**")
                if incluir_detalhes and d.get("contexto"):
                    ctx = d["contexto"]
                    if ctx:
                        lines.append(f"  - Contexto: `{ctx}`")
                if d.get("fix_suggestion"):
                    lines.append(f"  - Sugestão: {d['fix_suggestion']}")
                lines.append("")

        if warnings:
            lines.append("### Alertas")
            lines.append("")
            for d in warnings:
                icon = _status_icon(d["status"], d["severidade"])
                lines.append(f"**{icon} {d['id']}: {d['descricao']}**")
                if incluir_detalhes and d.get("contexto"):
                    ctx = d["contexto"]
                    if ctx:
                        lines.append(f"  - Contexto: `{ctx}`")
                if d.get("fix_suggestion"):
                    lines.append(f"  - Sugestão: {d['fix_suggestion']}")
                lines.append("")

        if passes or infos:
            lines.append("### Verificações OK")
            lines.append("")
            for d in passes + infos:
                icon = _status_icon(d["status"], d["severidade"])
                lines.append(f"- {icon} **{d['id']}**: {d['descricao']}")
            lines.append("")

    # Erros e avisos do pipeline
    if erros and incluir_detalhes:
        lines.append("## Erros do Pipeline")
        lines.append("")
        for e in erros[:20]:
            if isinstance(e, dict):
                stage = e.get("stage", "?")
                tipo = e.get("tipo", "?")
                msg = e.get("mensagem", str(e))
                fix = e.get("fix_suggestion", "")
                lines.append(f"- **[{stage}] {tipo}**: {msg}")
                if fix:
                    lines.append(f"  - Fix: {fix}")
            else:
                lines.append(f"- {e}")
        if len(erros) > 20:
            lines.append(f"- _...e mais {len(erros) - 20} erros_")
        lines.append("")

    # Etapas do pipeline
    lines.append("## Etapas do Pipeline")
    lines.append("")
    lines.append("| ID | Etapa | Status | Detalhes |")
    lines.append("|---|---|---|---|")
    for etapa in etapas:
        status_etapa = etapa.get("status", "?")
        icon = "✓" if status_etapa == "ok" else ("⚠" if status_etapa == "skipped" else "✗")
        lines.append(
            f"| {etapa.get('id', '?')} "
            f"| {etapa.get('nome', '?')} "
            f"| {icon} {status_etapa} "
            f"| {etapa.get('detalhes', '')} |"
        )
    lines.append("")

    lines.append("---")
    lines.append(f"*Relatório gerado pelo sistema de auditoria — {mes:02d}/{ano}*")

    return "\n".join(lines)


def _gerar_html(
    mes: int,
    ano: int,
    resultado_data: dict,
    invariantes_result: Optional[dict],
    incluir_detalhes: bool,
) -> str:
    md_content = _gerar_markdown(mes, ano, resultado_data, invariantes_result, incluir_detalhes)

    # Wrapper HTML simples — sem dependências externas
    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Auditoria Comissões {mes:02d}/{ano}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 1100px; margin: 40px auto; padding: 0 20px; color: #333; line-height: 1.6; }}
  h1 {{ color: #1a365d; border-bottom: 3px solid #2b6cb0; padding-bottom: 10px; }}
  h2 {{ color: #2b6cb0; margin-top: 30px; }}
  h3 {{ color: #e53e3e; }}
  table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
  th, td {{ border: 1px solid #e2e8f0; padding: 8px 12px; text-align: left; }}
  th {{ background: #ebf8ff; font-weight: 600; }}
  tr:nth-child(even) {{ background: #f7fafc; }}
  code {{ background: #edf2f7; padding: 2px 6px; border-radius: 3px; font-family: monospace; font-size: 0.9em; }}
  pre {{ background: #2d3748; color: #e2e8f0; padding: 16px; border-radius: 6px; overflow-x: auto; }}
  .pass {{ color: #276749; }} .fail {{ color: #c53030; }} .warn {{ color: #c05621; }} .info {{ color: #2b6cb0; }}
  hr {{ border: 0; border-top: 1px solid #e2e8f0; margin: 30px 0; }}
  blockquote {{ border-left: 4px solid #4299e1; margin: 0; padding: 10px 20px; background: #ebf8ff; }}
</style>
</head>
<body>
<div id="content">
"""

    # Converter markdown básico para HTML
    lines = md_content.split("\n")
    html_lines = []
    in_table = False
    in_list = False

    for line in lines:
        if line.startswith("# "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h1>{line[2:]}</h1>")
        elif line.startswith("## "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h2>{line[3:]}</h2>")
        elif line.startswith("### "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h3 class='fail'>{line[4:]}</h3>")
        elif line.startswith("|"):
            if not in_table:
                html_lines.append("<table>")
                in_table = True
            if "---|" in line:
                continue
            cells = [c.strip() for c in line.strip("|").split("|")]
            if html_lines and html_lines[-1] == "<table>":
                row = "<tr>" + "".join(f"<th>{c}</th>" for c in cells) + "</tr>"
            else:
                row = "<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"
            html_lines.append(row)
        elif in_table and not line.startswith("|"):
            html_lines.append("</table>")
            in_table = False
            if line.strip():
                html_lines.append(f"<p>{line}</p>")
        elif line.startswith("- "):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            content = line[2:].replace("**", "<strong>", 1).replace("**", "</strong>", 1)
            html_lines.append(f"<li>{content}</li>")
        elif line.startswith("---"):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append("<hr>")
        elif line.startswith("*") and line.endswith("*") and not line.startswith("**"):
            html_lines.append(f"<p><em>{line.strip('*')}</em></p>")
        elif line.strip() == "":
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            if in_table:
                html_lines.append("</table>")
                in_table = False
        else:
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            content = line.replace("**", "<strong>", 1).replace("**", "</strong>", 1)
            content = content.replace("`", "<code>", 1).replace("`", "</code>", 1)
            if content.strip():
                html_lines.append(f"<p>{content}</p>")

    if in_table:
        html_lines.append("</table>")
    if in_list:
        html_lines.append("</ul>")

    html += "\n".join(html_lines)
    html += "\n</div>\n</body>\n</html>"
    return html


def gerar(
    mes: int,
    ano: int,
    resultado_data: dict,
    invariantes_result: Optional[dict] = None,
    formato: str = "markdown",
    incluir_detalhes: bool = True,
) -> str:
    """Gera relatório consolidado de auditoria.

    Args:
        mes: Mês de apuração.
        ano: Ano de apuração.
        resultado_data: Conteúdo do resultado.json.
        invariantes_result: Resultado de invariant_checker.verificar() (opcional).
        formato: 'markdown' ou 'html'.
        incluir_detalhes: Se True, inclui detalhes de cada verificação.

    Returns:
        String com o relatório no formato solicitado.
    """
    if formato.lower() == "html":
        return _gerar_html(mes, ano, resultado_data, invariantes_result, incluir_detalhes)
    return _gerar_markdown(mes, ano, resultado_data, invariantes_result, incluir_detalhes)
