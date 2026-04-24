"""
cross_selling_modal.py — Modal GUI para configurar opção A/B de Cross-Selling

Exibe uma janela tkinter com todos os consultores externos detectados no mês,
permitindo ao usuário escolher individualmente a opção A ou B para cada um.

API pública:
    ask_cross_selling_options(cases) -> Dict[str, str]
    # {nome_consultor: "A" ou "B"}
"""

from __future__ import annotations

import tkinter as tk
from typing import Any, Dict, List


def ask_cross_selling_options(cases: List[Any]) -> Dict[str, str]:
    """Abre modal para selecionar opção A ou B por consultor de cross-selling.

    Args:
        cases: Lista de CrossSellingCase (com atributos .consultor,
               .taxa_cross_selling_pct, .processo) ou lista de dicts equivalentes.

    Returns:
        {nome_consultor: "A" ou "B"}. Se cases vazio, retorna {}.
    """
    # Deduplicar por consultor, acumular processos
    consultores: Dict[str, Dict] = {}
    for case in cases:
        if hasattr(case, "consultor"):
            nome = case.consultor
            taxa = float(case.taxa_cross_selling_pct)
            processo = str(case.processo)
        else:
            nome = case.get("consultor", "")
            taxa = float(case.get("taxa_cross_selling_pct", 0))
            processo = str(case.get("processo", ""))

        if not nome:
            continue
        if nome not in consultores:
            consultores[nome] = {"taxa": taxa, "processos": []}
        if processo and processo not in consultores[nome]["processos"]:
            consultores[nome]["processos"].append(processo)

    if not consultores:
        return {}

    result: Dict[str, str] = {}
    opcoes_vars: Dict[str, tk.StringVar] = {}

    # ── Cores ──────────────────────────────────────────────────────────
    BG = "#f2f4f6"
    HEADER_BG = "#1a2433"
    HEADER_FG = "white"
    ROW_A_BG = "#ffffff"
    ROW_B_BG = "#eef1f5"
    BTN_BG = "#2563eb"
    BTN_FG = "white"
    RED = "#c0392b"
    GREEN = "#1a7f37"

    root = tk.Tk()
    root.title("Cross-Selling — Selecionar Opção por Consultor")
    root.configure(bg=BG)
    root.resizable(False, False)
    root.attributes("-topmost", True)

    # ── Cabeçalho ──────────────────────────────────────────────────────
    hdr = tk.Frame(root, bg=HEADER_BG)
    hdr.pack(fill="x")
    tk.Label(
        hdr,
        text="Cross-Selling — Configuração de Opção por Consultor",
        font=("Segoe UI", 12, "bold"),
        bg=HEADER_BG,
        fg=HEADER_FG,
        pady=12,
        padx=20,
    ).pack(anchor="w")

    # ── Legenda ────────────────────────────────────────────────────────
    leg = tk.Frame(root, bg=BG)
    leg.pack(fill="x", padx=20, pady=(12, 6))

    for cor, letra, descricao in [
        (RED,   "A", "A taxa do consultor é descontada da taxa do colaborador (comissão total preservada)"),
        (GREEN, "B", "A taxa do consultor é adicional — colaborador mantém sua taxa integral  [padrão]"),
    ]:
        row = tk.Frame(leg, bg=BG)
        row.pack(anchor="w", pady=1)
        tk.Label(row, text=f"  Opção {letra}: ", font=("Segoe UI", 9, "bold"),
                 bg=BG, fg=cor).pack(side="left")
        tk.Label(row, text=descricao, font=("Segoe UI", 9), bg=BG).pack(side="left")

    # ── Tabela ─────────────────────────────────────────────────────────
    sep = tk.Frame(root, bg="#c5cbd3", height=1)
    sep.pack(fill="x", padx=20, pady=(6, 0))

    tbl_outer = tk.Frame(root, bg=BG)
    tbl_outer.pack(fill="both", padx=20, pady=0, expand=True)

    # Cabeçalho de colunas
    col_configs = [
        ("Consultor Externo", 24, "w"),
        ("Taxa CS (%)",       10, "center"),
        ("Processos",         20, "w"),
        ("Opção  A / B",      14, "center"),
    ]
    for col_idx, (label, width, anchor) in enumerate(col_configs):
        tk.Label(
            tbl_outer,
            text=label,
            font=("Segoe UI", 9, "bold"),
            bg=HEADER_BG,
            fg=HEADER_FG,
            width=width,
            anchor=anchor,
            padx=8,
            pady=5,
        ).grid(row=0, column=col_idx, sticky="nsew", padx=(0, 1), pady=(1, 0))

    # Linhas por consultor
    for i, (nome, info) in enumerate(consultores.items()):
        row_bg = ROW_A_BG if i % 2 == 0 else ROW_B_BG
        processos = info["processos"]
        procs_str = ", ".join(processos[:4])
        if len(processos) > 4:
            procs_str += f" +{len(processos) - 4} mais"

        tk.Label(tbl_outer, text=nome, bg=row_bg, anchor="w",
                 font=("Segoe UI", 9), padx=8, pady=6,
                 width=24).grid(row=i + 1, column=0, sticky="nsew", padx=(0, 1), pady=(0, 1))

        tk.Label(tbl_outer, text=f"{info['taxa']:.1f}%", bg=row_bg, anchor="center",
                 font=("Segoe UI", 9), padx=8,
                 width=10).grid(row=i + 1, column=1, sticky="nsew", padx=(0, 1), pady=(0, 1))

        tk.Label(tbl_outer, text=procs_str, bg=row_bg, anchor="w",
                 font=("Segoe UI", 9), padx=8,
                 width=20).grid(row=i + 1, column=2, sticky="nsew", padx=(0, 1), pady=(0, 1))

        var = tk.StringVar(value="B")
        opcoes_vars[nome] = var

        radio_cell = tk.Frame(tbl_outer, bg=row_bg)
        radio_cell.grid(row=i + 1, column=3, sticky="nsew", padx=(0, 1), pady=(0, 1))
        radio_cell.configure(width=14 * 7)

        tk.Radiobutton(
            radio_cell, text="A", variable=var, value="A",
            bg=row_bg, activebackground=row_bg,
            fg=RED, font=("Segoe UI", 10, "bold"),
            selectcolor=row_bg,
        ).pack(side="left", padx=(14, 4), pady=4)

        tk.Radiobutton(
            radio_cell, text="B", variable=var, value="B",
            bg=row_bg, activebackground=row_bg,
            fg=GREEN, font=("Segoe UI", 10, "bold"),
            selectcolor=row_bg,
        ).pack(side="left", padx=(4, 14), pady=4)

    sep2 = tk.Frame(root, bg="#c5cbd3", height=1)
    sep2.pack(fill="x", padx=20, pady=(0, 0))

    # ── Rodapé / Botão ─────────────────────────────────────────────────
    footer = tk.Frame(root, bg=BG)
    footer.pack(fill="x", pady=16)

    def confirmar() -> None:
        for nome, var in opcoes_vars.items():
            result[nome] = var.get()
        root.destroy()

    btn = tk.Button(
        footer,
        text="  Confirmar e Prosseguir  ",
        command=confirmar,
        bg=BTN_BG,
        fg=BTN_FG,
        font=("Segoe UI", 10, "bold"),
        relief="flat",
        padx=24,
        pady=8,
        cursor="hand2",
        activebackground="#1d4ed8",
        activeforeground=BTN_FG,
    )
    btn.pack()

    # Fechar pela janela usa defaults (B para todos)
    root.protocol("WM_DELETE_WINDOW", confirmar)

    # Centralizar na tela
    root.update_idletasks()
    w = root.winfo_reqwidth()
    h = root.winfo_reqheight()
    x = (root.winfo_screenwidth() - w) // 2
    y = (root.winfo_screenheight() - h) // 2
    root.geometry(f"{w}x{h}+{x}+{y}")

    root.mainloop()
    return result
