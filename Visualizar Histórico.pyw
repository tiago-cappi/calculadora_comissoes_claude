"""
Visualizar Histórico.pyw — Visualizador e gerenciador do banco de dados de comissões

Permite consultar, filtrar e excluir registros das três tabelas do historico.db:
- historico_comissoes
- historico_processo_pai
- historico_pagamentos_processo_pai
"""

from __future__ import annotations

import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path
import datetime as _dt

import customtkinter as ctk

ROOT = Path(__file__).parent
DB_PATH = ROOT / "historico.db"

COLORS = {
    "primary":        "#2563EB",
    "primary_hover":  "#1D4ED8",
    "primary_soft":   "#EFF6FF",
    "success":        "#16A34A",
    "success_soft":   "#ECFDF5",
    "warning":        "#D97706",
    "warning_soft":   "#FFFBEB",
    "danger":         "#DC2626",
    "danger_soft":    "#FEF2F2",
    "bg":             "#F4F6F9",
    "surface":        "#FFFFFF",
    "surface_alt":    "#F8FAFC",
    "border":         "#E4E9F0",
    "border_strong":  "#C9D3DF",
    "text":           "#0F172A",
    "text_muted":     "#64748B",
    "text_soft":      "#94A3B8",
}

FONT_FAMILY = "Segoe UI"

# "Todos" como primeiro item para filtros opcionais
MESES = [
    "Todos",
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
]
# Mapeia nome -> número do mês (Todos=0, Janeiro=1, ..., Dezembro=12)
MESES_NUM: dict[str, int] = {nome: i for i, nome in enumerate(MESES)}


class HistoricoApp:
    def __init__(self, root: ctk.CTk) -> None:
        self.root = root
        root.title("Visualizar Histórico — Comissões")
        self._center_window(1200, 740)
        root.minsize(920, 580)
        root.configure(fg_color=COLORS["bg"])

        self._setup_treeview_style()
        self._build_ui()
        self._set_status("idle", "Pronto. Selecione os filtros e clique em 'Buscar'.")

    # ─── Layout principal ─────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        self._build_header()
        self._build_status_bar()  # antes do body para ficar no bottom
        self._build_body()

    def _build_header(self) -> None:
        header = ctk.CTkFrame(
            self.root, height=64, fg_color=COLORS["surface"],
            corner_radius=0, border_width=0,
        )
        header.pack(fill="x", side="top")
        header.pack_propagate(False)

        inner = ctk.CTkFrame(header, fg_color="transparent")
        inner.pack(fill="both", expand=True, padx=24)

        ctk.CTkLabel(
            inner,
            text="Visualizar Histórico",
            font=(FONT_FAMILY, 18, "bold"),
            text_color=COLORS["text"],
            anchor="w",
        ).pack(side="left", pady=18)

        ctk.CTkLabel(
            inner,
            text="   Consulte e gerencie os dados armazenados no banco de dados",
            font=(FONT_FAMILY, 12),
            text_color=COLORS["text_muted"],
            anchor="w",
        ).pack(side="left", pady=18)

        ctk.CTkFrame(
            self.root, height=1, fg_color=COLORS["border"], corner_radius=0,
        ).pack(fill="x", side="top")

    def _build_status_bar(self) -> None:
        self.status_bar = ctk.CTkFrame(
            self.root, height=36, corner_radius=0,
            fg_color=COLORS["surface_alt"], border_width=0,
        )
        self.status_bar.pack(fill="x", side="bottom")
        self.status_bar.pack_propagate(False)

        self._status_icon_var = ctk.StringVar(value="●")
        self._status_icon = ctk.CTkLabel(
            self.status_bar,
            textvariable=self._status_icon_var,
            font=(FONT_FAMILY, 12, "bold"),
            text_color=COLORS["text_muted"],
        )
        self._status_icon.pack(side="left", padx=(16, 6))

        self._status_var = ctk.StringVar(value="Pronto.")
        self._status_label = ctk.CTkLabel(
            self.status_bar,
            textvariable=self._status_var,
            font=(FONT_FAMILY, 11),
            text_color=COLORS["text_muted"],
            anchor="w",
        )
        self._status_label.pack(side="left", fill="x", expand=True)

        self._count_var = ctk.StringVar(value="")
        ctk.CTkLabel(
            self.status_bar,
            textvariable=self._count_var,
            font=(FONT_FAMILY, 11),
            text_color=COLORS["text_muted"],
            anchor="e",
        ).pack(side="right", padx=16)

    def _build_body(self) -> None:
        body = ctk.CTkFrame(self.root, fg_color="transparent")
        body.pack(fill="both", expand=True, padx=14, pady=(10, 6))

        self.tabs = ctk.CTkTabview(
            body,
            fg_color=COLORS["surface"],
            segmented_button_fg_color=COLORS["surface_alt"],
            segmented_button_selected_color=COLORS["primary"],
            segmented_button_selected_hover_color=COLORS["primary_hover"],
            segmented_button_unselected_color=COLORS["surface_alt"],
            segmented_button_unselected_hover_color=COLORS["border"],
            text_color=COLORS["text"],
            text_color_disabled=COLORS["text_muted"],
            border_color=COLORS["border"],
            border_width=1,
            corner_radius=10,
        )
        self.tabs.pack(fill="both", expand=True)

        self.tabs.add("Comissões")
        self.tabs.add("Processos Pai")
        self.tabs.add("Pagamentos")

        self._build_tab_comissoes(self.tabs.tab("Comissões"))
        self._build_tab_processos(self.tabs.tab("Processos Pai"))
        self._build_tab_pagamentos(self.tabs.tab("Pagamentos"))

    # ─── Tab: Comissões ───────────────────────────────────────────────────────

    def _build_tab_comissoes(self, tab: ctk.CTkFrame) -> None:
        hoje = _dt.date.today()
        self._c_mes_var = ctk.StringVar(value=MESES[hoje.month])
        self._c_ano_var = ctk.StringVar(value=str(hoje.year))
        self._c_colab_var = ctk.StringVar(value="Todos")

        # ── Painel de filtros ──
        filter_card = self._card(tab)
        filter_card.pack(fill="x", padx=8, pady=(8, 0))

        filter_inner = ctk.CTkFrame(filter_card, fg_color="transparent")
        filter_inner.pack(fill="x", padx=14, pady=10)

        # Linha de filtros
        filters_row = ctk.CTkFrame(filter_inner, fg_color="transparent")
        filters_row.pack(side="left", fill="x", expand=True)

        self._dropdown(filters_row, "Mês", MESES, self._c_mes_var, 130).pack(side="left", padx=(0, 12))

        anos = ["Todos"] + [str(y) for y in range(hoje.year - 5, hoje.year + 3)]
        self._dropdown(filter_inner, "Ano", anos, self._c_ano_var, 90).pack(side="left", padx=(0, 12))

        colabs = ["Todos"] + self._get_colaboradores_db()
        self._c_colab_menu_wrap = self._dropdown(filter_inner, "Colaborador", colabs, self._c_colab_var, 200)
        self._c_colab_menu_wrap.pack(side="left", padx=(0, 12))

        # Botões de filtro
        btn_row = ctk.CTkFrame(filter_inner, fg_color="transparent")
        btn_row.pack(side="right", padx=(12, 0))
        ctk.CTkLabel(btn_row, text=" ", font=(FONT_FAMILY, 11)).pack()
        inner_btns = ctk.CTkFrame(btn_row, fg_color="transparent")
        inner_btns.pack()
        ctk.CTkButton(
            inner_btns, text="Buscar", command=self._buscar_comissoes,
            width=90, height=32, fg_color=COLORS["primary"],
            hover_color=COLORS["primary_hover"], text_color="#FFFFFF",
            font=(FONT_FAMILY, 12, "bold"), corner_radius=6,
        ).pack(side="left")
        ctk.CTkButton(
            inner_btns, text="Limpar", command=self._limpar_comissoes,
            width=80, height=32, fg_color="transparent",
            hover_color=COLORS["surface_alt"], text_color=COLORS["text_muted"],
            border_width=1, border_color=COLORS["border"],
            font=(FONT_FAMILY, 12), corner_radius=6,
        ).pack(side="left", padx=(6, 0))

        # ── Barra de ações ──
        action_bar = ctk.CTkFrame(tab, fg_color="transparent")
        action_bar.pack(fill="x", padx=8, pady=(8, 0))

        ctk.CTkButton(
            action_bar, text="☑  Selecionar todos do período",
            command=lambda: self._selecionar_todos(self._c_tree),
            height=32, fg_color="transparent",
            hover_color=COLORS["primary_soft"], text_color=COLORS["primary"],
            border_width=1, border_color=COLORS["border"],
            font=(FONT_FAMILY, 12), corner_radius=6,
        ).pack(side="left")

        ctk.CTkButton(
            action_bar, text="✕  Excluir selecionadas",
            command=self._excluir_comissoes,
            height=32, fg_color=COLORS["danger"],
            hover_color="#B91C1C", text_color="#FFFFFF",
            font=(FONT_FAMILY, 12, "bold"), corner_radius=6,
        ).pack(side="left", padx=(8, 0))

        ctk.CTkButton(
            action_bar, text="↺  Atualizar colaboradores",
            command=self._recarregar_colaboradores,
            height=32, fg_color="transparent",
            hover_color=COLORS["surface_alt"], text_color=COLORS["text_muted"],
            border_width=1, border_color=COLORS["border"],
            font=(FONT_FAMILY, 12), corner_radius=6,
        ).pack(side="right")

        # ── Tabela ──
        columns = [
            ("nome",           "Nome",          150),
            ("cargo",          "Cargo",         110),
            ("processo",       "Processo",      110),
            ("numero_pc",      "Nº PC",          80),
            ("codigo_cliente", "Cód. Cliente",   95),
            ("tipo",           "Tipo",           80),
            ("tipo_pagamento", "Tipo Pagto.",     95),
            ("documento",      "Documento",     110),
            ("linha_negocio",  "Linha Neg.",    110),
            ("status_processo","Status Proc.",  110),
            ("mes_apuracao",   "Mês",            50),
            ("ano_apuracao",   "Ano",            55),
            ("valor_documento","Vlr. Doc.",       90),
            ("valor_processo", "Vlr. Proc.",      90),
            ("comissao_total", "Comissão Total", 115),
            ("reconciliado",   "Reconciliado",    90),
            ("created_at",     "Criado em",      140),
        ]
        self._c_tree = self._build_treeview(tab, columns)
        self.root.after(150, self._buscar_comissoes)

    # ─── Tab: Processos Pai ───────────────────────────────────────────────────

    def _build_tab_processos(self, tab: ctk.CTkFrame) -> None:
        hoje = _dt.date.today()
        self._p_mes_var = ctk.StringVar(value=MESES[hoje.month])
        self._p_ano_var = ctk.StringVar(value=str(hoje.year))

        filter_card = self._card(tab)
        filter_card.pack(fill="x", padx=8, pady=(8, 0))

        filter_inner = ctk.CTkFrame(filter_card, fg_color="transparent")
        filter_inner.pack(fill="x", padx=14, pady=10)

        self._dropdown(filter_inner, "Mês", MESES, self._p_mes_var, 130).pack(side="left", padx=(0, 12))
        anos = ["Todos"] + [str(y) for y in range(hoje.year - 5, hoje.year + 3)]
        self._dropdown(filter_inner, "Ano", anos, self._p_ano_var, 90).pack(side="left", padx=(0, 12))

        btn_row = ctk.CTkFrame(filter_inner, fg_color="transparent")
        btn_row.pack(side="right", padx=(12, 0))
        ctk.CTkLabel(btn_row, text=" ", font=(FONT_FAMILY, 11)).pack()
        inner_btns = ctk.CTkFrame(btn_row, fg_color="transparent")
        inner_btns.pack()
        ctk.CTkButton(
            inner_btns, text="Buscar", command=self._buscar_processos,
            width=90, height=32, fg_color=COLORS["primary"],
            hover_color=COLORS["primary_hover"], text_color="#FFFFFF",
            font=(FONT_FAMILY, 12, "bold"), corner_radius=6,
        ).pack(side="left")
        ctk.CTkButton(
            inner_btns, text="Limpar", command=self._limpar_processos,
            width=80, height=32, fg_color="transparent",
            hover_color=COLORS["surface_alt"], text_color=COLORS["text_muted"],
            border_width=1, border_color=COLORS["border"],
            font=(FONT_FAMILY, 12), corner_radius=6,
        ).pack(side="left", padx=(6, 0))

        action_bar = ctk.CTkFrame(tab, fg_color="transparent")
        action_bar.pack(fill="x", padx=8, pady=(8, 0))
        ctk.CTkButton(
            action_bar, text="☑  Selecionar todos do período",
            command=lambda: self._selecionar_todos(self._p_tree),
            height=32, fg_color="transparent",
            hover_color=COLORS["primary_soft"], text_color=COLORS["primary"],
            border_width=1, border_color=COLORS["border"],
            font=(FONT_FAMILY, 12), corner_radius=6,
        ).pack(side="left")
        ctk.CTkButton(
            action_bar, text="✕  Excluir selecionadas",
            command=self._excluir_processos,
            height=32, fg_color=COLORS["danger"],
            hover_color="#B91C1C", text_color="#FFFFFF",
            font=(FONT_FAMILY, 12, "bold"), corner_radius=6,
        ).pack(side="left", padx=(8, 0))

        columns = [
            ("numero_pc",        "Nº PC",          110),
            ("codigo_cliente",   "Cód. Cliente",   110),
            ("processo",         "Processo",       130),
            ("is_processo_pai",  "É Pai?",          65),
            ("status_faturado",  "Faturado",        80),
            ("status_pago",      "Pago",            65),
            ("mes_referencia",   "Mês",             50),
            ("ano_referencia",   "Ano",             55),
            ("created_at",       "Criado em",      150),
        ]
        self._p_tree = self._build_treeview(tab, columns)
        self.root.after(250, self._buscar_processos)

    # ─── Tab: Pagamentos ──────────────────────────────────────────────────────

    def _build_tab_pagamentos(self, tab: ctk.CTkFrame) -> None:
        hoje = _dt.date.today()
        self._pg_mes_var = ctk.StringVar(value=MESES[hoje.month])
        self._pg_ano_var = ctk.StringVar(value=str(hoje.year))

        filter_card = self._card(tab)
        filter_card.pack(fill="x", padx=8, pady=(8, 0))

        filter_inner = ctk.CTkFrame(filter_card, fg_color="transparent")
        filter_inner.pack(fill="x", padx=14, pady=10)

        self._dropdown(filter_inner, "Mês", MESES, self._pg_mes_var, 130).pack(side="left", padx=(0, 12))
        anos = ["Todos"] + [str(y) for y in range(hoje.year - 5, hoje.year + 3)]
        self._dropdown(filter_inner, "Ano", anos, self._pg_ano_var, 90).pack(side="left", padx=(0, 12))

        btn_row = ctk.CTkFrame(filter_inner, fg_color="transparent")
        btn_row.pack(side="right", padx=(12, 0))
        ctk.CTkLabel(btn_row, text=" ", font=(FONT_FAMILY, 11)).pack()
        inner_btns = ctk.CTkFrame(btn_row, fg_color="transparent")
        inner_btns.pack()
        ctk.CTkButton(
            inner_btns, text="Buscar", command=self._buscar_pagamentos,
            width=90, height=32, fg_color=COLORS["primary"],
            hover_color=COLORS["primary_hover"], text_color="#FFFFFF",
            font=(FONT_FAMILY, 12, "bold"), corner_radius=6,
        ).pack(side="left")
        ctk.CTkButton(
            inner_btns, text="Limpar", command=self._limpar_pagamentos,
            width=80, height=32, fg_color="transparent",
            hover_color=COLORS["surface_alt"], text_color=COLORS["text_muted"],
            border_width=1, border_color=COLORS["border"],
            font=(FONT_FAMILY, 12), corner_radius=6,
        ).pack(side="left", padx=(6, 0))

        action_bar = ctk.CTkFrame(tab, fg_color="transparent")
        action_bar.pack(fill="x", padx=8, pady=(8, 0))
        ctk.CTkButton(
            action_bar, text="☑  Selecionar todos do período",
            command=lambda: self._selecionar_todos(self._pg_tree),
            height=32, fg_color="transparent",
            hover_color=COLORS["primary_soft"], text_color=COLORS["primary"],
            border_width=1, border_color=COLORS["border"],
            font=(FONT_FAMILY, 12), corner_radius=6,
        ).pack(side="left")
        ctk.CTkButton(
            action_bar, text="✕  Excluir selecionadas",
            command=self._excluir_pagamentos,
            height=32, fg_color=COLORS["danger"],
            hover_color="#B91C1C", text_color="#FFFFFF",
            font=(FONT_FAMILY, 12, "bold"), corner_radius=6,
        ).pack(side="left", padx=(8, 0))

        columns = [
            ("numero_pc",       "Nº PC",          110),
            ("codigo_cliente",  "Cód. Cliente",   110),
            ("processo",        "Processo",       130),
            ("numero_nf",       "Nº NF",           80),
            ("documento",       "Documento",      110),
            ("situacao_codigo", "Cód. Situação",   95),
            ("situacao_texto",  "Situação",        150),
            ("dt_prorrogacao",  "Prorrogação",    110),
            ("data_baixa",      "Data Baixa",     110),
            ("valor_documento", "Vlr. Doc.",        95),
            ("mes_referencia",  "Mês",              50),
            ("ano_referencia",  "Ano",              55),
            ("created_at",      "Criado em",       150),
        ]
        self._pg_tree = self._build_treeview(tab, columns)
        self.root.after(350, self._buscar_pagamentos)

    # ─── Builders auxiliares ──────────────────────────────────────────────────

    def _card(self, parent) -> ctk.CTkFrame:
        return ctk.CTkFrame(
            parent,
            fg_color=COLORS["surface"],
            border_color=COLORS["border"],
            border_width=1,
            corner_radius=8,
        )

    def _dropdown(self, parent, label: str, values: list[str], variable: ctk.StringVar, width: int) -> ctk.CTkFrame:
        wrap = ctk.CTkFrame(parent, fg_color="transparent")
        ctk.CTkLabel(
            wrap, text=label, font=(FONT_FAMILY, 11, "bold"),
            text_color=COLORS["text_muted"], anchor="w",
        ).pack(anchor="w")
        ctk.CTkOptionMenu(
            wrap, values=values, variable=variable,
            width=width, height=32,
            font=(FONT_FAMILY, 12), dropdown_font=(FONT_FAMILY, 12),
            fg_color=COLORS["surface"], button_color=COLORS["primary"],
            button_hover_color=COLORS["primary_hover"],
            text_color=COLORS["text"], dropdown_fg_color=COLORS["surface"],
            dropdown_text_color=COLORS["text"],
            dropdown_hover_color=COLORS["primary_soft"],
            corner_radius=6,
        ).pack(pady=(4, 0))
        return wrap

    def _build_treeview(self, parent, columns: list[tuple[str, str, int]]) -> ttk.Treeview:
        frame = tk.Frame(parent, bg=COLORS["bg"], bd=0, highlightthickness=0)
        frame.pack(fill="both", expand=True, padx=8, pady=(8, 8))

        col_ids = [c[0] for c in columns]
        tree = ttk.Treeview(
            frame,
            columns=col_ids,
            show="headings",
            selectmode="extended",
            style="Custom.Treeview",
        )

        for col_id, col_name, col_width in columns:
            tree.heading(col_id, text=col_name, anchor="w",
                         command=lambda c=col_id, t=tree: self._sort_column(t, c))
            tree.column(col_id, width=col_width, minwidth=40, anchor="w", stretch=False)

        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview, style="Custom.Vertical.TScrollbar")
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview, style="Custom.Horizontal.TScrollbar")
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)

        # Feedback visual de seleção via bind
        tree.bind("<<TreeviewSelect>>", lambda e: self._on_selection_change(tree))

        return tree

    def _setup_treeview_style(self) -> None:
        style = ttk.Style()
        style.theme_use("clam")
        style.configure(
            "Custom.Treeview",
            background=COLORS["surface"],
            foreground=COLORS["text"],
            fieldbackground=COLORS["surface"],
            rowheight=26,
            font=(FONT_FAMILY, 11),
            borderwidth=0,
            relief="flat",
        )
        style.configure(
            "Custom.Treeview.Heading",
            background=COLORS["surface_alt"],
            foreground=COLORS["text_muted"],
            font=(FONT_FAMILY, 11, "bold"),
            borderwidth=1,
            relief="flat",
            padding=(6, 4),
        )
        style.map(
            "Custom.Treeview",
            background=[("selected", COLORS["primary_soft"])],
            foreground=[("selected", COLORS["primary"])],
        )
        style.map(
            "Custom.Treeview.Heading",
            background=[("active", COLORS["border"])],
            relief=[("active", "flat")],
        )
        style.configure("Custom.Vertical.TScrollbar",
            troughcolor=COLORS["surface_alt"], background=COLORS["border_strong"],
            borderwidth=0, arrowsize=12,
        )
        style.configure("Custom.Horizontal.TScrollbar",
            troughcolor=COLORS["surface_alt"], background=COLORS["border_strong"],
            borderwidth=0, arrowsize=12,
        )

    # ─── DB helpers ───────────────────────────────────────────────────────────

    def _get_db(self) -> sqlite3.Connection | None:
        if not DB_PATH.exists():
            messagebox.showerror(
                "Banco de dados não encontrado",
                f"O arquivo historico.db não foi encontrado em:\n{DB_PATH}\n\n"
                "Execute o cálculo de comissões pelo menos uma vez para gerar o banco.",
            )
            return None
        return sqlite3.connect(DB_PATH)

    def _get_colaboradores_db(self) -> list[str]:
        if not DB_PATH.exists():
            return []
        try:
            con = sqlite3.connect(DB_PATH)
            cur = con.execute(
                "SELECT DISTINCT nome FROM historico_comissoes ORDER BY nome"
            )
            result = [r[0] for r in cur.fetchall() if r[0]]
            con.close()
            return result
        except Exception:
            return []

    def _populate_tree(self, tree: ttk.Treeview, rows: list, rowids: list[int]) -> None:
        tree.delete(*tree.get_children())
        for i, (row, rowid) in enumerate(zip(rows, rowids)):
            tag = "even" if i % 2 == 0 else "odd"
            tree.insert(
                "", "end", iid=str(rowid),
                values=[str(v) if v is not None else "" for v in row],
                tags=(tag,),
            )
        tree.tag_configure("even", background=COLORS["surface"])
        tree.tag_configure("odd", background=COLORS["surface_alt"])

    def _build_where(self, conditions: list[str], params: list) -> str:
        return ("WHERE " + " AND ".join(conditions)) if conditions else ""

    # ─── Ordenação de colunas ─────────────────────────────────────────────────

    _sort_state: dict[str, tuple[str, bool]] = {}  # tree_id -> (col, reverse)

    def _sort_column(self, tree: ttk.Treeview, col: str) -> None:
        tree_id = str(id(tree))
        reverse = self._sort_state.get(tree_id, (None, False))[1]
        if self._sort_state.get(tree_id, (None,))[0] == col:
            reverse = not reverse
        else:
            reverse = False
        self._sort_state[tree_id] = (col, reverse)

        items = [(tree.set(iid, col), iid) for iid in tree.get_children("")]
        try:
            items.sort(key=lambda x: float(x[0]) if x[0] else -1, reverse=reverse)
        except ValueError:
            items.sort(key=lambda x: x[0].lower(), reverse=reverse)

        for index, (_, iid) in enumerate(items):
            tree.move(iid, "", index)
            tag = "even" if index % 2 == 0 else "odd"
            tree.item(iid, tags=(tag,))
        tree.tag_configure("even", background=COLORS["surface"])
        tree.tag_configure("odd", background=COLORS["surface_alt"])

        arrow = " ↓" if reverse else " ↑"
        col_meta = [c for c in tree["columns"]]
        for c in col_meta:
            heading_text = tree.heading(c)["text"].replace(" ↑", "").replace(" ↓", "")
            tree.heading(c, text=heading_text)
        current_text = tree.heading(col)["text"].replace(" ↑", "").replace(" ↓", "")
        tree.heading(col, text=current_text + arrow)

    # ─── Comissões ─────────────────────────────────────────────────────────────

    def _buscar_comissoes(self) -> None:
        con = self._get_db()
        if con is None:
            return
        try:
            conditions, params = [], []
            mes_str = self._c_mes_var.get()
            ano_str = self._c_ano_var.get()
            colab = self._c_colab_var.get()

            if mes_str != "Todos":
                conditions.append("mes_apuracao = ?")
                params.append(MESES_NUM[mes_str])
            if ano_str != "Todos":
                conditions.append("ano_apuracao = ?")
                params.append(int(ano_str))
            if colab != "Todos":
                conditions.append("nome = ?")
                params.append(colab)

            where = self._build_where(conditions, params)
            rows = con.execute(f"""
                SELECT rowid, nome, cargo, processo, numero_pc, codigo_cliente,
                       tipo, tipo_pagamento, documento, linha_negocio,
                       status_processo, mes_apuracao, ano_apuracao,
                       valor_documento, valor_processo, comissao_total,
                       reconciliado, created_at
                FROM historico_comissoes {where}
                ORDER BY ano_apuracao DESC, mes_apuracao DESC, nome
            """, params).fetchall()

            self._populate_tree(self._c_tree, [r[1:] for r in rows], [r[0] for r in rows])
            n = len(rows)
            self._set_status("idle", f"Comissões — {n} registro(s) encontrado(s).")
            self._count_var.set(f"{n} linha(s)")
        except Exception as e:
            self._set_status("error", f"Erro ao buscar comissões: {e}")
        finally:
            con.close()

    def _limpar_comissoes(self) -> None:
        self._c_mes_var.set("Todos")
        self._c_ano_var.set("Todos")
        self._c_colab_var.set("Todos")
        self._buscar_comissoes()

    def _excluir_comissoes(self) -> None:
        selected = self._c_tree.selection()
        if not selected:
            messagebox.showwarning("Nenhuma linha selecionada",
                "Selecione ao menos uma linha antes de excluir.\n\n"
                "Dica: use 'Selecionar todos do período' para marcar todas as linhas visíveis.")
            return
        if not messagebox.askyesno(
            "Confirmar exclusão",
            f"Deseja excluir {len(selected)} registro(s) de comissões?\n\nEsta ação não pode ser desfeita.",
            icon="warning",
        ):
            return
        con = self._get_db()
        if con is None:
            return
        try:
            rowids = [int(iid) for iid in selected]
            placeholders = ",".join("?" * len(rowids))
            con.execute(f"DELETE FROM historico_comissoes WHERE rowid IN ({placeholders})", rowids)
            con.commit()
            self._set_status("success", f"{len(rowids)} registro(s) de comissões excluído(s) com sucesso.")
            self._buscar_comissoes()
        except Exception as e:
            self._set_status("error", f"Erro ao excluir: {e}")
        finally:
            con.close()

    def _recarregar_colaboradores(self) -> None:
        colabs = ["Todos"] + self._get_colaboradores_db()
        # Encontra o CTkOptionMenu de colaborador e atualiza
        current = self._c_colab_var.get()
        if current not in colabs:
            self._c_colab_var.set("Todos")
        # Percorre widgets do tab de Comissões para encontrar o OptionMenu de colaborador
        for widget in self.tabs.tab("Comissões").winfo_children():
            self._update_option_menu_recursive(widget, self._c_colab_var, colabs)
        n = len(colabs) - 1
        self._set_status("idle", f"Lista de colaboradores atualizada — {n} colaborador(es).")

    def _update_option_menu_recursive(self, widget, var: ctk.StringVar, values: list[str]) -> None:
        if isinstance(widget, ctk.CTkOptionMenu) and widget.cget("variable") == var:
            widget.configure(values=values)
            return
        for child in widget.winfo_children():
            self._update_option_menu_recursive(child, var, values)

    # ─── Processos Pai ─────────────────────────────────────────────────────────

    def _buscar_processos(self) -> None:
        con = self._get_db()
        if con is None:
            return
        try:
            conditions, params = [], []
            mes_str = self._p_mes_var.get()
            ano_str = self._p_ano_var.get()

            if mes_str != "Todos":
                conditions.append("mes_referencia = ?")
                params.append(MESES_NUM[mes_str])
            if ano_str != "Todos":
                conditions.append("ano_referencia = ?")
                params.append(int(ano_str))

            where = self._build_where(conditions, params)
            rows = con.execute(f"""
                SELECT rowid, numero_pc, codigo_cliente, processo,
                       is_processo_pai, status_faturado, status_pago,
                       mes_referencia, ano_referencia, created_at
                FROM historico_processo_pai {where}
                ORDER BY ano_referencia DESC, mes_referencia DESC
            """, params).fetchall()

            self._populate_tree(self._p_tree, [r[1:] for r in rows], [r[0] for r in rows])
            n = len(rows)
            self._set_status("idle", f"Processos Pai — {n} registro(s) encontrado(s).")
            self._count_var.set(f"{n} linha(s)")
        except Exception as e:
            self._set_status("error", f"Erro ao buscar processos: {e}")
        finally:
            con.close()

    def _limpar_processos(self) -> None:
        self._p_mes_var.set("Todos")
        self._p_ano_var.set("Todos")
        self._buscar_processos()

    def _excluir_processos(self) -> None:
        selected = self._p_tree.selection()
        if not selected:
            messagebox.showwarning("Nenhuma linha selecionada",
                "Selecione ao menos uma linha antes de excluir.")
            return
        if not messagebox.askyesno(
            "Confirmar exclusão",
            f"Deseja excluir {len(selected)} registro(s) de processos pai?\n\nEsta ação não pode ser desfeita.",
            icon="warning",
        ):
            return
        con = self._get_db()
        if con is None:
            return
        try:
            rowids = [int(iid) for iid in selected]
            placeholders = ",".join("?" * len(rowids))
            con.execute(f"DELETE FROM historico_processo_pai WHERE rowid IN ({placeholders})", rowids)
            con.commit()
            self._set_status("success", f"{len(rowids)} registro(s) de processos pai excluído(s).")
            self._buscar_processos()
        except Exception as e:
            self._set_status("error", f"Erro ao excluir: {e}")
        finally:
            con.close()

    # ─── Pagamentos ───────────────────────────────────────────────────────────

    def _buscar_pagamentos(self) -> None:
        con = self._get_db()
        if con is None:
            return
        try:
            conditions, params = [], []
            mes_str = self._pg_mes_var.get()
            ano_str = self._pg_ano_var.get()

            if mes_str != "Todos":
                conditions.append("mes_referencia = ?")
                params.append(MESES_NUM[mes_str])
            if ano_str != "Todos":
                conditions.append("ano_referencia = ?")
                params.append(int(ano_str))

            where = self._build_where(conditions, params)
            rows = con.execute(f"""
                SELECT rowid, numero_pc, codigo_cliente, processo,
                       numero_nf, documento, situacao_codigo, situacao_texto,
                       dt_prorrogacao, data_baixa, valor_documento,
                       mes_referencia, ano_referencia, created_at
                FROM historico_pagamentos_processo_pai {where}
                ORDER BY ano_referencia DESC, mes_referencia DESC
            """, params).fetchall()

            self._populate_tree(self._pg_tree, [r[1:] for r in rows], [r[0] for r in rows])
            n = len(rows)
            self._set_status("idle", f"Pagamentos — {n} registro(s) encontrado(s).")
            self._count_var.set(f"{n} linha(s)")
        except Exception as e:
            self._set_status("error", f"Erro ao buscar pagamentos: {e}")
        finally:
            con.close()

    def _limpar_pagamentos(self) -> None:
        self._pg_mes_var.set("Todos")
        self._pg_ano_var.set("Todos")
        self._buscar_pagamentos()

    def _excluir_pagamentos(self) -> None:
        selected = self._pg_tree.selection()
        if not selected:
            messagebox.showwarning("Nenhuma linha selecionada",
                "Selecione ao menos uma linha antes de excluir.")
            return
        if not messagebox.askyesno(
            "Confirmar exclusão",
            f"Deseja excluir {len(selected)} registro(s) de pagamentos?\n\nEsta ação não pode ser desfeita.",
            icon="warning",
        ):
            return
        con = self._get_db()
        if con is None:
            return
        try:
            rowids = [int(iid) for iid in selected]
            placeholders = ",".join("?" * len(rowids))
            con.execute(
                f"DELETE FROM historico_pagamentos_processo_pai WHERE rowid IN ({placeholders})",
                rowids,
            )
            con.commit()
            self._set_status("success", f"{len(rowids)} registro(s) de pagamentos excluído(s).")
            self._buscar_pagamentos()
        except Exception as e:
            self._set_status("error", f"Erro ao excluir: {e}")
        finally:
            con.close()

    # ─── Helpers compartilhados ───────────────────────────────────────────────

    def _selecionar_todos(self, tree: ttk.Treeview) -> None:
        items = tree.get_children()
        tree.selection_set(items)
        n = len(items)
        self._set_status("idle", f"{n} linha(s) selecionada(s). Clique em 'Excluir selecionadas' para remover.")
        self._count_var.set(f"{n} selecionada(s)")

    def _on_selection_change(self, tree: ttk.Treeview) -> None:
        n_sel = len(tree.selection())
        n_total = len(tree.get_children())
        if n_sel:
            self._count_var.set(f"{n_sel} de {n_total} selecionada(s)")
        else:
            self._count_var.set(f"{n_total} linha(s)")

    def _set_status(self, state: str, message: str) -> None:
        if state == "idle":
            self.status_bar.configure(fg_color=COLORS["surface_alt"])
            self._status_icon.configure(text_color=COLORS["text_muted"])
            self._status_label.configure(text_color=COLORS["text_muted"])
            self._status_icon_var.set("●")
        elif state == "success":
            self.status_bar.configure(fg_color=COLORS["success_soft"])
            self._status_icon.configure(text_color=COLORS["success"])
            self._status_label.configure(text_color=COLORS["success"])
            self._status_icon_var.set("✓")
            self.root.after(4000, lambda: self._set_status("idle", "Pronto."))
        elif state == "error":
            self.status_bar.configure(fg_color=COLORS["danger_soft"])
            self._status_icon.configure(text_color=COLORS["danger"])
            self._status_label.configure(text_color=COLORS["danger"])
            self._status_icon_var.set("✗")
        self._status_var.set(message)

    def _center_window(self, w: int, h: int) -> None:
        self.root.update_idletasks()
        x = (self.root.winfo_screenwidth() - w) // 2
        y = (self.root.winfo_screenheight() - h) // 2
        self.root.geometry(f"{w}x{h}+{max(0, x)}+{max(0, y)}")


def main() -> None:
    ctk.set_appearance_mode("light")
    ctk.set_default_color_theme("blue")
    root = ctk.CTk()
    HistoricoApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
