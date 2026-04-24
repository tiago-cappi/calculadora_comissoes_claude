"""
launcher.pyw — Calculadora de Comissões

Interface gráfica para rodar o pipeline de comissões sem usar terminal.
Pensada para usuários leigos, com linguagem amigável e hierarquia visual clara.

Recursos:
- Abas: "Calcular Comissões" e "Arquivos de Entrada" (upload)
- Mês por nome (Janeiro..Dezembro) e Ano via dropdown
- Colaborador opcional (lista vinda de configuracoes_comissoes.xlsx)
- Botão primário "Calcular comissões" + secundários (Editar configurações / Abrir saída)
- Barra de progresso real (0%→100%) com etapa atual e timer mm:ss
- Log detalhado recolhível (colapsado por padrão) com botão "Copiar log"
- Status bar colorido (idle / processando / concluído / erro)
- Abertura automática da pasta de saída ao concluir com sucesso
- Oferece criar planilha modelo quando configuracoes_comissoes.xlsx não existe
- Upload de arquivos de entrada (Excel/CSV) com verificação de formato

Duplo clique no .pyw abre sem console preto (Python for Windows).
"""

from __future__ import annotations

import datetime as _dt
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
import tkinter as tk
import traceback
from pathlib import Path
from tkinter import filedialog, messagebox

import customtkinter as ctk

ROOT = Path(__file__).parent
EXCEL_PATH = ROOT / "configuracoes_comissoes.xlsx"
PIPELINE_PATH = ROOT / "rodar_pipeline.py"
SAIDA_PATH = ROOT / "saida"
DADOS_ENTRADA = ROOT / "dados_entrada"
TEMPLATE_SCRIPT = ROOT / "scripts" / "gerar_template_excel.py"

# ─── Design tokens ────────────────────────────────────────────────────────────
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
    "bg":             "#D9E4F0",
    "surface":        "#FFFFFF",
    "surface_alt":    "#EBF3FB",
    "border":         "#B8CCDE",
    "border_strong":  "#8AAEC8",
    "text":           "#0F172A",
    "text_muted":     "#475569",
    "text_soft":      "#64748B",
    "log_bg":         "#0F172A",
    "log_fg":         "#E2E8F0",
    "log_muted":      "#64748B",
}

FONT_FAMILY = "Segoe UI"
FONT_MONO = "Consolas"

# ─── Escala tipográfica ────────────────────────────────────────────────────────
FONTS = {
    "display":  (FONT_FAMILY, 18, "bold"),
    "title":    (FONT_FAMILY, 13, "bold"),
    "section":  (FONT_FAMILY, 11, "bold"),
    "body":     (FONT_FAMILY, 12),
    "body_sm":  (FONT_FAMILY, 11),
    "label":    (FONT_FAMILY, 11, "bold"),
    "caption":  (FONT_FAMILY, 10),
    "btn_pri":  (FONT_FAMILY, 13, "bold"),
    "btn_sec":  (FONT_FAMILY, 12),
    "mono":     (FONT_MONO, 10),
}

MESES = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
]

# ─── Arquivos de entrada esperados pelo pipeline ──────────────────────────────
ARQUIVOS_ENTRADA = [
    {
        "nome": "analise-comercial.xlsx",
        "descricao": "Análise Comercial",
        "detalhe": "Vendas do mês (origem dos cálculos de faturamento)",
        "obrigatorio": True,
        "tipo": "Excel",
        "exts": [".xlsx", ".xlsm"],
    },
    {
        "nome": "Classificação de Produtos.xlsx",
        "descricao": "Classificação de Produtos",
        "detalhe": "Mapa de códigos de produto → família/linha",
        "obrigatorio": True,
        "tipo": "Excel",
        "exts": [".xlsx", ".xlsm"],
    },
    {
        "nome": "analise-financeira.xlsx",
        "descricao": "Análise Financeira",
        "detalhe": "Recebimento (usada no pipeline de recebimento)",
        "obrigatorio": False,
        "tipo": "Excel",
        "exts": [".xlsx", ".xlsm"],
    },
    {
        "nome": "devolucoes.xlsx",
        "descricao": "Devoluções",
        "detalhe": "Devoluções do período",
        "obrigatorio": False,
        "tipo": "Excel",
        "exts": [".xlsx", ".xlsm"],
    },
    {
        "nome": "Processo x Pedido de Compra.xlsx",
        "descricao": "Processo x Pedido de Compra",
        "detalhe": "Tabela auxiliar para casamento de processos",
        "obrigatorio": False,
        "tipo": "Excel",
        "exts": [".xlsx", ".xlsm"],
    },
    {
        "nome": "fat_rent_gpe.csv",
        "descricao": "Rentabilidade por Produto (GPE)",
        "detalhe": "CSV exportado do GPE — componente de rentabilidade",
        "obrigatorio": False,
        "tipo": "CSV",
        "exts": [".csv"],
    },
]

# ─── Etapas do pipeline → porcentagem ──────────────────────────────────────────
ETAPAS_PROGRESSO = [
    (re.compile(r"\[0\]"),                        2,  "Verificando configuração"),
    (re.compile(r"\[1\]"),                        6,  "Verificando arquivos de entrada"),
    (re.compile(r"\[2\]"),                        15, "Carregando arquivos"),
    (re.compile(r"\[3b?\]"),                      25, "Processando rentabilidade"),
    (re.compile(r"\[4\]"),                        40, "Atribuindo comissões"),
    (re.compile(r"\[5\]"),                        55, "Calculando realizados"),
    (re.compile(r"\[6\]"),                        70, "Calculando Fator de Correção"),
    (re.compile(r"\[7\]"),                        80, "Calculando comissões por faturamento"),
    (re.compile(r"\[8\]"),                        90, "Exportando planilhas"),
    (re.compile(r"PIPELINE DE RECEBIMENTO"),      95, "Calculando comissões de recebimento"),
]


def listar_colaboradores() -> list[str]:
    """Lê a aba 'colaboradores' do Excel para popular o dropdown."""
    try:
        import pandas as pd
        if not EXCEL_PATH.exists():
            return []
        df = pd.read_excel(EXCEL_PATH, sheet_name="colaboradores", engine="openpyxl")
        col = "nome_colaborador"
        if col not in df.columns:
            col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
        return sorted(str(x) for x in df[col].dropna().unique())
    except Exception:
        return []


class App:
    TODOS_LABEL_PREFIX = "Todos os colaboradores"

    def __init__(self, root: ctk.CTk) -> None:
        self.root = root
        root.title("Calculadora de Comissões")
        self._center_window(820, 660)
        root.minsize(740, 560)
        root.configure(fg_color=COLORS["bg"])

        self.log_queue: queue.Queue[str] = queue.Queue()
        self.process: subprocess.Popen | None = None
        self.start_time: float | None = None
        self._timer_job: str | None = None
        self.log_expanded: bool = False
        self.arquivo_status_vars: dict[str, ctk.StringVar] = {}

        self._build_ui()
        self._set_state("idle")
        self._poll_log_queue()

    # ─── Layout ───────────────────────────────────────────────────────────────
    def _build_ui(self) -> None:
        self._build_header()
        self._build_body()
        self._build_status_bar()

    def _build_header(self) -> None:
        header = ctk.CTkFrame(
            self.root, height=56, fg_color=COLORS["surface"],
            corner_radius=0, border_width=0,
        )
        header.pack(fill="x", side="top")
        header.pack_propagate(False)

        inner = ctk.CTkFrame(header, fg_color="transparent")
        inner.pack(fill="both", expand=True, padx=24)

        ctk.CTkLabel(
            inner, text="Calculadora de Comissões",
            font=FONTS["display"], text_color=COLORS["text"], anchor="w",
        ).pack(side="left", pady=14)

        ctk.CTkLabel(
            inner, text="  ·  Gere os relatórios mensais em poucos cliques",
            font=FONTS["body_sm"], text_color=COLORS["text_soft"], anchor="w",
        ).pack(side="left", pady=14)

        ctk.CTkFrame(
            self.root, height=1, fg_color=COLORS["border"], corner_radius=0,
        ).pack(fill="x", side="top")

    def _build_body(self) -> None:
        self.tabview = ctk.CTkTabview(
            self.root,
            fg_color=COLORS["bg"],
            segmented_button_selected_color=COLORS["primary"],
            segmented_button_selected_hover_color=COLORS["primary_hover"],
            segmented_button_unselected_color=COLORS["surface"],
            segmented_button_unselected_hover_color=COLORS["surface_alt"],
            segmented_button_fg_color=COLORS["surface"],
            text_color=COLORS["text"],
            border_width=0,
            corner_radius=10,
            anchor="w",
        )
        self.tabview.pack(fill="both", expand=True, padx=24, pady=(12, 8))

        tab_calcular = self.tabview.add("Calcular comissões")
        tab_upload = self.tabview.add("Arquivos de entrada")

        self._build_tab_calcular(tab_calcular)
        self._build_tab_upload(tab_upload)

    # ─── Aba: Calcular ────────────────────────────────────────────────────────
    def _build_tab_calcular(self, parent: ctk.CTkFrame) -> None:
        body = ctk.CTkFrame(parent, fg_color="transparent")
        body.pack(fill="both", expand=True, padx=0, pady=(8, 4))

        self._build_config_card(body)
        self._build_actions(body)
        self._build_progress(body)
        self._build_log(body)

    def _build_config_card(self, parent: ctk.CTkFrame) -> None:
        """Card único com período (mês + ano) e colaborador na mesma linha."""
        card = self._card(parent)
        card.pack(fill="x", pady=(0, 8))

        grid = ctk.CTkFrame(card, fg_color="transparent")
        grid.pack(fill="x", padx=20, pady=16)

        hoje = _dt.date.today()

        # Mês
        mes_wrap = ctk.CTkFrame(grid, fg_color="transparent")
        mes_wrap.pack(side="left", padx=(0, 8))
        ctk.CTkLabel(
            mes_wrap, text="Mês", font=FONTS["label"],
            text_color=COLORS["text_muted"], anchor="w",
        ).pack(anchor="w")
        self.mes_var = ctk.StringVar(value=MESES[hoje.month - 1])
        self.mes_menu = ctk.CTkOptionMenu(
            mes_wrap,
            values=MESES,
            variable=self.mes_var,
            width=150, height=34,
            font=FONTS["body"],
            dropdown_font=FONTS["body"],
            fg_color=COLORS["surface"],
            button_color=COLORS["primary"],
            button_hover_color=COLORS["primary_hover"],
            text_color=COLORS["text"],
            dropdown_fg_color=COLORS["surface"],
            dropdown_text_color=COLORS["text"],
            dropdown_hover_color=COLORS["primary_soft"],
            corner_radius=6,
        )
        self.mes_menu.pack(pady=(4, 0))

        # Ano
        ano_wrap = ctk.CTkFrame(grid, fg_color="transparent")
        ano_wrap.pack(side="left", padx=(0, 16))
        ctk.CTkLabel(
            ano_wrap, text="Ano", font=FONTS["label"],
            text_color=COLORS["text_muted"], anchor="w",
        ).pack(anchor="w")
        anos = [str(y) for y in range(hoje.year - 5, hoje.year + 3)]
        self.ano_var = ctk.StringVar(value=str(hoje.year))
        self.ano_menu = ctk.CTkOptionMenu(
            ano_wrap,
            values=anos,
            variable=self.ano_var,
            width=95, height=34,
            font=FONTS["body"],
            dropdown_font=FONTS["body"],
            fg_color=COLORS["surface"],
            button_color=COLORS["primary"],
            button_hover_color=COLORS["primary_hover"],
            text_color=COLORS["text"],
            dropdown_fg_color=COLORS["surface"],
            dropdown_text_color=COLORS["text"],
            dropdown_hover_color=COLORS["primary_soft"],
            corner_radius=6,
        )
        self.ano_menu.pack(pady=(4, 0))

        # Divisor vertical
        ctk.CTkFrame(
            grid, width=2, fg_color=COLORS["border"], corner_radius=0,
        ).pack(side="left", fill="y", padx=(0, 16), pady=4)

        # Colaborador
        colab_wrap = ctk.CTkFrame(grid, fg_color="transparent")
        colab_wrap.pack(side="left", fill="x", expand=True)
        ctk.CTkLabel(
            colab_wrap, text="Colaborador", font=FONTS["label"],
            text_color=COLORS["text_muted"], anchor="w",
        ).pack(anchor="w")
        colabs = self._options_colabs()
        self.colab_var = ctk.StringVar(value=colabs[0])
        self.colab_menu = ctk.CTkOptionMenu(
            colab_wrap,
            values=colabs,
            variable=self.colab_var,
            width=300, height=34,
            font=FONTS["body"],
            dropdown_font=FONTS["body"],
            fg_color=COLORS["surface"],
            button_color=COLORS["primary"],
            button_hover_color=COLORS["primary_hover"],
            text_color=COLORS["text"],
            dropdown_fg_color=COLORS["surface"],
            dropdown_text_color=COLORS["text"],
            dropdown_hover_color=COLORS["primary_soft"],
            corner_radius=6,
        )
        self.colab_menu.pack(anchor="w", pady=(4, 0))

        # Botão atualizar lista
        refresh_wrap = ctk.CTkFrame(grid, fg_color="transparent")
        refresh_wrap.pack(side="right", padx=(8, 0))
        ctk.CTkLabel(refresh_wrap, text=" ", font=FONTS["label"]).pack()
        self.btn_recarregar = ctk.CTkButton(
            refresh_wrap,
            text="↻",
            command=self.on_recarregar,
            width=34, height=34,
            fg_color="transparent",
            hover_color=COLORS["primary_soft"],
            text_color=COLORS["primary"],
            font=(FONT_FAMILY, 16),
            border_width=1,
            border_color=COLORS["border"],
            corner_radius=6,
        )
        self.btn_recarregar.pack(pady=(4, 0))

    def _build_actions(self, parent: ctk.CTkFrame) -> None:
        ctk.CTkFrame(
            parent, height=1, fg_color=COLORS["border"], corner_radius=0,
        ).pack(fill="x", pady=(0, 8))

        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill="x", pady=(0, 8))

        self.btn_calc = ctk.CTkButton(
            row,
            text="Calcular comissões",
            command=self.on_calcular,
            width=210, height=44,
            fg_color=COLORS["primary"],
            hover_color=COLORS["primary_hover"],
            text_color="#FFFFFF",
            font=FONTS["btn_pri"],
            corner_radius=8,
        )
        self.btn_calc.pack(side="left")

        self.btn_editar = ctk.CTkButton(
            row,
            text="Editar configurações",
            command=self.on_editar,
            width=170, height=44,
            fg_color=COLORS["surface"],
            hover_color=COLORS["surface_alt"],
            text_color=COLORS["text"],
            border_width=1,
            border_color=COLORS["border_strong"],
            font=FONTS["btn_sec"],
            corner_radius=8,
        )
        self.btn_editar.pack(side="left", padx=(8, 0))

        self.btn_saida = ctk.CTkButton(
            row,
            text="Abrir pasta de saída",
            command=self.on_abrir_saida,
            width=170, height=44,
            fg_color=COLORS["surface"],
            hover_color=COLORS["surface_alt"],
            text_color=COLORS["text"],
            border_width=1,
            border_color=COLORS["border_strong"],
            font=FONTS["btn_sec"],
            corner_radius=8,
        )
        self.btn_saida.pack(side="left", padx=(8, 0))

    def _build_progress(self, parent: ctk.CTkFrame) -> None:
        progress_card = self._card(parent)
        progress_card.pack(fill="x", pady=(0, 8))
        self.progress_frame = ctk.CTkFrame(progress_card, fg_color="transparent")
        self.progress_frame.pack(fill="x", padx=16, pady=12)

        head = ctk.CTkFrame(self.progress_frame, fg_color="transparent")
        head.pack(fill="x")

        self.progress_stage_var = ctk.StringVar(value="")
        ctk.CTkLabel(
            head,
            textvariable=self.progress_stage_var,
            font=FONTS["body_sm"],
            text_color=COLORS["text_muted"],
            anchor="w",
        ).pack(side="left")

        self.progress_pct_var = ctk.StringVar(value="")
        ctk.CTkLabel(
            head,
            textvariable=self.progress_pct_var,
            font=FONTS["section"],
            text_color=COLORS["primary"],
            anchor="e",
        ).pack(side="right")

        self.progress = ctk.CTkProgressBar(
            self.progress_frame,
            height=18,
            corner_radius=9,
            fg_color=COLORS["border"],
            progress_color=COLORS["primary"],
            mode="determinate",
        )
        self.progress.pack(fill="x", side="top", pady=(4, 0))
        self.progress.set(0)

        self.timer_var = ctk.StringVar(value="")
        self.timer_label = ctk.CTkLabel(
            self.progress_frame,
            textvariable=self.timer_var,
            font=FONTS["caption"],
            text_color=COLORS["text_soft"],
            anchor="e",
        )
        self.timer_label.pack(fill="x", pady=(4, 0))

    def _build_log(self, parent: ctk.CTkFrame) -> None:
        self.log_wrap = ctk.CTkFrame(parent, fg_color="transparent")
        self.log_wrap.pack(fill="both", expand=True)

        header = ctk.CTkFrame(self.log_wrap, fg_color="transparent", height=32)
        header.pack(fill="x")

        self.btn_toggle_log = ctk.CTkButton(
            header,
            text="▸  Detalhes da execução",
            command=self._toggle_log,
            anchor="w",
            height=26,
            width=190,
            fg_color="transparent",
            hover_color=COLORS["surface_alt"],
            text_color=COLORS["text_soft"],
            border_width=1,
            border_color=COLORS["border"],
            font=FONTS["body_sm"],
            corner_radius=20,
        )
        self.btn_toggle_log.pack(side="left")

        self.btn_copy_log = ctk.CTkButton(
            header,
            text="Copiar log",
            command=self.on_copiar_log,
            width=100, height=26,
            fg_color="transparent",
            hover_color=COLORS["primary_soft"],
            text_color=COLORS["primary"],
            border_width=1,
            border_color=COLORS["border"],
            font=FONTS["body_sm"],
            corner_radius=20,
        )
        self.btn_copy_log.pack(side="right")

        self.log_box = ctk.CTkFrame(
            self.log_wrap,
            fg_color=COLORS["log_bg"],
            border_color=COLORS["border"],
            border_width=1,
            corner_radius=8,
        )
        # NÃO empacotado por padrão — começa recolhido

        self.log_text = ctk.CTkTextbox(
            self.log_box,
            font=FONTS["mono"],
            fg_color=COLORS["log_bg"],
            text_color=COLORS["log_fg"],
            border_width=0,
            corner_radius=8,
            wrap="word",
            height=180,
        )
        self.log_text.pack(fill="both", expand=True, padx=12, pady=10)
        self.log_text.configure(state="disabled")
        self._write_log_placeholder()

    def _toggle_log(self) -> None:
        if self.log_expanded:
            self.log_box.pack_forget()
            self.btn_toggle_log.configure(text="▸  Detalhes da execução")
            self.log_expanded = False
        else:
            self.log_box.pack(fill="both", expand=True, pady=(6, 0))
            self.btn_toggle_log.configure(text="▾  Ocultar detalhes")
            self.log_expanded = True

    # ─── Aba: Upload de arquivos ─────────────────────────────────────────────
    def _build_tab_upload(self, parent: ctk.CTkFrame) -> None:
        wrap = ctk.CTkScrollableFrame(
            parent,
            fg_color=COLORS["bg"],
            scrollbar_button_color=COLORS["border_strong"],
            scrollbar_button_hover_color=COLORS["text_soft"],
        )
        wrap.pack(fill="both", expand=True, padx=0, pady=4)

        # Card informativo
        intro_card = ctk.CTkFrame(
            wrap,
            fg_color=COLORS["primary_soft"],
            border_color=COLORS["border"],
            border_width=0,
            corner_radius=8,
        )
        intro_card.pack(fill="x", pady=(0, 16))
        ctk.CTkLabel(
            intro_card,
            text=(
                "Envie os arquivos exigidos pelo pipeline. Os arquivos serão copiados "
                "para a pasta 'dados_entrada/' com o nome correto.\n"
                "Formatos aceitos: Excel (.xlsx / .xlsm) e CSV apenas para o arquivo de rentabilidade."
            ),
            font=FONTS["body_sm"],
            text_color=COLORS["text"],
            anchor="w",
            justify="left",
            wraplength=680,
        ).pack(fill="x", padx=14, pady=10)

        # Seções agrupadas por obrigatoriedade
        obrigatorios = [a for a in ARQUIVOS_ENTRADA if a["obrigatorio"]]
        opcionais = [a for a in ARQUIVOS_ENTRADA if not a["obrigatorio"]]

        if obrigatorios:
            self._section_header(wrap, "Obrigatórios")
            for arq in obrigatorios:
                self._build_upload_row(wrap, arq)

        if opcionais:
            self._section_header(wrap, "Opcionais", top_pad=16)
            for arq in opcionais:
                self._build_upload_row(wrap, arq)

        # Ações
        actions = ctk.CTkFrame(wrap, fg_color="transparent")
        actions.pack(fill="x", pady=(16, 4))

        ctk.CTkButton(
            actions,
            text="Abrir pasta 'dados_entrada'",
            command=self.on_abrir_entrada,
            height=36,
            fg_color=COLORS["surface"],
            hover_color=COLORS["surface_alt"],
            text_color=COLORS["text"],
            border_width=1,
            border_color=COLORS["border_strong"],
            font=FONTS["btn_sec"],
            corner_radius=8,
        ).pack(side="left")

        ctk.CTkButton(
            actions,
            text="↻  Atualizar status",
            command=self._refresh_upload_status,
            height=36,
            fg_color="transparent",
            hover_color=COLORS["primary_soft"],
            text_color=COLORS["primary"],
            border_width=0,
            font=FONTS["btn_sec"],
            corner_radius=8,
        ).pack(side="left", padx=(8, 0))

    def _section_header(self, parent: ctk.CTkFrame, text: str, top_pad: int = 0) -> None:
        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill="x", pady=(top_pad, 8))
        pill = ctk.CTkFrame(row, fg_color=COLORS["primary_soft"], corner_radius=4)
        pill.pack(side="left")
        ctk.CTkLabel(
            pill, text=text,
            font=FONTS["section"],
            text_color=COLORS["primary"],
            anchor="w",
        ).pack(padx=10, pady=4)
        ctk.CTkFrame(
            row, height=1, fg_color=COLORS["border"], corner_radius=0,
        ).pack(side="left", fill="x", expand=True, padx=(10, 0), pady=1)

    def _build_upload_row(self, parent: ctk.CTkFrame, arq: dict) -> None:
        stripe_color = COLORS["primary"] if arq["obrigatorio"] else COLORS["border_strong"]
        outer = ctk.CTkFrame(parent, fg_color=stripe_color, corner_radius=8)
        outer.pack(fill="x", pady=(0, 8))
        card = ctk.CTkFrame(outer, fg_color=COLORS["surface"], corner_radius=6)
        card.pack(fill="both", expand=True, padx=(4, 1), pady=1)

        row = ctk.CTkFrame(card, fg_color="transparent")
        row.pack(fill="x", padx=14, pady=12)

        # Ícone de status à esquerda
        icon_var = ctk.StringVar(value="·")
        self.arquivo_status_vars[arq["nome"] + "__icon"] = icon_var
        ctk.CTkLabel(
            row,
            textvariable=icon_var,
            font=(FONT_FAMILY, 18, "bold"),
            text_color=COLORS["text_soft"],
            width=24,
            anchor="center",
        ).pack(side="left", padx=(0, 12))

        # Conteúdo central
        left = ctk.CTkFrame(row, fg_color="transparent")
        left.pack(side="left", fill="x", expand=True)

        label_line = ctk.CTkFrame(left, fg_color="transparent")
        label_line.pack(fill="x", anchor="w")

        ctk.CTkLabel(
            label_line,
            text=arq["descricao"],
            font=FONTS["section"],
            text_color=COLORS["text"],
            anchor="w",
        ).pack(side="left")

        # Badge obrigatorio/opcional
        if arq["obrigatorio"]:
            badge_text = "obrigatório"
            badge_color = COLORS["danger_soft"]
            badge_fg = COLORS["danger"]
        else:
            badge_text = "opcional"
            badge_color = COLORS["surface_alt"]
            badge_fg = COLORS["text_soft"]

        badge = ctk.CTkFrame(
            label_line, fg_color=badge_color, corner_radius=10,
        )
        badge.pack(side="left", padx=(8, 0), pady=1)
        ctk.CTkLabel(
            badge, text=badge_text,
            font=FONTS["caption"],
            text_color=badge_fg,
        ).pack(padx=6, pady=1)

        ctk.CTkLabel(
            left,
            text=arq["detalhe"],
            font=FONTS["caption"],
            text_color=COLORS["text_soft"],
            anchor="w",
        ).pack(fill="x", anchor="w", pady=(2, 0))

        ctk.CTkLabel(
            left,
            text=f"{arq['nome']}  ·  {arq['tipo']}",
            font=FONTS["mono"],
            text_color=COLORS["text_soft"],
            anchor="w",
        ).pack(fill="x", anchor="w", pady=(2, 0))

        status_var = ctk.StringVar()
        self.arquivo_status_vars[arq["nome"]] = status_var
        ctk.CTkLabel(
            left,
            textvariable=status_var,
            font=FONTS["caption"],
            text_color=COLORS["text_soft"],
            anchor="w",
        ).pack(fill="x", anchor="w", pady=(4, 0))

        # Botão à direita
        btn = ctk.CTkButton(
            row,
            text="Selecionar",
            command=lambda a=arq: self.on_upload_arquivo(a),
            width=110, height=34,
            fg_color=COLORS["primary"],
            hover_color=COLORS["primary_hover"],
            text_color="#FFFFFF",
            font=FONTS["btn_sec"],
            corner_radius=6,
        )
        btn.pack(side="right", padx=(12, 0))

        self._atualizar_status_arquivo(arq)

    def _atualizar_status_arquivo(self, arq: dict) -> None:
        status_var = self.arquivo_status_vars.get(arq["nome"])
        icon_var = self.arquivo_status_vars.get(arq["nome"] + "__icon")
        if status_var is None:
            return
        destino = DADOS_ENTRADA / arq["nome"]
        if destino.exists():
            try:
                size_kb = destino.stat().st_size / 1024
                mtime = _dt.datetime.fromtimestamp(destino.stat().st_mtime)
                status_var.set(
                    f"Carregado  ·  {size_kb:,.1f} KB  ·  {mtime.strftime('%d/%m/%Y %H:%M')}"
                )
            except OSError:
                status_var.set("Carregado")
            if icon_var:
                icon_var.set("✓")
                # update icon label color to success
        else:
            status_var.set("Não carregado")
            if icon_var:
                icon_var.set("·")

    def _refresh_upload_status(self) -> None:
        for arq in ARQUIVOS_ENTRADA:
            self._atualizar_status_arquivo(arq)
        self._set_state("idle", "Status dos arquivos de entrada atualizado.")

    def on_upload_arquivo(self, arq: dict) -> None:
        exts = arq["exts"]
        tipo = arq["tipo"]
        filetypes = [
            (f"Arquivos {tipo}", " ".join(f"*{e}" for e in exts)),
            ("Todos os arquivos", "*.*"),
        ]
        path = filedialog.askopenfilename(
            title=f"Selecione o arquivo: {arq['descricao']}",
            filetypes=filetypes,
        )
        if not path:
            return

        src = Path(path)

        # Verificação 1: extensão
        if src.suffix.lower() not in exts:
            messagebox.showerror(
                "Formato inválido",
                f"O arquivo selecionado não está no formato {tipo}.\n\n"
                f"Esperado: {', '.join(exts)}\n"
                f"Selecionado: {src.suffix or '(sem extensão)'}",
            )
            return

        # Verificação 2: conteúdo (abre o arquivo como Excel/CSV)
        if tipo == "Excel":
            if not self._verificar_excel(src):
                return
        elif tipo == "CSV":
            if not self._verificar_csv(src):
                return

        try:
            DADOS_ENTRADA.mkdir(exist_ok=True)
            destino = DADOS_ENTRADA / arq["nome"]
            shutil.copy2(src, destino)
        except Exception as e:
            messagebox.showerror(
                "Erro ao copiar arquivo",
                f"Não foi possível copiar o arquivo para 'dados_entrada/':\n\n{e}",
            )
            return

        self._atualizar_status_arquivo(arq)
        self._set_state("idle", f"Arquivo '{arq['nome']}' enviado com sucesso.")

    def _verificar_excel(self, src: Path) -> bool:
        try:
            import openpyxl
            wb = openpyxl.load_workbook(str(src), read_only=True, data_only=True)
            wb.close()
            return True
        except Exception as e:
            messagebox.showerror(
                "Arquivo Excel inválido",
                f"O arquivo '{src.name}' não pôde ser aberto como Excel.\n\n"
                f"Detalhe: {e}\n\n"
                "Verifique se o arquivo não está corrompido e se é realmente um .xlsx/.xlsm.",
            )
            return False

    def _verificar_csv(self, src: Path) -> bool:
        try:
            with open(src, "rb") as f:
                head = f.read(4096)
            if b"\x00" in head:
                raise ValueError("arquivo parece binário, não CSV")
            try:
                head.decode("utf-8")
            except UnicodeDecodeError:
                head.decode("latin-1")
            return True
        except Exception as e:
            messagebox.showerror(
                "Arquivo CSV inválido",
                f"O arquivo '{src.name}' não pôde ser lido como CSV.\n\n"
                f"Detalhe: {e}",
            )
            return False

    def on_abrir_entrada(self) -> None:
        DADOS_ENTRADA.mkdir(exist_ok=True)
        try:
            self._open_path(DADOS_ENTRADA)
        except Exception as e:
            messagebox.showerror("Não foi possível abrir", f"Erro ao abrir a pasta:\n{e}")

    # ─── Status bar ───────────────────────────────────────────────────────────
    def _build_status_bar(self) -> None:
        ctk.CTkFrame(
            self.root, height=1, fg_color=COLORS["border"], corner_radius=0,
        ).pack(fill="x", side="bottom")

        self.status_bar = ctk.CTkFrame(
            self.root, height=36, corner_radius=0,
            fg_color=COLORS["surface_alt"], border_width=0,
        )
        self.status_bar.pack(fill="x", side="bottom")
        self.status_bar.pack_propagate(False)

        self.status_icon_var = ctk.StringVar(value="●")
        self.status_icon = ctk.CTkLabel(
            self.status_bar,
            textvariable=self.status_icon_var,
            font=FONTS["section"],
            text_color=COLORS["text_soft"],
        )
        self.status_icon.pack(side="left", padx=(16, 6))

        self.status_var = ctk.StringVar(value="Pronto.")
        self.status_label = ctk.CTkLabel(
            self.status_bar,
            textvariable=self.status_var,
            font=FONTS["body_sm"],
            text_color=COLORS["text_soft"],
            anchor="w",
        )
        self.status_label.pack(side="left", fill="x", expand=True)

    # ─── Helpers de construção ────────────────────────────────────────────────
    def _card(self, parent: ctk.CTkFrame) -> ctk.CTkFrame:
        return ctk.CTkFrame(
            parent,
            fg_color=COLORS["surface"],
            border_color=COLORS["border"],
            border_width=1,
            corner_radius=10,
        )

    def _options_colabs(self) -> list[str]:
        colabs = listar_colaboradores()
        return [f"{self.TODOS_LABEL_PREFIX} ({len(colabs)})"] + colabs

    def _center_window(self, w: int, h: int) -> None:
        self.root.update_idletasks()
        x = (self.root.winfo_screenwidth() - w) // 2
        y = (self.root.winfo_screenheight() - h) // 2
        self.root.geometry(f"{w}x{h}+{max(0, x)}+{max(0, y)}")

    # ─── Estado visual ────────────────────────────────────────────────────────
    def _set_state(self, state: str, message: str | None = None) -> None:
        """Estados: idle, loading, success, error."""
        if state == "idle":
            self.status_bar.configure(fg_color=COLORS["surface_alt"])
            self.status_icon.configure(text_color=COLORS["text_soft"])
            self.status_label.configure(text_color=COLORS["text_soft"])
            self.status_icon_var.set("●")
            self.status_var.set(message or "Pronto. Selecione o mês e clique em Calcular comissões.")
            self.timer_var.set("")
        elif state == "loading":
            self.status_bar.configure(fg_color=COLORS["warning_soft"])
            self.status_icon.configure(text_color=COLORS["warning"])
            self.status_label.configure(text_color=COLORS["warning"])
            self.status_icon_var.set("⟳")
            self.status_var.set(message or "Calculando...")
        elif state == "success":
            self.status_bar.configure(fg_color=COLORS["success_soft"])
            self.status_icon.configure(text_color=COLORS["success"])
            self.status_label.configure(text_color=COLORS["success"])
            self.status_icon_var.set("✓")
            self.status_var.set(message or "Tudo certo! Pasta de saída aberta automaticamente.")
        elif state == "error":
            self.status_bar.configure(fg_color=COLORS["danger_soft"])
            self.status_icon.configure(text_color=COLORS["danger"])
            self.status_label.configure(text_color=COLORS["danger"])
            self.status_icon_var.set("✗")
            self.status_var.set(message or "Algo deu errado. Veja os detalhes abaixo.")

    # ─── Ações ────────────────────────────────────────────────────────────────
    def on_calcular(self) -> None:
        if self.process is not None:
            messagebox.showwarning(
                "Cálculo em andamento",
                "Um cálculo já está rodando. Aguarde ele terminar antes de iniciar outro.",
            )
            return

        if not PIPELINE_PATH.exists():
            messagebox.showerror(
                "Instalação incompleta",
                "Não encontrei o arquivo do cálculo (rodar_pipeline.py). "
                "A instalação pode estar incompleta.",
            )
            return

        if not EXCEL_PATH.exists():
            resposta = messagebox.askyesno(
                "Planilha de configurações não encontrada",
                "A planilha 'configuracoes_comissoes.xlsx' não foi encontrada na pasta.\n\n"
                "Deseja criá-la automaticamente agora?",
            )
            if resposta:
                if self._criar_planilha_modelo():
                    messagebox.showinfo(
                        "Planilha criada",
                        "Planilha modelo criada com sucesso!\n\n"
                        "Clique em 'Editar configurações' para preencher os dados antes "
                        "de calcular.",
                    )
                    self.on_recarregar()
                return
            else:
                return

        mes = MESES.index(self.mes_var.get()) + 1
        try:
            ano = int(self.ano_var.get())
        except ValueError:
            messagebox.showerror("Ano inválido", "Selecione um ano válido na lista.")
            return

        cmd = [sys.executable, str(PIPELINE_PATH), "--mes", str(mes), "--ano", str(ano)]
        colab = self.colab_var.get().strip()
        if colab and not colab.startswith(self.TODOS_LABEL_PREFIX):
            cmd.extend(["--colaborador", colab])

        self._clear_log()
        periodo = f"{self.mes_var.get()}/{ano}"
        self._log_with_timestamp(f"Iniciando cálculo para {periodo}...\n")
        self._log_with_timestamp(f"Comando: {' '.join(cmd)}\n\n")

        self._set_state(
            "loading",
            f"Calculando comissões de {periodo} — isso pode levar alguns minutos.",
        )
        self.btn_calc.configure(state="disabled", text="Calculando...")

        self.progress.configure(mode="determinate", progress_color=COLORS["primary"])
        self.progress.set(0)
        self.progress_pct_var.set("0%")
        self.progress_stage_var.set("Inicializando...")

        self.start_time = time.time()
        self._tick_timer()

        threading.Thread(target=self._run_pipeline, args=(cmd, mes, ano), daemon=True).start()

    def _run_pipeline(self, cmd: list[str], mes: int, ano: int) -> None:
        try:
            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            self.process = subprocess.Popen(
                cmd,
                cwd=str(ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=env,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            assert self.process.stdout is not None
            for line in self.process.stdout:
                self.log_queue.put(line)
            self.process.wait()
            code = self.process.returncode
            saida_mes = SAIDA_PATH / f"{mes:02d}_{ano}"
            self.log_queue.put("\n" + "─" * 56 + "\n")
            if code == 0:
                self.log_queue.put("Cálculo concluído com sucesso!\n")
                if saida_mes.exists():
                    self.log_queue.put(f"Arquivos gerados em: saida/{mes:02d}_{ano}/\n")
                self.log_queue.put(f"__DONE__:{code}:{mes}:{ano}\n")
            else:
                self.log_queue.put(
                    f"O cálculo terminou com erro (código {code}). "
                    "Veja os detalhes acima.\n"
                )
                self.log_queue.put(f"__DONE__:{code}:{mes}:{ano}\n")
        except Exception as e:
            self.log_queue.put(f"\n[ERRO] {e}\n__DONE__:-1:{mes}:{ano}\n")

    def _poll_log_queue(self) -> None:
        try:
            while True:
                line = self.log_queue.get_nowait()
                if line.startswith("__DONE__:"):
                    parts = line.split(":")
                    code = int(parts[1].strip())
                    mes = int(parts[2]) if len(parts) > 2 else 0
                    ano = int(parts[3]) if len(parts) > 3 else 0
                    self._on_finished(code, mes, ano)
                else:
                    self._check_progress(line)
                    self._append_log(line)
        except queue.Empty:
            pass
        self.root.after(100, self._poll_log_queue)

    def _check_progress(self, line: str) -> None:
        if self.process is None and self.start_time is None:
            return
        for pattern, pct, stage_name in ETAPAS_PROGRESSO:
            if pattern.search(line):
                self._update_progress(pct, stage_name)
                return

    def _update_progress(self, pct: int, stage_name: str) -> None:
        self.progress.set(pct / 100)
        self.progress_pct_var.set(f"{pct}%")
        self.progress_stage_var.set(stage_name)
        self._set_state("loading", f"{stage_name}... ({pct}%)")

    def _on_finished(self, code: int, mes: int, ano: int) -> None:
        self.process = None
        self.progress.configure(mode="determinate")
        self.progress.set(1.0 if code == 0 else max(self.progress.get(), 0.0))
        self.progress.configure(
            progress_color=COLORS["success"] if code == 0 else COLORS["danger"]
        )
        self.progress_pct_var.set("100%" if code == 0 else self.progress_pct_var.get())
        self.progress_stage_var.set(
            "Concluído" if code == 0 else "Interrompido com erro"
        )
        self.btn_calc.configure(state="normal", text="Calcular comissões")

        elapsed = self._elapsed_str() if self.start_time else ""
        self.start_time = None
        if self._timer_job:
            self.root.after_cancel(self._timer_job)
            self._timer_job = None

        if code == 0:
            saida_mes = SAIDA_PATH / f"{mes:02d}_{ano}"
            opened = False
            try:
                target = saida_mes if saida_mes.exists() else SAIDA_PATH
                target.mkdir(exist_ok=True)
                self._open_path(target)
                opened = True
            except Exception:
                opened = False
            msg = "Tudo certo! "
            msg += "Pasta de saída aberta automaticamente." if opened else "Abra a pasta de saída para ver os arquivos."
            if elapsed:
                msg += f"  ({elapsed})"
            self._set_state("success", msg)
            self.timer_var.set(f"Concluído em {elapsed}" if elapsed else "")
            self.root.after(4000, lambda: self.progress.configure(progress_color=COLORS["primary"]))
        else:
            msg = f"Erro no cálculo (código {code}). Clique em 'Copiar log' e envie para suporte."
            if elapsed:
                msg += f"  ({elapsed})"
            self._set_state("error", msg)
            self.timer_var.set(f"Interrompido após {elapsed}" if elapsed else "")
            if not self.log_expanded:
                self._toggle_log()

    def on_editar(self) -> None:
        if not EXCEL_PATH.exists():
            resposta = messagebox.askyesno(
                "Planilha não encontrada",
                "A planilha 'configuracoes_comissoes.xlsx' não existe.\n\n"
                "Deseja criá-la automaticamente agora?",
            )
            if resposta and self._criar_planilha_modelo():
                self.on_recarregar()
            else:
                return
        try:
            self._open_path(EXCEL_PATH)
        except Exception as e:
            messagebox.showerror("Não foi possível abrir", f"Erro ao abrir a planilha:\n{e}")

    def on_abrir_saida(self) -> None:
        SAIDA_PATH.mkdir(exist_ok=True)
        try:
            self._open_path(SAIDA_PATH)
        except Exception as e:
            messagebox.showerror("Não foi possível abrir", f"Erro ao abrir a pasta:\n{e}")

    def on_recarregar(self) -> None:
        colabs = self._options_colabs()
        self.colab_menu.configure(values=colabs)
        if self.colab_var.get() not in colabs:
            self.colab_var.set(colabs[0])
        else:
            if self.colab_var.get().startswith(self.TODOS_LABEL_PREFIX):
                self.colab_var.set(colabs[0])
        qtd = len(colabs) - 1
        self._set_state("idle", f"Lista atualizada — {qtd} colaborador(es) carregado(s).")

    def on_copiar_log(self) -> None:
        try:
            self.log_text.configure(state="normal")
            conteudo = self.log_text.get("1.0", "end")
            self.log_text.configure(state="disabled")
            self.root.clipboard_clear()
            self.root.clipboard_append(conteudo)
            self.root.update()
            self._set_state("idle", "Log copiado para a área de transferência.")
        except tk.TclError as e:
            messagebox.showerror("Erro", f"Não foi possível copiar o log:\n{e}")

    # ─── Infra ────────────────────────────────────────────────────────────────
    def _criar_planilha_modelo(self) -> bool:
        if not TEMPLATE_SCRIPT.exists():
            messagebox.showerror(
                "Script não encontrado",
                f"Não encontrei o gerador de template em:\n{TEMPLATE_SCRIPT}",
            )
            return False
        try:
            result = subprocess.run(
                [sys.executable, str(TEMPLATE_SCRIPT)],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            if result.returncode != 0:
                messagebox.showerror(
                    "Erro ao criar planilha",
                    f"O gerador retornou código {result.returncode}.\n\n"
                    f"{(result.stderr or result.stdout or '')[:500]}",
                )
                return False
            return True
        except Exception as e:
            messagebox.showerror("Erro ao criar planilha", str(e))
            return False

    def _open_path(self, path: Path) -> None:
        if hasattr(os, "startfile"):
            os.startfile(str(path))  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])

    # ─── Timer ────────────────────────────────────────────────────────────────
    def _tick_timer(self) -> None:
        if self.start_time is None:
            return
        self.timer_var.set(f"{self._elapsed_str()} decorrido")
        self._timer_job = self.root.after(1000, self._tick_timer)

    def _elapsed_str(self) -> str:
        if self.start_time is None:
            return ""
        total = int(time.time() - self.start_time)
        return f"{total // 60:02d}:{total % 60:02d}"

    # ─── Log ──────────────────────────────────────────────────────────────────
    def _write_log_placeholder(self) -> None:
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.insert(
            "end",
            "Aguardando execução. Clique em 'Calcular comissões' para começar.\n",
        )
        self.log_text.configure(state="disabled")

    def _log_with_timestamp(self, text: str) -> None:
        ts = _dt.datetime.now().strftime("%H:%M:%S")
        if text.strip():
            self._append_log(f"[{ts}] {text}")
        else:
            self._append_log(text)

    def _append_log(self, text: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert("end", text)
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _clear_log(self) -> None:
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")


def main() -> None:
    ctk.set_appearance_mode("light")
    ctk.set_default_color_theme("blue")
    root = ctk.CTk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    try:
        main()
    except BaseException:
        _log = Path(__file__).with_name("_startup_error.log")
        _log.write_text(traceback.format_exc(), encoding="utf-8")
        raise
