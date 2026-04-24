"""
TraceCollector — Opt-in tracer for pipeline audit.

IMPORTANT: All hooks MUST be guarded by TraceCollector.is_enabled()
Zero impact when disabled (just an attribute access O(1)).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ItemTrace:
    """Full trace of a single item through the pipeline."""
    codigo_produto: str = ""
    processo: str = ""
    nf: str = ""

    # Stage 1: Loader
    hierarquia_cp: str = ""
    match_cp: bool = False

    # Stage 2: Attribution
    regras_candidatas: List[Dict] = field(default_factory=list)
    regra_selecionada: Optional[Dict] = None
    motivo_selecao: str = ""
    hierarquia_truncada: str = ""
    colaboradores: List[Dict] = field(default_factory=list)

    # Stage 3: Realizados (context)
    valores_realizados: Dict[str, float] = field(default_factory=dict)

    # Stage 4: FC
    componentes_fc: List[Dict] = field(default_factory=list)
    meta_niveis_usados: Dict[str, str] = field(default_factory=dict)  # component → "L2"
    fc_rampa: float = 0.0
    fc_final: float = 0.0

    # Stage 5: Commission
    formula_aplicada: str = ""
    comissao_potencial: float = 0.0
    comissao_final: float = 0.0


class TraceCollector:
    """Opt-in singleton that collects pipeline traces.

    Usage:
        TraceCollector.enable(item_filter="ABC123")  # Enable before pipeline
        # ... run pipeline ...
        traces = TraceCollector.export()  # Get traces
        TraceCollector.disable()  # Reset
    """

    _enabled: bool = False
    _filter: Optional[str] = None  # If set, only trace items matching this
    _traces: Dict[str, ItemTrace] = {}
    _raw_records: List[Dict] = []  # Raw records for stages that don't fit ItemTrace

    @classmethod
    def enable(cls, item_filter: Optional[str] = None) -> None:
        cls._enabled = True
        cls._filter = item_filter.lower().strip() if item_filter else None
        cls._traces = {}
        cls._raw_records = []

    @classmethod
    def disable(cls) -> None:
        cls._enabled = False
        cls._filter = None

    @classmethod
    def is_enabled(cls) -> bool:
        return cls._enabled

    @classmethod
    def _matches_filter(cls, key: str) -> bool:
        if cls._filter is None:
            return True
        return cls._filter in key.lower()

    @classmethod
    def record(cls, item_key: str, stage: str, data: Dict) -> None:
        """Record data for an item at a specific stage."""
        if not cls._enabled:
            return
        if not cls._matches_filter(item_key):
            return

        if item_key not in cls._traces:
            cls._traces[item_key] = ItemTrace()

        trace = cls._traces[item_key]

        if stage == "loader":
            trace.codigo_produto = data.get("codigo_produto", "")
            trace.processo = data.get("processo", "")
            trace.nf = data.get("nf", "")
            trace.hierarquia_cp = data.get("hierarquia_cp", "")
            trace.match_cp = data.get("match_cp", False)

        elif stage == "atribuicao":
            trace.regras_candidatas = data.get("regras_candidatas", [])
            trace.regra_selecionada = data.get("regra_selecionada")
            trace.motivo_selecao = data.get("motivo", "")
            trace.hierarquia_truncada = data.get("hierarquia_truncada", "")
            trace.colaboradores.append(data.get("colaborador_info", {}))

        elif stage == "fc_meta":
            comp = data.get("componente", "")
            nivel = data.get("meta_encontrada_em", "")
            if comp:
                trace.meta_niveis_usados[comp] = nivel
            trace.componentes_fc.append(data)

        elif stage == "fc_result":
            trace.fc_rampa = data.get("fc_rampa", 0.0)
            trace.fc_final = data.get("fc_final", 0.0)

        elif stage == "comissao":
            trace.formula_aplicada = data.get("formula", "")
            trace.comissao_potencial = data.get("comissao_potencial", 0.0)
            trace.comissao_final = data.get("comissao_final", 0.0)

    @classmethod
    def export(cls) -> List[Dict]:
        """Export all collected traces as dicts."""
        result = []
        for key, trace in cls._traces.items():
            result.append({
                "item_key": key,
                "codigo_produto": trace.codigo_produto,
                "processo": trace.processo,
                "nf": trace.nf,
                "hierarquia_cp": trace.hierarquia_cp,
                "match_cp": trace.match_cp,
                "regras_candidatas": trace.regras_candidatas,
                "regra_selecionada": trace.regra_selecionada,
                "motivo_selecao": trace.motivo_selecao,
                "hierarquia_truncada": trace.hierarquia_truncada,
                "colaboradores": trace.colaboradores,
                "valores_realizados": trace.valores_realizados,
                "componentes_fc": trace.componentes_fc,
                "meta_niveis_usados": trace.meta_niveis_usados,
                "fc_rampa": trace.fc_rampa,
                "fc_final": trace.fc_final,
                "formula_aplicada": trace.formula_aplicada,
                "comissao_potencial": trace.comissao_potencial,
                "comissao_final": trace.comissao_final,
            })
        return result
