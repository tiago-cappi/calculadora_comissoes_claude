"""
structured_errors.py — Framework de Erros Estruturados para o Pipeline

Coleta erros de cada etapa do pipeline sem interromper a execução.
Cada erro contém contexto suficiente para que Claude possa agir
sem precisar ler documentação de regras de negócio.
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, List, Optional


@dataclass
class StructuredError:
    """Erro estruturado com contexto para auto-healing."""

    stage: str                              # Ex: "loader", "fc_calculator"
    tipo: str                               # Ex: "missing_meta", "column_not_found"
    mensagem: str                           # Human-readable
    contexto: Dict[str, Any] = field(default_factory=dict)
    impacto: str = ""                       # O que foi afetado
    recovery: str = ""                      # O que o pipeline fez para continuar
    fix_suggestion: str = ""                # O que Claude deve fazer para corrigir

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class StageResult:
    """Resultado de uma etapa do pipeline."""

    stage_id: int
    stage_name: str
    status: str = "ok"                      # "ok" | "partial" | "error" | "skipped"
    detalhes: str = ""
    data: Any = None                        # Objeto retornado pela etapa
    errors: List[StructuredError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.stage_id,
            "nome": self.stage_name,
            "status": self.status,
            "detalhes": self.detalhes,
            "erros": [e.to_dict() for e in self.errors],
            "avisos": self.warnings,
        }


class PipelineCollector:
    """Coleta resultados e erros de todas as etapas do pipeline."""

    def __init__(self, mes: int, ano: int):
        self.mes = mes
        self.ano = ano
        self.stages: List[StageResult] = []
        self._current_stage: Optional[StageResult] = None

    # ── Stage lifecycle ──────────────────────────────────────────

    def begin_stage(self, stage_id: int, stage_name: str) -> StageResult:
        """Inicia uma nova etapa."""
        sr = StageResult(stage_id=stage_id, stage_name=stage_name)
        self._current_stage = sr
        self.stages.append(sr)
        return sr

    def end_stage_ok(self, detalhes: str = "", data: Any = None) -> None:
        """Finaliza a etapa atual como sucesso."""
        if self._current_stage:
            self._current_stage.status = "ok" if not self._current_stage.errors else "partial"
            self._current_stage.detalhes = detalhes
            self._current_stage.data = data

    def end_stage_error(self, detalhes: str = "", data: Any = None) -> None:
        """Finaliza a etapa atual como erro."""
        if self._current_stage:
            self._current_stage.status = "error"
            self._current_stage.detalhes = detalhes
            self._current_stage.data = data

    def skip_stage(self, stage_id: int, stage_name: str, reason: str) -> None:
        """Registra uma etapa pulada."""
        sr = StageResult(
            stage_id=stage_id,
            stage_name=stage_name,
            status="skipped",
            detalhes=reason,
        )
        self.stages.append(sr)

    # ── Error collection ─────────────────────────────────────────

    def add_error(
        self,
        stage: str,
        tipo: str,
        mensagem: str,
        contexto: Optional[Dict[str, Any]] = None,
        impacto: str = "",
        recovery: str = "",
        fix_suggestion: str = "",
    ) -> None:
        """Adiciona um erro estruturado à etapa atual."""
        err = StructuredError(
            stage=stage,
            tipo=tipo,
            mensagem=mensagem,
            contexto=contexto or {},
            impacto=impacto,
            recovery=recovery,
            fix_suggestion=fix_suggestion,
        )
        if self._current_stage:
            self._current_stage.errors.append(err)

    def add_warning(self, msg: str) -> None:
        """Adiciona aviso à etapa atual."""
        if self._current_stage:
            self._current_stage.warnings.append(msg)

    def capture_exception(
        self,
        stage: str,
        exc: Exception,
        contexto: Optional[Dict[str, Any]] = None,
        recovery: str = "stage_aborted",
        fix_suggestion: str = "",
    ) -> None:
        """Captura uma exceção Python e converte em erro estruturado."""
        tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
        last_frame = tb[-2].strip() if len(tb) >= 2 else ""

        self.add_error(
            stage=stage,
            tipo=type(exc).__name__,
            mensagem=str(exc),
            contexto={
                **(contexto or {}),
                "traceback_last_frame": last_frame,
                "exception_type": type(exc).__name__,
            },
            impacto=f"Etapa '{stage}' falhou com exceção",
            recovery=recovery,
            fix_suggestion=fix_suggestion or f"Leia scripts/{stage}.py para entender o erro",
        )

    # ── Helpers for common error patterns ────────────────────────

    def collect_result_warnings(self, stage: str, result_obj: Any) -> None:
        """Extrai warnings e errors de um Result object do pipeline existente."""
        if hasattr(result_obj, "warnings"):
            for w in result_obj.warnings:
                self.add_warning(w)
        if hasattr(result_obj, "errors"):
            for e in result_obj.errors:
                self.add_error(
                    stage=stage,
                    tipo="pipeline_error",
                    mensagem=str(e),
                    recovery="reported_from_original_pipeline",
                )

    # ── Aggregation ──────────────────────────────────────────────

    @property
    def global_status(self) -> str:
        """Status global do pipeline."""
        statuses = [s.status for s in self.stages]
        if "error" in statuses:
            return "error"
        if "partial" in statuses:
            return "partial"
        return "ok"

    @property
    def all_errors(self) -> List[StructuredError]:
        """Todos os erros de todas as etapas."""
        errors: List[StructuredError] = []
        for s in self.stages:
            errors.extend(s.errors)
        return errors

    @property
    def all_warnings(self) -> List[str]:
        """Todos os avisos de todas as etapas."""
        warnings: List[str] = []
        for s in self.stages:
            warnings.extend(s.warnings)
        return warnings

    def to_dict(self) -> Dict[str, Any]:
        """Serializa o resultado completo para JSON."""
        return {
            "status": self.global_status,
            "mes": self.mes,
            "ano": self.ano,
            "etapas": [s.to_dict() for s in self.stages],
            "erros": [e.to_dict() for e in self.all_errors],
            "avisos": self.all_warnings,
        }


def safe_execute(
    collector: PipelineCollector,
    stage_id: int,
    stage_name: str,
    fn: Callable[[], Any],
    detalhes_ok: str = "",
    skip_on_error: bool = False,
) -> Optional[Any]:
    """Executa uma função coletando erros sem interromper o pipeline.

    Args:
        collector: Coletor de resultados.
        stage_id: ID sequencial da etapa.
        stage_name: Nome legível da etapa.
        fn: Função a executar (sem argumentos — use closure/lambda).
        detalhes_ok: Texto de detalhe se sucesso.
        skip_on_error: Se True, retorna None em vez de propagar.

    Returns:
        O retorno de fn() ou None se houve erro e skip_on_error=True.
    """
    collector.begin_stage(stage_id, stage_name)
    try:
        from lean_conductor.live_debug import log_current_event

        log_current_event(
            "info",
            "pipeline_wrapper",
            stage_name,
            f"Etapa {stage_id} iniciada: {stage_name}",
            {"stage_id": stage_id},
        )
    except Exception:
        pass
    try:
        result = fn()
        # Coleta warnings/errors do result object se existirem
        if result is not None:
            collector.collect_result_warnings(stage_name, result)
        collector.end_stage_ok(detalhes=detalhes_ok, data=result)
        try:
            from lean_conductor.live_debug import log_current_event

            log_current_event(
                "success",
                "pipeline_wrapper",
                stage_name,
                f"Etapa {stage_id} concluida: {stage_name}",
                {"detalhes": detalhes_ok},
            )
        except Exception:
            pass
        return result
    except Exception as exc:
        collector.capture_exception(
            stage=stage_name,
            exc=exc,
            recovery="continued" if skip_on_error else "pipeline_stopped",
        )
        collector.end_stage_error(detalhes=str(exc))
        try:
            from lean_conductor.live_debug import log_current_event

            log_current_event(
                "error",
                "pipeline_wrapper",
                stage_name,
                f"Etapa {stage_id} falhou: {stage_name}",
                {"erro": str(exc), "skip_on_error": bool(skip_on_error)},
            )
        except Exception:
            pass
        if not skip_on_error:
            raise
        return None
