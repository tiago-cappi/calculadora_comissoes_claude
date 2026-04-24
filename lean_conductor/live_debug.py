from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parent.parent
_LOCK = Lock()
_ACTIVE_CONTEXT: Optional[Tuple[int, int]] = None
_ACTIVE_FLAGS: Dict[Tuple[int, int], bool] = {}


def _period_dir(mes: int, ano: int) -> Path:
    return ROOT / "saida" / f"{mes:02d}_{ano}"


def _log_path(mes: int, ano: int) -> Path:
    return _period_dir(mes, ano) / "pipeline_debug.jsonl"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def start_run_log(mes: int, ano: int, metadata: Optional[Dict[str, Any]] = None) -> Path:
    global _ACTIVE_CONTEXT
    path = _log_path(mes, ano)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK:
        path.write_text("", encoding="utf-8")
        _ACTIVE_CONTEXT = (mes, ano)
        _ACTIVE_FLAGS[(mes, ano)] = True
    log_event(
        mes=mes,
        ano=ano,
        level="info",
        source="ui",
        stage="run_start",
        message=f"Execucao iniciada para {mes:02d}/{ano}.",
        details=metadata or {},
    )
    return path


def finish_run_log(mes: int, ano: int, status: str, details: Optional[Dict[str, Any]] = None) -> None:
    global _ACTIVE_CONTEXT
    log_event(
        mes=mes,
        ano=ano,
        level="success" if status == "ok" else "error",
        source="ui",
        stage="run_finish",
        message=f"Execucao finalizada com status={status}.",
        details=details or {},
    )
    with _LOCK:
        _ACTIVE_FLAGS[(mes, ano)] = False
        if _ACTIVE_CONTEXT == (mes, ano):
            _ACTIVE_CONTEXT = None


def log_event(
    mes: int,
    ano: int,
    level: str,
    source: str,
    stage: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    path = _log_path(mes, ano)
    path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "timestamp": _utc_now(),
        "level": str(level or "info"),
        "source": str(source or "pipeline"),
        "stage": str(stage or ""),
        "message": str(message or ""),
        "details": details or {},
    }
    line = json.dumps(entry, ensure_ascii=False, default=str)
    terminal_line = (
        f"[pipeline-debug {mes:02d}/{ano}] "
        f"{entry['level'].upper()} "
        f"{entry['source']}::{entry['stage']} - {entry['message']}"
    )
    try:
        print(terminal_line)
    except UnicodeEncodeError:
        print(terminal_line.encode("ascii", errors="replace").decode("ascii"))
    with _LOCK:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def log_current_event(
    level: str,
    source: str,
    stage: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    context = _ACTIVE_CONTEXT
    if context is None:
        return
    mes, ano = context
    log_event(mes, ano, level, source, stage, message, details)


def read_run_log(mes: int, ano: int, limit: int = 400) -> List[Dict[str, Any]]:
    path = _log_path(mes, ano)
    if not path.exists():
        return []

    entries: List[Dict[str, Any]] = []
    with _LOCK:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

    start_index = max(0, len(lines) - max(int(limit), 1))
    for idx, raw in enumerate(lines[start_index:], start=start_index + 1):
        try:
            entry = json.loads(raw)
        except json.JSONDecodeError:
            entry = {
                "timestamp": _utc_now(),
                "level": "warning",
                "source": "debug",
                "stage": "parse",
                "message": raw,
                "details": {},
            }
        entry["seq"] = idx
        entries.append(entry)
    return entries


def is_run_active(mes: int, ano: int) -> bool:
    return bool(_ACTIVE_FLAGS.get((mes, ano), False))
