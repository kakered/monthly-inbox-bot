# -*- coding: utf-8 -*-
"""
state_store.py
State + audit collector (single source of truth).

State is stored as JSON on Dropbox (STATE_PATH).
Audit is stored as JSONL on Dropbox (LOGS_DIR), 1 run = 1 file.

AUDIT SCHEMA (1 line = 1 event)
- timestamp: ISO8601 UTC (e.g., 2026-01-27T12:34:56Z)
- run_id: GitHub Actions run unit (e.g., gh-<RUN_ID>-<ATTEMPT>)
- stage: "00" / "10" / "20" / "30" / "40" or "--"
- event: list / move / write_state / error / run_start / run_end / write / skip / stop
- src_path, dst_path, filename (optional)
- message (optional)

NOTE
- Audit is best-effort and should not crash the pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import time
from typing import Any, Optional

from .dropbox_io import DropboxIO


def now_iso_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def stable_key(path: str, content_hash: str | None = None) -> str:
    if content_hash:
        return f"{path}|sha256:{content_hash}"
    return path


@dataclass
class StateStore:
    path: str
    data: dict[str, Any] = field(default_factory=dict)
    _audit: list[dict[str, Any]] = field(default_factory=list)

    @staticmethod
    def _empty() -> dict[str, Any]:
        return {"done": [], "errors": [], "processed": {}}

    @classmethod
    def load(cls, dbx: DropboxIO, path: str) -> "StateStore":
        try:
            b = dbx.download_to_bytes(path)
            d = json.loads(b.decode("utf-8"))
            base = cls._empty()
            if isinstance(d, dict):
                base.update(d)
            if not isinstance(base.get("done"), list):
                base["done"] = []
            if not isinstance(base.get("errors"), list):
                base["errors"] = []
            if not isinstance(base.get("processed"), dict):
                base["processed"] = {}
            return cls(path=path, data=base)
        except Exception:
            return cls(path=path, data=cls._empty())

    def save(self, dbx: DropboxIO) -> None:
        out = json.dumps(self.data, ensure_ascii=False, indent=2).encode("utf-8")
        dbx.write_file_bytes(self.path, out, overwrite=True)

    def is_processed(self, key: str) -> bool:
        return key in self.data.get("processed", {})

    def mark_processed(self, key: str, value: str) -> None:
        self.data.setdefault("processed", {})[key] = value

    def add_done(self, src_path: str) -> None:
        self.data.setdefault("done", []).append(src_path)

    def add_error(self, item: dict[str, Any]) -> None:
        self.data.setdefault("errors", []).append(item)

    def audit_event(
        self,
        *,
        run_id: str,
        stage: str,
        event: str,
        src_path: Optional[str] = None,
        dst_path: Optional[str] = None,
        filename: Optional[str] = None,
        message: Optional[str] = None,
        **extra: Any,
    ) -> None:
        rec: dict[str, Any] = {
            "timestamp": now_iso_utc(),
            "run_id": run_id,
            "stage": stage,
            "event": event,
        }
        if src_path is not None:
            rec["src_path"] = src_path
        if dst_path is not None:
            rec["dst_path"] = dst_path
        if filename is not None:
            rec["filename"] = filename
        if message is not None:
            rec["message"] = message
        if extra:
            rec.update(extra)
        self._audit.append(rec)

    def flush_audit_jsonl(self, dbx: DropboxIO, logs_dir: str, run_id: str) -> str:
        # best effort flush: do not raise
        out_path = f"{logs_dir.rstrip('/')}/monthly_audit_{run_id}.jsonl"
        try:
            lines = [json.dumps(e, ensure_ascii=False) for e in self._audit]
            body = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
            dbx.write_file_bytes(out_path, body, overwrite=True)
        except Exception as e:
            # try to record the failure in-memory (won't be persisted if this also fails)
            self._audit.append(
                {
                    "timestamp": now_iso_utc(),
                    "run_id": run_id,
                    "stage": "--",
                    "event": "error",
                    "message": f"audit_flush_failed: {e}",
                }
            )
        return out_path