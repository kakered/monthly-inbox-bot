# -*- coding: utf-8 -*-
"""
audit_logger.py
Write JSONL audit logs to Dropbox.

- Append is implemented by download + re-upload (Dropbox has no append API).
- Robust even if the log file doesn't exist yet.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .dropbox_io import DropboxIO


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today_utc_ymd() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


@dataclass
class AuditLogger:
    dbx: DropboxIO
    logs_dir: str
    run_id: str

    def _log_path(self) -> str:
        day = _today_utc_ymd()
        base = self.logs_dir.rstrip("/")
        return f"{base}/{day}/audit_{self.run_id}.jsonl"

    def write(self, record: Dict[str, Any]) -> None:
        rec = dict(record)
        rec.setdefault("timestamp", _utc_now_iso())
        rec.setdefault("run_id", self.run_id)

        line = (json.dumps(rec, ensure_ascii=False) + "\n").encode("utf-8")
        path = self._log_path()

        try:
            prev = b""
            if self.dbx.exists(path):
                prev = self.dbx.read_file_bytes(path)
            self.dbx.write_file_bytes(path, prev + line, overwrite=True)
        except Exception:
            # last resort: write only the new line
            self.dbx.write_file_bytes(path, line, overwrite=True)


def write_audit_record(
    dbx: DropboxIO,
    logs_dir: str,
    run_id: str,
    stage: str,
    event: str,
    src_path: Optional[str] = None,
    dst_path: Optional[str] = None,
    filename: Optional[str] = None,
    message: Optional[str] = None,
    **extra: Any,
) -> None:
    logger = AuditLogger(dbx=dbx, logs_dir=logs_dir, run_id=run_id)
    rec: Dict[str, Any] = {"stage": stage, "event": event}
    if src_path is not None:
        rec["src_path"] = src_path
    if dst_path is not None:
        rec["dst_path"] = dst_path
    if filename is not None:
        rec["filename"] = filename
    if message is not None:
        rec["message"] = message
    rec.update(extra)
    logger.write(rec)