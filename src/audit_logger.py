# -*- coding: utf-8 -*-
"""
audit_logger.py
Dropbox に残る JSONL 監査ログ（1行=1イベント）

- 1 run_id ごとに 1ファイル: /_system/logs/monthly_audit_<run_id>.jsonl
- 途中で落ちても「できる限り」残すため、flush() は何度呼んでもOK
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from typing import Any, Dict, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _norm_dir(p: str) -> str:
    p = (p or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    if len(p) > 1 and p.endswith("/"):
        p = p[:-1]
    return p


@dataclass
class AuditLogger:
    logs_dir: str
    run_id: str
    buffer: list[dict[str, Any]] = field(default_factory=list)

    def event(
        self,
        *,
        stage: str,
        event: str,
        message: str | None = None,
        src_path: str | None = None,
        dst_path: str | None = None,
        filename: str | None = None,
        **extra: Any,
    ) -> None:
        rec: Dict[str, Any] = {
            "timestamp": _now_iso(),
            "run_id": self.run_id,
            "stage": stage,
            "event": event,
        }
        if message is not None:
            rec["message"] = message
        if src_path is not None:
            rec["src_path"] = src_path
        if dst_path is not None:
            rec["dst_path"] = dst_path
        if filename is not None:
            rec["filename"] = filename
        for k, v in extra.items():
            if v is not None:
                rec[k] = v
        self.buffer.append(rec)

    def flush(self, dbx: Any) -> None:
        """
        dbx は src.dropbox_io.DropboxIO を想定（write_file_bytes を持つ）
        """
        logs_dir = _norm_dir(self.logs_dir)
        path = f"{logs_dir}/monthly_audit_{self.run_id}.jsonl"
        data = ("\n".join(json.dumps(x, ensure_ascii=False) for x in self.buffer) + "\n").encode("utf-8")
        dbx.write_file_bytes(path, data, overwrite=True)


def build_run_id() -> str:
    """
    GitHub Actions 上なら GITHUB_RUN_ID / GITHUB_RUN_ATTEMPT がある。
    無ければローカル実行用に時刻ベース。
    """
    rid = os.getenv("GITHUB_RUN_ID", "").strip()
    att = os.getenv("GITHUB_RUN_ATTEMPT", "").strip()
    if rid:
        return f"gh-{rid}-{att or '1'}"
    return f"local-{datetime.now().strftime('%Y%m%d-%H%M%S')}"