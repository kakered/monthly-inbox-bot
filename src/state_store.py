# -*- coding: utf-8 -*-
"""state_store.py
State + audit collector.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import time
from typing import Any

from .dropbox_io import DropboxIO


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def stable_key(stage: str, file_id: str | None, path: str) -> str:
    if file_id:
        return f"{stage}:id:{file_id}"
    return f"{stage}:path:{path}"


@dataclass
class StateStore:
    path: str
    data: dict[str, Any] = field(default_factory=dict)
    audit: list[dict[str, Any]] = field(default_factory=list)

    @staticmethod
    def _empty() -> dict[str, Any]:
        return {"done": [], "seen": [], "errors": [], "processed": {}}

    @classmethod
    def load(cls, dbx: DropboxIO, path: str) -> "StateStore":
        try:
            b = dbx.download_to_bytes(path)
            d = json.loads(b.decode("utf-8"))
            base = cls._empty()
            base.update(d if isinstance(d, dict) else {})
            if not isinstance(base.get("done"), list):
                base["done"] = []
            if not isinstance(base.get("seen"), list):
                base["seen"] = []
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

    def log(self, **event: Any) -> None:
        ev = {"timestamp": now_iso(), **event}
        self.audit.append(ev)

    def flush_audit_jsonl(self, dbx: DropboxIO, logs_dir: str, run_id: str) -> str:
        dbx.ensure_folder(logs_dir)
        lines = [json.dumps(e, ensure_ascii=False) for e in self.audit]
        body = ("\n".join(lines) + "\n").encode("utf-8") if lines else b"{}\n"
        out_path = f"{logs_dir.rstrip('/')}/monthly_audit_{run_id}.jsonl"
        dbx.write_file_bytes(out_path, body, overwrite=True)
        return out_path