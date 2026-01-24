# -*- coding: utf-8 -*-
"""
state_store.py

StateStore: keeps track of processed file keys (path_lower).
This version is compatible with DropboxIO providing read_file_bytes / write_file_bytes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class StateStore:
    state_path: str
    data: Dict[str, Any] = field(default_factory=lambda: {"processed": {}})

    @classmethod
    def load(cls, dbx, state_path: str) -> "StateStore":
        try:
            raw = dbx.read_file_bytes(state_path)
            data = json.loads(raw.decode("utf-8", errors="replace"))
            if not isinstance(data, dict):
                data = {"processed": {}}
        except Exception:
            data = {"processed": {}}
        if "processed" not in data or not isinstance(data["processed"], dict):
            data["processed"] = {}
        return cls(state_path=state_path, data=data)

    def save(self, dbx) -> None:
        payload = json.dumps(self.data, ensure_ascii=False, indent=2).encode("utf-8")
        dbx.write_file_bytes(self.state_path, payload)

    def is_processed(self, key: str) -> bool:
        return key in self.data.get("processed", {})

    def mark_processed(self, key: str, rev: Optional[str] = None) -> None:
        # rev is optional; keep compatibility with callers
        self.data.setdefault("processed", {})[key] = rev or "done"
