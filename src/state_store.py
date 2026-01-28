# -*- coding: utf-8 -*-
"""
state_store.py
Dropbox 上の /_system/state.json 用の最小 state 管理。

目的:
- 同じファイルを何度も処理しない（stage単位の processed）
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict
import json


@dataclass
class StateStore:
    data: Dict[str, Any]

    @classmethod
    def default(cls) -> "StateStore":
        return cls({"processed": {}})

    @classmethod
    def load(cls, dbx: Any, path: str) -> "StateStore":
        try:
            raw = dbx.download_to_bytes(path)
            return cls(json.loads(raw.decode("utf-8")))
        except Exception:
            return cls.default()

    def save(self, dbx: Any, path: str) -> None:
        b = (json.dumps(self.data, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        dbx.write_file_bytes(path, b, overwrite=True)

    def _proc(self) -> Dict[str, Dict[str, bool]]:
        self.data.setdefault("processed", {})
        return self.data["processed"]

    def is_processed(self, stage: str, src_path: str) -> bool:
        return bool(self._proc().get(stage, {}).get(src_path))

    def mark_processed(self, stage: str, src_path: str) -> None:
        p = self._proc()
        p.setdefault(stage, {})
        p[stage][src_path] = True