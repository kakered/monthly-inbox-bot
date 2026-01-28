# -*- coding: utf-8 -*-
"""
state_store.py
/_system/state.json を読み書きする。

今回の目的:
- 保存時に malformed_path を起こさない（親フォルダ ensure を安定化）
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, Any

from src.dropbox_io import DropboxIO


@dataclass
class StateStore:
    path: str
    data: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, dbx: DropboxIO, path: str) -> "StateStore":
        try:
            raw = dbx.read_file_bytes(path)
            data = json.loads(raw.decode("utf-8"))
            return cls(path=path, data=data if isinstance(data, dict) else {})
        except Exception:
            return cls(path=path, data={})

    def save(self, dbx: DropboxIO) -> None:
        out = (json.dumps(self.data, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        dbx.write_file_bytes(self.path, out, overwrite=True)