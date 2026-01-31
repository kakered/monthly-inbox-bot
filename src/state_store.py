# -*- coding: utf-8 -*-
"""
state_store.py
- Dropbox 上の state.json を安全に読み書きする最小実装
- "TypeError: StateStore.load() takes 1 positional argument but 2 were given" を根治
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class StateStore:
    """
    Dropbox 上の state.json を管理するための軽量ストア。
    """
    stages: Dict[str, Any] = field(default_factory=dict)
    updated_at_utc: str = ""

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "StateStore":
        return cls(
            stages=d.get("stages", {}) if isinstance(d.get("stages", {}), dict) else {},
            updated_at_utc=d.get("updated_at_utc", "") if isinstance(d.get("updated_at_utc", ""), str) else "",
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stages": self.stages,
            "updated_at_utc": self.updated_at_utc,
        }

    @classmethod
    def load(cls, dbx, state_path: str) -> "StateStore":
        """
        Dropbox から state.json を読み込む。
        state_path が存在しない/壊れている場合は空の state を返す。
        """
        if not state_path:
            return cls()

        try:
            _md, resp = dbx.files_download(state_path)
            raw = resp.content.decode("utf-8", errors="replace")
            obj = json.loads(raw)
            if isinstance(obj, dict):
                return cls.from_dict(obj)
            return cls()
        except Exception:
            # 「壊れた state」で全体が止まるより、空 state で走らせる（ログに warn を出すのは呼び出し側）
            return cls()

    def save(self, dbx, state_path: str) -> None:
        """
        Dropbox に state.json を上書き保存する。
        """
        if not state_path:
            return
        data = json.dumps(self.to_dict(), ensure_ascii=False, indent=2).encode("utf-8")
        # overwrite=True が欲しいが SDK 仕様で mode 指定
        import dropbox  # local import

        dbx.files_upload(data, state_path, mode=dropbox.files.WriteMode.overwrite)