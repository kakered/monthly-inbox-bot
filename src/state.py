# -*- coding: utf-8 -*-
"""
State store for Dropbox-based pipelines.

- DropboxIO 実装差（download_to_bytes/read_file_bytes 等）を吸収します。
- state.json に "processed" dict を保存します。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


def _dbx_read_bytes(dbx: Any, path: str) -> bytes:
    # 本プロジェクト内で出てきた読み出しAPIを順に試す
    if hasattr(dbx, "download_to_bytes"):
        return dbx.download_to_bytes(path)
    if hasattr(dbx, "read_file_bytes"):
        return dbx.read_file_bytes(path)
    if hasattr(dbx, "download"):
        return dbx.download(path)
    raise AttributeError(
        "DropboxIO has no supported read method (download_to_bytes/read_file_bytes/download)."
    )


def _dbx_write_bytes(dbx: Any, path: str, data: bytes) -> None:
    # 本プロジェクト内で出てきた書き込みAPIを順に試す
    if hasattr(dbx, "upload_bytes"):
        dbx.upload_bytes(path, data)
        return
    if hasattr(dbx, "write_file_bytes"):
        dbx.write_file_bytes(path, data)
        return
    if hasattr(dbx, "upload"):
        dbx.upload(path, data)
        return
    raise AttributeError(
        "DropboxIO has no supported write method (upload_bytes/write_file_bytes/upload)."
    )


@dataclass
class StateStore:
    dbx: Any
    state_path: str
    processed: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def load(cls, dbx: Any, state_path: str) -> "StateStore":
        try:
            raw = _dbx_read_bytes(dbx, state_path)
            data = json.loads(raw.decode("utf-8", errors="replace"))
            processed = data.get("processed", {}) if isinstance(data, dict) else {}
            if not isinstance(processed, dict):
                processed = {}
            processed = {str(k): str(v) for k, v in processed.items()}
            return cls(dbx=dbx, state_path=state_path, processed=processed)
        except Exception:
            # 無い/壊れてる場合は空で開始
            return cls(dbx=dbx, state_path=state_path, processed={})

    def is_processed(self, key: str) -> bool:
        return key in self.processed

    def mark_processed(self, key: str, rev: Optional[str] = None, **_ignored: Any) -> None:
        # rev=... や flush 呼び出し等の「版ズレ」を吸収するため kwargs を受ける
        self.processed[str(key)] = "" if rev is None else str(rev)

    def save(self, dbx: Optional[Any] = None) -> None:
        if dbx is None:
            dbx = self.dbx
        payload = json.dumps({"processed": self.processed}, ensure_ascii=False, indent=2).encode("utf-8")
        _dbx_write_bytes(dbx, self.state_path, payload)