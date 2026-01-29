# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .dropbox_io import DropboxIO


def _utc_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@dataclass
class StateStore:
    """
    Robust state store:
    - If state missing OR invalid OR empty dict: still allows processing.
    - Writes are atomic-upload-overwrite (no 3-bytes broken file).
    """
    io: DropboxIO
    state_path: str

    def load(self) -> Dict[str, Any]:
        if not self.state_path:
            # No state path => run without persistent state
            return {}

        try:
            raw = self.io.download(self.state_path)
        except Exception:
            return {}

        try:
            obj = json.loads(raw.decode("utf-8"))
            if isinstance(obj, dict):
                return obj
            return {}
        except Exception:
            # If corrupted, do not block; just start fresh.
            return {}

    def save(self, state: Dict[str, Any]) -> None:
        if not self.state_path:
            return
        # ensure JSON serializable, and include last_update
        state = dict(state)
        state["updated_at_utc"] = _utc_ts()
        payload = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
        self.io.atomic_upload_overwrite(self.state_path, payload, suffix=".tmp")

    @staticmethod
    def get_stage_bucket(state: Dict[str, Any], stage: str) -> Dict[str, Any]:
        buckets = state.setdefault("stages", {})
        b = buckets.setdefault(stage, {})
        # normalized keys
        b.setdefault("done", [])         # list of identifiers
        b.setdefault("last_run_utc", "")
        return b

    @staticmethod
    def mark_done(bucket: Dict[str, Any], key: str) -> None:
        done = bucket.setdefault("done", [])
        if key not in done:
            done.append(key)

    @staticmethod
    def is_done(bucket: Dict[str, Any], key: str) -> bool:
        done = bucket.get("done", [])
        return isinstance(done, list) and key in done