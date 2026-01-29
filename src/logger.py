# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

from .dropbox_io import DropboxIO


def _jst_date() -> str:
    # keep it simple: JST folder name. (UTC+9)
    t = time.time() + 9 * 3600
    return time.strftime("%Y%m%d", time.gmtime(t))


def _utc_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class JsonlLogger:
    """
    Append-style JSONL logger.
    Even if logging fails, pipeline should continue.
    """
    def __init__(self, io: DropboxIO, logs_dir: str):
        self.io = io
        self.logs_dir = logs_dir.rstrip("/")

    def log(self, obj: Dict[str, Any]) -> None:
        try:
            day = _jst_date()
            path_dir = f"{self.logs_dir}/{day}"
            self.io.ensure_folder(self.logs_dir)
            self.io.ensure_folder(path_dir)

            ts = _utc_ts().replace(":", "").replace("-", "")
            path = f"{path_dir}/run_{ts}.jsonl"

            obj = dict(obj)
            obj.setdefault("ts_utc", _utc_ts())
            payload = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")

            # Read existing (if any) then append (Dropbox SDK has upload_session for true append,
            # but for simplicity in early stage we overwrite with appended content if exists.)
            try:
                existing = self.io.download(path)
                payload = existing + payload
            except Exception:
                pass

            self.io.upload_overwrite(path, payload)
        except Exception:
            # swallow logging errors
            return