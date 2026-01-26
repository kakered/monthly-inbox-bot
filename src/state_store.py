# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Optional

from .state import PipelineState
from .dropbox_io import DropboxIO


def load_state(dbx: DropboxIO, path: str) -> PipelineState:
    raw = dbx.read_bytes_or_none(path)
    if raw is None:
        return PipelineState()
    try:
        obj = json.loads(raw.decode("utf-8"))
    except Exception:
        # If corrupted, start fresh instead of crashing
        return PipelineState()
    return PipelineState.from_dict(obj)


def save_state(dbx: DropboxIO, path: str, state: PipelineState) -> None:
    data = json.dumps(state.to_dict(), ensure_ascii=False, indent=2).encode("utf-8")
    dbx.write_bytes(path, data, mode="overwrite")