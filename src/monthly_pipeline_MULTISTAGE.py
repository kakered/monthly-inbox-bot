# -*- coding: utf-8 -*-
"""
monthly_pipeline_MULTISTAGE.py

Stage switch runner.

Behavior (safe default):
- Read files from STAGExx_IN
- Copy the same bytes to STAGExx_OUT (same filename)
- Move original file to STAGExx_DONE

This prevents "stuck in IN" and avoids destructive behavior.
You can later replace per-stage transform logic.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import List

from src.dropbox_io import DropboxIO
from src.monthly_spec import MonthlyCfg


def _join(folder: str, name: str) -> str:
    folder = folder.rstrip("/")
    if not folder.startswith("/"):
        folder = "/" + folder
    return f"{folder}/{name}"


def run_switch_stage(dbx: DropboxIO, cfg: MonthlyCfg) -> int:
    st = cfg.stages[cfg.monthly_stage]

    # Ensure folders exist
    dbx.ensure_folder(st.in_path)
    dbx.ensure_folder(st.out_path)
    dbx.ensure_folder(st.done_path)

    files = dbx.list_files(st.in_path)
    if not files:
        return 0

    # deterministic order
    files = sorted(files, key=lambda f: (f.server_modified or f.client_modified or datetime(1970, 1, 1, tzinfo=timezone.utc), f.name))

    n = 0
    for f in files[: cfg.max_files_per_run]:
        src = f.path_lower or _join(st.in_path, f.name)
        out = _join(st.out_path, f.name)
        done = _join(st.done_path, f.name)

        data = dbx.download(src)
        dbx.upload(out, data, overwrite=True)
        dbx.move(src, done, overwrite=True)

        n += 1

    return n