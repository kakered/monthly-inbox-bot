# -*- coding: utf-8 -*-
"""
Stage 00 (minimal): Move/Copy files from IN -> OUT, then IN -> DONE.

Purpose:
- Prove stage dispatch wiring works.
- Keep behavior simple and observable in Dropbox folders.

Expected env (already provided by workflow):
- paths['in'], paths['out'], paths['done']
"""

from __future__ import annotations

import os
import time
from typing import Any


def _ts_localish() -> str:
    # use UTC to be deterministic in actions
    return time.strftime("%Y%m%d-%H%M%S", time.gmtime())


def _join(base: str, name: str) -> str:
    return base.rstrip("/") + "/" + name


def _list_files(dbx: Any, folder: str):
    """
    Return a list of entries (FileMetadata-like) from Dropbox folder.
    Supports:
      - raw dropbox.Dropbox client: files_list_folder
      - DropboxIO wrapper: list_folder (if exists)
    """
    if hasattr(dbx, "list_folder"):
        return dbx.list_folder(folder)  # type: ignore

    # raw SDK
    res = dbx.files_list_folder(folder)
    return res.entries


def _download_bytes(dbx: Any, path: str) -> bytes:
    """
    Supports:
      - DropboxIO: read_file_bytes/read_bytes
      - raw SDK: files_download
    """
    if hasattr(dbx, "read_file_bytes"):
        return dbx.read_file_bytes(path)  # type: ignore
    if hasattr(dbx, "read_bytes"):
        return dbx.read_bytes(path)  # type: ignore

    _md, resp = dbx.files_download(path)
    return resp.content


def _upload_bytes(dbx: Any, path: str, body: bytes, overwrite: bool = True):
    """
    Supports:
      - DropboxIO: write_file_bytes/upload_bytes
      - raw SDK: files_upload
    """
    if hasattr(dbx, "write_file_bytes"):
        return dbx.write_file_bytes(path, body, overwrite=overwrite)  # type: ignore
    if hasattr(dbx, "upload_bytes"):
        return dbx.upload_bytes(path, body, overwrite=overwrite)  # type: ignore

    import dropbox
    mode = dropbox.files.WriteMode.overwrite if overwrite else dropbox.files.WriteMode.add
    return dbx.files_upload(body, path, mode=mode)


def _move(dbx: Any, src: str, dst: str):
    """
    Supports:
      - DropboxIO: move
      - raw SDK: files_move_v2
    """
    if hasattr(dbx, "move"):
        return dbx.move(src, dst)  # type: ignore

    return dbx.files_move_v2(src, dst, autorename=True)


def run(*, dbx: Any, stage: str, paths: dict[str, str], state: Any = None, run_id: str = "", config: dict | None = None) -> int:
    p_in = paths.get("in", "")
    p_out = paths.get("out", "")
    p_done = paths.get("done", "")

    if not (p_in and p_out and p_done):
        raise RuntimeError(f"Stage00 requires paths in/out/done. got={paths!r}")

    entries = _list_files(dbx, p_in)
    files = [e for e in entries if type(e).__name__ == "FileMetadata"]

    # If wrapper returns plain dicts etc, accept those too
    if not files and entries:
        # best effort: treat anything with path_display as file
        files = [e for e in entries if getattr(e, "path_display", None)]

    processed = 0

    for e in files:
        src = getattr(e, "path_display", None) or getattr(e, "path_lower", None)
        name = getattr(e, "name", None) or os.path.basename(src or "")
        if not src or not name:
            continue

        body = _download_bytes(dbx, src)

        base, ext = os.path.splitext(name)
        out_name = f"{base}__stage00__{_ts_localish()}{ext}"
        out_path = _join(p_out, out_name)

        _upload_bytes(dbx, out_path, body, overwrite=True)

        done_name = f"{base}__rev-{_ts_localish()}{ext}"
        done_path = _join(p_done, done_name)
        _move(dbx, src, done_path)

        processed += 1

    # Optional: write a tiny marker into state if available
    if state is not None:
        try:
            if hasattr(state, "set_stage_result"):
                state.set_stage_result(stage, {"processed": processed, "run_id": run_id})  # type: ignore
        except Exception:
            pass

    return 0