# -*- coding: utf-8 -*-
"""
dropbox_io.py
Thin wrapper around Dropbox SDK used by monthly-inbox-bot.

Goals:
- Keep interface stable (list_folder / read_file_bytes / write_file_bytes / move / delete / ensure_folder)
- Be robust to SDK changes (NO res.to_dict()).
- Minimal behavior: prefer correctness + clear failures.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import List, Optional

import dropbox
from dropbox.files import WriteMode
from dropbox.exceptions import ApiError


def _norm_path(path: str) -> str:
    """
    Normalize a Dropbox path:
    - must start with '/'
    - collapse multiple slashes
    - strip trailing slash except root
    """
    if path is None:
        return ""
    p = str(path).strip()
    if not p:
        return ""
    p = p.replace("\\", "/")
    while "//" in p:
        p = p.replace("//", "/")
    if not p.startswith("/"):
        p = "/" + p
    if len(p) > 1 and p.endswith("/"):
        p = p[:-1]
    return p


def _parent_dir(path: str) -> str:
    p = _norm_path(path)
    if not p or p == "/":
        return ""
    parent = os.path.dirname(p)
    if parent in (".", "/"):
        return "" if parent == "." else "/"
    return parent


@dataclass
class DropboxIO:
    dbx: dropbox.Dropbox

    # ---------- folder ops ----------
    def ensure_folder(self, path: str) -> None:
        """
        Ensure a folder exists. Creates parents as needed.
        Safe for:
        - '' or '/' (no-op)
        - already-existing folders
        """
        p = _norm_path(path)
        if not p or p == "/":
            return

        parts = [x for x in p.split("/") if x]
        cur = ""
        for part in parts:
            cur = cur + "/" + part
            try:
                self.dbx.files_create_folder_v2(cur)
            except ApiError as e:
                # already exists or cannot create -> ignore if it's "already exists"
                # Dropbox SDK doesn't expose a single stable code, so we accept common patterns.
                msg = repr(e)
                if ("conflict" in msg and "folder" in msg) or ("already exists" in msg) or ("conflict" in msg):
                    continue
                # malformed_path typically happens when cur is invalid; we want to fail loudly then.
                raise

    # ---------- list/read/write ----------
    def list_folder(self, path: str) -> List[object]:
        """
        Return a list of Metadata objects (FileMetadata/FolderMetadata/DeletedMetadata).
        Do NOT convert to dict (SDK objects don't guarantee to_dict()).
        """
        p = _norm_path(path)
        if not p:
            return []
        try:
            res = self.dbx.files_list_folder(p)
        except ApiError as e:
            # if folder doesn't exist, treat as empty
            msg = repr(e)
            if "not_found" in msg or "path" in msg and "not_found" in msg:
                return []
            raise

        entries = list(res.entries or [])
        while getattr(res, "has_more", False):
            res = self.dbx.files_list_folder_continue(res.cursor)
            entries.extend(list(res.entries or []))
        return entries

    def read_file_bytes(self, path: str) -> bytes:
        p = _norm_path(path)
        md, resp = self.dbx.files_download(p)
        return resp.content

    def write_file_bytes(self, path: str, data: bytes, overwrite: bool = True) -> None:
        p = _norm_path(path)
        parent = _parent_dir(p)
        if parent and parent != "/":
            self.ensure_folder(parent)

        mode = WriteMode.overwrite if overwrite else WriteMode.add
        # mute=True avoids creating extra notifications
        self.dbx.files_upload(data, p, mode=mode, mute=True)

    # ---------- move/delete ----------
    def delete(self, path: str) -> None:
        p = _norm_path(path)
        if not p:
            return
        try:
            self.dbx.files_delete_v2(p)
        except ApiError as e:
            msg = repr(e)
            if "not_found" in msg:
                return
            raise

    def move(self, src_path: str, dst_path: str, overwrite: bool = False) -> None:
        """
        Move file/folder.
        If overwrite=True and destination exists, delete destination then move.
        """
        src = _norm_path(src_path)
        dst = _norm_path(dst_path)

        # ensure destination parent exists
        parent = _parent_dir(dst)
        if parent and parent not in ("", "/"):
            self.ensure_folder(parent)

        if overwrite:
            # best-effort delete; ignore not_found
            try:
                self.delete(dst)
            except Exception:
                pass

        # Dropbox move can fail due to transient conflicts; retry a bit
        for i in range(3):
            try:
                self.dbx.files_move_v2(src, dst, autorename=False)
                return
            except ApiError as e:
                # If conflict and overwrite was requested, attempt delete+retry.
                msg = repr(e)
                if overwrite and ("conflict" in msg or "already_exists" in msg):
                    try:
                        self.delete(dst)
                    except Exception:
                        pass
                    time.sleep(0.2 * (i + 1))
                    continue
                raise
        # if not returned, raise a clear error
        raise RuntimeError(f"DropboxIO.move failed after retries: {src} -> {dst} (overwrite={overwrite})")