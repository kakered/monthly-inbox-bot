# -*- coding: utf-8 -*-
"""
dropbox_io.py
Thin wrapper around Dropbox SDK.

Goals:
- Provide a single, stable interface used by the pipeline.
- Ensure folders exist (recursive) before writing/moving.
- Keep behavior conservative (no implicit overwrite on move).

Public API:
- DropboxIO.from_env()
- ensure_folder(path)  (recursive)
- list_folder(path) -> list[Metadata]
- download_to_bytes(path) -> bytes
- write_file_bytes(path, data, overwrite=True)
- write_text(path, text, overwrite=True)
- move(src, dst)

NOTE:
- We intentionally keep move() as non-overwriting (autorename=False) so problems surface.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
import time

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import WriteMode, FolderMetadata


def _norm(p: str) -> str:
    p = (p or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    if len(p) > 1 and p.endswith("/"):
        p = p[:-1]
    return p


def _parent_dir(path: str) -> str:
    path = _norm(path)
    if path == "/":
        return "/"
    parts = [x for x in path.split("/") if x]
    if len(parts) <= 1:
        return "/"
    return "/" + "/".join(parts[:-1])


@dataclass
class DropboxIO:
    dbx: dropbox.Dropbox

    @staticmethod
    def from_env() -> "DropboxIO":
        # Preferred: refresh token flow
        rt = os.getenv("DROPBOX_REFRESH_TOKEN", "").strip()
        app_key = os.getenv("DROPBOX_APP_KEY", "").strip()
        app_secret = os.getenv("DROPBOX_APP_SECRET", "").strip()
        if rt and app_key and app_secret:
            return DropboxIO(
                dbx=dropbox.Dropbox(
                    oauth2_refresh_token=rt,
                    app_key=app_key,
                    app_secret=app_secret,
                )
            )

        # Fallback: access token (short-lived)
        at = os.getenv("DROPBOX_ACCESS_TOKEN", "").strip()
        if at:
            return DropboxIO(dbx=dropbox.Dropbox(oauth2_access_token=at))

        raise RuntimeError(
            "Dropbox auth missing. Set either "
            "(DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET) "
            "or DROPBOX_ACCESS_TOKEN."
        )

    def ensure_folder(self, path: str) -> None:
        """Ensure a folder exists (recursive)."""
        path = _norm(path)
        if path in ("", "/"):
            return
        parts = [p for p in path.split("/") if p]
        cur = ""
        for part in parts:
            cur = cur + "/" + part
            try:
                self.dbx.files_create_folder_v2(cur)
            except ApiError:
                # already exists is OK (or created by another process)
                try:
                    md = self.dbx.files_get_metadata(cur)
                    if isinstance(md, FolderMetadata):
                        continue
                except Exception:
                    raise

    def list_folder(self, path: str) -> list[dropbox.files.Metadata]:
        path = _norm(path)
        res = self.dbx.files_list_folder(path)
        entries = list(res.entries)
        while res.has_more:
            res = self.dbx.files_list_folder_continue(res.cursor)
            entries.extend(res.entries)
        return entries

    def download_to_bytes(self, path: str) -> bytes:
        path = _norm(path)
        _md, resp = self.dbx.files_download(path)
        return resp.content

    def write_file_bytes(self, path: str, data: bytes, overwrite: bool = True) -> None:
        path = _norm(path)
        self.ensure_folder(_parent_dir(path))
        mode = WriteMode.overwrite if overwrite else WriteMode.add

        for i in range(3):
            try:
                self.dbx.files_upload(data, path, mode=mode, mute=True)
                return
            except ApiError:
                if i == 2:
                    raise
                time.sleep(0.8 * (i + 1))

    def write_text(self, path: str, text: str, overwrite: bool = True) -> None:
        self.write_file_bytes(path, text.encode("utf-8"), overwrite=overwrite)

    def move(self, src: str, dst: str) -> None:
        src = _norm(src)
        dst = _norm(dst)
        self.ensure_folder(_parent_dir(dst))
        self.dbx.files_move_v2(
            src,
            dst,
            autorename=False,
            allow_shared_folder=True,
            allow_ownership_transfer=False,
        )