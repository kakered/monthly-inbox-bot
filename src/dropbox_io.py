# -*- coding: utf-8 -*-
"""
dropbox_io.py
Dropbox SDK thin wrapper:
- list_folder
- download_to_bytes
- write_file_bytes
- move
- ensure_folder
"""

from __future__ import annotations

from dataclasses import dataclass
import os
import time
from typing import List

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import WriteMode, FolderMetadata, Metadata


def _norm(p: str) -> str:
    p = (p or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    if len(p) > 1 and p.endswith("/"):
        p = p[:-1]
    return p


@dataclass
class DropboxIO:
    dbx: dropbox.Dropbox

    @staticmethod
    def from_env() -> "DropboxIO":
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

        at = os.getenv("DROPBOX_ACCESS_TOKEN", "").strip()
        if at:
            return DropboxIO(dbx=dropbox.Dropbox(oauth2_access_token=at))

        raise RuntimeError(
            "Dropbox auth missing. Set either "
            "(DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET) "
            "or DROPBOX_ACCESS_TOKEN."
        )

    def ensure_folder(self, path: str) -> None:
        path = _norm(path)
        if path == "/":
            return
        try:
            self.dbx.files_create_folder_v2(path)
        except ApiError:
            # exists is OK
            try:
                md = self.dbx.files_get_metadata(path)
                if isinstance(md, FolderMetadata):
                    return
            except Exception:
                pass
            return

    def list_folder(self, path: str) -> List[Metadata]:
        path = _norm(path)
        res = self.dbx.files_list_folder(path)
        entries = list(res.entries)
        while res.has_more:
            res = self.dbx.files_list_folder_continue(res.cursor)
            entries.extend(res.entries)
        return entries

    def download_to_bytes(self, path: str) -> bytes:
        path = _norm(path)
        _, resp = self.dbx.files_download(path)
        return resp.content

    def write_file_bytes(self, path: str, data: bytes, overwrite: bool = True) -> None:
        path = _norm(path)
        parent = "/" + "/".join(path.split("/")[:-1]) if path.count("/") >= 2 else "/"
        self.ensure_folder(parent)

        mode = WriteMode.overwrite if overwrite else WriteMode.add
        for i in range(3):
            try:
                self.dbx.files_upload(data, path, mode=mode, mute=True)
                return
            except ApiError:
                if i == 2:
                    raise
                time.sleep(0.8 * (i + 1))

    def move(self, src: str, dst: str) -> None:
        src = _norm(src)
        dst = _norm(dst)
        parent = "/" + "/".join(dst.split("/")[:-1]) if dst.count("/") >= 2 else "/"
        self.ensure_folder(parent)
        self.dbx.files_move_v2(src, dst, autorename=False, allow_shared_folder=True)