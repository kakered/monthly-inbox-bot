# -*- coding: utf-8 -*-
"""
dropbox_io.py

DropboxIO wrapper used by the pipeline.

This is a compatibility patch:
- Adds read_file_bytes / write_file_bytes aliases expected by StateStore
- Adds upload_bytes for binary payloads
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional

import dropbox
from dropbox.files import FileMetadata, FolderMetadata, WriteMode


@dataclass
class DropboxItem:
    name: str
    path: str
    path_lower: str
    rev: Optional[str] = None


class DropboxIO:
    def __init__(self, dbx: dropbox.Dropbox):
        self.dbx = dbx

    @classmethod
    def from_env(cls) -> "DropboxIO":
        token = os.getenv("DROPBOX_REFRESH_TOKEN") or os.getenv("DROPBOX_ACCESS_TOKEN")
        app_key = os.getenv("DROPBOX_APP_KEY")
        app_secret = os.getenv("DROPBOX_APP_SECRET")

        if os.getenv("DROPBOX_ACCESS_TOKEN"):
            dbx = dropbox.Dropbox(os.getenv("DROPBOX_ACCESS_TOKEN"))
            return cls(dbx)

        if not token:
            raise RuntimeError("DROPBOX_REFRESH_TOKEN or DROPBOX_ACCESS_TOKEN is required.")
        if not (app_key and app_secret):
            raise RuntimeError("DROPBOX_APP_KEY and DROPBOX_APP_SECRET are required for refresh token flow.")

        oauth = dropbox.DropboxOAuth2FlowNoRedirect(app_key, app_secret)
        # Using refresh token directly (Dropbox SDK supports oauth2_refresh_token)
        dbx = dropbox.Dropbox(oauth2_refresh_token=token, app_key=app_key, app_secret=app_secret)
        return cls(dbx)

    # -------- listing --------
    def list_folder(self, path: str) -> List[DropboxItem]:
        res = self.dbx.files_list_folder(path)
        out: List[DropboxItem] = []
        for e in res.entries:
            if isinstance(e, FileMetadata):
                out.append(DropboxItem(name=e.name, path=e.path_display, path_lower=e.path_lower, rev=e.rev))
        return out

    # -------- download --------
    def download_to_bytes(self, path: str) -> bytes:
        md, resp = self.dbx.files_download(path)
        return resp.content

    # Backward/compat aliases
    def read_file_bytes(self, path: str) -> bytes:
        return self.download_to_bytes(path)

    # -------- upload --------
    def upload_text(self, path: str, text: str, mode: str = "overwrite") -> None:
        data = text.encode("utf-8")
        self.upload_bytes(path, data, mode=mode)

    def upload_bytes(self, path: str, data: bytes, mode: str = "overwrite") -> None:
        wm = WriteMode.overwrite if mode == "overwrite" else WriteMode.add
        self.dbx.files_upload(data, path, mode=wm, mute=True)

    # Backward/compat aliases
    def write_file_bytes(self, path: str, data: bytes, mode: str = "overwrite") -> None:
        self.upload_bytes(path, data, mode=mode)
