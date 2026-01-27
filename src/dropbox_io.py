# -*- coding: utf-8 -*-
"""
dropbox_io.py
A small Dropbox helper for this repo.

Design goals:
- Work with Dropbox OAuth *refresh token* (DROPBOX_REFRESH_TOKEN + app key/secret)
- Provide only the primitives this project needs: list / download / upload / move / mkdir
- Be tolerant of "root" path differences ("" vs "/")
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List

import dropbox
from dropbox.files import FileMetadata, FolderMetadata, Metadata
from dropbox.exceptions import ApiError


def _norm_path(p: str) -> str:
    """Normalize Dropbox paths."""
    p = (p or "").strip()
    if p in ("", "/"):
        return ""
    if not p.startswith("/"):
        p = "/" + p
    if p.endswith("/"):
        p = p[:-1]
    return p


@dataclass
class DropboxIO:
    dbx: dropbox.Dropbox

    @classmethod
    def from_env(cls) -> "DropboxIO":
        """Create a Dropbox client from environment variables.

        Required:
          - DROPBOX_REFRESH_TOKEN
          - DROPBOX_APP_KEY
          - DROPBOX_APP_SECRET
        Optional:
          - DROPBOX_TIMEOUT (seconds; default 60)
        """
        refresh = os.getenv("DROPBOX_REFRESH_TOKEN", "").strip()
        app_key = os.getenv("DROPBOX_APP_KEY", "").strip()
        app_secret = os.getenv("DROPBOX_APP_SECRET", "").strip()
        timeout_s = int(os.getenv("DROPBOX_TIMEOUT", "60").strip() or "60")

        if not refresh or not app_key or not app_secret:
            missing = [k for k in ["DROPBOX_REFRESH_TOKEN", "DROPBOX_APP_KEY", "DROPBOX_APP_SECRET"] if not os.getenv(k)]
            raise RuntimeError(f"Missing required Dropbox env var(s): {', '.join(missing)}")

        dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh,
            app_key=app_key,
            app_secret=app_secret,
            timeout=timeout_s,
        )
        return cls(dbx=dbx)

    def list_folder(self, path: str, recursive: bool = False) -> List[Metadata]:
        p = _norm_path(path)
        res = self.dbx.files_list_folder(p, recursive=recursive)
        items: List[Metadata] = list(res.entries)
        while res.has_more:
            res = self.dbx.files_list_folder_continue(res.cursor)
            items.extend(res.entries)
        return items

    def ensure_folder(self, path: str) -> None:
        p = _norm_path(path)
        if p == "":
            return
        try:
            self.dbx.files_create_folder_v2(p)
        except ApiError:
            # ignore exists/conflict
            pass

    def download(self, path: str) -> bytes:
        p = _norm_path(path)
        _, resp = self.dbx.files_download(p)
        return resp.content

    def upload(self, path: str, data: bytes, overwrite: bool = True) -> None:
        p = _norm_path(path)
        mode = dropbox.files.WriteMode.overwrite if overwrite else dropbox.files.WriteMode.add
        self.dbx.files_upload(data, p, mode=mode, mute=True)

    def move(self, src: str, dst: str, autorename: bool = True) -> None:
        s = _norm_path(src)
        d = _norm_path(dst)
        self.dbx.files_move_v2(
            s,
            d,
            autorename=autorename,
            allow_shared_folder=True,
            allow_ownership_transfer=False,
        )

    def copy(self, src: str, dst: str, autorename: bool = True) -> None:
        s = _norm_path(src)
        d = _norm_path(dst)
        self.dbx.files_copy_v2(
            s,
            d,
            autorename=autorename,
            allow_shared_folder=True,
            allow_ownership_transfer=False,
        )

    @staticmethod
    def is_file(md: Metadata) -> bool:
        return isinstance(md, FileMetadata)

    @staticmethod
    def is_folder(md: Metadata) -> bool:
        return isinstance(md, FolderMetadata)