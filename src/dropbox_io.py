# -*- coding: utf-8 -*-
"""
dropbox_io.py

Dropbox helper:
- Prefer OAuth refresh token (DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET)
- Fallback to DROPBOX_ACCESS_TOKEN if provided (optional)

This file must NOT contain merge-conflict markers.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import dropbox
from dropbox.files import FileMetadata, FolderMetadata


def _must_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _norm_path(p: str) -> str:
    p = (p or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    # Dropbox treats '//' oddly; normalize a bit
    while "//" in p:
        p = p.replace("//", "/")
    return p


@dataclass
class DropboxIO:
    dbx: dropbox.Dropbox

    @staticmethod
    def from_env() -> "DropboxIO":
        # Prefer refresh token method (recommended)
        refresh = os.getenv("DROPBOX_REFRESH_TOKEN", "").strip()
        if refresh:
            app_key = _must_env("DROPBOX_APP_KEY").strip()
            app_secret = _must_env("DROPBOX_APP_SECRET").strip()
            dbx = dropbox.Dropbox(
                oauth2_refresh_token=refresh,
                app_key=app_key,
                app_secret=app_secret,
                timeout=120,
            )
            return DropboxIO(dbx=dbx)

        # Optional: access token
        access = os.getenv("DROPBOX_ACCESS_TOKEN", "").strip()
        if access:
            dbx = dropbox.Dropbox(oauth2_access_token=access, timeout=120)
            return DropboxIO(dbx=dbx)

        raise RuntimeError(
            "Missing Dropbox credentials. Provide either "
            "(DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET) "
            "or DROPBOX_ACCESS_TOKEN."
        )

    def ensure_folder(self, path: str) -> None:
        path = _norm_path(path)
        if path == "/":
            return
        try:
            self.dbx.files_get_metadata(path)
            return
        except dropbox.exceptions.ApiError:
            pass

        # Create recursively
        parts = [p for p in path.split("/") if p]
        cur = ""
        for part in parts:
            cur = _norm_path(cur + "/" + part)
            try:
                self.dbx.files_get_metadata(cur)
            except dropbox.exceptions.ApiError:
                try:
                    self.dbx.files_create_folder_v2(cur)
                except dropbox.exceptions.ApiError:
                    # race / already exists
                    pass

    def list_files(self, folder: str) -> List[FileMetadata]:
        folder = _norm_path(folder)
        out: List[FileMetadata] = []
        res = self.dbx.files_list_folder(folder)
        while True:
            for e in res.entries:
                if isinstance(e, FileMetadata):
                    out.append(e)
            if not res.has_more:
                break
            res = self.dbx.files_list_folder_continue(res.cursor)
        return out

    def download(self, path: str) -> bytes:
        path = _norm_path(path)
        md, resp = self.dbx.files_download(path)
        return resp.content

    def upload(self, path: str, data: bytes, overwrite: bool = True) -> None:
        path = _norm_path(path)
        mode = dropbox.files.WriteMode.overwrite if overwrite else dropbox.files.WriteMode.add
        self.dbx.files_upload(data, path, mode=mode, mute=True)

    def move(self, src: str, dst: str, overwrite: bool = True) -> None:
        src = _norm_path(src)
        dst = _norm_path(dst)
        try:
            self.dbx.files_move_v2(
                src,
                dst,
                autorename=False,
                allow_shared_folder=True,
                allow_ownership_transfer=False,
            )
        except dropbox.exceptions.ApiError:
            if not overwrite:
                raise
            # Overwrite by deleting dst then moving
            try:
                self.dbx.files_delete_v2(dst)
            except dropbox.exceptions.ApiError:
                pass
            self.dbx.files_move_v2(
                src,
                dst,
                autorename=False,
                allow_shared_folder=True,
                allow_shared_folder=True,
            )