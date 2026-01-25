# -*- coding: utf-8 -*-
"""
dropbox_io.py

DropboxIO wrapper used by the pipeline.

Goals:
- Provide a thin, stable interface around Dropbox SDK
- Support both Access Token and Refresh Token flows
- Provide helpers used across pipeline (list/download/upload, ensure_folder, exists)
- Keep behavior explicit and log-friendly

Env (either):
- Access token flow:
    DROPBOX_ACCESS_TOKEN
- Refresh token flow:
    DROPBOX_REFRESH_TOKEN
    DROPBOX_APP_KEY
    DROPBOX_APP_SECRET
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Union

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import (
    FileMetadata,
    FolderMetadata,
    WriteMode,
)


@dataclass
class DropboxItem:
    name: str
    path: str
    path_lower: str
    is_file: bool
    is_folder: bool
    rev: Optional[str] = None
    size: Optional[int] = None


class DropboxIO:
    def __init__(self, dbx: dropbox.Dropbox):
        self.dbx = dbx

    # ---------------------------
    # Auth / Factory
    # ---------------------------
    @classmethod
    def from_env(cls) -> "DropboxIO":
        """
        Build Dropbox client from environment variables.

        Priority:
        1) DROPBOX_ACCESS_TOKEN (simple)
        2) DROPBOX_REFRESH_TOKEN + (DROPBOX_APP_KEY, DROPBOX_APP_SECRET)
        """
        access_token = os.getenv("DROPBOX_ACCESS_TOKEN")
        refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN") or os.getenv("DROPBOX_REFRESH_TOKEN".replace("_TOKEN", ""))  # no-op fallback
        app_key = os.getenv("DROPBOX_APP_KEY")
        app_secret = os.getenv("DROPBOX_APP_SECRET")

        if access_token:
            return cls(dropbox.Dropbox(access_token))

        if not refresh_token:
            raise RuntimeError("DROPBOX_REFRESH_TOKEN or DROPBOX_ACCESS_TOKEN is required.")
        if not (app_key and app_secret):
            raise RuntimeError("DROPBOX_APP_KEY and DROPBOX_APP_SECRET are required for refresh token flow.")

        # Dropbox SDK supports refresh token via oauth2_refresh_token
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )
        return cls(dbx)

    # ---------------------------
    # Folder utilities
    # ---------------------------
    def ensure_folder(self, path: str) -> None:
        """
        Create folder if missing. No error if it already exists.
        """
        try:
            self.dbx.files_create_folder_v2(path)
        except ApiError as e:
            # folder already exists
            if getattr(e.error, "is_path", lambda: False)() and e.error.get_path().is_conflict():
                return
            raise

    def exists(self, path: str) -> bool:
        """
        Return True if file/folder exists.
        """
        try:
            self.dbx.files_get_metadata(path)
            return True
        except ApiError as e:
            # not found
            if getattr(e.error, "is_path", lambda: False)() and e.error.get_path().is_not_found():
                return False
            raise

    # ---------------------------
    # Listing
    # ---------------------------
    def list_folder(self, path: str, recursive: bool = False, include_folders: bool = False) -> List[DropboxItem]:
        """
        List items in folder. Returns files by default (include_folders=False).
        """
        out: List[DropboxItem] = []
        try:
            res = self.dbx.files_list_folder(path, recursive=recursive)
        except ApiError as e:
            # helpful explicit error for not-found
            if getattr(e.error, "is_path", lambda: False)() and e.error.get_path().is_not_found():
                raise RuntimeError(f"Dropbox folder not found: {path}") from e
            raise

        def _append_entries(entries: Iterable[object]) -> None:
            for e in entries:
                if isinstance(e, FileMetadata):
                    out.append(
                        DropboxItem(
                            name=e.name,
                            path=e.path_display,
                            path_lower=e.path_lower,
                            is_file=True,
                            is_folder=False,
                            rev=e.rev,
                            size=getattr(e, "size", None),
                        )
                    )
                elif include_folders and isinstance(e, FolderMetadata):
                    out.append(
                        DropboxItem(
                            name=e.name,
                            path=e.path_display,
                            path_lower=e.path_lower,
                            is_file=False,
                            is_folder=True,
                            rev=None,
                            size=None,
                        )
                    )

        _append_entries(res.entries)
        while res.has_more:
            res = self.dbx.files_list_folder_continue(res.cursor)
            _append_entries(res.entries)

        return out

    # ---------------------------
    # Download
    # ---------------------------
    def download_to_bytes(self, path: str) -> bytes:
        """
        Download file content into bytes.
        """
        md, resp = self.dbx.files_download(path)
        return resp.content

    def download_to_file(self, dropbox_path: str, local_path: Union[str, Path]) -> Path:
        """
        Download file to local path (overwrite).
        """
        local_path = Path(local_path)
        data = self.download_to_bytes(dropbox_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(data)
        return local_path

    # Backward/compat aliases (used by some components)
    def read_file_bytes(self, path: str) -> bytes:
        return self.download_to_bytes(path)

    # ---------------------------
    # Upload
    # ---------------------------
    def upload_bytes(self, dropbox_path: str, data: bytes, mode: str = "overwrite") -> None:
        """
        Upload bytes to Dropbox path.
        mode: "overwrite" | "add"
        """
        wm = WriteMode.overwrite if mode == "overwrite" else WriteMode.add
        self.dbx.files_upload(data, dropbox_path, mode=wm, mute=True)

    def upload_text(self, dropbox_path: str, text: str, mode: str = "overwrite") -> None:
        self.upload_bytes(dropbox_path, text.encode("utf-8"), mode=mode)

    def upload_file(self, local_path: Union[str, Path], dropbox_path: str, mode: str = "overwrite") -> None:
        """
        Upload a local file to Dropbox.
        """
        local_path = Path(local_path)
        data = local_path.read_bytes()
        self.upload_bytes(dropbox_path, data, mode=mode)

    # Backward/compat aliases (used by StateStore etc.)
    def write_file_bytes(self, path: str, data: bytes, mode: str = "overwrite") -> None:
        self.upload_bytes(path, data, mode=mode)

    # ---------------------------
    # Optional helpers
    # ---------------------------
    def delete(self, path: str) -> None:
        self.dbx.files_delete_v2(path)

    def move(self, from_path: str, to_path: str, autorename: bool = True) -> None:
        self.dbx.files_move_v2(from_path, to_path, autorename=autorename)

    def copy(self, from_path: str, to_path: str, autorename: bool = True) -> None:
        self.dbx.files_copy_v2(from_path, to_path, autorename=autorename)