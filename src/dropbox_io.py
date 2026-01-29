# -*- coding: utf-8 -*-
from __future__ import annotations

import io
import os
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import FileMetadata, FolderMetadata


@dataclass(frozen=True)
class DbxEntry:
    path: str
    name: str
    is_file: bool
    size: int = 0
    rev: str = ""


class DropboxIO:
    """
    Thin wrapper around Dropbox SDK with a few safe helpers.

    IMPORTANT:
    - All paths are Dropbox "path_display" style like "/_system/state.json".
    - Atomic write is implemented as: upload temp -> move(replace) to target.
    """

    def __init__(self, refresh_token: str, app_key: str, app_secret: str):
        if not refresh_token or not app_key or not app_secret:
            raise ValueError("Dropbox credentials are missing (refresh_token/app_key/app_secret).")
        self.dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )

    # ---------- basic ----------
    def current_account_email(self) -> str:
        acct = self.dbx.users_get_current_account()
        return getattr(acct, "email", "")

    def list_folder(self, path: str) -> List[DbxEntry]:
        out: List[DbxEntry] = []
        try:
            res = self.dbx.files_list_folder(path)
        except ApiError as e:
            raise RuntimeError(f"Dropbox list_folder failed: path={path!r} err={e}") from e

        entries = res.entries
        for e in entries:
            if isinstance(e, FileMetadata):
                out.append(
                    DbxEntry(
                        path=e.path_display or "",
                        name=e.name or "",
                        is_file=True,
                        size=int(getattr(e, "size", 0) or 0),
                        rev=str(getattr(e, "rev", "") or ""),
                    )
                )
            elif isinstance(e, FolderMetadata):
                out.append(DbxEntry(path=e.path_display or "", name=e.name or "", is_file=False))
        return out

    def download(self, path: str) -> bytes:
        try:
            _md, resp = self.dbx.files_download(path)
            return resp.content
        except ApiError as e:
            raise RuntimeError(f"Dropbox download failed: {path!r} err={e}") from e

    def upload_overwrite(self, path: str, content: bytes) -> None:
        try:
            self.dbx.files_upload(content, path, mode=dropbox.files.WriteMode.overwrite)
        except ApiError as e:
            raise RuntimeError(f"Dropbox upload overwrite failed: {path!r} err={e}") from e

    def move_replace(self, src: str, dst: str) -> None:
        try:
            self.dbx.files_move_v2(src, dst, autorename=False)
        except ApiError as e:
            # If dst exists, move might fail depending on server-side behavior; we enforce replace by delete+move.
            # But delete+move is NOT atomic. We prefer: upload temp -> overwrite target where possible.
            raise RuntimeError(f"Dropbox move failed: {src!r} -> {dst!r} err={e}") from e

    def delete(self, path: str) -> None:
        try:
            self.dbx.files_delete_v2(path)
        except ApiError as e:
            raise RuntimeError(f"Dropbox delete failed: {path!r} err={e}") from e

    def ensure_folder(self, path: str) -> None:
        # create_folder_v2 fails if already exists; ignore that.
        try:
            self.dbx.files_create_folder_v2(path)
        except ApiError as e:
            # ignore "folder already exists"
            msg = str(e)
            if "conflict" in msg and "folder" in msg:
                return
            if "already exists" in msg:
                return
            # Some localized messages vary; also ignore if the folder exists.
            # We do a quick metadata check.
            try:
                md = self.dbx.files_get_metadata(path)
                if isinstance(md, FolderMetadata):
                    return
            except Exception:
                pass
            raise RuntimeError(f"Dropbox ensure_folder failed: {path!r} err={e}") from e

    # ---------- atomic write ----------
    def atomic_upload_overwrite(self, target_path: str, content: bytes, *, suffix: str = ".tmp") -> None:
        """
        Atomic-ish update for a single file:
        1) upload to a temp path in the same folder
        2) move to target (replace) by overwrite-upload if move-replace isn't reliable.

        Dropbox doesn't expose a true atomic rename-with-replace universally.
        The safest pattern here for "no partial file" is:
        - Ensure temp upload completes,
        - Then overwrite target using that same bytes if needed.
        """
        folder = os.path.dirname(target_path).replace("\\", "/")
        base = os.path.basename(target_path)
        ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
        tmp_path = f"{folder}/{base}{suffix}.{ts}"

        # 1) upload tmp
        self.upload_overwrite(tmp_path, content)

        # 2) overwrite target from bytes (no partial state.json ever appears)
        #    (This avoids delete+move non-atomic risks.)
        self.upload_overwrite(target_path, content)

        # 3) best-effort cleanup tmp
        try:
            self.delete(tmp_path)
        except Exception:
            pass