# -*- coding: utf-8 -*-
"""
dropbox_io.py
Thin wrapper around Dropbox SDK for:
- ensure_folder
- list_folder
- download/upload
- move (overwrite-safe, keyword-compat)
"""

from __future__ import annotations

from dataclasses import dataclass
import os
import re
import time
from typing import Optional

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import WriteMode, FolderMetadata


_SCHEME_PREFIXES = ("dbx:", "dropbox:")


def _strip_scheme(p: str) -> str:
    s = (p or "").strip()
    low = s.lower()
    for pref in _SCHEME_PREFIXES:
        if low.startswith(pref):
            return s[len(pref):].strip()
    # common accidents: "Dropbox:/path", "DBX:/path"
    if low.startswith("dropbox:/"):
        return s.split(":", 1)[1].strip()
    return s


def _collapse_slashes(p: str) -> str:
    # keep "id:..." untouched if ever passed (not expected in this project)
    if p.startswith("id:"):
        return p
    # collapse multiple slashes
    return re.sub(r"/{2,}", "/", p)


def _norm(p: str) -> str:
    p = _strip_scheme(p)
    p = p.replace("\\", "/")  # windows-style -> dropbox-style
    p = _collapse_slashes(p)
    p = p.strip()

    if not p:
        return "/"
    if not p.startswith("/"):
        p = "/" + p
    # avoid trailing slash except root
    if len(p) > 1 and p.endswith("/"):
        p = p[:-1]
    return p


def _parent_dir(path: str) -> str:
    path = _norm(path)
    if path == "/":
        return "/"
    if path.count("/") < 2:
        return "/"
    return "/" + "/".join(path.split("/")[:-1])


def _is_already_exists_error(e: ApiError) -> bool:
    # Dropbox SDK error typing varies; safest is to string-match common patterns.
    msg = repr(e)
    return ("conflict" in msg) or ("already_exists" in msg) or ("path/conflict" in msg)


@dataclass
class DropboxIO:
    dbx: dropbox.Dropbox

    @staticmethod
    def from_env() -> "DropboxIO":
        rt = os.getenv("DROPBOX_REFRESH_TOKEN", "").strip()
        app_key = os.getenv("DROPBOX_APP_KEY", "").strip()
        app_secret = os.getenv("DROPBOX_APP_SECRET", "").strip()

        if rt and app_key and app_secret:
            dbx = dropbox.Dropbox(
                oauth2_refresh_token=rt,
                app_key=app_key,
                app_secret=app_secret,
            )
            return DropboxIO(dbx=dbx)

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
            return
        except ApiError as e:
            # If it's already there, that's fine.
            if _is_already_exists_error(e):
                return
            # If it exists as folder, also fine.
            try:
                md = self.dbx.files_get_metadata(path)
                if isinstance(md, FolderMetadata):
                    return
            except Exception:
                pass
            # Anything else (including malformed_path) must surface.
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
        parent = _parent_dir(path)
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

    def delete_if_exists(self, path: str) -> None:
        path = _norm(path)
        try:
            self.dbx.files_delete_v2(path)
        except ApiError as e:
            msg = repr(e)
            if "not_found" in msg:
                return
            raise

    def move(self, src: str, dst: str, overwrite: bool = True, **_ignored) -> None:
        """
        Move src -> dst.

        - Accepts overwrite= keyword for compatibility.
        - If overwrite=True: delete dst first (best-effort) then move.
        - If overwrite=False: autorename=True (avoid collision).
        """
        src = _norm(src)
        dst = _norm(dst)

        self.ensure_folder(_parent_dir(dst))

        if overwrite:
            self.delete_if_exists(dst)
            self.dbx.files_move_v2(
                src,
                dst,
                autorename=False,
                allow_shared_folder=True,
                allow_ownership_transfer=False,
            )
        else:
            self.dbx.files_move_v2(
                src,
                dst,
                autorename=True,
                allow_shared_folder=True,
                allow_ownership_transfer=False,
            )