# -*- coding: utf-8 -*-
"""
dropbox_io.py

Dropbox I/O wrapper.

Auth (either):
- DROPBOX_ACCESS_TOKEN  (legacy)
- DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET (recommended)
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, List, Optional

import dropbox
from dropbox import files
from dropbox.exceptions import ApiError, AuthError


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def _norm_path(p: str) -> str:
    """
    Normalize Dropbox path:
    - "" or "/" -> "" (root)
    - otherwise ensure starts with "/"
    """
    p = (p or "").strip()
    if p in ("", "/"):
        return ""
    if not p.startswith("/"):
        p = "/" + p
    if len(p) > 1 and p.endswith("/"):
        p = p.rstrip("/")
    return p


@dataclass(frozen=True)
class DropboxItem:
    name: str
    path_display: str
    is_file: bool
    is_folder: bool

    @staticmethod
    def from_metadata(md: files.Metadata) -> "DropboxItem":
        name = getattr(md, "name", "") or ""
        path_display = getattr(md, "path_display", "") or getattr(md, "path_lower", "") or ""
        is_file = isinstance(md, files.FileMetadata)
        is_folder = isinstance(md, files.FolderMetadata)
        return DropboxItem(name=name, path_display=path_display, is_file=is_file, is_folder=is_folder)


class DropboxIO:
    def __init__(
        self,
        *,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        app_key: Optional[str] = None,
        app_secret: Optional[str] = None,
        timeout: int = 120,
    ) -> None:
        self.timeout = int(timeout)

        if access_token:
            self.dbx = dropbox.Dropbox(oauth2_access_token=access_token, timeout=self.timeout)
        elif refresh_token and app_key and app_secret:
            self.dbx = dropbox.Dropbox(
                oauth2_refresh_token=refresh_token,
                app_key=app_key,
                app_secret=app_secret,
                timeout=self.timeout,
            )
        else:
            raise RuntimeError(
                "Dropbox auth not configured. Set either:\n"
                "  - DROPBOX_ACCESS_TOKEN\n"
                "  OR\n"
                "  - DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET"
            )

        try:
            self.dbx.users_get_current_account()
        except AuthError as e:
            raise RuntimeError(f"Dropbox auth failed: {e!r}") from e

    @classmethod
    def from_env(cls) -> "DropboxIO":
        access = _env("DROPBOX_ACCESS_TOKEN")
        refresh = _env("DROPBOX_REFRESH_TOKEN")
        app_key = _env("DROPBOX_APP_KEY")
        app_secret = _env("DROPBOX_APP_SECRET")
        timeout = int(_env("OPENAI_TIMEOUT") or "120")

        if access:
            return cls(access_token=access, timeout=timeout)

        if refresh and app_key and app_secret:
            return cls(refresh_token=refresh, app_key=app_key, app_secret=app_secret, timeout=timeout)

        raise RuntimeError(
            "Missing Dropbox env vars. Provide either:\n"
            "  DROPBOX_ACCESS_TOKEN\n"
            "or\n"
            "  DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET"
        )

    # ---------- folders ----------
    def ensure_folder(self, path: str) -> None:
        p = _norm_path(path)
        if p == "":
            return
        try:
            self.dbx.files_create_folder_v2(p)
        except ApiError as e:
            # ignore "already exists"
            try:
                if e.error.is_path() and e.error.get_path().is_conflict():
                    return
            except Exception:
                pass
            raise

    def list_folder(self, path: str, recursive: bool = False) -> List[DropboxItem]:
        p = _norm_path(path)
        res = self.dbx.files_list_folder(p, recursive=recursive)
        entries = list(res.entries)
        while res.has_more:
            res = self.dbx.files_list_folder_continue(res.cursor)
            entries.extend(res.entries)
        return [DropboxItem.from_metadata(md) for md in entries]

    # ---------- read/write ----------
    def read_bytes_or_none(self, path: str) -> Optional[bytes]:
        p = _norm_path(path)
        try:
            _meta, resp = self.dbx.files_download(p)
            return resp.content
        except ApiError as e:
            try:
                if e.error.is_path() and e.error.get_path().is_not_found():
                    return None
            except Exception:
                pass
            raise

    def read_bytes(self, path: str) -> bytes:
        b = self.read_bytes_or_none(path)
        if b is None:
            raise FileNotFoundError(path)
        return b

    def write_bytes(self, path: str, data: bytes, *, overwrite: bool = True) -> None:
        p = _norm_path(path)
        mode = files.WriteMode.overwrite if overwrite else files.WriteMode.add
        self.dbx.files_upload(data, p, mode=mode)

    def move(self, src: str, dst: str, *, overwrite: bool = True) -> None:
        s = _norm_path(src)
        d = _norm_path(dst)
        self.dbx.files_move_v2(s, d, autorename=(not overwrite))

    # ---------- json ----------
    def read_json_or_none(self, path: str) -> Optional[Any]:
        raw = self.read_bytes_or_none(path)
        if raw is None:
            return None
        return json.loads(raw.decode("utf-8"))

    def write_json(self, path: str, obj: Any, *, overwrite: bool = True) -> None:
        data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self.write_bytes(path, data, overwrite=overwrite)