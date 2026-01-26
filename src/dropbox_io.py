# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import dropbox
from dropbox.files import FileMetadata, FolderMetadata, WriteMode


@dataclass
class DropboxItem:
    path: str
    name: str
    is_file: bool
    is_folder: bool
    size: int = 0


class DropboxIO:
    """
    Thin wrapper around Dropbox SDK with the methods the pipeline expects.
    """

    def __init__(self, access_token: str) -> None:
        self._dbx = dropbox.Dropbox(oauth2_access_token=access_token)

    # ---------- basic filesystem ----------
    def list_folder(self, path: str) -> List[DropboxItem]:
        path = self._norm(path)
        res = self._dbx.files_list_folder(path)
        items: List[DropboxItem] = []
        for e in res.entries:
            if isinstance(e, FileMetadata):
                items.append(DropboxItem(path=e.path_lower or e.path_display or "", name=e.name, is_file=True, is_folder=False, size=int(e.size)))
            elif isinstance(e, FolderMetadata):
                items.append(DropboxItem(path=e.path_lower or e.path_display or "", name=e.name, is_file=False, is_folder=True, size=0))
        return items

    def ensure_folder(self, path: str) -> None:
        path = self._norm(path)
        if path in ("", "/"):
            return
        try:
            self._dbx.files_get_metadata(path)
            return
        except Exception:
            pass
        # create (recursive-ish)
        parts = [p for p in path.split("/") if p]
        cur = ""
        for p in parts:
            cur = cur + "/" + p
            try:
                self._dbx.files_get_metadata(cur)
            except Exception:
                try:
                    self._dbx.files_create_folder_v2(cur)
                except Exception:
                    # ignore race
                    pass

    def exists(self, path: str) -> bool:
        path = self._norm(path)
        try:
            self._dbx.files_get_metadata(path)
            return True
        except Exception:
            return False

    def move(self, src: str, dst: str, overwrite: bool = True) -> None:
        src = self._norm(src)
        dst = self._norm(dst)
        self.ensure_folder(os.path.dirname(dst))
        if overwrite and self.exists(dst):
            try:
                self._dbx.files_delete_v2(dst)
            except Exception:
                pass
        self._dbx.files_move_v2(src, dst)

    def copy(self, src: str, dst: str, overwrite: bool = True) -> None:
        src = self._norm(src)
        dst = self._norm(dst)
        self.ensure_folder(os.path.dirname(dst))
        if overwrite and self.exists(dst):
            try:
                self._dbx.files_delete_v2(dst)
            except Exception:
                pass
        self._dbx.files_copy_v2(src, dst)

    # ---------- bytes ----------
    def read_bytes_or_none(self, path: str) -> Optional[bytes]:
        path = self._norm(path)
        try:
            md, resp = self._dbx.files_download(path)
            return resp.content
        except Exception:
            return None

    def write_bytes(self, path: str, data: bytes, mode: str = "overwrite") -> None:
        path = self._norm(path)
        self.ensure_folder(os.path.dirname(path))
        wm = WriteMode.overwrite if mode == "overwrite" else WriteMode.add
        self._dbx.files_upload(data, path, mode=wm, mute=True)

    # ---------- helpers ----------
    @staticmethod
    def _norm(p: str) -> str:
        if p is None:
            return ""
        p = p.strip()
        if p == "":
            return ""
        if not p.startswith("/"):
            p = "/" + p
        # remove trailing slash (except root)
        if len(p) > 1 and p.endswith("/"):
            p = p[:-1]
        return p