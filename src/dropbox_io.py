# -*- coding: utf-8 -*-
"""
dropbox_io.py
Lightweight Dropbox wrapper used by monthly-inbox-bot.

Notes
- Dropbox SDK v12+ objects (e.g., ListFolderResult) do NOT provide .to_dict().
- This wrapper returns plain Python types (list/bytes/bool) to keep the rest of
  the pipeline stable and testable.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

import dropbox
from dropbox.files import WriteMode

from .utils_dropbox_item import is_file, get_path_lower


def _parent_dir(path: str) -> str:
    path = (path or "").rstrip("/")
    if not path or path == "/":
        return ""
    parts = path.split("/")
    if len(parts) <= 2:
        return ""
    return "/".join(parts[:-1]) or ""


@dataclass
class DropboxIO:
    dbx: dropbox.Dropbox

    # ---------- folder / metadata ----------
    def ensure_folder(self, path: str) -> None:
        """
        Create folder if missing.
        Safe for '', '/'.
        """
        if not path or path == "/":
            return
        try:
            self.dbx.files_get_metadata(path)
            return
        except dropbox.exceptions.ApiError:
            try:
                self.dbx.files_create_folder_v2(path)
            except dropbox.exceptions.ApiError:
                # If already created by a race, ignore; otherwise re-raise by re-checking
                self.dbx.files_get_metadata(path)
                return

    def exists(self, path: str) -> bool:
        try:
            self.dbx.files_get_metadata(path)
            return True
        except dropbox.exceptions.ApiError:
            return False

    def list_folder(self, folder_path: str) -> List[dropbox.files.Metadata]:
        """
        Returns a list of Dropbox SDK Metadata objects.
        """
        res = self.dbx.files_list_folder(folder_path)
        out: List[dropbox.files.Metadata] = []
        out.extend(res.entries)
        while res.has_more:
            res = self.dbx.files_list_folder_continue(res.cursor)
            out.extend(res.entries)
        return out

    def list_folder_files(self, folder_path: str) -> List[str]:
        """
        Convenience: returns lower paths for file entries only.
        """
        items = self.list_folder(folder_path)
        return [get_path_lower(x) for x in items if is_file(x)]

    # ---------- data I/O ----------
    def read_file_bytes(self, path: str) -> bytes:
        _md, resp = self.dbx.files_download(path)
        return resp.content

    def write_file_bytes(self, path: str, data: bytes, overwrite: bool = True) -> None:
        parent = _parent_dir(path)
        if parent:
            self.ensure_folder(parent)
        mode = WriteMode.overwrite if overwrite else WriteMode.add
        self.dbx.files_upload(data, path, mode=mode)

    # ---------- delete / move ----------
    def delete(self, path: str) -> None:
        try:
            self.dbx.files_delete_v2(path)
        except dropbox.exceptions.ApiError:
            pass

    def move(self, src_path: str, dst_path: str, overwrite: bool = True) -> None:
        """
        Dropbox move has no 'overwrite' flag; implement overwrite by deleting destination first.
        """
        if overwrite and self.exists(dst_path):
            self.delete(dst_path)
        self.dbx.files_move_v2(src_path, dst_path, autorename=False, allow_shared_folder=True)