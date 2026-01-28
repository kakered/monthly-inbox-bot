# -*- coding: utf-8 -*-
"""
dropbox_io.py
Dropbox I/O wrapper.

- Dropbox SDK v12 系で ListFolderResult.to_dict() が無い問題に対応
- move(overwrite=...) の引数を受けて monthly_pipeline から呼べるようにする
- ensure_folder("/") や "" で malformed_path になるのを回避
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import dropbox
from dropbox.exceptions import ApiError

from .utils_dropbox_item import as_min_dict, is_file, is_folder, get_path_lower


def _parent_dir(path: str) -> str:
    path = (path or "").rstrip("/")
    if not path or path == "/":
        return "/"
    i = path.rfind("/")
    if i <= 0:
        return "/"
    return path[:i]


class DropboxIO:
    def __init__(self, refresh_token: str, app_key: str, app_secret: str):
        self.dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )

    # ---------- folder helpers ----------
    def ensure_folder(self, path: str) -> None:
        """
        フォルダが無ければ作る。Dropboxは "/" や "" を create_folder できないので弾く。
        """
        path = (path or "").strip()
        if not path or path == "/":
            return
        if not path.startswith("/"):
            # malformed_path を避ける
            path = "/" + path

        try:
            self.dbx.files_create_folder_v2(path)
        except ApiError as e:
            # already exists は無視
            try:
                err = e.error
                # path/conflict/folder のようなケースをざっくり吸収
                if "conflict" in str(err).lower():
                    return
            except Exception:
                pass
            # それ以外は再raise
            raise

    # ---------- list ----------
    def list_folder(self, path: str) -> Dict[str, Any]:
        """
        monthly_pipeline 側が res.get("entries") を期待しているので dict で返す。
        Dropbox SDK v12 では res.to_dict() が無いので自前で整形する。
        """
        if not path.startswith("/"):
            path = "/" + path

        res = self.dbx.files_list_folder(path)
        entries = []
        for it in getattr(res, "entries", []) or []:
            entries.append(as_min_dict(it))

        out = {
            "entries": entries,
            "cursor": getattr(res, "cursor", None),
            "has_more": bool(getattr(res, "has_more", False)),
        }
        return out

    # ---------- read/write ----------
    def read_file_bytes(self, path: str) -> bytes:
        if not path.startswith("/"):
            path = "/" + path
        md, resp = self.dbx.files_download(path)
        return resp.content

    def write_file_bytes(self, path: str, data: bytes, overwrite: bool = True) -> None:
        if not path.startswith("/"):
            path = "/" + path

        self.ensure_folder(_parent_dir(path))

        mode = dropbox.files.WriteMode.overwrite if overwrite else dropbox.files.WriteMode.add
        self.dbx.files_upload(data, path, mode=mode, mute=True)

    # ---------- move/copy ----------
    def delete(self, path: str) -> None:
        if not path.startswith("/"):
            path = "/" + path
        try:
            self.dbx.files_delete_v2(path)
        except ApiError as e:
            # not_found は無視
            if "not_found" in str(e).lower():
                return
            raise

    def move(self, src_path: str, dst_path: str, overwrite: bool = False) -> None:
        """
        monthly_pipeline から overwrite=... で呼ばれても落ちないようにする。
        Dropbox API は overwrite という引数を持たないので、
        overwrite=True の場合は dst を消してから move する。
        """
        if not src_path.startswith("/"):
            src_path = "/" + src_path
        if not dst_path.startswith("/"):
            dst_path = "/" + dst_path

        self.ensure_folder(_parent_dir(dst_path))

        if overwrite:
            self.delete(dst_path)

        # autorename=False で「同名があればエラー」にして挙動を明確化
        self.dbx.files_move_v2(src_path, dst_path, autorename=False)

    def copy(self, src_path: str, dst_path: str, overwrite: bool = False) -> None:
        if not src_path.startswith("/"):
            src_path = "/" + src_path
        if not dst_path.startswith("/"):
            dst_path = "/" + dst_path

        self.ensure_folder(_parent_dir(dst_path))

        if overwrite:
            self.delete(dst_path)

        self.dbx.files_copy_v2(src_path, dst_path, autorename=False)