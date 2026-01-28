# -*- coding: utf-8 -*-
"""
dropbox_io.py
Dropbox SDK の薄いラッパ。

今回の目的:
- move(overwrite=True) を受け付ける（既存dstがあれば削除してから移動）
- ensure_folder の malformed_path を潰す（空/"/" を作らない・連続"/"を潰す）
- write_file_bytes(overwrite=True) を安定化
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Dict, Any

import dropbox
from dropbox.files import WriteMode


def _norm_path(p: str) -> str:
    """Dropbox path を正規化。必ず '/' 始まり、連続'//'は潰す。空なら '/'。"""
    if p is None:
        return "/"
    p = str(p).strip()
    if not p:
        return "/"
    p = p.replace("\\", "/")
    while "//" in p:
        p = p.replace("//", "/")
    if not p.startswith("/"):
        p = "/" + p
    if len(p) > 1 and p.endswith("/"):
        p = p[:-1]
    return p


def _parent_dir(path: str) -> str:
    path = _norm_path(path)
    if path == "/":
        return "/"
    parent = os.path.dirname(path)
    parent = _norm_path(parent)
    return parent


@dataclass
class DropboxIO:
    dbx: dropbox.Dropbox

    def list_folder(self, path: str) -> Dict[str, Any]:
        path = _norm_path(path)
        res = self.dbx.files_list_folder(path)
        return res.to_dict()

    def ensure_folder(self, path: str) -> None:
        """
        Dropbox 上にフォルダがなければ作る。
        '/' や '' は何もしない。
        """
        path = _norm_path(path)
        if path == "/":
            return

        # まずmetadata取得を試み、なければ作成
        try:
            self.dbx.files_get_metadata(path)
            return
        except Exception:
            pass

        # 親を先に作る（再帰は浅く）
        parent = _parent_dir(path)
        if parent not in ("/", path):
            self.ensure_folder(parent)

        try:
            self.dbx.files_create_folder_v2(path)
        except dropbox.exceptions.ApiError as e:
            # 既にある/競合は無視
            # malformed_path はここに来ないように _norm_path で潰す
            _ = e
            return

    def exists(self, path: str) -> bool:
        path = _norm_path(path)
        try:
            self.dbx.files_get_metadata(path)
            return True
        except Exception:
            return False

    def delete(self, path: str) -> None:
        path = _norm_path(path)
        try:
            self.dbx.files_delete_v2(path)
        except Exception:
            return

    def write_file_bytes(self, path: str, content: bytes, *, overwrite: bool = True) -> None:
        path = _norm_path(path)
        self.ensure_folder(_parent_dir(path))
        mode = WriteMode.overwrite if overwrite else WriteMode.add
        self.dbx.files_upload(content, path, mode=mode, mute=True)

    def read_file_bytes(self, path: str) -> bytes:
        path = _norm_path(path)
        md, resp = self.dbx.files_download(path)
        _ = md
        return resp.content

    def move(self, src: str, dst: str, *, overwrite: bool = False) -> None:
        """
        overwrite=True の場合:
        - dst が存在したら削除してから move（Dropboxはmoveでの強制上書きが弱いので明示削除）
        """
        src = _norm_path(src)
        dst = _norm_path(dst)
        self.ensure_folder(_parent_dir(dst))

        if overwrite and self.exists(dst):
            self.delete(dst)

        # autorename は False（意図せず名前が変わるのを避ける）
        self.dbx.files_move_v2(src, dst, autorename=False)

    def copy(self, src: str, dst: str, *, overwrite: bool = False) -> None:
        src = _norm_path(src)
        dst = _norm_path(dst)
        self.ensure_folder(_parent_dir(dst))

        if overwrite and self.exists(dst):
            self.delete(dst)

        self.dbx.files_copy_v2(src, dst, autorename=False)