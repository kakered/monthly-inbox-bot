# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional, Union

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import FileMetadata, FolderMetadata

DbxMeta = Union[FileMetadata, FolderMetadata]


def _norm_path(p: str) -> str:
    """
    Dropbox API (App folder scope):
      - root is "" (empty string)
      - "/" should be treated as root
    """
    p = (p or "").strip()
    if p == "/" or p == "":
        return ""
    if not p.startswith("/"):
        p = "/" + p
    if len(p) > 1 and p.endswith("/"):
        p = p[:-1]
    return p


@dataclass
class DropboxIO:
    dbx: dropbox.Dropbox

    @classmethod
    def from_env(cls) -> "DropboxIO":
        app_key = os.environ.get("DROPBOX_APP_KEY", "").strip()
        app_secret = os.environ.get("DROPBOX_APP_SECRET", "").strip()
        refresh_token = os.environ.get("DROPBOX_REFRESH_TOKEN", "").strip()

        if not app_key or not app_secret or not refresh_token:
            raise RuntimeError(
                "Missing Dropbox env vars. Need DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN"
            )

        dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )
        return cls(dbx=dbx)

    def list_folder(self, path: str, recursive: bool = False) -> List[DbxMeta]:
        p = _norm_path(path)
        res = self.dbx.files_list_folder(p, recursive=recursive)
        entries: List[DbxMeta] = list(res.entries)
        while res.has_more:
            res = self.dbx.files_list_folder_continue(res.cursor)
            entries.extend(res.entries)
        return entries

    def ensure_folder(self, path: str) -> None:
        p = _norm_path(path)
        if p == "":
            return
        try:
            self.dbx.files_create_folder_v2(p)
        except ApiError as e:
            # already exists / conflict -> ignore
            if "conflict" in str(e).lower():
                return
            raise

    def download_to_bytes(self, path: str) -> bytes:
        p = _norm_path(path)
        _md, resp = self.dbx.files_download(p)
        return resp.content

    def upload_bytes(self, path: str, data: bytes, mode_overwrite: bool = True) -> None:
        p = _norm_path(path)
        mode = dropbox.files.WriteMode.overwrite if mode_overwrite else dropbox.files.WriteMode.add
        self.dbx.files_upload(data, p, mode=mode, mute=True)

    def move(self, src: str, dst: str, overwrite: bool = True) -> None:
        s = _norm_path(src)
        d = _norm_path(dst)
        # autorename=False に近い挙動を overwrite で制御したいがSDKのmoveは上書き指定がないため、
        # overwrite=Trueなら同名でもautorename=Falseで衝突→例外になり得る。
        # ここは「基本はユニーク名でdstを作る」設計で回避する。
        self.dbx.files_move_v2(s, d, autorename=not overwrite)

    def exists(self, path: str) -> bool:
        p = _norm_path(path)
        try:
            self.dbx.files_get_metadata(p)
            return True
        except ApiError:
            return False

    def read_json_bytes_or_none(self, path: str) -> Optional[bytes]:
        """
        ★ 今回のエラー原因のメソッド：monthly_pipeline_MULTISTAGE が呼びます
        """
        if not self.exists(path):
            return None
        return self.download_to_bytes(path)