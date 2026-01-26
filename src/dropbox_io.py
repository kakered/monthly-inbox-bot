# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Union
import json
import os

import dropbox
from dropbox.files import FileMetadata, FolderMetadata, DeletedMetadata


@dataclass
class DropboxItem:
    """
    Dropbox SDK の metadata を「dictっぽく」扱える薄いラッパー。
    既存コードが item.get("name") のように触っても落ちないようにする。
    """
    raw: Any

    def to_dict(self) -> Dict[str, Any]:
        if hasattr(self.raw, "to_dict"):
            return self.raw.to_dict()
        # 念のため
        return {"_raw": repr(self.raw)}

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    @property
    def name(self) -> Optional[str]:
        return self.get("name")

    @property
    def path_lower(self) -> Optional[str]:
        return self.get("path_lower")

    @property
    def path_display(self) -> Optional[str]:
        return self.get("path_display")

    @property
    def is_folder(self) -> bool:
        return isinstance(self.raw, FolderMetadata) or self.get(".tag") == "folder"

    @property
    def is_file(self) -> bool:
        return isinstance(self.raw, FileMetadata) or self.get(".tag") == "file"


class DropboxIO:
    def __init__(
        self,
        refresh_token: str,
        app_key: str,
        app_secret: str,
        timeout: int = 120,
    ) -> None:
        self.dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
            timeout=timeout,
        )

    # -------- path helpers --------
    @staticmethod
    def _norm_path(path: str) -> str:
        """
        Dropbox SDK は path を `""` または `"/xxx"` 形式で受ける。
        余計な空白・引用符・末尾スラッシュ等を正規化。
        """
        if path is None:
            return ""
        p = str(path).strip()

        # ありがちな事故対策
        if p in {"***", "<***>", '""', "''"}:
            return p  # 上位で検出してエラーにしたいのでそのまま返す

        # 引用符除去
        if (p.startswith('"') and p.endswith('"')) or (p.startswith("'") and p.endswith("'")):
            p = p[1:-1].strip()

        # Dropbox root は "" が正
        if p == "/":
            return ""

        # 先頭は "/" に寄せる（空はOK）
        if p != "" and not p.startswith("/"):
            p = "/" + p

        # 末尾スラッシュは落とす（ただし "/" 単体は上で処理済み）
        if len(p) > 1 and p.endswith("/"):
            p = p[:-1]

        return p

    # -------- core --------
    def list_folder(self, path: str, recursive: bool = False) -> List[DropboxItem]:
        p = self._norm_path(path)
        if p == "***":
            raise RuntimeError(
                "Invalid Dropbox path: got '***'. "
                "GitHub Secrets (MONTHLY_INBOX_PATH etc.) are likely still placeholders."
            )

        res = self.dbx.files_list_folder(p, recursive=recursive)
        out: List[DropboxItem] = [DropboxItem(e) for e in res.entries]

        while res.has_more:
            res = self.dbx.files_list_folder_continue(res.cursor)
            out.extend(DropboxItem(e) for e in res.entries)

        return out

    def folder_exists(self, path: str) -> bool:
        p = self._norm_path(path)
        try:
            md = self.dbx.files_get_metadata(p)
            return isinstance(md, FolderMetadata)
        except Exception:
            return False

    def ensure_folder(self, path: str) -> None:
        p = self._norm_path(path)
        if p in {"", "/"}:
            return
        if self.folder_exists(p):
            return
        self.dbx.files_create_folder_v2(p)

    def download(self, path: str) -> bytes:
        p = self._norm_path(path)
        md, resp = self.dbx.files_download(p)
        return resp.content

    def upload(self, path: str, content: bytes, overwrite: bool = True) -> None:
        p = self._norm_path(path)
        mode = dropbox.files.WriteMode.overwrite if overwrite else dropbox.files.WriteMode.add
        self.dbx.files_upload(content, p, mode=mode, mute=True)

    def move(self, src: str, dst: str, overwrite: bool = True) -> None:
        s = self._norm_path(src)
        d = self._norm_path(dst)
        self.dbx.files_move_v2(s, d, autorename=not overwrite)

    def delete(self, path: str) -> None:
        p = self._norm_path(path)
        self.dbx.files_delete_v2(p)

    # -------- convenience --------
    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.download(path).decode(encoding, errors="replace")

    def write_text(self, path: str, text: str, encoding: str = "utf-8") -> None:
        self.upload(path, text.encode(encoding))

    def read_json(self, path: str) -> Any:
        return json.loads(self.read_text(path))

    def write_json(self, path: str, obj: Any, indent: int = 2) -> None:
        self.write_text(path, json.dumps(obj, ensure_ascii=False, indent=indent) + "\n")


def make_dropbox_io_from_env() -> DropboxIO:
    """
    既存コードが環境変数から DropboxIO を作る場合のために用意。
    """
    refresh_token = os.environ["DROPBOX_REFRESH_TOKEN"]
    app_key = os.environ["DROPBOX_APP_KEY"]
    app_secret = os.environ["DROPBOX_APP_SECRET"]
    timeout = int(os.getenv("DROPBOX_TIMEOUT", "120"))
    return DropboxIO(
        refresh_token=refresh_token,
        app_key=app_key,
        app_secret=app_secret,
        timeout=timeout,
    )