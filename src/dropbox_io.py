# -*- coding: utf-8 -*-
from __future__ import annotations
import json
from typing import Optional
import dropbox


class DropboxIO:
    def __init__(self, dbx: dropbox.Dropbox):
        self.dbx = dbx

    # -------- basic --------
    def list_folder(self, path: str):
        return self.dbx.files_list_folder(path).entries

    def download(self, path: str) -> bytes:
        _, res = self.dbx.files_download(path)
        return res.content

    def upload(self, path: str, data: bytes, overwrite: bool = True):
        mode = dropbox.files.WriteMode.overwrite if overwrite else dropbox.files.WriteMode.add
        self.dbx.files_upload(data, path, mode=mode)

    # -------- JSON helpers (NEW / REQUIRED) --------
    def read_json_bytes_or_none(self, path: str) -> Optional[bytes]:
        try:
            return self.download(path)
        except dropbox.exceptions.ApiError:
            return None

    def read_json_or_none(self, path: str):
        raw = self.read_json_bytes_or_none(path)
        if raw is None:
            return None
        return json.loads(raw.decode("utf-8"))

    def write_json(self, path: str, obj):
        data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self.upload(path, data, overwrite=True)