# -*- coding: utf-8 -*-
"""
dropbox_io.py

DropboxIO wrapper used by the pipeline.

Goals:
- Provide a small, stable interface for the rest of the codebase.
- Keep backwards-compatible aliases (read_file_bytes / write_file_bytes).
- Add upload_file() (local file -> Dropbox) used for log uploads.
- Be resilient (pagination, common errors) and explicit in failures.

Env (either):
- Access token flow:
    DROPBOX_ACCESS_TOKEN
  or Refresh token flow:
    DROPBOX_REFRESH_TOKEN, DROPBOX_APP_KEY, DROPBOX_APP_SECRET
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import dropbox
from dropbox.files import (
    FileMetadata,
    FolderMetadata,
    WriteMode,
    UploadSessionCursor,
    CommitInfo,
)
from dropbox.exceptions import ApiError, AuthError


# Dropbox single-call upload limit is 150MB. Above that, use upload sessions.
_SIMPLE_UPLOAD_LIMIT = 150 * 1024 * 1024
_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB


@dataclass
class DropboxItem:
    name: str
    path: str
    path_lower: str
    rev: Optional[str] = None
    size: Optional[int] = None


class DropboxIO:
    def __init__(self, dbx: dropbox.Dropbox):
        self.dbx = dbx

    # -------------------------
    # Construction
    # -------------------------
    @classmethod
    def from_env(cls) -> "DropboxIO":
        """
        Prefers access token if provided; otherwise uses refresh token flow.
        """
        access_token = os.getenv("DROPBOX_ACCESS_TOKEN")
        refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
        app_key = os.getenv("DROPBOX_APP_KEY")
        app_secret = os.getenv("DROPBOX_APP_SECRET")

        if access_token:
            dbx = dropbox.Dropbox(access_token)
            _sanity_check(dbx)
            return cls(dbx)

        if not refresh_token:
            raise RuntimeError("DROPBOX_REFRESH_TOKEN or DROPBOX_ACCESS_TOKEN is required.")
        if not (app_key and app_secret):
            raise RuntimeError("DROPBOX_APP_KEY and DROPBOX_APP_SECRET are required for refresh token flow.")

        # Dropbox SDK supports oauth2_refresh_token directly.
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )
        _sanity_check(dbx)
        return cls(dbx)

    # -------------------------
    # Listing
    # -------------------------
    def list_folder(self, path: str, recursive: bool = False) -> List[DropboxItem]:
        """
        Returns files only (FolderMetadata is ignored).
        Paginates until done.
        """
        out: List[DropboxItem] = []
        try:
            res = self.dbx.files_list_folder(path, recursive=recursive)
            out.extend(_entries_to_items(res.entries))
            while res.has_more:
                res = self.dbx.files_list_folder_continue(res.cursor)
                out.extend(_entries_to_items(res.entries))
            return out
        except ApiError as e:
            # Re-raise with clearer message for common "path not found"
            raise RuntimeError(f"Dropbox list_folder failed for path={path!r}: {e}") from e

    # -------------------------
    # Download
    # -------------------------
    def download_to_bytes(self, path: str) -> bytes:
        try:
            _md, resp = self.dbx.files_download(path)
            return resp.content
        except ApiError as e:
            raise RuntimeError(f"Dropbox download failed for path={path!r}: {e}") from e

    def download_to_file(self, dropbox_path: str, local_path: str | Path) -> Path:
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        data = self.download_to_bytes(dropbox_path)
        local_path.write_bytes(data)
        return local_path

    # Backward/compat alias
    def read_file_bytes(self, path: str) -> bytes:
        return self.download_to_bytes(path)

    # -------------------------
    # Upload (bytes/text/json/local file)
    # -------------------------
    def upload_text(self, path: str, text: str, mode: str = "overwrite") -> None:
        self.upload_bytes(path, text.encode("utf-8"), mode=mode)

    def upload_json(self, path: str, obj, mode: str = "overwrite", ensure_ascii: bool = False) -> None:
        payload = json.dumps(obj, ensure_ascii=ensure_ascii, indent=2, sort_keys=True).encode("utf-8")
        self.upload_bytes(path, payload, mode=mode)

    def upload_bytes(self, path: str, data: bytes, mode: str = "overwrite") -> None:
        """
        Upload small binary payloads (in-memory).
        """
        wm = WriteMode.overwrite if mode == "overwrite" else WriteMode.add
        try:
            if len(data) <= _SIMPLE_UPLOAD_LIMIT:
                self.dbx.files_upload(data, path, mode=wm, mute=True)
                return
            # Large payload: use session
            self._upload_session(data, path, wm)
        except ApiError as e:
            raise RuntimeError(f"Dropbox upload_bytes failed for path={path!r}: {e}") from e

    def upload_file(self, local_path: str | Path, dropbox_path: str, mode: str = "overwrite") -> None:
        """
        Upload a local file to Dropbox (used for logs/artifacts).
        """
        lp = Path(local_path)
        if not lp.exists():
            raise RuntimeError(f"Local file not found: {str(lp)}")

        wm = WriteMode.overwrite if mode == "overwrite" else WriteMode.add
        size = lp.stat().st_size
        try:
            if size <= _SIMPLE_UPLOAD_LIMIT:
                with lp.open("rb") as f:
                    self.dbx.files_upload(f.read(), dropbox_path, mode=wm, mute=True)
                return

            # Large file: upload session streaming
            with lp.open("rb") as f:
                session_start = self.dbx.files_upload_session_start(f.read(_CHUNK_SIZE))
                cursor = UploadSessionCursor(session_id=session_start.session_id, offset=f.tell())

                while cursor.offset < size:
                    chunk = f.read(_CHUNK_SIZE)
                    if not chunk:
                        break
                    # Finish?
                    if cursor.offset + len(chunk) >= size:
                        commit = CommitInfo(path=dropbox_path, mode=wm, mute=True)
                        self.dbx.files_upload_session_finish(chunk, cursor, commit)
                        return
                    else:
                        self.dbx.files_upload_session_append_v2(chunk, cursor)
                        cursor.offset = f.tell()
        except ApiError as e:
            raise RuntimeError(f"Dropbox upload_file failed local={str(lp)!r} -> path={dropbox_path!r}: {e}") from e

    # Backward/compat alias
    def write_file_bytes(self, path: str, data: bytes, mode: str = "overwrite") -> None:
        self.upload_bytes(path, data, mode=mode)

    # -------------------------
    # Simple file ops
    # -------------------------
    def ensure_folder(self, path: str) -> None:
        """
        Create folder if missing. No-op if it exists.
        """
        try:
            self.dbx.files_get_metadata(path)
            return
        except ApiError:
            pass

        try:
            self.dbx.files_create_folder_v2(path)
        except ApiError as e:
            # Might already exist due to race; ignore that case.
            msg = str(e)
            if "conflict" in msg and "folder" in msg:
                return
            raise RuntimeError(f"Dropbox ensure_folder failed for path={path!r}: {e}") from e

    def move(self, from_path: str, to_path: str, autorename: bool = True) -> None:
        try:
            self.dbx.files_move_v2(from_path, to_path, autorename=autorename)
        except ApiError as e:
            raise RuntimeError(f"Dropbox move failed {from_path!r} -> {to_path!r}: {e}") from e

    def delete(self, path: str) -> None:
        try:
            self.dbx.files_delete_v2(path)
        except ApiError as e:
            raise RuntimeError(f"Dropbox delete failed for path={path!r}: {e}") from e

    # -------------------------
    # Internals
    # -------------------------
    def _upload_session(self, data: bytes, dropbox_path: str, wm: WriteMode) -> None:
        """
        Upload large in-memory bytes via upload sessions.
        """
        size = len(data)
        idx = 0

        start_chunk = data[:_CHUNK_SIZE]
        session_start = self.dbx.files_upload_session_start(start_chunk)
        idx += len(start_chunk)

        cursor = UploadSessionCursor(session_id=session_start.session_id, offset=idx)

        while idx < size:
            chunk = data[idx : idx + _CHUNK_SIZE]
            idx += len(chunk)

            if idx >= size:
                commit = CommitInfo(path=dropbox_path, mode=wm, mute=True)
                self.dbx.files_upload_session_finish(chunk, cursor, commit)
                return
            else:
                self.dbx.files_upload_session_append_v2(chunk, cursor)
                cursor.offset = idx


def _entries_to_items(entries) -> List[DropboxItem]:
    out: List[DropboxItem] = []
    for e in entries:
        if isinstance(e, FileMetadata):
            out.append(
                DropboxItem(
                    name=e.name,
                    path=e.path_display or e.path_lower or "",
                    path_lower=e.path_lower or "",
                    rev=getattr(e, "rev", None),
                    size=getattr(e, "size", None),
                )
            )
        elif isinstance(e, FolderMetadata):
            # ignore folders for now
            pass
    return out


def _sanity_check(dbx: dropbox.Dropbox) -> None:
    """
    Fail fast with a clearer error if auth is broken.
    """
    try:
        dbx.users_get_current_account()
    except AuthError as e:
        raise RuntimeError(f"Dropbox auth failed: {e}") from e