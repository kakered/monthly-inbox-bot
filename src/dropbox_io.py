# -*- coding: utf-8 -*-
"""
dropbox_io.py
Dropbox I/O helper with:
- refresh token -> access token exchange
- list / download / upload / move
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import requests


@dataclass(frozen=True)
class DbxItem:
    path: str
    name: str
    id: Optional[str] = None
    client_modified: Optional[str] = None
    server_modified: Optional[str] = None
    size: Optional[int] = None


class DropboxIO:
    """
    Minimal Dropbox API v2 wrapper.
    Auth supports:
      (A) short-lived access token (DROPBOX_ACCESS_TOKEN)
      (B) refresh token flow (DROPBOX_REFRESH_TOKEN + APP_KEY + APP_SECRET) [recommended]
    """

    API = "https://api.dropboxapi.com/2"
    CONTENT = "https://content.dropboxapi.com/2"

    def __init__(
        self,
        access_token: str,
        *,
        timeout: int = 120,
        max_retries: int = 3,
        backoff: float = 1.5,
    ) -> None:
        self._token = access_token
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff = backoff

    @staticmethod
    def _must_env(name: str) -> str:
        v = os.getenv(name, "").strip()
        if not v:
            raise RuntimeError(f"Missing required env var: {name}")
        return v

    @classmethod
    def from_env(cls, *, timeout: int = 120, max_retries: int = 3) -> "DropboxIO":
        """
        Priority:
          1) DROPBOX_ACCESS_TOKEN (if set)
          2) Refresh token flow (DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET)
        """
        access = os.getenv("DROPBOX_ACCESS_TOKEN", "").strip()
        if access:
            return cls(access, timeout=timeout, max_retries=max_retries)

        refresh = os.getenv("DROPBOX_REFRESH_TOKEN", "").strip()
        if refresh:
            app_key = cls._must_env("DROPBOX_APP_KEY")
            app_secret = cls._must_env("DROPBOX_APP_SECRET")
            token = cls._exchange_refresh_token(refresh, app_key, app_secret, timeout=timeout)
            return cls(token, timeout=timeout, max_retries=max_retries)

        raise RuntimeError(
            "No Dropbox auth configured. Provide either "
            "DROPBOX_ACCESS_TOKEN or (DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET)."
        )

    @staticmethod
    def _exchange_refresh_token(refresh_token: str, app_key: str, app_secret: str, *, timeout: int = 120) -> str:
        url = "https://api.dropbox.com/oauth2/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        auth = (app_key, app_secret)
        r = requests.post(url, data=data, auth=auth, timeout=timeout)
        r.raise_for_status()
        j = r.json()
        token = j.get("access_token", "")
        if not token:
            raise RuntimeError(f"Failed to obtain access_token from refresh_token response: {j}")
        return token

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._token}"}

    def _post(self, url: str, *, json_body: Optional[dict] = None, data: Optional[bytes] = None, headers: Optional[dict] = None) -> requests.Response:
        h = {}
        h.update(self._headers())
        if headers:
            h.update(headers)

        last_err: Optional[Exception] = None
        for i in range(self._max_retries):
            try:
                r = requests.post(url, json=json_body, data=data, headers=h, timeout=self._timeout)
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep((self._backoff ** i) + 0.2)
                    continue
                r.raise_for_status()
                return r
            except Exception as e:
                last_err = e
                time.sleep((self._backoff ** i) + 0.2)
        raise RuntimeError(f"Dropbox request failed after retries: {url}") from last_err

    # ---------- list ----------
    def list_folder(self, path: str, *, recursive: bool = False) -> List[DbxItem]:
        url = f"{self.API}/files/list_folder"
        body = {"path": path, "recursive": recursive, "include_deleted": False, "include_mounted_folders": True}
        r = self._post(url, json_body=body)
        j = r.json()
        items = []
        for e in j.get("entries", []):
            if e.get(".tag") != "file":
                continue
            items.append(
                DbxItem(
                    path=e.get("path_lower") or e.get("path_display") or "",
                    name=e.get("name") or "",
                    id=e.get("id"),
                    client_modified=e.get("client_modified"),
                    server_modified=e.get("server_modified"),
                    size=e.get("size"),
                )
            )
        return items

    # ---------- download ----------
    def download(self, path: str) -> bytes:
        url = f"{self.CONTENT}/files/download"
        headers = {"Dropbox-API-Arg": json.dumps({"path": path})}
        r = self._post(url, headers=headers)
        return r.content

    # ---------- upload ----------
    def upload(self, path: str, content: bytes, *, mode: str = "overwrite") -> dict:
        url = f"{self.CONTENT}/files/upload"
        headers = {
            "Content-Type": "application/octet-stream",
            "Dropbox-API-Arg": json.dumps({"path": path, "mode": mode, "autorename": False, "mute": True}),
        }
        r = self._post(url, data=content, headers=headers)
        return r.json()

    # ---------- move ----------
    def move(self, from_path: str, to_path: str, *, autorename: bool = False) -> dict:
        url = f"{self.API}/files/move_v2"
        body = {"from_path": from_path, "to_path": to_path, "autorename": autorename, "allow_ownership_transfer": False}
        r = self._post(url, json_body=body)
        return r.json()

    # ---------- exists ----------
    def exists(self, path: str) -> bool:
        url = f"{self.API}/files/get_metadata"
        body = {"path": path, "include_media_info": False, "include_deleted": False, "include_has_explicit_shared_members": False}
        try:
            r = self._post(url, json_body=body)
            _ = r.json()
            return True
        except Exception:
            return False