# -*- coding: utf-8 -*-
"""
utils_dropbox_item.py
Dropbox SDK(v11/v12)の返り値（FileMetadata/FolderMetadata 等）や
dict っぽい構造の両方を扱える小さなヘルパー群。

このファイルは「存在しない関数 import」で落ちるのを防ぐため、
is_file / get_path_lower などを必ず提供する。
"""

from __future__ import annotations

from typing import Any, Optional

try:
    import dropbox
    from dropbox import files as dbx_files
except Exception:
    dropbox = None
    dbx_files = None


def _get_attr(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def is_file(item: Any) -> bool:
    """DropboxのFileMetadata相当か？"""
    if item is None:
        return False
    if dbx_files is not None:
        try:
            return isinstance(item, dbx_files.FileMetadata)
        except Exception:
            pass
    # dict/未知型フォールバック
    tag = _get_attr(item, ".tag", None) or _get_attr(item, "tag", None)
    if tag == "file":
        return True
    # size / rev / client_modified があれば file っぽい
    return _get_attr(item, "size", None) is not None or _get_attr(item, "rev", None) is not None


def is_folder(item: Any) -> bool:
    """DropboxのFolderMetadata相当か？"""
    if item is None:
        return False
    if dbx_files is not None:
        try:
            return isinstance(item, dbx_files.FolderMetadata)
        except Exception:
            pass
    tag = _get_attr(item, ".tag", None) or _get_attr(item, "tag", None)
    return tag == "folder"


def get_path_lower(item: Any) -> str:
    """path_lower を安全に取り出す（無ければ空文字）"""
    return str(_get_attr(item, "path_lower", "") or "")


def get_path_display(item: Any) -> str:
    return str(_get_attr(item, "path_display", "") or "")


def get_name(item: Any) -> str:
    return str(_get_attr(item, "name", "") or "")


def get_size(item: Any) -> int:
    v = _get_attr(item, "size", 0)
    try:
        return int(v or 0)
    except Exception:
        return 0


def as_min_dict(item: Any) -> dict:
    """
    SDKオブジェクト/辞書のどちらでも、「よく使うキーだけ」を dict に寄せる。
    monthly_pipeline 側が entries を dict として扱うのを助ける。
    """
    if item is None:
        return {}

    if isinstance(item, dict):
        # 既に dict なら必要最低限だけ整形して返す
        out = dict(item)
        if "path_lower" not in out:
            out["path_lower"] = out.get("path_display", "")
        return out

    out = {
        "name": get_name(item),
        "path_lower": get_path_lower(item),
        "path_display": get_path_display(item),
    }
    tag = getattr(item, ".tag", None)
    if tag:
        out[".tag"] = tag

    # file っぽい情報
    for k in ["id", "rev", "size", "client_modified", "server_modified"]:
        v = getattr(item, k, None)
        if v is not None:
            out[k] = v

    return out