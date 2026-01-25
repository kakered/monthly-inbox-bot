# -*- coding: utf-8 -*-
"""
Dropbox list_folder entries can be:
- dict-like (our own)
- dropbox.files.Metadata (SDK objects)
- our own DropboxItem dataclass/object

This helper normalizes field access safely.
"""

from __future__ import annotations
from typing import Any, Optional


def dbx_get(item: Any, key: str, default: Any = None) -> Any:
    """
    Safe getter:
    - dict: item.get(key)
    - object: getattr(item, key)
    - nested 'metadata' attribute: getattr(item.metadata, key)
    """
    if item is None:
        return default

    # dict-like
    if isinstance(item, dict):
        return item.get(key, default)

    # direct attribute
    if hasattr(item, key):
        return getattr(item, key, default)

    # sometimes wrapped
    meta = getattr(item, "metadata", None)
    if meta is not None and hasattr(meta, key):
        return getattr(meta, key, default)

    return default


def dbx_path(item: Any) -> Optional[str]:
    # try common fields in order
    for k in ("path_lower", "path_display", "path"):
        v = dbx_get(item, k, None)
        if isinstance(v, str) and v:
            return v
    return None


def dbx_name(item: Any) -> Optional[str]:
    v = dbx_get(item, "name", None)
    return v if isinstance(v, str) else None


def dbx_is_folder(item: Any) -> bool:
    """
    Tries to infer folder-ness from:
    - .is_folder bool
    - .tag == 'folder'
    - .__class__.__name__ contains 'Folder'
    """
    v = dbx_get(item, "is_folder", None)
    if isinstance(v, bool):
        return v

    tag = dbx_get(item, ".tag", None) or dbx_get(item, "tag", None)
    if isinstance(tag, str) and tag.lower() == "folder":
        return True

    cls = item.__class__.__name__.lower()
    if "folder" in cls:
        return True

    return False