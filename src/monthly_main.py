# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.dropbox_io import DropboxIO

JST = timezone(timedelta(hours=9))


@dataclass(frozen=True)
class Paths:
    stage00: str
    stage10: str
    logs: str
    outbox: str


def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y%m%d_%H%M%S")


def safe_name(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)
    name = re.sub(r"\s+", " ", name)
    return name


def log_event(log_path_local: str, event: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(log_path_local), exist_ok=True)
    with open(log_path_local, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def _need_env(key: str) -> str:
    v = os.environ.get(key)
    if not v:
        raise ValueError(f"Missing required env: {key}")
    return v


def _norm_path(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return p
    if not p.startswith("/"):
        p = "/" + p
    if len(p) > 1:
        p = p.rstrip("/")
    return p


def _app_prefixes(app_folder_name: str) -> List[str]:
    """
    Try both English and Japanese 'Apps' folder roots just in case.
    (Dropbox UI may show アプリ; API is usually /Apps, but we try both.)
    """
    app_folder_name = (app_folder_name or "").strip() or "monthly-inbox-bot"
    return [
        f"/Apps/{app_folder_name}",
        f"/アプリ/{app_folder_name}",
    ]


def _candidate_paths(raw: str, app_folder_name: str) -> List[str]:
    raw = _norm_path(raw)
    if not raw:
        return []

    candidates: List[str] = []

    def add(x: str) -> None:
        x = _norm_path(x)
        if x and x not in candidates:
            candidates.append(x)

    # as-is
    add(raw)

    # add prefixes (full-dropbox style)
    for pref in _app_prefixes(app_folder_name):
        if not raw.startswith(pref + "/") and raw != pref:
            add(pref + raw)

    # strip prefixes (app-folder style)
    for pref in _app_prefixes(app_folder_name):
        if raw.startswith(pref + "/"):
            add(raw[len(pref):])
        elif raw == pref:
            add("/")

    return candidates


def _exists_folder_by_list(dbx: DropboxIO, path: str) -> Tuple[bool, Optional[str]]:
    try:
        dbx.list_folder(path)
        return True, None
    except Exception as e:
        return False, repr(e)


def _try_list_root(dbx: DropboxIO) -> Dict[str, Any]:
    """
    Print what the token can see at root.
    This is the single most important diagnostic for 'folder not found'.
    """
    out: Dict[str, Any] = {"attempts": []}

    for p in ("", "/", "/Apps", "/アプリ"):
        try:
            items = dbx.list_folder(p)
            # keep it small and safe
            names = []
            for it in (items or [])[:50]:
                nm = it.get("name") or it.get("path_display") or it.get("path") or ""
                if nm:
                    names.append(nm)
            out["attempts"].append({"path": p, "ok": True, "count": len(items or []), "sample": names})
        except Exception as e:
            out["attempts"].append({"path": p, "ok": False, "err": repr(e)})

    return out


def resolve_paths_or_die(dbx: DropboxIO) -> Paths:
    stage00_raw = _need_env("MONTHLY_INBOX_PATH")
    stage10_raw = _need_env("MONTHLY_PREP_DIR")
    logs_raw = _need_env("MONTHLY_OVERVIEW_DIR")
    outbox_raw = _need_env("MONTHLY_OUTBOX_DIR")

    app_folder_name = os.environ.get("DROPBOX_APP_FOLDER_NAME", "monthly-inbox-bot").strip() or "monthly-inbox-bot"

    c_stage00 = _candidate_paths(stage00_raw, app_folder_name)
    c_stage10 = _candidate_paths(stage10_raw, app_folder_name)
    c_logs = _candidate_paths(logs_raw, app_folder_name)
    c_outbox = _candidate_paths(outbox_raw, app_folder_name)

    debug: Dict[str, Any] = {
        "app_folder_name": app_folder_name,
        "candidates": {
            "stage00": c_stage00,
            "stage10": c_stage10,
            "logs": c_logs,
            "outbox": c_outbox,
        },
        "checks": {
            "stage00": [],
            "stage10": [],
            "logs": [],
            "outbox": [],
        },
        "root_listing": _try_list_root(dbx),
    }

    def pick(key: str, cands: List[str]) -> str:
        for p in cands:
            ok, err = _exists_folder_by_list(dbx, p)
            debug["checks"][key].append({"path": p, "ok": ok, "err": err})
            if ok:
                return p
        raise RuntimeError(f"Dropbox folder not found ({key}): {cands[0] if cands else '(empty)'}")

    try:
        stage00 = pick("stage00", c_stage00)
        stage10 = pick("stage10", c_stage10)
        logs = pick("logs", c_logs)
        outbox = pick("outbox", c_outbox)
    except Exception as e:
        print(f"[monthly_main] FATAL: resolve paths failed: {e!r}", file=sys.stderr)
        print("[monthly_main] resolve debug (includes root listing):", file=sys.stderr)
        print(json.dumps(debug, ensure_ascii=False, indent=2), file=sys.stderr)
        raise

    return Paths(stage00=stage00, stage10=stage10, logs=logs, outbox=outbox)


def list_stage00_xlsx(dbx: DropboxIO, stage00: str) -> List[Dict[str, Any]]:
    files = dbx.list_folder(stage00)
    out: List[Dict[str, Any]] = []
    for it in files or []:
        name = (it.get("name") or "")
        if name.lower().endswith(".xlsx") and not name.startswith("~$"):
            out.append(it)
    return out


def _best_path(it: Dict[str, Any], fallback_dir: str) -> str:
    for k in ("path", "path_display", "path_lower"):
        p = it.get(k)
        if p:
            return p
    nm = it.get("name") or "input.xlsx"
    return f"{fallback_dir.rstrip('/')}/{nm}"


def main() -> int:
    run_id = now_jst_str()
    log_path_local = f"/tmp/monthly_{run_id}.jsonl"

    try:
        dbx = DropboxIO.from_env()
    except Exception as e:
        print(f"[monthly_main] FATAL: DropboxIO.from_env() failed: {e!r}", file=sys.stderr)
        log_event(log_path_local, {
            "ts": datetime.now(JST).isoformat(),
            "run_id": run_id,
            "stage": "bootstrap",
            "level": "fatal",
            "error": repr(e),
        })
        return 1

    try:
        paths = resolve_paths_or_die(dbx)
    except Exception as e:
        log_event(log_path_local, {
            "ts": datetime.now(JST).isoformat(),
            "run_id": run_id,
            "stage": "bootstrap",
            "level": "fatal",
            "error": repr(e),
        })
        return 1

    log_event(log_path_local, {
        "ts": datetime.now(JST).isoformat(),
        "run_id": run_id,
        "stage": "bootstrap",
        "msg": "start",
        "paths": paths.__dict__,
    })

    try:
        inputs = list_stage00_xlsx(dbx, paths.stage00)
        log_event(log_path_local, {
            "ts": datetime.now(JST).isoformat(),
            "run_id": run_id,
            "stage": "00_list",
            "count": len(inputs),
            "files": [x.get("name") for x in inputs],
        })
    except Exception as e:
        print(f"[monthly_main] ERROR: listing stage00 failed. stage00={paths.stage00!r} err={e!r}", file=sys.stderr)
        log_event(log_path_local, {
            "ts": datetime.now(JST).isoformat(),
            "run_id": run_id,
            "stage": "00_list",
            "level": "error",
            "stage00": paths.stage00,
            "error": repr(e),
        })
        return 1

    if not inputs:
        try:
            dbx.upload_file(log_path_local, f"{paths.logs}/run_{run_id}.jsonl")
        except Exception as e:
            print(f"[monthly_main] WARN: upload log failed (no inputs): {e!r}", file=sys.stderr)
        return 0

    processed = 0
    for it in inputs:
        src_path = _best_path(it, paths.stage00)
        src_name = it.get("name") or "input.xlsx"
        base = safe_name(os.path.splitext(src_name)[0])
        dst_name = f"{base}__preformat.xlsx"
        dst_path = f"{paths.stage10}/{dst_name}"

        try:
            if hasattr(dbx, "copy"):
                dbx.copy(src_path, dst_path)
            else:
                data = dbx.download_bytes(src_path)
                dbx.upload_bytes(data, dst_path)

            processed += 1
            log_event(log_path_local, {
                "ts": datetime.now(JST).isoformat(),
                "run_id": run_id,
                "stage": "10_write",
                "src": src_path,
                "dst": dst_path,
                "status": "ok",
            })
        except Exception as e:
            print(f"[monthly_main] ERROR: copy failed src={src_path!r} dst={dst_path!r} err={e!r}", file=sys.stderr)
            log_event(log_path_local, {
                "ts": datetime.now(JST).isoformat(),
                "run_id": run_id,
                "stage": "10_write",
                "src": src_path,
                "dst": dst_path,
                "status": "error",
                "error": repr(e),
            })

    try:
        dbx.upload_file(log_path_local, f"{paths.logs}/run_{run_id}.jsonl")
    except Exception as e:
        print(f"[monthly_main] WARN: failed to upload run log: {e!r}", file=sys.stderr)

    return 0 if processed > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())