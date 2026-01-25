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
    stage00: str  # input xlsx folder on Dropbox
    stage10: str  # preformat output folder on Dropbox
    logs: str     # logs folder on Dropbox


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


def build_paths_from_env() -> Paths:
    """
    Use the secrets already defined in monthly.yml:
      MONTHLY_INBOX_PATH, MONTHLY_PREP_DIR, MONTHLY_OVERVIEW_DIR
    """
    stage00 = _need_env("MONTHLY_INBOX_PATH").rstrip("/")
    stage10 = _need_env("MONTHLY_PREP_DIR").rstrip("/")
    logs = _need_env("MONTHLY_OVERVIEW_DIR").rstrip("/")
    return Paths(stage00=stage00, stage10=stage10, logs=logs)


def _strip_apps_prefix(p: str) -> str:
    """
    Convert '/Apps/<appname>/xxx/yyy' -> '/xxx/yyy'
    Useful when the Dropbox token is App-folder scoped (root is already app folder).
    """
    if not p:
        return p
    p = p.strip()
    if not p.startswith("/"):
        p = "/" + p
    if not p.startswith("/Apps/"):
        return p
    parts = p.split("/")
    # parts: ["", "Apps", "<appname>", ...]
    if len(parts) <= 3:
        return "/"
    remainder = "/" + "/".join(parts[3:])
    return remainder


def _ensure_slash(p: str) -> str:
    if not p:
        return p
    p = p.strip()
    return p if p.startswith("/") else "/" + p


def _resolve_one_folder(dbx: DropboxIO, path: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Try list_folder(path). If success return (True, used_path, mode).
    mode is 'as_is' or 'app_relative' when using stripped '/Apps/<appname>' prefix.
    """
    path = _ensure_slash(path).rstrip("/") or "/"
    try:
        dbx.list_folder(path)
        return True, path, "as_is"
    except Exception:
        pass

    # Fallback for App-folder scoped token: try stripping '/Apps/<appname>'
    if path.startswith("/Apps/"):
        alt = _strip_apps_prefix(path).rstrip("/") or "/"
        try:
            dbx.list_folder(alt)
            return True, alt, "app_relative"
        except Exception:
            pass

    return False, None, None


def _resolve_paths(dbx: DropboxIO, raw: Paths) -> Tuple[Paths, Dict[str, Any]]:
    """
    Resolve stage00 existence first; if we need app_relative mode, apply consistently to stage10/logs too.
    """
    ok, used_stage00, mode = _resolve_one_folder(dbx, raw.stage00)
    if not ok or not used_stage00 or not mode:
        raise RuntimeError(f"Dropbox folder not found (stage00): {raw.stage00}")

    # Apply same mode to other paths (to keep them in same namespace)
    if mode == "app_relative":
        stage10 = _strip_apps_prefix(raw.stage10)
        logs = _strip_apps_prefix(raw.logs)
    else:
        stage10 = _ensure_slash(raw.stage10)
        logs = _ensure_slash(raw.logs)

    # Optional: do a light validation (best-effort). If not found, keep going but log.
    diag: Dict[str, Any] = {"resolve_mode": mode, "stage00_used": used_stage00}
    for k, p in [("stage10_used", stage10), ("logs_used", logs)]:
        try:
            dbx.list_folder(p)
            diag[k] = p
            diag[k + "_exists"] = True
        except Exception:
            diag[k] = p
            diag[k + "_exists"] = False

    return Paths(stage00=used_stage00, stage10=stage10, logs=logs), diag


def list_stage00_xlsx(dbx: DropboxIO, stage00: str) -> List[Dict[str, Any]]:
    """
    DropboxIO.list_folder should return list[dict] where each dict has at least:
      - name
      - path OR path_display (optional)
    """
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

    # 1) read env paths
    try:
        raw_paths = build_paths_from_env()
    except Exception as e:
        print(f"[monthly_main] FATAL: invalid env/paths: {e!r}", file=sys.stderr)
        log_event(log_path_local, {
            "ts": datetime.now(JST).isoformat(),
            "run_id": run_id,
            "stage": "bootstrap",
            "level": "fatal",
            "error": repr(e),
        })
        return 1

    # 2) dropbox connect
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

    # 3) resolve paths (handles App-folder scoped tokens)
    try:
        paths, diag = _resolve_paths(dbx, raw_paths)
    except Exception as e:
        print(f"[monthly_main] FATAL: resolve paths failed: {e!r}", file=sys.stderr)
        log_event(log_path_local, {
            "ts": datetime.now(JST).isoformat(),
            "run_id": run_id,
            "stage": "bootstrap",
            "level": "fatal",
            "error": repr(e),
        })
        # try upload log (best-effort)
        try:
            # Attempt both modes for log upload
            for candidate_logs in [raw_paths.logs, _strip_apps_prefix(raw_paths.logs)]:
                try:
                    dbx.upload_file(log_path_local, f"{_ensure_slash(candidate_logs).rstrip('/')}/run_{run_id}.jsonl")
                    break
                except Exception:
                    pass
        except Exception:
            pass
        return 1

    # Log start + resolved mode (no secret reveal beyond existence)
    log_event(log_path_local, {
        "ts": datetime.now(JST).isoformat(),
        "run_id": run_id,
        "stage": "bootstrap",
        "msg": "start",
        "resolve": diag,
        "paths_effective": {"stage00": paths.stage00, "stage10": paths.stage10, "logs": paths.logs},
    })
    print(f"[monthly_main] resolve_mode={diag.get('resolve_mode')} stage00_exists=True stage10_exists={diag.get('stage10_used_exists')} logs_exists={diag.get('logs_used_exists')}", file=sys.stderr)

    # 4) list inputs
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
        try:
            dbx.upload_file(log_path_local, f"{paths.logs.rstrip('/')}/run_{run_id}.jsonl")
        except Exception as up_e:
            print(f"[monthly_main] WARN: failed to upload log to Dropbox: {up_e!r}", file=sys.stderr)
        return 1

    if not inputs:
        try:
            dbx.upload_file(log_path_local, f"{paths.logs.rstrip('/')}/run_{run_id}.jsonl")
        except Exception as e:
            print(f"[monthly_main] WARN: upload log failed (no inputs): {e!r}", file=sys.stderr)
        return 0

    # 5) copy xlsx -> preformat folder
    processed = 0
    for it in inputs:
        src_path = _best_path(it, paths.stage00)
        src_name = it.get("name") or "input.xlsx"

        base = safe_name(os.path.splitext(src_name)[0])
        dst_name = f"{base}__preformat.xlsx"
        dst_path = f"{paths.stage10.rstrip('/')}/{dst_name}"

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

    # 6) upload run log
    try:
        dbx.upload_file(log_path_local, f"{paths.logs.rstrip('/')}/run_{run_id}.jsonl")
    except Exception as e:
        print(f"[monthly_main] WARN: failed to upload run log: {e!r}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())