# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

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
    # Prefer explicit path fields if present
    for k in ("path", "path_display", "path_lower"):
        p = it.get(k)
        if p:
            return p
    # Fallback: compose from dir + name
    nm = it.get("name") or "input.xlsx"
    return f"{fallback_dir.rstrip('/')}/{nm}"


def main() -> int:
    run_id = now_jst_str()
    log_path_local = f"/tmp/monthly_{run_id}.jsonl"

    # 1) paths
    try:
        paths = build_paths_from_env()
    except Exception as e:
        # Make sure we see this in Actions logs
        print(f"[monthly_main] FATAL: invalid env/paths: {e!r}", file=sys.stderr)
        # also write a small local log
        log_event(log_path_local, {
            "ts": datetime.now(JST).isoformat(),
            "run_id": run_id,
            "stage": "bootstrap",
            "level": "fatal",
            "error": repr(e),
        })
        # show the local log content
        try:
            with open(log_path_local, "r", encoding="utf-8") as f:
                print("[monthly_main] local log:", file=sys.stderr)
                print(f.read(), file=sys.stderr)
        except Exception:
            pass
        return 1

    log_event(log_path_local, {
        "ts": datetime.now(JST).isoformat(),
        "run_id": run_id,
        "stage": "bootstrap",
        "msg": "start",
        "paths": paths.__dict__,
    })

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

    # 3) list input files
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
        # IMPORTANT: print so Actions shows the real reason
        print(f"[monthly_main] ERROR: listing stage00 failed. stage00={paths.stage00!r} err={e!r}", file=sys.stderr)
        log_event(log_path_local, {
            "ts": datetime.now(JST).isoformat(),
            "run_id": run_id,
            "stage": "00_list",
            "level": "error",
            "stage00": paths.stage00,
            "error": repr(e),
        })
        # Try to upload the log so you can see it on Dropbox too
        try:
            dbx.upload_file(log_path_local, f"{paths.logs}/run_{run_id}.jsonl")
        except Exception as up_e:
            print(f"[monthly_main] WARN: failed to upload log to Dropbox: {up_e!r}", file=sys.stderr)
        # Also print the local log content
        try:
            with open(log_path_local, "r", encoding="utf-8") as f:
                print("[monthly_main] local log:", file=sys.stderr)
                print(f.read(), file=sys.stderr)
        except Exception:
            pass
        return 1

    if not inputs:
        # nothing to do; still upload log
        try:
            dbx.upload_file(log_path_local, f"{paths.logs}/run_{run_id}.jsonl")
        except Exception as e:
            print(f"[monthly_main] WARN: upload log failed (no inputs): {e!r}", file=sys.stderr)
        return 0

    # 4) copy xlsx -> preformat folder
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

    # 5) upload run log
    try:
        dbx.upload_file(log_path_local, f"{paths.logs}/run_{run_id}.jsonl")
    except Exception as e:
        print(f"[monthly_main] WARN: failed to upload run log: {e!r}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())