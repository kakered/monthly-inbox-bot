# -*- coding: utf-8 -*-
"""
monthly_pipeline_MULTISTAGE.py

Stage00 ONLY pipeline (robust against MonthlyCfg shape mismatch).

Key idea:
- Do NOT assume cfg has attributes like stage00_in / logs_dir.
- Always resolve config via:
    getattr(cfg, attr, None) -> env var -> default

Outputs:
- state.json is updated (if STATE_PATH resolves)
- audit JSONL is written to LOGS_DIR on Dropbox:
    /_system/logs/monthly_audit_<run_id>.jsonl
"""

from __future__ import annotations

import os
import time
from dropbox.files import FileMetadata

from .dropbox_io import DropboxIO
from .state_store import StateStore, stable_key


def _run_id_default() -> str:
    return time.strftime("%Y%m%d-%H%M%SZ", time.gmtime())


def _pick(cfg, attr: str, env_name: str, default: str) -> str:
    v = getattr(cfg, attr, None)
    if isinstance(v, str) and v.strip():
        return v.strip()
    ev = os.getenv(env_name, "").strip()
    if ev:
        return ev
    return default


def _pick_int(cfg, attr: str, env_name: str, default: int) -> int:
    v = getattr(cfg, attr, None)
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip().isdigit():
        return int(v.strip())
    ev = os.getenv(env_name, "").strip()
    if ev:
        try:
            return int(ev)
        except Exception:
            pass
    return default


def run_multistage(dbx: DropboxIO, cfg, run_id: str | None = None) -> int:
    rid = run_id or _run_id_default()

    # Resolve paths (never rely on cfg shape)
    state_path = _pick(cfg, "state_path", "STATE_PATH", "/_system/state.json")
    logs_dir = _pick(cfg, "logs_dir", "LOGS_DIR", "/_system/logs")

    stage00_in = _pick(cfg, "stage00_in", "STAGE00_IN", "/00_inbox_raw/IN")
    stage00_out = _pick(cfg, "stage00_out", "STAGE00_OUT", "/00_inbox_raw/OUT")
    stage00_done = _pick(cfg, "stage00_done", "STAGE00_DONE", "/00_inbox_raw/DONE")

    stage10_in = _pick(cfg, "stage10_in", "STAGE10_IN", "/10_preformat_py/IN")

    max_files_per_run = _pick_int(cfg, "max_files_per_run", "MAX_FILES_PER_RUN", 200)

    store = StateStore.load(dbx, state_path)

    # ---- run start ----
    store.audit_event(
        run_id=rid,
        stage="--",
        event="run_start",
        message="monthly pipeline start (stage00 only; cfg robust)",
    )

    # ---- stage00 list ----
    entries = dbx.list_folder(stage00_in)
    files = [e for e in entries if isinstance(e, FileMetadata)]
    store.audit_event(
        run_id=rid,
        stage="00",
        event="list",
        src_path=stage00_in,
        message=f"n={len(files)}",
    )

    processed_count = 0
    for f in files:
        if processed_count >= max_files_per_run:
            store.audit_event(
                run_id=rid,
                stage="00",
                event="stop",
                message="max_files_per_run",
                limit=max_files_per_run,
            )
            break

        src = f.path_display or f.path_lower or ""
        if not src:
            continue

        base = src.split("/")[-1]
        key = stable_key(src)

        if store.is_processed(key):
            store.audit_event(
                run_id=rid,
                stage="00",
                event="skip",
                src_path=src,
                filename=base,
                message="already_processed",
            )
            continue

        try:
            b = dbx.download_to_bytes(src)

            out_path = f"{stage00_out.rstrip('/')}/{base}"
            done_path = f"{stage00_done.rstrip('/')}/{base}"
            next_in_path = f"{stage10_in.rstrip('/')}/{base}"

            dbx.write_file_bytes(out_path, b, overwrite=True)
            store.audit_event(
                run_id=rid,
                stage="00",
                event="write",
                src_path=src,
                dst_path=out_path,
                filename=base,
                size=len(b),
            )

            dbx.move(src, done_path)
            store.audit_event(
                run_id=rid,
                stage="00",
                event="move",
                src_path=src,
                dst_path=done_path,
                filename=base,
            )

            dbx.write_file_bytes(next_in_path, b, overwrite=True)
            store.audit_event(
                run_id=rid,
                stage="10",
                event="write",
                src_path=done_path,
                dst_path=next_in_path,
                filename=base,
                size=len(b),
                message="forward to stage10 IN",
            )

            store.add_done(src_path=src)
            store.mark_processed(key, f"stage00:{rid}")
            processed_count += 1

            print(f"[MONTHLY] stage00: {base} -> OUT/DONE + next stage 10 IN")

        except Exception as e:
            store.audit_event(
                run_id=rid,
                stage="00",
                event="error",
                src_path=src,
                filename=base,
                message=str(e),
            )
            store.add_error(
                {"run_id": rid, "stage": "00", "src": src, "error": str(e)}
            )

    # ---- persist ----
    try:
        store.save(dbx)
        store.audit_event(
            run_id=rid,
            stage="00",
            event="write_state",
            filename=state_path,
            message="state saved",
        )
    except Exception as e:
        store.audit_event(
            run_id=rid,
            stage="--",
            event="error",
            message=f"state_save_failed: {e}",
        )

    store.audit_event(
        run_id=rid,
        stage="--",
        event="run_end",
        message="monthly pipeline end",
    )

    # Write audit JSONL to Dropbox (best effort inside flush)
    store.flush_audit_jsonl(dbx, logs_dir, rid)
    return 0