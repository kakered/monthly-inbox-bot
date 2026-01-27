# -*- coding: utf-8 -*-
"""
monthly_pipeline_MULTISTAGE.py

Stage00 minimal pipeline (keeps current behavior):
- list files from STAGE00_IN
- copy bytes to STAGE00_OUT
- move original to STAGE00_DONE
- also place a copy into STAGE10_IN
- update state.json
- write audit JSONL into LOGS_DIR

This file is intentionally minimal to avoid big refactors.
"""

from __future__ import annotations

import time
from dropbox.files import FileMetadata

from .dropbox_io import DropboxIO
from .state_store import StateStore, stable_key


def _run_id_default() -> str:
    return time.strftime("%Y%m%d-%H%M%SZ", time.gmtime())


def run_multistage(dbx: DropboxIO, cfg, run_id: str | None = None) -> int:
    rid = run_id or _run_id_default()
    store = StateStore.load(dbx, cfg.state_path)

    store.audit_event(
        run_id=rid,
        stage="--",
        event="run_start",
        message="monthly pipeline start",
        monthly_stage=cfg.monthly_stage,
    )

    # Only stage00 in this minimal build
    if cfg.monthly_stage != "00":
        store.audit_event(run_id=rid, stage=str(cfg.monthly_stage), event="skip", message="Only stage00 implemented")
        store.save(dbx)
        store.audit_event(run_id=rid, stage=str(cfg.monthly_stage), event="write_state", filename=cfg.state_path, message="state saved (skip)")
        store.audit_event(run_id=rid, stage="--", event="run_end", message="monthly pipeline end (skip)")
        store.flush_audit_jsonl(dbx, cfg.logs_dir, rid)
        return 0

    entries = dbx.list_folder(cfg.stage00_in)
    files = [e for e in entries if isinstance(e, FileMetadata)]
    store.audit_event(run_id=rid, stage="00", event="list", src_path=cfg.stage00_in, message=f"n={len(files)}")

    processed_count = 0
    for f in files:
        if processed_count >= cfg.max_files_per_run:
            store.audit_event(run_id=rid, stage="00", event="stop", message="max_files_per_run", limit=cfg.max_files_per_run)
            break

        src = f.path_display or f.path_lower or ""
        if not src:
            continue

        base = src.split("/")[-1]
        key = stable_key(src)

        if store.is_processed(key):
            store.audit_event(run_id=rid, stage="00", event="skip", src_path=src, filename=base, message="already_processed")
            continue

        try:
            b = dbx.download_to_bytes(src)

            out_path = f"{cfg.stage00_out.rstrip('/')}/{base}"
            done_path = f"{cfg.stage00_done.rstrip('/')}/{base}"
            next_in_path = f"{cfg.stage10_in.rstrip('/')}/{base}"

            dbx.write_file_bytes(out_path, b, overwrite=True)
            store.audit_event(run_id=rid, stage="00", event="write", src_path=src, dst_path=out_path, filename=base, size=len(b))

            dbx.move(src, done_path)
            store.audit_event(run_id=rid, stage="00", event="move", src_path=src, dst_path=done_path, filename=base)

            dbx.write_file_bytes(next_in_path, b, overwrite=True)
            store.audit_event(run_id=rid, stage="10", event="write", src_path=done_path, dst_path=next_in_path, filename=base, size=len(b))

            store.add_done(src_path=src)
            store.mark_processed(key, f"stage00:{rid}")
            processed_count += 1

            # keep your existing console style
            print(f"[MONTHLY] stage00: {base} -> OUT/DONE + next stage 10 IN")

        except Exception as e:
            store.audit_event(run_id=rid, stage="00", event="error", src_path=src, filename=base, message=str(e))
            store.add_error({"run_id": rid, "stage": "00", "src": src, "error": str(e)})

    # Persist state + audit
    try:
        store.save(dbx)
        store.audit_event(run_id=rid, stage="00", event="write_state", filename=cfg.state_path, message="state saved")
    except Exception as e:
        store.audit_event(run_id=rid, stage="--", event="error", message=f"state_save_failed: {e}")

    store.audit_event(run_id=rid, stage="--", event="run_end", message="monthly pipeline end")
    store.flush_audit_jsonl(dbx, cfg.logs_dir, rid)
    return 0