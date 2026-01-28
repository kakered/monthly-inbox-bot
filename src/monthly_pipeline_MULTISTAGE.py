# -*- coding: utf-8 -*-
"""
monthly_pipeline_MULTISTAGE.py

Stage00 + Stage10 + Stage20 (COPY-ONLY) pipeline.

- Robust against cfg shape mismatch (env-first resolution)
- COPY first, EDIT later (edit hook can be added per stage)
- Writes audit JSONL to Dropbox via StateStore.flush_audit_jsonl()

Flow:
00: 00/IN -> 00/OUT -> 00/DONE -> 10/IN
10: 10/IN -> 10/OUT -> 10/DONE -> 20/IN
20: 20/IN -> 20/OUT -> 20/DONE -> 30/IN
"""

from __future__ import annotations

import os
import time
import traceback as tb
from dropbox.files import FileMetadata

from .dropbox_io import DropboxIO
from .state_store import StateStore, stable_key


print("### BOOT monthly_pipeline_MULTISTAGE.py ###", __file__)


def _run_id_default() -> str:
    return time.strftime("%Y%m%d-%H%M%SZ", time.gmtime())


def _pick(cfg, attr: str, env_name: str, default: str) -> str:
    v = getattr(cfg, attr, None)
    if isinstance(v, str) and v.strip():
        return v.strip()
    ev = os.getenv(env_name, "").strip()
    return ev if ev else default


def _pick_int(cfg, attr: str, env_name: str, default: int) -> int:
    v = getattr(cfg, attr, None)
    if isinstance(v, int):
        return v
    ev = os.getenv(env_name, "").strip()
    try:
        return int(ev)
    except Exception:
        return default


def _list_files(dbx: DropboxIO, path: str):
    entries = dbx.list_folder(path)
    return [e for e in entries if isinstance(e, FileMetadata)]


def run_multistage(dbx: DropboxIO, cfg, run_id: str | None = None) -> int:
    rid = run_id or _run_id_default()

    # ---- resolve paths (env-first) ----
    state_path = _pick(cfg, "state_path", "STATE_PATH", "/_system/state.json")
    logs_dir = _pick(cfg, "logs_dir", "LOGS_DIR", "/_system/logs")

    s00_in   = _pick(cfg, "stage00_in",   "STAGE00_IN",   "/00_inbox_raw/IN")
    s00_out  = _pick(cfg, "stage00_out",  "STAGE00_OUT",  "/00_inbox_raw/OUT")
    s00_done = _pick(cfg, "stage00_done", "STAGE00_DONE", "/00_inbox_raw/DONE")

    s10_in   = _pick(cfg, "stage10_in",   "STAGE10_IN",   "/10_preformat_py/IN")
    s10_out  = _pick(cfg, "stage10_out",  "STAGE10_OUT",  "/10_preformat_py/OUT")
    s10_done = _pick(cfg, "stage10_done", "STAGE10_DONE", "/10_preformat_py/DONE")

    s20_in   = _pick(cfg, "stage20_in",   "STAGE20_IN",   "/20_overview_api/IN")
    s20_out  = _pick(cfg, "stage20_out",  "STAGE20_OUT",  "/20_overview_api/OUT")
    s20_done = _pick(cfg, "stage20_done", "STAGE20_DONE", "/20_overview_api/DONE")

    s30_in   = _pick(cfg, "stage30_in",   "STAGE30_IN",   "/30_personalize_py/IN")

    max_files = _pick_int(cfg, "max_files_per_run", "MAX_FILES_PER_RUN", 200)

    store = StateStore.load(dbx, state_path)

    # ---- run start ----
    store.audit_event(
        run_id=rid,
        stage="--",
        event="run_start",
        message="monthly pipeline start (stage00+10+20 copy-only)",
    )

    # ======================
    # Stage 00 (COPY)
    # ======================
    try:
        files00 = _list_files(dbx, s00_in)
        store.audit_event(run_id=rid, stage="00", event="list", src_path=s00_in, count=len(files00))
        cnt = 0
        for f in files00:
            if cnt >= max_files:
                store.audit_event(run_id=rid, stage="00", event="stop", message="max_files_per_run", limit=max_files)
                break

            src = f.path_display or f.path_lower or ""
            if not src:
                continue
            base = src.split("/")[-1]
            key = stable_key(src)

            if store.is_processed(key):
                store.audit_event(run_id=rid, stage="00", event="skip", src_path=src, filename=base, message="already_processed")
                continue

            b = dbx.download_to_bytes(src)
            outp  = f"{s00_out.rstrip('/')}/{base}"
            donep = f"{s00_done.rstrip('/')}/{base}"
            nextp = f"{s10_in.rstrip('/')}/{base}"

            dbx.write_file_bytes(outp, b, overwrite=True)
            store.audit_event(run_id=rid, stage="00", event="write", src_path=src, dst_path=outp, filename=base, size=len(b))

            dbx.move(src, donep)
            store.audit_event(run_id=rid, stage="00", event="move", src_path=src, dst_path=donep, filename=base)

            dbx.write_file_bytes(nextp, b, overwrite=True)
            store.audit_event(run_id=rid, stage="10", event="write", src_path=donep, dst_path=nextp, filename=base,
                              message="forward to stage10 IN", size=len(b))

            store.add_done(src)
            store.mark_processed(key, f"stage00:{rid}")
            cnt += 1
            print(f"[MONTHLY] stage00: {base} -> OUT/DONE + next stage 10 IN")

    except Exception as e:
        store.audit_event(run_id=rid, stage="00", event="error", message=str(e), traceback=tb.format_exc())

    # ======================
    # Stage 10 (COPY ONLY)
    # ======================
    try:
        files10 = _list_files(dbx, s10_in)
        store.audit_event(run_id=rid, stage="10", event="list", src_path=s10_in, count=len(files10))
        cnt = 0
        for f in files10:
            if cnt >= max_files:
                store.audit_event(run_id=rid, stage="10", event="stop", message="max_files_per_run", limit=max_files)
                break

            src = f.path_display or f.path_lower or ""
            if not src:
                continue
            base = src.split("/")[-1]
            key = stable_key(src)

            if store.is_processed(key):
                store.audit_event(run_id=rid, stage="10", event="skip", src_path=src, filename=base, message="already_processed")
                continue

            b = dbx.download_to_bytes(src)
            outp  = f"{s10_out.rstrip('/')}/{base}"
            donep = f"{s10_done.rstrip('/')}/{base}"
            nextp = f"{s20_in.rstrip('/')}/{base}"

            dbx.write_file_bytes(outp, b, overwrite=True)
            store.audit_event(run_id=rid, stage="10", event="write", src_path=src, dst_path=outp, filename=base, size=len(b))

            dbx.move(src, donep)
            store.audit_event(run_id=rid, stage="10", event="move", src_path=src, dst_path=donep, filename=base)

            dbx.write_file_bytes(nextp, b, overwrite=True)
            store.audit_event(run_id=rid, stage="20", event="write", src_path=donep, dst_path=nextp, filename=base,
                              message="forward to stage20 IN", size=len(b))

            store.mark_processed(key, f"stage10:{rid}")
            cnt += 1
            print(f"[MONTHLY] stage10: {base} -> OUT/DONE + next stage 20 IN")

    except Exception as e:
        store.audit_event(run_id=rid, stage="10", event="error", message=str(e), traceback=tb.format_exc())

    # ======================
    # Stage 20 (COPY ONLY)
    # ======================
    try:
        files20 = _list_files(dbx, s20_in)
        store.audit_event(run_id=rid, stage="20", event="list", src_path=s20_in, count=len(files20))
        cnt = 0
        for f in files20:
            if cnt >= max_files:
                store.audit_event(run_id=rid, stage="20", event="stop", message="max_files_per_run", limit=max_files)
                break

            src = f.path_display or f.path_lower or ""
            if not src:
                continue
            base = src.split("/")[-1]
            key = stable_key(src)

            if store.is_processed(key):
                store.audit_event(run_id=rid, stage="20", event="skip", src_path=src, filename=base, message="already_processed")
                continue

            b = dbx.download_to_bytes(src)
            outp  = f"{s20_out.rstrip('/')}/{base}"
            donep = f"{s20_done.rstrip('/')}/{base}"
            nextp = f"{s30_in.rstrip('/')}/{base}"

            dbx.write_file_bytes(outp, b, overwrite=True)
            store.audit_event(run_id=rid, stage="20", event="write", src_path=src, dst_path=outp, filename=base, size=len(b))

            dbx.move(src, donep)
            store.audit_event(run_id=rid, stage="20", event="move", src_path=src, dst_path=donep, filename=base)

            dbx.write_file_bytes(nextp, b, overwrite=True)
            store.audit_event(run_id=rid, stage="30", event="write", src_path=donep, dst_path=nextp, filename=base,
                              message="forward to stage30 IN", size=len(b))

            store.mark_processed(key, f"stage20:{rid}")
            cnt += 1
            print(f"[MONTHLY] stage20: {base} -> OUT/DONE + next stage 30 IN")

    except Exception as e:
        store.audit_event(run_id=rid, stage="20", event="error", message=str(e), traceback=tb.format_exc())

    # ---- persist ----
    try:
        store.save(dbx)
        store.audit_event(run_id=rid, stage="--", event="write_state", filename=state_path, message="state saved")
    except Exception as e:
        store.audit_event(run_id=rid, stage="--", event="error", message=f"state_save_failed: {e}", traceback=tb.format_exc())

    store.audit_event(run_id=rid, stage="--", event="run_end", message="monthly pipeline end")
    store.flush_audit_jsonl(dbx, logs_dir, rid)
    return 0