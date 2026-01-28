# -*- coding: utf-8 -*-
"""
monthly_pipeline_MULTISTAGE.py

COPY-ONLY pipeline with audit JSONL.
IMPORTANT: Only ONE stage is processed per run, controlled by env MONTHLY_STAGE (00/10/20/30/40).

Flow (each run processes one stage and forwards to next stage IN, but does NOT process next stage):
00: 00/IN -> 00/OUT -> 00/DONE -> 10/IN
10: 10/IN -> 10/OUT -> 10/DONE -> 20/IN
20: 20/IN -> 20/OUT -> 20/DONE -> 30/IN
30: 30/IN -> 30/OUT -> 30/DONE -> 40/IN
40: 40/IN -> 40/OUT -> 40/DONE (end)

- env-first resolution (no cfg shape dependency)
- audit JSONL always written
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


def _normalize_stage(s: str) -> str:
    s = (s or "").strip()
    if s in {"0", "00"}:
        return "00"
    if s in {"10"}:
        return "10"
    if s in {"20"}:
        return "20"
    if s in {"30"}:
        return "30"
    if s in {"40"}:
        return "40"
    # fallback
    return "00"


def _copy_stage(
    *,
    dbx: DropboxIO,
    store: StateStore,
    rid: str,
    stage: str,
    in_path: str,
    out_path: str,
    done_path: str,
    next_stage: str | None = None,
    next_in_path: str | None = None,
    max_files: int = 200,
) -> None:
    files = _list_files(dbx, in_path)
    store.audit_event(run_id=rid, stage=stage, event="list", src_path=in_path, count=len(files))

    cnt = 0
    for f in files:
        if cnt >= max_files:
            store.audit_event(run_id=rid, stage=stage, event="stop", message="max_files_per_run", limit=max_files)
            break

        src = f.path_display or f.path_lower or ""
        if not src:
            continue
        base = src.split("/")[-1]
        key = stable_key(src)

        if store.is_processed(key):
            store.audit_event(run_id=rid, stage=stage, event="skip", src_path=src, filename=base, message="already_processed")
            continue

        b = dbx.download_to_bytes(src)

        outp = f"{out_path.rstrip('/')}/{base}"
        donep = f"{done_path.rstrip('/')}/{base}"

        dbx.write_file_bytes(outp, b, overwrite=True)
        store.audit_event(run_id=rid, stage=stage, event="write", src_path=src, dst_path=outp, filename=base, size=len(b))

        dbx.move(src, donep)
        store.audit_event(run_id=rid, stage=stage, event="move", src_path=src, dst_path=donep, filename=base)

        if next_stage and next_in_path:
            nextp = f"{next_in_path.rstrip('/')}/{base}"
            dbx.write_file_bytes(nextp, b, overwrite=True)
            store.audit_event(
                run_id=rid,
                stage=next_stage,
                event="write",
                src_path=donep,
                dst_path=nextp,
                filename=base,
                message=f"forward to stage{next_stage} IN",
                size=len(b),
            )

        store.mark_processed(key, f"stage{stage}:{rid}")
        cnt += 1
        print(f"[MONTHLY] stage{stage}: {base} -> OUT/DONE" + (f" + next stage {next_stage} IN" if next_stage else ""))


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
    s30_out  = _pick(cfg, "stage30_out",  "STAGE30_OUT",  "/30_personalize_py/OUT")
    s30_done = _pick(cfg, "stage30_done", "STAGE30_DONE", "/30_personalize_py/DONE")

    s40_in   = _pick(cfg, "stage40_in",   "STAGE40_IN",   "/40_trends_api/IN")
    s40_out  = _pick(cfg, "stage40_out",  "STAGE40_OUT",  "/40_trends_api/OUT")
    s40_done = _pick(cfg, "stage40_done", "STAGE40_DONE", "/40_trends_api/DONE")

    max_files = _pick_int(cfg, "max_files_per_run", "MAX_FILES_PER_RUN", 200)

    # ---- stage gate ----
    stage_gate = _normalize_stage(os.getenv("MONTHLY_STAGE", "00"))

    store = StateStore.load(dbx, state_path)
    store.audit_event(
        run_id=rid,
        stage="--",
        event="run_start",
        message=f"monthly pipeline start (copy-only; one-stage-per-run; gate={stage_gate})",
    )

    try:
        if stage_gate == "00":
            _copy_stage(
                dbx=dbx, store=store, rid=rid, stage="00",
                in_path=s00_in, out_path=s00_out, done_path=s00_done,
                next_stage="10", next_in_path=s10_in, max_files=max_files,
            )
        elif stage_gate == "10":
            _copy_stage(
                dbx=dbx, store=store, rid=rid, stage="10",
                in_path=s10_in, out_path=s10_out, done_path=s10_done,
                next_stage="20", next_in_path=s20_in, max_files=max_files,
            )
        elif stage_gate == "20":
            _copy_stage(
                dbx=dbx, store=store, rid=rid, stage="20",
                in_path=s20_in, out_path=s20_out, done_path=s20_done,
                next_stage="30", next_in_path=s30_in, max_files=max_files,
            )
        elif stage_gate == "30":
            _copy_stage(
                dbx=dbx, store=store, rid=rid, stage="30",
                in_path=s30_in, out_path=s30_out, done_path=s30_done,
                next_stage="40", next_in_path=s40_in, max_files=max_files,
            )
        elif stage_gate == "40":
            _copy_stage(
                dbx=dbx, store=store, rid=rid, stage="40",
                in_path=s40_in, out_path=s40_out, done_path=s40_done,
                next_stage=None, next_in_path=None, max_files=max_files,
            )
        else:
            store.audit_event(run_id=rid, stage="--", event="error", message=f"invalid MONTHLY_STAGE={stage_gate!r}")
    except Exception as e:
        store.audit_event(run_id=rid, stage=stage_gate, event="error", message=str(e), traceback=tb.format_exc())

    # persist
    try:
        store.save(dbx)
        store.audit_event(run_id=rid, stage="--", event="write_state", filename=state_path, message="state saved")
    except Exception as e:
        store.audit_event(run_id=rid, stage="--", event="error", message=f"state_save_failed: {e}", traceback=tb.format_exc())

    store.audit_event(run_id=rid, stage="--", event="run_end", message="monthly pipeline end")
    store.flush_audit_jsonl(dbx, logs_dir, rid)
    return 0