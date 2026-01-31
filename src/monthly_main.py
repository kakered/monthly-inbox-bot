# -*- coding: utf-8 -*-
"""
src/monthly_main.py

MONTHLY pipeline entrypoint.

Design goal:
- Must run safely with NO CLI args (GitHub Actions runs: python -m src.monthly_main)
- Stage-driven routing via env vars:
    MONTHLY_STAGE in {"00","10","20","30","40"}
    STAGE00_IN/OUT/DONE, ... STAGE40_IN/OUT/DONE
- Minimal robust behavior (stage_copy_forward):
    * list .xlsx/.xlsm in STAGEXX_IN
    * download bytes
    * upload copy to STAGEXX_OUT (stage-tagged filename)
    * move original to STAGEXX_DONE (rev-tagged filename)
    * optionally copy original bytes forward to next stage IN (same basename)
    * persist state.json to skip already-processed items

Notes:
- This module intentionally does NOT require sys.argv[1].
- If you later want a "single file mode", add optional args but keep default no-arg path.
"""

from __future__ import annotations

import os
import time
from typing import Tuple

from .dropbox_io import DropboxIO, DbxEntry
from .state_store import StateStore
from .logger import JsonlLogger


# ----------------------------
# helpers
# ----------------------------
def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name, default) or "").strip()


def _utc_stamp() -> str:
    return time.strftime("%Y%m%d-%H%M%S", time.gmtime())


def _stage_vars(stage: str) -> Tuple[str, str, str]:
    return (
        _env(f"STAGE{stage}_IN"),
        _env(f"STAGE{stage}_OUT"),
        _env(f"STAGE{stage}_DONE"),
    )


def _next_stage(stage: str) -> str:
    order = ["00", "10", "20", "30", "40"]
    if stage not in order:
        return ""
    i = order.index(stage)
    return order[i + 1] if (i + 1) < len(order) else ""


def _is_xlsx(name: str) -> bool:
    n = name.lower()
    return n.endswith(".xlsx") or n.endswith(".xlsm")


def _file_key(e: DbxEntry) -> str:
    # stable-ish: path + rev if present
    if getattr(e, "rev", None):
        return f"{e.path}@{e.rev}"
    return e.path


def _require_nonempty(name: str, value: str) -> None:
    if not value:
        raise RuntimeError(f"Missing required env: {name}")


# ----------------------------
# stage runner
# ----------------------------
def stage_copy_forward(
    *,
    io: DropboxIO,
    logger: JsonlLogger,
    store: StateStore,
    stage: str,
    max_files: int,
) -> int:
    """
    Minimal robust behavior:
    - Read files from STAGEXX_IN
    - Copy bytes to STAGEXX_OUT with a stage-tagged filename
    - Move original to STAGEXX_DONE with a rev-tagged filename
    - Also copy original bytes to NEXT_STAGE_IN with same basename (optional)
    """
    p_in, p_out, p_done = _stage_vars(stage)
    if not (p_in and p_out and p_done):
        raise RuntimeError(
            f"Stage{stage} paths are missing. "
            f"Need STAGE{stage}_IN / STAGE{stage}_OUT / STAGE{stage}_DONE."
        )

    # ensure folders exist (best-effort; DropboxIO should be idempotent)
    io.ensure_folder(os.path.dirname(p_in) or "/")
    io.ensure_folder(p_in)
    io.ensure_folder(p_out)
    io.ensure_folder(p_done)

    state = store.load()
    bucket = store.get_stage_bucket(state, stage)
    bucket["last_run_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # list inputs
    entries = [e for e in io.list_folder(p_in) if e.is_file and _is_xlsx(e.name)]
    entries = entries[: max_files if max_files > 0 else 0]

    processed = 0
    for e in entries:
        k = _file_key(e)
        if store.is_done(bucket, k):
            continue

        src_path = e.path
        base = e.name

        # 1) download bytes
        data = io.download(src_path)

        # 2) write OUT (keep original copy for debugging)
        stem, ext = os.path.splitext(base)
        out_name = f"{stem}__stage{stage}__{_utc_stamp()}{ext}"
        out_path = f"{p_out}/{out_name}"
        io.upload_overwrite(out_path, data)

        # 3) move to DONE (rename with rev)
        rev = e.rev or "no-rev"
        done_name = f"{stem}__rev-{rev}__{_utc_stamp()}{ext}"
        done_path = f"{p_done}/{done_name}"
        io.move_replace(src_path, done_path)

        # 4) copy forward to next stage IN (optional)
        nxt = _next_stage(stage)
        copied_forward = False
        if nxt:
            nxt_in, _, _ = _stage_vars(nxt)
            if nxt_in:
                io.ensure_folder(nxt_in)
                nxt_path = f"{nxt_in}/{base}"
                io.upload_overwrite(nxt_path, data)
                copied_forward = True

        # 5) mark done + persist state
        store.mark_done(bucket, k)
        store.save(state)

        logger.log(
            {
                "event": "file_processed",
                "stage": stage,
                "src": src_path,
                "out": out_path,
                "done": done_path,
                "copied_to_next_in": copied_forward,
                "size": len(data),
            }
        )

        processed += 1

    logger.log(
        {
            "event": "stage_end",
            "stage": stage,
            "processed": processed,
            "in_count_scanned": len(entries),
        }
    )
    return processed


# ----------------------------
# main
# ----------------------------
def main() -> int:
    # credentials
    tok = _env("DROPBOX_REFRESH_TOKEN")
    app_key = _env("DROPBOX_APP_KEY")
    app_secret = _env("DROPBOX_APP_SECRET")

    # controls
    stage = _env("MONTHLY_STAGE", "00")
    max_files = int(_env("MAX_FILES_PER_RUN", "200") or "200")

    # state & logs
    state_path = _env("STATE_PATH", "/_system/state.json")
    logs_dir = _env("LOGS_DIR", "/_system/logs")

    # validate minimum envs
    _require_nonempty("DROPBOX_REFRESH_TOKEN", tok)
    _require_nonempty("DROPBOX_APP_KEY", app_key)
    _require_nonempty("DROPBOX_APP_SECRET", app_secret)
    if stage not in {"00", "10", "20", "30", "40"}:
        raise RuntimeError("MONTHLY_STAGE must be one of 00/10/20/30/40")

    io = DropboxIO(refresh_token=tok, app_key=app_key, app_secret=app_secret)
    logger = JsonlLogger(io, logs_dir=logs_dir)
    store = StateStore(io=io, state_path=state_path)

    logger.log(
        {
            "event": "run_start",
            "stage": stage,
            "state_path": state_path,
            "logs_dir": logs_dir,
            "max_files": max_files,
        }
    )

    processed = stage_copy_forward(
        io=io,
        logger=logger,
        store=store,
        stage=stage,
        max_files=max_files,
    )

    logger.log({"event": "run_end", "stage": stage, "processed": processed})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())