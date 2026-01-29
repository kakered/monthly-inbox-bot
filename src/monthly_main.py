# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
from typing import Dict, List, Tuple

from .dropbox_io import DropboxIO, DbxEntry
from .state_store import StateStore
from .logger import JsonlLogger


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


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
    i = order.index(stage)
    return order[i + 1] if i + 1 < len(order) else ""


def _is_xlsx(name: str) -> bool:
    n = name.lower()
    return n.endswith(".xlsx") or n.endswith(".xlsm")


def _file_key(e: DbxEntry) -> str:
    # stable-ish: path + rev if present
    if e.rev:
        return f"{e.path}@{e.rev}"
    return e.path


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
    - Also copy original bytes to NEXT_STAGE_IN with same basename (so next stage can be run separately)
    """
    p_in, p_out, p_done = _stage_vars(stage)
    if not (p_in and p_out and p_done):
        raise RuntimeError(f"Stage{stage} paths are missing. IN/OUT/DONE must be set.")

    # ensure folders
    io.ensure_folder(os.path.dirname(p_in) or "/")
    io.ensure_folder(p_in)
    io.ensure_folder(p_out)
    io.ensure_folder(p_done)

    state = store.load()
    bucket = store.get_stage_bucket(state, stage)
    bucket["last_run_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    entries = [e for e in io.list_folder(p_in) if e.is_file and _is_xlsx(e.name)]
    entries = entries[:max_files]

    processed = 0
    for e in entries:
        k = _file_key(e)
        if store.is_done(bucket, k):
            continue

        src_path = e.path
        base = e.name

        # 1) download bytes
        data = io.download(src_path)

        # 2) write OUT (keep original for debugging)
        out_name = f"{os.path.splitext(base)[0]}__stage{stage}__{_utc_stamp()}{os.path.splitext(base)[1]}"
        out_path = f"{p_out}/{out_name}"
        io.upload_overwrite(out_path, data)

        # 3) move to DONE (rename with rev)
        rev = e.rev or "no-rev"
        done_name = f"{os.path.splitext(base)[0]}__rev-{rev}__{_utc_stamp()}{os.path.splitext(base)[1]}"
        done_path = f"{p_done}/{done_name}"
        # Dropbox move keeps server-side file, fast.
        io.move_replace(src_path, done_path)

        # 4) copy forward to next stage IN (optional)
        nxt = _next_stage(stage)
        if nxt:
            nxt_in, _, _ = _stage_vars(nxt)
            if nxt_in:
                io.ensure_folder(nxt_in)
                nxt_path = f"{nxt_in}/{base}"
                io.upload_overwrite(nxt_path, data)

        # 5) mark done + persist state (atomic)
        store.mark_done(bucket, k)
        store.save(state)

        logger.log(
            {
                "event": "file_processed",
                "stage": stage,
                "src": src_path,
                "out": out_path,
                "done": done_path,
                "copied_to_next_in": bool(nxt),
                "size": len(data),
            }
        )

        processed += 1

    logger.log({"event": "stage_end", "stage": stage, "processed": processed, "in_count": len(entries)})
    return processed


def main() -> int:
    # credentials
    tok = _env("DROPBOX_REFRESH_TOKEN")
    app_key = _env("DROPBOX_APP_KEY")
    app_secret = _env("DROPBOX_APP_SECRET")

    # controls
    stage = _env("MONTHLY_STAGE", "00")
    max_files = int(_env("MAX_FILES_PER_RUN", "200") or "200")

    # state & logs
    state_path = _env("STATE_PATH", "/_system/state.json")  # IMPORTANT default
    logs_dir = _env("LOGS_DIR", "/_system/logs")

    io = DropboxIO(refresh_token=tok, app_key=app_key, app_secret=app_secret)
    logger = JsonlLogger(io, logs_dir=logs_dir)
    store = StateStore(io=io, state_path=state_path)

    logger.log({"event": "run_start", "stage": stage, "state_path": state_path, "logs_dir": logs_dir})

    if stage not in {"00", "10", "20", "30", "40"}:
        raise RuntimeError("MONTHLY_STAGE must be one of 00/10/20/30/40")

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