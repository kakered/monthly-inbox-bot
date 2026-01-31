# -*- coding: utf-8 -*-
"""
Monthly pipeline entry (GitHub Actions-friendly):
- No positional argv required.
- Uses env vars (MONTHLY_STAGE, STAGExx_IN/OUT/DONE, LOGS_DIR, STATE_PATH, etc.)
- Creates minimal audit records and uploads them to Dropbox logs folder.
"""

from __future__ import annotations

import os
import sys
import traceback
from typing import Callable, Optional, Tuple

from src.monthly_cfg import MonthlyConfig
from src.logger import write_audit_record
from src.state_store import StateStore
from src.utils_dropbox_item import DropboxClient


def _env(key: str, default: str = "") -> str:
    return (os.environ.get(key, default) or default).strip()


def _require_env(key: str) -> str:
    v = _env(key)
    if not v:
        raise RuntimeError(f"Missing required env: {key}")
    return v


def _import_pipeline_entry() -> Callable:
    """
    Resolve pipeline function dynamically to keep monthly_main small and stable.
    Preferred: src.monthly_pipeline_MULTISTAGE.run
    """
    try:
        from src.monthly_pipeline_MULTISTAGE import run as pipeline_run  # type: ignore
        return pipeline_run
    except Exception as e:
        raise RuntimeError("Failed to import pipeline entry: src.monthly_pipeline_MULTISTAGE.run") from e


def _call_pipeline(func: Callable, dbx: DropboxClient, state: StateStore, cfg: MonthlyConfig) -> int:
    """
    Call pipeline with flexible signature:
    - run(dbx, state, cfg)
    - run(cfg, dbx, state)
    - run(cfg)
    """
    try:
        # Most likely: (dbx, state, cfg)
        return int(func(dbx, state, cfg))
    except TypeError:
        pass

    try:
        # Alternate: (cfg, dbx, state)
        return int(func(cfg, dbx, state))
    except TypeError:
        pass

    try:
        # Minimal: (cfg)
        return int(func(cfg))
    except TypeError as e:
        raise RuntimeError(
            "Pipeline entry signature not supported. "
            "Expected one of: run(dbx, state, cfg) / run(cfg, dbx, state) / run(cfg)"
        ) from e


def _resolve_stage_paths(stage: str) -> Tuple[str, str, str]:
    stage = stage.zfill(2)
    p_in = _env(f"STAGE{stage}_IN")
    p_out = _env(f"STAGE{stage}_OUT")
    p_done = _env(f"STAGE{stage}_DONE")
    return p_in, p_out, p_done


def main() -> int:
    # ---- required env ----
    dropbox_refresh_token = _require_env("DROPBOX_REFRESH_TOKEN")
    dropbox_app_key = _require_env("DROPBOX_APP_KEY")
    dropbox_app_secret = _require_env("DROPBOX_APP_SECRET")

    state_path = _require_env("STATE_PATH")
    logs_dir = _require_env("LOGS_DIR")

    stage = _env("MONTHLY_STAGE", "00") or "00"
    stage = stage.strip()

    # stage folder env sanity (not strictly required here, but pipeline will use them)
    p_in, p_out, p_done = _resolve_stage_paths(stage)

    # ---- client ----
    dbx = DropboxClient(
        oauth2_refresh_token=dropbox_refresh_token,
        app_key=dropbox_app_key,
        app_secret=dropbox_app_secret,
    )

    # ---- state ----
    state = StateStore(dbx=dbx, state_path_dropbox=state_path)

    # ---- config ----
    cfg = MonthlyConfig.from_env()
    cfg.monthly_stage = stage

    # Helpful context (stdout only; do NOT print secrets)
    print(f"[monthly_main] stage={stage!r}", flush=True)
    print(f"[monthly_main] folders: IN={p_in!r} OUT={p_out!r} DONE={p_done!r}", flush=True)
    print(f"[monthly_main] state_path={state_path!r} logs_dir={logs_dir!r}", flush=True)
    print(f"[monthly_main] model={_env('OPENAI_MODEL')} depth={_env('DEPTH')}", flush=True)

    # Ensure logs folder exists (best-effort)
    try:
        dbx.ensure_folder(logs_dir)
    except Exception:
        print("[monthly_main] ensure_folder(logs_dir) failed (non-fatal)", file=sys.stderr)
        traceback.print_exc()

    # --- audit: start snapshot ---
    try:
        local_audit = write_audit_record(
            dbx,
            logs_dir_dropbox=logs_dir,
            folders={"in": p_in, "out": p_out, "done": p_done},
            event="start",
            extra={
                "monthly_stage": stage,
                "openai_model": _env("OPENAI_MODEL"),
            },
        )
        try:
            dbx.upload(local_audit, f"{logs_dir}/" + os.path.basename(local_audit), mode="overwrite")
        except Exception:
            print("[audit] upload failed (start)", file=sys.stderr)
            traceback.print_exc()
    except Exception:
        local_audit = ""
        print("[audit] write failed (start)", file=sys.stderr)
        traceback.print_exc()

    # --- run pipeline ---
    rc = 1
    try:
        func = _import_pipeline_entry()
        rc = _call_pipeline(func, dbx, state, cfg)
    except Exception:
        print("[monthly_main] pipeline crashed", file=sys.stderr)
        traceback.print_exc()
        rc = 1

    # --- audit: end snapshot ---
    try:
        rid = ""
        if local_audit:
            rid = os.path.basename(local_audit).replace("monthly_audit_", "").replace(".jsonl", "")
        local_audit2 = write_audit_record(
            dbx,
            logs_dir_dropbox=logs_dir,
            folders={"in": p_in, "out": p_out, "done": p_done},
            event="end",
            extra={"return_code": rc},
            run_id=rid or None,
        )
        try:
            dbx.upload(local_audit2, f"{logs_dir}/" + os.path.basename(local_audit2), mode="overwrite")
        except Exception:
            print("[audit] upload failed (end)", file=sys.stderr)
            traceback.print_exc()
    except Exception:
        print("[audit] write failed (end)", file=sys.stderr)
        traceback.print_exc()

    return int(rc)


if __name__ == "__main__":
    raise SystemExit(main())