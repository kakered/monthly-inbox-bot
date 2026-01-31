# -*- coding: utf-8 -*-
"""
monthly_main.py
Monthly pipeline orchestrator.

GitHub Actions runs:
  python -m src.monthly_main

This module MUST NOT require CLI positional args.
Control via env vars:
- MONTHLY_STAGE: "00" | "10" | "20" | "30" | "40"  (default: "00")
- DEPTH: "medium" | "heavy" (default: "medium")
- OPENAI_MODEL: e.g., "gpt-5-mini" (default: "gpt-5-mini")
- MAX_FILES_PER_RUN (default: 10)
- MAX_INPUT_CHARS (default: 80000)
- OPENAI_MAX_OUTPUT_TOKENS (default: 5000)
- OPENAI_TIMEOUT (default: 90)
- LOGS_DIR (default: "/_system/logs")
- STATE_PATH (default: "/_system/state_monthly.json")
- STAGE00_IN, STAGE00_OUT, STAGE00_DONE, ... similarly for other stages

Secrets required:
- OPENAI_API_KEY
- DROPBOX_REFRESH_TOKEN
- DROPBOX_APP_KEY
- DROPBOX_APP_SECRET
"""

from __future__ import annotations

import os
import time
import traceback
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from .dropbox_io import DropboxIO
from .logger import JsonlLogger
from .state_store import StateStore, stable_key
from .utils_dropbox_item import dbx_get, dbx_is_folder, dbx_name, dbx_path


@dataclass
class Cfg:
    stage: str
    depth: str
    model: str
    max_files: int
    max_input_chars: int
    max_output_tokens: int
    timeout: int

    logs_dir: str
    state_path: str

    in_dir: str
    out_dir: str
    done_dir: str


def _env(key: str, default: str) -> str:
    v = os.environ.get(key)
    if v is None or str(v).strip() == "":
        return default
    return str(v).strip()


def _env_int(key: str, default: int) -> int:
    v = os.environ.get(key)
    if v is None or str(v).strip() == "":
        return int(default)
    try:
        return int(str(v).strip())
    except Exception:
        return int(default)


def load_cfg() -> Cfg:
    stage = _env("MONTHLY_STAGE", "00")
    if stage not in {"00", "10", "20", "30", "40"}:
        stage = "00"

    depth = _env("DEPTH", "medium").lower()
    if depth not in {"medium", "heavy"}:
        depth = "medium"

    model = _env("OPENAI_MODEL", "gpt-5-mini")
    max_files = _env_int("MAX_FILES_PER_RUN", 10)
    max_input_chars = _env_int("MAX_INPUT_CHARS", 80000)
    max_output_tokens = _env_int("OPENAI_MAX_OUTPUT_TOKENS", 5000)
    timeout = _env_int("OPENAI_TIMEOUT", 90)

    logs_dir = _env("LOGS_DIR", "/_system/logs")
    state_path = _env("STATE_PATH", "/_system/state_monthly.json")

    in_dir = _env(f"STAGE{stage}_IN", f"/{stage}_inbox/IN")
    out_dir = _env(f"STAGE{stage}_OUT", f"/{stage}_inbox/OUT")
    done_dir = _env(f"STAGE{stage}_DONE", f"/{stage}_inbox/DONE")

    return Cfg(
        stage=stage,
        depth=depth,
        model=model,
        max_files=max_files,
        max_input_chars=max_input_chars,
        max_output_tokens=max_output_tokens,
        timeout=timeout,
        logs_dir=logs_dir,
        state_path=state_path,
        in_dir=in_dir,
        out_dir=out_dir,
        done_dir=done_dir,
    )


def _now_run_id() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def _list_inbox_files(dbx: DropboxIO, in_dir: str) -> List[Any]:
    entries = dbx.list_folder(in_dir)
    out: List[Any] = []
    for it in entries:
        if dbx_is_folder(it):
            continue
        p = dbx_path(it)
        if not p:
            continue
        out.append(it)
    return out


def _sort_entries(entries: List[Any]) -> List[Any]:
    # stable sort by name then path (so deterministic)
    def keyfn(it: Any) -> Tuple[str, str]:
        n = dbx_name(it) or ""
        p = dbx_path(it) or ""
        return (n.lower(), p.lower())

    return sorted(entries, key=keyfn)


def _basename_without_ext(name: str) -> str:
    # handles .xlsx, .xls, etc.
    if "." not in name:
        return name
    return ".".join(name.split(".")[:-1])


def _out_filename(src_name: str, stage: str) -> str:
    base = _basename_without_ext(src_name)
    stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    return f"{base}__stage{stage}__{stamp}.xlsx"


def _process_stage00(dbx: DropboxIO, cfg: Cfg, state: StateStore, logger: JsonlLogger) -> int:
    """
    Stage00: 'raw inbox' -> 'normalized excel'
    For now, this stage simply copies the file to OUT with renaming.
    (Your next iterations can transform, split rows, etc.)
    """
    entries = _sort_entries(_list_inbox_files(dbx, cfg.in_dir))
    processed = 0

    for it in entries[: cfg.max_files]:
        src_path = dbx_path(it)
        src_name = dbx_name(it) or "input.xlsx"
        if not src_path:
            continue

        # key uses path + content_hash if present
        ch = dbx_get(it, "content_hash", None)
        key = stable_key(src_path, ch if isinstance(ch, str) else None)
        if state.is_processed(key):
            logger.log(event="skip_already_processed", stage=cfg.stage, src=src_path)
            continue

        try:
            b = dbx.download_to_bytes(src_path)
            out_name = _out_filename(src_name, cfg.stage)
            out_path = f"{cfg.out_dir.rstrip('/')}/{out_name}"
            dbx.write_file_bytes(out_path, b, overwrite=True)

            # move original to DONE
            dst_done = f"{cfg.done_dir.rstrip('/')}/{src_name}"
            dbx.move(src_path, dst_done, autorename=True)

            state.mark_processed(key, f"{cfg.stage}:{_now_run_id()}")
            state.add_done(src_path)
            state.log(event="processed", stage=cfg.stage, src=src_path, out=out_path, done=dst_done)
            logger.log(event="processed", stage=cfg.stage, src=src_path, out=out_path, done=dst_done)
            processed += 1
        except Exception as e:
            tb = traceback.format_exc()
            err = {"src": src_path, "error": repr(e), "traceback": tb, "stage": cfg.stage}
            state.add_error(err)
            state.log(event="error", **err)
            logger.log(event="error", **err)

    return processed


def main() -> int:
    run_id = _now_run_id()
    cfg = load_cfg()

    # Dropbox client
    dbx = DropboxIO.from_env()

    # logger (writes to Dropbox via DropboxIO)
    logger = JsonlLogger(dbx=dbx, logs_dir=cfg.logs_dir, prefix=f"stage{cfg.stage}", run_id=run_id)

    # state
    state = StateStore.load(dbx=dbx, path=cfg.state_path)

    logger.log(event="run_start", stage=cfg.stage, run_id=run_id, cfg=cfg.__dict__)
    state.log(event="run_start", stage=cfg.stage, run_id=run_id)

    processed = 0
    try:
        if cfg.stage == "00":
            processed = _process_stage00(dbx, cfg, state, logger)
        else:
            # Placeholder for other stages
            logger.log(event="noop_stage", stage=cfg.stage, msg="Stage not implemented yet")
            state.log(event="noop_stage", stage=cfg.stage, msg="Stage not implemented yet")

        # save state
        state.save(dbx)
        # flush audit
        audit_path = state.flush_audit_jsonl(dbx, cfg.logs_dir, run_id)
        logger.log(event="audit_flushed", stage=cfg.stage, path=audit_path)

        logger.log(event="run_end", stage=cfg.stage, processed=processed)
        return 0
    except Exception as e:
        tb = traceback.format_exc()
        logger.log(event="fatal", stage=cfg.stage, error=repr(e), traceback=tb)
        state.add_error({"stage": cfg.stage, "error": repr(e), "traceback": tb})
        try:
            state.save(dbx)
        except Exception:
            pass
        return 1
    finally:
        try:
            logger.flush()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())