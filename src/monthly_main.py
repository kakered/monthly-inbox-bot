# -*- coding: utf-8 -*-
from __future__ import annotations

"""src.monthly_main

- MONTHLY_MODE=multistage: multistage pipeline を実行
- それ以外: 単発（月報→overview/per_person）を実行

NEW (logging):
- 1 run = 1 log file on Dropbox:
  /_system/logs/YYYY-MM-DD/monthly_YYYYMMDD-HHMMSS_<runid>.log
- Always logs START/END + config snapshot
- On error: logs full traceback then re-raises (GitHub Actions fails =気づける)
"""

import os
import sys
import uuid
import traceback
from datetime import datetime, timezone, timedelta
from typing import Optional, List


# -------------------------
# Time helpers (JST)
# -------------------------
_JST = timezone(timedelta(hours=9))


def _now_jst() -> datetime:
    return datetime.now(tz=_JST)


def _ts_jst() -> str:
    return _now_jst().strftime("%Y-%m-%d %H:%M:%S JST")


def _date_jst() -> str:
    return _now_jst().strftime("%Y-%m-%d")


def _stamp_jst_compact() -> str:
    return _now_jst().strftime("%Y%m%d-%H%M%S")


# -------------------------
# Dropbox logging
# -------------------------
def _norm_dbx_path(p: str) -> str:
    p = (p or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    return p


def _ensure_folder_best_effort(dbx_io, folder: str) -> None:
    """
    Best-effort folder creation.
    - Works even if DropboxIO has no ensure_folder().
    - Never raises (logging should not break pipeline).
    """
    try:
        folder = _norm_dbx_path(folder)
        # If DropboxIO has ensure_folder, use it.
        if hasattr(dbx_io, "ensure_folder") and callable(getattr(dbx_io, "ensure_folder")):
            dbx_io.ensure_folder(folder)  # type: ignore
            return

        # Fallback: call Dropbox SDK directly
        dbx = getattr(dbx_io, "dbx", None)
        if dbx is None:
            return

        try:
            dbx.files_get_metadata(folder)
            return
        except Exception:
            pass

        try:
            dbx.files_create_folder_v2(folder)
        except Exception:
            return
    except Exception:
        return


class RunLogger:
    """
    Minimal run logger that writes to Dropbox (overwrite on flush).
    Designed to never crash the main pipeline.
    """

    def __init__(self, dbx_io, log_path: str):
        self.dbx_io = dbx_io
        self.log_path = _norm_dbx_path(log_path)
        self.lines: List[str] = []

    def log(self, msg: str) -> None:
        try:
            line = f"[{_ts_jst()}] {msg}\n"
            self.lines.append(line)
        except Exception:
            # never crash
            pass

    def flush(self) -> None:
        try:
            text = "".join(self.lines)
            # DropboxIO has upload_text in your repo snapshot
            if hasattr(self.dbx_io, "upload_text") and callable(getattr(self.dbx_io, "upload_text")):
                self.dbx_io.upload_text(self.log_path, text, mode="overwrite")  # type: ignore
                return

            # fallback: upload_bytes if only binary uploader exists
            if hasattr(self.dbx_io, "upload_bytes") and callable(getattr(self.dbx_io, "upload_bytes")):
                self.dbx_io.upload_bytes(self.log_path, text.encode("utf-8"), mode="overwrite")  # type: ignore
                return

            # last resort: direct SDK
            dbx = getattr(self.dbx_io, "dbx", None)
            if dbx is None:
                return
            from dropbox.files import WriteMode  # local import
            dbx.files_upload(text.encode("utf-8"), self.log_path, mode=WriteMode.overwrite, mute=True)
        except Exception:
            # logging must never crash pipeline
            return


def _build_log_path(run_id: str) -> str:
    root = os.getenv("MONTHLY_LOG_ROOT", "/_system/logs").strip() or "/_system/logs"
    root = _norm_dbx_path(root).rstrip("/")
    day_dir = f"{root}/{_date_jst()}"
    filename = f"monthly_{_stamp_jst_compact()}_{run_id}.log"
    return day_dir, f"{day_dir}/{filename}"


def _snapshot_cfg() -> str:
    """
    Log-friendly config snapshot (NO secrets).
    """
    keys = [
        "MONTHLY_MODE",
        "MONTHLY_STAGE",
        "MONTHLY_INBOX_PATH",
        "MONTHLY_PREP_DIR",
        "MONTHLY_OUTBOX_DIR",
        "MONTHLY_OVERVIEW_DIR",
        "STATE_PATH",
        "OPENAI_MODEL",
        "DEPTH",
        "MAX_FILES_PER_RUN",
        "MONTHLY_MAX_FILES",
    ]
    parts = []
    for k in keys:
        v = os.getenv(k, "")
        if v is None:
            v = ""
        v = str(v).strip()
        if v:
            parts.append(f"{k}={v}")
    # GitHub Actions context (safe)
    sha = (os.getenv("GITHUB_SHA") or "").strip()
    if sha:
        parts.append(f"GITHUB_SHA={sha[:12]}")
    return " ".join(parts) if parts else "(no env snapshot)"


# -------------------------
# main
# -------------------------
def main() -> None:
    # Always create Dropbox logger first (best effort).
    run_id = uuid.uuid4().hex[:8]

    # Build Dropbox logger using existing DropboxIO
    logger: Optional[RunLogger] = None
    dbx_io = None
    try:
        from .dropbox_io import DropboxIO  # lazy import
        dbx_io = DropboxIO.from_env()
        day_dir, log_path = _build_log_path(run_id)
        _ensure_folder_best_effort(dbx_io, day_dir)
        logger = RunLogger(dbx_io, log_path)
        logger.log(f"START run_id={run_id}")
        logger.log(f"cfg { _snapshot_cfg() }")
    except Exception:
        # If Dropbox logger cannot be initialized, continue without it.
        logger = None

    def _log(msg: str) -> None:
        if logger:
            logger.log(msg)

    def _flush() -> None:
        if logger:
            logger.flush()

    try:
        mode = (os.getenv("MONTHLY_MODE") or "single").strip().lower()
        _log(f"mode={mode}")

        if mode == "multistage":
            _log("stage=multistage ENTER")
            _flush()
            from .monthly_pipeline_MULTISTAGE import run_multistage

            run_multistage()
            _log("stage=multistage EXIT ok=True")
            _flush()
            return

        # -------------------------
        # single-stage (legacy)
        # -------------------------
        _log("stage=single ENTER")
        _flush()

        from .excel_exporter import process_monthly_workbook

        inbox_path = os.getenv("MONTHLY_INBOX_PATH", "/0-Inbox/monthlyreports")
        outbox_dir = os.getenv("MONTHLY_OUTBOX_DIR", "/0-Outbox/monthly")
        password = os.getenv("RPA_XLSX_PASSWORD") or None

        items = dbx_io.list_folder(inbox_path) if dbx_io else []
        target = None
        for it in items:
            name = getattr(it, "name", "")
            if str(name).lower().endswith((".xlsx", ".xls")):
                target = it
                break

        if not target:
            _log(f"No Excel found under: {inbox_path}")
            _flush()
            print(f"[MONTHLY] No Excel found under: {inbox_path}")
            return

        path = getattr(target, "path", None) or getattr(target, "path_lower", None)
        _log(f"Processing: {path}")
        _flush()

        xlsx_bytes = dbx_io.download_to_bytes(path)

        overview_bytes, per_person_bytes = process_monthly_workbook(
            xlsx_bytes=xlsx_bytes,
            password=password,
        )

        base = os.path.basename(path)
        ts = os.getenv("MONTHLY_TS") or _stamp_jst_compact()

        overview_name = f"{base}__overview__{ts}.xlsx"
        per_name = f"{base}__per_person__{ts}.xlsx"

        # ensure outbox folder exists (best effort)
        _ensure_folder_best_effort(dbx_io, outbox_dir)

        dbx_io.upload_bytes(f"{outbox_dir}/{overview_name}", overview_bytes)
        dbx_io.upload_bytes(f"{outbox_dir}/{per_name}", per_person_bytes)

        _log(f"Wrote: {outbox_dir}/{overview_name}")
        _log(f"Wrote: {outbox_dir}/{per_name}")
        _log("stage=single EXIT ok=True")
        _flush()

        print(f"[MONTHLY] Wrote: {outbox_dir}/{overview_name}")
        print(f"[MONTHLY] Wrote: {outbox_dir}/{per_name}")

    except Exception as e:
        tb = traceback.format_exc()
        _log(f"ERROR type={type(e).__name__} msg={e}")
        _log("TRACEBACK_BEGIN")
        if logger:
            # traceback lines can be large; write as-is
            for line in tb.splitlines():
                logger.lines.append(line + "\n")
        _log("TRACEBACK_END")
        _log("END ok=False")
        _flush()
        # Re-raise so GitHub Actions shows failure
        raise

    finally:
        _log("END ok=True (finally)")
        _flush()


if __name__ == "__main__":
    main()