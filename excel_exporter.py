# -*- coding: utf-8 -*-
"""
excel_exporter.py

Goal:
- Provide a stable entrypoint `process_monthly_workbook` expected by monthly_pipeline_MULTISTAGE.py.
- Keep behavior safe (no destructive operations) and allow pipeline to proceed.
- If later stages require concrete artifacts, we'll implement them after seeing the next failure point.

This module intentionally implements a "no-op but well-logged" exporter to unblock the workflow.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class ExportResult:
    """A minimal structured return value that downstream code can inspect if needed."""
    ok: bool
    message: str
    details: Dict[str, Any]


def _log(msg: str) -> None:
    # monthly pipeline seems to rely on stdout logs in GitHub Actions
    print(f"[excel_exporter] {msg}")


def process_monthly_workbook(*args: Any, **kwargs: Any) -> ExportResult:
    """
    Compatibility shim.

    Expected (likely) call patterns in the pipeline:
    - process_monthly_workbook(dbx, state, cfg, xlsx_item, ...)
    - process_monthly_workbook(cfg=..., dbx=..., ...)
    - process_monthly_workbook(workbook_bytes=..., ...)

    We don't assume a specific signature to avoid breaking when pipeline code changes.
    This function:
    - logs what it received (types only, not secrets)
    - returns an ExportResult that indicates "skipped" safely.

    If later stages require actual outputs, we will implement:
    - download xlsx from Dropbox
    - read with openpyxl
    - generate required intermediate files (json/md/xlsx) to cfg dirs
    """
    try:
        # Extract some common objects if present, without assuming
        cfg = kwargs.get("cfg", None)
        dbx = kwargs.get("dbx", None)

        # If called positionally, attempt to detect cfg-like object
        if cfg is None:
            for a in args:
                # cfg probably has inbox_path/outbox_dir/overview_dir/prep_dir attributes
                if hasattr(a, "inbox_path") or hasattr(a, "outbox_dir") or hasattr(a, "overview_dir") or hasattr(a, "prep_dir"):
                    cfg = a
                    break

        # Log safely
        _log(f"called: args={len(args)} kwargs={sorted(list(kwargs.keys()))}")
        if cfg is not None:
            # do not print full paths if you consider them sensitive; but GitHub logs already mask env vars
            attrs = {}
            for k in ["inbox_path", "outbox_dir", "overview_dir", "prep_dir", "mode"]:
                if hasattr(cfg, k):
                    v = getattr(cfg, k)
                    attrs[k] = v
            _log(f"cfg attrs detected: {list(attrs.keys())}")

        if dbx is not None:
            _log("dbx provided (type only): " + type(dbx).__name__)

        # No-op success (skipped)
        return ExportResult(
            ok=True,
            message="excel exporter is currently a no-op shim (skipped).",
            details={
                "note": "Provide pipeline file expectations if you want real excel processing here.",
                "received_kwargs": sorted(list(kwargs.keys())),
                "received_args_types": [type(a).__name__ for a in args],
            },
        )

    except Exception as e:
        # Never crash the whole pipeline here; return failure info.
        _log(f"ERROR in shim: {type(e).__name__}: {e}")
        return ExportResult(
            ok=False,
            message=f"excel exporter shim failed: {type(e).__name__}",
            details={"error": str(e)},
        )