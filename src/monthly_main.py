# -*- coding: utf-8 -*-
from __future__ import annotations

"""src.monthly_main

- MONTHLY_MODE=multistage: multistage pipeline を実行
- それ以外: 単発（月報→overview/per_person）を実行
"""

import os
from datetime import datetime


def main() -> None:
    mode = (os.getenv("MONTHLY_MODE") or "single").strip().lower()

    if mode == "multistage":
        from .monthly_pipeline_MULTISTAGE import run_multistage

        run_multistage()
        return

    # single-stage
    from .dropbox_io import DropboxIO
    from .excel_exporter import process_monthly_workbook

    inbox_path = os.getenv("MONTHLY_INBOX_PATH", "/0-Inbox/monthlyreports")
    outbox_dir = os.getenv("MONTHLY_OUTBOX_DIR", "/0-Outbox/monthly")
    password = os.getenv("RPA_XLSX_PASSWORD") or None

    dbx = DropboxIO.from_env()
    items = dbx.list_folder(inbox_path)

    target = None
    for it in items:
        name = getattr(it, "name", "")
        if name.lower().endswith((".xlsx", ".xls")):
            target = it
            break

    if not target:
        print(f"[MONTHLY] No Excel found under: {inbox_path}")
        return

    path = getattr(target, "path", None) or getattr(target, "path_lower", None)
    print(f"[MONTHLY] Processing: {path}")

    xlsx_bytes = dbx.download_to_bytes(path)
    overview_bytes, per_person_bytes = process_monthly_workbook(
        xlsx_bytes=xlsx_bytes,
        password=password,
    )

    base = os.path.basename(path)
    ts = os.getenv("MONTHLY_TS") or datetime.now().strftime("%Y%m%d-%H%M%S")

    overview_name = f"{base}__overview__{ts}.xlsx"
    per_name = f"{base}__per_person__{ts}.xlsx"

    dbx.ensure_folder(outbox_dir)
    dbx.upload_bytes(f"{outbox_dir}/{overview_name}", overview_bytes)
    dbx.upload_bytes(f"{outbox_dir}/{per_name}", per_person_bytes)

    print(f"[MONTHLY] Wrote: {outbox_dir}/{overview_name}")
    print(f"[MONTHLY] Wrote: {outbox_dir}/{per_name}")


if __name__ == "__main__":
    main()
