# -*- coding: utf-8 -*-
from __future__ import annotations

import os


def main() -> int:
    mode = (os.getenv("MONTHLY_MODE") or "multistage").strip().lower()

    if mode == "multistage":
        from .monthly_pipeline_MULTISTAGE import run_multistage

        run_multistage()
        return 0

    # single (legacy) - keep minimal behavior if you ever use it
    from .dropbox_io import DropboxIO
    from .excel_exporter import process_monthly_workbook

    inbox_path = os.getenv("MONTHLY_INBOX_PATH", "/00_inbox_raw/IN")
    outbox_dir = os.getenv("MONTHLY_OUTBOX_DIR", "/30_personalize_py/OUT")

    dbx = DropboxIO.from_env()
    dbx.ensure_folder(outbox_dir)

    items = dbx.list_folder(inbox_path)
    target = None
    for it in items:
        name = getattr(it, "name", None)
        if isinstance(name, str) and name.lower().endswith((".xlsx", ".xlsm", ".xls")):
            target = it
            break

    if not target:
        print(f"[MONTHLY] No Excel found under: {inbox_path}")
        return 0

    path = getattr(target, "path_display", None) or getattr(target, "path_lower", None)
    if not isinstance(path, str):
        print("[MONTHLY] Could not resolve target path")
        return 1

    data = dbx.download_to_bytes(path)
    overview_bytes, per_person_bytes = process_monthly_workbook(data, password=None)

    base = os.path.splitext(os.path.basename(path))[0]
    dbx.upload_bytes(f"{outbox_dir}/{base}__overview.xlsx", overview_bytes)
    dbx.upload_bytes(f"{outbox_dir}/{base}__per_person.xlsx", per_person_bytes)

    print(f"[MONTHLY] wrote outputs to: {outbox_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())