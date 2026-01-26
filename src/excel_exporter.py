# -*- coding: utf-8 -*-
from __future__ import annotations

import io
from typing import Optional, Tuple

import openpyxl


def process_monthly_workbook(xlsx_bytes: bytes, password: Optional[str] = None) -> Tuple[bytes, bytes]:
    """
    Minimal, robust placeholder:
      - open the workbook
      - (optionally) you can add transformations here
      - return (overview_xlsx_bytes, per_person_xlsx_bytes)

    Note:
      - openpyxl cannot decrypt password-protected Excel.
      - If you need password support later, we will switch to a decrypt step (e.g., msoffcrypto-tool).
    """
    bio = io.BytesIO(xlsx_bytes)

    # If the file is encrypted, openpyxl will raise.
    wb = openpyxl.load_workbook(bio)

    # --- TODO: put your real logic here ---
    # For now: just save as-is into two outputs.
    out1 = io.BytesIO()
    wb.save(out1)

    out2 = io.BytesIO()
    wb.save(out2)

    return out1.getvalue(), out2.getvalue()