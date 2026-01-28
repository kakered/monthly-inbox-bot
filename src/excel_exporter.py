# -*- coding: utf-8 -*-
"""
excel_exporter.py
Stage10: Excel preformat (py)
- input xlsx bytes -> output xlsx bytes (overview/per_person)

※ まずは「壊さない最小の編集」で2派生を作る。
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Dict, Any, Tuple

import openpyxl


@dataclass
class PreformatResult:
    overview_bytes: bytes
    per_person_bytes: bytes


def process_monthly_workbook(xlsx_bytes: bytes) -> PreformatResult:
    """
    いまは最小実装：
    - overview: 先頭シート名を 'overview' に寄せる（存在する範囲で）
    - per_person: 先頭シート名を 'per_person' に寄せる
    """
    wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes))

    # --- overview copy ---
    wb_over = wb
    try:
        wb_over.active.title = "overview"
    except Exception:
        pass
    buf1 = io.BytesIO()
    wb_over.save(buf1)
    overview_bytes = buf1.getvalue()

    # --- per_person copy (reload to avoid side effects) ---
    wb2 = openpyxl.load_workbook(io.BytesIO(xlsx_bytes))
    try:
        wb2.active.title = "per_person"
    except Exception:
        pass
    buf2 = io.BytesIO()
    wb2.save(buf2)
    per_person_bytes = buf2.getvalue()

    return PreformatResult(overview_bytes=overview_bytes, per_person_bytes=per_person_bytes)