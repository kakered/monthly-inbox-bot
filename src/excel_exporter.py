# -*- coding: utf-8 -*-
"""Excel exporter for Monthly reports.

Goal (実務優先):
- 出力Excelの列をユーザーの仕様（monthly_spec.py）に厳密に合わせる
- “月報が無かったことにされる”を防ぐため、元テキスト（Full_Text）を必ず出力
- AI要約（Key+Lens / Draft_Feedback）は空欄でも動く（後で埋められる）

This module is intentionally conservative: it produces schema-correct workbooks first.
"""

from __future__ import annotations

import hashlib
import io
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import openpyxl

from .monthly_spec import (
    OVERVIEW_OUTPUT_COLUMNS,
    PER_PERSON_OUTPUT_COLUMNS,
)

# Optional: AI generation (存在しない環境でも動くように import guard)
try:
    from .openai_client import run_prompt_text
except Exception:  # pragma: no cover
    run_prompt_text = None  # type: ignore


def _sha256_text(parts: List[str]) -> str:
    h = hashlib.sha256()
    for p in parts:
        if p is None:
            continue
        if not isinstance(p, str):
            p = str(p)
        h.update(p.encode('utf-8', errors='ignore'))
        h.update(b'\n')
    return h.hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec='seconds')


def _guess_month(source_name: Optional[str]) -> str:
    """Try to infer YYYY-MM from filename patterns like *_2509_* => 2025-09.
    If not confident, return empty string.
    """
    if not source_name:
        return ''
    m = re.search(r'([12]\d{3})(0[1-9]|1[0-2])', source_name)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m2 = re.search(r'_(\d{2})(0[1-9]|1[0-2))', source_name)
    if m2:
        yy = int(m2.group(1))
        year = 2000 + yy
        return f"{year}-{m2.group(2)}"
    m3 = re.search(r'(\d{2})(0[1-9]|1[0-2))', source_name)
    return ''


def _load_workbook_from_bytes(data: bytes) -> openpyxl.Workbook:
    """Load .xlsx (ZIP) or .xls (OLE2) into an openpyxl Workbook."""
    if data[:2] == b'PK':
        return openpyxl.load_workbook(io.BytesIO(data))

    # OLE2 (legacy .xls) header starts with D0 CF 11 E0
    if data[:4] == b"\xD0\xCF\x11\xE0":
        # Requires pandas+xlrd; we import lazily.
        import pandas as pd  # type: ignore

        # pandas can read bytes via a temp file
        import tempfile

        with tempfile.NamedTemporaryFile(suffix='.xls', delete=True) as tf:
            tf.write(data)
            tf.flush()
            sheets = pd.read_excel(tf.name, sheet_name=None, header=None, engine='xlrd')

        wb = openpyxl.Workbook()
        # remove default sheet
        wb.remove(wb.active)
        for name, df in sheets.items():
            ws = wb.create_sheet(title=str(name)[:31])
            for r in range(df.shape[0]):
                for c in range(df.shape[1]):
                    v = df.iat[r, c]
                    if v != v:  # NaN
                        v = None
                    ws.cell(row=r + 1, column=c + 1, value=v)
        return wb

    # Fallback: try openpyxl anyway
    return openpyxl.load_workbook(io.BytesIO(data))


def _sheet_rows_as_dict(ws: openpyxl.worksheet.worksheet.Worksheet) -> List[Dict[str, Any]]:
    """Assume first non-empty row is header. Return list of dict rows."""
    max_col = ws.max_column
    header_row = None
    headers: List[str] = []
    for r in range(1, min(ws.max_row, 50) + 1):
        row = [ws.cell(row=r, column=c).value for c in range(1, max_col + 1)]
        if any(v not in (None, '') for v in row):
            header_row = r
            headers = [str(v).strip() if v not in (None, '') else f'COL_{c}' for c, v in enumerate(row, 1)]
            break
    if header_row is None:
        return []

    out: List[Dict[str, Any]] = []
    for r in range(header_row + 1, ws.max_row + 1):
        vals = [ws.cell(row=r, column=c).value for c in range(1, len(headers) + 1)]
        if all(v in (None, '') for v in vals):
            continue
        d = {headers[i]: vals[i] for i in range(len(headers))}
        out.append(d)
    return out


def _build_overview_row(row: Dict[str, Any], source_name: Optional[str] = None) -> Dict[str, Any]:
    """Map input row -> overview schema row (dict keyed by output column names)."""
    # Input keys (as in your sample workbook)
    person_id = str(row.get('社員ID', '') or '').strip()
    month = str(row.get('対象月', '') or '').strip()
    if not month:
        month = _guess_month(source_name)

    # Full texts
    rel = str(row.get('人間関係', '') or '').strip()
    qty = str(row.get('仕事の量', '') or '').strip()
    qual = str(row.get('仕事の質', '') or '').strip()
    pro = str(row.get('積極性', '') or '').strip()
    resp = str(row.get('責任性', '') or '').strip()
    hh = str(row.get('ヒヤリハット', '') or '').strip()
    free = str(row.get('自由記述（感想）', '') or '').strip()
    hq = str(row.get('本社への依頼事項', '') or '').strip()

    fp = _sha256_text([person_id, month, rel, qty, qual, pro, resp, hh, free, hq])

    out = {c: '' for c in OVERVIEW_OUTPUT_COLUMNS}
    out['Month / 対象月（YYYY-MM）'] = month
    out['Person_ID / 社員ID'] = person_id
    out['Content_Fingerprint / 入力内容指紋（sha256）'] = fp

    # Full_Text columns
    out['人間関係_Full_Text'] = rel
    out['仕事の量_Full_Text'] = qty
    out['仕事の質_Full_Text'] = qual
    out['積極性_Full_Text'] = pro
    out['責任性_Full_Text'] = resp
    out['ヒヤリハット_Full_Text'] = hh
    out['自由記述（感想）_Full_Text'] = free
    out['本社依頼事項_Full_Text'] = hq

    # (Optional) Keep simple timestamp in Notes so users can trace a run
    out['Notes（任意メモ）'] = f'generated_at={_now_iso()}'
    return out


def _build_per_person_row(ov: Dict[str, Any]) -> Dict[str, Any]:
    out = {c: '' for c in PER_PERSON_OUTPUT_COLUMNS}
    # Copy common columns
    for k, v in ov.items():
        if k in out:
            out[k] = v
    return out


def _write_xlsx(headers: List[str], rows: List[Dict[str, Any]], sheet_name: str) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]

    # Header
    for c, h in enumerate(headers, 1):
        ws.cell(row=1, column=c, value=h)

    # Rows
    for r, d in enumerate(rows, 2):
        for c, h in enumerate(headers, 1):
            ws.cell(row=r, column=c, value=d.get(h, ''))

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def process_monthly_workbook(
    xlsx_bytes: bytes,
    model: Optional[str] = None,
    depth: Optional[str] = None,
    password: Optional[str] = None,
    timeout: Optional[int] = None,
    source_name: Optional[str] = None,
) -> Tuple[bytes, bytes]:
    """Return (overview_xlsx_bytes, per_person_xlsx_bytes)."""
    wb = _load_workbook_from_bytes(xlsx_bytes)
    ws = wb.active
    rows = _sheet_rows_as_dict(ws)

    overview_rows: List[Dict[str, Any]] = []
    per_person_rows: List[Dict[str, Any]] = []

    for row in rows:
        # Skip rows without 社員ID (your sheet often has empty B/C etc)
        pid = str(row.get('社員ID', '') or '').strip()
        if not pid:
            continue
        ov = _build_overview_row(row, source_name=source_name)

        # Optional AI (only if run_prompt_text is available)
        if run_prompt_text and model:
            # Keep it minimal: generate Draft_Feedback only when possible
            # (Key+Lens columns can be added later; this preserves the schema now)
            system = os.getenv('FEEDBACK_DRAFT_SYSTEM', '')
            user = os.getenv('FEEDBACK_DRAFT_USER', '')
            # If env not set, just use monthly_spec prompt via alias
            if not system:
                from .monthly_spec import FEEDBACK_DRAFT_SYSTEM as _sys
                system = _sys
            if not user:
                from .monthly_spec import FEEDBACK_DRAFT_USER as _usr
                user = _usr

            # Give the model the full text so the “月報が無い”にならない
            context = (
                f"Person_ID: {ov['Person_ID / 社員ID']}\n"
                f"Month: {ov['Month / 対象月（YYYY-MM）']}\n\n"
                f"[人間関係]\n{ov['人間関係_Full_Text']}\n\n"
                f"[仕事の量]\n{ov['仕事の量_Full_Text']}\n\n"
                f"[仕事の質]\n{ov['仕事の質_Full_Text']}\n\n"
                f"[積極性]\n{ov['積極性_Full_Text']}\n\n"
                f"[責任性]\n{ov['責任性_Full_Text']}\n\n"
                f"[ヒヤリハット]\n{ov['ヒヤリハット_Full_Text']}\n\n"
                f"[自由記述（感想）]\n{ov['自由記述（感想）_Full_Text']}\n\n"
                f"[本社依頼事項]\n{ov['本社依頼事項_Full_Text']}\n"
            )

            try:
                draft = run_prompt_text(
                    prompt=user + "\n\n" + context,
                    system=system,
                    model=model,
                    timeout=timeout,
                )
                ov['Draft_Feedback（草案本文）'] = (draft or '').strip()
            except Exception as e:  # pragma: no cover
                ov['Notes（任意メモ）'] = (ov.get('Notes（任意メモ）', '') + f' | ai_error={e}')

        overview_rows.append(ov)
        per_person_rows.append(_build_per_person_row(ov))

    overview_bytes = _write_xlsx(OVERVIEW_OUTPUT_COLUMNS, overview_rows, sheet_name='overview')
    per_person_bytes = _write_xlsx(PER_PERSON_OUTPUT_COLUMNS, per_person_rows, sheet_name='per_person')
    return overview_bytes, per_person_bytes
