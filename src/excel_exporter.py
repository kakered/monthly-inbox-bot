# -*- coding: utf-8 -*-
"""
excel_exporter.py

Stage20: Read Excel from STAGE20_IN, fill feedback, write to STAGE20_OUT,
then move input to STAGE20_DONE.

- Uses OpenAI Responses API via HTTPS (no openai python package required).
- Column mapping (default):
  - A: input text (相談/振り返り etc.)
  - B: output text (フィードバック)
You can tweak by env vars:
  EXCEL_INPUT_COL (default "A")
  EXCEL_OUTPUT_COL (default "B")
  EXCEL_START_ROW (default "2")
"""

from __future__ import annotations

import os
import io
import re
import json
import time
import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Any, Optional, List, Tuple

import requests
from openpyxl import load_workbook


JST = dt.timezone(dt.timedelta(hours=9))


def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return default if v is None else str(v)


def _now_jst_compact() -> str:
    return dt.datetime.now(JST).strftime("%Y%m%d_%H%M%S")


def _sha12(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]


def _safe_name(name: str) -> str:
    name = re.sub(r"[^\w.\-]+", "_", name, flags=re.UNICODE)
    name = re.sub(r"_+", "_", name).strip("_")
    return name or "file"


def _col_to_index(col: str) -> int:
    col = col.strip().upper()
    n = 0
    for ch in col:
        if "A" <= ch <= "Z":
            n = n * 26 + (ord(ch) - ord("A") + 1)
    return max(1, n)


@dataclass
class OpenAIConfig:
    api_key: str
    model: str
    timeout: int
    max_retries: int


def _openai_call(cfg: OpenAIConfig, prompt: str) -> str:
    """
    Calls OpenAI Responses API and returns plain text output.
    Keeps it minimal and robust.
    """
    url = "https://api.openai.com/v1/responses"
    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": cfg.model,
        "input": prompt,
        "max_output_tokens": int(_env("OPENAI_MAX_OUTPUT_TOKENS", "2000")),
    }

    last_err: Optional[Exception] = None
    for i in range(cfg.max_retries + 1):
        try:
            r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=cfg.timeout)
            if r.status_code >= 400:
                raise RuntimeError(f"OpenAI HTTP {r.status_code}: {r.text[:300]}")
            data = r.json()
            # Responses API: try common places
            # 1) output_text helper field (often present)
            if isinstance(data, dict) and isinstance(data.get("output_text"), str) and data["output_text"].strip():
                return data["output_text"].strip()

            # 2) output[*].content[*].text
            outs = data.get("output", [])
            texts: List[str] = []
            for o in outs:
                for c in o.get("content", []) if isinstance(o, dict) else []:
                    t = c.get("text") if isinstance(c, dict) else None
                    if isinstance(t, str) and t.strip():
                        texts.append(t.strip())
            if texts:
                return "\n\n".join(texts).strip()

            return ""
        except Exception as e:
            last_err = e
            if i < cfg.max_retries:
                time.sleep(1.5 * (i + 1))
                continue
            raise RuntimeError(f"OpenAI call failed after retries: {last_err!r}") from last_err


def _default_feedback_prompt(user_text: str) -> str:
    # JP主体 + EN補助（あなたの好みに合わせた"無難に強い"テンプレ）
    return f"""あなたは、対象者と同じ職場で日常的に業務を見ている立場の、落ち着いた現場の先輩です。
目的は「実際のフィードバック担当者が、要点を絞って、少し丁寧に書いた」と自然に読める月報フィードバックを作ることです。

制約:
- 偉そうに評価しない / 押し付けない
- 入力に書かれていない背景・意図・性格を推測しない
- 事実と提案を混ぜない（事実は事実、提案は提案）
- 200〜400字目安（長い場合は箇条書き少しOK）
- 最後に1つだけ「次に試す小さな一手」を添える

入力:
{user_text}
"""


def process_monthly_workbook(dbx: Any, state: Any, cfg: Any) -> int:
    """
    Expected to be called from monthly_pipeline_MULTISTAGE.stage2_api

    Uses env vars for Dropbox paths:
      STAGE20_IN, STAGE20_OUT, STAGE20_DONE
    """
    stage_in = _env("STAGE20_IN")
    stage_out = _env("STAGE20_OUT")
    stage_done = _env("STAGE20_DONE")

    if not stage_in or not stage_out or not stage_done:
        raise RuntimeError("Missing env vars: STAGE20_IN/STAGE20_OUT/STAGE20_DONE")

    api_key = _env("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY")

    oai = OpenAIConfig(
        api_key=api_key,
        model=_env("OPENAI_MODEL", "gpt-5-mini"),
        timeout=int(_env("OPENAI_TIMEOUT", "120")),
        max_retries=int(_env("OPENAI_MAX_RETRIES", "2")),
    )

    in_col = _env("EXCEL_INPUT_COL", "A")
    out_col = _env("EXCEL_OUTPUT_COL", "B")
    start_row = int(_env("EXCEL_START_ROW", "2"))

    in_idx = _col_to_index(in_col)
    out_idx = _col_to_index(out_col)

    items = dbx.list_folder(stage_in)
    # items may be dicts or objects; normalize
    norm: List[Tuple[str, str]] = []
    for it in items or []:
        name = getattr(it, "name", None) or (it.get("name") if isinstance(it, dict) else None)
        path = getattr(it, "path_display", None) or getattr(it, "path_lower", None) or (it.get("path_display") if isinstance(it, dict) else None)
        if not name or not path:
            continue
        if str(name).lower().endswith(".xlsx") and not str(name).startswith("~$"):
            norm.append((str(name), str(path)))

    processed = 0
    for name, path in norm:
        # download
        data = dbx.download_bytes(path)

        wb = load_workbook(filename=io.BytesIO(data))
        ws = wb.active

        changed = 0
        max_row = ws.max_row or 0
        for r in range(start_row, max_row + 1):
            v_in = ws.cell(row=r, column=in_idx).value
            v_out = ws.cell(row=r, column=out_idx).value

            if v_in is None:
                continue
            txt = str(v_in).strip()
            if not txt:
                continue

            # 既に出力があるならスキップ（追記運用）
            if v_out is not None and str(v_out).strip():
                continue

            prompt = _default_feedback_prompt(txt)
            fb = _openai_call(oai, prompt).strip()
            ws.cell(row=r, column=out_idx).value = fb
            changed += 1

        # save
        out_buf = io.BytesIO()
        wb.save(out_buf)
        out_bytes = out_buf.getvalue()

        ts = _now_jst_compact()
        tag = _sha12(name + ts)
        base = _safe_name(os.path.splitext(name)[0])
        out_name = f"{base}__stage20__{ts}__{tag}.xlsx"
        out_path = f"{stage_out}/{out_name}"

        dbx.upload_bytes(out_bytes, out_path)

        # move input to DONE (keep original name + timestamp)
        done_name = f"{base}__DONE__{ts}__{tag}.xlsx"
        done_path = f"{stage_done}/{done_name}"
        dbx.move(path, done_path)

        processed += 1

    return processed