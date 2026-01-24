# -*- coding: utf-8 -*-
"""
openai_client.py

- Python 3.9 compatible
- Uses OpenAI Responses API
- DOES NOT send `temperature`
- Supports `max_output_tokens`
- Supports Structured Outputs (JSON Schema) for stable bilingual output
- More robust JSON extraction across SDK variations
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from openai import OpenAI


def _client() -> OpenAI:
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing.")
    return OpenAI(api_key=api_key)


# Fixed bilingual schema (extend later if needed)
BILINGUAL_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "jp": {"type": "string", "description": "Japanese version (Markdown allowed)"},
        "en": {"type": "string", "description": "English version (Markdown allowed)"},
        "decision_box": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "conclusions_hypotheses_en": {"type": "string"},
                "key_risks_assumptions_en": {"type": "string"},
                "next_actions_en": {"type": "string"},
                "conclusions_hypotheses_jp": {"type": "string"},
                "key_risks_assumptions_jp": {"type": "string"},
                "next_actions_jp": {"type": "string"},
            },
            "required": [
                "conclusions_hypotheses_en",
                "key_risks_assumptions_en",
                "next_actions_en",
                "conclusions_hypotheses_jp",
                "key_risks_assumptions_jp",
                "next_actions_jp",
            ],
        },
    },
    "required": ["jp", "en", "decision_box"],
}


def _coerce_to_dict(maybe_json: Any) -> Dict[str, Any]:
    """
    Accepts:
      - dict (already parsed)
      - JSON string
    """
    if isinstance(maybe_json, dict):
        return maybe_json
    if isinstance(maybe_json, str):
        s = maybe_json.strip()
        if not s:
            raise RuntimeError("Empty string where JSON was expected.")
        return json.loads(s)
    raise RuntimeError(f"Unsupported JSON container type: {type(maybe_json).__name__}")


def _extract_structured_json(resp: Any) -> Dict[str, Any]:
    """
    Robust extraction across SDK variations.
    Priority:
      1) resp.output_text if it looks like JSON
      2) resp.output[*].content[*].json (when SDK provides parsed JSON)
      3) resp.output[*].content[*].text (as JSON string)
    """
    # 1) output_text path (common)
    out_text = getattr(resp, "output_text", None)
    if isinstance(out_text, dict):
        return out_text
    if isinstance(out_text, str):
        t = out_text.strip()
        if t.startswith("{") and t.endswith("}"):
            return _coerce_to_dict(t)

    # 2) scan output content items
    output = getattr(resp, "output", None)
    if output:
        for item in output:
            contents = getattr(item, "content", None) or []
            for c in contents:
                # parsed json field
                j = getattr(c, "json", None)
                if isinstance(j, dict):
                    return j

                # sometimes JSON is stored in a "text" field
                txt = getattr(c, "text", None)
                if isinstance(txt, str):
                    s = txt.strip()
                    if s.startswith("{") and s.endswith("}"):
                        return _coerce_to_dict(s)

    # 3) fallback: try to json-parse output_text even if it doesn't end with "}"
    if isinstance(out_text, str):
        try:
            return _coerce_to_dict(out_text)
        except Exception:
            pass

    raise RuntimeError("Structured JSON output not found in response.")


def run_prompt_json(
    *,
    model: str,
    system_prompt: str,
    user_text: str,
    max_output_tokens: Optional[int] = None,
    schema: Optional[Dict[str, Any]] = None,
    schema_name: str = "bilingual_result",
) -> Dict[str, Any]:
    """
    Single-turn request that returns strictly schema-valid JSON.

    - Do NOT pass temperature (some models reject it)
    - Uses Structured Outputs (json_schema) via Responses API `text.format`

    Notes:
    - Newer SDKs accept json_schema in the form:
        text={"format": {"type":"json_schema","json_schema": {...}}}
      where json_schema contains name/strict/schema.
    - This function uses that form and extracts JSON robustly.
    """
    client = _client()
    schema = schema or BILINGUAL_SCHEMA

    resp = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ],
        text={
            "format": {
                "type": "json_schema",
                "json_schema": {
                    "name": schema_name,
                    "strict": True,
                    "schema": schema,
                },
            }
        },
        **({"max_output_tokens": int(max_output_tokens)} if max_output_tokens else {}),
    )

    obj = _extract_structured_json(resp)

    # Minimal validation: ensure required keys exist (schema is already strict, but keep guard)
    for k in ("jp", "en", "decision_box"):
        if k not in obj:
            raise RuntimeError(f"Structured output missing required key: {k}")
    return obj


def run_prompt_text(
    *,
    model: str,
    # preferred names
    system_prompt: Optional[str] = None,
    user_text: Optional[str] = None,
    # alternate names used by other modules / older code
    system: Optional[str] = None,
    user: Optional[str] = None,
    max_output_tokens: Optional[int] = None,
    timeout: Optional[float] = None,
    depth: Optional[str] = None,
    max_retries: Optional[int] = None,
    **_ignored: object,
) -> str:
    """
    Plain-text variant (optional).
    """
    # Resolve aliases (system/user -> system_prompt/user_text)
    if system_prompt is None and system is not None:
        system_prompt = system
    if user_text is None and user is not None:
        user_text = user
    if system_prompt is None:
        raise TypeError("run_prompt_text(): missing required argument: system_prompt (or system)")
    if user_text is None:
        raise TypeError("run_prompt_text(): missing required argument: user_text (or user)")

    client = _client()
    resp = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ],
        **({"max_output_tokens": int(max_output_tokens)} if max_output_tokens else {}),
    )
    return (resp.output_text or "").strip()