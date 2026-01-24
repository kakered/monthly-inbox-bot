# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class ModeDecision:
    mode: str            # 'paper' | 'patent' | 'memo' | 'other'
    confidence: str      # High/Medium/Low
    reason: str


_DOI_RE = re.compile(r"\b10\.\d{4,9}/[^\s]+", re.IGNORECASE)
_WO_RE = re.compile(r"\bWO\s?\d{4}/\d{6,}\b", re.IGNORECASE)
_USPUB_RE = re.compile(r"\bUS\s?\d{4}/\d{7,}\b", re.IGNORECASE)
_EP_RE = re.compile(r"\bEP\s?\d{6,}\b", re.IGNORECASE)


def detect_mode(text: str) -> ModeDecision:
    t = (text or "")
    tl = t.lower()

    # Patent signals
    if _WO_RE.search(t) or _USPUB_RE.search(t) or "claims" in tl or "請求項" in t or "特許" in t:
        return ModeDecision(mode="patent", confidence="Medium", reason="patent-like identifiers/claims keywords detected")

    # Paper signals
    if _DOI_RE.search(t) or "abstract" in tl or "introduction" in tl or "references" in tl or "図" in t or "table" in tl:
        return ModeDecision(mode="paper", confidence="Medium", reason="doi/section headings detected")

    # Memo/default
    return ModeDecision(mode="memo", confidence="Low", reason="no strong paper/patent signals")