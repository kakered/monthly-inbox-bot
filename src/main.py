# -*- coding: utf-8 -*-
"""
main.py (text + PDF, folder-based routing, structured + medium/heavy switch, title-in-filename)
+ Gmail SMTP notification (Outbox content mail, body up to 20k chars + attachment)
+ PDF robustness: structured parse retry w/ token backoff, final fallback to non-structured formatted output
+ Log flushing to Dropbox during run
+ NEW: Copy original input file (pdf/txt/md) to Outbox with the SAME base name as the output markdown (safe default)
+ FIX: Fallback output naming also uses make_output_filename(...) to keep naming consistent
+ FIX: Scan cap bug (avoid "alphabetical first N" trap) by iterating all files and capping by processed_count

Dropbox /0-Inbox を走査し、未処理の .txt/.md/.pdf を OpenAI Responses API で処理して /0_Outbox に保存。

Routing:
- /0-Inbox/papers/**   => PAPER (pdf/txt/md)
- /0-Inbox/patents/**  => PATENT (pdf/txt/md)
- /0-Inbox/misc/**     => MEMO (txt/md only; pdf skipped)
- /0-Inbox root/*.txt|*.md => OTHER (auto-detect by content)
- /0-Inbox root/*.pdf => skipped (safety default)

Env:
- OPENAI_API_KEY (required)
- OPENAI_MODEL (default: gpt-5.2)
- OPENAI_TIMEOUT (seconds, default: 120)
- OPENAI_MAX_RETRIES (default: 2)
- OPENAI_MAX_OUTPUT_TOKENS or MAX_OUTPUT_TOKENS (default: 5000)
- COPY_INPUT_TO_OUTBOX (default: 0)  # optional: copy original INPUT (txt/md only) into Outbox with same base as output name as output
- DROPBOX_ACCESS_TOKEN (required)
- INBOX_PATH / OUTBOX_PATH / STATE_PATH / LOG_DIR
- MAX_FILES_PER_RUN / MAX_INPUT_CHARS
- DEPTH = "medium" | "heavy" (default: medium)

Mail (Gmail SMTP):
- MAIL_ENABLE=1 (default: disabled)
- MAIL_TO (required to enable)
- MAIL_FROM (gmail address)
- MAIL_APP_PASSWORD (16-char app password, no spaces)
- MAIL_BODY_LIMIT (default: 20000)
- MAIL_SUBJECT_PREFIX (default: "[inbox-bot]")
- MAIL_SMTP_HOST (default: smtp.gmail.com)
- MAIL_SMTP_PORT (default: 587)

Backward-compatible SMTP env (optional):
- SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASS
"""

from __future__ import annotations

import datetime
import os
import re
import traceback
import uuid
import hashlib
from typing import Optional, Tuple, List, Dict

from dotenv import load_dotenv
from pydantic import BaseModel, Field
from openai import OpenAI

# mail
import smtplib
from email.message import EmailMessage

# dropbox (for upload bytes)
import dropbox
from dropbox.files import WriteMode

from .dropbox_io import (
    list_input_files,
    download_text,
    download_bytes,
    upload_text,
    ensure_folder_exists_best_effort,
)
from .prompts import PROMPTS_BY_MODE, prompt_hash
from .classify import detect_mode
from .state import load_state, dump_state


# -------------------------
# Time / util
# -------------------------
def now_jst_iso() -> str:
    utc = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    jst = utc.astimezone(datetime.timezone(datetime.timedelta(hours=9)))
    return jst.strftime("%Y-%m-%d %H:%M:%S JST")


def _norm_path(p: str, default: str) -> str:
    p = (p or "").strip()
    if not p:
        p = default
    return p if p.startswith("/") else "/" + p


def _get_int_env(*keys: str, default: int) -> int:
    for k in keys:
        v = os.environ.get(k, "").strip()
        if v:
            try:
                return int(v)
            except ValueError:
                pass
    return default


def _get_bool_env(*keys: str, default: bool = False) -> bool:
    for k in keys:
        v = (os.environ.get(k, "") or "").strip().lower()
        if not v:
            continue
        if v in ("1", "true", "yes", "y", "on"):
            return True
        if v in ("0", "false", "no", "n", "off"):
            return False
    return default


def _get_depth_env() -> str:
    d = (os.environ.get("DEPTH") or "").strip().lower()
    return d if d in ("medium", "heavy") else "medium"


def sha8(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:8]


def _client() -> OpenAI:
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing.")

    timeout = float(os.environ.get("OPENAI_TIMEOUT", "120"))
    max_retries = int(os.environ.get("OPENAI_MAX_RETRIES", "2"))

    return OpenAI(api_key=api_key, timeout=timeout, max_retries=max_retries)


# -------------------------
# Dropbox bytes upload helper
# -------------------------
_DBX: Optional[dropbox.Dropbox] = None


def _dbx() -> dropbox.Dropbox:
    global _DBX
    if _DBX is not None:
        return _DBX
    token = (os.environ.get("DROPBOX_ACCESS_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("DROPBOX_ACCESS_TOKEN is missing.")
    _DBX = dropbox.Dropbox(token)
    return _DBX


def upload_bytes_dbx(path: str, data: bytes) -> None:
    """
    Upload raw bytes to Dropbox path (overwrite).
    Used to copy original inputs into Outbox with same base name as output md.
    """
    p = (path or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    dbx = _dbx()
    dbx.files_upload(data, p, mode=WriteMode.overwrite, mute=True)


# -------------------------
# Memory prompt (optional)
# -------------------------
def load_memory_md(path: str = "/0-System/Memory.md", max_bytes: int = 200_000) -> Tuple[str, Optional[str]]:
    try:
        mem = download_text(path, max_bytes=max_bytes).strip()
        if not mem:
            return "", None
        return mem, sha8(mem)
    except Exception:
        return "", None


def compose_system_prompt(base_prompt: str, memory_text: str) -> str:
    extra = (
        "\n\nAdditional output constraints (must follow schema):\n"
        "- Provide title_en (English) and, if possible, a concise title_ja (Japanese, ~20-30 chars).\n"
        "- Provide snippet_ja: one sentence (<=300 Japanese chars) describing the document at a glance.\n"
        "- Provide tags: up to ~5 short tags (EN/JP mixed OK), no long phrases.\n"
        "- Do not fabricate bibliographic facts; if unknown, leave fields blank or best-effort.\n"
    )
    if not memory_text:
        return base_prompt + extra
    return (
        "### Memory / Operating Principles (user-provided)\n"
        f"{memory_text}\n\n"
        "### Task Prompt\n"
        f"{base_prompt}{extra}"
    )


# -------------------------
# Output schema (Structured Outputs)
# -------------------------
class DocMeta(BaseModel):
    doc_type: str = Field(..., description="PAPER | PATENT | MEMO | OTHER")
    # Titles
    title_en: str = Field(
        ..., description="English title (as in document). If unavailable, best-effort short descriptor."
    )
    title_ja: Optional[str] = Field(
        None,
        description=(
            "Concise Japanese title (20-30 chars, interpretive, not necessarily literal). "
            "If unknown, best-effort."
        ),
    )

    # One-line snippet for fast triage (used by INDEX)
    snippet_ja: Optional[str] = Field(
        None,
        description=(
            "One-sentence Japanese snippet (<=300 chars) describing what this document is about. "
            "Avoid quoting; avoid mixing FACT/HYPOTHESIS labels here."
        ),
    )

    # Lightweight tags for searching (keep short)
    tags: List[str] = Field(
        default_factory=list,
        description="Up to ~5 short tags (EN/JP mixed OK) for search and grouping.",
    )
    source: str = Field(..., description="Journal name for papers; publication number for patents; otherwise source label.")
    year: Optional[int] = Field(None, description="Year if available")
    pages: Optional[str] = Field(None, description="e.g., '695–703' or 'pp. 1–23' if available")
    volume_issue: Optional[str] = Field(None, description="e.g., '64(7)' if available")
    authors: Optional[str] = Field(None, description="Authors (comma-separated) if available")
    affiliations: Optional[str] = Field(None, description="Affiliations if available")
    assignee: Optional[str] = Field(None, description="Assignee/Applicant (patents) if available")
    doc_id: Optional[str] = Field(None, description="DOI / WO / EP / US publication etc if available")
    keywords: Optional[str] = Field(None, description="A few key keywords (EN/JP mixed OK)")


class DecisionBox(BaseModel):
    conclusions_hypotheses_en: str = Field(..., description="Conclusions as hypotheses (English)")
    key_risks_assumptions_en: str = Field(..., description="Key risks/assumptions (English)")
    next_actions_en: str = Field(..., description="Next actions, low-cost first, up to 5 (English)")

    conclusions_hypotheses_jp: str = Field(..., description="結論（仮説）（日本語）")
    key_risks_assumptions_jp: str = Field(..., description="主要リスク/前提（日本語）")
    next_actions_jp: str = Field(..., description="次アクション（低コスト順・最大5）（日本語）")


class BilingualOutput(BaseModel):
    doc_meta: DocMeta
    jp: str = Field(..., description="Main body in Japanese")
    en: str = Field(..., description="Main body in English (selective but reusable)")
    decision_box: DecisionBox


# -------------------------
# Front header (YAML + human header)
# -------------------------
def _v(x: Optional[str]) -> str:
    return (x or "").strip()


def yaml_escape(s: Optional[str]) -> str:
    return (s or "").replace('"', "'").strip()


def format_front_header(meta: DocMeta) -> str:
    y = meta.year if meta.year is not None else ""
    tags_yaml = "[" + ", ".join([f'"{yaml_escape(t)}"' for t in (meta.tags or [])]) + "]"
    lines: List[str] = [
        "---",
        f'doc_type: "{yaml_escape(_v(meta.doc_type))}"',
        f'title_en: "{yaml_escape(_v(meta.title_en))}"',
        f'title_ja: "{yaml_escape(_v(meta.title_ja))}"',
        f'snippet_ja: "{yaml_escape(_v(meta.snippet_ja))}"',
        f"tags: {tags_yaml}",
        f'source: "{yaml_escape(_v(meta.source))}"',
        f'year: "{y}"',
        f'volume_issue: "{yaml_escape(_v(meta.volume_issue))}"',
        f'pages: "{yaml_escape(_v(meta.pages))}"',
        f'doc_id: "{yaml_escape(_v(meta.doc_id))}"',
        f'authors: "{yaml_escape(_v(meta.authors))}"',
        f'affiliations: "{yaml_escape(_v(meta.affiliations))}"',
        f'assignee: "{yaml_escape(_v(meta.assignee))}"',
        f'keywords: "{yaml_escape(_v(meta.keywords))}"',
        "---",
        "",
        "## Document summary header",
        "",
        f"- **Type**: {meta.doc_type}",
        f"- **Title (EN)**: {meta.title_en}",
        f"- **Title (JP)**: {meta.title_ja or ''}",
        f"- **Source**: {meta.source}",
    ]
    if meta.year:
        lines.append(f"- **Year**: {meta.year}")
    if meta.volume_issue:
        lines.append(f"- **Volume/Issue**: {meta.volume_issue}")
    if meta.pages:
        lines.append(f"- **Pages**: {meta.pages}")
    if meta.doc_id:
        lines.append(f"- **ID**: {meta.doc_id}")
    if meta.authors:
        lines.append(f"- **Authors**: {meta.authors}")
    if meta.affiliations:
        lines.append(f"- **Affiliations**: {meta.affiliations}")
    if meta.assignee:
        lines.append(f"- **Assignee/Applicant**: {meta.assignee}")
    if meta.keywords:
        lines.append(f"- **Keywords**: {meta.keywords}")
    lines.append("")
    return "\n".join(lines)


def format_markdown_out(obj: BilingualOutput, meta_block: str) -> str:
    return (
        f"{format_front_header(obj.doc_meta)}\n"
        "## 日本語\n\n"
        f"{obj.jp.strip()}\n\n"
        "## English\n\n"
        f"{obj.en.strip()}\n\n"
        "## Decision box (EN/JP)\n\n"
        "### EN\n"
        f"**Conclusions (as hypotheses)**\n{obj.decision_box.conclusions_hypotheses_en.strip()}\n\n"
        f"**Key risks / assumptions**\n{obj.decision_box.key_risks_assumptions_en.strip()}\n\n"
        f"**Next actions (low-cost first, up to 5)**\n{obj.decision_box.next_actions_en.strip()}\n\n"
        "### JP\n"
        f"**結論（仮説）**\n{obj.decision_box.conclusions_hypotheses_jp.strip()}\n\n"
        f"**主要リスク／前提**\n{obj.decision_box.key_risks_assumptions_jp.strip()}\n\n"
        f"**次アクション（低コスト順・最大5）**\n{obj.decision_box.next_actions_jp.strip()}\n\n"
        f"{meta_block}"
    )


# -------------------------
# Metadata block
# -------------------------
def build_metadata_block(
    *,
    route: str,
    input_kind: str,
    mode: str,
    confidence: str,
    reason: str,
    prompt_id: str,
    prompt_ver: str,
    prompt_text: str,
    input_path: str,
    memory_hash: Optional[str],
    depth: str,
    multi_ref_warning: Optional[str],
    out_path: str,
) -> str:
    mem_line = f"- Memory hash: {memory_hash}\n" if memory_hash else "- Memory hash: (none)\n"
    warn_line = f"- Warning: {multi_ref_warning}\n" if multi_ref_warning else ""
    return (
        "\n\n---\n\n"
        "## Processing metadata (auto-generated)\n\n"
        f"- Route: {route}\n"
        f"- Input kind: {input_kind}\n"
        f"- Mode: {mode}\n"
        f"- Confidence: {confidence}\n"
        f"- Reason: {reason}\n"
        f"- Depth: {depth}\n"
        f"- Prompt ID: {prompt_id} v{prompt_ver}\n"
        f"- Prompt hash: {prompt_hash(prompt_text)}\n"
        f"{mem_line}"
        f"{warn_line}"
        f"- Processed at: {now_jst_iso()}\n"
        f"- Input file: {input_path}\n"
        f"- Output file: {out_path}\n"
    )


# -------------------------
# Prompt selection (case-insensitive; prevents KeyError)
# -------------------------
def _get_prompt(mode: str, depth: str):
    keys_lower_to_actual = {k.lower(): k for k in PROMPTS_BY_MODE.keys()}
    md_key = f"{mode}_{depth}".lower()
    plain_key = mode.lower()

    if md_key in keys_lower_to_actual:
        return PROMPTS_BY_MODE[keys_lower_to_actual[md_key]]
    if plain_key in keys_lower_to_actual:
        return PROMPTS_BY_MODE[keys_lower_to_actual[plain_key]]

    if "memo" in keys_lower_to_actual:
        return PROMPTS_BY_MODE[keys_lower_to_actual["memo"]]
    return list(PROMPTS_BY_MODE.values())[0]


# -------------------------
# Multi-ref detection (text inputs only)
# -------------------------
_DOI_RE = re.compile(r"\b10\.\d{4,9}/[^\s]+", re.IGNORECASE)
_URL_DOI_RE = re.compile(r"https?://doi\.org/\S+", re.IGNORECASE)


def detect_multiple_refs(text: str) -> Tuple[int, Optional[str]]:
    text = text or ""
    dois = _DOI_RE.findall(text)
    doi_urls = _URL_DOI_RE.findall(text)
    n = len(set([d.strip().rstrip(".,;") for d in dois + doi_urls if d.strip()]))
    if n >= 2:
        return n, f"Multiple references detected (count={n}). Recommend 1 file per paper/patent for best summaries."
    return n, None


# -------------------------
# Routing
# -------------------------
def route_by_path(inbox_root: str, path: str) -> str:
    root = inbox_root.rstrip("/").lower()
    p = (path or "").lower()

    if p.startswith(f"{root}/papers/"):
        return "papers"
    if p.startswith(f"{root}/patents/"):
        return "patents"
    if p.startswith(f"{root}/misc/"):
        return "misc"

    if p.startswith(root + "/"):
        rel = p[len(root) + 1 :]
        if "/" not in rel:
            return "root"

    return "other"


def decide_mode(route: str, text_for_detect: Optional[str]) -> Tuple[str, str, str]:
    if route == "papers":
        return "paper", "High", "folder=/papers"
    if route == "patents":
        return "patent", "High", "folder=/patents"
    if route == "misc":
        return "memo", "High", "folder=/misc"

    if not text_for_detect:
        return "other", "Low", "no text for auto-detect"
    d = detect_mode(text_for_detect)
    return d.mode, d.confidence, d.reason


# -------------------------
# Filename slugging
# -------------------------
_STOPWORDS = {
    "the", "a", "an", "of", "for", "and", "or", "to", "in", "on", "with", "by", "from", "using", "via",
    "method", "methods", "compound", "compounds", "study", "studies", "analysis", "evaluation",
}


def slugify_title(title: str, max_len: int = 40) -> str:
    t = (title or "").strip().lower()
    if not t:
        return "untitled"
    t = re.sub(r'[\/\\\:\*\?\"\<\>\|\[\]\(\)\{\}\,\;]+', " ", t)
    t = re.sub(r"[^a-z0-9\s\-]+", " ", t)
    words = [w for w in re.split(r"\s+", t) if w and w not in _STOPWORDS]
    if not words:
        words = [w for w in re.split(r"\s+", t) if w]
    slug = "-".join(words)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    if not slug:
        slug = "untitled"
    if len(slug) > max_len:
        slug = slug[:max_len].rstrip("-")
    return slug or "untitled"


def safe_filename(s: str, max_len: int = 120) -> str:
    s = (s or "").strip()
    s = re.sub(r'[\/\\\:\*\?\"\<\>\|]+', "-", s)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_{2,}", "_", s).strip("_")
    if len(s) > max_len:
        s = s[:max_len].rstrip("_")
    return s or "output"


def make_output_filename(
    *,
    doc_type: str,
    title: str,
    year: Optional[int],
    source: str,
    input_path: str,
) -> str:
    """Create an Outbox markdown filename.

    Design goals:
    - Human-friendly mapping to the original Inbox filename (PDF is kept in Inbox in "A" design)
    - Preserve the existing generated identifier (doc_type/year/slug/source/hash) for uniqueness/debugging
    - Add a JST date suffix for quick recency cues and to reduce accidental collisions in manual workflows

    Format:
        <inbox_basename>__<generated_base>__YYYY-MM-DD.out.md
    """

    # 1) Original inbox basename (without extension)
    inbox_name = os.path.basename(input_path or "").strip()
    inbox_base = os.path.splitext(inbox_name)[0] if inbox_name else "input"

    # 2) Existing generated base (keeps uniqueness & debug value)
    slug = slugify_title(title, max_len=40)
    y = str(year) if year else "na"
    src_slug = slugify_title(source or "source", max_len=18)
    h = sha8(f"{doc_type}|{title}|{source}|{input_path}")
    generated_base = "_".join([doc_type.upper(), y, slug, src_slug, h])
    generated_base = safe_filename(generated_base, max_len=120)

    # 3) JST date suffix (YYYY-MM-DD)
    utc = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    jst = utc.astimezone(datetime.timezone(datetime.timedelta(hours=9)))
    date = jst.strftime("%Y-%m-%d")

    combined = f"{inbox_base}__{generated_base}__{date}"
    combined = safe_filename(combined, max_len=200)
    return combined + ".out.md"


def md_to_pdf_name(out_md_name: str) -> str:
    # "xxx.out.md" -> "xxx.pdf" (otherwise fallback)
    n = (out_md_name or "").strip()
    if n.endswith(".out.md"):
        return n[:-7] + ".pdf"
    if n.endswith(".md"):
        return n[:-3] + ".pdf"
    return n + ".pdf"


def md_to_same_base_name(out_md_name: str, ext: str) -> str:
    """Return a filename with the same base as out_md_name but different extension (ext includes leading dot)."""
    n = (out_md_name or "").strip()
    ext = (ext or "").strip()
    if not ext.startswith("."):
        ext = "." + ext if ext else ""
    if n.endswith(".out.md"):
        return n[:-7] + ext
    if n.endswith(".md"):
        return n[:-3] + ext
    if "." in n:
        return n.rsplit(".", 1)[0] + ext
    return n + ext


# -------------------------
# Outbox INDEX.md (append-only, idempotent per out_md_name)
# -------------------------
def _esc_pipes(s: str) -> str:
    return (s or "").replace("|", "/").strip()


def build_index_line(
    *,
    processed_date_jst: str,
    doc_type: str,
    title_en: str,
    title_ja: Optional[str],
    year: Optional[int],
    tags: List[str],
    snippet_ja: Optional[str],
    out_md_name: str,
    input_path: str,
) -> str:
    y = str(year) if year else ""
    t_en = _esc_pipes(title_en)
    t_ja = _esc_pipes(title_ja or "")
    tag_str = ", ".join([_esc_pipes(t) for t in (tags or [])])
    snip = (snippet_ja or "").strip()
    if len(snip) > 260:
        snip = snip[:260].rstrip() + "…"
    snip = _esc_pipes(snip)
    inp = _esc_pipes(input_path)

    # Use a relative link (works in many Markdown viewers when INDEX.md is in Outbox)
    link = f"[out](./{out_md_name})"

    return f"- {processed_date_jst} | {doc_type} | {t_en} | {t_ja} | {y} | {tag_str} | {snip} | {link} | {inp}"


def append_outbox_index(
    *,
    outbox_path: str,
    out_md_name: str,
    line: str,
    max_bytes: int = 400_000,
) -> None:
    index_path = (os.environ.get("OUTBOX_INDEX_PATH") or f"{outbox_path}/INDEX.md").strip()
    try:
        existing = download_text(index_path, max_bytes=max_bytes)
    except Exception:
        existing = ""

    # Idempotency: if the out file name is already referenced, do nothing.
    if out_md_name and out_md_name in existing:
        return

    if not existing.strip():
        header = (
            "# Outbox Index\n\n"
            "Columns: processed_date | doc_type | title_en | title_ja | year | tags | snippet_ja | out_link | input_path\n\n"
        )
        new_text = header + line + "\n"
    else:
        new_text = existing.rstrip() + "\n" + line + "\n"

    upload_text(index_path, new_text)


def guess_title_from_input_name(name: str) -> str:
    base = (name or "").strip()
    if not base:
        return "untitled"
    base = os.path.splitext(base)[0]
    t = base.replace("_", " ").replace("-", " ").strip()
    return t or base


def make_output_filename_fallback(*, mode: str, input_name: str, input_path: str) -> str:
    """Unify naming even when structured output fails (fixes 'inputname_hash' divergence)."""
    doc_type = (mode or "other").upper()
    title = guess_title_from_input_name(input_name or "untitled")
    source = mode or "source"
    return make_output_filename(
        doc_type=doc_type,
        title=title,
        year=None,
        source=source,
        input_path=input_path,
    )


# -------------------------
# OpenAI calls
# -------------------------
def run_structured_text(
    *,
    model: str,
    system_prompt: str,
    user_text: str,
    max_output_tokens: Optional[int],
) -> BilingualOutput:
    client = _client()
    resp = client.responses.parse(
        model=model,
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_text},
        ],
        text_format=BilingualOutput,
        **({"max_output_tokens": int(max_output_tokens)} if max_output_tokens else {}),
    )
    obj = resp.output_parsed
    if obj is None:
        raise RuntimeError("Structured output parse returned None.")
    return obj


def run_structured_pdf_once(
    *,
    model: str,
    system_prompt: str,
    pdf_bytes: bytes,
    filename: str,
    max_output_tokens: Optional[int],
) -> BilingualOutput:
    client = _client()
    up = client.files.create(file=(filename, pdf_bytes), purpose="user_data")

    resp = client.responses.parse(
        model=model,
        input=[
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [
                    {"type": "input_file", "file_id": up.id},
                    {
                        "type": "input_text",
                        "text": (
                            "Read the attached PDF and produce the requested structured review. "
                            "First extract bibliographic/header metadata (title/type/pages/authors/affiliations, etc.). "
                            "Then provide a clear plain-language explanation, key points, positioning, limitations, and actionable implications."
                        ),
                    },
                ],
            },
        ],
        text_format=BilingualOutput,
        **({"max_output_tokens": int(max_output_tokens)} if max_output_tokens else {}),
    )
    obj = resp.output_parsed
    if obj is None:
        raise RuntimeError("Structured output parse returned None (PDF).")
    return obj


def run_structured_pdf_with_retries(
    *,
    model: str,
    system_prompt: str,
    pdf_bytes: bytes,
    filename: str,
    max_output_tokens: int,
) -> Tuple[BilingualOutput, List[str]]:
    logs: List[str] = []
    t0 = int(max_output_tokens)
    t1 = max(1200, t0 // 2)
    t2 = max(800, t0 // 3)

    for i, tok in enumerate([t0, t1, t2], start=0):
        logs.append(f"pdf_retry{i}_tokens={tok}")
        obj = run_structured_pdf_once(
            model=model,
            system_prompt=system_prompt,
            pdf_bytes=pdf_bytes,
            filename=filename,
            max_output_tokens=tok,
        )
        return obj, logs

    raise RuntimeError("PDF structured parse failed after retries.")


def run_fallback_text(
    *,
    model: str,
    system_prompt: str,
    user_text: str,
    max_output_tokens: Optional[int],
) -> str:
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


def run_fallback_pdf_markdown(
    *,
    model: str,
    system_prompt: str,
    pdf_bytes: bytes,
    filename: str,
    max_output_tokens: int,
) -> str:
    client = _client()
    up = client.files.create(file=(filename, pdf_bytes), purpose="user_data")
    resp = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": [
                    {"type": "input_file", "file_id": up.id},
                    {
                        "type": "input_text",
                        "text": (
                            "Read the attached PDF. Output a clean Markdown review with these sections:\n"
                            "1) Bibliographic metadata (title/journal/year/pages/authors/affiliations/doi if found)\n"
                            "2) Plain explanation (JP main)\n"
                            "3) Technical review (facts vs hypotheses separated)\n"
                            "4) Limitations\n"
                            "5) Practical implications\n"
                            "6) Decision box (EN/JP) with Conclusions as hypotheses / Key risks / Next actions (<=5)\n"
                            "Be concise but complete. Avoid quoting long passages."
                        ),
                    },
                ],
            },
        ],
        max_output_tokens=max_output_tokens,
    )
    return (resp.output_text or "").strip()


# -------------------------
# Log flushing (Dropbox)
# -------------------------
def _flush_logs(run_log: str, err_log: str, run_lines: List[str], err_lines: List[str]) -> None:
    try:
        upload_text(run_log, "".join(run_lines))
    except Exception:
        pass
    if err_lines:
        try:
            upload_text(err_log, "".join(err_lines))
        except Exception:
            pass


# -------------------------
# Mail notification (Gmail SMTP)
# -------------------------
def _mail_cfg() -> Dict[str, object]:
    enable = (os.environ.get("MAIL_ENABLE", "") or "").strip() in ("1", "true", "True", "yes", "on", "ON")

    # Primary (MAIL_*)
    mail_to = (os.environ.get("MAIL_TO") or "").strip()
    mail_from = (os.environ.get("MAIL_FROM") or "").strip()
    mail_pass = (os.environ.get("MAIL_APP_PASSWORD") or "").replace(" ", "").strip()
    smtp_host = (os.environ.get("MAIL_SMTP_HOST") or "smtp.gmail.com").strip()
    smtp_port = _get_int_env("MAIL_SMTP_PORT", default=587)

    # Backward-compatible (SMTP_*) fallback if MAIL_* not set
    if not mail_from:
        mail_from = (os.environ.get("SMTP_USER") or "").strip()
    if not mail_pass:
        mail_pass = (os.environ.get("SMTP_PASS") or "").replace(" ", "").strip()
    if smtp_host == "smtp.gmail.com":
        smtp_host = (os.environ.get("SMTP_HOST") or smtp_host).strip()
    if smtp_port == 587:
        smtp_port = _get_int_env("SMTP_PORT", default=smtp_port)

    if not mail_to:
        mail_to = (os.environ.get("SMTP_TO") or "").strip()

    return {
        "enable": enable,
        "to": mail_to,
        "from": mail_from,
        "app_password": mail_pass,
        "body_limit": _get_int_env("MAIL_BODY_LIMIT", default=20000),
        "subject_prefix": (os.environ.get("MAIL_SUBJECT_PREFIX") or "[inbox-bot]").strip(),
        "smtp_host": smtp_host,
        "smtp_port": int(smtp_port),
    }


def send_mail_with_attachment(
    *,
    subject: str,
    body: str,
    attachment_name: str,
    attachment_text: str,
) -> Tuple[bool, str]:
    cfg = _mail_cfg()

    if not cfg["enable"]:
        return False, "MAIL_ENABLE not set; email disabled."
    if not cfg["to"]:
        return False, "MAIL_TO not set; email disabled."
    if not cfg["from"]:
        return False, "MAIL_FROM/SMTP_USER not set; email disabled."
    if not cfg["app_password"]:
        return False, "MAIL_APP_PASSWORD/SMTP_PASS not set; email disabled."

    limit = int(cfg["body_limit"])
    b = (body or "")
    if len(b) > limit:
        b = b[:limit] + f"\n\n... (truncated to {limit} chars) ...\n"

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = str(cfg["from"])
    msg["To"] = str(cfg["to"])
    msg.set_content(b)

    att_bytes = (attachment_text or "").encode("utf-8")
    msg.add_attachment(att_bytes, maintype="text", subtype="markdown", filename=attachment_name)

    try:
        with smtplib.SMTP(str(cfg["smtp_host"]), int(cfg["smtp_port"])) as s:
            s.ehlo()
            s.starttls()
            s.login(str(cfg["from"]), str(cfg["app_password"]))
            s.send_message(msg)
        return True, "ok"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def build_mail_subject(doc_type: str, title: str, out_name: str) -> str:
    cfg = _mail_cfg()
    prefix = str(cfg["subject_prefix"])
    t = (title or "").strip()
    if len(t) > 80:
        t = t[:80] + "..."
    return f"{prefix} {doc_type}: {t} ({out_name})"


def build_mail_body(out_text: str, out_path: str, input_path: str) -> str:
    return (
        f"Output saved: {out_path}\n"
        f"Input: {input_path}\n\n"
        "---- BEGIN OUTPUT (truncated by MAIL_BODY_LIMIT) ----\n\n"
        f"{out_text}\n\n"
        "---- END OUTPUT ----\n"
    )


# -------------------------
# main
# -------------------------
def main() -> None:
    load_dotenv(".env", override=True)

    inbox = _norm_path(os.environ.get("INBOX_PATH"), "/0-Inbox")
    outbox = _norm_path(os.environ.get("OUTBOX_PATH"), "/0-Outbox")
    state_path = _norm_path(os.environ.get("STATE_PATH"), "/0-System/state.json")
    log_dir = _norm_path(os.environ.get("LOG_DIR"), "/0-System/logs").rstrip("/")

    model = (os.environ.get("OPENAI_MODEL") or "").strip() or "gpt-5.2"
    depth = _get_depth_env()

    max_files_per_run = _get_int_env("MAX_FILES_PER_RUN", default=10)
    max_input_chars = _get_int_env("MAX_INPUT_CHARS", default=80000)
    max_output_tokens = _get_int_env("OPENAI_MAX_OUTPUT_TOKENS", "MAX_OUTPUT_TOKENS", default=5000)

    copy_input_to_outbox = _get_bool_env("COPY_INPUT_TO_OUTBOX", default=False)

    ensure_folder_exists_best_effort(outbox)
    ensure_folder_exists_best_effort(log_dir)
    state_dir = os.path.dirname(state_path).replace("\\", "/")
    if state_dir:
        ensure_folder_exists_best_effort(state_dir)

    day = datetime.datetime.utcnow().strftime("%Y%m%d")
    run_id = uuid.uuid4().hex[:8]
    run_log = f"{log_dir}/{day}_{run_id}_run.log"
    err_log = f"{log_dir}/{day}_{run_id}_errors.log"

    run_lines: List[str] = []
    err_lines: List[str] = []

    run_lines.append(f"[{now_jst_iso()}] START model={model}\n")
    run_lines.append(f"inbox={inbox}\n")
    run_lines.append(f"outbox={outbox}\n")
    run_lines.append(f"state_path={state_path}\n")
    run_lines.append(f"depth={depth}\n")
    run_lines.append(f"max_files_per_run={max_files_per_run} max_in_chars={max_input_chars} max_out_tokens={max_output_tokens}\n")
    run_lines.append(f"copy_input_to_outbox={copy_input_to_outbox}\n")
    run_lines.append(f"prompt_keys={[k for k in PROMPTS_BY_MODE.keys()]}\n")

    _flush_logs(run_log, err_log, run_lines, err_lines)

    processed_any = False
    mail_any = False

    try:
        try:
            state_text = download_text(state_path, max_bytes=500_000)
        except Exception:
            state_text = None
        state = load_state(state_text)

        memory_text, memory_hash = load_memory_md("/0-System/Memory.md")

        all_files = list_input_files(inbox)
        run_lines.append(f"[{now_jst_iso()}] LIST all_files={len(all_files)} max_files_per_run={max_files_per_run}\n")
        _flush_logs(run_log, err_log, run_lines, err_lines)

        processed_count = 0

        for f in all_files:
            if processed_count >= max_files_per_run:
                break
            try:
                prev_rev = state.processed.get(f.path)
                if prev_rev == f.rev:
                    run_lines.append(f"[{now_jst_iso()}] SKIP {f.path} reason=already_processed\n")
                    _flush_logs(run_log, err_log, run_lines, err_lines)
                    continue

                route = route_by_path(inbox, f.path)
                name_l = (f.name or "").lower()
                is_pdf = name_l.endswith(".pdf")
                is_text = name_l.endswith(".txt") or name_l.endswith(".md")

                if route == "misc" and is_pdf:
                    run_lines.append(f"[{now_jst_iso()}] SKIP {f.path} reason=misc_disallow_pdf\n")
                    state.processed[f.path] = f.rev
                    processed_any = True
                    processed_count += 1
                    _flush_logs(run_log, err_log, run_lines, err_lines)
                    continue

                if route == "root" and is_pdf:
                    run_lines.append(f"[{now_jst_iso()}] SKIP {f.path} reason=root_pdf_disabled\n")
                    state.processed[f.path] = f.rev
                    processed_any = True
                    processed_count += 1
                    _flush_logs(run_log, err_log, run_lines, err_lines)
                    continue

                truncated = False
                multi_warn = None
                input_kind = "text"
                user_text: Optional[str] = None
                pdf_bytes: Optional[bytes] = None

                if is_text:
                    user_text = download_text(f.path)
                    if len(user_text) > max_input_chars:
                        head = user_text[: max_input_chars // 2]
                        tail = user_text[-(max_input_chars // 2) :]
                        user_text = head + "\n\n[...TRUNCATED...]\n\n" + tail
                        truncated = True
                    _, multi_warn = detect_multiple_refs(user_text)
                    mode, conf, reason = decide_mode(route, user_text)

                elif is_pdf:
                    input_kind = "pdf"
                    if route not in ("papers", "patents"):
                        run_lines.append(f"[{now_jst_iso()}] SKIP {f.path} reason=pdf_outside_papers_patents\n")
                        state.processed[f.path] = f.rev
                        processed_any = True
                        processed_count += 1
                        _flush_logs(run_log, err_log, run_lines, err_lines)
                        continue

                    mode, conf, reason = decide_mode(route, None)
                    pdf_bytes = download_bytes(f.path, max_bytes=25_000_000)

                else:
                    run_lines.append(f"[{now_jst_iso()}] SKIP {f.path} reason=unsupported_ext\n")
                    state.processed[f.path] = f.rev
                    processed_any = True
                    processed_count += 1
                    _flush_logs(run_log, err_log, run_lines, err_lines)
                    continue

                prompt = _get_prompt(mode, depth)
                system_prompt = compose_system_prompt(prompt.text, memory_text)

                run_lines.append(
                    f"[{now_jst_iso()}] FILE {f.path} rev={f.rev} route={route} mode={mode} depth={depth} "
                    f"kind={input_kind} trunc={truncated} "
                    f"in_size={len(user_text) if user_text else (len(pdf_bytes) if pdf_bytes else 0)}\n"
                )
                _flush_logs(run_log, err_log, run_lines, err_lines)

                out_text: str
                out_path: str
                out_name_for_subject: str = safe_filename(f.name or "output", max_len=80) + ".out.md"
                doc_type_for_subject: str = mode.upper()
                title_for_subject: str = f.name or "untitled"

                out_md_name: Optional[str] = None
                index_title_en: str = guess_title_from_input_name(f.name or "untitled")
                index_title_ja: Optional[str] = None
                index_year: Optional[int] = None
                index_tags: List[str] = []
                index_snippet_ja: Optional[str] = None

                try:
                    if input_kind == "text":
                        obj = run_structured_text(
                            model=model,
                            system_prompt=system_prompt,
                            user_text=user_text or "",
                            max_output_tokens=max_output_tokens,
                        )
                        doc_type_for_subject = obj.doc_meta.doc_type or mode.upper()
                        title_for_subject = obj.doc_meta.title_en or (f.name or "untitled")

                        index_title_en = obj.doc_meta.title_en or index_title_en
                        index_title_ja = obj.doc_meta.title_ja or index_title_ja
                        index_year = obj.doc_meta.year or index_year
                        index_tags = obj.doc_meta.tags or index_tags
                        index_snippet_ja = obj.doc_meta.snippet_ja or index_snippet_ja

                        out_name = make_output_filename(
                            doc_type=doc_type_for_subject,
                            title=title_for_subject,
                            year=obj.doc_meta.year,
                            source=obj.doc_meta.source or mode,
                            input_path=f.path,
                        )
                        out_md_name = out_name
                        out_name_for_subject = out_name
                        out_path = f"{outbox}/{out_name}"

                        meta = build_metadata_block(
                            route=route,
                            input_kind=input_kind,
                            mode=mode,
                            confidence=conf,
                            reason=reason,
                            prompt_id=prompt.id,
                            prompt_ver=prompt.version,
                            prompt_text=prompt.text,
                            input_path=f.path,
                            memory_hash=memory_hash,
                            depth=depth,
                            multi_ref_warning=multi_warn,
                            out_path=out_path,
                        )
                        out_text = format_markdown_out(obj, meta)

                    else:
                        pdf_logs: List[str] = []
                        try:
                            obj, pdf_logs = run_structured_pdf_with_retries(
                                model=model,
                                system_prompt=system_prompt,
                                pdf_bytes=pdf_bytes or b"",
                                filename=f.name or "input.pdf",
                                max_output_tokens=max_output_tokens,
                            )
                            for pl in pdf_logs:
                                run_lines.append(f"[{now_jst_iso()}] {pl}\n")

                            doc_type_for_subject = obj.doc_meta.doc_type or mode.upper()
                            title_for_subject = obj.doc_meta.title_en or (f.name or "untitled")

                            index_title_en = obj.doc_meta.title_en or index_title_en
                            index_title_ja = obj.doc_meta.title_ja or index_title_ja
                            index_year = obj.doc_meta.year or index_year
                            index_tags = obj.doc_meta.tags or index_tags
                            index_snippet_ja = obj.doc_meta.snippet_ja or index_snippet_ja

                            out_name = make_output_filename(
                                doc_type=doc_type_for_subject,
                                title=title_for_subject,
                                year=obj.doc_meta.year,
                                source=obj.doc_meta.source or mode,
                                input_path=f.path,
                            )
                            out_md_name = out_name
                            out_name_for_subject = out_name
                            out_path = f"{outbox}/{out_name}"

                            meta = build_metadata_block(
                                route=route,
                                input_kind=input_kind,
                                mode=mode,
                                confidence=conf,
                                reason=reason,
                                prompt_id=prompt.id,
                                prompt_ver=prompt.version,
                                prompt_text=prompt.text,
                                input_path=f.path,
                                memory_hash=memory_hash,
                                depth=depth,
                                multi_ref_warning=multi_warn,
                                out_path=out_path,
                            )
                            out_text = format_markdown_out(obj, meta)

                        except Exception as e_pdf_struct:
                            fb_md = run_fallback_pdf_markdown(
                                model=model,
                                system_prompt=system_prompt,
                                pdf_bytes=pdf_bytes or b"",
                                filename=f.name or "input.pdf",
                                max_output_tokens=max(1800, max_output_tokens),
                            )

                            # FIX: unify naming on fallback too
                            out_md_name = make_output_filename_fallback(mode=mode, input_name=f.name or "output", input_path=f.path)
                            out_path = f"{outbox}/{out_md_name}"
                            out_name_for_subject = out_md_name

                            meta = build_metadata_block(
                                route=route,
                                input_kind=input_kind,
                                mode=mode,
                                confidence=conf,
                                reason=reason,
                                prompt_id=prompt.id,
                                prompt_ver=prompt.version,
                                prompt_text=prompt.text,
                                input_path=f.path,
                                memory_hash=memory_hash,
                                depth=depth,
                                multi_ref_warning=multi_warn,
                                out_path=out_path,
                            )
                            out_text = (
                                "## Output (fallback text)\n\n"
                                + (fb_md or "（PDFのfallback出力が空でした。PDF/モデル/トークンを確認してください。）")
                                + "\n\n"
                                + "## Note\n\n"
                                + "- Structured output failed for PDF; used non-structured markdown fallback.\n"
                                + f"- Error: {type(e_pdf_struct).__name__}: {e_pdf_struct}\n"
                                + meta
                            )

                except Exception as e_struct:
                    if input_kind == "pdf":
                        fb = (
                            "（PDFのStructured解析が失敗しました。PDFがスキャン画像中心/破損/大きすぎる等の可能性があります。\n"
                            "対処: 1) PDFを軽量化 2) 文字が選択可能なPDFで試す 3) 章ごとに分割 など）"
                        )
                    else:
                        fb = run_fallback_text(
                            model=model,
                            system_prompt=system_prompt,
                            user_text=user_text or "",
                            max_output_tokens=max_output_tokens,
                        )
                        if not fb:
                            fb = "（出力が空でした。モデル/キー/入力サイズを確認してください。）"

                    # FIX: unify naming on fallback too
                    out_md_name = make_output_filename_fallback(mode=mode, input_name=f.name or "output", input_path=f.path)
                    out_path = f"{outbox}/{out_md_name}"
                    out_name_for_subject = out_md_name

                    meta = build_metadata_block(
                        route=route,
                        input_kind=input_kind,
                        mode=mode,
                        confidence=conf,
                        reason=reason,
                        prompt_id=prompt.id,
                        prompt_ver=prompt.version,
                        prompt_text=prompt.text,
                        input_path=f.path,
                        memory_hash=memory_hash,
                        depth=depth,
                        multi_ref_warning=multi_warn,
                        out_path=out_path,
                    )
                    out_text = (
                        "## Output (fallback text)\n\n"
                        + fb
                        + "\n\n"
                        + "## Note\n\n"
                        + "- Structured output failed; used fallback text.\n"
                        + f"- Error: {type(e_struct).__name__}: {e_struct}\n"
                        + meta
                    )

                # save markdown output
                upload_text(out_path, out_text)

                # Update Outbox index (append a single line per output)
                if out_md_name:
                    try:
                        line = build_index_line(
                            processed_date_jst=now_jst_iso().split(" ")[0],
                            doc_type=str(doc_type_for_subject or mode).upper(),
                            title_en=index_title_en,
                            title_ja=index_title_ja,
                            year=index_year,
                            tags=index_tags,
                            snippet_ja=index_snippet_ja,
                            out_md_name=out_md_name,
                            input_path=f.path,
                        )
                        append_outbox_index(outbox_path=outbox, out_md_name=out_md_name, line=line)
                    except Exception as e_index:
                        run_lines.append(f"[{now_jst_iso()}] INDEX_FAIL err={type(e_index).__name__}: {e_index}\n")
                        _flush_logs(run_log, err_log, run_lines, err_lines)

                # Optional: copy original INPUT into Outbox with the SAME base name as the output markdown.
                # Design (A): PDFs remain in Inbox (no duplication). If enabled, only text inputs are copied.
                if copy_input_to_outbox and out_md_name and input_kind != "pdf":
                    try:
                        orig_name = f.name or ""
                        orig_ext = os.path.splitext(orig_name)[1].lower()
                        if not orig_ext:
                            orig_ext = ".pdf" if input_kind == "pdf" else ".txt"

                        orig_copy_name = md_to_same_base_name(out_md_name, orig_ext)
                        orig_copy_path = f"{outbox}/{orig_copy_name}"

                        if input_kind == "pdf" and pdf_bytes:
                            data = pdf_bytes
                        elif input_kind == "text":
                            data = (user_text or "").encode("utf-8")
                        else:
                            data = b""

                        if data:
                            upload_bytes_dbx(orig_copy_path, data)
                            run_lines.append(
                                f"[{now_jst_iso()}] ORIGINAL_COPY {f.path} -> {orig_copy_path} bytes={len(data)}\n"
                            )
                            _flush_logs(run_log, err_log, run_lines, err_lines)

                    except Exception as e_copy:
                        run_lines.append(
                            f"[{now_jst_iso()}] ORIGINAL_COPY_FAIL err={type(e_copy).__name__}: {e_copy}\n"
                        )
                        _flush_logs(run_log, err_log, run_lines, err_lines)

                state.processed[f.path] = f.rev
                processed_any = True
                processed_count += 1
                run_lines.append(f"[{now_jst_iso()}] OK   {f.path} -> {out_path} out_chars={len(out_text)} processed_count={processed_count}\n")
                _flush_logs(run_log, err_log, run_lines, err_lines)

                # mail notify (per file)
                subj = build_mail_subject(doc_type_for_subject, title_for_subject, out_name_for_subject)
                body = build_mail_body(out_text, out_path, f.path)
                sent, msg = send_mail_with_attachment(
                    subject=subj,
                    body=body,
                    attachment_name=out_name_for_subject,
                    attachment_text=out_text,
                )
                mail_any = mail_any or sent
                run_lines.append(f"[{now_jst_iso()}] MAIL sent={sent} msg={msg}\n")
                _flush_logs(run_log, err_log, run_lines, err_lines)

            except Exception as e:
                tb = traceback.format_exc()
                err_lines.append(f"\n[{now_jst_iso()}] ERROR file={getattr(f, 'path', '(unknown)')}\n{repr(e)}\n{tb}\n")
                run_lines.append(f"[{now_jst_iso()}] FAIL {getattr(f, 'path', '(unknown)')} err={type(e).__name__}\n")
                _flush_logs(run_log, err_log, run_lines, err_lines)
                continue

        if processed_any:
            upload_text(state_path, dump_state(state))

        run_lines.append(f"[{now_jst_iso()}] END processed_any={processed_any} mail_any={mail_any}\n")
        _flush_logs(run_log, err_log, run_lines, err_lines)

    except KeyboardInterrupt:
        run_lines.append(f"[{now_jst_iso()}] INTERRUPTED KeyboardInterrupt\n")
        err_lines.append(f"\n[{now_jst_iso()}] KeyboardInterrupt (user abort)\n")
        _flush_logs(run_log, err_log, run_lines, err_lines)
        raise

    finally:
        _flush_logs(run_log, err_log, run_lines, err_lines)


if __name__ == "__main__":
    main()