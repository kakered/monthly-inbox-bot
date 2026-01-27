# -*- coding: utf-8 -*-
from __future__ import annotations

"""Monthly report (月報) pipeline specification.

このファイルは、excel_exporter / monthly_pipeline_MULTISTAGE が参照する「列名・プロンプト・定数」を一箇所に集約するための定義です。
（ここに無い名前を import しようとして落ちる事故を防ぐ目的もあります）
"""

from typing import List, Set

# -----------------------------
# 0) Misc / safety knobs
# -----------------------------

# マスキング/除外したい「見出し候補」（必要なら運用に合わせて追加）
SENSITIVE_HEADER_NAMES: Set[str] = {
    "氏名",
    "名前",
    "メール",
    "住所",
    "電話",
}

# ざっくりのポジ/ネガ判定に使うキーワード（未使用でも互換のため残す）
POSITIVE_KEYWORDS: List[str] = [
    "良かった", "できた", "達成", "改善", "感謝", "助かった", "順調",
]
NEGATIVE_KEYWORDS: List[str] = [
    "できない", "難しい", "困った", "ミス", "トラブル", "遅れ", "不安",
]

# -----------------------------
# 1) Canonical input columns (Stage1 PREP sheet)
# -----------------------------
# Stage1(=API無し)で作るシート "INPUT" のヘッダ。以降はこの名前を前提に処理します。
# ※ユーザー最新仕様：社員番号/一致確認用/就業先 は廃止
INPUT_CANONICAL_COLUMNS: List[str] = [
    "Person_ID",
    "Month",
    "HumanRelations_ReportLine",   # 人間関係（報連相）
    "Work_QuantityQuality",        # 仕事の量・質（同一セル）
    "Proactivity",                 # 積極性
    "Responsibility",              # 責任性
    "NearMiss",                    # ヒヤリハットの抽出・分析・対策
    "Site_Improvement",            # 就業先での業務改善提案
    "HQ_Request",                  # 本社への相談・要望
]

# -----------------------------
# 2) Output columns (Overview / Per-person)
# -----------------------------
# 「出力ファイルイメージver2.xlsx」の趣旨：ID+月+原文（各項目）+ Draft_Feedback
OVERVIEW_OUTPUT_COLUMNS: List[str] = [
    "Person_ID",
    "Month",
    "HumanRelations_ReportLine",
    "Work_QuantityQuality",
    "Proactivity",
    "Responsibility",
    "NearMiss",
    "Site_Improvement",
    "HQ_Request",
    "Draft_Feedback",
]

# 現状は「個人別」も同じ列でOK（必要になったら追加列をここに足す）
PER_PERSON_OUTPUT_COLUMNS: List[str] = list(OVERVIEW_OUTPUT_COLUMNS)

# -----------------------------
# 3) OpenAI prompt (draft feedback)
# -----------------------------
FEEDBACK_DRAFT_SYSTEM: str = """あなたは、対象者と同じ職場で日常的に業務を見ている立場の、落ち着いた現場の先輩です。

目的：
「実際のフィードバック担当者が、要点を絞って、少し丁寧に書いた」
と自然に読める月報フィードバック草案を作る。

必ず守ること：
- 偉そうに評価しない
- 押し付けない
- 原文に書かれていない背景・意図・性格を推測しない
- 一緒の部署で見ている視点だが、憶測で断定しない
- 良い点を必ず拾う（最低1つ）
- 事実(原文)と提案(あなたの言葉)を混同しない
"""

FEEDBACK_DRAFT_PROMPT_JP: str = """以下の形式で出力してください（余計な見出しは増やさない）。

【良い点】
- 1〜3点

【気になった点 / リスク】
- 0〜2点（無ければ「特になし」でも可）

【次にやると良さそうなこと】
- 1〜3点（低コスト順、具体的に）
"""