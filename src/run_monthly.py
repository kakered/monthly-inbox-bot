# -*- coding: utf-8 -*-
"""
run_monthly.py

目的:
- 将来 `python -m src.run_monthly` を実行入口にしたい場合の薄いラッパー
- monthly_main.main() を呼ぶだけ（引数不要）
"""

from __future__ import annotations

from .monthly_main import main

if __name__ == "__main__":
    raise SystemExit(main())