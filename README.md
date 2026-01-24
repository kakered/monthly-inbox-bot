# medchem-inbox-bot (level 0)

Dropbox の /0-Inbox に置いた .txt / .md を、内容から自動分類して
OpenAI APIで処理し、/0-Outbox に結果を保存する最小パイプラインです。

## Dropbox側フォルダ（推奨）
- `/0-Inbox/` : 入力（.txt / .md）
- `/0-Outbox/` : 出力（.out.md）
- `/0-System/state.json` : 処理済み管理

## GitHub Secrets
- `OPENAI_API_KEY`
- `OPENAI_MODEL`（例: `gpt-5`）
- `DROPBOX_ACCESS_TOKEN`

## 監査
出力末尾に `Processing metadata` が必ず付きます：
Mode / Confidence / Prompt ID / Prompt hash / 処理時刻 / 入力パス
