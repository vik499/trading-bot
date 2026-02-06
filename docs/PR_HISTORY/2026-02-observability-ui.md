# 2026-02 Observability UI Console + File Artifacts

## Summary
- Console теперь в режиме короткого UI (по умолчанию `CONSOLE_MODE=ui`).
- Подробные логи пишутся в файлы: `logs/app.log`, `logs/warnings.log`, `logs/errors.log`.
- Состояние и health‑снапшоты пишутся в `logs/health.jsonl` (JSONL).

## Env (defaults)
- `CONSOLE_MODE=ui|verbose`
- `CONSOLE_HEARTBEAT_MS=30000`
- `CONSOLE_TRANSITION_COOLDOWN_MS=10000`
- `CONSOLE_SHOW_TICKER=false`

## Data quality staleness policy (topic-aware)
False-positive stale warnings for slow-moving aggregates are suppressed by a per-topic policy.

Defaults (can be overridden via env):
- `BOT_DQ_OI_AGG_EXPECTED_INTERVAL_MS=300000` (5 min)
- `BOT_DQ_OI_AGG_STALE_THRESHOLD_MS=900000` (15 min)
- `BOT_DQ_FUNDING_AGG_EXPECTED_INTERVAL_MS=28800000` (8 h)
- `BOT_DQ_FUNDING_AGG_STALE_THRESHOLD_MS=43200000` (12 h)

Notes:
- `warnings.log` stale/mismatch lines include `key=...` and `sourceId=...` for grep-friendly correlation with UI and `health.jsonl`.
- Mismatch warnings include `unit`, `baseline`, `observed`, `diffAbs`, `diffPct`, and venues.

## What stays in console (UI mode)
- Стартовые строки (версия, runId, режим, где логи).
- Изменения состояния MarketData (transition‑only).
- Короткий heartbeat раз в `CONSOLE_HEARTBEAT_MS`.
- Краткие события деградации/восстановления источников.
- Агрегированная сводка по отклонённым сигналам (раз в 60с).

## Example console output (UI mode)
```
[vavanbot] Старт: режим=LIVE runId=2026-02-03T12-00-00Z_ab12cd
[vavanbot] Версия: app=0.4.0 git=v0.4.0 node=v20.11.0
[vavanbot] Логи: logs/app.log, logs/warnings.log, logs/errors.log, logs/health.jsonl
[vavanbot] Подключения WS: 0/4
[vavanbot] MarketData: ПРОГРЕВ conf=0.42
[vavanbot] Подключение: +1 (1/4)
[vavanbot] Данные: источник okx.public.swap:market:price_index деградировал (stale)
[vavanbot] Пульс: режим=LIVE пауза=НЕТ состояние=RUNNING рынок=ПРОГРЕВ поток=OK
[vavanbot] MarketData: ГОТОВ conf=0.86
```
