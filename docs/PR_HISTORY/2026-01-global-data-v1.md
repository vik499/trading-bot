# Historical PR Context — Global Data v1

⚠️ Historical context only  
Architecture rules: `docs/CANON.md`  
Current priorities: `docs/ROADMAP.md`  
Long-term decisions: `docs/DECISIONS.md`

This document describes what was implemented during the Global Data v1 phase and why at that time.

# PR Description — Live Market Data Stabilization

## Кратко
- Прокинут `marketType` через raw/agg события + инференс из `streamId`.
- Readiness учитывает `marketType` и `expectedSources` (flow/derivatives).
- Дедуп подписок и backoff для Binance 1008; сериализация ресинка и cooldown для OKX.
- Стратегия гейтится readiness, шум логов снижен.
- Зафиксирован единый `BOT_TARGET_MARKET_TYPE` (без смешения spot/futures в runtime).
- Добавлен startup grace window для readiness (`BOT_READINESS_STARTUP_GRACE_MS`).
- OKX reconnect backoff и per‑reason resync cooldown, снижены resync‑шторма.
- Добавлены тесты: `marketType` resolution, `expectedSources` config, trade flow routing, Binance WS backoff/dedup.
 - Добавлены тесты Step 4: startup grace, target marketType, skip‑лог throttling, OKX backoff, resync cooldown.

## Анализ (по итогам стабилизации live)
### Цели
- Довести readiness до READY (убрать WARMING/DEGRADED из‑за `marketType` и ожидаемых источников).
- Устранить шторм подписок/ресинков и лимиты Binance 1008.
- Снизить шум логов и блокировать стратегию при not-ready.

### Основные причины проблем
- `marketType` был неизвестен на части raw/agg событий → readiness не мог корректно учитывать источники.
- `expectedSources` отсутствовали/не резолвились → readiness всегда WARMING/DEGRADED.
- Дублирующиеся подписки и параллельные ресинки (особенно OKX) → rate limit и ресинк‑лупы.
- Readiness деградировал слишком рано на старте из‑за отсутствия данных/WS.
- Смешение spot/futures в одном запуске → деградации по «чужому» типу рынка.
- Стратегия спамила skip‑логи при not‑ready.

### Принятые решения
- Прокидывание `marketType` через raw и agg события и его инференс из `streamId`.
- Введение конфигурации `expectedSources` и резолвера ожиданий (flow/derivatives) с фолбэком.
- Дедуп подписок и backoff для Binance 1008; сериализация ресинка и cooldown для OKX.
- Гейтинг стратегии по readiness и троттлинг not-ready логов.
- Принудительная фильтрация `BOT_TARGET_MARKET_TYPE` в orchestrator и live‑bot.
- Startup grace window для readiness, чтобы избежать ложных DEGRADED на старте.
- OKX backoff с ростом + reset при стабильном соединении.
- Per‑reason resync cooldown и rate‑limit WARN, чтобы снизить resync‑шторм.

### Ключевые изменения (сводно)
- Readiness использует `marketType`, `expectedSources`, ожидаемые типы flow/derivatives.
- WS/REST клиенты добавляют `marketType` в orderbook/OI/funding события.
- Агрегаторы сегментированы по `marketType` (цены, ликвидность, OI, funding, liquidation, CVD/индексы).
- Улучшена устойчивость подписок/ресинков и уменьшен шум.
- Readiness: `startupGraceWindowMs` гасит ранние `PRICE_STALE`, `SOURCES_MISSING`, `WS_DISCONNECTED`, `LAG_TOO_HIGH`.
- Orchestrator/live‑bot: target marketType → только один контур (spot **или** futures).
- Strategy: skip‑лог ограничен по ключу и времени.
- OKX: backoff base=500, multiplier=2, max=8000 + reset; MarketGateway: resync cooldown per‑reason.

### Тесты
- Добавлены детерминированные тесты для резолва `marketType`, `expectedSources`, роутинга trade flow, backoff/dedup Binance WS.
- Добавлены тесты Step 4:
	- readiness startup grace window
	- target marketType filter (orchestrator)
	- стратегия: rate limit skip‑лога
	- OKX reconnect backoff
	- MarketGateway resync cooldown
- `npm test` — passed.
