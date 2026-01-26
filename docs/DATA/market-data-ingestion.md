# Market Data Ingestion

## Raw Event Contract

### Общие правила
- Все raw события содержат `meta.ts = exchangeTsMs`.
- Canonical `symbol` формируется из `venueSymbol` и должен быть детерминированным.
- `sourceId/connectionId`: если доступен, используем `streamId` (сохраняется как `venueSymbol` + `streamId` в контекстах, но не обязателен).
- Запрещённые поля для raw: `qualityFlags`, `confidenceScore`, `venueBreakdown`, `staleSourcesDropped`, `mismatchDetected`, `sourcesUsed`, `weightsUsed`.

### Raw topics (эмитируются Bybit/Binance/OKX)
Список raw топиков:
- `market:trade_raw`
- `market:orderbook_snapshot_raw`
- `market:orderbook_delta_raw`
- `market:candle_raw`
- `market:mark_price_raw`
- `market:index_price_raw`
- `market:funding_raw`
- `market:open_interest_raw`
- `market:liquidation_raw`

### Trades (`market:trade_raw`)
Обязательные поля:
- `venue`: bybit | binance | okx
- `symbol`: canonical symbol (string)
- `exchangeTsMs`: number (ms)
- `recvTsMs`: number (ms)
- `price`: string
- `size`: string
- `side`: buy | sell
Опциональные:
- `venueSymbol`: string
- `tradeId`: string
- `seq`: number

### Orderbook
Topics: `market:orderbook_snapshot_raw`, `market:orderbook_delta_raw`

Обязательные поля:
- `venue`, `symbol`, `exchangeTsMs`, `recvTsMs`
- `bids`, `asks`: array of [price, size] strings
Опциональные:
- `venueSymbol`: string
- `sequence`: number (если есть у биржи)
- `prevSequence` (если доступно)
- `range` (binance U..u)

Если `sequence` отсутствует, считаем книгу валидной только после snapshot и продолжаем обработку без seq‑гарантий.

### Candles (`market:candle_raw`)
Обязательные поля:
- `venue`, `symbol`, `interval`
- `startTsMs`, `endTsMs`, `exchangeTsMs`, `recvTsMs`
- `open`, `high`, `low`, `close`, `volume`: strings
- `isClosed`: boolean
Опциональные:
- `venueSymbol`

### Derivatives
Topics:
- `market:mark_price_raw`
- `market:index_price_raw`
- `market:funding_raw`
- `market:open_interest_raw`
- `market:liquidation_raw`

Обязательные поля:
- `venue`, `symbol`, `exchangeTsMs`, `recvTsMs`
Опциональные:
- `venueSymbol`
- `nextFundingTsMs` (funding)
- `notionalUsd` (liquidations)

Все числовые значения — строки.

## Resync Contract

### Gap detection (orderbook)
**Binance depth**
- Snapshot via REST (`lastUpdateId`).
- WS `depthUpdate` uses `U`/`u`.
- After snapshot, apply the first delta where `U <= lastUpdateId + 1 <= u`.
- Deltas can arrive slightly out of order; sort buffered updates by `U/u` before applying.
- If `U > lastUpdateId + 1` after snapshot -> emit `market:resync_requested` (`reason=gap`).

**Bybit orderbook**
- Use `u/seq` monotonicity.
- If `sequence` gap -> emit `market:resync_requested` (`reason=gap`).

**OKX books**
- Use `prevSeqId` + `seqId` sequencing (per OKX docs).
- Snapshot: `prevSeqId = -1`.
- Incremental: `prevSeqId` must equal last applied `seqId`.
- Heartbeat/no-update: `prevSeqId == seqId` with empty `bids/asks` is valid (no gap).
- Sequence reset can occur (`seqId < prevSeqId`) during maintenance; subsequent messages follow normal sequencing.
- Deltas before first snapshot are ignored (do not resync on them).
- Resync requested only after repeated gaps within a window (throttled).

### `market:resync_requested`
Кто эмитит:
- WS клиенты бирж (gap/out‑of‑order/snapshot missing).

Что делает MarketGateway:
- throttling/coalescing (защита от resync‑storm)
- `disconnect()` → `connect()`
- resubscribe всех подписок
- обновление snapshot для orderbook (если требуется)

Readiness:
- до получения валидных данных статус остаётся `degraded` или `warmingUp`
- на первых валидных payloads (raw/agg) -> восстановление в `ready`

### Book valid criteria
- Snapshot получен и принят.
- После snapshot последовательность монотонна (если seq доступен).
- Дубликаты/старые deltas игнорируются, разрывы -> resync.

## Heartbeat / reconnect contract

- Bybit: client `ping` (op: "ping"), handle `pong`.
- Binance: server ping, client pong (ws `ping` handler).
- OKX: client `ping` per interval, server `pong`.

Reconnect:
- Exponential backoff + deterministic jitter.
- On reconnect: resubscribe to all channels and refresh orderbook snapshots.
- On resync request: full reconnect with snapshot refresh.

## OKX public vs business routing

Причина: OKX возвращает ошибку `60018` при подписке на `candle*` каналы через public WS.
Чтобы избежать ошибок и потери данных, свечи (klines) для OKX подписываются **только** через business WS,
а остальные публичные каналы остаются на public WS.

Маршрутизация:
- OKX public WS: `tickers`, `trades`, `books`, `liquidation-orders`.
- OKX business WS: `candle*` (klines).

Технически это реализовано через разделение WS клиентов и `topicFilter` в `MarketGateway`:
- public gateway исключает `kline.*`
- business gateway принимает только `kline.*`

Флаг `OKX_ENABLE_KLINES` по-прежнему включает/отключает klines end-to-end:
- не создаётся OKX business WS клиент
- `MarketGateway` и OKX WS клиент не подписываются на `kline.*`

## Risk Gate Contract

Гейт находится в RiskManager.
- Вход: `strategy:intent`
- Проверка: последнее `system:market_data_status`
- Блокирует, если `warmingUp === true` или `degraded === true`
- Результат: `risk:rejected_intent` с `reasonCode = MARKET_DATA`
- Лог: строка с причиной (`market data not ready: ...`)
