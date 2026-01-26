# Liquidations

## Overview
Liquidation data is ingested on a best-effort basis from public exchange streams.
Raw events are bucketed into aggregated liquidation stats for analytics.

## Pipeline
1) Exchange adapters publish `market:liquidation` with `side`, `price`, `size`, and `meta.ts`.
2) `LiquidationAggregator` buckets events by `event.meta.ts` and emits `market:liquidations_agg`.

## Aggregated fields
- `liquidationUsd`: bucket notional (price * size when available)
- `liquidationQty`: raw size sum (unit depends on venue)
- `sideBreakdown`: `buyUsd` vs `sellUsd`
- `bucketStartTs` / `bucketEndTs`

## Sources
- Binance USDT-M WebSocket `forceOrder`
- OKX public WebSocket `liquidation-orders`
- Bybit WebSocket `allLiquidation`

## Config
- `BOT_LIQUIDATIONS_ENABLED`
- `BOT_LIQ_BUCKET_MS` (default 10000)
- `BOT_LIQUIDATIONS_TTL_MS` (fallback: `BOT_GLOBAL_TTL_MS`)

## Limitations
- Per-venue liquidation feeds may be sampled and not complete.
- Size units vary (contracts vs base); `liquidationQty` is not normalized.
