# Liquidity Metrics

## Overview
Liquidity metrics are derived from local L2 orderbooks and aggregated across venues.
The metrics aim to capture spread, depth, and imbalance at the top of book.

## Pipeline
1) Exchange adapters publish `market:orderbook_l2_snapshot` and `market:orderbook_l2_delta`.
2) `LiquidityAggregator` maintains per-venue orderbook state and emits `market:liquidity_agg`.

## Metrics
- `bestBid`, `bestAsk`
- `spread`
- `depthBid`, `depthAsk` (sum of top N levels)
- `imbalance` (depth-based)
- `midPrice` (best bid/ask midpoint)

## Config
- `BOT_LIQUIDITY_DEPTH_LEVELS` (default 10)
- `BOT_LIQUIDITY_EMIT_MS` (default 500)
- `BOT_LIQUIDITY_MAX_UPDATES` (default 20)
- `BOT_LIQUIDITY_TTL_MS` (fallback: `BOT_GLOBAL_TTL_MS`) for staleness drop

## Limitations
- Depth uses top N levels, not bps- or USD-normalized depth.
- Aggregation is a weighted average across venues; venue-specific units may differ.
