# Global Data Plan

## Goal
Build a global, multi-venue market view that feeds analytics and research while keeping execution on a single venue (Bybit).

## Canonical Aggregated Topics
- `market:oi_agg`: aggregated open interest
- `market:funding_agg`: aggregated funding rate
- `market:liquidations_agg`: aggregated liquidation value
- `market:liquidity_agg`: aggregated liquidity metrics (spread/depth/imbalance)
- `market:volume_agg`: spot vs futures volume
- `market:cvd_agg`: cumulative volume delta
- `market:cvd_spot_agg`: spot-only cumulative volume delta
- `market:cvd_futures_agg`: futures-only cumulative volume delta
- `market:price_index`: aggregated index price

Each payload includes:
- `symbol`
- `ts` (market timestamp, unix ms)
- primary value(s)
- optional `venueBreakdown` (per-venue values)
- `meta` with `source=global_data`

## Normalized Raw Topics
- `market:trade`: venue trades (spot/futures) -> drives local CVD buckets
- `market:orderbook_l2_snapshot` / `market:orderbook_l2_delta`: local orderbooks -> liquidity metrics
- `market:cvd_spot` / `market:cvd_futures`: per-venue CVD buckets emitted by `CvdCalculator`

## Provider Adapters
Provider adapters normalize vendor-specific formats into canonical topics.
- Providers should emit `GlobalDataProviderEvent` with topic + payload.
- Provider errors are routed to `GlobalDataGateway` for health signals.

## Quality Signals
The system emits:
- `data:sourceDegraded` / `data:sourceRecovered`
- `data:stale`
- `data:mismatch`

Staleness and mismatch are detected deterministically from incoming events using `event.meta.ts`.

## Determinism and Replay
- No `Date.now()` in global data plane.
- All timestamps derived from upstream market timestamps or `event.meta.ts`.
- Aggregated events are replayable via journal + replay pipeline.
