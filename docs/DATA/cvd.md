# CVD (Cumulative Volume Delta)

## Overview
CVD is computed from normalized trade events and emitted separately for spot and futures.
The pipeline is additive across venues and deterministic for replay.

## Pipeline
1) Exchange adapters publish `market:trade` (with `marketType`, `streamId`, and `meta.ts`).
2) `CvdCalculator` buckets trades by `event.meta.ts` and emits:
   - `market:cvd_spot`
   - `market:cvd_futures`
3) `CvdAggregator` merges per-venue buckets into:
   - `market:cvd_spot_agg`
   - `market:cvd_futures_agg`
   - `market:cvd_agg`

## Bucketing and determinism
- Bucket size: `BOT_CVD_BUCKET_MS` (default 5000).
- Bucket boundaries use `event.meta.ts` (no `Date.now`).
- Aggregation drops stale sources using `BOT_CVD_TTL_MS` (fallback: `BOT_GLOBAL_TTL_MS`).

## Payload notes
Raw bucket (`market:cvd_*`) fields:
- `bucketStartTs`, `bucketEndTs`, `cvdDelta`, `cvdTotal`, `streamId`, `marketType`.

Aggregated (`market:cvd_*_agg`) fields:
- `sourcesUsed`, `weightsUsed`, `qualityFlags.staleSourcesDropped`.

## Normalization (units/sign)
`CvdAggregator` supports per-stream normalization to align units and sign:
- `unitMultipliers` — per-source multiplier to convert into base units.
- `signOverrides` — per-source sign override to align buy/sell convention (e.g., invert a venue).

When these overrides are used, `venueBreakdown` values reflect the normalized totals.

## Sources
- Binance USDT-M WebSocket (aggTrade stream)
- OKX public WebSocket trades channel
- Bybit public WebSocket trade channel

## Limitations
- Aggregated CVD is additive across venues; it is not notional-normalized.
- Trade side comes from exchange payloads and may vary by venue.
