# Data Contracts

This document defines canonical measurement contracts for market data events.

## Normalized market event meta

Applies to: `market:ticker`, `market:kline`, `market:trade`, `market:orderbook_l2_snapshot`, `market:orderbook_l2_delta`, `market:oi`, `market:funding`, `market:liquidation`.

Rules:
- `marketType` must be a known value: `spot` or `futures`.
- `streamId` is required and must match `meta.streamId`.
- `meta.tsEvent` is required (event-time).
- `meta.tsIngest` SHOULD be present (ingest-time). Some producers may omit it; consumers must handle missing.
- Aggregators MUST NOT set `meta.tsIngest = meta.tsEvent`. If upstream `tsIngest` is missing, aggregators use local `now()`.

## Canonical price

Topic: `market:price_canonical`

Fields:
- `indexPrice`: preferred normalization price when present.
- `markPrice`: fallback for normalization when index is missing.
- `lastPrice`: last-traded price for diagnostics only.
- `priceTypeUsed`: `index` | `mark` | `last`.
- `fallbackReason`: `NO_INDEX` | `INDEX_STALE` | `NO_MARK` | `MARK_STALE`.
- `sourcesUsed`: sources contributing to the canonical selection.
- `freshSourcesCount`: count of fresh sources used.
- `staleSourcesDropped`: sources excluded due to TTL.
- `mismatchDetected`: whether venue divergence exceeds the mismatch threshold.
- `confidenceScore`: 0-1 score derived from freshness and mismatch penalties.

Rules:
- Prefer `indexPrice` for USD normalization.
- Use `markPrice` only when index is missing or stale.
- Use `lastPrice` only when both index and mark are missing or stale.
- Never use raw trade price for notional conversions without explicit labeling.

## Open interest

Topic: `market:oi` (raw), `market:oi_agg` (aggregate)

Fields:
- `openInterest`
- `openInterestUnit`: `base` | `contracts` | `usd` | `quote` | `unknown`
- `openInterestValueUsd` (optional)
- `priceTypeUsed` (optional, only when USD value derived from canonical price)

Rules:
- Aggregators must not mix units.
- USD conversion is only allowed with a fresh canonical price.
- If multiple units are present in the same aggregate, emit the dominant unit and set `qualityFlags.consistentUnits=false` + `mismatchDetected=true`.
- Raw feeds must set the correct unit (for example, contract-based OI should use `contracts`).

## CVD

Topics: `market:cvd_spot`, `market:cvd_futures`, `market:cvd_*_agg`

Fields:
- `cvdDelta`
- `cvdTotal`
- `unit`: `base` | `usd`
- `bucketSizeMs`
- `bucketStartTs` / `bucketEndTs` (bucket close timestamp)

Rules:
- Spot and futures are emitted separately.
- USD normalization must be explicit via `unit`.
- For aggregated CVD, `venueBreakdown` reflects normalized totals (after per-source unit/sign normalization when configured).
- Aggregated CVD may include mismatch diagnostics:
  - `mismatchType`: `NONE` | `SIGN` | `DISPERSION`
  - `mismatchReason`: `SIGN` | `DISPERSION`
  - `signAgreementRatio` (number | null)
  - `scaleFactors` / `scaledVenueBreakdown` (per-venue scaling diagnostics)
 - `mismatchDetected` is reserved for HARD sustained divergence (policy-driven), not every mismatchType.

## Liquidations

Topic: `market:liquidations_agg`

Fields:
- `liquidationCount`
- `liquidationNotional`
- `unit`: `base` | `usd`
- `sideBreakdown`
- `bucketStartTs` / `bucketEndTs` (bucket close timestamp)

Rules:
- If USD notional is not available for all sources, emit `unit=base`.

## Liquidity

Topic: `market:liquidity_agg`

Fields:
- `spread`
- `midPrice`
- `depthBid`
- `depthAsk`
- `imbalance`
- `depthMethod`: `levels` | `usd_band` | `bps_band`
- `depthLevels`
- `depthUnit`: `base` | `usd`
- `priceTypeUsed` (optional): `index` | `mark` | `last`
- `bucketStartTs` / `bucketEndTs` (bucket close timestamp)

Rules:
- Depth metrics must always declare a method.

## Quality fields (all aggregates)

All aggregated events include:
- `sourcesUsed`
- `freshSourcesCount`
- `staleSourcesDropped`
- `mismatchDetected`
- `qualityFlags.sequenceBroken` when L2 sequence integrity is violated.
- `confidenceScore`

Confidence is derived from freshness and deterministic penalties and is stable across replay.

## Deterministic bucket close

For any bucketed aggregate, compute:

$$
	ext{bucketCloseTs} = \left\lfloor \frac{\text{exchangeTs}}{\text{bucketMs}} \right\rfloor \cdot \text{bucketMs} + \text{bucketMs}
$$

`bucketStartTs = bucketCloseTs - bucketMs`.

## Determinism requirements

- `sourcesUsed` and `staleSourcesDropped` are emitted in deterministic lexicographic order.
- `degradedReasons` are emitted in a fixed priority order.
- Replay must use deterministic ordering by `exchangeTs`, then `sequenceId`, then `streamId`.

## Market data readiness

Topic: `system:market_data_status`

Fields:
- `overallConfidence`
- `blockConfidence`: `price` | `flow` | `liquidity` | `derivatives`
- `degraded` / `degradedReasons`
- `warmingUp` / `warmingProgress` / `warmingWindowMs`
- `activeSources` / `expectedSources`
- `lastBucketTs`
- `aggSourcesUsed` / `aggSourcesSeen` / `aggSourcesUnexpected` (optional diagnostics)
- `rawSourcesUsed` / `rawSourcesSeen` / `rawSourcesUnexpected` (optional diagnostics)
- `exchangeLagP95Ms` / `exchangeLagEwmaMs` (optional, ms)
- `processingLagP95Ms` / `processingLagEwmaMs` (optional, ms)

Rules:
- Deterministic buckets (no wall clock).
- Reasons are emitted in a fixed order.
- Missing inputs in a bucket lower confidence deterministically.
- Readiness degradation uses critical blocks (default: price, flow, liquidity). Derivatives can be non-critical.
- Critical blocks are configurable; derivatives can be treated as non-blocking when desired.
- Flow confidence is evaluated for the reported `marketType` when available (spot uses `flow_spot`, futures uses `flow_futures`); unknown marketType may consider both.
- `LAG_TOO_HIGH` refers to processing lag only (system falling behind).
- Exchange/transport lag (tsIngest - tsEvent) must never degrade readiness; it is surfaced as warning `EXCHANGE_LAG_TOO_HIGH` only.
- Exchange lag stats ignore events older than 10 minutes to avoid backfill contaminating lag warnings.
- Lag stats are expressed in milliseconds.

Health snapshots (`logs/health.jsonl`) may include optional telemetry when reasons are present:
- `gapTelemetry` (GAPS_DETECTED attribution: markers + input gap stats)
- `priceStaleTelemetry` (PRICE_STALE attribution: age/bucket + sources)
- `refPriceTelemetry` (NO_REF_PRICE attribution: price validity + source/suppression summary)
