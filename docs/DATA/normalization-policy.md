# Normalization Policy

This policy defines how market data is normalized and bucketed so results are comparable and reproducible.

## Deterministic time

- All aggregation uses `event.meta.ts` or exchange timestamps.
- Bucket boundaries are computed from the event timestamp only.
- Emissions occur on bucket close.

Bucket close rule:

$$
	ext{bucketCloseTs} = \left\lfloor \frac{\text{exchangeTs}}{\text{bucketMs}} \right\rfloor \cdot \text{bucketMs} + \text{bucketMs}
$$

`bucketStartTs = bucketCloseTs - bucketMs`.

## Canonical price usage

- `market:price_canonical` is the only accepted reference for USD conversions.
- Index price is preferred, mark price is the fallback.
- `priceTypeUsed` records the chosen price type: `index` | `mark` | `last`.
- `fallbackReason` records why index/mark was not used: `NO_INDEX` | `INDEX_STALE` | `NO_MARK` | `MARK_STALE`.
- Raw trade prices are never used for notional conversions without explicit labeling.

## Unit visibility

- Every notional or quantity field carries an explicit unit.
- Aggregators do not mix units inside a single event.
- If conversion cannot be validated, emit the base unit with a quality flag.

## TTL policy

- Aggregation TTLs are configurable per topic.
- Per-topic TTLs override the global fallback:
	- `BOT_OI_TTL_MS`
	- `BOT_FUNDING_TTL_MS`
	- `BOT_CVD_TTL_MS`
	- `BOT_LIQUIDATIONS_TTL_MS`
	- `BOT_LIQUIDITY_TTL_MS`
	- `BOT_PRICE_TTL_MS`
- `BOT_GLOBAL_TTL_MS` remains the default when a per-topic TTL is not set.

## Aggregation rules

- CVD is bucketed by `bucketSizeMs` and emitted on bucket close.
- Liquidity uses deterministic buckets and declares `depthMethod`.
- Liquidation buckets emit counts plus notional with explicit unit.

### TTL policy (per-topic)

Aggregators drop stale sources based on per-topic TTLs. Defaults fall back to `BOT_GLOBAL_TTL_MS`.

- `BOT_OI_TTL_MS`
- `BOT_FUNDING_TTL_MS`
- `BOT_CVD_TTL_MS`
- `BOT_LIQUIDATIONS_TTL_MS`
- `BOT_LIQUIDITY_TTL_MS`
- `BOT_PRICE_TTL_MS`

## Quality scoring

- `freshSourcesCount` reflects sources within TTL for the bucket.
- `staleSourcesDropped` lists sources excluded due to staleness.
- `mismatchDetected` is raised when venue divergence exceeds the mismatch threshold.
- `confidenceScore` is computed deterministically from freshness and mismatch.

Confidence formula (version: `v1`):

Let $f$ = fresh sources count, $e$ = expected sources, $s$ = stale dropped count.

Base:

$$
	ext{base} = \begin{cases}
\frac{f}{e} & e > 0 \\
\frac{f}{f + s} & e = 0 \;\land\; f + s > 0 \\
0 & \text{otherwise}
\end{cases}
$$

Penalties (multiplicative):
- mismatch: $\times 0.5$
- gap: $\times 0.7$
- sequenceBroken: $\times 0.5$
- lag: $\times 0.8$
- outlier: $\times 0.8$
- fallback (canonical price): $\times 0.85$ for mark, $\times 0.6$ for last

### Source trust calibration

Some feeds have known limitations, applied deterministically as caps or penalties:

- OKX liquidation-orders → confidence cap: $\le 0.7$
- Bybit liquidation bankruptcy price → penalty: $\times 0.9$
- Binance aggTrade precision → optional penalty: $\times 0.95$

When multiple sources are present, penalties multiply and caps take the minimum.

Clamp to $[0,1]$.

## Replay determinism

- Journal replay must reproduce identical aggregated outputs for the same raw inputs.
- Any new aggregate must be validated with replay determinism tests.
- Replays use deterministic ordering: `exchangeTs`, then `sequenceId`, then `streamId`.
- Output arrays are stable-sorted:
	- `sourcesUsed` and `staleSourcesDropped` are lexicographically ordered.
	- `degradedReasons` use fixed priority ordering.

## Market data readiness

- `system:market_data_status` is derived from aggregated metrics only.
- Buckets use deterministic timestamps (bucket close).
- Warm-up progress uses bucket timestamps (no wall clock).
