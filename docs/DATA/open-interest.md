# Open Interest (OI) Data Pipeline

## Status
- The Bybit OI REST call now includes `intervalTime` to avoid `IntervalTime Is Required`.
- OI is ingested as `market:oi` and aggregated to `market:oi_agg`.
- Aggregation is multi-source (Bybit + Binance + OKX) with per-source weights and TTL filtering.

## Binance USDT-M source notes
- Open interest endpoint: `GET /fapi/v1/openInterest` (request weight 1). Response fields include `openInterest`, `symbol`, and `time`. Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest
- Funding/mark price endpoint: `GET /fapi/v1/premiumIndex` (request weight 1 with `symbol`). Response fields include `lastFundingRate`, `nextFundingTime`, `time`, and `markPrice`. Source: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Mark-Price
- Symbol format: `BTCUSDT` (canonical). We uppercase/trim in the collector before requests.
- Normalization: treat Binance OI as contract units (`openInterestUnit=contracts`). Do not infer USD notional without an explicit contract size.

## Bybit linear source notes
- Open interest endpoint: `GET /v5/market/open-interest`.
- Symbol format: `BTCUSDT`.
- Normalization: treat Bybit OI as contract units (`openInterestUnit=contracts`).

## OKX swaps source notes
- Open interest endpoint: `GET /api/v5/public/open-interest` (Rate Limit: 20 requests per 2 seconds). Response fields include `oi` (contracts), `oiCcy` (coin), `oiUsd` (USD), and `ts`. Source: https://www.okx.com/docs-v5/en/#public-data-rest-api-get-open-interest
- Funding rate endpoint: `GET /api/v5/public/funding-rate` (Rate Limit: 10 requests per 2 seconds). Response fields include `fundingRate`, `fundingTime`, `nextFundingTime`, and `ts`. Source: https://www.okx.com/docs-v5/en/#public-data-rest-api-get-funding-rate
- Symbol format: `BTC-USDT-SWAP` (OKX instId). We normalize canonical `BTCUSDT` to OKX instId when polling.
- Normalization: use `oiCcy` as base-asset units (`openInterestUnit=base`); set `openInterestValueUsd` from `oiUsd` when provided.

## Architecture (event-driven)
1) **Collector (exchange plane)**: `BybitDerivativesRestPoller` calls `/v5/market/open-interest` and publishes `market:oi`.
2) **Aggregator (global data plane)**: `OpenInterestAggregator` subscribes to `market:oi`, applies TTL + weights, emits `market:oi_agg`.
3) **Consumers**: MarketViewBuilder, EventTap, quality monitors.

All cross-module communication is via EventBus topics. Consumers use `event.meta` for timing/correlation.
Collector interface: `src/core/market/OpenInterestCollector.ts` (startForSymbol/stop) for future multi-venue collectors.

## Parameters and defaults
- `intervalTime`: required by Bybit; passed as-is from config.
  - **Default**: `5min`
  - **Env**: `BOT_OI_INTERVAL_TIME=5min`
- Polling interval:
  - `BybitDerivativesRestPoller` defaults to 30s and uses backoff + jitter on errors.
- Aggregator TTL:
  - Default 120s; configurable in `OpenInterestAggregator` options or `BOT_OI_TTL_MS` (falls back to `BOT_GLOBAL_TTL_MS`).
- Aggregator weights:
  - `BOT_GLOBAL_WEIGHTS` supports per-stream weights (e.g., `bybit.public.linear.v5:0.4,binance.usdm.public:0.4,okx.public.swap:0.2`).

## Multi-exchange expansion plan
To add new sources:
1) Implement an OI collector for the exchange (Binance/OKX/Deribit/etc.) that publishes `market:oi`.
2) Use a distinct `streamId` per venue (e.g., `binance.futures`, `okx.swap`).
3) Optionally supply weights in `OpenInterestAggregator` to balance sources.

## Decision matrix (placeholders – requires validation against official docs)
This table is intentionally conservative and must be verified with official documentation and ToS.

| Source | Freshness | Coverage | Reliability | Rate limits | Cost | Integration complexity | Legal/ToS risk | API stability |
|---|---|---|---|---|---|---|---|---|
| Bybit | Medium | Perp/linear | High | Medium | Free | Low | Low | High |
| Binance | Medium | Futures | High | Medium | Free | Medium | Medium | High |
| OKX | Medium | Swaps/Futures | High | Medium | Free | Medium | Medium | High |
| Deribit | Medium | Options/Perps | High | Medium | Free | Medium | Medium | High |
| Aggregator (e.g., market data provider) | High | Multi-venue | High | High | Paid | Medium | Medium | High |

## Limitations
- `market:oi_agg` does not blend incompatible units; a single unit is chosen based on available sources.
- `openInterestValueUsd` is only populated when all contributing sources supply notional values.
- The decision matrix above is a placeholder and must be validated against official documentation.
