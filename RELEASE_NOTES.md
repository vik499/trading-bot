# Release v0.4.0

Date: 2026-01-26

## Highlights
- Event-driven market data pipeline expanded with global data aggregation (price, flow, liquidity, open interest, funding) and quality monitoring.
- Exchange connectivity hardened with OKX/Binance/Bybit normalization, gap detection, resync throttling, and kline bootstrap support.
- Determinism enforced across planes (meta.ts propagation, ordering guards, replay determinism).
- New analytics plane (features, flow, liquidity, regimes, market context/view) and observability (EventTap, MarketDataReadiness).
- Comprehensive test suite additions across pipelines, contracts, and determinism invariants.

## Breaking Changes
- Event schema expanded and normalized; some event field names/types updated to align with deterministic pipelines.
- Hard test entry moved to src/hard-tests (npm script updated).

## Known Issues
- MarketDataReadiness warns when expected sources config is missing for specific symbols (requires config updates).

## Next Steps (Phase 0)
- Finalize expected sources configuration for all target markets.
- Extend OKX public channel coverage and add additional fixture parity.
- Harden replay storage compaction and add size-based retention policy.