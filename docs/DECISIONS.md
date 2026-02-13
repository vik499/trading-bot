# Architecture Decisions Log

This file records key engineering decisions and their rationale.

---

## Decision: Event-driven architecture only
Reason: Ensures decoupling, replayability, and deterministic pipelines.

## Decision: Raw vs Aggregated separation
Reason: Exchanges provide facts; platform produces interpretations.

## Decision: Multi-exchange aggregation
Reason: Single exchange data is not global truth.

## Decision: Deterministic time via event.meta.ts
Reason: Guarantees replay reproducibility.

## Decision: Research vs Production contour separation
Reason: Prevents experimental logic from causing financial risk.

(Add new decisions below as the system evolves)

## Decision: OKX public vs business split
Reason: OKX rejects candle subscriptions on public WS (error 60018). Klines are routed to business WS while public WS carries tickers/trades/books/liquidations to prevent subscription errors and data loss.

## Decision: OKX orderbook resync state machine
Reason: OKX orderbook sequencing uses `prevSeqId` → `seqId` (not strict +1). We ignore deltas before the first snapshot, accept heartbeat updates (`prevSeqId == seqId`), and trigger resync only after repeated gaps within a window to avoid reconnect storms.

## Decision: Binance WS client registry and subscription dedup
Reason: Prevent duplicate Binance WS instances and repeated subscribe frames by caching clients per stream key and reconciling subscriptions deterministically on reconnect.

## Decision: Platform-first, not strategy-first
Reason:
	•	Individual strategies are ephemeral.
	•	Market regimes persist longer than signals.
	•	The platform’s job is to:
	•	observe regimes
	•	estimate expectancy
	•	control risk
	•	explain decisions
    
## Decision: Decision Quality over PnL in early phases
Reason: Early trading results are dominated by variance, not edge.
The system is evaluated by stability, explainability, and reproducibility.

## Decision: Read-only and paper trading as mandatory phases
Reason: Capital before validated regime detection and risk control
turns unknown bugs into financial losses.

## 2026-02-11 — Decision: Phase 0 readiness semantics hardening (false-alarm closure)
Reason:
- `PRICE_STALE` is strictly age-based and may be emitted only when `ageMs >= staleMs`.
- `PRICE_BUCKET_MISMATCH` is warning-only (только предупреждение) (truth-loop/observability (контур наблюдаемости)) and does not degrade readiness in Phase 0.
- `NO_VALID_REF_PRICE` is the canonical degradation reason; `NO_REF_PRICE` is deprecated (устаревшее) terminology.
- `NO_VALID_REF_PRICE` is prohibited when ref price is present, fresh (`ageMs < staleMs`), and confidence is above threshold.
- `GAPS_DETECTED` may be triggered only by connector-confirmed (подтверждённый коннектором) venue-native continuity failure that emits `market:resync_requested`; debug (отладочная) sequence-gap telemetry is non-authoritative.

## 2026-02-11 — Reason Name Canonicalization
Reason:
- Canonical degraded reason: `NO_VALID_REF_PRICE`.
- Legacy alias `NO_REF_PRICE` normalized and removed from emitted output.
- Next planned: full removal of alias after one release window.

“Decision: Introduce Definition of Success as project compass.”
