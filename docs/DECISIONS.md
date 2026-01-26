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