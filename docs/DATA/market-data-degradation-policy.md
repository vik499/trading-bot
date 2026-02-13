# Market Data Degradation Policy (v1.2)

**Scope**  
This document defines **how market data quality is evaluated**, **how degradation is classified**, and **how non-binary, industry-grade behavior is enforced**.  
This policy is **normative**: implementation, tests, analytics, and observability **must follow it**.

---

## 1. Core Principles

### 1.1 Non-binary readiness
Market data readiness is **not boolean**.

We explicitly separate:
- **Transient anomalies** (expected in live exchange feeds)
- **Sustained quality failures** (actionable degradation)

Short-lived issues **must not immediately flip system state**.

---

### 1.2 Truth vs stability (multi-loop)
The system operates **two parallel loops**:

- **Truth loop (analytics)**  
  Records *everything* that happens:
  - worst conditions per minute
  - full reason unions
  - raw explainability

- **Stability loop (runtime / UI / control)**  
  Exposes a **stable status**:
  - hysteresis-protected
  - no flapping
  - actionable for humans and automation

These loops **must never be conflated**.

---

### 1.3 Worst-case visibility, not panic
We always **record the worst condition observed** (truth),
but **do not immediately act on it** (stability) unless policy thresholds are met.

This enables:
- honest observability
- stable runtime behavior
- post-mortem explainability

---

### 1.4 Policy before heuristics
All degradation logic is:
- policy-defined
- deterministic
- testable
- auditable via `runId`

No implicit heuristics.

---

## 2. Degradation Dimensions (Buckets)

Each degradation reason belongs to **exactly one primary bucket**.

### 2.1 GAP — Sequence / Continuity
Structural issues in event ordering or completeness.

Examples:
- orderbook sequence gaps
- missing incremental updates
- replay / backfill inconsistencies

**Nature:** structural  
**Behavior:** bursty, exchange-side

---

### 2.2 PRICE — Freshness / Reference
Issues with price validity.

Examples:
- `PRICE_STALE`
- `NO_VALID_REF_PRICE`
- `PRICE_BUCKET_MISMATCH` (warning-only)
- missing index / mark price

**Nature:** temporal  
**Behavior:** transient, cross-source

---

### 2.3 LIQUIDITY / FLOW — Confidence
Issues with statistical reliability.

Examples:
- `LIQUIDITY_LOW_CONF`
- `FLOW_LOW_CONF`
- sparse trades / volume

**Nature:** informational  
**Behavior:** market-regime dependent

---

### 2.4 SOURCE AVAILABILITY
Issues with venue availability.

Examples:
- `SOURCES_MISSING`
- single-venue fallback

**Nature:** infrastructural

---

## 3. Severity Model

Each degradation reason maps to a **severity class**.

### 3.1 Severity classes

| Severity | Meaning | Default impact |
|--------|--------|----------------|
| `INFO` | Informational anomaly | No state change |
| `SOFT` | Quality warning | Confidence reduction |
| `HARD` | Structural failure | Candidate for degradation |

Only **HARD** reasons may trigger a sustained `DEGRADED` state.

---

### 3.2 HARD sub-classes (mandatory)

HARD reasons are split into **two operational profiles**:

#### HARD-fast (immediate risk)
Examples:
- `NO_DATA`
- `WS_DISCONNECTED`

Characteristics:
- fast enter
- slow exit
- infrastructure-breaking

#### HARD-sustained (structural quality)
Examples:
- `GAPS_DETECTED`
- `PRICE_STALE`
- `NO_VALID_REF_PRICE`

Characteristics:
- enter only if persistent
- exit only after sustained recovery
- typical live-feed noise source

---

## 4. Canonical Minute Semantics (Truth Loop)

### 4.1 Instant vs rollup

We explicitly distinguish:

- `status` — instantaneous snapshot decision
- `worstStatusInMinute` — worst severity observed within the minute
- `reasonsUnionInMinute` — deterministic union of reasons (sorted, deduped)
- `warningsUnionInMinute` — deterministic union of warnings

Rules:
- `worstStatusInMinute` reflects **truth**
- unions are **deterministic**
- minute rollups are **analytics-only**, never used directly for live control

This prevents hidden oscillations and preserves explainability.

---

## 5. Hysteresis (Stability Loop)

### 5.1 General rule
**State changes are time-based, not event-based.**

No READY ↔ DEGRADED flapping on single pulses.

---

### 5.2 Entering `DEGRADED`

System enters sustained `DEGRADED` only if:

- a **HARD** reason is active
- it persists for at least `enterWindowMs`

Default profiles:

| HARD class | enterWindowMs | exitWindowMs |
|----------|---------------|--------------|
| HARD-fast | 1 000 ms | 12 000 ms |
| HARD-sustained | 5 000 ms | 12 000 ms |

Single pulses **must not** flip state.

---

### 5.3 Exiting `DEGRADED`

System exits `DEGRADED` only if:

- **no HARD reasons** are active
- system remains clean for `exitWindowMs`

This guarantees stability and prevents “пила”.

---

### 5.4 Phase 0 reason semantics (normative)

`PRICE_STALE` is strictly age-based:

- Emit `PRICE_STALE` **iff** `priceStaleTelemetry.ageMs >= priceStaleTelemetry.staleMs`.
- `bucketCloseTs` mismatch is not staleness.
- Bucket alignment (выравнивание бакета) issues must be emitted as `PRICE_BUCKET_MISMATCH` (warning-only (только предупреждение), truth-loop/observability (контур наблюдаемости), non-HARD (не жёсткий)).

`NO_VALID_REF_PRICE` replaces `NO_REF_PRICE` as the canonical degradation reason:

- Emit `NO_VALID_REF_PRICE` only if at least one condition is true:
  - `refPriceTelemetry.hasPriceEvent == false`
  - `refPriceTelemetry.ageMs >= refPriceTelemetry.staleMs`
  - `refPriceTelemetry.confidence < critical threshold`
  - `refPriceTelemetry.sourcesUsed` is empty
  - all candidate sources are suppressed/dropped
- Explicit prohibition for Phase 0:
  - if `hasPriceEvent == true` AND `ageMs < staleMs` AND `confidence >= critical threshold`, degradation must not be emitted.
- `NO_REF_PRICE` is deprecated (устаревшее) (legacy compatibility label only) and must not be used as the canonical policy term.

### Canonical Degraded Reason: NO_VALID_REF_PRICE

- `NO_VALID_REF_PRICE` is the canonical degraded reason.
- `NO_REF_PRICE` is a legacy alias and must be normalized.
- Emitted degraded reasons must never contain `NO_REF_PRICE`.

`GAPS_DETECTED` is connector-validated only:

- `GAPS_DETECTED` may be driven only by venue-native continuity checks that result in `market:resync_requested`.
- `data:sequence_gap_or_out_of_order` is debug/journal telemetry (диагностическая телеметрия журнала) and must not independently trigger sustained degradation.

---

## 6. Confidence (`conf`) Interaction

- `conf` is **continuous**, not binary
- SOFT reasons reduce `conf` immediately
- HARD reasons cap `conf`

### 6.1 Invariant
`conf` **must never override HARD degradation**.

Confidence affects *quality perception*, not *structural safety*.

---

## 7. Observability & Explainability (Invariant)

For every degraded minute:

- degradation **must be explainable**
- matching telemetry **must exist**

Examples:
- GAP → sequence counters / gap metrics
- PRICE → age / reference telemetry + bucket-mismatch warning telemetry
- LIQUIDITY → sample size / volume metrics

**If a reason is emitted without telemetry, it is a bug.**

### 7.1 Source Coverage & Attribution

Each health snapshot must include deterministic source coverage per metric bucket:
- `expected` — configured expected streamIds from source registry
- `active` — streamIds actually used in current aggregated computation
- `missing` — `expected - active`
- `unexpected` — `active - expected`
- `coverageRatio` — covered expected sources / expected total (`1` when expected is empty)
- `unexpectedTotal` — count of active-but-not-expected sources

Arrays are always sorted and deduplicated. This attribution is emitted for every snapshot (including READY) so Phase 0 diagnostics can explain coverage regressions before they become sustained degradation.
`expectedTotal=0 => coverageRatio=1` is a reporting convention only; configuration validity is enforced separately via `EXPECTED_SOURCES_MISSING_CONFIG` (HARD-fast).

### 7.2 Timebase Integrity (Phase 0 truth-loop, warning-only)

Each health snapshot may include `timebase` diagnostics per readiness block:
- event-time age stats (`ageMsP50/P95/max`, `samples`)
- future-event symptoms (`futureEventCount`, `futureEventMaxSkewMs`)
- monotonicity symptoms (`nonMonotonicCount`, `lastEventTs`)
- deterministic warning union (`TIMEBASE_FUTURE_EVENT`, `TIMEBASE_NON_MONOTONIC`, `TIMEBASE_AGE_TOO_HIGH`, optional `TIMEBASE_DRIFT_SUSPECTED`)

In Phase 0 these are observability warnings only and do not directly trigger readiness degradation. They exist to explain analytics correctness issues (event-time vs ingest-time divergence) before escalation policy is explicitly revised.

---

## 8. Explicitly Forbidden

- Immediate state flips on single events
- Boolean “data ok / data broken”
- Alert spam from transient noise
- Silent degradation without attribution
- Implementation-defined thresholds

---

## 9. Enforcement

This policy is enforced by:
- synthetic hysteresis tests
- minute rollup analytics
- runId-scoped diagnostics
- code review

**Any logic change requires updating this file first.**

---

## Status

- Version: `v1.2`
- Phase: **Phase 0 — Data Correctness & Observability**
- Next extensions:
  - regime-aware thresholds
  - symbol-specific tuning
  - venue weighting
