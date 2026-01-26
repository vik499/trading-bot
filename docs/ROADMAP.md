# Trading Platform — Roadmap

Architecture rules live in CANON.md.  
This file contains evolving development priorities.

## Current Phase — Phase 0: Data Correctness

Goal: Make all market data comparable, reproducible and interpretable.

Focus:
- Symbol normalization
- Unit normalization
- Deterministic aggregation
- Data quality monitoring
- Replay validation

Exit Criteria:
- Aggregates reproducible via replay
- No silent unit mixing
- Quality gates deterministic
- MarketView built only on aggregated data

---

## Next Phases

### Phase 1 — Market Regime Engine (HMM)
### Phase 2 — Walk-forward replay validation
### Phase 3 — Pattern discovery (research contour)
### Phase 4 — Data expansion
### Phase 5 — Advanced risk management