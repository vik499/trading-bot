Definition of Success (DoS)

This document defines what success means for the trading-analytics platform at each stage of development.

It exists to:
	•	remove ambiguity about progress
	•	protect the project from premature trading and false positives
	•	provide a shared compass for humans and AI agents

PnL is explicitly NOT a primary success metric in early phases.

⸻

0. Purpose and Scope

This document answers a single question:

Are we building the system correctly, regardless of current profitability?

It applies to:
	•	all contributors (human and AI)
	•	all phases before and during trading
	•	all architectural and research decisions

If a proposal improves PnL but violates this document, it is rejected.

⸻

1. Success Is NOT PnL (Early Phases)

Until explicitly allowed by hard gates (Section 7):
	•	Positive PnL does not imply success
	•	Negative or zero PnL does not imply failure

Early-stage results are dominated by variance, not edge.

The system is evaluated by quality, stability, and explainability, not money.

⸻

2. Phase 0 — Data Success

The system is successful at the data layer if all of the following are true:

2.1 Readiness
	•	MarketData remains in READY state for extended periods
	•	No persistent NO_DATA / PRICE_STALE / GAPS / MISMATCH / LAG_TOO_HIGH
	•	Exchange lag warnings do not cause readiness degradation

2.2 Correctness by Market Meaning
	•	Aggregated price, volume, CVD, OI, funding, liquidations:
	•	correlate directionally with external references
	•	behave logically under market stress and calm
	•	Pixel-perfect equality is NOT required

2.3 Determinism
	•	Replaying the same canonical market tape produces identical aggregates
	•	Ordering, bucketing, and timestamps are fully deterministic

2.4 Storage Integrity
	•	Canonical tape is complete and replayable
	•	Raw vs aggregated separation is never violated

If any of these fail, all higher phases are blocked.

⸻

3. Phase 1 — Analytics & Regime Success

The system is successful at the analytics layer if:

3.1 Regime Detection
	•	The system produces a market regime classification
	•	Regime changes are:
	•	explainable
	•	not flapping
	•	correlated with observable market changes

3.2 Explainability
	•	Every regime output includes:
	•	drivers (what changed)
	•	confidence score
	•	known failure modes

3.3 Stability
	•	Identical input data → identical regime outputs
	•	Small data perturbations do not cause chaotic regime flips

A wrong regime is acceptable.
An unexplainable regime is not.

⸻

4. Phase 2 — Decision Quality Success

The system is successful at the decision layer if:

4.1 Proposals, Not Signals
	•	The system emits trade proposals, not direct orders
	•	Each proposal includes:
	•	rationale
	•	regime context
	•	confidence
	•	invalidation conditions

4.2 Decision Consistency
	•	Decisions change logically when regime or features change
	•	Identical context produces identical proposals

4.3 Outcome Independence
	•	A losing trade does not imply a bad decision
	•	A winning trade does not validate a decision

Decision quality is evaluated independently from outcome.

⸻

5. Phase 3 — Risk & Safety Success

The system is successful at the risk layer if:

5.1 Risk Containment
	•	Every proposal is gated by Risk Plane
	•	Position sizing is deterministic and bounded

5.2 Stress Behavior
	•	In stress regimes:
	•	risk is reduced or disabled
	•	system prefers no-trade over forced trade

5.3 Kill Switches
	•	Trading can be halted instantly
	•	Read-only monitoring continues safely

Any scenario where a single bug can cause catastrophic loss is unacceptable.

⸻

6. Phase 4 — Operational Success (24/7)

The system is operationally successful if:
	•	It runs continuously without supervision
	•	Restarts are safe and deterministic
	•	Logs, health snapshots, and status are understandable by an operator
	•	The operator can always answer:
	•	what the system is doing
	•	why it is doing it
	•	whether it is safe

⸻

7. Hard Gates: When Trading Is Allowed

Trading (even micro-risk) is allowed only if all are true:
	1.	Phase 0 (Data Success) passed and stable
	2.	Phase 1 (Regime) outputs are explainable
	3.	Phase 2 (Decisions) are consistent and reproducible
	4.	Phase 3 (Risk) blocks unsafe actions deterministically
	5.	Replay and walk-forward validation completed

If any gate regresses, trading is paused automatically.

⸻

8. Anti-Goals (What Is NOT Success)

The following are explicitly NOT considered success:
	•	Profitable backtests
	•	Single winning trades
	•	Temporary PnL spikes
	•	Parameter optimization without robustness
	•	Speed improvements that reduce determinism
	•	Any progress that breaks CANON.md

⸻

9. How This Document Is Used
	•	Humans use this as a progress checklist
	•	AI agents must:
	•	read this document before proposing changes
	•	justify how a change improves Definition of Success
	•	refuse actions that violate current phase constraints

If there is a conflict:

CANON.md > DEFINITION_OF_SUCCESS.md > DECISIONS.md > ROADMAP.md

⸻

This document is living, but conservative.
Changes must be deliberate, justified, and additive.