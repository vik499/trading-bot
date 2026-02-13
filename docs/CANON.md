Trading Platform — Engineering Canon (Event-Driven, Deterministic, Multi-Exchange)

You are working in a TypeScript/Node.js event-driven trading platform with Planes + EventBus architecture.

This file is the project’s engineering constitution:
	•	It defines how the system must be built.
	•	It must not contain short-lived sprint tasks.
	•	Roadmap and “what’s next” live in ROADMAP.md.

⸻

0) Non-Negotiable Architecture Rules
	1.	Event-driven only — all cross-plane communication goes through EventBus (шина событий)
	2.	No cross-plane calls/imports — only src/core/** shared types/contracts/utilities
	3.	Strict typing — no any, no unsafe casts in planes, no weakening types to “fix” compilation
	4.	Deterministic time — business logic uses event.meta.ts (время события), not wall-clock time
	5.	Raw vs Aggregated separation — exchanges emit raw facts, platform emits derived aggregates
	6.	Multi-exchange truth — global state comes from multi-venue aggregation when available
	7.	Production vs Research safety — research never trades; production trades only through Strategy → Risk → Execution
	8.	Pause semantics — pause blocks trading actions, monitoring/analytics may continue
	9.	Fix-now — architectural/type invariant violations are fixed immediately and guarded by tests/asserts
	10.	Console discipline — console is high-level status; details go to logs/metrics/traces

⸻

1) Event System Contract

1.1 BotEventMap is the source of truth
	•	Every topic must exist in BotEventMap
	•	No “stringly-typed” topics in business code
	•	New topics require:
	•	payload type
	•	meta included
	•	tests/guards updated

1.2 Meta rules (meta как контракт)

Every event includes meta containing:
	•	source (идентичность эмиттера)
	•	ts (unix ms, derived from event source, not wall-clock)
	•	optional correlationId

Rule: handlers trust event.meta, not payload meta.

⸻

2) Raw vs Aggregated Contract (Hard Invariant)

2.1 Raw events (market:*_raw)

Raw events may contain only exchange facts:
	•	trades, orderbook, klines, OI, funding, liquidations, mark/index price
Raw events must NOT contain:
	•	qualityFlags, confidence, venueBreakdown, staleSourcesDropped
	•	derived metrics (CVD, composite index, global OI, etc.)

2.2 Aggregated events (market:*_agg)

Aggregated events may contain:
	•	multi-venue computed values
	•	derived metrics
	•	data quality fields:
	•	qualityFlags
	•	confidence
	•	venueBreakdown
	•	staleSourcesDropped
	•	mismatchDetected
These belong only to *_agg topics.

⸻

3) Planes and Responsibilities

3.1 Control Plane
	•	Owns lifecycle state transitions and operator commands
	•	Publishes control:command
	•	Renders control:state

Lifecycle states:
STARTING → RUNNING → PAUSED → STOPPING → STOPPED

Pause rule:
	•	while PAUSED: no trading actions, monitoring/analytics may continue

3.2 Market Data Plane (Per-Exchange Ingestion)

Purpose: ingest raw exchange data only.
Must:
	•	be idempotent on connect/disconnect
	•	reconnect with backoff + jitter (deterministic where feasible)
	•	implement heartbeat/watchdog based on incoming activity
Must not:
	•	compute global metrics
	•	embed derived “truth” beyond normalization into canonical raw form

3.3 Global Data Plane (Multi-Exchange Aggregation)

Purpose: produce global metrics and quality signals.
Must include:
	•	canonical symbol mapping
	•	unit normalization
	•	quality monitor (stale/mismatch/degrade/recover)
	•	aggregators that emit market:*_agg

3.4 Analytics Plane

Purpose: build interpretable market context.
Must be side-effect free except event emission.
Emits:
	•	analytics:market_view
	•	analytics:regime
	•	analytics:regime_explain

3.5 Strategy Plane (Production)
	•	Produces intents, not orders
	•	Emits strategy:intent

3.6 Risk Plane
	•	Gatekeeper: validates/sizes/denies intents
	•	Emits risk:approved_intent / risk:rejected_intent

3.7 Execution Plane
	•	Only plane allowed to call private exchange endpoints
	•	Must be idempotent, state-aware, replay-safe

3.8 Storage/State Plane
	•	Passive subscribers persisting events/journal/snapshots
	•	Supports replay and walk-forward validation

3.9 Observability Plane
	•	Logs/metrics/alerts, operator-friendly and throttled
	•	Must preserve correlationId
	•	Must not leak secrets

⸻

4) Determinism (Reproducibility as a Feature)

4.1 Time source

Business logic uses:
	•	event.meta.ts (event time)
Never uses:
	•	Date.now() or wall-clock time for decisions
	•	runtime timing artifacts for aggregation outcomes

4.2 Deterministic aggregation
	•	Aggregation pipelines must be deterministic given the same input events
	•	Ordering must be explicit when needed (sorting by ts/source)
	•	Bucket boundaries must be defined by event time, not wall-clock

4.3 Replay contract
	•	Replaying the same journal must produce identical aggregated outputs (byte-for-byte where feasible)
	•	Research and validation depend on this

⸻

5) Data Modeling Laws

5.1 Canonical instruments

All exchange-specific symbols must map to canonical form:
	•	asset (e.g. BTC)
	•	quote (e.g. USDT)
	•	venue (bybit/binance/okx)
	•	instrumentType (spot/perp/futures)

5.2 Units must be explicit
	•	No hidden conversions
	•	Aggregators must never mix units silently
	•	If conversion requires a price reference, it must be explicitly defined and validated for freshness

5.3 Multi-exchange quality signals

Aggregators must track and expose:
	•	sources used
	•	freshness/lag
	•	stale sources dropped
	•	mismatch detection
	•	confidence score (0–1) derived from quality signals

⸻

6) Engineering Standards

6.1 No cross-plane imports
	•	Only src/core/** is shared
	•	Planes communicate only via events

6.2 Error handling and resilience

All external calls must:
	•	backoff + jitter
	•	bounded retries
	•	deterministic throttling where feasible
	•	structured diagnostics (status, codes, requestId, params)
No log spam.

6.3 Logging rules
	•	High-signal, operator-friendly
	•	Aggregate repetitive noise
	•	CorrelationId preserved end-to-end
	•	No secrets

6.4 Definition of Done

A change is done only if:
	•	TypeScript is clean
	•	Tests are green
	•	No cross-plane calls added
	•	New topics added to BotEventMap with strict payload types and meta
	•	Determinism preserved (no Date.now leakage)
	•	Observability updated if behavior changed

⸻

7) Guardrails and Tests (Required)
	•	Determinism guards: fail if Date.now() appears in planes
	•	Contract tests: topic strings and payload shapes validated via BotEventMap
	•	Pipeline tests: collector → raw → aggregator → agg, verifying deterministic behavior and correlationId propagation
	•	Quality monitor tests: stale/mismatch/degrade/recover deterministic emission

⸻

8) Guided Engineering Autonomy (Freedom with Proof)

Codex/Copilot may propose and apply better solutions (refactors, structure changes, performance improvements), but before any significant change it must:
	1.	Justify: what’s wrong now, why the proposal is better, trade-offs
	2.	Verify invariants: event-driven, determinism, raw/agg separation, multi-exchange truth, contour safety
	3.	Provide incremental migration steps (no big bang rewrites)
	4.	Ensure Definition of Done still passes
	5.	Provide rollback plan for risky changes

⸻

9) Canon vs Roadmap

This file is permanent architecture law.
It must NOT include:
	•	next tasks
	•	sprint checklists
	•	phase plans
Those live in ROADMAP.md.
If roadmap conflicts with Canon, Canon wins.

10) Trading & Decision Canon (Non-Negotiable)

10.1 Market ≠ Chart
	•	Рынок — это процесс потоков и ликвидности, а не визуальный паттерн.
	•	Свечи — производная, не первоисточник.

10.2 Decisions are probabilistic
	•	Любое решение — ставка с распределением исходов.
	•	Одиночный результат не валидирует и не опровергает систему.

10.3 Regime-first law
	•	Любая стратегия валидна только внутри режима рынка.
	•	Универсальных стратегий не существует.

10.4 Edge must survive costs
	•	Любое преимущество считается после комиссий, спреда и проскальзывания.
	•	Если edge исчезает после costs → его нет.

10.5 Risk defines the business
	•	Risk Plane определяет, существует ли система как бизнес.
	•	Стратегия не имеет права “пробивать” риск-контур.

10.6 Good decision ≠ profitable outcome
	•	Хорошее решение может закончиться убытком.
	•	Плохое решение может закончиться прибылью.
	•	Оценка идёт по decision quality, не по PnL.

10.7 Capital is a load test
	•	Деньги — не источник интеллекта.
	•	Деньги — нагрузка на систему.

“Progress evaluation is defined in DEFINITION_OF_SUCCESS.md.”
