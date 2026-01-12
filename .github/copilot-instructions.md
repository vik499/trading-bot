# Trading Bot / Trading Platform — Canon Instructions (vNext)

> This repo is an event-driven trading system (bot now, platform later).  
> Architecture is **Planes + EventBus**. No direct cross-module calls.  
> Everything communicates via typed events. We optimize for correctness, extensibility, and 24/7 reliability.

---

## 0) Core Principles (Non-negotiable)

### 0.1 Event-driven only
- **All communication happens through `EventBus`**.
- Modules do **NOT** call each other directly (no imports between planes except shared core types/contracts).
- Modules publish events, subscribe to events, and act on them.

### 0.2 Typed Events (no “any payload”)
- Every topic has a strict payload type.
- Every event payload includes `meta: EventMeta`:
  - `source` (who emitted)
  - `ts` (timestamp, unix ms)
  - optional `correlationId`
- Do not weaken types to “make it compile”.
- If typing gets hard: improve types/contracts, add helper types, or refactor event maps.

> Note: EventBus implementation may contain an internal cast due to emitter library tuple typing edge-cases.  
> **Do not spread `as any` into business modules.**

### 0.3 Separation of responsibility (Planes)
- **Control Plane**: operator commands + bot lifecycle control.
- **Market Data Plane**: stream/fetch raw exchange data → publish normalized market events.
- **Analytics Plane**: compute indicators/features/context → publish computed events.
- **Strategy Plane (Production)**: decide **intents**, not orders → publish intents.
- **Risk Plane**: validate/size/deny intents → publish approved/rejected.
- **Execution/Trading Plane**: translate approved intents into exchange actions → publish execution results.
- **Storage/State Plane**: persist state, snapshots, recovery, journal.
- **Observability Plane**: logs, metrics, alerts.
- **Research/ML Plane (Discovery)**: drift detection, labeling/outcomes, discovery, candidates, backtests, safe promotion.

### 0.4 Reliability (24/7)
- WebSocket client must handle:
  - reconnect (with backoff + jitter)
  - idempotent disconnect
  - heartbeat/watchdog based on **incoming activity**
  - clean shutdown without duplicated cleanup logs
- Logging must be operator-friendly: prompt usable, logs manageable, history available.

### 0.5 Pause semantics (professional)
- Pause is a state flag: `paused: boolean`.
- `lifecycle` indicates RUNNING/PAUSED/STOPPING/etc.
- While paused:
  - **NO trading actions**
  - monitoring continues (market + analytics + research can stay live)

### 0.6 Two-contour safety model (Discovery vs Production)
We run two loops:
- **Production (battle)**: trades only approved strategies/models.
- **Discovery (research)**: explores, tests, proposes.
- Discovery **never trades real money directly**.

---

## 1) High-Level Architecture (Planes)

### 1.1 Control Plane
Purpose: accept operator commands (CLI now, later Telegram/API/UI) and drive lifecycle.

**Modules**
- `CLI App`
  - reads user commands
  - publishes `control:command`
  - subscribes to `control:state` to render status
  - never calls orchestrator directly
- `Orchestrator`
  - central lifecycle coordinator
  - subscribes to `control:command`
  - publishes `control:state`
  - initiates Market plane via `market:*` events (connect/subscribe)
  - owns startup/shutdown transitions

**Invariants**
- Orchestrator is the only owner of lifecycle transitions.
- CLI is a client, not a coordinator.

---

### 1.2 Market Data Plane (Exchange/Data Layer)
Purpose: connect to exchange streams/APIs, normalize raw messages, publish market events.

**Modules**
- `MarketGateway` (adapter / lifecycle controller for market plane)
  - subscribes to:
    - `market:connect`
    - `market:disconnect`
    - `market:subscribe`
  - orchestrates exchange client(s) and emits:
    - `market:connected`
    - `market:disconnected`
    - `market:error`
- `BybitPublicWsClient` (exchange WS client)
  - connect/disconnect (idempotent)
  - subscriptions (store + resubscribe)
  - heartbeat ping + watchdog by **incoming activity**
  - parses Bybit messages
  - publishes normalized market events:
    - `market:ticker` (now)
    - later: `market:trade`, `market:orderbook_delta`, `market:kline`, `market:oi`, `market:funding`

**Invariants**
- Exchange modules emit **only normalized market data**, no decisions.
- Reconnect policy lives inside WS client, not outside.
- Orchestrator does not call WS client directly: only emits `market:*` requests.

---

### 1.3 Analytics Plane (planned / future)
Purpose: convert market stream into indicators/features and market context.

**Modules**
- `FeatureEngine` (planned)
  - subscribes to `market:*`
  - computes indicators/features
  - publishes `analytics:features`
- `MarketContextBuilder` (planned)
  - builds regime context
  - publishes `analytics:context`

**Invariants**
- Analytics does no trading and has no side effects besides publishing events.
- Derived state may be snapshotted via Storage plane.

---

### 1.4 Strategy Plane (Production) (planned / future)
Purpose: interpret features/context into **intents**.

**Modules**
- `StrategyManager` (planned)
  - subscribes to `analytics:*`
  - runs strategy state machines
  - publishes `strategy:intent`

**Invariants**
- Strategy produces **Intent**, not Order.
- Intent is declarative: “I want exposure X with constraints”, not “place order now”.

---

### 1.5 Risk Plane (planned / future)
Purpose: gatekeeper between strategy intent and execution.

**Modules**
- `RiskManager` (planned)
  - subscribes to `strategy:intent`
  - checks paused/lifecycle, exposure limits, dd, volatility, cooldowns, etc.
  - outputs `risk:approved_intent` / `risk:rejected_intent`

**Invariants**
- Execution may only act on approved intents.
- RiskManager is authoritative for sizing and blocking.

---

### 1.6 Trading / Execution Plane (planned / future)
Purpose: translate approved intents into exchange execution.

**Modules**
- `ExecutionEngine` (planned)
  - subscribes to `risk:approved_intent`
  - uses exchange REST/private WS (future)
  - publishes `exec:order_event` and/or `error:event`
- `PositionManager` (planned)
  - subscribes to exec events
  - publishes `portfolio:position_update`

**Invariants**
- Execution is the only layer that calls exchange trading endpoints.
- Must be idempotent and state-aware.

---

### 1.7 Storage / State Plane (planned, partially present via Research)
Purpose: persistence, recovery, auditability, replay.

**Modules**
- `StateStore` (planned)
- `EventJournal` (planned)
- `FeatureStore` (implemented in Research plane)
  - stores feature vectors (timestamped + versioned)
  - feeds DriftDetector / Discovery / Backtests

**Invariants**
- Storage is passive: subscribes to events and persists.
- Recovery boot sequence is orchestrator-owned.

---

### 1.8 Observability Plane
Purpose: logs, metrics, monitoring, alerts.

**Modules**
- `logger.ts`
  - structured logging, CLI-friendly output
  - tail history
  - log level control
  - logs on/off

---

## 2) Research / ML Plane (Discovery Loop)

> Discovery modules produce research outputs and proposals.  
> They **never** place real-money orders directly.

**Implemented modules (current repo)**
- `DriftDetector`
- `FeatureStore`
- `OutcomeEngine`
- `ResearchOrchestrator`
- `ResearchRunner`

### 2.1 Drift Detector
- compares recent window vs baseline
- emits `analytics:marketDriftDetected`

### 2.2 Feature capture
- emits `research:featureVectorRecorded` (dataset / feature store writes)

### 2.3 Outcomes / labeling
- emits `research:outcomeRecorded`

### 2.4 Discovery / candidates / backtests (pipeline events)
- emits:
  - `research:newClusterFound`
  - `research:candidateScenarioBuilt`
  - `research:backtestCompleted`
  - `research:scenarioApproved`
  - `research:scenarioRejected`

### 2.5 Production hooks + kill switch events
- `strategy:scenarioEnabled`
- `strategy:scenarioDisabled`
- `control:rollbackModel` (payload is `RollbackRequested`)

---

## 3) EventBus Contract (Topics + Payload Rules)

### 3.1 Mandatory meta
All event payloads include:
- `meta.source` (emitter): `cli | telegram | system | market | analytics | strategy | risk | trading | storage | research`
- `meta.ts` unix ms
- optional `meta.correlationId`

### 3.2 Naming convention
- Plane prefix: `market:*`, `analytics:*`, `strategy:*`, `risk:*`, `exec:*`, `portfolio:*`, `control:*`, `state:*`, `error:*`, `research:*`
- Topics should remain stable. Breaking changes require migration.

---

## 4) BotEventMap — Current (Implemented) vs Planned

### 4.1 Implemented (current repo)
- `control:command` → `ControlCommand`
- `control:state` → `ControlState`
- `market:ticker` → `TickerEvent`

**Market lifecycle (implemented)**
- `market:connect` → `MarketConnectRequest`
- `market:disconnect` → `MarketDisconnectRequest`
- `market:subscribe` → `MarketSubscribeRequest`
- `market:connected` → `MarketConnected`
- `market:disconnected` → `MarketDisconnected`
- `market:error` → `MarketErrorEvent`

**Research / Discovery (implemented)**
- `analytics:marketDriftDetected` → `MarketDriftDetected`
- `research:featureVectorRecorded` → `FeatureVectorRecorded`
- `research:outcomeRecorded` → `OutcomeRecorded`
- `research:newClusterFound` → `NewClusterFound`
- `research:candidateScenarioBuilt` → `CandidateScenarioBuilt`
- `research:backtestCompleted` → `BacktestCompleted`
- `research:scenarioApproved` → `ScenarioApproved`
- `research:scenarioRejected` → `ScenarioRejected`
- `strategy:scenarioEnabled` → `ScenarioEnabled`
- `strategy:scenarioDisabled` → `ScenarioDisabled`
- `control:rollbackModel` → `RollbackRequested`

### 4.2 Planned (future)
- `analytics:features`
- `analytics:context`
- `strategy:intent`
- `risk:approved_intent`
- `risk:rejected_intent`
- `exec:order_event`
- `portfolio:position_update`
- `state:snapshot`
- `state:recovery`
- `error:event`

> When adding topics:
> - add topic entry in `BotEventMap`
> - add payload interface/type with `meta`
> - implement publisher and at least one subscriber
> - add minimal logs/tests

---

## 5) WebSocket Client Requirements (Bybit)

### 5.1 Heartbeat & Watchdog policy
- send ping on interval
- track `lastIncomingAt` updated on any incoming message (system ack, data, etc.)
- watchdog triggers reconnect if `now - lastIncomingAt > timeout`
- keep backoff + jitter

### 5.2 Cleanup ownership
- only `on('close')` performs cleanup (single-owned, guarded)
- `disconnect()` triggers close and awaits completion; no duplicated cleanup logs

### 5.3 Subscriptions
- store topics in Set
- on open: resubscribe all
- subscribe() safe even if socket not open: store + log

---

## 6) CLI / Operator UX Rules
- commands publish `control:command` only
- prompt must remain usable
- throttle noisy outputs (don’t print every tick)

---

## 7) Development Rules for Copilot / Contributors

### 7.1 Do not break architecture
- never call Market WS client directly from strategy/risk/execution
- never import orchestrator into CLI or exchange modules
- orchestrator initiates market plane via `market:*` events
- use EventBus topics

### 7.2 Do not “solve” errors by weakening types
- no `any`, no unsafe casts for payloads
- if typing is complex, introduce helper types (e.g. `EventPayload<Topic>`)

### 7.3 Definition of Done for changes
- TypeScript clean
- tests green
- no duplicated cleanup logs
- no direct cross-plane calls
- every new event has meta + typed topic entry
- logs readable, timestamps consistent

---

## 8) Boot Sequence (Orchestrator-owned, event-driven)

1) App starts
2) Orchestrator publishes initial `control:state` (STARTING)
3) Orchestrator emits `market:connect`
4) Orchestrator emits `market:subscribe` (topics)
5) `MarketGateway` handles connect/subscribe and reports:
   - `market:connected` / `market:error`
6) Orchestrator reaches readiness and sets lifecycle RUNNING
7) CLI runs and listens

Shutdown:
- publish shutdown command
- orchestrator transitions STOPPING
- orchestrator emits `market:disconnect`
- gateway/ws disconnect
- final STOPPED
- process exit

---

END.