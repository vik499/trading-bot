# Trading Bot / Trading Platform — Canon Instructions (for Copilot GPT-5.1-Codex-Max)

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
- Every event payload includes `meta: EventMeta` (source + timestamp).
- Never cast payloads to `as any` to “fix” typing.
- If typing gets hard, improve the type layer instead.

### 0.3 Separation of responsibility
- Market Data plane: fetch/stream raw data and publish normalized events.
- Analytics plane: compute indicators/features, publish computed events.
- Strategy plane: decide intents (not orders), publish intents.
- Risk plane: validate/size/deny intents, publish approved/rejected decisions.
- Execution/Trading plane: translate approved intents into exchange actions, publish execution results.
- Storage/State plane: persist state, event snapshots, and recovery.
- Control plane: operator commands and bot lifecycle control.

### 0.4 Reliability (24/7)
- WebSocket client must handle:
  - reconnect (with backoff + jitter)
  - idempotent disconnect
  - heartbeat/watchdog based on **incoming activity** (not “pong only” assumptions)
  - clean shutdown without duplicated cleanup logs
- Logging must be operator-friendly: prompt usable, logs manageable, history available.

### 0.5 Pause semantics (professional)
- Pause is not a “fake mode”.
- `paused` is a state flag. `lifecycle` indicates RUNNING/PAUSED/STOPPING/etc.
- While paused: **no trading actions**, but monitoring continues (market data stays live).

---

## 1) High-Level Architecture (Planes)

### 1.1 Control Plane
Purpose: accept operator commands (CLI now, later Telegram/API/UI) and drive lifecycle.

**Modules**
- `CLI App`:
  - reads user commands
  - publishes `control:command`
  - subscribes to `control:state` to render status
  - does not call orchestrator directly
- `Orchestrator`:
  - central lifecycle coordinator
  - subscribes to `control:command`
  - publishes `control:state`
  - controls subscriptions and runtime components via event topics (not direct calls)

**Key invariants**
- Orchestrator is the only owner of lifecycle transitions.
- CLI is a client, not a coordinator.

---

### 1.2 Market Data Plane (Exchange/Data Layer)
Purpose: connect to exchange streams/APIs, normalize raw messages, publish market events.

**Modules**
- `BybitPublicWsClient`:
  - connect/disconnect (idempotent)
  - subscriptions (store + resubscribe)
  - heartbeat ping + watchdog by **incoming data**
  - parses Bybit messages
  - publishes `market:ticker` (and later trades, orderbook, kline, OI, etc.)
  - never touches strategy/risk/execution
- (future) `BybitRestClient`:
  - fetch initial snapshots (orderbook snapshot, symbol info, etc.)
  - publish `market:snapshot:*`

**Key invariants**
- WS client does not emit domain decisions, only normalized market data.
- Reconnect policy is inside WS client, not outside.

---

### 1.3 Analytics Plane
Purpose: convert market stream into indicators/features.

**Modules**
- `FeatureEngine`:
  - subscribes to market events
  - computes indicators: EMA, RSI, Stoch, ATR, CVD, OI deltas etc.
  - publishes `analytics:features`
- `MarketContextBuilder`:
  - maintains market regime context (volatility, trend, liquidity, session, etc.)
  - publishes `analytics:context`

**Key invariants**
- Analytics modules are pure-ish: no side effects besides publishing.
- Keep derived state internal + optionally snapshot via storage plane.

---

### 1.4 Strategy Plane
Purpose: interpret features/context into intents.

**Modules**
- `StrategyManager`:
  - loads active strategy configuration
  - subscribes to `analytics:*`
  - runs strategy state machine
  - publishes `strategy:intent` (BUY/SELL/LONG/SHORT/CLOSE/etc.)
- (future) multi-strategy routing:
  - multiple strategies can run simultaneously
  - intents must be tagged with strategyId

**Key invariants**
- Strategy produces **Intent**, not Order.
- Intent is declarative: “I want exposure X with constraints”, not “place order now”.

---

### 1.5 Risk Plane
Purpose: gatekeeper between strategy intent and execution.

**Modules**
- `RiskManager`:
  - subscribes to `strategy:intent`
  - checks:
    - paused state
    - exposure limits
    - max drawdown
    - max open positions
    - volatility constraints
    - cooldowns
    - slippage expectations
  - outputs:
    - `risk:approved_intent` (with position size, order constraints)
    - `risk:rejected_intent` (with reason)

**Key invariants**
- Execution may only act on approved intents.
- RiskManager is authoritative for sizing.

---

### 1.6 Trading / Execution Plane
Purpose: translate approved intents into exchange execution.

**Modules**
- `ExecutionEngine`:
  - subscribes to `risk:approved_intent`
  - decides execution tactics (market/limit, laddering, post-only, etc.)
  - uses exchange REST/private WS (future)
  - publishes:
    - `exec:order_submitted`
    - `exec:order_filled`
    - `exec:order_cancelled`
    - `exec:error`
- `PositionManager`:
  - subscribes to exec events
  - maintains current positions
  - publishes `portfolio:position_update`

**Key invariants**
- Execution is the only layer that calls exchange trading endpoints.
- Must be idempotent and state-aware (avoid duplicate orders on reconnect).

---

### 1.7 Storage / State Plane
Purpose: persistence, recovery, auditability.

**Modules**
- `StateStore` (initially in-memory, later durable):
  - stores:
    - last known control state
    - last market context/features snapshot
    - strategy state snapshots
    - open positions/orders
- (future) `EventJournal`:
  - append-only event log
  - enables replay/backtest consistency
- (future) `Postgres/Redis`:
  - Redis for fast state, Postgres for durable history

**Key invariants**
- Storage is passive: subscribes to events and persists.
- Recovery boot sequence is controlled by orchestrator.

---

### 1.8 Observability Plane
Purpose: logs, metrics, monitoring, alerts.

**Modules**
- `logger.ts`:
  - structured logger with:
    - timestamps with timezone
    - levels
    - controllable display (logs on/off)
    - tail history
    - CLI-friendly output
- (future) `Telemetry`:
  - metrics counters (reconnect count, tick rate, latency, errors)
- (future) notifications:
  - Telegram alerts for errors and signals
  - Pause mode disables trading but keeps monitoring + alerting

**Key invariants**
- Operator must be able to control noise without losing history.

---

## 2) EventBus Contract (Topics + Payload Rules)

### 2.1 Mandatory Event Meta
All payloads include:
- `meta.source` (who emitted): e.g. `market`, `cli`, `orchestrator`, `strategy`, `risk`, `exec`, `system`
- `meta.ts` number (Date.now)

### 2.2 Naming convention
- Plane prefix: `market:*`, `analytics:*`, `strategy:*`, `risk:*`, `exec:*`, `control:*`, `error:*`
- Keep topics stable. Breaking changes require migration.

### 2.3 Core control topics
- `control:command` → `ControlCommand`
- `control:state` → `ControlState`
- `control:error` → `BotErrorEvent` (if used)

### 2.4 Market topics (current)
- `market:ticker` → `TickerEvent` (normalized)

### 2.5 Future topics (planned)
- `market:trade`
- `market:orderbook_delta`
- `market:kline`
- `market:oi`
- `analytics:features`
- `analytics:context`
- `strategy:intent`
- `risk:approved_intent`
- `risk:rejected_intent`
- `exec:*` (order lifecycle)

---

## 3) Control Plane — Behavior & State Machine

### 3.1 ControlCommand
Commands include:
- `pause`
- `resume`
- `set_mode` (LIVE/PAPER/BACKTEST)
- `shutdown` (optional reason)
- `status` (request/response if needed, but current design pushes state continuously)

All commands contain `meta`.

### 3.2 ControlState
State includes:
- `mode: BotMode`
- `paused: boolean`
- `lifecycle: STARTING | RUNNING | PAUSED | STOPPING | STOPPED | ERROR`
- `startedAt`
- `lastCommandAt`
- `lastCommand`

### 3.3 Pause behavior
When paused:
- Market data continues
- Analytics continues
- Strategy may continue computing but must not produce actionable intents OR RiskManager must deny them
- Execution must never place orders while paused

Preferred: both Strategy and Risk honor pause defensively.

---

## 4) WebSocket Client Requirements (Bybit)

### 4.1 Problem we solved
Bybit v5 may not reliably produce `pong` the way a naive client expects.
We must not assume “no pong in 60s = dead”.
Instead:
- heartbeat ping is fine
- liveness watchdog should rely on **incoming traffic** (any message) with a reasonable timeout.

### 4.2 Heartbeat & Watchdog policy
- Send `{ op: 'ping' }` every 20s (silent send)
- Track:
  - `lastIncomingAt` (updated on any message, including system acks and market data)
- Watchdog:
  - if `now - lastIncomingAt > 120s`, consider connection stale and reconnect
- Keep backoff + jitter for reconnect.

### 4.3 Cleanup ownership
- Only `on('close')` performs cleanup (single-owned, guarded).
- `disconnect()` triggers close and awaits completion; it must not duplicate cleanup logs.
- connect-timeout path must not cause double cleanup when close fires after.

### 4.4 Subscriptions
- Store topics in Set
- On open: resubscribe all
- subscribe() is safe even when socket not open: store + log.

---

## 5) CLI / Operator UX Rules

### 5.1 Commands supported
- help
- status
- pause
- resume
- mode live|paper|backtest
- logs on|off
- logs tail <N>
- level debug|info|warn|error
- exit

### 5.2 Output rules
- Prompt must remain usable.
- Logs should not corrupt typed input.
- Prefer throttled ticker output (2–5s) instead of every tick.

---

## 6) Development Rules for Copilot (Very Important)

### 6.1 Do not break architecture
- Never call Market WS client directly from strategy/risk/execution.
- Never import orchestrator into CLI or exchange modules.
- Use EventBus topics.

### 6.2 Do not “solve” errors by weakening types
- No `any`, no unsafe casts for payloads.
- If typing is complex, introduce helper types (e.g. EventPayload<Topic>).

### 6.3 Additions must include:
- new topic typed in EventBus map
- payload type with `meta`
- publisher + subscriber updates
- minimal logging

### 6.4 Future readiness
Assume we will add:
- Telegram control + alerts
- Paper trading simulator
- Backtesting (replay from journal)
- AI/ML modules (strategy suggestions), but they still must publish intents only

---

## 7) Boot Sequence (Orchestrator-owned)

1) App starts
2) Orchestrator publishes initial `control:state` (STARTING)
3) WS client connect
4) Subscribe market topics
5) Orchestrator sets lifecycle RUNNING
6) CLI runs and listens
7) Shutdown:
   - publish shutdown command
   - orchestrator transitions STOPPING
   - ws disconnect
   - final STOPPED
   - process exit

---

## 8) Definition of Done (for any PR)

- TypeScript clean
- No duplicated cleanup logs
- Reconnect stable
- Commands work under heavy logging
- No direct cross-plane calls
- Every new event has meta + typed topic entry
- Logging readable, timestamps consistent with timezone

---

## 9) Current Known Baseline (what is already implemented)

- Typed EventBus with ControlCommand/ControlState/EventMeta/EventSource
- CLI publishes typed control commands and subscribes to control state
- Orchestrator owns lifecycle and pause semantics
- Logger supports operator-friendly CLI UX
- Bybit public WS client:
  - ping heartbeat
  - watchdog based on incoming activity (120s)
  - reconnect backoff+jitter
  - idempotent manual disconnect
  - cleanup single-owned by onClose

---

End of instructions.