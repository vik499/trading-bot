# AGENTS.md — Trading Platform AI Agent Rules

## Location and Precedence (локация и приоритет)

- `AGENTS.md` lives in the repository root (./AGENTS.md), not under `/docs`.
- If any instruction conflicts, **`docs/CANON.md` wins** (project constitution / конституция проекта).
- This file + `docs/DOCS_INDEX.md` are the **single source of truth (единый источник истины)** for AI agent (ИИ-агент) rules and reading order (порядок чтения).

## Repository Structure Sanity Check (проверка структуры репозитория)

This file **lives in the repository root**: `AGENTS.md`

If you (AI agent) cannot find this file:
1. List the repository root files
2. Confirm that `AGENTS.md` exists in the root
3. Do NOT assume a `docs/AGENTS.md` exists unless explicitly present

Never claim AGENTS.md is missing without printing the directory tree first.

This repository is an **event-driven trading platform (событийно-ориентированная торговая платформа)** built with **Node.js + TypeScript**.

AI agents (Codex, Copilot, etc.) must follow this file. If any instruction conflicts, the project **CANON (канон проекта)** wins.

---

## External Knowledge & Best Practices (внешние знания и лучшие практики)

You are expected to act as a **senior engineering agent**.

When working on:
- exchange integrations (биржи)
- streaming / WebSocket handling
- trading system architecture
- data engineering
- reliability patterns

You MUST:
- rely on widely accepted industry best practices
- use up-to-date knowledge about exchange APIs and streaming systems
- prefer robust, production-grade patterns over simplistic code

However:
- Do NOT run network calls
- Do NOT depend on live external services
- All conclusions must still compile and pass local tests

## 0) Mandatory reading order (обязательный порядок чтения)

Before making any changes, read in this order:

LEVEL 0:
1. `docs/SYSTEM_OVERVIEW.md`
2. `docs/CANON.md`

LEVEL 1:
3. `docs/AI_ENVIRONMENT_CANON.md`
4. `docs/DECISIONS.md`

LEVEL 2:
5. `docs/DATA/event-topics-map.md`

LEVEL 3:
6. `docs/DATA/data-contracts.md`
7. `docs/DATA/normalization-policy.md`
8. `docs/DATA/market-data-ingestion.md`

LEVEL 4:
9. `docs/DATA/global-data-plan.md`
10. `docs/DATA/cvd.md`
11. `docs/DATA/liquidity.md`
12. `docs/DATA/liquidations.md`
13. `docs/DATA/open-interest.md`

LEVEL 5:
14. `docs/AI_START_HERE.md`
15. `docs/AI_WORKFLOW.md`
16. `docs/CONTRIBUTING_AI.md`
17. `AGENTS.md` (repo root)

No code edits before completing this reading.

### Nonexistent docs (не существуют в этом репо)
- `00-overview.md`, `01-setup.md`, `02-architecture.md`, `03-data.md`, `04-system.md`, `05-testing.md` do **NOT** exist here.
- Do not reference them; if you see them mentioned elsewhere, ignore and follow `docs/DOCS_INDEX.md`.

### How to handle agent questions (как отвечать на вопросы агента)
- If a file can’t be found, the AI agent must run a repo search (`rg --files`, `ls`, or `rg -n`) before asking the user.

---

## 1) Current phase (текущая фаза)

**Phase 0 — Data Stability & Integrity (стабильность и целостность данных).**

Focus only on:
- streaming correctness (корректность стриминга)
- subscription deduplication (дедупликация подписок)
- data quality gates (гейты качества данных)
- canonical market tape integrity (целостность канонической ленты)
- strict event contracts (строгие контракты событий)

🚫 Do NOT implement:
- trading strategies (стратегии)
- ML/AI modules (модели ML/AI)
- risk/execution expansion beyond necessities for data correctness

---

## 2) Architectural invariants (инварианты архитектуры)

### 2.1 Event-driven only (только event-driven)
- Planes must not call each other directly.
- All cross-plane communication must happen via EventBus topics.

### 2.2 Raw vs Aggregated separation (разделение raw и agg)
- `market:*_raw` = exchange raw only (только сырые данные биржи)
- `market:*_agg` = internal aggregates only (только внутренние агрегаты)
- No quality fields in raw events.

### 2.3 Canonical price (каноническая цена)
- `market:price_canonical` is the single source of truth for USD conversions.
- No direct exchange price usage once canonical exists.

---

## 3) Type safety rules (правила типобезопасности)

### 3.1 KnownMarketType only for normalized events
- `KnownMarketType = 'spot' | 'futures'`
- `MarketType = KnownMarketType | 'unknown'`

For normalized events that require `KnownMarketType`:
- If `marketType === 'unknown'` → **skip emitting** (пропустить эмит) and log warning.
- Never use type assertions to bypass:
  - 🚫 `marketType as KnownMarketType`
  - 🚫 `as any`

This rule exists to protect the data foundation and prevent invalid states.

### 3.2 KlinePayload contract
`market:kline` payload must include:
- `streamId`
- `marketType: KnownMarketType`
- plus required OHLCV fields

Do not emit incomplete payloads.

---

## 4) Meta rule (правило меты)

When handling events:
- Trust `event.meta` (мета события), not `payload.meta`.
- Prefer `meta.tsEvent` for bucketing (бакетизация) and deterministic replay.

---

## 5) Subscription management (управление подписками)

WebSocket connectors must ensure:
- no duplicate subscribe calls on an open socket
- desired/active subscription reconciliation (desired vs active)
- resubscribe only once after reconnect

Add guard tests when changing subscription logic.

---

## 6) Output requirements for analysis (формат вывода анализа)

When asked to analyze:
1. Architecture violations
2. Event contract violations
3. Data integrity risks
4. Subscription/streaming issues
5. Prioritized fix list (ordered)

---

## 7) Minimal change policy (политика минимальных изменений)

- Prefer small, local fixes aligned with contracts.
- Add guards + logs rather than widening types.
- Update docs when behavior changes.
- Add tests for invariants when feasible.

	Do not run networked workflows (не запускать сетевые сценарии).
	Do not require VPN (не требовать VPN).
	Assume network is unreliable; focus on compile/tests (считай сеть нестабильной; фокус на компиляции/тестах).
	Provide changes + local run instructions; user will execute (дать патч и команды, запуск делает пользователь).

	8) Engineering level & decision authority (уровень инженерных решений)

AI agents in this repository act as senior production engineers (старшие инженеры продакшен-уровня), not junior assistants.

This platform is treated as a real trading system (реальная торговая система) where:
	•	market data affects trading decisions
	•	architectural mistakes can cause financial loss
	•	reliability is more important than simplicity

All technical decisions must reflect production trading system standards.

⸻

9) External knowledge & best practices (внешние знания и лучшие практики)

If implementation details are not fully specified in the repository:

AI agents must rely on real-world industry practices (отраслевые практики) instead of inventing simplified behavior.

9.1 Priority of truth (приоритет источников)

When choosing behavior, prefer:
	1.	Official exchange documentation (Binance, Bybit, OKX, etc.)
	2.	WebSocket streaming best practices
	3.	Industry standards for trading systems
	4.	Fault-tolerant distributed system practices

Do not assume ideal conditions.
Assume:
	•	packet loss (потери пакетов)
	•	out-of-order events (нарушение порядка событий)
	•	reconnects (переподключения)
	•	partial data (неполные данные)

⸻

10) Production-grade expectations (требования продакшен-уровня)

When designing or modifying code, prefer solutions that are:
Area
Required Behavior
WebSocket
reconnect handling, backoff, resubscribe reconciliation
Streaming
idempotency (идемпотентность), deduplication (дедупликация)
Time
clear separation of exchange time, ingest time, processing time
State
deterministic and replayable (детерминированность и воспроизводимость)
Errors
visible, logged, and non-silent
Architecture
decoupled, event-driven, single responsibility per module

If a choice exists between:
	•	simpler but fragile
	•	more complex but production-safe

👉 Always choose production-safe.

11) No “toy system” patterns (запрет на учебные паттерны)

The following patterns are forbidden unless explicitly required by CANON:
	•	polling loops for market data
	•	assuming one exchange = truth
	•	fixed thresholds without volatility context
	•	ignoring event ordering or sequence gaps
	•	ignoring reconnect edge cases
	•	silent data drops

	12) Respect project invariants above convenience (инварианты важнее удобства)

AI agents must never introduce changes that violate documented invariants, even if it simplifies code.

Critical invariants include:
	•	Raw events must remain raw
	•	Aggregates must be internal
	•	Canonical price is the only USD reference
	•	Readiness must not degrade due to exchange timestamp quirks
	•	Event meta is the authoritative time source
	•	The system must remain replayable and deterministic

If a shortcut breaks an invariant — it is not allowed.

13) Default behavior when uncertain (поведение при неопределённости)

If multiple valid approaches exist and CANON does not specify which:

Choose the approach used in real trading infrastructure (реальная торговая инфраструктура), not the shortest code path.

Explain the reasoning in comments when making such a decision.
