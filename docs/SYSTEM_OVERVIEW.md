# Trading Platform — System Overview

This project is a modular event-driven trading platform built in Node.js + TypeScript.

It is NOT a simple trading bot.  
It is a long-term system designed for:

- Market state understanding
- Deterministic research and replay
- Safe transition from research to production trading

## Core Architecture

The system uses **Planes + EventBus**:

Control → Market Data → Global Data → Analytics → Strategy → Risk → Execution

All communication is event-driven. No cross-plane calls.

⚠️ Architecture rules are defined in `docs/CANON.md`.

## Two Operational Contours

| Contour | Purpose |
|--------|---------|
| Research | Data analysis, modeling, validation |
| Production | Controlled trading via Strategy → Risk → Execution |

Research never places trades directly.

## Data Philosophy

We prioritize:
- Multi-exchange aggregation
- Data normalization before analytics
- Deterministic replayability
- Explicit units and measurement contracts

## Documentation Map (Read in This Order)

1. `docs/CANON.md`
2. `docs/DATA/data-contracts.md`
3. `docs/DATA/normalization-policy.md`
4. `docs/DATA/event-topics-map.md`
5. `docs/ROADMAP.md`
6. `docs/AI_WORKFLOW.md`
7. `docs/CONTRIBUTING_AI.md` — How AI must record knowledge and changes