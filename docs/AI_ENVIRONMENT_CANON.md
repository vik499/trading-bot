# AI Environment Canon — Network, Exchange Access, and Reproducible Testing

This repository is developed with assistance from AI tools (Codex/Copilot/ChatGPT).
Network conditions (VPN, geo restrictions, ISP filtering) can create false failures or false fixes.
This canon defines environment rules that preserve correctness, determinism, and reproducibility.

---

## 0) Scope

This document is permanent engineering law for AI-assisted work.
It must NOT contain sprint tasks.
Roadmap and next steps live in `docs/ROADMAP.md`.

AI must follow:
- `docs/CONTRIBUTING_AI.md`
- `docs/CANON.md`
- `docs/AI_WORKFLOW.md`

If any document conflicts with `CANON.md`, `CANON.md` wins.

---

## 1) Reality: Exchange Access Is Not Stable

Public and private endpoints of exchanges may be blocked or partially blocked depending on:
- VPN routing
- region-based restrictions
- ISP filtering
- rate limits and WAF (Web Application Firewall)

Therefore:
- A failure observed under VPN may be unrelated to code correctness.
- A “fix” made under VPN can be incorrect in normal operation.

---

## 2) Environments (Explicit)

All development and testing must declare which environment is used:

### 2.1 Clean Net (No VPN)
Definition:
- No VPN or geo-masking
- Direct network access

Purpose:
- Validate real exchange integrations and live data pipelines.

This is the only environment where “exchange connectivity success” is considered meaningful.

### 2.2 Restricted Net (VPN / Geo-Restricted)
Definition:
- VPN enabled or known geo restrictions

Purpose:
- Code review, refactoring, unit tests, deterministic replay tests.
Not valid for:
- concluding real exchange connectivity status
- tuning production thresholds based on observed failures

### 2.3 Offline / Deterministic Mode (Mock/Replay)
Definition:
- No real exchange access
- Uses recorded events, mocks, or replay journals

Purpose:
- deterministic correctness, reproducibility, research safety
- CI-friendly validation

---

## 3) Non-Negotiable Rule: Connectivity Is Not a Correctness Signal

AI must never “fix” exchange connectivity by weakening:
- determinism rules
- data contracts
- quality gates
- multi-exchange truth requirements

If connectivity fails under VPN:
- treat it as an environment artifact first
- verify with deterministic tests/mocks
- validate final behavior in Clean Net

---

## 4) Testing Requirements Under Environment Uncertainty

### 4.1 Deterministic Tests Are Primary Evidence
AI must prioritize:
- unit tests
- contract tests
- replay determinism tests

These must pass regardless of VPN.

### 4.2 Live Tests Are Secondary Evidence
Live tests against real exchanges:
- must be run in Clean Net for validity
- can be flaky and must not drive architecture changes alone

---

## 5) AI Workflow Rule (Practical)

Before making non-trivial changes, AI must:
1) Read docs per the mandatory LEVEL 0–5 order.
2) Propose a hypothesis.
3) Implement incrementally.
4) Prove with deterministic tests.
5) Only then suggest live validation in Clean Net.

AI must not attribute VPN failures to code without evidence.

---

## 6) Documentation Updates (When Environment Rules Change)

If a new environment constraint is discovered (new geo block, new endpoint restriction):
- Record it in `docs/DECISIONS.md` with evidence
- Update this file if the rule is structural and long-lived
- Do not add transient “today’s outage” notes here

---

## 7) Canon Summary

- VPN can produce false failures and false fixes.
- Deterministic tests are the primary source of truth.
- Live exchange validation is meaningful only in Clean Net.
- Never weaken CANON constraints to “make it work under VPN”.