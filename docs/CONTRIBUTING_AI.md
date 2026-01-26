# AI Contribution Protocol

AI assistants (Codex, Copilot, GPT) are not only allowed to write code, but are responsible for maintaining project knowledge.

This repository is a learning engineering system.  
Documentation is part of the system state.

---

## 1) When AI MUST update documentation

AI must update documentation when:

- Architectural behavior changes
- Data contracts change
- A bug reveals a new invariant
- An approach fails and a new one is tried
- A new subsystem or metric is introduced

If code changes system behavior, documentation must reflect it.

---

## 2) Where AI must write information

| Situation | File to update |
|----------|----------------|
| Architecture rules clarified | `docs/CANON.md` |
| Long-term reasoning / design decisions | `docs/DECISIONS.md` |
| Failed experiment or rejected approach | `docs/DECISIONS.md` under "Attempted Solution (Failed)" |
| Change in measurement units or fields | `docs/DATA/data-contracts.md` |
| Change in normalization or bucket logic | `docs/DATA/normalization-policy.md` |
| Event topic changes | `docs/DATA/event-topics-map.md` |
| Change in ingestion/resync logic | `docs/DATA/market-data-ingestion.md` |
| Change in aggregation logic | Relevant file under `docs/DATA/` |
| Change in current engineering focus | `docs/ROADMAP.md` |
| Major implemented feature | Add summary entry to `docs/PR_HISTORY/` |

---

## 3) AI must record failed attempts

If a solution is attempted and later reverted or found ineffective:

AI must append to `docs/DECISIONS.md`:

### Attempted Solution (Failed)

Problem:
Approach tried:
Why it failed:
What was learned:

This prevents repeated ineffective solutions.

---

## 4) AI must treat tests as system memory

When fixing a bug:
- Add or update a test that reproduces the issue
- Ensure replay determinism still holds

Tests are historical knowledge of the system.

---

## 5) If AI changes system behavior but not docs — change is incomplete

A change is not considered finished unless relevant documentation is updated.

---

AI is part of the engineering process, not just a code generator.
Documentation is mandatory system state, not optional notes.