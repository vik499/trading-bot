AI must follow the documentation update rules defined in `docs/CONTRIBUTING_AI.md`.

AI must follow architectural laws in `docs/CANON.md`.
Violating them breaks system determinism and research reproducibility.

# AI Engineering Workflow — Learning, Testing and Iteration Rules

This project uses AI (Codex/Copilot) not only as a coder, but as an iterative engineering assistant.

AI must behave like an R&D engineer, not a one-shot code generator.

---

## 1) Core Principle

Every change is an experiment.  
Every experiment must produce evidence.  
Evidence must be used in future decisions.

AI must learn from previous attempts inside this repository.

---

## 1.1) Operational Guardrails

- Codex must not run `npm run dev`, live bot modes, or network-connected validation workflows.
- This applies to Codex-run validation only; runtime networking is user-executed.

---

## 2) Mandatory Testing Loop

For any non-trivial change, AI must follow this cycle:

1. **Understand the system context**
   - Read SYSTEM_OVERVIEW.md
   - Check CANON.md for architectural constraints
   - Check DECISIONS.md for prior reasoning
   - Check ROADMAP.md for current priorities

2. **Propose a hypothesis**
   - What problem is being solved?
   - Why this approach might work?
   - What invariants are affected?

3. **Implement incrementally**
   - Small changes only
   - No architecture-breaking shortcuts

4. **Run verification**
   - TypeScript must pass
   - Tests must pass
   - Logs must not become noisy
   - Determinism must not be broken

5. **Record outcome**
   - Did it fix the issue?
   - Did it introduce regressions?
   - Performance or data quality impact?

---

## 3) AI Memory of Experiments

AI must not retry the same failed idea repeatedly.

If an approach fails, AI must:

- Document it in `docs/DECISIONS.md` under a new section:

### Attempted Solution (Failed)
Problem:
Approach tried:
Why it failed:
What constraint it violated or why it was ineffective:

This prevents cycling on the same bad idea.

---

## 4) Evidence-Based Iteration

Before proposing a new solution, AI must ask internally:

- Was a similar approach already tried?
- Why did it fail?
- What constraint must change?
- What assumption might be wrong?

New solutions must differ meaningfully from previous failed attempts.

---

## 5) Test-Driven Stability

AI must treat tests as system memory.

When fixing a bug or edge case:
- Add or extend a test capturing the failure scenario
- Ensure replay determinism still holds
- Avoid removing failing tests without documented reason

Tests represent institutional knowledge of the system.

---

## 6) Determinism Preservation

Any experiment that affects:
- Aggregation logic
- Time alignment
- Multi-source merging

Must include:
- Replay test
- Deterministic output assertion

If replay produces different results → change is invalid.

---

## 7) When AI Is Stuck

If two different approaches fail:

AI must:
1. Stop making random changes
2. Re-read CANON.md and DECISIONS.md
3. Re-evaluate assumptions
4. Propose a **design-level adjustment**, not another patch

---

## 8) Definition of a Good AI Contribution

A change is considered high-quality if:

- It respects architectural invariants
- It improves data correctness, determinism, or clarity
- It adds tests or strengthens guarantees
- It documents reasoning when behavior changes

---

## 9) What AI Must Never Do

- Reintroduce previously rejected designs
- Break determinism for convenience
- Add silent unit conversions
- Bypass EventBus for "quick fixes"
- Delete tests to make builds pass
- Retry identical failed approaches without new reasoning

---

AI is expected to evolve solutions, not loop blindly.

The repository is a learning system.  
Each iteration should leave the system more reliable, more reproducible, and more understandable.

Before making changes, AI must also read:

- `docs/DATA/data-contracts.md`
- `docs/DATA/normalization-policy.md`
- `docs/DATA/event-topics-map.md`

These define measurement semantics and determinism rules. Violating them breaks research reproducibility.
