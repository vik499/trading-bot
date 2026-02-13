# RUNBOOK_PHASE0 — Market Data Validation by `runId`

## Purpose

Reusable checklist for Phase 0 run analysis after a human executes live runtime.
This runbook focuses on data correctness and observability for Market Data readiness.
This runbook does not run live market execution by itself.

## Initial Setup (one-time)

1. Verify collector script exists:

```bash
test -f scripts/phase0_collect_run.sh && echo "collector found"
```

2. Make script executable:

```bash
chmod +x scripts/phase0_collect_run.sh
```

3. Normal invocation after a live run:

```bash
./scripts/phase0_collect_run.sh <runId>
```

## Dry Run Self-Test (filesystem-only)

Run collector self-test without live logs:

```bash
scripts/phase0_collect_run.sh --self-test
```

Expected result:
- folder `logs/runs/TEST_RUN_ID/` is created;
- placeholder bundle files are created with expected names;
- `SELF-TEST OK` means script wiring and file output contract are valid.

## Preconditions

1. Ensure local branch/commit is recorded before live run.
2. Ensure no pending local changes that would make run attribution ambiguous.
3. Confirm run time target (recommended: 20-30 minutes minimum).
4. Confirm stop conditions are known before starting.

## Human-Run Live Step (manual)

1. Start runtime manually (human only).
2. Capture `runId` from startup log line.
3. Let run continue through normal market conditions.
4. Stop runtime when one of the following is true:
   - planned duration reached;
   - repeated hard degradation persists;
   - operator stop criteria reached.

## Artifact Collection

Run after live stops:

```bash
scripts/phase0_collect_run.sh <runId>
```

Collector output directory:

```text
logs/runs/<runId>/
```

Expected artifacts:

- `meta.txt`
- `app.tail.txt`
- `readiness.timeline.txt`
- `readiness.reasons.txt`
- `unions.truth.txt`
- `warnings.txt`
- `errors.txt`
- `health.tail.jsonl` (or a `missing` placeholder)
- `files.ls.after.txt`
- `notes.md`

## PASS Criteria (Phase 0)

1. No persistent hard-failure reasons:
   - `NO_DATA`
   - `WS_DISCONNECTED`
   - `EXPECTED_SOURCES_MISSING_CONFIG`
2. Hysteresis behavior is stable:
   - no rapid READY<->DEGRADED flapping from transient pulses;
   - sustained hard reasons enter degradation after configured enter windows;
   - recovery happens only after configured stable exit windows.
3. Warning-only signals remain warning-only:
   - `EXCHANGE_LAG_TOO_HIGH` appears in warnings but does not directly degrade.
4. Truth loop visibility is intact:
   - minute rollups (`worstStatusInMinute`, reason/warning unions) show transient degradations even if snapshot status ends READY.
5. No unexplained degradation:
   - degraded minutes have corresponding reasons and telemetry evidence.

## FAIL Criteria

Any of the following is a FAIL for the run:

1. Persistent hard degradation without clear root cause.
2. `EXPECTED_SOURCES_MISSING_CONFIG` appears and then "heals" without config correction.
3. Runtime state oscillates rapidly while truth loop and hysteresis expectations disagree.
4. Missing or empty artifacts that block run-level diagnosis.

## Analysis Attachments (for review / Codex analysis)

Attach from `logs/runs/<runId>/`:

1. `meta.txt`
2. `readiness.timeline.txt`
3. `readiness.reasons.txt`
4. `unions.truth.txt`
5. `warnings.txt`
6. `errors.txt`
7. `health.tail.jsonl`
8. `notes.md`

## Notes Template Usage

Update `notes.md` with:

1. run summary (start/end, duration),
2. dominant degradation reasons,
3. warning context,
4. one focused next probe.
