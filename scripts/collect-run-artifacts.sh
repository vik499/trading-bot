#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: scripts/collect-run-artifacts.sh [runId] [outDir] [tailN]

Collects a run artifacts bundle for a single runId from logs/.

Args:
  runId   Optional. If omitted, uses the latest runId from logs/app.log ("Старт:" line).
  outDir  Optional. Default: logs/_bundle
  tailN   Optional. Default: 200

Outputs:
  <outDir>/<runId>/summary.txt
  <outDir>/<runId>/app.degrad.log
  <outDir>/<runId>/readiness.tail.log
  <outDir>/<runId>/cvd.mismatch.log
  <outDir>/<runId>/warnings.tail.log (if logs/warnings.log exists)
  <outDir>/<runId>/warnings.runid.log (if logs/warnings.log exists)
  <outDir>/<runId>/errors.tail.log (if logs/errors.log exists)
  <outDir>/<runId>/errors.runid.log (if logs/errors.log exists)
  <outDir>/<runId>/health.tail.jsonl (if logs/health.jsonl exists)
  <outDir>/<runId>/health.runid.jsonl (if logs/health.jsonl exists)

Also creates:
  <outDir>/<runId>.tar.gz
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
fi

if [[ ! -f logs/app.log ]]; then
    echo "ERROR: logs/app.log not found." >&2
    usage >&2
    exit 1
fi

run_id="${1:-}"
out_dir="${2:-logs/_bundle}"
tail_n="${3:-200}"

if [[ -z "$run_id" ]]; then
    run_id=$(
        {
            grep -E "Старт:.*runId=" logs/app.log | tail -n 1 | sed -E 's/.*runId=([^ ]+).*/\1/'
        } || true
    )
    if [[ -z "$run_id" ]]; then
        run_id=$(
            {
                grep -E "runId=" logs/app.log | tail -n 1 | sed -E 's/.*runId=([^ ]+).*/\1/'
            } || true
        )
    fi
fi

if [[ -z "$run_id" ]]; then
    echo "ERROR: Unable to determine runId from logs/app.log." >&2
    exit 1
fi

bundle_dir="${out_dir}/${run_id}"
mkdir -p "$bundle_dir"

runid_lines="${bundle_dir}/_app.runid.log"
if ! grep -F "runId=${run_id}" logs/app.log > "$runid_lines"; then
    : > "$runid_lines"
fi

degrad_pattern='MarketData: ДЕГРАД|PRICE_STALE|NO_REF_PRICE|GAPS_DETECTED|FLOW_LOW_CONF|LIQUIDITY_LOW_CONF'
if [[ -s "$runid_lines" ]]; then
    grep -E "$degrad_pattern" "$runid_lines" | tail -n "$tail_n" > "${bundle_dir}/app.degrad.log" || :
else
    : > "${bundle_dir}/app.degrad.log"
fi

if [[ -s "$runid_lines" ]]; then
    grep -F "[ReadinessDiag]" "$runid_lines" | tail -n "$tail_n" > "${bundle_dir}/readiness.tail.log" || :
    grep -F "[CvdMismatchContext]" "$runid_lines" | tail -n "$tail_n" > "${bundle_dir}/cvd.mismatch.log" || :
else
    : > "${bundle_dir}/readiness.tail.log"
    : > "${bundle_dir}/cvd.mismatch.log"
fi

if [[ -f logs/warnings.log ]]; then
    tail -n "$tail_n" logs/warnings.log > "${bundle_dir}/warnings.tail.log" || :
    grep -F "runId=${run_id}" logs/warnings.log | tail -n "$tail_n" > "${bundle_dir}/warnings.runid.log" || :
fi

if [[ -f logs/errors.log ]]; then
    tail -n "$tail_n" logs/errors.log > "${bundle_dir}/errors.tail.log" || :
    grep -F "runId=${run_id}" logs/errors.log | tail -n "$tail_n" > "${bundle_dir}/errors.runid.log" || :
fi

if [[ -f logs/health.jsonl ]]; then
    tail -n "$tail_n" logs/health.jsonl > "${bundle_dir}/health.tail.jsonl" || :
    grep -F "\"runId\":\"${run_id}\"" logs/health.jsonl | tail -n "$tail_n" > "${bundle_dir}/health.runid.jsonl" || :
fi

summary_file="${bundle_dir}/summary.txt"
{
    echo "runId: ${run_id}"
    echo ""
    echo "MarketData status counts:"
    if [[ -s "$runid_lines" ]]; then
        echo "  MarketData: ДЕГРАД = $(grep -c "MarketData: ДЕГРАД" "$runid_lines" || true)"
        echo "  MarketData: ГОТОВ = $(grep -c "MarketData: ГОТОВ" "$runid_lines" || true)"
        echo "  MarketData: ПРОГРЕВ = $(grep -c "MarketData: ПРОГРЕВ" "$runid_lines" || true)"
    else
        echo "  (no lines found for runId)"
    fi
    echo ""
    echo "Degraded reason counts:"
    for reason in PRICE_STALE NO_REF_PRICE GAPS_DETECTED FLOW_LOW_CONF LIQUIDITY_LOW_CONF; do
        if [[ -s "$runid_lines" ]]; then
            echo "  ${reason} = $(grep -c "${reason}" "$runid_lines" || true)"
        else
            echo "  ${reason} = 0"
        fi
    done
    echo ""

    if [[ -s "$runid_lines" ]]; then
        first_ts=$(head -n 1 "$runid_lines" | sed -n 's/^\[\([^]]*\)\].*/\1/p')
        last_ts=$(tail -n 1 "$runid_lines" | sed -n 's/^\[\([^]]*\)\].*/\1/p')
        echo "First timestamp: ${first_ts:-N/A}"
        echo "Last timestamp: ${last_ts:-N/A}"
    else
        echo "First timestamp: N/A"
        echo "Last timestamp: N/A"
    fi
    echo ""
    echo "Top 10 gaps between consecutive runId log lines (seconds):"
} > "$summary_file"

# Compute gaps (Python for fast, reliable timestamp parsing)
if [[ -s "$runid_lines" ]]; then
    python3 - "$runid_lines" "$summary_file" <<'PY'
import sys
import re
from datetime import datetime

path = sys.argv[1]
summary = sys.argv[2]

lines = []
with open(path, "r", errors="ignore") as f:
    lines = f.read().splitlines()

timestamps = []
for line in lines:
    m = re.match(r'\[([^\]]+)\]', line)
    if not m:
        continue
    raw = m.group(1)
    ts = raw
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(ts)
    except Exception:
        ts2 = re.sub(r'\.\d+', '', ts)
        try:
            dt = datetime.fromisoformat(ts2)
        except Exception:
            continue
    timestamps.append((dt, raw))

gaps = []
for (prev_dt, prev_ts), (cur_dt, cur_ts) in zip(timestamps, timestamps[1:]):
    gap = (cur_dt - prev_dt).total_seconds()
    if gap < 0:
        gap = 0
    gaps.append((gap, prev_ts, cur_ts))

gaps.sort(key=lambda x: x[0], reverse=True)
with open(summary, "a") as f:
    if gaps:
        for gap, prev_ts, cur_ts in gaps[:10]:
            f.write(f"  {gap:.0f} sec  {prev_ts} -> {cur_ts}\\n")
    else:
        f.write("  (no gaps computed)\\n")
PY
else
    echo "  (no lines found for runId)" >> "$summary_file"
fi

rm -f "$runid_lines"

tarball="${out_dir}/${run_id}.tar.gz"
mkdir -p "$out_dir"
tar -czf "$tarball" -C "$out_dir" "$run_id"

echo "Bundle created: ${bundle_dir}"
echo "Tarball created: ${tarball}"
