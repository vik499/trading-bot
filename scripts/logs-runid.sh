#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/logs-runid.sh [runId] [tailN]

Prints surgical runId excerpts from logs/app.log.

Args:
  runId  Optional. If omitted, latest runId from "Старт:" line is used.
  tailN  Optional. Max lines per section. Default: 50.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

LOG_FILE="${LOG_FILE:-logs/app.log}"
RUNID="${1:-}"
TAIL_N="${2:-50}"

if [[ ! -f "${LOG_FILE}" ]]; then
  echo "log file not found: ${LOG_FILE}"
  exit 1
fi

if [[ -z "${RUNID}" ]]; then
  RUNID="$(grep -F "Старт:" "${LOG_FILE}" | sed -n 's/.*runId=\\([^ ]*\\).*/\\1/p' | tail -n 1)"
fi

if [[ -z "${RUNID}" ]]; then
  echo "runId not found in ${LOG_FILE} (pass it as first arg)"
  exit 1
fi

echo "runId=${RUNID}"
echo
echo "== MarketData: ДЕГРАД =="
grep -F "runId=${RUNID}" "${LOG_FILE}" | grep -F "MarketData: ДЕГРАД" | tail -n "${TAIL_N}" || true
echo
echo "== Reasons (PRICE_STALE|NO_REF_PRICE|GAPS_DETECTED|FLOW_LOW_CONF|LIQUIDITY_LOW_CONF) =="
grep -F "runId=${RUNID}" "${LOG_FILE}" | grep -E "PRICE_STALE|NO_REF_PRICE|GAPS_DETECTED|FLOW_LOW_CONF|LIQUIDITY_LOW_CONF" | tail -n "${TAIL_N}" || true
echo
echo "== Last ${TAIL_N} ReadinessDiag =="
grep -F "runId=${RUNID}" "${LOG_FILE}" | grep -F "[ReadinessDiag]" | tail -n "${TAIL_N}" || true
