#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/phase0_collect_run.sh <runId>
  scripts/phase0_collect_run.sh --self-test

Collect runId-scoped Phase 0 diagnostics bundle into:
  logs/runs/<runId>/

This script is local-only and bounded (tail/filter caps only).
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

MAX_LINES="${MAX_LINES:-300}"
LOG_DIR="logs"
SELF_TEST=0
RUN_ID=""

if [[ "${1:-}" == "--self-test" ]]; then
  SELF_TEST=1
  RUN_ID="TEST_RUN_ID"
elif [[ $# -eq 1 ]]; then
  RUN_ID="$1"
else
  usage >&2
  exit 1
fi

OUT_DIR="${LOG_DIR}/runs/${RUN_ID}"
mkdir -p "${OUT_DIR}"

has_rg() {
  command -v rg >/dev/null 2>&1
}

pick_file() {
  for f in "$@"; do
    if [[ -f "$f" ]]; then
      echo "$f"
      return 0
    fi
  done
  return 1
}

pick_first_glob() {
  local pattern="$1"
  local found=""
  found="$(ls -1 ${pattern} 2>/dev/null | head -n 1 || true)"
  if [[ -n "$found" && -f "$found" ]]; then
    echo "$found"
    return 0
  fi
  return 1
}

write_missing() {
  local path="$1"
  local message="$2"
  printf "%s\n" "$message" > "$path"
}

filter_runid() {
  local file="$1"
  if has_rg; then
    rg -n --no-heading -e "runId=${RUN_ID}" -e "\"runId\":\"${RUN_ID}\"" "$file" 2>/dev/null || true
  else
    grep -nE "runId=${RUN_ID}|\"runId\":\"${RUN_ID}\"" "$file" 2>/dev/null || true
  fi
}

filter_with_pattern() {
  local pattern="$1"
  if has_rg; then
    rg -e "$pattern" 2>/dev/null || true
  else
    grep -E "$pattern" 2>/dev/null || true
  fi
}

run_self_test() {
  local self_out_dir="${LOG_DIR}/runs/TEST_RUN_ID"
  mkdir -p "${self_out_dir}"

  cat > "${self_out_dir}/meta.txt" <<EOF
runId=TEST_RUN_ID
mode=self-test
utcCollectedAt=$(date -u +%Y-%m-%dT%H:%M:%SZ)
gitHead=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
gitBranch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)
logsDir=${LOG_DIR}
chosenAppLog=self-test
chosenHealthLog=self-test
chosenWarningsLog=self-test
chosenErrorsLog=self-test
maxLines=${MAX_LINES}
EOF

  printf "self-test: app tail placeholder\n" > "${self_out_dir}/app.tail.txt"
  printf "self-test: readiness timeline placeholder\n" > "${self_out_dir}/readiness.timeline.txt"
  printf "self-test: readiness reasons placeholder\n" > "${self_out_dir}/readiness.reasons.txt"
  printf "self-test: unions truth placeholder\n" > "${self_out_dir}/unions.truth.txt"
  printf "self-test: source coverage placeholder\n" > "${self_out_dir}/source.coverage.txt"
  printf "self-test: timebase placeholder\n" > "${self_out_dir}/timebase.txt"
  printf "self-test: warnings placeholder\n" > "${self_out_dir}/warnings.txt"
  printf "self-test: errors placeholder\n" > "${self_out_dir}/errors.txt"
  printf "{\"runId\":\"TEST_RUN_ID\",\"selfTest\":true}\n" > "${self_out_dir}/health.tail.jsonl"
  printf "self-test: logs directory listing placeholder\n" > "${self_out_dir}/files.ls.after.txt"

  cat > "${self_out_dir}/notes.md" <<'EOF'
# Phase 0 Run Notes

- runId: TEST_RUN_ID
- duration:
- dominant degraded reasons:
- dominant warnings:
- readiness transitions summary:
- one focused next probe:
EOF

  local required=(
    "meta.txt"
    "app.tail.txt"
    "readiness.timeline.txt"
    "readiness.reasons.txt"
    "unions.truth.txt"
    "source.coverage.txt"
    "timebase.txt"
    "warnings.txt"
    "errors.txt"
    "health.tail.jsonl"
    "files.ls.after.txt"
    "notes.md"
  )

  local missing=0
  for file in "${required[@]}"; do
    if [[ ! -s "${self_out_dir}/${file}" ]]; then
      echo "SELF-TEST ERROR: missing or empty ${self_out_dir}/${file}" >&2
      missing=1
    fi
  done

  if [[ "${missing}" -ne 0 ]]; then
    exit 2
  fi

  echo "SELF-TEST OK: ${self_out_dir}"
}

if [[ "${SELF_TEST}" -eq 1 ]]; then
  run_self_test
  exit 0
fi

APP_LOG=""
HEALTH_LOG=""
WARN_LOG=""
ERROR_LOG=""

if [[ -d "${LOG_DIR}" ]]; then
  APP_LOG="$(pick_file "${LOG_DIR}/app.log" "${LOG_DIR}/app.jsonl" || true)"
  if [[ -z "${APP_LOG}" ]]; then
    APP_LOG="$(pick_first_glob "${LOG_DIR}/*app*.log*" || true)"
  fi

  HEALTH_LOG="$(pick_file "${LOG_DIR}/health.jsonl" "${LOG_DIR}/health.log" || true)"
  if [[ -z "${HEALTH_LOG}" ]]; then
    HEALTH_LOG="$(pick_first_glob "${LOG_DIR}/*health*.jsonl*" || true)"
  fi
  if [[ -z "${HEALTH_LOG}" ]]; then
    HEALTH_LOG="$(pick_first_glob "${LOG_DIR}/*health*.log*" || true)"
  fi

  WARN_LOG="$(pick_file "${LOG_DIR}/warnings.log" || true)"
  if [[ -z "${WARN_LOG}" ]]; then
    WARN_LOG="$(pick_first_glob "${LOG_DIR}/*warn*.log*" || true)"
  fi

  ERROR_LOG="$(pick_file "${LOG_DIR}/errors.log" || true)"
  if [[ -z "${ERROR_LOG}" ]]; then
    ERROR_LOG="$(pick_first_glob "${LOG_DIR}/*error*.log*" || true)"
  fi
fi

{
  echo "runId=${RUN_ID}"
  echo "utcCollectedAt=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "gitHead=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "gitBranch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
  echo "logsDir=${LOG_DIR}"
  echo "chosenAppLog=${APP_LOG:-missing}"
  echo "chosenHealthLog=${HEALTH_LOG:-missing}"
  echo "chosenWarningsLog=${WARN_LOG:-missing}"
  echo "chosenErrorsLog=${ERROR_LOG:-missing}"
  echo "maxLines=${MAX_LINES}"
} > "${OUT_DIR}/meta.txt"

if [[ -n "${APP_LOG}" && -f "${APP_LOG}" ]]; then
  filter_runid "${APP_LOG}" | tail -n "${MAX_LINES}" > "${OUT_DIR}/app.tail.txt"
  cat "${OUT_DIR}/app.tail.txt" | filter_with_pattern "MarketData status=|MarketData:|ReadinessDiag|system:market_data_status" | tail -n "${MAX_LINES}" > "${OUT_DIR}/readiness.timeline.txt"
  cat "${OUT_DIR}/app.tail.txt" | filter_with_pattern "PRICE_STALE|NO_REF_PRICE|GAPS_DETECTED|FLOW_LOW_CONF|LIQUIDITY_LOW_CONF|DERIVATIVES_LOW_CONF|SOURCES_MISSING|MISMATCH_DETECTED|EXPECTED_SOURCES_MISSING_CONFIG|NO_DATA|WS_DISCONNECTED|LAG_TOO_HIGH" | tail -n "${MAX_LINES}" > "${OUT_DIR}/readiness.reasons.txt"
else
  write_missing "${OUT_DIR}/app.tail.txt" "missing app log"
  write_missing "${OUT_DIR}/readiness.timeline.txt" "missing app log"
  write_missing "${OUT_DIR}/readiness.reasons.txt" "missing app log"
fi

if [[ -n "${HEALTH_LOG}" && -f "${HEALTH_LOG}" ]]; then
  filter_runid "${HEALTH_LOG}" | tail -n "${MAX_LINES}" > "${OUT_DIR}/health.tail.jsonl"
  cat "${OUT_DIR}/health.tail.jsonl" | filter_with_pattern "worstStatusInMinute|reasonsUnionInMinute|warningsUnionInMinute|degradedReasons|warnings|sourceCoverage|timebase" | tail -n "${MAX_LINES}" > "${OUT_DIR}/unions.truth.txt"
  cat "${OUT_DIR}/health.tail.jsonl" | filter_with_pattern "sourceCoverage" | tail -n "${MAX_LINES}" > "${OUT_DIR}/source.coverage.txt"
  cat "${OUT_DIR}/health.tail.jsonl" | filter_with_pattern "timebase" | tail -n "${MAX_LINES}" > "${OUT_DIR}/timebase.txt"
else
  write_missing "${OUT_DIR}/health.tail.jsonl" "missing health log"
  write_missing "${OUT_DIR}/unions.truth.txt" "missing health log"
  write_missing "${OUT_DIR}/source.coverage.txt" "missing health log"
  write_missing "${OUT_DIR}/timebase.txt" "missing health log"
fi

if [[ -n "${WARN_LOG}" && -f "${WARN_LOG}" ]]; then
  filter_runid "${WARN_LOG}" | tail -n "${MAX_LINES}" > "${OUT_DIR}/warnings.txt"
else
  if [[ -f "${OUT_DIR}/app.tail.txt" ]]; then
    cat "${OUT_DIR}/app.tail.txt" | filter_with_pattern "WARN|EXCHANGE_LAG_TOO_HIGH|TIMEBASE_QUALITY_WARN|GAP_ATTRIBUTION_MISSING" | tail -n "${MAX_LINES}" > "${OUT_DIR}/warnings.txt"
  else
    write_missing "${OUT_DIR}/warnings.txt" "missing warnings log"
  fi
fi

if [[ -n "${ERROR_LOG}" && -f "${ERROR_LOG}" ]]; then
  filter_runid "${ERROR_LOG}" | tail -n "${MAX_LINES}" > "${OUT_DIR}/errors.txt"
else
  if [[ -f "${OUT_DIR}/app.tail.txt" ]]; then
    cat "${OUT_DIR}/app.tail.txt" | filter_with_pattern "ERROR|FATAL|Exception|TSError" | tail -n "${MAX_LINES}" > "${OUT_DIR}/errors.txt"
  else
    write_missing "${OUT_DIR}/errors.txt" "missing errors log"
  fi
fi

if [[ -d "${LOG_DIR}" ]]; then
  ls -la "${LOG_DIR}" | head -n 300 > "${OUT_DIR}/files.ls.after.txt"
else
  write_missing "${OUT_DIR}/files.ls.after.txt" "missing logs directory"
fi

if [[ ! -f "${OUT_DIR}/notes.md" ]]; then
  cat > "${OUT_DIR}/notes.md" <<'EOF'
# Phase 0 Run Notes

- runId:
- duration:
- dominant degraded reasons:
- dominant warnings:
- readiness transitions summary:
- one focused next probe:
EOF
fi

echo "Collected run bundle: ${OUT_DIR}"
