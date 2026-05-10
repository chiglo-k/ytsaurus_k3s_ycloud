#!/usr/bin/env bash
set -euo pipefail

DAG_ID="${DAG_ID:-streaming_silver_yql}"
TASK_ID="${TASK_ID:?TASK_ID is required}"
SQL_FILE="${SQL_FILE:?SQL_FILE is required}"
TARGET_TABLE="${TARGET_TABLE:?TARGET_TABLE is required}"
RUN_ID="${RUN_ID:-manual__$(date -u +%Y%m%dT%H%M%SZ)}"

YT_PROXY="${YT_PROXY:-http://localhost:31103}"
LOG_TABLE="${LOG_TABLE:-//home/ops_logs/airflow/streaming_silver_runs}"

export YT_PROXY

STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
START_TS="$(date +%s)"

TMP_OUT="$(mktemp)"
TMP_ERR="$(mktemp)"
TMP_LOG="$(mktemp)"

set +e
yt query --format json yql "$(cat "$SQL_FILE")" > "$TMP_OUT" 2> "$TMP_ERR"
RC=$?
set -e

FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
END_TS="$(date +%s)"
DURATION_SEC="$((END_TS - START_TS))"

if [ "$RC" -eq 0 ]; then
  STATUS="success"
else
  STATUS="failed"
fi

MESSAGE="$(tail -40 "$TMP_ERR" | tr '\n' ' ' | sed 's/\\/\\\\/g; s/"/\\"/g')"
YT_QUERY_ID="$(grep -Eo '/queries/[a-zA-Z0-9-]+' "$TMP_ERR" | tail -1 | sed 's#/queries/##' || true)"

cat > "$TMP_LOG" <<JSON
{"run_id":"${RUN_ID}","dag_id":"${DAG_ID}","task_id":"${TASK_ID}","status":"${STATUS}","started_at":"${STARTED_AT}","finished_at":"${FINISHED_AT}","duration_sec":${DURATION_SEC},"yt_query_id":"${YT_QUERY_ID}","target_table":"${TARGET_TABLE}","message":"${MESSAGE}"}
JSON

yt write-table "<append=%true>${LOG_TABLE}" --format json < "$TMP_LOG" || true

cat "$TMP_ERR" >&2
cat "$TMP_OUT"

rm -f "$TMP_OUT" "$TMP_ERR" "$TMP_LOG"
exit "$RC"
