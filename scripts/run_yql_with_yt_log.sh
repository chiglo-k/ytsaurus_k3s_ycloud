#!/usr/bin/env bash
set -euo pipefail

: "${RUN_ID:?RUN_ID is required}"
: "${DAG_ID:?DAG_ID is required}"
: "${TASK_ID:?TASK_ID is required}"
: "${SQL_FILE:?SQL_FILE is required}"
: "${TARGET_TABLE:?TARGET_TABLE is required}"
: "${YT_PROXY:?YT_PROXY is required}"

export PATH="/home/chig_k3s/yt-env/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
export YT_USE_HOSTS=0

LOG_ROOT="//home/ops_logs/airflow"
LOG_TABLE="${LOG_ROOT}/streaming_silver_runs"

START_TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
TMP_LOG="$(mktemp /tmp/streaming_silver_yql.XXXXXX.log)"

STATUS="success"
ERROR_MESSAGE=""
QUERY_RC=0

echo "[INFO] RUN_ID=${RUN_ID}"
echo "[INFO] DAG_ID=${DAG_ID}"
echo "[INFO] TASK_ID=${TASK_ID}"
echo "[INFO] SQL_FILE=${SQL_FILE}"
echo "[INFO] TARGET_TABLE=${TARGET_TABLE}"
echo "[INFO] YT_PROXY=${YT_PROXY}"

echo "[INFO] check files"
test -s "${SQL_FILE}"
command -v yt
yt --version || true

echo "[INFO] ensure YTsaurus log table"
yt create map_node //home/ops_logs --ignore-existing || true
yt create map_node "${LOG_ROOT}" --ignore-existing || true

yt create table "${LOG_TABLE}" \
  --ignore-existing \
  --attributes '{
    schema = [
      {name=run_id; type=string; required=%false;};
      {name=dag_id; type=string; required=%false;};
      {name=task_id; type=string; required=%false;};
      {name=target_table; type=string; required=%false;};
      {name=sql_file; type=string; required=%false;};
      {name=status; type=string; required=%false;};
      {name=start_ts; type=string; required=%false;};
      {name=end_ts; type=string; required=%false;};
      {name=duration_sec; type=double; required=%false;};
      {name=rc; type=int64; required=%false;};
      {name=error_message; type=string; required=%false;};
      {name=message; type=string; required=%false;};
    ];
  }' || true

START_EPOCH="$(date +%s)"

echo "[INFO] run YQL"
set +e
yt query --format json yql "$(cat "${SQL_FILE}")" > "${TMP_LOG}" 2>&1
QUERY_RC=$?
set -e

cat "${TMP_LOG}"

if [ "${QUERY_RC}" -ne 0 ]; then
  STATUS="failed"
  ERROR_MESSAGE="yt query failed with rc=${QUERY_RC}"
fi

END_TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
END_EPOCH="$(date +%s)"
DURATION_SEC="$((END_EPOCH - START_EPOCH))"

echo "[INFO] write run log to ${LOG_TABLE}"

python3 - "${TMP_LOG}" <<PY | yt write-table --append "${LOG_TABLE}" --format json
import json
import sys
from pathlib import Path

log_path = Path(sys.argv[1])
message = log_path.read_text(encoding="utf-8", errors="replace")

row = {
    "run_id": "${RUN_ID}",
    "dag_id": "${DAG_ID}",
    "task_id": "${TASK_ID}",
    "target_table": "${TARGET_TABLE}",
    "sql_file": "${SQL_FILE}",
    "status": "${STATUS}",
    "start_ts": "${START_TS}",
    "end_ts": "${END_TS}",
    "duration_sec": float("${DURATION_SEC}"),
    "rc": int("${QUERY_RC}"),
    "error_message": "${ERROR_MESSAGE}",
    "message": message[-20000:],
}

print(json.dumps(row, ensure_ascii=False))
PY

rm -f "${TMP_LOG}"

if [ "${STATUS}" != "success" ]; then
  exit "${QUERY_RC}"
fi

echo "[DONE] task=${TASK_ID} target=${TARGET_TABLE} status=${STATUS} rc=${QUERY_RC}"