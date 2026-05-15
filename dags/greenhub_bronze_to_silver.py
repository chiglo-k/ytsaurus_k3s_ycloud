#!/usr/bin/env python3
"""
greenhub_bronze_to_silver.py

DAG-2: YTsaurus bronze fact + dim -> silver wide table.

Airflow -> SSH vm1 -> bash launch_bronze_to_silver.sh -> spark-submit-yt/SPYT.
State/log lineage пишется в S3: _state/greenhub_silver_runs.json.
"""

from __future__ import annotations

import json
import logging
import re
import shlex
import uuid as _uuid
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ssh.hooks.ssh import SSHHook

try:
    from airflow.operators.python import get_current_context
except ImportError:
    def get_current_context() -> dict:
        return {}


logger = logging.getLogger(__name__)

DATASET_TAG = "greenhub"
LOADER_NAME = "spyt_bronze_to_silver"

ROW_IN_RE = re.compile(r"\[DONE\]\s+bronze fact in\s*:\s*([\d,]+)")
ROW_DEDUP_RE = re.compile(r"\[DONE\]\s+after dedup\s*:\s*([\d,]+)")
ROW_OUT_RE = re.compile(r"\[DONE\]\s+silver out\s*:\s*([\d,]+)")

REMOTE_LOG_DIR = "/home/chig_k3s/git/spyt-logs"
OPS_LOG_ROOT = "//home/ops_logs/greenhub"
S3_LOG_PREFIX = "_logs/greenhub"
DEFAULT_YT_PROXY = "http://localhost:31103"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def q(value: Any) -> str:
    return shlex.quote("" if value is None else str(value))


def safe_name(value: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in str(value))


def load_json_from_s3(hook: S3Hook, bucket_name: str, key: str, default: Any) -> Any:
    if not hook.check_for_key(key=key, bucket_name=bucket_name):
        return default

    obj = hook.get_key(key=key, bucket_name=bucket_name)
    if obj is None:
        return default

    body = obj.get()["Body"].read()
    if not body:
        return default

    try:
        return json.loads(body.decode("utf-8"))
    except json.JSONDecodeError:
        return default


def save_json_to_s3(hook: S3Hook, bucket_name: str, key: str, data: Any) -> None:
    payload = json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    hook.load_bytes(bytes_data=payload, bucket_name=bucket_name, key=key, replace=True)


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def duration_seconds(started_at: str | None, finished_at: str | None) -> float | None:
    start = parse_iso(started_at)
    finish = parse_iso(finished_at)
    if not start or not finish:
        return None
    return max((finish - start).total_seconds(), 0.0)


def bronze_silver_remote_log_path(batch_id: str) -> str:
    return f"{REMOTE_LOG_DIR}/bronze_to_silver_{safe_name(batch_id)}.log"


def current_run_id() -> str | None:
    try:
        context = get_current_context()
    except Exception:
        return None
    return context.get("run_id")


def save_run_json_log_to_s3(s3: S3Hook, bucket: str, category: str, batch_id: str, payload: dict[str, Any]) -> str:
    key = f"{S3_LOG_PREFIX}/{category}/{safe_name(batch_id)}.json"
    save_json_to_s3(s3, bucket, key, payload)
    return key


def write_json_rows_to_yt(table_path: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    yt_proxy = Variable.get("YT_PROXY", default_var=DEFAULT_YT_PROXY)
    ssh_conn_id = Variable.get("GREENHUB_SSH_CONN_ID", default_var="vm1_ssh")
    payload = "\n".join(json.dumps(row, ensure_ascii=False, default=str) for row in rows)

    command = f"""
set -euo pipefail
export YT_PROXY={q(yt_proxy)}
export YT_USE_HOSTS=0
export PATH="/home/chig_k3s/yt-env/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
cat <<'JSONL' | yt write-table --format json {q(f'<append=%true>{table_path}')}
{payload}
JSONL
""".strip()

    ssh = SSHHook(ssh_conn_id=ssh_conn_id, conn_timeout=30)
    with ssh.get_conn() as conn:
        stdin, stdout, stderr = conn.exec_command(command, get_pty=True, timeout=120)
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        rc = stdout.channel.recv_exit_status()

    if out:
        print(out[-12000:])
    if err:
        print(err[-12000:])
    if rc != 0:
        raise RuntimeError(f"Failed to write YT log table {table_path}: rc={rc}")


def empty_silver_state() -> dict[str, Any]:
    now = now_utc_iso()
    return {
        "schema_version": 1,
        "dataset": DATASET_TAG,
        "loader": LOADER_NAME,
        "last_success": None,
        "events": [],
        "created_at": now,
        "updated_at": now,
    }


def build_remote_command(batch_id: str) -> str:
    launcher = Variable.get(
        "GREENHUB_BRONZE_SILVER_LAUNCHER",
        default_var="/home/chig_k3s/git/repo/spyt/launch_bronze_to_silver.sh",
    )
    yt_proxy = Variable.get("YT_PROXY", default_var="http://localhost:31103")

    remote_log = bronze_silver_remote_log_path(batch_id)

    return f"""
set -u

mkdir -p {q(REMOTE_LOG_DIR)}

export BATCH_ID={q(batch_id)}
export YT_PROXY={q(yt_proxy)}
export YT_USE_HOSTS=0
export DRIVER_HOST=10.130.0.24
export SPARK_LOCAL_IP=10.130.0.24

echo "[AIRFLOW] host=$(hostname)"
echo "[AIRFLOW] started_at=$(date -Is)"
echo "[AIRFLOW] launcher={launcher}"
echo "[AIRFLOW] full_log={remote_log}"

set +e

bash {q(launcher)} > {q(remote_log)} 2>&1

RC=$?

echo "[AIRFLOW] spark_rc=$RC"
echo "[AIRFLOW] finished_at=$(date -Is)"
echo "[AIRFLOW] full_log={remote_log}"

echo ""
echo "========== ERROR GREP =========="
grep -n -Ei 'AnalysisException|Py4JJavaError|Exception|Caused by|YtError|Yt|Unsupported|schema|denied|exists|failed|Traceback|Error|Cannot|Initial job has not accepted|Lost executor|Disconnected|PYTHON_VERSION_MISMATCH|Permission denied' {q(remote_log)} | head -n 240 || true

echo ""
echo "========== DONE GREP =========="
grep -n -Ei '\\[DONE\\]|\\[INFO\\] bronze fact rows|\\[INFO\\] silver wide rows|silver append candidates|appending silver|no new silver' {q(remote_log)} || true

echo ""
echo "========== FULL LOG TAIL 300 =========="
tail -n 300 {q(remote_log)} || true

exit $RC
""".strip()


@dag(
    dag_id="greenhub_bronze_to_silver",
    description="YT bronze fact + dim -> silver wide greenhub_telemetry",
    schedule=None,
    start_date=datetime(2026, 5, 7, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["greenhub", "spyt", "silver", "diploma"],
)
def greenhub_bronze_to_silver():

    @task
    def make_batch_id() -> str:
        batch_id = str(_uuid.uuid4())
        logger.info("batch_id=%s", batch_id)
        return batch_id

    @task(
        pool="spyt_pool",
        execution_timeout=timedelta(minutes=60),
        retries=1,
        retry_delay=timedelta(minutes=3),
    )
    def submit_bronze_to_silver(batch_id: str) -> dict[str, Any]:
        started_at = now_utc_iso()
        ssh_conn_id = Variable.get("GREENHUB_SSH_CONN_ID", default_var="vm1_ssh")
        command = build_remote_command(batch_id)

        logger.info("SSH (%s) submit command:\n%s", ssh_conn_id, command)

        result: dict[str, Any] = {
            "batch_id": batch_id,
            "loader": LOADER_NAME,
            "started_at": started_at,
            "finished_at": None,
            "load_status": None,
            "rc": None,
            "rows_bronze_in": None,
            "rows_after_dedup": None,
            "rows_silver_out": None,
            "target_table": "//home/silver_stage/greenhub_telemetry",
            "remote_log": bronze_silver_remote_log_path(batch_id),
            "error": None,
        }

        try:
            ssh = SSHHook(ssh_conn_id=ssh_conn_id, conn_timeout=30)
            with ssh.get_conn() as conn:
                stdin, stdout, stderr = conn.exec_command(command, get_pty=True, timeout=3600)
                out_str = stdout.read().decode("utf-8", errors="replace")
                err_str = stderr.read().decode("utf-8", errors="replace")
                rc = stdout.channel.recv_exit_status()
        except Exception as exc:
            logger.exception("SSH/spark-submit transport error")
            return {
                **result,
                "load_status": "FAILED_TRANSPORT",
                "finished_at": now_utc_iso(),
                "error": repr(exc),
            }

        print("========== REMOTE STDOUT ==========")
        print(out_str[-30000:] if out_str else "<empty>")

        print("========== REMOTE STDERR ==========")
        print(err_str[-30000:] if err_str else "<empty>")

        if (m := ROW_IN_RE.search(out_str)):
            result["rows_bronze_in"] = int(m.group(1).replace(",", ""))
        if (m := ROW_DEDUP_RE.search(out_str)):
            result["rows_after_dedup"] = int(m.group(1).replace(",", ""))
        if (m := ROW_OUT_RE.search(out_str)):
            result["rows_silver_out"] = int(m.group(1).replace(",", ""))

        finished_at = now_utc_iso()

        return {
            **result,
            "load_status": "SUCCESS" if rc == 0 else "FAILED",
            "finished_at": finished_at,
            "rc": rc,
            "error": None if rc == 0 else f"spark-submit-yt exit={rc}",
        }

    @task(trigger_rule="all_done")
    def mark_silver_run(result: dict[str, Any]) -> None:
        if not result:
            logger.info("No silver run to record")
            return

        aws_conn = Variable.get("GREENHUB_AWS_CONN_ID", default_var="seaweedfs_s3")
        bucket = Variable.get("GREENHUB_S3_BUCKET", default_var="greenhub")
        state_key = Variable.get(
            "GREENHUB_SILVER_STATE_KEY",
            default_var="_state/greenhub_silver_runs.json",
        )

        s3 = S3Hook(aws_conn_id=aws_conn)
        state = load_json_from_s3(s3, bucket, state_key, empty_silver_state())
        state.setdefault("events", [])

        event = {
            "event_at": now_utc_iso(),
            "dag_id": "greenhub_bronze_to_silver",
            "run_id": current_run_id(),
            "batch_id": result.get("batch_id"),
            "loader": LOADER_NAME,
            "source_layer": "bronze",
            "target_layer": "silver",
            "target_table": result.get("target_table") or "//home/silver_stage/greenhub_telemetry",
            "load_status": result.get("load_status"),
            "started_at": result.get("started_at"),
            "finished_at": result.get("finished_at"),
            "duration_sec": duration_seconds(result.get("started_at"), result.get("finished_at")),
            "rows_bronze_in": result.get("rows_bronze_in"),
            "rows_after_dedup": result.get("rows_after_dedup"),
            "rows_silver_out": result.get("rows_silver_out"),
            "rc": result.get("rc"),
            "error": result.get("error"),
            "remote_log": result.get("remote_log"),
            "message": None,
        }
        state["events"].append(event)

        if result.get("load_status") == "SUCCESS":
            state["last_success"] = {
                "batch_id": result.get("batch_id"),
                "finished_at": result.get("finished_at"),
                "rows_silver_out": result.get("rows_silver_out"),
            }

        state["updated_at"] = now_utc_iso()
        save_json_to_s3(s3, bucket, state_key, state)

        log_key = save_run_json_log_to_s3(
            s3,
            bucket,
            "layer_runs",
            result.get("batch_id") or _uuid.uuid4().hex,
            {"layer_run": event},
        )
        event["message"] = f"state_key={state_key}; s3_log_key={log_key}"
        write_json_rows_to_yt(f"{OPS_LOG_ROOT}/layer_runs", [event])

        logger.info("Silver state %s/%s updated; log=%s", bucket, state_key, log_key)

        if result.get("load_status") != "SUCCESS":
            raise RuntimeError("SPYT bronze_to_silver failed. Logs were written to S3 and YTsaurus.")

    batch_id = make_batch_id()
    result = submit_bronze_to_silver(batch_id)
    mark_silver_run(result)


dag = greenhub_bronze_to_silver()
