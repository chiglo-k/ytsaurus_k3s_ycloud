#!/usr/bin/env python3
"""
greenhub_silver_to_gold.py

DAG-3: YTsaurus silver wide table -> gold marts.

Airflow -> SSH vm1 -> bash launch_silver_to_gold.sh <mart> -> spark-submit-yt/SPYT.
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


logger = logging.getLogger(__name__)

DATASET_TAG = "greenhub"
LOADER_NAME = "spyt_silver_to_gold"

MARTS = [
    "daily_country_stats",
]

ROW_SILVER_RE = re.compile(r"\[DONE\]\s+silver in\s*:\s*([\d,]+)")
ROW_GOLD_RE = re.compile(r"\[DONE\]\s+gold rows\s*:\s*([\d,]+)")

REMOTE_LOG_DIR = "/home/chig_k3s/git/spyt-logs"


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


def empty_gold_state() -> dict[str, Any]:
    now = now_utc_iso()
    return {
        "schema_version": 1,
        "dataset": DATASET_TAG,
        "loader": LOADER_NAME,
        "last_success": {},
        "events": [],
        "created_at": now,
        "updated_at": now,
    }


def build_remote_command(mart: str, batch_id: str) -> str:
    launcher = Variable.get(
        "GREENHUB_SILVER_GOLD_LAUNCHER",
        default_var="/home/chig_k3s/git/repo/spyt/launch_silver_to_gold.sh",
    )
    yt_proxy = Variable.get("YT_PROXY", default_var="http://localhost:31103")

    remote_log = f"{REMOTE_LOG_DIR}/silver_to_gold_{safe_name(mart)}_{safe_name(batch_id)}.log"

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
echo "[AIRFLOW] mart={mart}"
echo "[AIRFLOW] launcher={launcher}"
echo "[AIRFLOW] full_log={remote_log}"

set +e

bash {q(launcher)} {q(mart)} > {q(remote_log)} 2>&1

RC=$?

echo "[AIRFLOW] spark_rc=$RC"
echo "[AIRFLOW] finished_at=$(date -Is)"
echo "[AIRFLOW] full_log={remote_log}"

echo ""
echo "========== ERROR GREP =========="
grep -n -Ei 'AnalysisException|Py4JJavaError|Exception|Caused by|YtError|Yt|Unsupported|schema|denied|exists|failed|Traceback|Error|Cannot|Initial job has not accepted|Lost executor|Disconnected|PYTHON_VERSION_MISMATCH|unknown mart|Permission denied' {q(remote_log)} | head -n 240 || true

echo ""
echo "========== DONE GREP =========="
grep -n -Ei '\\[DONE\\]|\\[INFO\\] silver rows in|\\[INFO\\] mart rows out|writing' {q(remote_log)} || true

echo ""
echo "========== FULL LOG TAIL 300 =========="
tail -n 300 {q(remote_log)} || true

exit $RC
""".strip()


@dag(
    dag_id="greenhub_silver_to_gold",
    description="YT silver greenhub_telemetry -> gold marts",
    schedule=None,
    start_date=datetime(2026, 5, 7, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["greenhub", "spyt", "gold", "diploma"],
)
def greenhub_silver_to_gold():

    @task
    def make_batch_id() -> str:
        batch_id = str(_uuid.uuid4())
        logger.info("batch_id=%s", batch_id)
        return batch_id

    @task
    def list_marts() -> list[str]:
        return MARTS

    @task(
        pool="spyt_pool",
        execution_timeout=timedelta(minutes=60),
        retries=1,
        retry_delay=timedelta(minutes=3),
    )
    def submit_gold_mart(mart: str, batch_id: str) -> dict[str, Any]:
        started_at = now_utc_iso()
        ssh_conn_id = Variable.get("GREENHUB_SSH_CONN_ID", default_var="vm1_ssh")
        command = build_remote_command(mart, batch_id)

        logger.info("SSH (%s) mart=%s command:\n%s", ssh_conn_id, mart, command)

        result: dict[str, Any] = {
            "mart": mart,
            "batch_id": batch_id,
            "loader": LOADER_NAME,
            "started_at": started_at,
            "finished_at": None,
            "load_status": None,
            "rc": None,
            "rows_silver_in": None,
            "rows_gold_out": None,
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
            logger.exception("SSH/spark-submit transport error for mart=%s", mart)
            return {
                **result,
                "load_status": "FAILED_TRANSPORT",
                "finished_at": now_utc_iso(),
                "error": repr(exc),
            }

        print(f"========== REMOTE STDOUT mart={mart} ==========")
        print(out_str[-30000:] if out_str else "<empty>")

        print(f"========== REMOTE STDERR mart={mart} ==========")
        print(err_str[-30000:] if err_str else "<empty>")

        if (m := ROW_SILVER_RE.search(out_str)):
            result["rows_silver_in"] = int(m.group(1).replace(",", ""))
        if (m := ROW_GOLD_RE.search(out_str)):
            result["rows_gold_out"] = int(m.group(1).replace(",", ""))

        finished_at = now_utc_iso()

        if rc != 0:
            raise RuntimeError(
                f"SPYT silver_to_gold mart={mart} failed with rc={rc}. "
                f"Check REMOTE STDOUT above; it includes ERROR GREP and full remote log tail."
            )

        return {
            **result,
            "load_status": "SUCCESS",
            "finished_at": finished_at,
            "rc": rc,
            "error": None,
        }

    @task(trigger_rule="none_failed_min_one_success")
    def mark_gold_runs(results: list[dict[str, Any]]) -> None:
        if not results:
            logger.info("No gold runs to record")
            return

        aws_conn = Variable.get("GREENHUB_AWS_CONN_ID", default_var="seaweedfs_s3")
        bucket = Variable.get("GREENHUB_S3_BUCKET", default_var="greenhub")
        state_key = Variable.get(
            "GREENHUB_GOLD_STATE_KEY",
            default_var="_state/greenhub_gold_runs.json",
        )

        s3 = S3Hook(aws_conn_id=aws_conn)
        state = load_json_from_s3(s3, bucket, state_key, empty_gold_state())
        state.setdefault("events", [])
        state.setdefault("last_success", {})

        for item in results:
            event = {
                "event_at": now_utc_iso(),
                "mart": item.get("mart"),
                "batch_id": item.get("batch_id"),
                "load_status": item.get("load_status"),
                "rows_silver_in": item.get("rows_silver_in"),
                "rows_gold_out": item.get("rows_gold_out"),
                "started_at": item.get("started_at"),
                "finished_at": item.get("finished_at"),
                "rc": item.get("rc"),
                "error": item.get("error"),
            }
            state["events"].append(event)

            if item.get("load_status") == "SUCCESS":
                state["last_success"][item.get("mart")] = {
                    "batch_id": item.get("batch_id"),
                    "finished_at": item.get("finished_at"),
                    "rows_gold_out": item.get("rows_gold_out"),
                }

        state["updated_at"] = now_utc_iso()
        save_json_to_s3(s3, bucket, state_key, state)
        logger.info("Gold state %s/%s updated", bucket, state_key)

    batch_id = make_batch_id()
    marts = list_marts()
    results = submit_gold_mart.partial(batch_id=batch_id).expand(mart=marts)
    mark_gold_runs(results)


dag = greenhub_silver_to_gold()
