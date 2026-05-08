"""
greenhub_bronze_to_silver.py

DAG-2 of 3: bronze fact 12 dim -> silver wide table.

Триггерится после успешного завершения greenhub_s3_to_bronze

Логика:
  Один task -> SPYT job через SSH -> vm1 -> spark-submit-yt
  Job делает overwrite //home/silver_stage/greenhub_telemetry
  State: events лог в S3 (lineage / runs history)

VARIABLES:
    GREENHUB_SSH_CONN_ID  vm1_ssh
    GREENHUB_AWS_CONN_ID  seaweedfs_s3
    GREENHUB_BRONZE_SILVER_LAUNCHER /home/chig_k3s/main_d/spyt/launch_bronze_to_silver.sh
    GREENHUB_SILVER_STATE_KEY  _state/greenhub_silver_runs.json
    GREENHUB_S3_BUCKET greenhub
    YT_PROXY  http://localhost:31103
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
LOADER_NAME = "spyt_bronze_to_silver"

ROW_IN_RE = re.compile(r"\[DONE\]\s+bronze fact in\s*:\s*([\d,]+)")
ROW_DEDUP_RE = re.compile(r"\[DONE\]\s+after dedup\s*:\s*([\d,]+)")
ROW_OUT_RE = re.compile(r"\[DONE\]\s+silver out\s*:\s*([\d,]+)")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json_from_s3(hook: S3Hook, bucket_name: str, key: str, default: Any) -> Any:
    if not hook.check_for_key(key=key, bucket_name=bucket_name):
        return default
    body = hook.get_key(key=key, bucket_name=bucket_name).get()["Body"].read()
    if not body:
        return default
    try:
        return json.loads(body.decode("utf-8"))
    except json.JSONDecodeError:
        return default


def save_json_to_s3(hook: S3Hook, bucket_name: str, key: str, data: Any) -> None:
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    hook.load_bytes(bytes_data=payload, bucket_name=bucket_name, key=key, replace=True)


def empty_silver_state() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset": DATASET_TAG,
        "loader": LOADER_NAME,
        "last_success": None,
        "events": [],
        "created_at": now_utc_iso(),
        "updated_at": now_utc_iso(),
    }


@dag(
    dag_id="greenhub_bronze_to_silver",
    description="YT bronze fact ⋈ 12 dim → silver wide (greenhub_telemetry)",
    schedule=timedelta(minutes=45),
    start_date=datetime(2026, 5, 7),
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
        bid = str(_uuid.uuid4())
        logger.info("batch_id = %s", bid)
        return bid

    @task(
        pool="spyt_pool",
        execution_timeout=timedelta(minutes=60),
        retries=1,
        retry_delay=timedelta(minutes=3),
    )
    def submit_bronze_to_silver(batch_id: str) -> dict[str, Any]:
        started_at = now_utc_iso()

        ssh_conn_id = Variable.get("GREENHUB_SSH_CONN_ID",            default_var="vm1_ssh")
        launcher = Variable.get(
            "GREENHUB_BRONZE_SILVER_LAUNCHER",
            default_var="/home/chig_k3s/main_d/spyt/launch_bronze_to_silver.sh",
        )
        yt_proxy    = Variable.get("YT_PROXY",                        default_var="http://localhost:31103")

        remote_cmd = (
            f"BATCH_ID={shlex.quote(batch_id)} "
            f"YT_PROXY={shlex.quote(yt_proxy)} "
            f"YT_USE_HOSTS=0 "
            f"{shlex.quote(launcher)}"
        )
        logger.info("SSH (%s): %s", ssh_conn_id, remote_cmd)

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
            "error": None,
        }

        try:
            ssh = SSHHook(ssh_conn_id=ssh_conn_id, conn_timeout=30)
            with ssh.get_conn() as conn:
                stdin, stdout, stderr = conn.exec_command(remote_cmd, timeout=3600)
                rc = stdout.channel.recv_exit_status()
                out_str = stdout.read().decode("utf-8", errors="replace")
                err_str = stderr.read().decode("utf-8", errors="replace")
        except Exception as exc:
            logger.exception("SSH/spark-submit transport error")
            return {
                **result,
                "load_status": "FAILED_TRANSPORT",
                "finished_at": now_utc_iso(),
                "error": repr(exc),
            }

        if (m := ROW_IN_RE.search(out_str)):
            result["rows_bronze_in"]   = int(m.group(1).replace(",", ""))
        if (m := ROW_DEDUP_RE.search(out_str)):
            result["rows_after_dedup"] = int(m.group(1).replace(",", ""))
        if (m := ROW_OUT_RE.search(out_str)):
            result["rows_silver_out"]  = int(m.group(1).replace(",", ""))

        logger.info("STDOUT tail (4kb):\n%s", out_str[-4096:] if out_str else "<empty>")

        if rc != 0:
            logger.error("STDERR tail (4kb):\n%s", err_str[-4096:] if err_str else "<empty>")
            return {
                **result,
                "load_status": "FAILED",
                "finished_at": now_utc_iso(),
                "rc": rc,
                "error": f"spark-submit-yt exit={rc}; stderr_tail={err_str[-1024:]}",
            }

        return {
            **result,
            "load_status": "SUCCESS",
            "finished_at": now_utc_iso(),
            "rc": rc,
            "error": None,
        }

    @task(trigger_rule="none_failed_min_one_success")
    def mark_silver_run(result: dict[str, Any]) -> None:
        if not result:
            logger.info("No silver run to record")
            return

        aws_conn  = Variable.get("GREENHUB_AWS_CONN_ID", default_var="seaweedfs_s3")
        bucket = Variable.get("GREENHUB_S3_BUCKET", default_var="greenhub")
        state_key = Variable.get(
            "GREENHUB_SILVER_STATE_KEY",
            default_var="_state/greenhub_silver_runs.json",
        )
        s3 = S3Hook(aws_conn_id=aws_conn)

        state = load_json_from_s3(s3, bucket, state_key, empty_silver_state())
        state.setdefault("events", [])

        state["events"].append({
            "event_at": now_utc_iso(),
            "batch_id": result.get("batch_id"),
            "load_status": result.get("load_status"),
            "rows_bronze_in": result.get("rows_bronze_in"),
            "rows_after_dedup": result.get("rows_after_dedup"),
            "rows_silver_out": result.get("rows_silver_out"),
            "started_at": result.get("started_at"),
            "finished_at": result.get("finished_at"),
            "rc": result.get("rc"),
            "error": result.get("error"),
        })

        if result.get("load_status") == "SUCCESS":
            state["last_success"] = {
                "batch_id": result.get("batch_id"),
                "finished_at": result.get("finished_at"),
                "rows_silver_out": result.get("rows_silver_out"),
            }

        state["updated_at"] = now_utc_iso()
        save_json_to_s3(s3, bucket, state_key, state)
        logger.info("Silver state %s/%s updated", bucket, state_key)


    batch_id = make_batch_id()
    result   = submit_bronze_to_silver(batch_id)
    mark_silver_run(result)


dag = greenhub_bronze_to_silver()
