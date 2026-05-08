"""
greenhub_silver_to_gold.py

silver wide -> gold marts (агрегаты для DataLens / CHYT).

Запускает SPYT job по одному на каждую витрину параллельно через
.expand(mart=...). Каждый mart живёт независимо: одна витрина сломалась —
остальные пишутся.

Витрины (spyt/jobs/silver_to_gold_greenhub.py):
  - daily_battery_health
  - device_lifecycle
  - hourly_network_usage
  - country_overview

VARIABLES:
    GREENHUB_SSH_CONN_ID  vm1_ssh
    GREENHUB_AWS_CONN_ID seaweedfs_s3
    GREENHUB_SILVER_GOLD_LAUNCHER  /home/chig_k3s/main_d/spyt/launch_silver_to_gold.sh
    GREENHUB_GOLD_MARTS   JSON: ["daily_battery_health","device_lifecycle",
                                "hourly_network_usage","country_overview"]
    GREENHUB_GOLD_STATE_KEY  _state/greenhub_gold_runs.json
    GREENHUB_S3_BUCKET   greenhub
    YT_PROXY   http://localhost:31103
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


DATASET_TAG  = "greenhub"
LOADER_NAME  = "spyt_silver_to_gold"

DEFAULT_MARTS = [
    "daily_battery_health",
    "device_lifecycle",
    "hourly_network_usage",
    "country_overview",
]

ROW_IN_RE = re.compile(r"\[DONE\]\s+silver in\s*:\s*([\d,]+)")
ROW_OUT_RE = re.compile(r"\[DONE\]\s+gold rows\s*:\s*([\d,]+)")


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


def empty_gold_state() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset": DATASET_TAG,
        "loader": LOADER_NAME,
        "marts": {},    
        "events": [],
        "created_at": now_utc_iso(),
        "updated_at": now_utc_iso(),
    }


@dag(
    dag_id="greenhub_silver_to_gold",
    description="YT silver -> gold marts (4 витрины параллельно через SPYT)",
    schedule=timedelta(hours=1),
    start_date=datetime(2026, 5, 7),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["greenhub", "spyt", "gold", "datalens", "diploma"],
)
def greenhub_silver_to_gold():

    @task
    def list_marts() -> list[str]:
        try:
            marts = Variable.get("GREENHUB_GOLD_MARTS", deserialize_json=True)
            if not isinstance(marts, list) or not all(isinstance(m, str) for m in marts):
                raise ValueError
            logger.info("Marts from variable: %s", marts)
            return marts
        except Exception:
            logger.info("Variable GREENHUB_GOLD_MARTS not set / invalid, using defaults: %s",
                        DEFAULT_MARTS)
            return list(DEFAULT_MARTS)

    @task
    def make_batch_id() -> str:
        bid = str(_uuid.uuid4())
        logger.info("batch_id = %s", bid)
        return bid

    @task(
        pool="spyt_pool",
        execution_timeout=timedelta(minutes=30),
        retries=1,
        retry_delay=timedelta(minutes=3),
    )
    def submit_mart(mart: str, batch_id: str) -> dict[str, Any]:
        started_at = now_utc_iso()

        ssh_conn_id = Variable.get("GREENHUB_SSH_CONN_ID", default_var="vm1_ssh")
        launcher = Variable.get(
            "GREENHUB_SILVER_GOLD_LAUNCHER",
            default_var="/home/chig_k3s/main_d/spyt/launch_silver_to_gold.sh",
        )
        yt_proxy    = Variable.get("YT_PROXY",  default_var="http://localhost:31103")

        remote_cmd = (
            f"BATCH_ID={shlex.quote(batch_id)} "
            f"YT_PROXY={shlex.quote(yt_proxy)} "
            f"YT_USE_HOSTS=0 "
            f"{shlex.quote(launcher)} {shlex.quote(mart)}"
        )
        logger.info("SSH (%s) mart=%s: %s", ssh_conn_id, mart, remote_cmd)

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
                stdin, stdout, stderr = conn.exec_command(remote_cmd, timeout=1800)
                rc = stdout.channel.recv_exit_status()
                out_str = stdout.read().decode("utf-8", errors="replace")
                err_str = stderr.read().decode("utf-8", errors="replace")
        except Exception as exc:
            logger.exception("SSH/spark-submit transport error for mart=%s", mart)
            return {
                **result,
                "load_status": "FAILED_TRANSPORT",
                "finished_at": now_utc_iso(),
                "error": repr(exc),
            }

        if (m := ROW_IN_RE.search(out_str)):
            result["rows_silver_in"] = int(m.group(1).replace(",", ""))
        if (m := ROW_OUT_RE.search(out_str)):
            result["rows_gold_out"]  = int(m.group(1).replace(",", ""))

        logger.info("[mart=%s] STDOUT tail (4kb):\n%s",
                    mart, out_str[-4096:] if out_str else "<empty>")

        if rc != 0:
            logger.error("[mart=%s] STDERR tail (4kb):\n%s",
                         mart, err_str[-4096:] if err_str else "<empty>")
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
    def mark_gold_runs(results: list[dict[str, Any]]) -> None:
        results = list(results) if results else []
        if not results:
            logger.info("No gold mart runs to record")
            return

        aws_conn = Variable.get("GREENHUB_AWS_CONN_ID", default_var="seaweedfs_s3")
        bucket = Variable.get("GREENHUB_S3_BUCKET", default_var="greenhub")
        state_key = Variable.get(
            "GREENHUB_GOLD_STATE_KEY",
            default_var="_state/greenhub_gold_runs.json",
        )
        s3 = S3Hook(aws_conn_id=aws_conn)

        state = load_json_from_s3(s3, bucket, state_key, empty_gold_state())
        state.setdefault("marts",  {})
        state.setdefault("events", [])

        for r in results:
            mart = r.get("mart")
            state["events"].append({
                "event_at": now_utc_iso(),
                "mart": mart,
                "batch_id": r.get("batch_id"),
                "load_status": r.get("load_status"),
                "rows_silver_in": r.get("rows_silver_in"),
                "rows_gold_out": r.get("rows_gold_out"),
                "started_at": r.get("started_at"),
                "finished_at": r.get("finished_at"),
                "rc": r.get("rc"),
                "error": r.get("error"),
            })
            if r.get("load_status") == "SUCCESS" and mart:
                state["marts"][mart] = {
                    "batch_id": r.get("batch_id"),
                    "finished_at": r.get("finished_at"),
                    "rows_gold_out": r.get("rows_gold_out"),
                }

        state["updated_at"] = now_utc_iso()
        save_json_to_s3(s3, bucket, state_key, state)
        logger.info("Gold state %s/%s updated: %d mart events", bucket, state_key, len(results))


    marts = list_marts()
    batch_id = make_batch_id()

    # Параллельный запуск 4 витрин 
    results = (
        submit_mart
        .partial(batch_id=batch_id)
        .expand(mart=marts)
    )

    mark_gold_runs(results)


dag = greenhub_silver_to_gold()
