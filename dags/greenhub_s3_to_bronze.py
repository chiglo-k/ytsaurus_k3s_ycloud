"""
greenhub_s3_to_bronze.py

parquet из S3 (SeaweedFS) -> SPYT job -> YT bronze (fact + 12 dim).

  sources_map в Airflow Variable
  state как JSON в S3 (files + events)
  detection через etag (skip если SUCCESS && source_etag совпал)
  dynamic mapping .expand(item=...) - один task на файл
  pool spyt_pool (размер = количеству SPYT workers в cluster)
  orchestrator -> executor через SSHHook -> vm1 -> spark-submit-yt -> SPYT

VARIABLES:
    GREENHUB_S3_SOURCES        JSON: {"greenhub": {"bucket": "greenhub", "prefix": "",
                                                   "state_key": "_state/greenhub_load.json"}}
    GREENHUB_AWS_CONN_ID       seaweedfs_s3
    GREENHUB_SSH_CONN_ID       vm1_ssh
    GREENHUB_SPARK_LAUNCHER    /home/chig_k3s/main_d/spyt/launch_raw_to_bronze.sh
    S3_ACCESS_KEY              <SeaweedFS access>
    S3_SECRET_KEY              <SeaweedFS secret>
    YT_PROXY                   http://localhost:31103

CONNECTIONS:
    seaweedfs_s3   AWS-type, endpoint=http://10.130.0.35:8333, key/secret
    vm1_ssh        SSH-type, host=<vm1 internal IP>, login=chig_k3s,
                   Extra: {"private_key": "...", "no_host_key_check": true}

POOL:
    spyt_pool      slots=1 
"""

from __future__ import annotations

import io
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


SOURCES_VARIABLE_KEY = "GREENHUB_S3_SOURCES"
DATASET_TAG = "greenhub"
LOADER_NAME = "spyt_raw_to_bronze"

ROW_COUNT_RE = re.compile(r"\[DONE\]\s+fact rows\s*:\s*([\d,]+)")
DEVICE_COUNT_RE = re.compile(r"\[DONE\]\s+dim_device\s*:\s*([\d,]+)")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_prefix(prefix: str | None) -> str:
    if not prefix:
        return ""
    return prefix.strip().strip("/")


def make_state_key(prefix: str) -> str:
    base = "_state"
    if not prefix:
        return f"{base}/{DATASET_TAG}_load.json"
    return f"{prefix}/{base}/{DATASET_TAG}_load.json"


def empty_state(source_name: str, bucket: str, prefix: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "source_name": source_name,
        "bucket": bucket,
        "prefix": prefix,
        "dataset": DATASET_TAG,
        "loader": LOADER_NAME,
        "files": {},
        "events": [],
        "created_at": now_utc_iso(),
        "updated_at": now_utc_iso(),
    }


def load_json_from_s3(hook: S3Hook, bucket_name: str, key: str, default: Any) -> Any:
    if not hook.check_for_key(key=key, bucket_name=bucket_name):
        logger.info("State %s/%s not found, using default", bucket_name, key)
        return default
    body = hook.get_key(key=key, bucket_name=bucket_name).get()["Body"].read()
    if not body:
        return default
    try:
        return json.loads(body.decode("utf-8"))
    except json.JSONDecodeError:
        logger.exception("Failed to decode %s/%s, using default", bucket_name, key)
        return default


def save_json_to_s3(hook: S3Hook, bucket_name: str, key: str, data: Any) -> None:
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    hook.load_bytes(bytes_data=payload, bucket_name=bucket_name, key=key, replace=True)


def is_successfully_loaded(state: dict[str, Any], load_key: str, etag: str | None) -> bool:
    item = state.get("files", {}).get(load_key)
    if not item:
        return False
    return item.get("load_status") == "SUCCESS" and item.get("source_etag") == etag


def iter_parquet_candidates(
    s3: S3Hook,
    source_name: str,
    source: dict[str, Any],
    state: dict[str, Any],
) -> list[dict[str, Any]]:
    bucket = source["bucket"]
    prefix = normalize_prefix(source.get("prefix") or "")
    state_key = source.get("state_key") or make_state_key(prefix)

    keys = s3.list_keys(bucket_name=bucket, prefix=prefix or None) or []
    keys = [k for k in keys if k.endswith(".parquet")]

    out: list[dict[str, Any]] = []
    for k in sorted(keys):
        obj = s3.get_key(key=k, bucket_name=bucket)
        etag = (obj.e_tag or "").strip('"')
        size = obj.content_length

        if is_successfully_loaded(state, k, etag):
            continue

        m = re.search(r"part-(\d+)\.parquet", k)
        part_index = int(m.group(1)) if m else 0

        out.append({
            "source_name": source_name,
            "bucket":      bucket,
            "prefix":      prefix,
            "state_key":   state_key,
            "load_key":    k,
            "source_etag": etag,
            "size":        size,
            "part_index":  part_index,
            "s3a_path":    f"s3a://{bucket}/{k}",
        })

    return out


@dag(
    dag_id="greenhub_s3_to_bronze",
    description="S3 (SeaweedFS) parquet -> SPYT submit -> YT bronze (fact + 12 dim)",
    schedule=timedelta(minutes=30),
    start_date=datetime(2026, 5, 7),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "ytsaurus",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["greenhub", "spyt", "s3", "bronze", "diploma"],
)
def greenhub_s3_to_bronze():

    @task
    def list_load_candidates() -> list[dict[str, Any]]:
        aws_conn = Variable.get("GREENHUB_AWS_CONN_ID", default_var="seaweedfs_s3")
        s3 = S3Hook(aws_conn_id=aws_conn)

        sources_map = Variable.get(SOURCES_VARIABLE_KEY, deserialize_json=True)
        if not isinstance(sources_map, dict):
            raise ValueError(f"Airflow Variable {SOURCES_VARIABLE_KEY} must be a JSON object")

        all_candidates: list[dict[str, Any]] = []

        for source_name, source in sources_map.items():
            if not isinstance(source, dict):
                raise ValueError(f"Source {source_name} config must be a JSON object")

            bucket = source["bucket"]
            prefix = normalize_prefix(source.get("prefix") or "")
            state_key = source.get("state_key") or make_state_key(prefix)

            state = load_json_from_s3(
                hook=s3,
                bucket_name=bucket,
                key=state_key,
                default=empty_state(source_name, bucket, prefix),
            )

            candidates = iter_parquet_candidates(
                s3=s3,
                source_name=source_name,
                source={**source, "bucket": bucket, "prefix": prefix, "state_key": state_key},
                state=state,
            )
            logger.info("Source %s: %d new/changed parquet candidates",
                        source_name, len(candidates))
            all_candidates.extend(candidates)

        logger.info("Total candidates to load: %d", len(all_candidates))
        return all_candidates

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
    def submit_spark_job(item: dict[str, Any], batch_id: str) -> dict[str, Any]:
        """SSH -> vm1 -> launcher.sh -> spark-submit-yt -> SPYT cluster."""
        started_at = now_utc_iso()

        base_result: dict[str, Any] = {
            **item,
            "batch_id": batch_id,
            "loader": LOADER_NAME,
            "started_at": started_at,
            "finished_at": None,
            "load_status": None,
            "rows_inserted": None,
            "device_count": None,
            "rc": None,
            "error": None,
        }

        ssh_conn_id = Variable.get("GREENHUB_SSH_CONN_ID", default_var="vm1_ssh")
        launcher  = Variable.get("GREENHUB_SPARK_LAUNCHER",
                                   default_var="/home/chig_k3s/main_d/spyt/launch_raw_to_bronze.sh")
        s3_access = Variable.get("S3_ACCESS_KEY")
        s3_secret = Variable.get("S3_SECRET_KEY")
        yt_proxy = Variable.get("YT_PROXY", default_var="http://localhost:31103")

        remote_cmd = (
            f"BATCH_ID={shlex.quote(batch_id)} "
            f"S3_ACCESS_KEY={shlex.quote(s3_access)} "
            f"S3_SECRET_KEY={shlex.quote(s3_secret)} "
            f"YT_PROXY={shlex.quote(yt_proxy)} "
            f"YT_USE_HOSTS=0 "
            f"{shlex.quote(launcher)} "
            f"{shlex.quote(item['s3a_path'])} {int(item['part_index'])}"
        )

        logger.info("SSH (%s): %s", ssh_conn_id, remote_cmd[:200])

        try:
            ssh = SSHHook(ssh_conn_id=ssh_conn_id, conn_timeout=30)
            with ssh.get_conn() as conn:
                stdin, stdout, stderr = conn.exec_command(remote_cmd, timeout=3600)
                rc = stdout.channel.recv_exit_status()
                out_str = stdout.read().decode("utf-8", errors="replace")
                err_str = stderr.read().decode("utf-8", errors="replace")
        except Exception as exc:
            logger.exception("SSH/spark-submit transport error for %s", item["load_key"])
            return {
                **base_result,
                "load_status": "FAILED_TRANSPORT",
                "finished_at": now_utc_iso(),
                "error": repr(exc),
            }

        rows_inserted = None
        device_count  = None
        if (m := ROW_COUNT_RE.search(out_str)):
            rows_inserted = int(m.group(1).replace(",", ""))
        if (m := DEVICE_COUNT_RE.search(out_str)):
            device_count  = int(m.group(1).replace(",", ""))

        logger.info("STDOUT tail (4kb):\n%s", out_str[-4096:] if out_str else "<empty>")

        if rc != 0:
            logger.error("STDERR tail (4kb):\n%s", err_str[-4096:] if err_str else "<empty>")
            return {
                **base_result,
                "load_status": "FAILED",
                "finished_at": now_utc_iso(),
                "rc": rc,
                "rows_inserted": rows_inserted,
                "device_count": device_count,
                "error": f"spark-submit-yt exit={rc}; stderr_tail={err_str[-1024:]}",
            }

        return {
            **base_result,
            "load_status": "SUCCESS",
            "finished_at": now_utc_iso(),
            "rc": rc,
            "rows_inserted": rows_inserted,
            "device_count": device_count,
            "error": None,
        }

    @task(trigger_rule="none_failed_min_one_success")
    def mark_done(results: list[dict[str, Any]]) -> None:
        results = list(results) if results else []
        if not results:
            logger.info("No spark results to record")
            return

        aws_conn = Variable.get("GREENHUB_AWS_CONN_ID", default_var="seaweedfs_s3")
        s3 = S3Hook(aws_conn_id=aws_conn)

        grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
        for item in results:
            key = (item["bucket"], item["source_name"], item.get("prefix") or "", item["state_key"])
            grouped.setdefault(key, []).append(item)

        for (bucket, source_name, prefix, state_key), group in grouped.items():
            state = load_json_from_s3(
                hook=s3,
                bucket_name=bucket,
                key=state_key,
                default=empty_state(source_name, bucket, prefix),
            )
            state.setdefault("schema_version", 1)
            state.setdefault("source_name",    source_name)
            state.setdefault("bucket",         bucket)
            state.setdefault("prefix",         prefix)
            state.setdefault("dataset",        DATASET_TAG)
            state.setdefault("loader",         LOADER_NAME)
            state.setdefault("files",          {})
            state.setdefault("events",         [])
            state.setdefault("created_at",     now_utc_iso())

            for item in group:
                load_key = item["load_key"]
                state["files"][load_key] = {
                    "load_key": load_key,
                    "load_status": item.get("load_status"),
                    "loader": item.get("loader"),
                    "source_etag": item.get("source_etag"),
                    "size": item.get("size"),
                    "part_index": item.get("part_index"),
                    "started_at": item.get("started_at"),
                    "finished_at": item.get("finished_at"),
                    "batch_id": item.get("batch_id"),
                    "rows_inserted": item.get("rows_inserted"),
                    "device_count": item.get("device_count"),
                    "rc": item.get("rc"),
                    "error": item.get("error"),
                }
                state["events"].append({
                    "event_at": now_utc_iso(),
                    "load_key": load_key,
                    "load_status": item.get("load_status"),
                    "loader": item.get("loader"),
                    "source_etag": item.get("source_etag"),
                    "batch_id": item.get("batch_id"),
                    "rows_inserted": item.get("rows_inserted"),
                    "error": item.get("error"),
                })

            state["updated_at"] = now_utc_iso()
            save_json_to_s3(hook=s3, bucket_name=bucket, key=state_key, data=state)
            logger.info("State %s/%s: +%d files", bucket, state_key, len(group))


    candidates = list_load_candidates()
    batch_id = make_batch_id()

    results = (
        submit_spark_job
        .partial(batch_id=batch_id)
        .expand(item=candidates)
    )

    mark_done(results)


dag = greenhub_s3_to_bronze()
