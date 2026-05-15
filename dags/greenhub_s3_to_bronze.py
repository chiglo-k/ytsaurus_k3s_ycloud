#!/usr/bin/env python3

from __future__ import annotations

import json
import re
import shlex
import uuid
from datetime import datetime, timezone
from typing import Any

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ssh.hooks.ssh import SSHHook

try:
    from airflow.operators.python import get_current_context
except ImportError: 
    def get_current_context() -> dict:
        return {}
from botocore.exceptions import ClientError


DAG_ID = "greenhub_s3_to_bronze"

AWS_CONN_ID = "seaweedfs_s3"
SSH_CONN_ID = "vm1_ssh"

SOURCE_NAME = "greenhub"
S3_BUCKET = "greenhub"
S3_PREFIX = ""

STATE_KEY = "_state/greenhub_load.json"

SPYT_LAUNCHER = "/home/chig_k3s/git/repo/spyt/launch_raw_to_bronze.sh"
REMOTE_LOG_DIR = "/home/chig_k3s/git/spyt-logs"

DEFAULT_YT_PROXY = "http://localhost:31103"
OPS_LOG_ROOT = "//home/ops_logs/greenhub"
S3_LOG_PREFIX = "_logs/greenhub"

SOURCE_ROWS_RE = re.compile(r"\[DONE\]\s+source rows\s*:\s*([\d,]+)")
FACT_ROWS_RE = re.compile(r"\[DONE\]\s+fact rows\s*:\s*([\d,]+)")
DIM_DEVICE_RE = re.compile(r"\[DONE\]\s+dim_device\s*:\s*([\d,]+)")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def q(value: Any) -> str:
    return shlex.quote("" if value is None else str(value))


def safe_name(value: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in str(value))


def get_s3_hook() -> S3Hook:
    return S3Hook(aws_conn_id=AWS_CONN_ID)


def load_json_from_s3(bucket: str, key: str, default: dict | None = None) -> dict:
    hook = get_s3_hook()

    try:
        if not hook.check_for_key(key=key, bucket_name=bucket):
            return default or {}
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return default or {}
        raise

    obj = hook.get_key(key=key, bucket_name=bucket)
    if obj is None:
        return default or {}

    body = obj.get()["Body"].read().decode("utf-8")
    if not body.strip():
        return default or {}

    return json.loads(body)


def save_json_to_s3(bucket: str, key: str, payload: dict) -> None:
    hook = get_s3_hook()
    hook.load_string(
        string_data=json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        key=key,
        bucket_name=bucket,
        replace=True,
    )


def parse_done_int(pattern: re.Pattern[str], text: str) -> int | None:
    match = pattern.search(text or "")
    if not match:
        return None
    return int(match.group(1).replace(",", ""))


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


def raw_remote_log_path(batch_id: str, part_index: int | str) -> str:
    return (
        f"{REMOTE_LOG_DIR}/raw_to_bronze_"
        f"{safe_name(batch_id)}_{safe_name(str(part_index))}.log"
    )


def current_run_id() -> str | None:
    try:
        context = get_current_context()
    except Exception:
        return None
    return context.get("run_id")


def file_load_log_row(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_at": utc_now(),
        "dag_id": DAG_ID,
        "run_id": current_run_id(),
        "batch_id": item.get("batch_id"),
        "source_system": item.get("source_name") or SOURCE_NAME,
        "bucket": item.get("bucket"),
        "prefix": item.get("prefix"),
        "file_key": item.get("load_key"),
        "s3a_path": item.get("s3a_path"),
        "etag": item.get("source_etag"),
        "file_size": item.get("size"),
        "part_index": item.get("part_index"),
        "loader": item.get("loader"),
        "target_layer": "bronze",
        "target_tables": "//home/bronze_stage/greenhub/fact_telemetry,//home/bronze_stage/greenhub/dim_*",
        "load_status": item.get("load_status"),
        "started_at": item.get("started_at"),
        "finished_at": item.get("finished_at"),
        "duration_sec": duration_seconds(item.get("started_at"), item.get("finished_at")),
        "rows_inserted": item.get("rows_inserted"),
        "source_rows": item.get("source_rows"),
        "fact_rows": item.get("fact_rows"),
        "device_count": item.get("device_count"),
        "rc": item.get("rc"),
        "error": item.get("error"),
        "remote_log": item.get("remote_log"),
        "message": item.get("message"),
    }


def save_run_json_log_to_s3(category: str, batch_id: str, payload: dict[str, Any]) -> str:
    key = f"{S3_LOG_PREFIX}/{category}/{safe_name(batch_id)}.json"
    save_json_to_s3(S3_BUCKET, key, payload)
    return key


def write_json_rows_to_yt(table_path: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    yt_proxy = Variable.get("YT_PROXY", default_var=DEFAULT_YT_PROXY)
    ssh_conn_id = Variable.get("GREENHUB_SSH_CONN_ID", default_var=SSH_CONN_ID)
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


def build_remote_command(candidate: dict, batch_id: str) -> str:
    s3_access_key = Variable.get("S3_ACCESS_KEY")
    s3_secret_key = Variable.get("S3_SECRET_KEY")
    yt_proxy = Variable.get("YT_PROXY", default_var=DEFAULT_YT_PROXY)

    s3a_path = candidate["s3a_path"]
    part_index = candidate["part_index"]

    remote_log = raw_remote_log_path(batch_id, part_index)

    return f"""
set -u

mkdir -p {q(REMOTE_LOG_DIR)}

export BATCH_ID={q(batch_id)}
export S3_ACCESS_KEY={q(s3_access_key)}
export S3_SECRET_KEY={q(s3_secret_key)}
export YT_PROXY={q(yt_proxy)}
export YT_USE_HOSTS=0
export DRIVER_HOST=10.130.0.24
export SPARK_LOCAL_IP=10.130.0.24

echo "[AIRFLOW] host=$(hostname)"
echo "[AIRFLOW] started_at=$(date -Is)"
echo "[AIRFLOW] launcher={SPYT_LAUNCHER}"
echo "[AIRFLOW] input={s3a_path}"
echo "[AIRFLOW] part_index={part_index}"
echo "[AIRFLOW] full_log={remote_log}"

set +e

{q(SPYT_LAUNCHER)} {q(s3a_path)} {q(part_index)} > {q(remote_log)} 2>&1

RC=$?

echo "[AIRFLOW] spark_rc=$RC"
echo "[AIRFLOW] finished_at=$(date -Is)"
echo "[AIRFLOW] full_log={remote_log}"

echo ""
echo "========== ERROR GREP =========="
grep -n -Ei 'AnalysisException|Py4JJavaError|Exception|Caused by|YtError|Yt|Unsupported|schema|denied|exists|failed|Traceback|Error|Cannot|Initial job has not accepted|Lost executor|Disconnected' {q(remote_log)} | head -n 240 || true

echo ""
echo "========== FULL LOG TAIL 300 =========="
tail -n 300 {q(remote_log)} || true

exit $RC
""".strip()


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["greenhub", "s3", "spyt", "bronze"],
)
def greenhub_s3_to_bronze():
    @task
    def make_batch_id() -> str:
        return str(uuid.uuid4())

    @task
    def list_load_candidates(batch_id: str) -> list[dict]:
        hook = get_s3_hook()

        state = load_json_from_s3(S3_BUCKET, STATE_KEY, default={})
        loaded_etags = set(state.get("loaded_etags", []))

        keys = hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX) or []

        candidates: list[dict] = []
        part_index = 0

        for key in sorted(keys):
            if not key.endswith(".parquet"):
                continue

            obj = hook.get_key(key=key, bucket_name=S3_BUCKET)
            if obj is None:
                continue

            head = obj.meta.client.head_object(Bucket=S3_BUCKET, Key=key)
            etag = head.get("ETag", "").strip('"')
            size = int(head.get("ContentLength", 0))

            if etag and etag in loaded_etags:
                continue

            candidates.append(
                {
                    "size": size,
                    "bucket": S3_BUCKET,
                    "prefix": S3_PREFIX,
                    "load_key": key,
                    "s3a_path": f"s3a://{S3_BUCKET}/{key}",
                    "state_key": STATE_KEY,
                    "part_index": part_index,
                    "source_etag": etag,
                    "source_name": SOURCE_NAME,
                    "batch_id": batch_id,
                    "loader": "spyt_raw_to_bronze",
                    "started_at": None,
                    "finished_at": None,
                    "load_status": "PENDING",
                    "rows_inserted": None,
                    "device_count": None,
                    "rc": None,
                    "error": None,
                    "remote_log": raw_remote_log_path(batch_id, part_index),
                }
            )

            part_index += 1

        return candidates

    @task(pool="spyt_pool")
    def submit_spark_job(candidate: dict, batch_id: str) -> dict:
        started_at = utc_now()
        command = build_remote_command(candidate, batch_id)

        secret = Variable.get("S3_SECRET_KEY")
        safe_command = command.replace(secret, "***")

        print(f"SSH ({SSH_CONN_ID}) submit command:")
        print(safe_command)

        rc: int | None = None
        out = ""
        err = ""
        error: str | None = None

        try:
            hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
            client = hook.get_conn()
            stdin, stdout, stderr = client.exec_command(command, get_pty=True)

            out = stdout.read().decode("utf-8", errors="replace")
            err = stderr.read().decode("utf-8", errors="replace")
            rc = stdout.channel.recv_exit_status()
        except Exception as exc:
            error = repr(exc)

        print("========== REMOTE STDOUT ==========")
        print(out[-30000:] if out else "<empty>")

        print("========== REMOTE STDERR ==========")
        print(err[-30000:] if err else "<empty>")

        finished_at = utc_now()
        if error is None and rc not in (0, None):
            error = f"spark-submit-yt exit={rc}"

        fact_rows = parse_done_int(FACT_ROWS_RE, out)

        return {
            **candidate,
            "batch_id": batch_id,
            "loader": "spyt_raw_to_bronze",
            "started_at": started_at,
            "finished_at": finished_at,
            "load_status": "SUCCESS" if rc == 0 else "FAILED",
            "rows_inserted": fact_rows,
            "source_rows": parse_done_int(SOURCE_ROWS_RE, out),
            "fact_rows": fact_rows,
            "device_count": parse_done_int(DIM_DEVICE_RE, out),
            "rc": rc,
            "error": error,
            "remote_log": candidate.get("remote_log") or raw_remote_log_path(batch_id, candidate.get("part_index", 0)),
            "message": (out or err)[-20000:] if (out or err) else error,
        }

    @task(trigger_rule="all_done")
    def mark_done(results: list[dict]) -> dict:
        results = results or []
        state = load_json_from_s3(S3_BUCKET, STATE_KEY, default={})

        loaded_etags = set(state.get("loaded_etags", []))
        loaded_files = state.get("loaded_files", [])
        success_items = 0
        failed_items = 0

        for item in results:
            if item.get("load_status") == "SUCCESS":
                success_items += 1
                etag = item.get("source_etag")
                if etag:
                    loaded_etags.add(etag)

                loaded_files.append(
                    {
                        "bucket": item.get("bucket"),
                        "key": item.get("load_key"),
                        "etag": item.get("source_etag"),
                        "size": item.get("size"),
                        "batch_id": item.get("batch_id"),
                        "part_index": item.get("part_index"),
                        "loaded_at": utc_now(),
                    }
                )
            else:
                failed_items += 1

        new_state = {
            **state,
            "source_name": SOURCE_NAME,
            "bucket": S3_BUCKET,
            "updated_at": utc_now(),
            "loaded_etags": sorted(loaded_etags),
            "loaded_files": loaded_files,
        }

        save_json_to_s3(S3_BUCKET, STATE_KEY, new_state)

        file_rows = [file_load_log_row(item) for item in results]
        dag_row = {
            "event_at": utc_now(),
            "dag_id": DAG_ID,
            "run_id": current_run_id(),
            "batch_id": results[0].get("batch_id") if results else None,
            "source_system": SOURCE_NAME,
            "loader": "spyt_raw_to_bronze",
            "started_at": min((r.get("started_at") for r in results if r.get("started_at")), default=None),
            "finished_at": max((r.get("finished_at") for r in results if r.get("finished_at")), default=None),
            "duration_sec": None,
            "status": "SUCCESS" if failed_items == 0 else "FAILED",
            "total_items": len(results),
            "success_items": success_items,
            "failed_items": failed_items,
            "skipped_items": 0,
            "error": None if failed_items == 0 else f"{failed_items} file load(s) failed",
            "message": f"state_key={STATE_KEY}",
        }
        dag_row["duration_sec"] = duration_seconds(dag_row["started_at"], dag_row["finished_at"])

        log_key = save_run_json_log_to_s3(
            "file_loads",
            dag_row.get("batch_id") or uuid.uuid4().hex,
            {"file_loads": file_rows, "dag_run": dag_row},
        )
        dag_row["message"] = f"state_key={STATE_KEY}; s3_log_key={log_key}"

        write_json_rows_to_yt(f"{OPS_LOG_ROOT}/file_loads", file_rows)
        write_json_rows_to_yt(f"{OPS_LOG_ROOT}/dag_runs", [dag_row])

        if failed_items:
            raise RuntimeError(f"SPYT raw_to_bronze failed for {failed_items} file(s). Logs were written to S3 and YTsaurus.")

        return {
            "loaded_count": success_items,
            "state_key": STATE_KEY,
            "s3_log_key": log_key,
        }

    batch_id = make_batch_id()
    candidates = list_load_candidates(batch_id)
    results = submit_spark_job.partial(batch_id=batch_id).expand(candidate=candidates)
    mark_done(results)


greenhub_s3_to_bronze()
