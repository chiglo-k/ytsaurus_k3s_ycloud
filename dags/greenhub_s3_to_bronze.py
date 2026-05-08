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

#!/usr/bin/env python3
# greenhub_s3_to_bronze.py

from __future__ import annotations

import json
import shlex
import uuid
from datetime import datetime, timezone
from typing import Any

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ssh.hooks.ssh import SSHHook
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


def build_remote_command(candidate: dict, batch_id: str) -> str:
    s3_access_key = Variable.get("S3_ACCESS_KEY")
    s3_secret_key = Variable.get("S3_SECRET_KEY")
    yt_proxy = Variable.get("YT_PROXY", default_var=DEFAULT_YT_PROXY)

    s3a_path = candidate["s3a_path"]
    part_index = candidate["part_index"]

    remote_log = (
        f"{REMOTE_LOG_DIR}/raw_to_bronze_"
        f"{safe_name(batch_id)}_{safe_name(str(part_index))}.log"
    )

    return f"""
set -u

mkdir -p {q(REMOTE_LOG_DIR)}

export BATCH_ID={q(batch_id)}
export S3_ACCESS_KEY={q(s3_access_key)}
export S3_SECRET_KEY={q(s3_secret_key)}
export YT_PROXY={q(yt_proxy)}
export YT_USE_HOSTS=0

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
grep -n -Ei 'AnalysisException|Py4JJavaError|Exception|Caused by|YtError|Yt|Unsupported|schema|denied|exists|failed|Traceback|Error|Cannot' {q(remote_log)} | head -n 200 || true

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

            s3a_path = f"s3a://{S3_BUCKET}/{key}"

            candidates.append(
                {
                    "size": size,
                    "bucket": S3_BUCKET,
                    "prefix": S3_PREFIX,
                    "load_key": key,
                    "s3a_path": s3a_path,
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
                }
            )

            part_index += 1

        return candidates

    @task
    def submit_spark_job(candidate: dict, batch_id: str) -> dict:
        started_at = utc_now()
        command = build_remote_command(candidate, batch_id)

        secret = Variable.get("S3_SECRET_KEY")
        safe_command = command.replace(secret, "***")

        print(f"SSH ({SSH_CONN_ID}) submit command:")
        print(safe_command)

        hook = SSHHook(ssh_conn_id=SSH_CONN_ID)
        client = hook.get_conn()

        stdin, stdout, stderr = client.exec_command(command, get_pty=True)

        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        rc = stdout.channel.recv_exit_status()

        print("========== REMOTE STDOUT ==========")
        print(out[-30000:] if out else "<empty>")

        print("========== REMOTE STDERR ==========")
        print(err[-30000:] if err else "<empty>")

        finished_at = utc_now()

        result = {
            **candidate,
            "batch_id": batch_id,
            "loader": "spyt_raw_to_bronze",
            "started_at": started_at,
            "finished_at": finished_at,
            "load_status": "SUCCESS" if rc == 0 else "FAILED",
            "rows_inserted": None,
            "device_count": None,
            "rc": rc,
            "error": None if rc == 0 else f"spark-submit-yt exit={rc}",
        }

        if rc != 0:
            raise RuntimeError(
                f"SPYT raw_to_bronze failed with rc={rc}. "
                f"Check REMOTE STDOUT above; it includes ERROR GREP and full remote log tail."
            )

        return result

    @task
    def mark_done(results: list[dict]) -> dict:
        state = load_json_from_s3(S3_BUCKET, STATE_KEY, default={})

        loaded_etags = set(state.get("loaded_etags", []))
        loaded_files = state.get("loaded_files", [])

        for item in results:
            if item.get("load_status") != "SUCCESS":
                continue

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

        new_state = {
            **state,
            "source_name": SOURCE_NAME,
            "bucket": S3_BUCKET,
            "updated_at": utc_now(),
            "loaded_etags": sorted(loaded_etags),
            "loaded_files": loaded_files,
        }

        save_json_to_s3(S3_BUCKET, STATE_KEY, new_state)

        return {
            "loaded_count": len(results),
            "state_key": STATE_KEY,
        }

    batch_id = make_batch_id()
    candidates = list_load_candidates(batch_id)
    results = submit_spark_job.expand(candidate=candidates, batch_id=[batch_id])
    mark_done(results)


greenhub_s3_to_bronze()
