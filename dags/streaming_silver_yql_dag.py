#!/usr/bin/env python3
from __future__ import annotations

import logging
import shlex
import uuid as _uuid
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    from airflow.sdk import dag, task, Variable
except ImportError:
    from airflow.decorators import dag, task
    from airflow.models import Variable

from airflow.providers.ssh.hooks.ssh import SSHHook


logger = logging.getLogger(__name__)

DAG_ID = "streaming_silver_yql"
DATASET_TAG = "jsonplaceholder_streaming"
LOADER_NAME = "yql_streaming_silver"

REMOTE_REPO_ROOT = "/home/chig_k3s/git/repo"
REMOTE_SCRIPT = f"{REMOTE_REPO_ROOT}/scripts/run_yql_with_yt_log.sh"
REMOTE_SQL_DIR = f"{REMOTE_REPO_ROOT}/sql/streaming_silver"

REMOTE_LOG_DIR = "/home/chig_k3s/main_d/logs/airflow_streaming_silver"

STREAMING_TABLES = [
    {
        "task_name": "streaming_posts",
        "sql_file": f"{REMOTE_SQL_DIR}/streaming_posts.sql",
        "target_table": "//home/silver_stage/streaming_posts",
    },
    {
        "task_name": "streaming_comments",
        "sql_file": f"{REMOTE_SQL_DIR}/streaming_comments.sql",
        "target_table": "//home/silver_stage/streaming_comments",
    },
    {
        "task_name": "streaming_todos",
        "sql_file": f"{REMOTE_SQL_DIR}/streaming_todos.sql",
        "target_table": "//home/silver_stage/streaming_todos",
    },
]


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def q(value: Any) -> str:
    return shlex.quote("" if value is None else str(value))


def safe_name(value: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in str(value))


def build_remote_command(item: dict[str, str], batch_id: str) -> str:
    launcher = Variable.get(
        "STREAMING_SILVER_YQL_LAUNCHER",
        default=REMOTE_SCRIPT,
    )
    yt_proxy = Variable.get("YT_PROXY", default="http://localhost:31103")

    task_name = item["task_name"]
    sql_file = item["sql_file"]
    target_table = item["target_table"]

    remote_log = (
        f"{REMOTE_LOG_DIR}/"
        f"streaming_silver_{safe_name(task_name)}_{safe_name(batch_id)}.log"
    )

    return f"""
set -u

mkdir -p {q(REMOTE_LOG_DIR)}

export BATCH_ID={q(batch_id)}
export RUN_ID={q(batch_id)}
export DAG_ID={q(DAG_ID)}
export TASK_ID={q(task_name)}
export SQL_FILE={q(sql_file)}
export TARGET_TABLE={q(target_table)}

export YT_PROXY={q(yt_proxy)}
export YT_USE_HOSTS=0
export PATH="/home/chig_k3s/yt-env/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

echo "[AIRFLOW] host=$(hostname)"
echo "[AIRFLOW] started_at=$(date -Is)"
echo "[AIRFLOW] dag_id={DAG_ID}"
echo "[AIRFLOW] task_name={task_name}"
echo "[AIRFLOW] batch_id={batch_id}"
echo "[AIRFLOW] launcher={launcher}"
echo "[AIRFLOW] sql_file={sql_file}"
echo "[AIRFLOW] target_table={target_table}"
echo "[AIRFLOW] yt_proxy=$YT_PROXY"
echo "[AIRFLOW] full_log={remote_log}"

echo ""
echo "CHECKS"
whoami || true
pwd || true
command -v yt || true
yt --version || true
ls -lah {q(launcher)}
ls -lah {q(sql_file)}
test -f {q(sql_file)}
test -f {q(launcher)}

set +e

bash {q(launcher)} > {q(remote_log)} 2>&1

RC=$?

echo "[AIRFLOW] yql_rc=$RC"
echo "[AIRFLOW] finished_at=$(date -Is)"
echo "[AIRFLOW] full_log={remote_log}"

echo ""
echo "ERROR GREP"
grep -n -Ei 'Exception|Traceback|Error|failed|FAILED|denied|Cannot|YtError|YQL|syntax|Type annotation|not found|No such file|permission|timeout|aborted' {q(remote_log)} | head -n 240 || true

echo ""
echo "DONE GREP"
grep -n -Ei '\\[DONE\\]|SUCCESS|completed|insert|rows|target|silver|query' {q(remote_log)} | head -n 240 || true

echo ""
echo "FULL LOG TAIL 300"
tail -n 300 {q(remote_log)} || true

exit $RC
""".strip()


@dag(
    dag_id=DAG_ID,
    description="Streaming bronze -> silver через YQL в YTsaurus",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 5, 10, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["streaming", "ytsaurus", "yql", "silver", "diploma"],
)
def streaming_silver_yql():

    @task
    def make_batch_id() -> str:
        batch_id = str(_uuid.uuid4())
        logger.info("batch_id=%s", batch_id)
        return batch_id

    @task
    def list_streaming_tables() -> list[dict[str, str]]:
        return STREAMING_TABLES

    @task(
        execution_timeout=timedelta(minutes=20),
        retries=1,
        retry_delay=timedelta(minutes=2),
    )
    def submit_streaming_silver(item: dict[str, str], batch_id: str) -> dict[str, Any]:
        started_at = now_utc_iso()

        ssh_conn_id = Variable.get("GREENHUB_SSH_CONN_ID", default="vm1_ssh")
        command = build_remote_command(item, batch_id)

        task_name = item["task_name"]
        target_table = item["target_table"]

        logger.info("SSH (%s) streaming task=%s command:\n%s", ssh_conn_id, task_name, command)

        result: dict[str, Any] = {
            "dataset": DATASET_TAG,
            "loader": LOADER_NAME,
            "task_name": task_name,
            "batch_id": batch_id,
            "target_table": target_table,
            "started_at": started_at,
            "finished_at": None,
            "load_status": None,
            "rc": None,
            "error": None,
        }

        try:
            ssh = SSHHook(ssh_conn_id=ssh_conn_id, conn_timeout=30)
            with ssh.get_conn() as conn:
                stdin, stdout, stderr = conn.exec_command(command, get_pty=True, timeout=1200)
                out_str = stdout.read().decode("utf-8", errors="replace")
                err_str = stderr.read().decode("utf-8", errors="replace")
                rc = stdout.channel.recv_exit_status()
        except Exception as exc:
            logger.exception("SSH/YQL transport error for task=%s", task_name)
            return {
                **result,
                "load_status": "FAILED_TRANSPORT",
                "finished_at": now_utc_iso(),
                "error": repr(exc),
            }

        print(f"REMOTE STDOUT task={task_name}")
        print(out_str[-30000:] if out_str else "<empty>")

        print(f"REMOTE STDERR task={task_name}")
        print(err_str[-30000:] if err_str else "<empty>")

        finished_at = now_utc_iso()

        if rc != 0:
            raise RuntimeError(
                f"YQL streaming silver task={task_name} failed with rc={rc}. "
                f"Check REMOTE STDOUT above; it includes ERROR GREP and remote log tail."
            )

        return {
            **result,
            "load_status": "SUCCESS",
            "finished_at": finished_at,
            "rc": rc,
            "error": None,
        }

    @task(trigger_rule="none_failed_min_one_success")
    def print_summary(results: list[dict[str, Any]]) -> None:
        for item in results:
            logger.info(
                "task=%s status=%s target=%s batch_id=%s rc=%s",
                item.get("task_name"),
                item.get("load_status"),
                item.get("target_table"),
                item.get("batch_id"),
                item.get("rc"),
            )

    batch_id = make_batch_id()
    items = list_streaming_tables()
    results = submit_streaming_silver.partial(batch_id=batch_id).expand(item=items)
    print_summary(results)


dag = streaming_silver_yql()