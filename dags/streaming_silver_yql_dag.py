from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


DAG_ID = "streaming_silver_yql"

REPO_ROOT = "/opt/airflow/dags/repo"

SCRIPT = f"{REPO_ROOT}/scripts/run_yql_with_yt_log.sh"
SQL_DIR = f"{REPO_ROOT}/sql/streaming_silver"

YT_PROXY = "http://localhost:31103"

YT_PATH = (
    "/home/airflow/.local/bin:"
    "/opt/airflow/.local/bin:"
    "/home/chig_k3s/yt-env/bin:"
    "/usr/local/bin:"
    "/usr/bin:"
    "/bin"
)


def yql_task(task_id: str, sql_file: str, target_table: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        env={
            "YT_PROXY": YT_PROXY,
            "PATH": YT_PATH,
        },
        bash_command=f"""
bash -lc '
set -euo pipefail

export PATH="{YT_PATH}:$PATH"
export YT_PROXY="{YT_PROXY}"

echo "DAG_ID={DAG_ID}"
echo "TASK_ID={task_id}"
echo "SQL_FILE={SQL_DIR}/{sql_file}"
echo "TARGET_TABLE={target_table}"
echo "YT_PROXY=$YT_PROXY"

echo "== check yt cli =="
command -v yt
yt --version || true

export RUN_ID="{{{{ run_id }}}}"
export DAG_ID="{{{{ dag.dag_id }}}}"
export TASK_ID="{task_id}"
export SQL_FILE="{SQL_DIR}/{sql_file}"
export TARGET_TABLE="{target_table}"

bash "{SCRIPT}"
'
""",
    )


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 5, 10),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ytsaurus", "streaming", "silver", "yql"],
) as dag:

    start = EmptyOperator(task_id="start")

    build_streaming_posts = yql_task(
        task_id="build_streaming_posts",
        sql_file="streaming_posts.sql",
        target_table="//home/silver_stage/streaming_posts",
    )

    build_streaming_comments = yql_task(
        task_id="build_streaming_comments",
        sql_file="streaming_comments.sql",
        target_table="//home/silver_stage/streaming_comments",
    )

    build_streaming_todos = yql_task(
        task_id="build_streaming_todos",
        sql_file="streaming_todos.sql",
        target_table="//home/silver_stage/streaming_todos",
    )

    finish = EmptyOperator(task_id="finish")

    start >> [
        build_streaming_posts,
        build_streaming_comments,
        build_streaming_todos,
    ] >> finish