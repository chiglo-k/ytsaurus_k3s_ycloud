from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


DAG_ID = "streaming_silver_yql"

REPO_ROOT = "/opt/airflow/dags/repo"

SCRIPT = f"{REPO_ROOT}/scripts/run_yql_with_yt_log.sh"
SQL_DIR = f"{REPO_ROOT}/sql/streaming_silver"

YT_PROXY = "http://81.26.182.40:31103"


def yql_task(task_id: str, sql_file: str, target_table: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        env={
            "YT_PROXY": YT_PROXY,
        },
        bash_command=f"""
bash -lc '
set -euo pipefail

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