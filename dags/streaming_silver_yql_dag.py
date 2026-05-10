from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

BASE_DIR = os.environ.get("PROJECT_BASE_DIR", "/home/chig_k3s/main_d")
SQL_DIR = f"{BASE_DIR}/sql/streaming_silver"
SCRIPT = f"{BASE_DIR}/scripts/run_yql_with_yt_log.sh"

DEFAULT_ENV = {
    **os.environ,
    "YT_PROXY": os.environ.get("YT_PROXY", "http://localhost:31103"),
    "LOG_TABLE": "//home/ops_logs/airflow/streaming_silver_runs",
    "DAG_ID": "streaming_silver_yql",
}

default_args = {
    "owner": "chig_k3s",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="streaming_silver_yql",
    description="Build streaming silver tables from bronze_stage using YQL",
    default_args=default_args,
    start_date=datetime(2026, 5, 10),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ytsaurus", "yql", "streaming", "silver"],
) as dag:
    start = EmptyOperator(task_id="start")

    build_streaming_posts = BashOperator(
        task_id="build_streaming_posts",
        bash_command=(
            f"RUN_ID='{{{{ run_id }}}}' "
            f"TASK_ID='build_streaming_posts' "
            f"SQL_FILE='{SQL_DIR}/streaming_posts.sql' "
            f"TARGET_TABLE='//home/silver_stage/streaming_posts' "
            f"{SCRIPT}"
        ),
        env=DEFAULT_ENV,
        execution_timeout=timedelta(minutes=4),
    )

    build_streaming_comments = BashOperator(
        task_id="build_streaming_comments",
        bash_command=(
            f"RUN_ID='{{{{ run_id }}}}' "
            f"TASK_ID='build_streaming_comments' "
            f"SQL_FILE='{SQL_DIR}/streaming_comments.sql' "
            f"TARGET_TABLE='//home/silver_stage/streaming_comments' "
            f"{SCRIPT}"
        ),
        env=DEFAULT_ENV,
        execution_timeout=timedelta(minutes=4),
    )

    build_streaming_todos = BashOperator(
        task_id="build_streaming_todos",
        bash_command=(
            f"RUN_ID='{{{{ run_id }}}}' "
            f"TASK_ID='build_streaming_todos' "
            f"SQL_FILE='{SQL_DIR}/streaming_todos.sql' "
            f"TARGET_TABLE='//home/silver_stage/streaming_todos' "
            f"{SCRIPT}"
        ),
        env=DEFAULT_ENV,
        execution_timeout=timedelta(minutes=4),
    )

    end = EmptyOperator(task_id="end")

    start >> [build_streaming_posts, build_streaming_comments, build_streaming_todos] >> end
