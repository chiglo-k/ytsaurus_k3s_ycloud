#!/usr/bin/env bash
set -euo pipefail

export YT_PROXY="${YT_PROXY:-http://localhost:31103}"

yt create map_node //home/ops_logs --ignore-existing
yt create map_node //home/ops_logs/airflow --ignore-existing

yt create table //home/ops_logs/airflow/streaming_silver_runs \
  --ignore-existing \
  --attributes '{
    schema = [
      {name=run_id; type=string; required=%false;};
      {name=dag_id; type=string; required=%false;};
      {name=task_id; type=string; required=%false;};
      {name=status; type=string; required=%false;};
      {name=started_at; type=string; required=%false;};
      {name=finished_at; type=string; required=%false;};
      {name=duration_sec; type=double; required=%false;};
      {name=yt_query_id; type=string; required=%false;};
      {name=target_table; type=string; required=%false;};
      {name=message; type=string; required=%false;};
    ];
  }'
