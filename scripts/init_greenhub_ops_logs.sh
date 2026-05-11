#!/usr/bin/env bash
set -euo pipefail

export YT_PROXY="${YT_PROXY:-http://localhost:31103}"
export YT_USE_HOSTS="${YT_USE_HOSTS:-0}"
export PATH="/home/chig_k3s/yt-env/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

ROOT="//home/ops_logs"
GH="${ROOT}/greenhub"

echo "[INIT] YT_PROXY=${YT_PROXY}"
echo "[INIT] create ops log nodes"

yt create map_node "${ROOT}" --ignore-existing || true
yt create map_node "${GH}" --ignore-existing || true

echo "[INIT] create ${GH}/file_loads"
yt create table "${GH}/file_loads" \
  --ignore-existing \
  --attributes '{
    schema = [
      {name=event_at; type=string; required=%false;};
      {name=dag_id; type=string; required=%false;};
      {name=run_id; type=string; required=%false;};
      {name=batch_id; type=string; required=%false;};

      {name=source_system; type=string; required=%false;};
      {name=bucket; type=string; required=%false;};
      {name=prefix; type=string; required=%false;};
      {name=file_key; type=string; required=%false;};
      {name=s3a_path; type=string; required=%false;};
      {name=etag; type=string; required=%false;};
      {name=file_size; type=int64; required=%false;};
      {name=part_index; type=int64; required=%false;};

      {name=loader; type=string; required=%false;};
      {name=target_layer; type=string; required=%false;};
      {name=target_tables; type=string; required=%false;};
      {name=load_status; type=string; required=%false;};

      {name=started_at; type=string; required=%false;};
      {name=finished_at; type=string; required=%false;};
      {name=duration_sec; type=double; required=%false;};

      {name=rows_inserted; type=int64; required=%false;};
      {name=source_rows; type=int64; required=%false;};
      {name=fact_rows; type=int64; required=%false;};
      {name=device_count; type=int64; required=%false;};

      {name=rc; type=int64; required=%false;};
      {name=error; type=string; required=%false;};
      {name=remote_log; type=string; required=%false;};
      {name=message; type=string; required=%false;};
    ];
  }' || true

echo "[INIT] create ${GH}/layer_runs"
yt create table "${GH}/layer_runs" \
  --ignore-existing \
  --attributes '{
    schema = [
      {name=event_at; type=string; required=%false;};
      {name=dag_id; type=string; required=%false;};
      {name=run_id; type=string; required=%false;};
      {name=batch_id; type=string; required=%false;};
      {name=loader; type=string; required=%false;};

      {name=source_layer; type=string; required=%false;};
      {name=target_layer; type=string; required=%false;};
      {name=target_table; type=string; required=%false;};
      {name=load_status; type=string; required=%false;};

      {name=started_at; type=string; required=%false;};
      {name=finished_at; type=string; required=%false;};
      {name=duration_sec; type=double; required=%false;};

      {name=rows_bronze_in; type=int64; required=%false;};
      {name=rows_after_dedup; type=int64; required=%false;};
      {name=rows_silver_out; type=int64; required=%false;};

      {name=rc; type=int64; required=%false;};
      {name=error; type=string; required=%false;};
      {name=remote_log; type=string; required=%false;};
      {name=message; type=string; required=%false;};
    ];
  }' || true

echo "[INIT] create ${GH}/mart_runs"
yt create table "${GH}/mart_runs" \
  --ignore-existing \
  --attributes '{
    schema = [
      {name=event_at; type=string; required=%false;};
      {name=dag_id; type=string; required=%false;};
      {name=run_id; type=string; required=%false;};
      {name=batch_id; type=string; required=%false;};
      {name=loader; type=string; required=%false;};

      {name=mart; type=string; required=%false;};
      {name=source_layer; type=string; required=%false;};
      {name=target_layer; type=string; required=%false;};
      {name=target_table; type=string; required=%false;};
      {name=load_status; type=string; required=%false;};

      {name=started_at; type=string; required=%false;};
      {name=finished_at; type=string; required=%false;};
      {name=duration_sec; type=double; required=%false;};

      {name=rows_silver_in; type=int64; required=%false;};
      {name=rows_gold_out; type=int64; required=%false;};

      {name=rc; type=int64; required=%false;};
      {name=error; type=string; required=%false;};
      {name=remote_log; type=string; required=%false;};
      {name=message; type=string; required=%false;};
    ];
  }' || true

echo "[INIT] create ${GH}/dag_runs"
yt create table "${GH}/dag_runs" \
  --ignore-existing \
  --attributes '{
    schema = [
      {name=event_at; type=string; required=%false;};
      {name=dag_id; type=string; required=%false;};
      {name=run_id; type=string; required=%false;};
      {name=batch_id; type=string; required=%false;};
      {name=source_system; type=string; required=%false;};
      {name=loader; type=string; required=%false;};

      {name=started_at; type=string; required=%false;};
      {name=finished_at; type=string; required=%false;};
      {name=duration_sec; type=double; required=%false;};
      {name=status; type=string; required=%false;};

      {name=total_items; type=int64; required=%false;};
      {name=success_items; type=int64; required=%false;};
      {name=failed_items; type=int64; required=%false;};
      {name=skipped_items; type=int64; required=%false;};

      {name=error; type=string; required=%false;};
      {name=message; type=string; required=%false;};
    ];
  }' || true

echo "[INIT] done"
