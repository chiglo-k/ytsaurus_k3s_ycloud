#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="${PROJECT_BASE_DIR:-/home/chig_k3s/main_d}"
export YT_PROXY="${YT_PROXY:-http://localhost:31103}"

cd "$BASE_DIR"

echo "[$(date -Is)] git pull"
git pull --ff-only

echo "[$(date -Is)] init YTsaurus tables"
bash "$BASE_DIR/scripts/init_ytsaurus_logs.sh"
bash "$BASE_DIR/scripts/init_silver_tables.sh"

echo "[$(date -Is)] validate Airflow DAG"
airflow dags list | grep streaming_silver_yql || true

echo "[$(date -Is)] done"
