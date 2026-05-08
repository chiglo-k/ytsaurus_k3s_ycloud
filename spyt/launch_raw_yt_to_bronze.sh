#!/usr/bin/env bash

set -euo pipefail

if [[ -f /home/chig_k3s/yt-env/bin/activate ]]; then
    source /home/chig_k3s/yt-env/bin/activate
fi

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export PATH="$JAVA_HOME/bin:/home/chig_k3s/yt-env/bin:$PATH"
export YT_PROXY="${YT_PROXY:-http://localhost:31103}"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-10.130.0.24}"

if [[ -z "${YT_TOKEN:-}" && -f /home/chig_k3s/.yt/token ]]; then
    export YT_TOKEN="$(cat /home/chig_k3s/.yt/token)"
fi

if [[ -z "${YT_TOKEN:-}" ]]; then
    echo "ERROR: YT_TOKEN is not set and /home/chig_k3s/.yt/token not found" >&2
    exit 1
fi

BATCH_ID="${BATCH_ID:-$(uuidgen)}"
DRIVER_HOST="${DRIVER_HOST:-10.130.0.24}"
RAW_PATH="${RAW_PATH:-//home/raw/greenhub}"
BRONZE_ROOT="${BRONZE_ROOT:-//home/bronze_stage/greenhub}"
PART_INDEX="${PART_INDEX:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOB_FILE="$SCRIPT_DIR/raw_yt_to_bronze_greenhub.py"

if [[ ! -f "$JOB_FILE" ]]; then
    echo "ERROR: job file not found: $JOB_FILE" >&2
    exit 1
fi

if [[ ! -x /home/chig_k3s/yt-env/bin/spark-submit-yt ]]; then
    echo "ERROR: spark-submit-yt not found or not executable: /home/chig_k3s/yt-env/bin/spark-submit-yt" >&2
    exit 1
fi

echo "Stage:          raw YT -> bronze (greenhub)"
echo "Batch ID:       $BATCH_ID"
echo "Raw path:       $RAW_PATH"
echo "Bronze root:    $BRONZE_ROOT"
echo "Part index:     $PART_INDEX"
echo "Driver host:    $DRIVER_HOST"
echo "Spark local IP: $SPARK_LOCAL_IP"
echo "YT proxy:       $YT_PROXY"
echo "Job file:       $JOB_FILE"
echo "spark-submit:   $(command -v spark-submit-yt || true)"
echo "JAVA_HOME:      $JAVA_HOME"

/home/chig_k3s/yt-env/bin/spark-submit-yt \
  --proxy localhost:31103 \
  --discovery-path //home/spark/discovery/main \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=3g \
  --conf spark.executor.instances=2 \
  --conf spark.cores.max=4 \
  --conf spark.driver.memory=2g \
  --conf spark.driver.host="$DRIVER_HOST" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=28201 \
  --conf spark.blockManager.port=28202 \
  --conf spark.ui.port=28203 \
  --conf spark.port.maxRetries=32 \
  --conf spark.network.timeout=300s \
  --conf spark.executor.heartbeatInterval=30s \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.default.parallelism=8 \
  "$JOB_FILE" \
    --raw-path "$RAW_PATH" \
    --bronze-root "$BRONZE_ROOT" \
    --batch-id "$BATCH_ID" \
    --part-index "$PART_INDEX"
