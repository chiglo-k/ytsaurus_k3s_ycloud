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

INPUT="${1:?Usage: $0 <s3a-path> <part-index>}"
PART_INDEX="${2:?Usage: $0 <s3a-path> <part-index>}"
BATCH_ID="${BATCH_ID:-$(uuidgen)}"

DRIVER_HOST="${DRIVER_HOST:-10.130.0.24}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOB_FILE="$SCRIPT_DIR/raw_to_bronze_greenhub.py"

if [[ ! -f "$JOB_FILE" ]]; then
    echo "ERROR: job file not found: $JOB_FILE" >&2
    exit 1
fi

if [[ -z "${S3_ACCESS_KEY:-}" ]] || [[ -z "${S3_SECRET_KEY:-}" ]]; then
    echo "ERROR: S3_ACCESS_KEY / S3_SECRET_KEY not set" >&2
    exit 1
fi

if [[ ! -x /home/chig_k3s/yt-env/bin/spark-submit-yt ]]; then
    echo "ERROR: spark-submit-yt not found or not executable: /home/chig_k3s/yt-env/bin/spark-submit-yt" >&2
    exit 1
fi

echo "Input: $INPUT"
echo "Part index: $PART_INDEX"
echo "Batch ID: $BATCH_ID"
echo "Driver host: $DRIVER_HOST"
echo "Spark local IP: $SPARK_LOCAL_IP"
echo "YT proxy: $YT_PROXY"
echo "Job file: $JOB_FILE"
echo "spark-submit-yt: $(command -v spark-submit-yt || true)"
echo "JAVA_HOME: $JAVA_HOME"

/home/chig_k3s/yt-env/bin/spark-submit-yt \
  --proxy localhost:31103 \
  --discovery-path //home/spark/discovery/main \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=3g \
  --conf spark.executor.instances=2 \
  --conf spark.cores.max=4 \
  --conf spark.driver.memory=2g \
  --conf spark.driver.host="$DRIVER_HOST" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=28001 \
  --conf spark.blockManager.port=28002 \
  --conf spark.ui.port=28003 \
  --conf spark.port.maxRetries=32 \
  --conf spark.network.timeout=300s \
  --conf spark.executor.heartbeatInterval=30s \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.default.parallelism=8 \
  --conf spark.hadoop.fs.s3a.endpoint=http://10.130.0.35:8333 \
  --conf spark.hadoop.fs.s3a.access.key="$S3_ACCESS_KEY" \
  --conf spark.hadoop.fs.s3a.secret.key="$S3_SECRET_KEY" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  "$JOB_FILE" \
    --input "$INPUT" \
    --batch-id "$BATCH_ID" \
    --part-index "$PART_INDEX"
