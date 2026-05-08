#!/usr/bin/env bash

set -euo pipefail

MART="${1:?Usage: $0 <mart-name>}"
BATCH_ID="${BATCH_ID:-$(uuidgen)}"

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

DRIVER_HOST="${DRIVER_HOST:-10.130.0.24}"
SILVER_PATH="${SILVER_PATH:-//home/silver_stage/greenhub_telemetry}"
GOLD_ROOT="${GOLD_ROOT:-//home/gold_stage}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOB_FILE="$SCRIPT_DIR/silver_to_gold_greenhub.py"

if [[ ! -f "$JOB_FILE" ]]; then
    echo "ERROR: job file not found: $JOB_FILE" >&2
    exit 1
fi

if [[ ! -x /home/chig_k3s/yt-env/bin/spark-submit-yt ]]; then
    echo "ERROR: spark-submit-yt not found or not executable: /home/chig_k3s/yt-env/bin/spark-submit-yt" >&2
    exit 1
fi

case "$MART" in
  daily_country_stats)
    DRIVER_PORT=28311
    BLOCK_PORT=28312
    UI_PORT=28313
    ;;
  country_overview)
    DRIVER_PORT=28321
    BLOCK_PORT=28322
    UI_PORT=28323
    ;;
  device_lifecycle)
    DRIVER_PORT=28331
    BLOCK_PORT=28332
    UI_PORT=28333
    ;;
  hourly_battery_health)
    DRIVER_PORT=28341
    BLOCK_PORT=28342
    UI_PORT=28343
    ;;
  *)
    echo "ERROR: unknown mart: $MART" >&2
    echo "Allowed: daily_country_stats country_overview device_lifecycle hourly_battery_health" >&2
    exit 1
    ;;
esac

echo "Stage:          silver -> gold (greenhub)"
echo "Mart:           $MART"
echo "Batch ID:       $BATCH_ID"
echo "Silver path:    $SILVER_PATH"
echo "Gold root:      $GOLD_ROOT"
echo "Driver host:    $DRIVER_HOST"
echo "Spark local IP: $SPARK_LOCAL_IP"
echo "YT proxy:       $YT_PROXY"
echo "Job file:       $JOB_FILE"
echo "spark-submit:   $(command -v spark-submit-yt || true)"
echo "JAVA_HOME:      $JAVA_HOME"
echo "Driver port:    $DRIVER_PORT"
echo "Block port:     $BLOCK_PORT"
echo "UI port:        $UI_PORT"

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
  --conf spark.driver.port="$DRIVER_PORT" \
  --conf spark.blockManager.port="$BLOCK_PORT" \
  --conf spark.ui.port="$UI_PORT" \
  --conf spark.port.maxRetries=32 \
  --conf spark.network.timeout=300s \
  --conf spark.executor.heartbeatInterval=30s \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.default.parallelism=8 \
  "$JOB_FILE" \
    --mart "$MART" \
    --batch-id "$BATCH_ID" \
    --silver-path "$SILVER_PATH" \
    --gold-root "$GOLD_ROOT"
