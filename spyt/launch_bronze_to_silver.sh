#!/bin/bash
# launch_bronze_to_silver.sh
# Запуск bronze_to_silver_greenhub.py на SPYT cluster.
# Пример:
#   BATCH_ID=$(uuidgen) ./launch_bronze_to_silver.sh
set -euo pipefail

BATCH_ID="${BATCH_ID:-$(uuidgen)}"
DRIVER_HOST="${DRIVER_HOST:-10.130.0.24}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOB_FILE="$SCRIPT_DIR/jobs/bronze_to_silver_greenhub.py"

echo "Stage:        bronze → silver (greenhub)"
echo "Batch ID:     $BATCH_ID"
echo "Driver host:  $DRIVER_HOST"
echo "Job file:     $JOB_FILE"

spark-submit-yt \
  --proxy localhost:31103 \
  --discovery-path //home/spark/discovery/main \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.host="$DRIVER_HOST" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  "$JOB_FILE" \
    --batch-id "$BATCH_ID"
