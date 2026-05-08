#!/bin/bash
# launch_raw_to_bronze.sh
# Запуск PySpark job raw_to_bronze_greenhub.py.
# Аргументы: путь к parquet в S3 + порядковый номер части.
# Пример (тестовый прогон на одном файле):
#   ./launch_raw_to_bronze.sh s3a://greenhub//part-0000.parquet 0
# Пример (с явным batch_id для всей серии загрузок):
#   BATCH_ID=$(uuidgen) ./launch_raw_to_bronze.sh s3a://greenhub//part-0001.parquet 1
# Требования env:
#   S3_ACCESS_KEY, S3_SECRET_KEY    — креды от SeaweedFS
#   YT_PROXY=http://localhost:31103, YT_TOKEN=$(cat ~/.yt/token)
set -euo pipefail

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


echo "Input: $INPUT"
echo "Part index: $PART_INDEX"
echo "Batch ID: $BATCH_ID"
echo "Driver host: $DRIVER_HOST"
echo "Job file: $JOB_FILE"


spark-submit-yt \
  --proxy localhost:31103 \
  --discovery-path //home/spark/discovery/main \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.host="$DRIVER_HOST" \
  --conf spark.driver.bindAddress=0.0.0.0 \
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
