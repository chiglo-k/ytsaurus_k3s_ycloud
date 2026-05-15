#!/bin/bash
# /usr/local/bin/start-streaming-consumers.sh
# Три python-процесса:
#  1) api_to_kafka.py   — jsonplaceholder → Kafka topic raw-events
#  2) kafka_to_raw.py   — Kafka → //home/raw_stage/raw_events (dynamic queue, 3 tablets)
#  3) raw_to_bronze.py  — pull_consumer → //home/bronze_stage/bronze_t{0,1,2} через insert_rows
#
# Watchdog проверяет процессы каждые 30 сек и рестартует упавшие.
#
set -e

PY=/home/chig_k3s/yt-env/bin/python3
LOGS_DIR=/home/chig_k3s/main_d/logs
mkdir -p $LOGS_DIR

cd /home/chig_k3s/git/repo/kafka

export YT_PROXY=${YT_PROXY:-http://localhost:31103}
export YT_TOKEN=$(cat /home/chig_k3s/.yt/token)
export KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-10.130.0.27:30092}
export KAFKA_TOPIC=${KAFKA_TOPIC:-raw-events}
export KAFKA_GROUP_ID=${KAFKA_GROUP_ID:-raw-consumer-fresh}
export POLL_SECONDS=${POLL_SECONDS:-30}
export PYTHONUNBUFFERED=1
export PATH=/home/chig_k3s/yt-env/bin:$PATH

# Ждём пока YT proxy ответит
for i in {1..60}; do
  if $PY -c "import yt.wrapper as yt; yt.config['proxy']['url']='http://localhost:31103'; yt.list('/')" >/dev/null 2>&1; then
    echo "[$(date)] YT reachable"
    break
  fi
  sleep 5
done

# Запуск трёх процессов в фоне
nohup $PY api_to_kafka.py   > $LOGS_DIR/api_to_kafka.log 2>&1 &
echo $! > $LOGS_DIR/api_to_kafka.pid
echo "[$(date)] api_to_kafka started pid=$(cat $LOGS_DIR/api_to_kafka.pid)"

nohup $PY kafka_to_raw.py   > $LOGS_DIR/kafka_to_raw.log 2>&1 &
echo $! > $LOGS_DIR/kafka_to_raw.pid
echo "[$(date)] kafka_to_raw started pid=$(cat $LOGS_DIR/kafka_to_raw.pid)"

nohup $PY raw_to_bronze.py  > $LOGS_DIR/raw_to_bronze.log 2>&1 &
echo $! > $LOGS_DIR/raw_to_bronze.pid
echo "[$(date)] raw_to_bronze started pid=$(cat $LOGS_DIR/raw_to_bronze.pid)"

# Watchdog
while true; do
  for proc in api_to_kafka kafka_to_raw raw_to_bronze; do
    pid=$(cat $LOGS_DIR/$proc.pid 2>/dev/null)
    if ! kill -0 $pid 2>/dev/null; then
      echo "[$(date)] $proc died! Restarting..."
      nohup $PY $proc.py > $LOGS_DIR/$proc.log 2>&1 &
      echo $! > $LOGS_DIR/$proc.pid
    fi
  done
  sleep 30
done
