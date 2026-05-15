#!/usr/bin/env bash
# /usr/local/bin/start-spyt-cluster.sh
# Поднимает SPYT cluster внутри YTsaurus. Вызывается из systemd-юнита spyt-cluster.service.
#
# КРИТИЧНО:
#  - --disable-tmpfs нужен потому что aws-java-sdk-bundle ~280MB не влезает в tmpfs=1Gi
#  - --abort-existing чтобы перестартануть если кластер уже есть
#  - pyspark==3.5.3 должен совпадать с distrib загруженным в //home/spark/distrib/3/5/3
#
set -euo pipefail

export HOME=/home/chig_k3s
export USER=chig_k3s
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:/home/chig_k3s/yt-env/bin:/usr/local/bin:/usr/bin:/bin"
export YT_PROXY=http://localhost:31103

source /home/chig_k3s/yt-env/bin/activate

if [[ -z "${YT_TOKEN:-}" && -f /home/chig_k3s/.yt/token ]]; then
  export YT_TOKEN="$(cat /home/chig_k3s/.yt/token)"
fi

echo "[SPYT] waiting for YTsaurus proxy..."
for i in $(seq 1 120); do
  if curl -fsS http://localhost:31103 >/dev/null 2>&1; then
    echo "[SPYT] YTsaurus proxy is reachable"
    break
  fi
  if [[ "$i" == "120" ]]; then
    echo "[SPYT] ERROR: YTsaurus proxy not reachable after timeout" >&2
    exit 1
  fi
  sleep 5
done

echo "[SPYT] launching SPYT cluster..."

spark-launch-yt \
  --proxy localhost:31103 \
  --discovery-path //home/spark/discovery/main \
  --pool research \
  --worker-cores 5 \
  --worker-memory 20G \
  --worker-memory-overhead 4G \
  --worker-num 1 \
  --master-memory-limit 2G \
  --enable-livy \
  --livy-max-sessions 1 \
  --livy-driver-cores 1 \
  --livy-driver-memory 2G \
  --disable-history-server \
  --disable-tmpfs \
  --abort-existing \
  --spyt-version 2.9.1

echo "[SPYT] launched"
