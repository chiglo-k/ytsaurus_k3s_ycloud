#!/bin/bash
# scripts/chyt-persist.sh
# Накатывает все CHYT-настройки одной командой. Использовать ПОСЛЕ применения minisaurus-cr.yaml
# и создания chyt CR (kind: Chyt) с createPublicClique=true.
#
# Что делает:
#  1) entrypointWrapper на exec-node — ставит python deps (requests/certifi/urllib3/idna)
#     потому что clickhouse-trampoline (входит в chyt image) их требует, а на чистом
#     exec-node их нет.
#  2) jobImage для всех jobs = chyt:2.14.0-relwithdebinfo (нужно для CHYT и SPYT operations)
#  3) controllerAgents limits 4Gi (иначе snapshot timeout)
#  4) chyt pool guarantee 18 CPU
#  5) speclet //sys/strawberry/chyt/ch_public/speclet с instance_cpu=18, instance_memory 65GiB
#
set -e

NS=${NS:-default}
CR=${CR:-minisaurus}

echo "[chyt-persist] patching execNodes entrypointWrapper..."
kubectl patch ytsaurus $CR -n $NS --type='json' -p='[
  {"op": "replace", "path": "/spec/execNodes/0/entrypointWrapper", "value":
    ["sh","-c","pip3 install --quiet requests certifi urllib3 idna 2>/dev/null || true; exec \"$@\"","--"]
  }
]'

echo "[chyt-persist] patching jobImage to chyt:2.14.0..."
kubectl patch ytsaurus $CR -n $NS --type='json' -p='[
  {"op": "replace", "path": "/spec/execNodes/0/jobImage", "value": "ghcr.io/ytsaurus/chyt:2.14.0-relwithdebinfo"}
]'

echo "[chyt-persist] patching controllerAgents resources..."
kubectl patch ytsaurus $CR -n $NS --type='json' -p='[
  {"op": "replace", "path": "/spec/controllerAgents/resources/limits/cpu",    "value": "4"},
  {"op": "replace", "path": "/spec/controllerAgents/resources/limits/memory", "value": "8Gi"}
]'

echo "[chyt-persist] waiting for operator..."
sleep 30

# Активация окружения YT
source /home/chig_k3s/yt-env/bin/activate
export YT_PROXY=http://localhost:31103
export YT_TOKEN=$(cat /home/chig_k3s/.yt/token)

echo "[chyt-persist] setting chyt pool guarantee..."
yt set //sys/pool_trees/default/chyt/@strong_guarantee_resources '{cpu=18}'

echo "[chyt-persist] setting speclet for ch_public..."
yt set //sys/strawberry/chyt/ch_public/speclet '{
  active=%true;
  alias=ch_public;
  family=chyt;
  pool=chyt;
  instance_cpu=18;
  instance_memory=70368744177664;
  instance_count=1
}'

echo "[chyt-persist] OK. Wait 60s, then test:"
echo "  yt clickhouse execute \"SELECT 1+1\" --alias ch_public --proxy localhost:31103"
