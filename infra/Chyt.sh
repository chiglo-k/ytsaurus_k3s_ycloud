cat > ~/chyt-persist.sh <<'BASH_EOF'
#!/usr/bin/env bash
# CHYT setup script — выполняется один раз после rebuild кластера
# Применяет всё необходимое чтобы CHYT clique ch_public работала на 18 CPU / 65 GiB

set -euo pipefail

YT_PROXY="${YT_PROXY:-http://localhost:31103}"
export YT_PROXY

STRAWBERRY_SVC="http://10.43.168.14:80"
CLUSTER="minisaurus"

echo "[1/5] Patch YT CR: entrypointWrapper для exec-node (pip install requests перед стартом ytserver)"
kubectl patch ytsaurus minisaurus -n default --type='json' -p='[
  {
    "op": "add",
    "path": "/spec/execNodes/0/entrypointWrapper",
    "value": [
      "sh",
      "-c",
      "pip3 install --quiet requests certifi urllib3 idna 2>/dev/null || true; exec \"$@\"",
      "--"
    ]
  }
]' 2>/dev/null || echo "  (already patched, skipped)"

echo "[2/5] Patch YT CR: jobImage для CHYT/SPYT jobs"
kubectl patch ytsaurus minisaurus -n default --type='merge' \
  -p='{"spec":{"jobImage":"ghcr.io/ytsaurus/chyt:2.14.0-relwithdebinfo"}}'

echo "[3/5] Patch YT CR: бóльшие лимиты на controller-agent (snapshot building)"
kubectl patch ytsaurus minisaurus -n default --type='json' -p='[
  {"op":"replace","path":"/spec/controllerAgents/resources/limits/cpu","value":"4"},
  {"op":"replace","path":"/spec/controllerAgents/resources/limits/memory","value":"8Gi"},
  {"op":"replace","path":"/spec/controllerAgents/resources/requests/cpu","value":"500m"},
  {"op":"replace","path":"/spec/controllerAgents/resources/requests/memory","value":"2Gi"}
]' 2>/dev/null || true

echo "[4/5] Strong guarantee для pool 'chyt'"
yt set //sys/pool_trees/default/chyt/@strong_guarantee_resources '{cpu=18}' 2>/dev/null || true

echo "[5/5] Speclet для ch_public clique (18 CPU / 65 GiB)"
yt clickhouse ctl --address $STRAWBERRY_SVC --proxy $YT_PROXY \
  set-option --alias ch_public --cluster-name $CLUSTER instance_cpu 18
yt clickhouse ctl --address $STRAWBERRY_SVC --proxy $YT_PROXY \
  set-option --alias ch_public --cluster-name $CLUSTER instance_memory '{
    clickhouse=20000000000;
    chunk_meta_cache=4000000000;
    compressed_cache=20000000000;
    reader=20000000000;
    log_tailer=200000000;
    footprint=4000000000;
    clickhouse_watermark=2000000000;
    watchdog_oom_watermark=0;
    watchdog_oom_window_watermark=0;
  }'

echo ""
echo "===================================================="
echo "  DONE. Waiting 90s for strawberry to restart clique"
echo "===================================================="
sleep 90

yt clickhouse ctl --address $STRAWBERRY_SVC --proxy $YT_PROXY status --cluster-name $CLUSTER ch_public 2>&1 | head -25
echo ""
echo "--- Smoke test ---"
yt clickhouse execute "SELECT 1 + 1 AS x" --alias ch_public --proxy $YT_PROXY
BASH_EOF

chmod +x ~/chyt-persist.sh
ls -la ~/chyt-persist.sh