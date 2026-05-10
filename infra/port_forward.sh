sudo tee /usr/local/bin/start-spyt-portforward.sh >/dev/null <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

echo "[SPYT-PF] waiting for pod/end-0..."

for i in $(seq 1 120); do
  if /usr/local/bin/kubectl -n default get pod end-0 >/dev/null 2>&1; then
    phase="$(/usr/local/bin/kubectl -n default get pod end-0 -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [[ "$phase" == "Running" ]]; then
      echo "[SPYT-PF] end-0 is Running"
      break
    fi
  fi

  if [[ "$i" == "120" ]]; then
    echo "[SPYT-PF] ERROR: pod/end-0 not running after timeout" >&2
    exit 1
  fi

  sleep 5
done

echo "[SPYT-PF] starting port-forward..."
exec /usr/local/bin/kubectl \
  --kubeconfig=/etc/rancher/k3s/k3s.yaml \
  port-forward -n default pod/end-0 \
  27001:27001 \
  27002:27002
EOF

sudo chmod +x /usr/local/bin/start-spyt-portforward.sh