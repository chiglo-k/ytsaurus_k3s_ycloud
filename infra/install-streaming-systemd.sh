#!/bin/bash
# scripts/install-streaming-systemd.sh
# Кладёт streaming-скрипты в нужные места, регистрирует systemd units.
# Запускать ОТ ИМЕНИ chig_k3s (есть sudo).
#
# Использует файлы из текущего каталога: ../systemd/* и ../kafka/raw_to_bronze.py
#
set -e

PACK_DIR="$(cd "$(dirname "$0")/.." && pwd)"
echo "[install] using $PACK_DIR"

# 1. SPYT
sudo cp $PACK_DIR/systemd/start-spyt-cluster.sh /usr/local/bin/
sudo chmod +x /usr/local/bin/start-spyt-cluster.sh
sudo cp $PACK_DIR/systemd/spyt-cluster.service /etc/systemd/system/

# 2. Streaming consumers
sudo cp $PACK_DIR/systemd/start-streaming-consumers.sh /usr/local/bin/
sudo chmod +x /usr/local/bin/start-streaming-consumers.sh
sudo cp $PACK_DIR/systemd/greenhub-streaming-consumers.service /etc/systemd/system/

# 3. Fix raw_to_bronze.py (insert_rows вместо write_table)
cp $PACK_DIR/kafka/raw_to_bronze.py /home/chig_k3s/git/repo/kafka/raw_to_bronze.py
echo "[install] raw_to_bronze.py patched (insert_rows)"

# 4. Папка под логи
mkdir -p /home/chig_k3s/main_d/logs

# 5. Зависимости в yt-env
source /home/chig_k3s/yt-env/bin/activate
pip install --quiet requests confluent-kafka 2>&1 | tail -3 || true

# 6. Активация
sudo systemctl daemon-reload
sudo systemctl enable spyt-cluster.service
sudo systemctl enable greenhub-streaming-consumers.service

echo "[install] systemd units enabled. Start order:"
echo "  sudo systemctl start spyt-cluster.service             # ~3-5 минут до Spark master"
echo "  sudo systemctl start greenhub-streaming-consumers     # сразу"
echo ""
echo "Check:"
echo "  sudo systemctl status spyt-cluster"
echo "  sudo systemctl status greenhub-streaming-consumers"
echo "  tail -f /home/chig_k3s/main_d/logs/*.log"
