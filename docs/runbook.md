# RUNBOOK — пошаговое поднятие стенда GreenHub Lakehouse

Подробный план рестора стенда с нуля. Все шаги пронумерованы и идут в порядке выполнения. После каждого шага есть **критерий готовности** — что должно работать.

---

## 0 — Подготовка ВМ и SSH



- 5 ВМ в Yandex Cloud:
  - **vm1** — 4 CPU / 8 GiB + публичный IP (control plane)
  - **vm2** — 32 CPU / 128 GiB
  - **vm3** — 32 CPU / 128 GiB
  - **vm4** — 8 CPU / 16 GiB
  - **vm5** — 16 CPU / 32 GiB
- SeaweedFS (отдельная Docker-VM) — `10.130.0.35:8333`
- SSH-ключи разложены, firewall открыт для 6443 (k3s API) внутри сети

**Критерий готовности:** `ssh vm1`, `ssh vm2..vm5` работают, между ВМ есть сетевая связность по внутренним IP.

---

## 1 — k3s cluster

### 1.1 Server на vm1

```bash
curl -sfL https://get.k3s.io | sh -s - \
  --write-kubeconfig-mode 644 \
  --disable traefik

mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $USER:$USER ~/.kube/config
chmod 600 ~/.kube/config
echo 'export KUBECONFIG=~/.kube/config' >> ~/.bashrc
export KUBECONFIG=~/.kube/config

kubectl get nodes
```

### 1.2 Agents на vm2-vm5

```bash
# На vm1: получить токен
sudo cat /var/lib/rancher/k3s/server/node-token

# На каждой vm2..vm5
export K3S_URL="https://10.130.0.24:6443"
export K3S_TOKEN="<TOKEN>"
curl -sfL https://get.k3s.io | K3S_URL="$K3S_URL" K3S_TOKEN="$K3S_TOKEN" sh -
```

### 1.3 Labels

```bash
kubectl label node vm2-k3s-ing-server yt-role=heavy --overwrite
kubectl label node vm3-k3s-ing-server yt-role=heavy --overwrite
kubectl label node vm4-k3s-ing-server yt-role=storage --overwrite
kubectl label node vm5-k3s-ing-server yt-role=kafka --overwrite

kubectl get nodes --show-labels
```

### 1.4 DNS hack для cluster.local на всех ВМ

На каждой из 5 ВМ:

```bash
sudo mkdir -p /etc/systemd/resolved.conf.d
sudo tee /etc/systemd/resolved.conf.d/cluster-local.conf >/dev/null <<'EOF'
[Resolve]
DNS=10.43.0.10
Domains=~cluster.local
EOF
sudo systemctl restart systemd-resolved
getent hosts hp-0.http-proxies.default.svc.cluster.local
```

- `kubectl get nodes -o wide` → 5 Ready
- `getent hosts hp-0.http-proxies.default.svc.cluster.local` возвращает IP на всех 5 ВМ

---

## 2 — Окружение на vm1

### 2.1 Системные пакеты

```bash
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk-headless python3-venv unzip uuid-runtime
java -version
```

### 2.2 helm + cert-manager

```bash
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | sudo bash
helm version --short

kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml
kubectl get crd | grep cert-manager
```

### 2.3 Python venv

```bash
python3 -m venv /home/chig_k3s/yt-env
source /home/chig_k3s/yt-env/bin/activate
pip install --upgrade pip
pip install ytsaurus-client ytsaurus-yson ytsaurus-spyt
pip install "pyspark>=3.5,<4.0"
pip install requests confluent-kafka

yt --version
which spark-launch-yt
python -c "import pyspark; print(pyspark.__version__)"   # 3.5.3
```

### 2.4 Git repo

```bash
git clone https://github.com/chiglo-k/ytsaurus_k3s_ycloud.git /home/chig_k3s/git/repo
cd /home/chig_k3s/git/repo
```

 `yt --version` и `spark-launch-yt --help` работают.

---

## 3 — YTsaurus

### 3.1 Operator (ytop-chart)

```bash
helm install ytsaurus oci://ghcr.io/ytsaurus/ytop-chart \
  --version 0.31.0 \
  --namespace ytsaurus-operator --create-namespace

kubectl get pods -n ytsaurus-operator
```

### 3.2 Secret с creds

```bash
kubectl create secret generic ytadminsec \
  --from-literal=login=admin \
  --from-literal=password=passworf \
  --from-literal=token=$(openssl rand -hex 16) \
  -n default

kubectl get secret ytadminsec -n default
```

### 3.3 Custom Resource minisaurus-cr.yaml


```bash
# Удалить устаревшие поля (если применимо)
sed -i '/useIPv6:/d; /useIPv4:/d' ~/minisaurus-cr.yaml

kubectl apply -f ~/minisaurus-cr.yaml
```

### 3.4 Подождать готовности

```bash
kubectl get pods -n default -w
# Готовность: ms-0, ds-0/1/2, hp-0, sch-0, ca-0, ui-0, qt-0, yqla-0, qa-0,
#             end-0/1, dnd-0/1/2, tnd-0/1 — все Running 1/1
```


### 3.5 Token и Cypress

```bash
TOKEN=$(kubectl get secret ytadminsec -n default -o jsonpath='{.data.token}' | base64 -d)
mkdir -p ~/.yt
echo $TOKEN > ~/.yt/token
chmod 600 ~/.yt/token

export YT_PROXY=http://localhost:31103
export YT_TOKEN=$(cat ~/.yt/token)

yt list /
# → home, sys, tmp, ...

for d in raw_stage bronze_stage silver_stage gold_stage \
         ops_logs ops_logs/greenhub bronze_stage/greenhub \
         spark spark/discovery; do
  yt create map_node //home/$d --ignore-existing
done
```

### 3.6 PATCH execNodes (надо до CHYT)

```bash
kubectl patch ytsaurus minisaurus -n default --type='json' -p='[
  {"op":"replace","path":"/spec/execNodes/0/resources/requests/cpu","value":"24"},
  {"op":"replace","path":"/spec/execNodes/0/resources/requests/memory","value":"96Gi"},
  {"op":"replace","path":"/spec/execNodes/0/resources/limits/cpu","value":"24"},
  {"op":"replace","path":"/spec/execNodes/0/resources/limits/memory","value":"96Gi"}
]'

# Подождать пока operator выкатит изменения
sleep 60
kubectl get pods -n default | grep end-
```

`yt list /` работает, `kubectl get pods -n default` → все YT Running.

---

##4 — Таблицы YTsaurus

```bash
cd /home/chig_k3s/git/repo

# Bronze для batch (fact + 13 dim)
bash YTsaurus/script_table.sh
yt list //home/bronze_stage/greenhub   # 14 таблиц

# Silver для batch
bash YTsaurus/create_silver_tables.sh
yt list //home/silver_stage   # greenhub_telemetry

# Gold (4 витрины)
bash YTsaurus/create_gold_tables.sh
yt list //home/gold_stage   # 4 таблицы

# Silver для streaming
bash YTsaurus/silver_kafka.txt
yt list //home/silver_stage   # +3 streaming_*

# Ops логи
bash scripts/init_ytsaurus_logs.sh
bash scripts/init_greenhub_ops_logs.sh
yt list //home/ops_logs
```


---

## 5 — CHYT clique

### 5.1 CR

```bash
cat > ~/chyt-cr.yaml <<'EOF'
apiVersion: cluster.ytsaurus.tech/v1
kind: Chyt
metadata:
  name: ch-public
  namespace: default
spec:
  ytsaurus:
    name: minisaurus
  image: ghcr.io/ytsaurus/chyt:2.14.0-relwithdebinfo
  makeDefault: true
  createPublicClique: true
EOF

kubectl apply -f ~/chyt-cr.yaml
sleep 60

kubectl get pods -n default | grep strawberry
```

### 5.2 Накатить patches (entrypointWrapper, jobImage, pool, speclet)

```bash
bash infra/chyt-persist.sh
```

### 5.3 Проверка

```bash
sleep 60
yt clickhouse execute "SELECT 1+1" --alias ch_public --proxy localhost:31103
# → 2
```

---

## 6 — Kafka

### 6.1 Helm

```bash
helm install kafka oci://registry-1.docker.io/bitnamicharts/kafka \
  --version 32.4.3 \
  --namespace kafka --create-namespace \
  -f infra/kafka-values.yaml

helm install kafka-ui kafbat-ui/kafka-ui \
  --namespace kafka --version 1.6.4 \
  -f infra/kafka-ui-values.yaml

kubectl get pods -n kafka -w
# Ctrl+C когда kafka-broker-0 и kafka-controller-0 Running
```

### 6.2 Topics + YT queue + consumer

```bash
bash restore_pack/scripts/init-streaming.sh
```

Что внутри (для отладки):
- Создаёт `__consumer_offsets` topic вручную (50 partitions, compact)
- Создаёт `raw-events` topic
- Создаёт `//home/raw_stage/raw_events` (dynamic, 3 tablets)
- Создаёт `//home/raw_stage/raw_events_consumer`
- Регистрирует consumer на queue
- Создаёт `//home/bronze_stage/bronze_t{0,1,2}` (dynamic)



```bash
kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
# → __consumer_offsets, raw-events

yt get //home/raw_stage/raw_events/@tablet_count
# → 3
```

---

## 7 — Airflow

### 7.1 Helm

```bash
kubectl create namespace airflow

# Если приватный git
kubectl create secret generic airflow-git-credentials \
  -n airflow \
  --from-literal=GIT_SYNC_USERNAME='GH_USER' \
  --from-literal=GIT_SYNC_PASSWORD='GH_PAT'

helm repo add apache-airflow https://airflow.apache.org/
helm repo update

helm install airflow apache-airflow/airflow \
  --namespace airflow --version 1.20.0 \
  -f infra/airflow-values.yaml

kubectl get pods -n airflow -w
# ~5-10 минут до готовности
```

### 7.2 Airflow Variables и Connections

В Airflow UI (`http://<vm1_ip>:30081` или `30264`):

**Admin → Variables:**

```
YT_PROXY=http://localhost:31103
S3_ACCESS_KEY=<your_seaweedfs_key>
S3_SECRET_KEY=<your_seaweedfs_secret>
GREENHUB_S3_BUCKET=greenhub
GREENHUB_AWS_CONN_ID=seaweedfs_s3
GREENHUB_SSH_CONN_ID=vm1_ssh
GREENHUB_S3_STATE_KEY=_state/greenhub_load.json
GREENHUB_SILVER_LAUNCHER=/home/chig_k3s/git/repo/spyt/launch_bronze_to_silver.sh
GREENHUB_GOLD_LAUNCHER=/home/chig_k3s/git/repo/spyt/launch_silver_to_gold.sh
```

**Admin → Connections:**

| ID | Conn Type | Host | Login | Password / extra |
|------|-----------|------|--------|------------------|
| `seaweedfs_s3` | Amazon Web Services | `http://10.130.0.35:8333` | (S3_ACCESS_KEY) | (S3_SECRET_KEY) |
| `vm1_ssh` | SSH | `10.130.0.24` | `chig_k3s` | приватный ключ из `~/.ssh/id_ed25519` |

### 7.3 Smoke test

В Airflow UI запустить `test_dag`. Должен пройти Success.


---

## 8 — SPYT

### 8.1 systemd unit

```bash
cd /home/chig_k3s/git/repo

# Создать /usr/local/bin/start-spyt-cluster.sh
sudo bash infra/spyt_launch.sh

# Создать /etc/systemd/system/spyt-cluster.service
sudo bash infra/systemd_spyt.sh

sudo systemctl daemon-reload
sudo systemctl enable spyt-cluster.service
sudo systemctl start spyt-cluster.service

# Подождать 3-5 минут пока Spark master поднимется
sudo systemctl status spyt-cluster.service
```

### 8.2 Проверка

```bash
yt list //home/spark/discovery/main
# должны быть конфиги Spark master/worker

sudo journalctl -u spyt-cluster -n 30
# → [SPYT] launched
```

---

## 9 — Streaming consumers

### 9.1 Установка

```bash
cd /home/chig_k3s/git/repo
bash restore_pack/scripts/install-streaming-systemd.sh

# Стартуем
sudo systemctl start greenhub-streaming-consumers.service
sleep 60

sudo systemctl status greenhub-streaming-consumers.service
```

### 9.2 Проверка

```bash
# Логи 3 процессов
tail -10 /home/chig_k3s/main_d/logs/api_to_kafka.log
# cycle=N rows=800 grand_total=... sleeping=30s

tail -10 /home/chig_k3s/main_d/logs/kafka_to_raw.log
# inserted_to_raw rows=100 t0=100 t1=0 t2=0 batch_id=...

tail -10 /home/chig_k3s/main_d/logs/raw_to_bronze.log
# processed total=300 t0=100 t1=100 t2=100 at=...

# Сколько строк в YT
yt select-rows "sum(1) as c from [//home/raw_stage/raw_events] group by 1" --format '<format=text>yson'
yt select-rows "sum(1) as c from [//home/bronze_stage/bronze_t0] group by 1" --format '<format=text>yson'
yt select-rows "sum(1) as c from [//home/bronze_stage/bronze_t1] group by 1" --format '<format=text>yson'
yt select-rows "sum(1) as c from [//home/bronze_stage/bronze_t2] group by 1" --format '<format=text>yson'

# Cells healthy
yt list //sys/tablet_cells --attribute health --format '<format=text>yson' | head
```

-> все 3 счётчика растут, все cells `good`.

---

## 10 — Запуск pipeline-ов

### 10.1 Batch

```bash
# Положить файлы данных GreenHub в SeaweedFS bucket greenhub/

# В Airflow UI запустить:
# 1) greenhub_s3_to_bronze
# Проверить: yt select-rows "count(*) from [//home/bronze_stage/greenhub/fact_telemetry]"

# 2) greenhub_bronze_to_silver
# Проверить: yt select-rows "count(*) from [//home/silver_stage/greenhub_telemetry]"

# 3) greenhub_silver_to_gold (по очереди для каждой mart)
# Проверить: yt list //home/gold_stage
```

### 10.2 Streaming silver

```bash
# Включить (Unpause) DAG streaming_silver_yql в Airflow UI
# DAG будет автоматически запускаться каждые 5 минут

# Проверить:
yt select-rows "count(*) from [//home/silver_stage/streaming_posts]"
yt select-rows "count(*) from [//home/silver_stage/streaming_comments]"
yt select-rows "count(*) from [//home/silver_stage/streaming_todos]"

# Audit:
yt select-rows "* from [//home/ops_logs/airflow/streaming_silver_runs] limit 10"
```

---

## 11 — DataLens

1. DataLens UI → создать **Подключение → CHYT**
2. Host: `<vm1_public_ip>:31103`
3. Token: значение из `cat ~/.yt/token`
4. Clique alias: `*ch_public`
5. Тест запроса: `SELECT count(*) FROM "//home/gold_stage/gold_daily_country_stats"`
6. Создать датасет, дашборд

---


## Что делать если что-то сломалось

| Симптом | Диагностика | Лечение |
|---------|-------------|---------|
| CHYT clique не поднимается | `kubectl logs strawberry-...` | Проверить что chyt-persist.sh применён |
| SPYT executor падает на pulling jar | Spark logs в `/home/chig_k3s/git/spyt-logs` | Убедиться что `--disable-tmpfs` в spyt_launch.sh |
| kafka_to_raw висит на `COORDINATOR_NOT_AVAILABLE` | `kafka-consumer-groups.sh --list` пуст | Создать `__consumer_offsets` вручную |
| `write_table API is not supported` в raw_to_bronze | grep `yt.write_table` в файле | Применить fix из restore_pack/kafka/raw_to_bronze.py |
| Cells `failed` / `leader_recovery` | `yt list //sys/tablet_cells --attribute health` | Остановить streaming consumers на 1-2 минуты, дать восстановиться |
| Airflow worker не может SSH на vm1 | UI Connection vm1_ssh: test connection | Проверить ключ, проверить что vm1 firewall пускает 22 |
| YT pod падает с OOMKilled | `kubectl describe pod` | Поднять limits через kubectl patch |
| ytTable connector не находит таблицу | Spark logs | Проверить что таблица создана `yt list <path>` |
