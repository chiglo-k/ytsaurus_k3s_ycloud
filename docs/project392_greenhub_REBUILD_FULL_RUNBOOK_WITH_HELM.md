# Project 392 / GreenHub Lakehouse: полный rebuild-runbook от инфраструктуры до антихрупкости

Итоговый стенд — учебно-производственный lakehouse-контур:

```text
 BATCH CONTOUR

SeaweedFS/S3 / JSON/parquet files
    -> Airflow DAG greenhub_s3_to_bronze
    -> SSH на vm1
    -> SPYT / spark-submit-yt
    -> YTsaurus bronze fact/dim
    -> Airflow DAG greenhub_bronze_to_silver
    -> SPYT
    -> YTsaurus silver wide table
    -> Airflow DAG greenhub_silver_to_gold
    -> SPYT
    -> YTsaurus gold marts


                     STREAMING CONTOUR

JSONPlaceholder API
    -> api_to_kafka.py
    -> Kafka topic raw-events
    -> kafka_to_raw.py
    -> YTsaurus raw table //home/raw_stage/raw_events
    -> raw_to_bronze.py
    -> YTsaurus bronze_t0 / bronze_t1 / bronze_t2
    -> Airflow DAG streaming_silver_yql
    -> SSH на vm1
    -> YQL
    -> YTsaurus streaming silver tables


                    SERVING / ANALYTICS

YTsaurus tables
    -> CHYT clique *ch_public
    -> DataLens / SQL endpoint / HTTP API
```

Статус на момент фиксации:

```text
CHYT smoke:       yt query --format json chyt "SELECT 1 + 1 AS x" -> {"x":2}
Kafka lag:        raw-consumer-v1 LAG = 0
Raw latest:       kafka_offset > 41599
Bronze counts:    t0=7700+, t1=38500+, t2=15400+
Streaming silver: Airflow task returned load_status=SUCCESS, rc=0
YTsaurus proxy:   http://81.26.182.40:31103 доступен извне
```

---

# Часть I. Инфраструктура и распределение VM

## 1. Роли VM и текущее распределение нагрузки

Ниже — фактическая схема, которая сложилась на стенде.

| VM / адрес | Роль | Что на ней важно |
| `vm1-k3s-ing-server`, internal `10.130.0.24`, public `81.26.182.40` | Основная точка входа, k3s/server/ingress, SSH target для Airflow, запуск consumers, YTsaurus NodePort access | `YT_PROXY=http://localhost:31103`, git repo, `yt-env`, runtime scripts, Airflow ходит сюда через `vm1_ssh` |
| `vm2` | k3s worker, YTsaurus exec-node | `end-0.exec-nodes.default.svc.cluster.local`, часть SPYT/CHYT jobs |
| `vm3` | k3s worker, YTsaurus exec-node/master-related pods | `end-1.exec-nodes...`, `ms-0` в pod CIDR `10.42.2.*`, часто именно тут стартовал SPYT/CHYT |
| `vm4` | k3s worker, query/discovery-related pods | `qt-0`, `ds-0`, Query Tracker / Discovery |
| `10.130.0.27` | Kafka endpoint / broker access | `KAFKA_BOOTSTRAP_SERVERS=10.130.0.27:30092` |
| `10.130.0.35:8333` | SeaweedFS/S3 endpoint | Использовался Airflow S3 state; однажды VM/endpoint был недоступен, что дало `ConnectTimeoutError` |

pod/service:

```text
end-0.exec-nodes.default.svc.cluster.local:9029     -> exec node
end-1.exec-nodes.default.svc.cluster.local:9029     -> exec node
ms-0.masters.default.svc.cluster.local:9010         -> YTsaurus master
qt-0.query-trackers.default.svc.cluster.local:9028  -> Query Tracker
yqla-0.yql-agents.default.svc.cluster.local:9019    -> YQL Agent
ds-0.discovery.default.svc.cluster.local:9020       -> Discovery
qa-0.queue-agents.default.svc.cluster.local:9030    -> Queue Agent
http-proxies-lb.default.svc.cluster.local:80        -> YTsaurus HTTP proxy service
```

NodePorts:

```text
YTsaurus HTTP proxy: 31103
YTsaurus UI:         32118
HTTP proxy control:  32146
```

---

## 2. Базовая установка k3s

### 2.1 На vm1: k3s server

```bash
curl -sfL https://get.k3s.io | sh -s - \
  --write-kubeconfig-mode 644 \
  --disable traefik

sudo systemctl status k3s --no-pager
sudo kubectl get nodes -o wide
```

Kubeconfig:

```bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
kubectl get nodes -o wide
```

Token для worker-ов:

```bash
sudo cat /var/lib/rancher/k3s/server/node-token
```

### 2.2 На worker VM

```bash
export K3S_URL=https://<VM1_INTERNAL_IP>:6443
export K3S_TOKEN=<TOKEN_FROM_VM1>

curl -sfL https://get.k3s.io | K3S_URL="$K3S_URL" K3S_TOKEN="$K3S_TOKEN" sh -
sudo systemctl status k3s-agent --no-pager
```

### 2.3 Проверка сети pod/service

```bash
kubectl get nodes -o wide
kubectl get pods -A -o wide
kubectl get svc -A
```

Ожидаемые сети k3s:

```text
Pod CIDR:     10.42.0.0/16
Service CIDR: 10.43.0.0/16
```

---

## 3. VM1 DNS hack для доступа к Kubernetes service FQDN

### 3.1 Симптом

С `vm1` не резолвились имена вида:

```text
end-0.exec-nodes.default.svc.cluster.local
http-proxies-lb.default.svc.cluster.local
```

Ошибки:

```text
Temporary failure in name resolution
getaddrinfo failed
```

Это ломало `yt get-job-stderr`, Spark/CHYT connectivity и часть Airflow/SSH jobs.

### 3.2 Рабочее состояние

После фикса `resolvectl status` показывал:

```text
Global DNS Server: 10.43.0.10
DNS Domain: ~cluster.local

Link eth0 DNS: 10.130.0.2
DNS Domain: ru-central1.internal auto.internal
```

Проверка:

```bash
resolvectl query end-0.exec-nodes.default.svc.cluster.local
resolvectl query end-1.exec-nodes.default.svc.cluster.local
resolvectl query http-proxies-lb.default.svc.cluster.local

getent hosts end-0.exec-nodes.default.svc.cluster.local
getent hosts end-1.exec-nodes.default.svc.cluster.local
getent hosts http-proxies-lb.default.svc.cluster.local
```

### 3.3 Воспроизводимый способ

Создать drop-in для systemd-resolved:

```bash
sudo mkdir -p /etc/systemd/resolved.conf.d

sudo tee /etc/systemd/resolved.conf.d/k3s-cluster-local.conf >/dev/null <<'EOF'
[Resolve]
DNS=10.43.0.10
Domains=~cluster.local
EOF

sudo systemctl restart systemd-resolved
sudo ln -sf /run/systemd/resolve/stub-resolv.conf /etc/resolv.conf

resolvectl status | sed -n '1,120p'
```

Если это ломает внешние DNS, оставить внешний DNS на `eth0`:

```bash
sudo resolvectl dns eth0 10.130.0.2
sudo resolvectl domain eth0 ru-central1.internal auto.internal
```

### 3.4 Проверка портов

```bash
nc -vz end-1.exec-nodes.default.svc.cluster.local 27001 || true
nc -vz http-proxies-lb.default.svc.cluster.local 80 || true
nc -vz ms-0.masters.default.svc.cluster.local 9010 || true
nc -vz ds-0.discovery.default.svc.cluster.local 9020 || true
```


# Часть II. YTsaurus

## 3A. Helm как основной способ установки Kubernetes-компонентов

В стенде Kubernetes-компоненты поднимаются через Helm/HelmChart-подход.

### 3A.1 Установка Helm на VM1

```bash
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

helm version
kubectl version --client
kubectl get nodes -o wide
```

### 3A.2 Общая схема Helm deployment

Общий шаблон установки любого компонента:

```bash
helm repo add <repo_name> <repo_url>
helm repo update

helm upgrade --install <release_name> <repo_name>/<chart_name> \
  --namespace <namespace> \
  --create-namespace \
  -f <values_file>.yaml

kubectl get pods -n <namespace> -o wide
kubectl get svc -n <namespace>
```

Для фиксации текущего состояния:

```bash
helm list -A
helm status <release_name> -n <namespace>
helm get values <release_name> -n <namespace> -o yaml
helm get manifest <release_name> -n <namespace> > /tmp/<release_name>.manifest.yaml
```

Для обновления:

```bash
helm upgrade <release_name> <repo_name>/<chart_name> \
  --namespace <namespace> \
  -f <values_file>.yaml
```

Для отката:

```bash
helm history <release_name> -n <namespace>
helm rollback <release_name> <REVISION> -n <namespace>
```

### 3A.3 Airflow через Helm

Airflow используется как orchestration layer. DAG-и синхронизируются из Git repo и исполняются worker-ами. Для тяжёлых операций Airflow не запускает `yt` внутри контейнера, а ходит по SSH на VM1 через connection `vm1_ssh`.

Типовая установка:

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  -f airflow-values.yaml
```

Проверка:

```bash
kubectl get pods -n airflow -o wide
kubectl get svc -n airflow
kubectl logs -n airflow deploy/airflow-scheduler --tail=200
```

Типовые сущности Airflow:

```text
namespace: airflow
worker:    airflow-worker-0
scheduler: airflow-scheduler
webserver: airflow-webserver
DAG path:  /opt/airflow/dags/repo/dags
```

Проверка DAG sync:

```bash
kubectl exec -n airflow airflow-worker-0 -- bash -lc '
find /opt/airflow/dags -maxdepth 4 -type f | sort | head -100
'

kubectl exec -n airflow airflow-worker-0 -- bash -lc '
airflow dags list | grep -E "greenhub|streaming"
airflow dags list-import-errors
'
```

Airflow connections, которые нужны стенду:

```text
vm1_ssh       # SSH на vm1-k3s-ing-server
seaweedfs_s3  # S3-compatible connection на SeaweedFS
```

Airflow variables:

```bash
airflow variables set YT_PROXY http://localhost:31103
airflow variables set GREENHUB_SSH_CONN_ID vm1_ssh
airflow variables set GREENHUB_AWS_CONN_ID seaweedfs_s3
airflow variables set GREENHUB_S3_BUCKET greenhub
airflow variables set STREAMING_SILVER_YQL_LAUNCHER /home/chig_k3s/git/repo/scripts/run_yql_with_yt_log.sh
```

Проверка SSH из Airflow:

```bash
kubectl exec -n airflow airflow-worker-0 -- bash -lc '
airflow connections get vm1_ssh
'
```

### 3A.4 Kafka через Helm

Kafka используется как буфер streaming-контура. Фактический namespace:

```text
namespace: kafka
pod: kafka-controller-0
external/bootstrap endpoint: 10.130.0.27:30092
topic: raw-events
consumer group: raw-consumer-v1
```

Типовая установка через Bitnami chart:

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm upgrade --install kafka bitnami/kafka \
  --namespace kafka \
  --create-namespace \
  -f kafka-values.yaml
```

Проверка:

```bash
kubectl get pods -n kafka -o wide
kubectl get svc -n kafka
kubectl logs -n kafka kafka-controller-0 --tail=200
```

Topic:

```bash
kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic raw-events \
  --partitions 1 \
  --replication-factor 1
```

Lag:

```bash
kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group raw-consumer-v1
```

Нормальное состояние:

```text
CURRENT-OFFSET == LOG-END-OFFSET
LAG = 0
```

Если Kafka переустановлена и NodePort/endpoint изменился, нужно обновить переменную запуска consumers:

```bash
export KAFKA_BOOTSTRAP_SERVERS=<NEW_KAFKA_ENDPOINT>:30092
```

и systemd unit-ы / launch-команды.

### 3A.5 SeaweedFS/S3 через Helm

SeaweedFS используется как S3-compatible storage для batch-файлов и state-файлов Airflow.

Фактический endpoint, который использовался в стенде:

```text
http://10.130.0.35:8333
bucket: greenhub
```

Типовая установка через Helm:

```bash
helm repo add seaweedfs https://seaweedfs.github.io/seaweedfs/helm
helm repo update

helm upgrade --install seaweedfs seaweedfs/seaweedfs \
  --namespace seaweedfs \
  --create-namespace \
  -f seaweedfs-values.yaml
```

Проверка:

```bash
kubectl get pods -n seaweedfs -o wide
kubectl get svc -n seaweedfs
curl -v http://10.130.0.35:8333/
```

Если Airflow падает так:

```text
ConnectTimeoutError: http://10.130.0.35:8333/greenhub/_state/...
```

проверить:

```bash
kubectl get pods -n seaweedfs -o wide
kubectl get svc -n seaweedfs
curl -v http://10.130.0.35:8333/
```

### 3A.6 YTsaurus через operator/Helm

YTsaurus в стенде работает как k8s-managed cluster `minisaurus`. Основные сервисы:

```text
http-proxies-lb  NodePort 80:31103/TCP
ytsaurus-ui      NodePort 80:32118/TCP
```

Если operator ставится через Helm, общий шаблон:

```bash
helm repo add ytsaurus <YTSAURUS_HELM_REPO_URL>
helm repo update

helm upgrade --install ytsaurus-operator ytsaurus/<operator-chart> \
  --namespace ytsaurus-system \
  --create-namespace \
  -f ytsaurus-operator-values.yaml
```

Затем применяется Ytsaurus custom resource:

```bash
kubectl apply -f ytsaurus-minisaurus.yaml
```

Проверка:

```bash
kubectl get pods -n default -o wide
kubectl get svc -n default | grep -Ei 'http|proxy|yt|ui'
kubectl get svc -n default http-proxies-lb -o wide
kubectl get svc -n default ytsaurus-ui -o wide
```

Proxy smoke:

```bash
curl -sS http://localhost:31103/api/v4/list || true
```

Если ответ:

```text
Client is missing credentials
```

то proxy живой, но требует токен. Это нормальный smoke-test доступности.


### 3A.8 Helm troubleshooting

| Симптом | Проверка | Решение |
| Pod `Pending` | `kubectl describe pod ...` | Не хватает CPU/RAM/PVC/Node selector |
| Pod `CrashLoopBackOff` | `kubectl logs ... --previous` | Смотреть env/secrets/config |
| Service NodePort недоступен | `kubectl get svc`, security group/firewall | Открыть порт, проверить `externalTrafficPolicy` |
| Airflow не видит DAG | Проверить git-sync и `/opt/airflow/dags/repo/dags` | Исправить repo/branch/path |
| Kafka topic недоступен | `kafka-topics.sh --list` | Проверить bootstrap endpoint |
| S3 timeout | `curl endpoint`, pod logs | Поднять SeaweedFS или исправить endpoint |
| YTsaurus service DNS не резолвится на VM1 | `getent hosts ...svc.cluster.local` | DNS hack через `systemd-resolved` и CoreDNS `10.43.0.10` |


## 4. Подъём YTsaurus в k3s

Фактически использовался кластер `minisaurus`, управляемый YTsaurus k8s operator. Если ставить с нуля, логика такая:

1. Установить YTsaurus k8s operator.
2. Применить Ytsaurus CR.
3. Проверить pods/services.
4. Открыть UI/HTTP proxy через NodePort.

Минимальные проверки уже поднятого кластера:

```bash
kubectl get pods -n default -o wide
kubectl get svc -n default
kubectl get svc -n default http-proxies-lb -o wide
kubectl get svc -n default ytsaurus-ui -o wide
```

Ожидаемые сервисы:

```text
http-proxies-lb     NodePort 80:31103/TCP
ytsaurus-ui         NodePort 80:32118/TCP
```

Проверка HTTP proxy на VM1:

```bash
curl -sS http://localhost:31103/api/v4/list || true
```

Без токена должен быть ответ уровня:

```text
Client is missing credentials
```

### 4.1 YT CLI / venv

На VM используется venv:

```text
/home/chig_k3s/yt-env
```

Проверка:

```bash
/home/chig_k3s/yt-env/bin/python - <<'PY'
import yt.wrapper as yt
print("yt wrapper ok")
PY

/home/chig_k3s/yt-env/bin/yt --version
```

Рекомендуемые пакеты:

```bash
/home/chig_k3s/yt-env/bin/python -m pip install \
  ytsaurus-client \
  ytsaurus-yson \
  confluent-kafka \
  requests
```

Системный PATH для remote jobs:

```bash
export PATH="/home/chig_k3s/yt-env/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
export YT_PROXY=http://localhost:31103
export YT_USE_HOSTS=0
```

### 4.2 Токены

Токены не хранить в документации. Рабочие варианты:

```bash
export YT_TOKEN="$(cat /home/chig_k3s/.yt/token)"
```

## 5. YTsaurus tables и слои

Основные root paths:

```text
//home/raw_stage
//home/bronze_stage
//home/silver_stage
//home/gold_stage
//home/ops_logs
//sys/clickhouse/strawberry/ch_public
```

### 5.1 Batch bronze

```text
//home/bronze_stage/greenhub/fact_telemetry
//home/bronze_stage/greenhub/dim_device
//home/bronze_stage/greenhub/dim_country
//home/bronze_stage/greenhub/dim_timezone
...
```

DDL: `script_table.sh` / `create_bronze_tables_*.sh`.

### 5.2 Batch silver

```text
//home/silver_stage/greenhub_telemetry
```

DDL: `create_silver_tables.sh`.

### 5.3 Batch gold

```text
//home/gold_stage/gold_daily_country_stats
//home/gold_stage/gold_country_overview
//home/gold_stage/gold_device_lifecycle
//home/gold_stage/gold_hourly_battery_health
```

DDL: `create_gold_tables.sh`.

### 5.4 Streaming raw/bronze/silver

```text
//home/raw_stage/raw_events

//home/bronze_stage/bronze_t0   # posts
//home/bronze_stage/bronze_t1   # comments
//home/bronze_stage/bronze_t2   # todos

//home/silver_stage/streaming_posts
//home/silver_stage/streaming_comments
//home/silver_stage/streaming_todos
```

---

# Часть III. Kafka streaming contour

## 6. Kafka: установка и проверка

Фактически Kafka была доступна так:

```text
namespace: kafka
pod:       kafka-controller-0
endpoint:  10.130.0.27:30092
topic:     raw-events
group:     raw-consumer-v1
```

Проверка:

```bash
kubectl get pods -n kafka -o wide
kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Создание topic при необходимости:

```bash
kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic raw-events \
  --partitions 1 \
  --replication-factor 1
```

Проверка lag:

```bash
kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group raw-consumer-v1
```

Ожидаемое нормальное состояние:

```text
CURRENT-OFFSET == LOG-END-OFFSET
LAG = 0
```

## 7. Kafka runtime services на VM

Streaming scripts лежат:

```text
/home/chig_k3s/main_d/consumers
```

Основные файлы:

```text
api_to_kafka.py       # API -> Kafka
kafka_to_raw.py       # Kafka -> YTsaurus raw
raw_to_bronze.py      # raw -> bronze_t0/t1/t2
kafka_producer.py
kafka_consumer.py
kafka_event_schema.py
```

### 7.1 Запуск вручную

```bash
cd /home/chig_k3s/main_d/consumers
mkdir -p /home/chig_k3s/main_d/logs

export YT_PROXY=http://localhost:31103
export KAFKA_BOOTSTRAP_SERVERS=10.130.0.27:30092
export KAFKA_TOPIC=raw-events
export KAFKA_GROUP_ID=raw-consumer-v1
export POLL_SECONDS=30

nohup /home/chig_k3s/yt-env/bin/python api_to_kafka.py \
  > /home/chig_k3s/main_d/logs/api_to_kafka.log 2>&1 &

nohup /home/chig_k3s/yt-env/bin/python kafka_to_raw.py \
  > /home/chig_k3s/main_d/logs/kafka_to_raw.log 2>&1 &

nohup /home/chig_k3s/yt-env/bin/python raw_to_bronze.py \
  > /home/chig_k3s/main_d/logs/raw_to_bronze.log 2>&1 &
```

Проверка:

```bash
ps aux | grep -E 'api_to_kafka|kafka_to_raw|raw_to_bronze' | grep -v grep

tail -50 /home/chig_k3s/main_d/logs/api_to_kafka.log
tail -50 /home/chig_k3s/main_d/logs/kafka_to_raw.log
tail -50 /home/chig_k3s/main_d/logs/raw_to_bronze.log
```

Остановка:

```bash
pkill -f api_to_kafka.py || true
pkill -f kafka_to_raw.py || true
pkill -f raw_to_bronze.py || true
```

### 7.2 Постоянный запуск через systemd

Рекомендуется оформить три сервиса.

`/etc/systemd/system/greenhub-api-to-kafka.service`:

```ini
[Unit]
Description=GreenHub API to Kafka producer
After=network-online.target
Wants=network-online.target

[Service]
User=chig_k3s
WorkingDirectory=/home/chig_k3s/main_d/consumers
Environment=KAFKA_BOOTSTRAP_SERVERS=10.130.0.27:30092
Environment=KAFKA_TOPIC=raw-events
Environment=POLL_SECONDS=30
ExecStart=/home/chig_k3s/yt-env/bin/python /home/chig_k3s/main_d/consumers/api_to_kafka.py
Restart=always
RestartSec=10
StandardOutput=append:/home/chig_k3s/main_d/logs/api_to_kafka.log
StandardError=append:/home/chig_k3s/main_d/logs/api_to_kafka.err.log

[Install]
WantedBy=multi-user.target
```

`/etc/systemd/system/greenhub-kafka-to-raw.service`:

```ini
[Unit]
Description=GreenHub Kafka to YTsaurus raw writer
After=network-online.target
Wants=network-online.target

[Service]
User=chig_k3s
WorkingDirectory=/home/chig_k3s/main_d/consumers
Environment=YT_PROXY=http://localhost:31103
Environment=KAFKA_BOOTSTRAP_SERVERS=10.130.0.27:30092
Environment=KAFKA_TOPIC=raw-events
Environment=KAFKA_GROUP_ID=raw-consumer-v1
ExecStart=/home/chig_k3s/yt-env/bin/python /home/chig_k3s/main_d/consumers/kafka_to_raw.py
Restart=always
RestartSec=10
StandardOutput=append:/home/chig_k3s/main_d/logs/kafka_to_raw.log
StandardError=append:/home/chig_k3s/main_d/logs/kafka_to_raw.err.log

[Install]
WantedBy=multi-user.target
```

`/etc/systemd/system/greenhub-raw-to-bronze.service`:

```ini
[Unit]
Description=GreenHub YTsaurus raw to bronze processor
After=network-online.target
Wants=network-online.target

[Service]
User=chig_k3s
WorkingDirectory=/home/chig_k3s/main_d/consumers
Environment=YT_PROXY=http://localhost:31103
ExecStart=/home/chig_k3s/yt-env/bin/python /home/chig_k3s/main_d/consumers/raw_to_bronze.py
Restart=always
RestartSec=10
StandardOutput=append:/home/chig_k3s/main_d/logs/raw_to_bronze.log
StandardError=append:/home/chig_k3s/main_d/logs/raw_to_bronze.err.log

[Install]
WantedBy=multi-user.target
```

Включить:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now greenhub-api-to-kafka
sudo systemctl enable --now greenhub-kafka-to-raw
sudo systemctl enable --now greenhub-raw-to-bronze

systemctl status greenhub-api-to-kafka --no-pager
systemctl status greenhub-kafka-to-raw --no-pager
systemctl status greenhub-raw-to-bronze --no-pager
```

### 7.3 Проверка raw

Не использовать `head`, он показывает старые offset 0..N. Для свежих событий:

```bash
yt select-rows '
SELECT kafka_offset, source, stream_id, entity_id, batch_id, ingested_at
FROM `//home/raw_stage/raw_events`
ORDER BY kafka_offset DESC
LIMIT 20
' --format json
```

Если `select-rows` синтаксически капризен, можно:

```bash
yt read-table //home/raw_stage/raw_events --format json \
  | jq -r '.kafka_offset' \
  | sort -n \
  | tail -10
```

Для dynamic raw table `@row_count` может отсутствовать

# IV. Airflow и Git CI/CD

## 8. Airflow deployment model

Фактически DAG-и живут в git repo:

```text
/home/chig_k3s/git/repo/dags
```

В Airflow pod они видны как:

```text
/opt/airflow/dags/repo/dags
```

```text
Airflow task
  -> SSHHook(vm1_ssh)
  -> remote bash на vm1
  -> /home/chig_k3s/yt-env/bin/yt / spark-submit-yt
```

### 8.1 Airflow connections

Нужны connections:

```text
vm1_ssh       # SSH на vm1-k3s-ing-server
seaweedfs_s3  # S3-compatible connection на SeaweedFS
```

Проверка:

```bash
airflow connections get vm1_ssh
airflow connections get seaweedfs_s3
```

### 8.2 Airflow Variables

Минимально полезные:

```text
YT_PROXY=http://localhost:31103
GREENHUB_SSH_CONN_ID=vm1_ssh
GREENHUB_AWS_CONN_ID=seaweedfs_s3
GREENHUB_S3_BUCKET=greenhub

S3_ACCESS_KEY=<secret>
S3_SECRET_KEY=<secret>

GREENHUB_BRONZE_SILVER_LAUNCHER=/home/chig_k3s/git/repo/spyt/launch_bronze_to_silver.sh
GREENHUB_SILVER_GOLD_LAUNCHER=/home/chig_k3s/git/repo/spyt/launch_silver_to_gold.sh
STREAMING_SILVER_YQL_LAUNCHER=/home/chig_k3s/git/repo/scripts/run_yql_with_yt_log.sh
```

Установить:

```bash
airflow variables set YT_PROXY http://localhost:31103
airflow variables set GREENHUB_SSH_CONN_ID vm1_ssh
airflow variables set STREAMING_SILVER_YQL_LAUNCHER /home/chig_k3s/git/repo/scripts/run_yql_with_yt_log.sh
```

### 8.3 Airflow pools

Для SPYT лучше иметь pool:

```bash
airflow pools set spyt_pool 1 "Limit concurrent SPYT jobs"
```

---

## 9. Batch DAG-и

### 9.1 `greenhub_s3_to_bronze`

Что делает:

```text
S3/SeaweedFS -> list parquet -> check etag in _state/greenhub_load.json
-> SSH vm1 -> launch_raw_to_bronze.sh
-> SPYT -> bronze fact/dim
-> update S3 state
```

Особенность антидублей:

```text
loaded_etags в _state/greenhub_load.json
```

Если файл уже был успешно загружен, он пропускается.

### 9.2 `greenhub_bronze_to_silver`

Что делает:

```text
bronze fact/dim -> SPYT -> silver wide //home/silver_stage/greenhub_telemetry
```

Сохраняет S3 state:

```text
_state/greenhub_silver_runs.json
```

### 9.3 `greenhub_silver_to_gold`

Что делает:

```text
silver wide -> SPYT -> gold marts
```

Gold marts:

```text
daily_country_stats
country_overview
device_lifecycle
hourly_battery_health
```

Сохраняет S3 state:

```text
_state/greenhub_gold_runs.json
```

---

## 10. Streaming silver DAG

DAG:

```text
streaming_silver_yql
```

Расписание:

```text
*/5 * * * *
```

Рабочая схема:

```text
Airflow -> SSH vm1 -> /home/chig_k3s/git/repo/scripts/run_yql_with_yt_log.sh
```

SQL-файлы:

```text
/home/chig_k3s/git/repo/sql/streaming_silver/streaming_posts.sql
/home/chig_k3s/git/repo/sql/streaming_silver/streaming_comments.sql
/home/chig_k3s/git/repo/sql/streaming_silver/streaming_todos.sql
```

Целевые таблицы:

```text
//home/silver_stage/streaming_posts
//home/silver_stage/streaming_comments
//home/silver_stage/streaming_todos
```

Операционные логи:

```text
//home/ops_logs/airflow/streaming_silver_runs
```

### 10.1 Типовые ошибки streaming DAG

| Ошибка | Причина | Решение |
|---|---|---|
| `DAG.__init__() got unexpected keyword schedule_interval` | Airflow 3 | Использовать `schedule`, не `schedule_interval`. |
| `TemplateNotFound ... .sh` | BashOperator принял `.sh` за template | Не запускать `.sh` напрямую, либо перейти на SSHHook, как в рабочих DAG-ах. |
| `yt: command not found` в Airflow pod | `yt` нет внутри Airflow | Запускать через SSH на VM, где есть `/home/chig_k3s/yt-env/bin/yt`. |
| `No such file run_yql_with_yt_log.sh` | DAG указывал `/home/chig_k3s/main_d/scripts`, а файл в git repo | Исправить путь на `/home/chig_k3s/git/repo/scripts/...`. |
| SQL `<EOF>` | DAG передал старый/пустой SQL path | Исправить SQL path на `/home/chig_k3s/git/repo/sql/...`. |
| `Expected Json ... Optional<String>` | `payload_json` строка | Использовать `Yson::ParseJson(COALESCE(payload_json,"{}"))` или `CAST(payload_json AS Json)`. |
| `Error parsing JSON` при записи ops log | В message попали табы/переводы строк | Формировать JSON через `python json.dumps`, а не руками через echo. |
| `Invalid type expected double actual int64` | Схема ops log ждёт double, пишется int | Писать `float(duration_sec)` или пересоздать таблицу. |

---

# SPYT и Spark

## 11. SPYT setup и хаки

На VM используется:

```text
/home/chig_k3s/yt-env/bin/spark-submit-yt
/home/chig_k3s/git/repo/spyt/launch_*.sh
/home/chig_k3s/git/spyt-logs
```

Обязательные env в launchers:

```bash
export YT_PROXY=http://localhost:31103
export YT_USE_HOSTS=0
export DRIVER_HOST=10.130.0.24
export SPARK_LOCAL_IP=10.130.0.24
export PATH="/home/chig_k3s/yt-env/bin:$PATH"
```

### 11.1 Spark/port conflicts

Была проблема с портами `27001/27002`: они могли быть заняты port-forward или SPYT master endpoint.

Решение: в launchers использовать фиксированные driver/block/UI ports, которые не конфликтуют:

```text
spark.driver.port
spark.blockManager.port
spark.ui.port
```

Например:

```bash
--conf spark.driver.host=10.130.0.24
--conf spark.driver.bindAddress=0.0.0.0
--conf spark.driver.port=28001
--conf spark.blockManager.port=28002
--conf spark.ui.port=28003
```

### 11.2 SPYT master connectivity

Проверка DNS/портов:

```bash
getent hosts end-1.exec-nodes.default.svc.cluster.local
nc -vz end-1.exec-nodes.default.svc.cluster.local 27001 || true
```

Если Airflow пишет:

```text
Failed to connect to master end-1.exec-nodes.default.svc.cluster.local:27001
```

то проверять:

1. DNS на VM1.
2. Поднят ли SPYT cluster.
3. Где фактически master.
4. Нет ли конфликта port-forward.
5. Не умер ли YTsaurus operation.

---

# Часть VI. CHYT / DataLens

## 12. CHYT setup

CHYT clique:

```text
alias: ch_public
DataLens alias: *ch_public
operation path: //sys/clickhouse/strawberry/ch_public
```

Speclet стабилизационный:

```yson
{
    pool = "research";
    active = %true;
    instance_count = 1;
    cpu_limit = 16;
    memory_limit = 68719476736;
}
```

Запуск через strawberry-controller:

```bash
kubectl exec -n default deploy/strawberry-controller -- sh -lc '
cat > /tmp/chyt_public.yson <<EOF
{
    pool = "research";
    active = %true;
    instance_count = 1;
    cpu_limit = 16;
    memory_limit = 68719476736;
}
EOF

export YT_PROXY=http-proxies-lb.default.svc.cluster.local

/usr/bin/chyt-controller one-shot-run \
  --config-path /config/strawberry-controller.yson \
  --log-to-stderr \
  --family chyt \
  --alias ch_public \
  --speclet-path /tmp/chyt_public.yson
'
```

Проверка:

```bash
OP=$(yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_id | tr -d '"')
yt list-jobs "$OP" --include-runtime --include-archive --include-cypress --limit 20 --format json | jq '.state_counts'
yt query --format json chyt "SELECT 1 + 1 AS x"
```

Ожидаемо:

```json
{"x":2}
```

### 12.1 CHYT requests dependency

Первые CHYT jobs падали:

```text
ModuleNotFoundError: No module named 'requests'
```

Быстрый фикс:

```bash
kubectl exec -n default end-0 -- sh -lc 'python3 -m pip install requests || pip3 install requests'
kubectl exec -n default end-1 -- sh -lc 'python3 -m pip install requests || pip3 install requests'
```

Постоянный фикс: включить `requests` в image/init container для exec-node sandbox или исправить artifact.

### 12.2 CHYT Ares DNS / IPv6-only

Симптом:

```text
Group "/chyt/ch_public" does not exist
Error reporting heartbeat 10 times on startup
Ares DNS resolve failed for "ds-0.discovery.default.svc.cluster.local"
enable_ipv4 false
enable_ipv6 true
DNS server returned answer with no data
```

Суть: обычный `getent` в pod работал, но `ytserver-clickhouse` через c-ares пытался IPv6-only lookup в IPv4-only окружении.

Фикс, который помог стабилизировать:

```bash
yt get //sys/@cluster_connection > /tmp/cluster_connection.before.yson

MS_IP=$(getent hosts ms-0.masters.default.svc.cluster.local | awk '{print $1}' | head -1)
DS_IP=$(getent hosts ds-0.discovery.default.svc.cluster.local | awk '{print $1}' | head -1)

yt set //sys/@cluster_connection/primary_master/addresses "[\"${MS_IP}:9010\"]"
yt set //sys/@cluster_connection/primary_master/peers "[{address=\"${MS_IP}:9010\"; voting=%true;}]"
yt set //sys/@cluster_connection/master_cache/addresses "[\"${MS_IP}:9010\"]"

# Если снова проявится Ares DNS именно по discovery:
yt set //sys/@cluster_connection/discovery_connection/addresses "[\"${DS_IP}:9020\"]"
```

После изменения — abort/restart CHYT operation.

### 12.3 CHYT sample table для health checker

Если health checker ругается:

```text
Node //sys/clickhouse has no child with key "sample_table"
```

Создать:

```bash
yt create map_node //sys/clickhouse --ignore-existing || true

yt create table //sys/clickhouse/sample_table \
  --ignore-existing \
  --attributes '{schema=[{name=x;type=int64;}];}' || true

printf '{"x":1}\n' | yt write-table //sys/clickhouse/sample_table --format json
```

### 12.4 DataLens

Проверка публичного endpoint:

```bash
curl -v --max-time 10 http://81.26.182.40:31103/api/v4/list
```

Ожидаемо без токена:

```text
401 Client is missing credentials
```

Проверка с токеном:

```bash
export YT_TOKEN='<TOKEN>'

curl -sS \
  -H "Authorization: OAuth ${YT_TOKEN}" \
  -H 'X-YT-Parameters: {"path":"//sys/clickhouse/strawberry/ch_public/speclet"}' \
  "http://81.26.182.40:31103/api/v4/get"
```

В DataLens:

```text
Host:    81.26.182.40
Port:    31103
Clique:  *ch_public
Token:   без кавычек
```

Если DataLens пишет:

```text
Clique alias name must begin with an asterisk (*)
```

использовать `*ch_public`.

Если пишет:

```text
Node /runtime_parameters has no child with key "acl"
```

это integration issue с operation runtime ACL. Сам CHYT smoke может при этом работать.

Operation ACL:

```bash
OP=$(yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_id | tr -d '"')

yt update-op-parameters "$OP" '{
    acl = [
        {
            action = allow;
            subjects = ["admin"; "root"; "users"];
            permissions = ["read"; "manage"];
        };
    ];
}'
```

---

# Часть VII. SeaweedFS/S3

## 13. S3 / SeaweedFS

Используется S3-compatible bucket:

```text
bucket: greenhub
endpoint: http://10.130.0.35:8333
Airflow conn: seaweedfs_s3
```

Ошибки:

```text
ConnectTimeoutError: http://10.130.0.35:8333/greenhub/_state/...
```

Причина: VM/endpoint SeaweedFS был недоступен или умер. Это не проблема CHYT/Kafka/YTsaurus.

Проверка:

```bash
curl -v http://10.130.0.35:8333/
```

Airflow S3 connection должен быть типа AWS/S3-compatible. Warning:

```text
AWS Connection conn_type='git' expected 'aws'
```

желательно исправить в Airflow Connections.

---

# Часть VIII. Антихрупкость

## 14. Что уже реализовано

Антихрупкость не одна фича, а комбинация механизмов.

### 14.1 Kafka как буфер

Если `kafka_to_raw.py` умер:

```text
producer продолжает писать в Kafka
consumer lag растёт
raw не растёт
после рестарта consumer читает с committed offset
lag падает до 0
```

Kill test:

```bash
pkill -f kafka_to_raw.py || true

sleep 120

kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group raw-consumer-v1

cd /home/chig_k3s/main_d/consumers
export YT_PROXY=http://localhost:31103
export KAFKA_BOOTSTRAP_SERVERS=10.130.0.27:30092
export KAFKA_TOPIC=raw-events
export KAFKA_GROUP_ID=raw-consumer-v1

nohup /home/chig_k3s/yt-env/bin/python kafka_to_raw.py \
  > /home/chig_k3s/main_d/logs/kafka_to_raw.log 2>&1 &
```

Проверка:

```bash
kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group raw-consumer-v1
```

### 14.2 Offset audit

Raw хранит:

```text
kafka_topic
kafka_partition
kafka_offset
```

Уникальный технический ключ Kafka-события:

```text
(kafka_topic, kafka_partition, kafka_offset)
```

Проверка последних raw событий:

```bash
yt select-rows '
SELECT kafka_offset, source, stream_id, entity_id, batch_id, ingested_at
FROM `//home/raw_stage/raw_events`
ORDER BY kafka_offset DESC
LIMIT 20
' --format json
```

### 14.3 Batch file idempotency

Для S3/файлового контура используется:

```text
source_etag
loaded_etags
_state/greenhub_load.json
```

Одинаковый файл не обрабатывается дважды. Повторный DAG run безопасен.

### 14.4 Raw/Bronze as replay layer

Raw/Bronze хранят:

```text
payload_json
event_ts
processed_at
batch_id
row_index
stream_id
source
```

Это позволяет:

- пересобрать silver;
- пересобрать gold;
- доказать происхождение данных;
- проверить late-arriving events.

### 14.5 Idempotent silver

Streaming silver строится через:

```sql
INSERT INTO `//home/silver_stage/streaming_*` WITH TRUNCATE
```

Повторный запуск DAG не создаёт append-дубли в silver.

### 14.6 Recovery modes

| Режим | Что делает |
|---|---|
| Full rebuild | Полная пересборка silver/gold из bronze |
| By period | Пересчёт периода по `event_ts` |
| By key | Проверка/пересчёт сущности по `entity_id`, `post_id`, `comment_id`, `todo_id` |
| Kafka replay | Откат consumer group offset и повторное чтение |
| File replay | Повторная обработка только FAILED/новых etag |
| Snapshot rollback | Возврат на предыдущий state/snapshot table/date |

### 14.7 Почему не называем это strict exactly-once

Корректная формулировка:

```text
at-least-once ingestion + Kafka offset audit + idempotent silver/gold rebuild
```

Raw/bronze могут принять дубли при replay. Silver/gold должны быть очищающим/idempotent слоем.

---

# Часть IX. Runbook после reboot/redeploy

## 15. Проверка после перезагрузки

```bash
# k3s
sudo systemctl status k3s --no-pager
kubectl get nodes -o wide
kubectl get pods -A -o wide

# YTsaurus services
kubectl get svc -n default | grep -Ei 'http|proxy|ui'
curl -sS http://localhost:31103/api/v4/list || true

# DNS
getent hosts http-proxies-lb.default.svc.cluster.local
getent hosts ms-0.masters.default.svc.cluster.local
getent hosts ds-0.discovery.default.svc.cluster.local

# Kafka
kubectl get pods -n kafka -o wide
kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Airflow
kubectl get pods -n airflow -o wide
kubectl exec -n airflow airflow-worker-0 -- bash -lc 'airflow dags list | grep greenhub || true'
kubectl exec -n airflow airflow-worker-0 -- bash -lc 'airflow dags list | grep streaming || true'
```

## 16. Запуск consumers после reboot

```bash
cd /home/chig_k3s/main_d/consumers
mkdir -p /home/chig_k3s/main_d/logs

export YT_PROXY=http://localhost:31103
export KAFKA_BOOTSTRAP_SERVERS=10.130.0.27:30092
export KAFKA_TOPIC=raw-events
export KAFKA_GROUP_ID=raw-consumer-v1
export POLL_SECONDS=30

nohup /home/chig_k3s/yt-env/bin/python api_to_kafka.py \
  > /home/chig_k3s/main_d/logs/api_to_kafka.log 2>&1 &

nohup /home/chig_k3s/yt-env/bin/python kafka_to_raw.py \
  > /home/chig_k3s/main_d/logs/kafka_to_raw.log 2>&1 &

nohup /home/chig_k3s/yt-env/bin/python raw_to_bronze.py \
  > /home/chig_k3s/main_d/logs/raw_to_bronze.log 2>&1 &
```

## 17. Проверка streaming contour

```bash
ps aux | grep -E 'api_to_kafka|kafka_to_raw|raw_to_bronze' | grep -v grep

kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group raw-consumer-v1

yt select-rows '
SELECT kafka_offset, source, stream_id, entity_id, batch_id, ingested_at
FROM `//home/raw_stage/raw_events`
ORDER BY kafka_offset DESC
LIMIT 20
' --format json

yt get //home/bronze_stage/bronze_t0/@row_count
yt get //home/bronze_stage/bronze_t1/@row_count
yt get //home/bronze_stage/bronze_t2/@row_count
```

## 18. Проверка CHYT

```bash
OP=$(yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_id 2>/dev/null | tr -d '"')
echo "$OP"

yt list-jobs "$OP" --include-runtime --include-archive --include-cypress --limit 20 --format json | jq '.state_counts'

yt query --format json chyt "SELECT 1 + 1 AS x"
```

Если clique нет — перезапуск CHYT по разделу 12.

---

# Часть X. Типовые ошибки и решения

| Ошибка | Где | Причина | Решение |
|---|---|---|---|
| `Temporary failure in name resolution` | VM1/yt CLI | Не настроен DNS `cluster.local` | systemd-resolved + `10.43.0.10`, `~cluster.local` |
| `yt get-job-stderr` пытается `hp-0...` и не резолвит | VM1 | YT proxy выбирает internal FQDN | Починить DNS или использовать `localhost:31103` |
| `ModuleNotFoundError: requests` | CHYT job | Нет Python-модуля в exec sandbox | `pip install requests` в exec pods; лучше custom image |
| `YSON bindings required` | consumers / yt write | Не установлен `ytsaurus-yson` | `/home/chig_k3s/yt-env/bin/python -m pip install ytsaurus-yson` |
| `No module named confluent_kafka` | consumers | Не тот python | Запускать `/home/chig_k3s/yt-env/bin/python`, установить `confluent-kafka` |
| `Group "/chyt/ch_public" does not exist` | CHYT/DataLens | CHYT не зарегистрировался в discovery | Смотреть job stderr, фикс DNS/Ares/cluster_connection |
| `Ares DNS resolve failed enable_ipv4=false` | CHYT | c-ares IPv6-only в IPv4-only кластере | IP в `//sys/@cluster_connection` |
| `Clique alias name must begin with *` | DataLens | Alias без `*` | Использовать `*ch_public` |
| `Node /runtime_parameters has no child acl` | DataLens | Нет runtime ACL | `yt update-op-parameters` с operation ACL |
| `schedule_interval unexpected` | Airflow 3 | Старый API | Использовать `schedule` |
| `TemplateNotFound .sh` | BashOperator | Airflow считает `.sh` template | SSHHook или `bash -lc` |
| `yt command not found` | Airflow pod | yt нет в pod | Запуск через SSH на VM |
| SQL `<EOF>` | YQL | пустой/не тот SQL path | repo SQL path `/home/chig_k3s/git/repo/sql/...` |
| `Expected Json got Optional<String>` | YQL | payload_json string | `Yson::ParseJson` / `CAST(payload_json AS Json)` |
| `Error parsing JSON` в ops logs | run script | руками собран JSON с табами | `json.dumps` |
| `ConnectTimeoutError 10.130.0.35:8333` | Airflow/S3 | SeaweedFS VM/endpoint умер | Поднять endpoint или заменить |

---

# Часть XI. Что закрепить постоянно

1. Оформить consumers через `systemd`.
2. Вшить Python зависимости в bootstrap:
   - `requests`;
   - `confluent-kafka`;
   - `ytsaurus-yson`.
3. Не ставить `requests` вручную в exec pods, а сделать image/init.
4. Зафиксировать DNS `cluster.local` на VM1.
5. Зафиксировать `cluster_connection` bootstrap для CHYT.
6. Для raw добавить unique key `(kafka_topic, kafka_partition, kafka_offset)` или dedup в silver.
7. Перенести S3 state в YTsaurus ops_logs или оставить S3 как source/state legacy, а YTsaurus как единый lineage.
8. Регулярный `yt merge --spec '{combine_chunks=true;}'` для маленьких chunks bronze/silver.
9. Снять скриншоты:
   - Airflow successful DAG;
   - Kafka lag 0;
   - CHYT smoke `{"x":2}`;
   - DataLens connection;
   - latest raw/bronze/silver rows.
10. SCD2 dictionary оставить как развитие, если не хватает времени.

---

# Часть XII. Формулировка для диплома

```text
В работе реализован lakehouse-контур на базе YTsaurus и k3s. Batch-ветка обрабатывает файлы из S3-совместимого хранилища и строит bronze/silver/gold слои через Airflow и SPYT. Streaming-ветка получает события из внешних API, буферизует их в Kafka, сохраняет raw-события в YTsaurus, раскладывает их в bronze и регулярно материализует streaming silver слой через Airflow и YQL.

Ключевой особенностью является не только сама обработка, но и воспроизводимость. Kafka хранит offset и позволяет восстановиться после отказа consumer. Raw-слой сохраняет kafka_topic, kafka_partition, kafka_offset и исходный payload. Bronze хранит batch_id, processed_at, row_index и event_ts. Silver и gold строятся идемпотентно, что позволяет повторно запускать DAG-и без бесконтрольного накопления дублей.

Для batch-файлов реализована защита от повторной обработки через etag/checksum и state-журнал. Операционные логи и статусы запусков сохраняются в S3/YTsaurus, что позволяет анализировать ошибки, время выполнения и успешность загрузок. CHYT предоставляет SQL-доступ к данным, а DataLens используется как аналитический слой.
```

---

# Часть XIII. Старый объединённый runbook и подробные логи

Ниже сохранена предыдущая объединённая версия документа, собранная из чата и архива. Она содержит дополнительные детали по конкретным ошибкам, файлам, SPYT, CHYT и DataLens.

---


# Project 392 / GreenHub Lakehouse: единый технический runbook, ошибки, решения и антихрупкость

Дата сборки: 2026-05-10  
Среда: k3s + YTsaurus + SPYT + CHYT + Kafka + Airflow + SeaweedFS/S3 + DataLens.  
Основная VM: `vm1-k3s-ing-server`.  
Git repo на VM: `/home/chig_k3s/git/repo`.  
YTsaurus HTTP proxy на VM: `http://localhost:31103`.  
Публичный endpoint YTsaurus HTTP proxy: `http://81.26.182.40:31103`.  

> Документ объединяет: текущий чат, загруженный архив, рабочие DAG-и, shell-скрипты, PySpark jobs, Kafka consumers, DDL и уже написанные md-рекапы. Секреты и токены намеренно не фиксируются.

---

## 1. Итоговый статус

К моменту сборки документа контур доведён до рабочего дипломного MVP.

Работает:

- Airflow DAG-и для batch-ветки.
- SPYT jobs для S3/SeaweedFS -> bronze -> silver -> gold.
- Kafka streaming contour: API -> Kafka -> raw -> bronze.
- Streaming silver через Airflow + SSH + YQL.
- CHYT clique `ch_public` после фикса DNS/зависимостей.
- YTsaurus HTTP proxy доступен снаружи через `81.26.182.40:31103`.
- DataLens endpoint сетево доступен; отдельные ACL/runtime параметры CHYT/DataLens зафиксированы как integration issue.

Ключевые подтверждения из чата:

```text
CHYT smoke: yt query --format json chyt "SELECT 1 + 1 AS x" -> {"x":2}
Kafka lag: raw-consumer-v1 LAG = 0
Raw latest kafka_offset: 41599+
Bronze counts пример: t0=7700, t1=38500, t2=15400
Streaming Airflow YQL task: load_status SUCCESS, rc=0
```

---

## 2. Общая архитектура

```text
                           BATCH CONTOUR

SeaweedFS/S3 parquet
  -> Airflow greenhub_s3_to_bronze
  -> SSH vm1
  -> launch_raw_to_bronze.sh
  -> spark-submit-yt / SPYT
  -> //home/bronze_stage/greenhub/fact_telemetry + dim_*
  -> Airflow greenhub_bronze_to_silver
  -> launch_bronze_to_silver.sh
  -> //home/silver_stage/greenhub_telemetry
  -> Airflow greenhub_silver_to_gold
  -> launch_silver_to_gold.sh <mart>
  -> //home/gold_stage/gold_*


                         STREAMING CONTOUR

JSONPlaceholder API
  -> api_to_kafka.py
  -> Kafka topic raw-events
  -> kafka_to_raw.py
  -> //home/raw_stage/raw_events
  -> raw_to_bronze.py
  -> //home/bronze_stage/bronze_t0/t1/t2
  -> Airflow streaming_silver_yql
  -> SSH vm1
  -> run_yql_with_yt_log.sh
  -> YQL SQL files
  -> //home/silver_stage/streaming_posts/comments/todos


                         SERVING / ANALYTICS

YTsaurus tables
  -> CHYT clique ch_public
  -> DataLens / SQL smoke tests / HTTP API
```

---

## 3. Важные пути и компоненты

### 3.1 VM paths

```text
/home/chig_k3s/git/repo                         # основной git repo
/home/chig_k3s/git/repo/dags                    # Airflow DAG-и
/home/chig_k3s/git/repo/scripts                 # run/deploy/init scripts
/home/chig_k3s/git/repo/sql/streaming_silver    # YQL SQL для streaming silver
/home/chig_k3s/git/spyt-logs                    # remote SPYT logs
/home/chig_k3s/main_d/consumers                 # Kafka consumers/producers runtime copy
/home/chig_k3s/main_d/logs                      # runtime logs
/home/chig_k3s/yt-env                           # Python venv с yt/spark-submit-yt
```

### 3.2 Kubernetes/YTsaurus services

```text
http-proxies-lb.default.svc.cluster.local:80
hp-0.http-proxies.default.svc.cluster.local
ms-0.masters.default.svc.cluster.local:9010
qt-0.query-trackers.default.svc.cluster.local:9028
yqla-0.yql-agents.default.svc.cluster.local:9019
qa-0.queue-agents.default.svc.cluster.local:9030
ds-0.discovery.default.svc.cluster.local:9020
end-0.exec-nodes.default.svc.cluster.local:9029
end-1.exec-nodes.default.svc.cluster.local:9029
```

NodePorts:

```text
YTsaurus HTTP proxy: 31103
YTsaurus UI:         32118
```

---

## 4. Файловая карта проекта

### 4.1 Airflow DAG-и

| Файл | Назначение |
|---|---|
| `greenhub_s3_to_bronze.py` | Batch: S3/SeaweedFS parquet -> SPYT -> bronze fact/dim. |
| `greenhub_bronze_to_silver.py` | Batch: bronze fact + dim -> silver wide. |
| `greenhub_silver_to_gold.py` | Batch: silver wide -> gold marts. |
| `streaming_silver_yql_dag.py` | Streaming: bronze_t0/t1/t2 -> silver tables через YQL. |
| `test_dag.py` | Проверка, что Airflow видит DAG-и. |

### 4.2 Kafka/streaming code

| Файл | Назначение |
|---|---|
| `api_to_kafka.py` | Опрашивает JSONPlaceholder API и пишет события в Kafka. |
| `kafka_event_schema.py` | Dataclass события: `source`, `stream_id`, `event_type`, `event_ts`, `entity_id`, `payload_json`, `ingest_batch_id`, `ingest_ts`. |
| `kafka_producer.py` | Kafka producer с `enable.idempotence=True`, `acks=all`, retries. |
| `kafka_consumer.py` | Kafka consumer с `enable.auto.commit=False`. |
| `kafka_to_raw.py` | Kafka -> `//home/raw_stage/raw_events`. |
| `raw_to_bronze.py` | YTsaurus raw queue -> `bronze_t0/t1/t2`. |

### 4.3 SPYT jobs / launchers

| Файл | Назначение |
|---|---|
| `launch_raw_to_bronze.sh` | Запуск batch S3/parquet -> bronze. |
| `raw_to_bronze_greenhub*.py` | PySpark job для batch bronze fact/dim. |
| `launch_bronze_to_silver.sh` | Запуск bronze -> silver. |
| `bronze_to_silver_greenhub*.py` | PySpark job для silver wide. |
| `launch_silver_to_gold.sh` | Запуск gold mart. |
| `silver_to_gold_greenhub*.py` | PySpark job для gold marts. |

### 4.4 DDL

| Файл | Назначение |
|---|---|
| `create_bronze_tables_*.sh`, `script_table.sh` | Bronze fact/dim DDL. |
| `create_silver_tables.sh` | `//home/silver_stage/greenhub_telemetry`. |
| `create_gold_tables.sh` | Gold marts. |
| `silver_kafka.txt` | Streaming silver tables. |

---

## 5. Batch contour: S3/SeaweedFS -> Bronze

### 5.1 Что поднимали

Цепочка:

```text
SeaweedFS/S3 bucket greenhub
  -> Airflow DAG greenhub_s3_to_bronze
  -> SSH vm1
  -> /home/chig_k3s/git/repo/spyt/launch_raw_to_bronze.sh
  -> spark-submit-yt
  -> raw_to_bronze_greenhub.py
  -> //home/bronze_stage/greenhub/fact_telemetry
  -> //home/bronze_stage/greenhub/dim_*
```

### 5.2 Идемпотентность файлов

Защита от повторной загрузки одинаковых файлов уже есть в DAG:

```text
S3 object -> source_etag -> _state/greenhub_load.json -> loaded_etags
```

Если `etag` уже был успешно обработан, файл пропускается. Это защищает от повторного запуска DAG и повторной доставки одинакового файла.

Формулировка для диплома:

> Для batch/file ingestion реализована защита от повторной обработки одинаковых файлов. В журнале загрузок фиксируются путь файла, размер, etag/checksum и batch_id. При повторном запуске DAG уже успешно обработанные файлы пропускаются, что делает контур идемпотентным.

### 5.3 Ошибки и решения

| Ошибка | Причина | Решение |
|---|---|---|
| S3 credentials не найдены | Airflow Variables/Connection не заполнены | Добавлены `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `seaweedfs_s3`. |
| SSH connection не найден | Нет Airflow connection `vm1_ssh` | Создан SSH connection. |
| Permission denied на `.sh` | Нет executable bit | Запуск через `bash script.sh` или `chmod +x`. |
| `spark-submit-yt: command not found` | PATH/venv не настроен | Добавлен `/home/chig_k3s/yt-env/bin` в PATH. |
| Нет `YT_TOKEN` | Не передан token | Читать `/home/chig_k3s/.yt/token` или env. |
| Spark port conflict | Порты 27001/27002 заняты port-forward | Перенести driver/block/UI ports на 28001/28002/28003. |
| Python UDF mismatch | UDF исполнялся в другом Python | Убрать UDF, заменить на Spark built-ins. |
| timestamp уехал в 1970 | Некорректный тип timestamp | Использовать строковый `event_ts_str` / аккуратный cast. |

---

## 6. Batch contour: Bronze -> Silver -> Gold

### 6.1 Bronze -> Silver

`greenhub_bronze_to_silver.py` через SSH запускает SPYT job, который читает bronze fact/dim и строит wide silver table:

```text
//home/silver_stage/greenhub_telemetry
```

Silver добавляет:

- decoded dim values;
- date/hour/day features;
- flags: `is_night`, `is_charging`, `is_wifi`, `battery_low`;
- metrics: `memory_used_pct`, `storage_used_pct`;
- `_silver_batch_id`, `_silver_built_at`.

Дедупликация: по `fact_uid` выбирается актуальная запись. Silver строится как snapshot/overwrite, не как бесконтрольный append.

### 6.2 Silver -> Gold

`greenhub_silver_to_gold.py` строит marts:

```text
gold_daily_country_stats
gold_country_overview
gold_device_lifecycle
gold_hourly_battery_health
```

Стратегия: получить schema из DDL, truncate/recreate target table, append new result. Повторный запуск не должен плодить дубли.

---

## 7. Streaming contour: API -> Kafka -> Raw -> Bronze

### 7.1 API -> Kafka

`api_to_kafka.py` опрашивает:

| API | source | stream_id | event_type |
|---|---|---|---|
| `/posts` | `jsonplaceholder_posts` | `s0` | `post_event` |
| `/comments` | `jsonplaceholder_comments` | `s1` | `comment_event` |
| `/todos` | `jsonplaceholder_todos` | `s2` | `todo_event` |

Запуск:

```bash
cd /home/chig_k3s/main_d/consumers
export KAFKA_BOOTSTRAP_SERVERS=10.130.0.27:30092
export KAFKA_TOPIC=raw-events
export POLL_SECONDS=30
nohup /home/chig_k3s/yt-env/bin/python api_to_kafka.py > /home/chig_k3s/main_d/logs/api_to_kafka.log 2>&1 &
```

Producer настроен с `enable.idempotence=True`, `acks=all`, retries.

### 7.2 Kafka -> Raw

`kafka_to_raw.py` пишет в:

```text
//home/raw_stage/raw_events
```

Raw сохраняет:

```text
source, stream_id, event_type, event_ts, entity_id, payload_json,
kafka_topic, kafka_partition, kafka_offset,
ingested_at, batch_id
```

Kafka technical key:

```text
(kafka_topic, kafka_partition, kafka_offset)
```

### 7.3 Raw -> Bronze

`raw_to_bronze.py` читает raw queue и раскладывает:

| tablet_index | target |
|---|---|
| 0 | `//home/bronze_stage/bronze_t0` posts |
| 1 | `//home/bronze_stage/bronze_t1` comments |
| 2 | `//home/bronze_stage/bronze_t2` todos |

Пример подтверждённого состояния:

```text
bronze_t0 = 7700
bronze_t1 = 38500
bronze_t2 = 15400
```

---

## 8. Streaming silver через Airflow + YQL

### 8.1 Что сделали

DAG `streaming_silver_yql` каждые 5 минут запускает три YQL-трансформации:

```text
bronze_t0 -> streaming_posts
bronze_t1 -> streaming_comments
bronze_t2 -> streaming_todos
```

Правильная модель запуска такая же, как у рабочих SPYT DAG-ов:

```text
Airflow worker
  -> SSHHook(vm1_ssh)
  -> vm1
  -> /home/chig_k3s/git/repo/scripts/run_yql_with_yt_log.sh
  -> yt query --format json yql "$(cat sql_file)"
```

Это важно: `yt` не должен исполняться внутри Airflow pod, где CLI может отсутствовать.

### 8.2 Ошибки, которые были

| Ошибка | Причина | Решение |
|---|---|---|
| `DAG.__init__() got unexpected keyword schedule_interval` | Airflow 3.x | Использовать `schedule`. |
| `TemplateNotFound` для `.sh` | BashOperator воспринимал `.sh` как template | Перейти на SSHHook pattern. |
| `yt: command not found` | В Airflow pod нет YT CLI | Выполнять `yt` на VM через SSH. |
| SQL `<EOF>` | Передавался пустой/не тот SQL path | Исправить на `/home/chig_k3s/git/repo/sql/streaming_silver/*.sql`. |
| `Expected Json... Optional<String>` | `payload_json` строка | `CAST(payload_json AS Json)` или `Yson::ParseJson`. |
| `Error parsing JSON` при логировании | stdout/stderr с табами вставлялись руками в JSON | Формировать log row через `python json.dumps`. |

### 8.3 Текущий статус

В логах был успешный Airflow task:

```text
load_status = SUCCESS
rc = 0
```

Оставшаяся ошибка была не по YQL-трансформации, а по записи ops log в `//home/ops_logs/airflow/streaming_silver_runs` из-за неэкранированной JSON-строки.

---

## 9. CHYT / ClickHouse over YTsaurus

### 9.1 Симптом

```text
Ensure that the clique "ch_public" has started properly and its jobs are successfully running
Group "/chyt/ch_public" does not exist
```

### 9.2 Missing `requests`

CHYT job падал:

```text
ModuleNotFoundError: No module named 'requests'
```

Фикс:

```bash
kubectl exec -n default end-0 -- sh -lc 'python3 -m pip install requests || pip3 install requests'
kubectl exec -n default end-1 -- sh -lc 'python3 -m pip install requests || pip3 install requests'
```

### 9.3 Ares DNS / IPv6-only

Главная причина:

```text
Ares DNS resolve failed for "ds-0.discovery.default.svc.cluster.local"
enable_ipv4 false
enable_ipv6 true
DNS server returned answer with no data
```

c-ares внутри `ytserver-clickhouse` пытался IPv6-only lookup, а Kubernetes DNS отдавал IPv4.

### 9.4 Фикс `cluster_connection`

Заменили FQDN на IPv4 для master/master_cache, при необходимости discovery:

```bash
MS_IP=$(getent hosts ms-0.masters.default.svc.cluster.local | awk '{print $1}' | head -1)
yt set //sys/@cluster_connection/primary_master/addresses "[\"${MS_IP}:9010\"]"
yt set //sys/@cluster_connection/primary_master/peers "[{address=\"${MS_IP}:9010\"; voting=%true;}]"
yt set //sys/@cluster_connection/master_cache/addresses "[\"${MS_IP}:9010\"]"
```

Smoke:

```bash
yt query --format json chyt "SELECT 1 + 1 AS x"
# {"x":2}
```

---

## 10. DataLens / CHYT serving

Endpoint:

```text
http://81.26.182.40:31103
```

Проверка без токена:

```bash
curl -v --max-time 10 http://81.26.182.40:31103/api/v4/list
# 401 Client is missing credentials
```

HTTP API требует JSON в `X-YT-Parameters`:

```bash
curl -sS \
  -H "Authorization: OAuth ${YT_TOKEN}" \
  -H 'X-YT-Parameters: {"path":"//sys/clickhouse/strawberry/ch_public/speclet"}' \
  "http://81.26.182.40:31103/api/v4/get"
```

Отдельная DataLens/CHYT проблема:

```text
Invalid clique specification
Node /runtime_parameters has no child with key "acl"
```

Это integration issue с operation ACL/runtime parameters. Сам CHYT как SQL endpoint рабочий.

---

## 11. Антихрупкость

Это сильная часть диплома. Текущая архитектура уже содержит несколько механизмов восстановления.

### 11.1 Kafka как буфер

Если consumer падает, producer продолжает писать, Kafka копит сообщения, lag растёт. После рестарта consumer дочитывает.

Kill-test:

```bash
pkill -f kafka_to_raw.py || true
sleep 120
kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group raw-consumer-v1
```

### 11.2 Offset audit

Raw хранит `kafka_topic`, `kafka_partition`, `kafka_offset`, поэтому можно проверить полноту и дубли.

### 11.3 File idempotency

Batch-файлы защищены через `etag`/state:

```text
если etag уже SUCCESS -> файл не обрабатывается повторно
```

### 11.4 Raw/Bronze как replay layer

Raw/bronze сохраняют `payload_json`, `batch_id`, `event_ts`, `processed_at`, `row_index`. Silver/gold можно пересобрать без повторного обращения к источнику.

### 11.5 Late-arriving data

Система различает:

```text
event_ts      # бизнес-время
processed_at  # время обработки
batch_id      # запуск/пачка
```

Данные задним числом можно пересчитать по периоду `event_ts`.

### 11.6 Recalculate modes

Поддерживаемые режимы восстановления:

| Режим | Как работает |
|---|---|
| Full rebuild | Полностью пересобрать silver/gold из bronze. |
| By period | Пересчитать окно по `event_ts`. |
| By key | Пересчитать конкретный `post_id/comment_id/todo_id/fact_uid`. |
| Periodic snapshot | Gold/silver snapshot можно откатить/пересобрать на дату. |
| Failed task retry | Airflow retry / manual rerun. |

### 11.7 Exactly-once формулировка

Не надо писать strict exactly-once. Корректнее:

```text
at-least-once delivery + offset audit + idempotent/deduplicated downstream rebuild
```

Для strict exactly-once потребовались бы:

- unique key `(kafka_topic, kafka_partition, kafka_offset)` в raw;
- commit Kafka offset только после успешного insert;
- idempotent upsert/dedup на downstream.

---

## 12. Runbook: запуск streaming-контура

```bash
cd /home/chig_k3s/main_d/consumers
mkdir -p /home/chig_k3s/main_d/logs

export YT_PROXY=http://localhost:31103
export KAFKA_BOOTSTRAP_SERVERS=10.130.0.27:30092
export KAFKA_TOPIC=raw-events
export KAFKA_GROUP_ID=raw-consumer-v1
export POLL_SECONDS=30

nohup /home/chig_k3s/yt-env/bin/python api_to_kafka.py \
  > /home/chig_k3s/main_d/logs/api_to_kafka.log 2>&1 &

nohup /home/chig_k3s/yt-env/bin/python kafka_to_raw.py \
  > /home/chig_k3s/main_d/logs/kafka_to_raw.log 2>&1 &

nohup /home/chig_k3s/yt-env/bin/python raw_to_bronze.py \
  > /home/chig_k3s/main_d/logs/raw_to_bronze.log 2>&1 &
```

Проверка:

```bash
ps aux | grep -E 'api_to_kafka|kafka_to_raw|raw_to_bronze' | grep -v grep
```

Kafka lag:

```bash
kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group raw-consumer-v1
```

Raw latest:

```bash
yt select-rows '
kafka_offset, source, stream_id, entity_id, batch_id, ingested_at
from `//home/raw_stage/raw_events`
order by kafka_offset desc
limit 20
' --format json
```

Bronze counts:

```bash
yt get //home/bronze_stage/bronze_t0/@row_count
yt get //home/bronze_stage/bronze_t1/@row_count
yt get //home/bronze_stage/bronze_t2/@row_count
```

---

## 13. Runbook: после reboot/redeploy

1. Проверить YTsaurus proxy:

   ```bash
   curl -sS http://localhost:31103/api/v4/list || true
   ```

2. Проверить CHYT:

   ```bash
   yt query --format json chyt "SELECT 1 + 1 AS x"
   ```

3. Проверить Kafka:

   ```bash
   kubectl get pods -n kafka
   kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
   ```

4. Запустить streaming consumers.
5. Проверить lag.
6. Проверить raw latest offsets.
7. Проверить bronze counts.
8. Проверить Airflow DAG-и:

   ```bash
   airflow dags list-import-errors
   airflow dags list | grep greenhub
   airflow dags list | grep streaming_silver_yql
   ```

9. Проверить silver/gold tables.

---

## 14. Что закрепить постоянно

1. Вшить `requests` в exec-node/CHYT environment.
2. Вшить `confluent-kafka`, `ytsaurus-yson`, `requests` в `yt-env` bootstrap.
3. Оформить streaming long-running scripts в `systemd`.
4. Зафиксировать `cluster_connection` bootstrap или DNS/IPv6 fix.
5. Держать DAG/SQL/scripts в Git и деплоить через CI/CD.
6. Исправить JSON logging через `json.dumps`.
7. Периодически делать `yt merge --spec '{combine_chunks=true;}'` для таблиц с большим числом маленьких чанков.
8. Для production-like exactly-once добавить unique raw key `(kafka_topic, kafka_partition, kafka_offset)`.

---

## 15. Что можно оставить на развитие

- SCD Type 2 для справочников/измерений.
- Строгий exactly-once raw через уникальный ключ.
- Единый ops-log layer в YTsaurus вместо части S3 state.
- Автоматический CHYT recovery script.
- DataLens ACL/runtime parameters hardening.

---

## 16. Формулировка для диплома

> В работе реализован не только ETL/ELT контур, но и механизмы воспроизводимости и восстановления. Kafka используется как буфер между источником и хранилищем. Raw-слой сохраняет технические Kafka offset-поля, bronze хранит исходный payload и batch-метаданные, а silver/gold слои строятся идемпотентно. Для batch-файлов используется защита от повторной загрузки через state-журнал и etag. Airflow обеспечивает оркестрацию, retries и аудит запусков. CHYT предоставляет SQL-доступ к данным, а DataLens используется как аналитический слой.

---

## 17. Инвентарь файлов архива

| Файл | Роль | Размер |
|---|---|---:|
| `api_to_kafka.py` | Streaming Kafka component | 3,104 bytes |
| `batch_pipeline_recap_2026-05-08.md` | Исходный runbook/заметки | 19,142 bytes |
| `bronze_to_silver_greenhub.py` | SPYT job: bronze -> silver | 7,222 bytes |
| `bronze_to_silver_greenhub_patched.py` | SPYT job: bronze -> silver | 8,263 bytes |
| `bronze_to_silver_greenhub_pathfix.py` | SPYT job: bronze -> silver | 8,613 bytes |
| `bronze_to_silver_greenhub_yttriple.py` | SPYT job: bronze -> silver | 8,630 bytes |
| `create_bronze_tables_event_str.sh` | DDL: bronze tables | 4,083 bytes |
| `create_bronze_tables_patched.sh` | DDL: bronze tables | 3,790 bytes |
| `create_gold_tables.sh` | DDL: gold marts | 8,032 bytes |
| `create_gold_tables_patched.sh` | DDL: gold marts | 5,127 bytes |
| `create_silver_tables.sh` | DDL: silver tables | 4,064 bytes |
| `create_silver_tables_patched.sh` | DDL: silver tables | 3,703 bytes |
| `greenhub_bronze_to_silver.py` | Airflow DAG: bronze fact/dim -> silver wide | 7,876 bytes |
| `greenhub_bronze_to_silver_bash_launcher.py` | Airflow DAG: bronze fact/dim -> silver wide | 8,895 bytes |
| `greenhub_bronze_to_silver_patched.py` | Airflow DAG: bronze fact/dim -> silver wide | 8,952 bytes |
| `greenhub_raw_yt_to_bronze_dag.py` | Вспомогательный файл | 9,272 bytes |
| `greenhub_s3_to_bronze.py` | Airflow DAG: S3/SeaweedFS -> SPYT -> YTsaurus bronze | 8,790 bytes |
| `greenhub_silver_to_gold.py` | Airflow DAG: silver wide -> gold marts | 9,070 bytes |
| `greenhub_silver_to_gold_updated.py` | Airflow DAG: silver wide -> gold marts | 9,070 bytes |
| `kafka_consumer.py` | Streaming Kafka component | 1,686 bytes |
| `kafka_event_schema.py` | Streaming Kafka component | 1,070 bytes |
| `kafka_producer.py` | Streaming Kafka component | 1,554 bytes |
| `kafka_to_raw.py` | Streaming Kafka component | 2,926 bytes |
| `launch_bronze_to_silver.sh` | SPYT/Spark launcher | 830 bytes |
| `launch_bronze_to_silver_patched.sh` | SPYT/Spark launcher | 2,164 bytes |
| `launch_raw_to_bronze.sh` | SPYT/Spark launcher | 3,007 bytes |
| `launch_raw_yt_to_bronze.sh` | SPYT/Spark launcher | 2,498 bytes |
| `launch_silver_to_gold.sh` | SPYT/Spark launcher | 3,008 bytes |
| `launch_silver_to_gold_fixed_ports.sh` | SPYT/Spark launcher | 3,167 bytes |
| `launch_silver_to_gold_fixed_ports_v2.sh` | SPYT/Spark launcher | 3,167 bytes |
| `limits.png` | Скриншот/диаграмма | 106,522 bytes |
| `logs_greenhub_s3_to_bronze_manual__2026-05-08T09_15_13.252349+00_00_submit_spark_job_0_1.txt` | Лог/DDL/заметки | 10,931 bytes |
| `project392_ytsaurus_spyt_chyt_airflow_datalens_runbook.md` | Исходный runbook/заметки | 27,136 bytes |
| `raw_to_bronze.py` | Streaming Kafka component | 3,632 bytes |
| `raw_to_bronze_greenhub.py` | SPYT job: raw/batch -> bronze | 10,870 bytes |
| `raw_to_bronze_greenhub_event_str.py` | SPYT job: raw/batch -> bronze | 9,859 bytes |
| `raw_to_bronze_greenhub_fixed.py` | SPYT job: raw/batch -> bronze | 11,512 bytes |
| `raw_to_bronze_greenhub_no_udf.py` | SPYT job: raw/batch -> bronze | 9,485 bytes |
| `raw_to_bronze_greenhub_patched.py` | SPYT job: raw/batch -> bronze | 11,053 bytes |
| `raw_yt_to_bronze_greenhub.py` | SPYT job: raw/batch -> bronze | 10,301 bytes |
| `raw_yt_to_bronze_greenhub_cypress_parquet.py` | SPYT job: raw/batch -> bronze | 10,999 bytes |
| `script_table` | DDL: bronze tables | 3,994 bytes |
| `script_table.sh` | DDL: bronze tables | 4,172 bytes |
| `silver_kafka.txt` | DDL: silver tables | 2,273 bytes |
| `silver_to_gold_greenhub.py` | SPYT job: silver -> gold | 14,843 bytes |
| `silver_to_gold_greenhub_no_force_client.py` | SPYT job: silver -> gold | 13,742 bytes |
| `silver_to_gold_greenhub_patched.py` | SPYT job: silver -> gold | 13,792 bytes |
| `streaming_silver_package.zip` | Пакет файлов | 8,111 bytes |
| `streaming_silver_sql.zip` | Пакет файлов | 1,671 bytes |
| `streaming_silver_yql_dag.py` | Airflow DAG: streaming bronze -> silver через YQL | 2,557 bytes |
| `test_dag.py` | Вспомогательный файл | 444 bytes |
| `ytsaurus_chyt_spyt_airflow_report.md` | Исходный runbook/заметки | 19,051 bytes |

---

# Приложение A. Исходный batch recap

# Рекап работ по batch-пайплайну S3 → Airflow → SPYT → YTsaurus Bronze

Дата: 2026-05-08  
Контекст: дипломный MVP Data Lakehouse на российском open-source стеке.

## 1. Цель работ

Настраивали batch-ветку lakehouse:

```text
SeaweedFS S3
  -> Airflow DAG
  -> SSH на vm1
  -> launch_raw_to_bronze.sh
  -> spark-submit-yt / SPYT
  -> raw_to_bronze_greenhub.py
  -> YTsaurus bronze fact + dim
```

Целевой путь данных:

```text
s3a://greenhub/part-0000.parquet
  -> //home/bronze_stage/greenhub/fact_telemetry
  -> //home/bronze_stage/greenhub/dim_*
```

---

## 2. Airflow DAG

Файл:

```text
greenhub_s3_to_bronze.py
```

Что было реализовано:

1. DAG читает список parquet-файлов из SeaweedFS/S3.
2. DAG проверяет state-файл:

```text
_state/greenhub_load.json
```

3. Если parquet уже был успешно обработан по `source_etag`, файл пропускается.
4. Для каждого нового parquet создаётся mapped task `submit_spark_job`.
5. `submit_spark_job` ходит по SSH на `vm1`.
6. На `vm1` запускается:

```bash
/home/chig_k3s/git/repo/spyt/launch_raw_to_bronze.sh s3a://greenhub/part-0000.parquet 0
```

7. Полный лог Spark теперь пишется на `vm1`:

```text
/home/chig_k3s/git/spyt-logs/raw_to_bronze_<batch_id>_<part_index>.log
```

8. В Airflow выводятся:
   - `ERROR GREP`;
   - последние 300 строк полного remote log;
   - stdout/stderr SSH-команды.

### Исправление dynamic mapping

Было неудачно:

```python
results = submit_spark_job.expand(candidate=candidates, batch_id=[batch_id])
```

Исправлено на:

```python
results = submit_spark_job.partial(batch_id=batch_id).expand(candidate=candidates)
```

Причина: `batch_id` должен быть общим параметром для всех mapped tasks, а не отдельным mapped-списком.

### Ограничение параллелизма

Для `submit_spark_job` добавлен Airflow pool:

```python
@task(pool="spyt_pool")
def submit_spark_job(...):
    ...
```

Цель: не запускать несколько SPYT/Spark job одновременно и не забивать один SPYT worker.

---

## 3. Launcher script

Файл:

```text
launch_raw_to_bronze.sh
```

Что добавлялось и исправлялось:

1. Активация виртуального окружения:

```bash
source /home/chig_k3s/yt-env/bin/activate
```

2. Явный `JAVA_HOME`:

```bash
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
```

3. PATH с `yt-env`:

```bash
export PATH="$JAVA_HOME/bin:/home/chig_k3s/yt-env/bin:$PATH"
```

4. Чтение `YT_TOKEN` из файла:

```bash
/home/chig_k3s/.yt/token
```

5. Явный `DRIVER_HOST`:

```bash
DRIVER_HOST="${DRIVER_HOST:-10.130.0.24}"
```

6. Явный `SPARK_LOCAL_IP`:

```bash
SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-10.130.0.24}"
```

7. Проверка наличия:
   - `JOB_FILE`;
   - `S3_ACCESS_KEY`;
   - `S3_SECRET_KEY`;
   - `/home/chig_k3s/yt-env/bin/spark-submit-yt`.

### Исправление конфликта портов

Проблема:

```text
sparkDriver could not bind on port 27001
SparkUI could not bind on port 27001
BlockTransferService could not bind on port 27001
```

Причина: `27001/27002` были заняты `kubectl port-forward` до Spark master.

Добавили отдельные порты для driver/block manager/ui:

```bash
--conf spark.driver.port=28001
--conf spark.blockManager.port=28002
--conf spark.ui.port=28003
--conf spark.port.maxRetries=32
```

### Настройка ресурсов Spark job

Так как на SPYT worker планировалось дать 5 cores, launcher был изменён под схему:

```bash
--conf spark.executor.cores=2
--conf spark.executor.memory=3g
--conf spark.executor.instances=2
--conf spark.cores.max=4
--conf spark.driver.memory=2g
--conf spark.sql.shuffle.partitions=8
--conf spark.default.parallelism=8
```

Идея: использовать до 4 cores под задачу, оставив запас для master/служебных процессов.

---

## 4. SPYT / Spark cluster

Рабочая команда запуска SPYT cluster:

```bash
source ~/yt-env/bin/activate

spark-launch-yt   --proxy localhost:31103   --discovery-path //home/spark/discovery/main   --pool research   --worker-cores 5   --worker-memory 20G   --worker-memory-overhead 4G   --worker-num 1   --master-memory-limit 2G   --enable-livy   --livy-max-sessions 1   --livy-driver-cores 1   --livy-driver-memory 2G   --disable-history-server   --disable-tmpfs   --abort-existing
```

После запуска SPYT нужен port-forward:

```bash
sudo systemctl restart end0-spark-portforward
```

Проверка:

```bash
nc -vz 127.0.0.1 27001

curl -s http://127.0.0.1:27002   | grep -Ei 'Alive Workers|Cores in use|Memory in use|Status'
```

Ожидаемо:

```text
Alive Workers: 1
Cores in use: 5 Total
Memory in use: 20.0 GiB Total
Status: ALIVE
```

---

## 5. systemd port-forward service

Сервис:

```text
/etc/systemd/system/end0-spark-portforward.service
```

Назначение: держать port-forward от vm1 до Spark master внутри pod `end-0`.

Содержимое:

```ini
[Unit]
Description=Port-forward Spark master from end-0
After=network-online.target
Wants=network-online.target

[Service]
User=chig_k3s
Environment=KUBECONFIG=/etc/rancher/k3s/k3s.yaml
Restart=always
RestartSec=5
ExecStart=/usr/local/bin/kubectl --kubeconfig=/etc/rancher/k3s/k3s.yaml port-forward -n default pod/end-0 27001:27001 27002:27002

[Install]
WantedBy=multi-user.target
```

Была проблема:

```text
permission denied: /etc/rancher/k3s/k3s.yaml
```

Временно исправлялось:

```bash
sudo chmod 644 /etc/rancher/k3s/k3s.yaml
```

После этого service стал active:

```bash
sudo systemctl status end0-spark-portforward
```

---

## 6. raw_to_bronze_greenhub.py

Файл:

```text
raw_to_bronze_greenhub.py
```

Назначение:

```text
parquet из S3
  -> генерация UID
  -> fact_telemetry
  -> dim_*
```

### 6.1 Проблема `wifi_status_uid`

Ошибка:

```text
UNRESOLVED_COLUMN.WITH_SUGGESTION
A column or function parameter with name `wifi_status_uid` cannot be resolved
```

Причина: `df_fact.select(...)` ожидал колонку `wifi_status_uid`, но она не создавалась выше.

Исправление: добавлена генерация `wifi_status_uid` и dim `wifi_status`.

### 6.2 Проблема Python UDF

Ошибка:

```text
PYTHON_VERSION_MISMATCH
Python in worker has different version (3, 8) than that in driver 3.12
```

Причина: UID генерировались через Python UDF. Executor в SPYT запускал Python 3.8, а driver на vm1 работал на Python 3.12. PySpark не допускает разные minor-версии для Python UDF.

Решение: полностью убрать Python UDF.

UID теперь генерируются через Spark SQL built-in функции:

```python
sha2
concat_ws
substring
concat
```

То есть UID стал deterministic UUID-like:

```python
def stable_uuid_expr(namespace: str, *cols: str):
    parts = [F.lit(namespace)]
    for col in cols:
        parts.append(F.coalesce(F.col(col).cast("string"), F.lit("__null__")))

    h = F.sha2(F.concat_ws("|", *parts), 256)

    return F.concat(
        F.substring(h, 1, 8),
        F.lit("-"),
        F.substring(h, 9, 4),
        F.lit("-5"),
        F.substring(h, 14, 3),
        F.lit("-8"),
        F.substring(h, 17, 3),
        F.lit("-"),
        F.substring(h, 20, 12),
    )
```

Плюсы:
- нет Python UDF;
- нет Python worker mismatch;
- ключи стабильны;
- идемпотентность сохраняется;
- lineage через deterministic natural key сохраняется.

Минус:
- это UUID-like, а не строгий RFC UUIDv5.

Для MVP это принято как инженерный компромисс.

### 6.3 Проблема `timestamp`

В исходном parquet `timestamp` нормальный:

```text
2020-05-29 03:49:06
2020-05-29 03:58:46
2020-05-29 14:32:33
```

Но после записи через SPYT/YTsaurus writer получалось:

```text
1970-01-03T19:27:22Z
```

Вывод: проблема не в parquet, а в маппинге Spark timestamp / timestamp_ntz в YTsaurus timestamp.

Решение для bronze: не писать `timestamp` как YTsaurus timestamp. Вместо этого писать строки:

```text
event_ts_str = "2020-05-29 03:49:06"
event_date   = "2020-05-29"
```

В bronze это допустимо, потому что bronze-слой хранит данные максимально близко к исходнику и без потерь. Строгую типизацию можно сделать дальше в silver.

В job добавлено:

```python
df_raw = (
    df_raw
    .withColumn("event_ts", F.col("timestamp").cast("timestamp"))
    .withColumn("event_ts_str", F.date_format(F.col("event_ts"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("event_date", F.date_format(F.to_date(F.col("event_ts")), "yyyy-MM-dd"))
    .drop("event_ts")
    .cache()
)
```

В `fact_uid` теперь используется `event_ts_str`:

```python
.withColumn("fact_uid", stable_uuid_expr("fact_telemetry", "id", "device_id", "event_ts_str"))
```

В fact вместо `timestamp` пишутся:

```text
event_ts_str
event_date
```

В dimension tables `first_seen` также стал строкой:

```text
first_seen string
```

---

## 7. Bronze tables

Файл создания таблиц:

```text
create_bronze_tables_event_str.sh
```

Назначение:
- создать `//home/bronze_stage`;
- создать `//home/bronze_stage/greenhub`;
- удалить старые fact/dim;
- пересоздать таблицы с новой схемой.

Основные таблицы:

```text
//home/bronze_stage/greenhub/fact_telemetry
//home/bronze_stage/greenhub/dim_device
//home/bronze_stage/greenhub/dim_country
//home/bronze_stage/greenhub/dim_timezone
//home/bronze_stage/greenhub/dim_battery_state
//home/bronze_stage/greenhub/dim_network_status
//home/bronze_stage/greenhub/dim_charger
//home/bronze_stage/greenhub/dim_health
//home/bronze_stage/greenhub/dim_network_type
//home/bronze_stage/greenhub/dim_mobile_network_type
//home/bronze_stage/greenhub/dim_mobile_data_status
//home/bronze_stage/greenhub/dim_mobile_data_activity
//home/bronze_stage/greenhub/dim_wifi_status
```

Команда пересоздания:

```bash
chmod +x create_bronze_tables_event_str.sh
./create_bronze_tables_event_str.sh
```

Ручное удаление:

```bash
yt remove --force //home/bronze_stage/greenhub/fact_telemetry
yt remove --force //home/bronze_stage/greenhub/dim_device

for dim in country timezone battery_state network_status charger health            network_type mobile_network_type mobile_data_status            mobile_data_activity wifi_status; do
  yt remove --force //home/bronze_stage/greenhub/dim_${dim}
done
```

---

## 8. S3 / SeaweedFS

SeaweedFS был поднят отдельно от k3s.

Проверка доступности S3:

```bash
curl -I http://10.130.0.35:8333
```

Ответ:

```text
405 Method Not Allowed
```

Это нормально для S3 endpoint на `HEAD /`.

Проверка bucket:

```bash
aws --endpoint-url http://10.130.0.35:8333 s3 ls
aws --endpoint-url http://10.130.0.35:8333 s3 ls s3://greenhub/
aws --endpoint-url http://10.130.0.35:8333 s3 ls s3://greenhub/ --recursive | head
```

Bucket:

```text
greenhub
```

Файл:

```text
part-0000.parquet
```

---

## 9. Ошибки, которые были пройдены

### 9.1 Не было S3 credentials в Airflow

Ошибка:

```text
NoCredentialsError: Unable to locate credentials
```

Причина: Airflow не нашёл connection или variables.

Исправлялось:
- Airflow connection `seaweedfs_s3`;
- Variables `S3_ACCESS_KEY`, `S3_SECRET_KEY`.

### 9.2 Не было SSH connection

Ошибка:

```text
AirflowNotFoundException: The conn_id `vm1_ssh` isn't defined
```

Исправление:
- создан Airflow SSH connection `vm1_ssh`.

### 9.3 Permission denied на launcher

Ошибка:

```text
launch_raw_to_bronze.sh: Permission denied
```

Исправление:

```bash
chmod +x /home/chig_k3s/git/repo/spyt/launch_raw_to_bronze.sh
```

### 9.4 Не найден job-файл

Ошибка:

```text
ERROR: job file not found: /home/chig_k3s/git/repo/spyt/jobs/raw_to_bronze_greenhub.py
```

Причина: неправильный путь в launcher.

Исправление:
- `JOB_FILE="$SCRIPT_DIR/raw_to_bronze_greenhub.py"`.

### 9.5 `spark-submit-yt: command not found`

Причина: SSH-сессия не видела `yt-env/bin`.

Исправление:
- явный PATH;
- запуск через полный путь:

```bash
/home/chig_k3s/yt-env/bin/spark-submit-yt
```

### 9.6 Нет YT_TOKEN

Ошибка:

```text
YtTokenError: Client is missing credentials
```

Исправление:
- launcher читает `/home/chig_k3s/.yt/token`.

### 9.7 Spark master / executor network issues

Ошибки:

```text
No route to host
All masters are unresponsive
Disconnected from Spark cluster
Lost executor
```

Исправления:
- systemd port-forward;
- `/etc/hosts` для `end-0.exec-nodes.default.svc.cluster.local`;
- отдельные порты driver/blockManager/ui;
- проверка `nc`;
- перезапуск SPYT cluster через `spark-launch-yt --abort-existing`.

### 9.8 Initial job has not accepted resources

Ошибка:

```text
Initial job has not accepted any resources
```

Причина: SPYT worker не был доступен / висели старые apps / ресурсов не хватало.

Исправления:
- kill старых Spark apps;
- перезапуск SPYT;
- ограничение ресурсов job;
- Airflow pool.

### 9.9 `wifi_status_uid` missing

Ошибка:

```text
UNRESOLVED_COLUMN: wifi_status_uid
```

Исправление:
- добавлена генерация `wifi_status_uid`;
- добавлена `dim_wifi_status`.

### 9.10 Python version mismatch

Ошибка:

```text
PYTHON_VERSION_MISMATCH
worker Python 3.8, driver Python 3.12
```

Исправление:
- удалены Python UDF;
- UID генерируются Spark SQL built-ins.

### 9.11 Timestamp стал 1970

Ошибка по данным:

```text
1970-01-03T...
```

Причина: некорректная запись Spark timestamp в YTsaurus timestamp.

Исправление:
- в bronze пишем `event_ts_str` и `event_date` как string;
- timestamp-типизацию переносим в silver.

---

## 10. Текущий статус

На момент фикса:

1. SeaweedFS S3 доступен.
2. Airflow DAG запускает batch через SSH.
3. SPYT cluster запускается.
4. Spark читает parquet из S3.
5. Spark считает строки:

```text
Source rows: 704,233
```

6. Spark пишет fact в YTsaurus.
7. Основные сетевые проблемы пройдены.
8. Python UDF проблема устранена через built-in Spark SQL.
9. Timestamp проблема устранена переносом времени в строковые поля bronze.

---

## 11. Что осталось проверить

После установки `raw_to_bronze_greenhub_event_str.py` и пересоздания bronze-таблиц:

1. Запустить DAG.
2. Проверить количество строк:

```bash
yt get //home/bronze_stage/greenhub/fact_telemetry/@row_count
```

3. Проверить первые строки:

```bash
yt read-table //home/bronze_stage/greenhub/fact_telemetry --format json | head
```

4. Проверить timestamp:

```text
event_ts_str должен быть 2020-...
event_date должен быть 2020-...
```

5. Проверить справочники:

```bash
yt list //home/bronze_stage/greenhub
yt get //home/bronze_stage/greenhub/dim_device/@row_count
yt get //home/bronze_stage/greenhub/dim_wifi_status/@row_count
```

6. Проверить state-файл в S3:

```bash
aws --endpoint-url http://10.130.0.35:8333 s3 cp s3://greenhub/_state/greenhub_load.json -
```

---

## 12. Что будет после выключения серверов

Данные должны сохраниться, если используются постоянные диски:

```text
YTsaurus data/master/tablet node данные
SeaweedFS S3 данные
Git repo
Airflow metadata, если Postgres/PVC на постоянном диске
```

После включения нужно проверить и поднять runtime:

```bash
sudo systemctl status k3s
kubectl get pods -A

sudo systemctl status seaweedfs
sudo systemctl status end0-spark-portforward
```

SPYT cluster почти наверняка нужно запустить заново:

```bash
source ~/yt-env/bin/activate

spark-launch-yt   --proxy localhost:31103   --discovery-path //home/spark/discovery/main   --pool research   --worker-cores 5   --worker-memory 20G   --worker-memory-overhead 4G   --worker-num 1   --master-memory-limit 2G   --enable-livy   --livy-max-sessions 1   --livy-driver-cores 1   --livy-driver-memory 2G   --disable-history-server   --disable-tmpfs   --abort-existing

sudo systemctl restart end0-spark-portforward
```

Итог: заново всё шаманить не нужно, но после shutdown нужен чек-лист запуска:
- k3s;
- YTsaurus pods;
- SeaweedFS;
- Airflow;
- SPYT cluster;
- port-forward service.

---

## 13. Инженерный вывод

SPYT как идея нормален, но внешний submit через SSH + port-forward оказался хрупким:

```text
Airflow pod
  -> SSH
  -> vm1 venv
  -> spark-submit-yt
  -> port-forward
  -> Spark master inside YTsaurus operation
  -> executor inside pod
  -> reverse connection back to driver
```

Для диплома это можно честно описать как эксплуатационный trade-off self-managed lakehouse.

При этом в итоге batch-ветка была доведена до рабочего состояния через:
- явные env;
- полный remote logging;
- фиксацию портов;
- отказ от Python UDF;
- перенос timestamp-типизации из bronze в silver;
- state-as-JSON для идемпотентности.


---

# Приложение B. Исходный CHYT/SPYT/Airflow report

# YTsaurus / CHYT / SPYT / Airflow: разбор проблем и текущие настройки

Дата контекста: 2026-05-10  
Основная VM: `vm1-k3s-ing-server`  
Кластер: k3s + YTsaurus + CHYT + SPYT + Airflow + Kafka + SeaweedFS/S3

---

## 1. Итог в одну строку

CHYT заработал после того, как:
1. на exec-node поставили Python-пакет `requests`, нужный `clickhouse-trampoline`;
2. в `//sys/@cluster_connection` заменили проблемные Kubernetes FQDN на IPv4 для master/master_cache/discovery;
3. перезапустили `ch_public`;
4. проверили smoke test:

```bash
yt query --format json chyt "SELECT 1 + 1 AS x"
```

Финальный результат:

```json
{"x":2}
```

---

## 2. Главная причина CHYT-инцидента

Ошибка сверху выглядела так:

```text
Ensure that the clique "ch_public" has started properly and its jobs are successfully running
Group "/chyt/ch_public" does not exist
```

Но это был симптом. Реальная причина была в stderr CHYT job:

```text
Error reporting heartbeat 10 times on startup
There are not enough healthy servers known
method Heartbeat
address ds-0.discovery.default.svc.cluster.local:9020
service DiscoveryClientService

Ares DNS resolve failed for "ds-0.discovery.default.svc.cluster.local"
enable_ipv4 false
enable_ipv6 true
DNS server returned answer with no data
```

То есть обычный `getent` и `nc` внутри pod работали, но `ytserver-clickhouse` использовал c-ares DNS и почему-то пытался IPv6-only:

```text
enable_ipv4 false
enable_ipv6 true
```

Kubernetes DNS отдавал IPv4, а AAAA-записи не было. Поэтому CHYT не мог heartbeat-иться в discovery, discovery group `/chyt/ch_public` не создавалась, Query Tracker не видел clique.

---

## 3. Компоненты и адреса

В диагностике участвовали:

```text
ms-0.masters.default.svc.cluster.local:9010
hp-0.http-proxies.default.svc.cluster.local
http-proxies-lb.default.svc.cluster.local
qt-0.query-trackers.default.svc.cluster.local:9028
ds-0.discovery.default.svc.cluster.local:9020
qa-0.queue-agents.default.svc.cluster.local:9030
end-0.exec-nodes.default.svc.cluster.local:9029
end-1.exec-nodes.default.svc.cluster.local:9029
```

Разрешенные IP:

```text
ms-0 -> 10.42.2.166
qt-0 -> 10.42.4.52
qa-0 -> 10.42.2.172
ds-0 -> 10.42.4.48
end-0 -> 10.42.1.170
end-1 -> 10.42.2.176
http-proxies-lb -> 10.43.106.181
```

---

## 4. DNS на vm1

Состояние после правки:

```bash
ls -l /etc/resolv.conf
resolvectl status | sed -n '1,120p'
```

Было:

```text
/etc/resolv.conf -> /run/systemd/resolve/stub-resolv.conf

Global:
DNS Server: 10.43.0.10
DNS Domain: ~cluster.local

Link eth0:
DNS Server: 10.130.0.2
DNS Domain: ru-central1.internal auto.internal
```

Проверки стали успешными:

```bash
getent hosts end-0.exec-nodes.default.svc.cluster.local
getent hosts end-1.exec-nodes.default.svc.cluster.local
getent hosts http-proxies-lb.default.svc.cluster.local
```

Результат:

```text
10.42.1.170     end-0.exec-nodes.default.svc.cluster.local
10.42.2.176     end-1.exec-nodes.default.svc.cluster.local
10.43.106.181   http-proxies-lb.default.svc.cluster.local
```

Но важно: это починило системный resolver на `vm1`, а не c-ares внутри `ytserver-clickhouse`.

---

## 5. Query Tracker

Первоначально запрос:

```bash
yt get-query 70824661-b52c971-890e8f0-7de3271e
```

показывал:

```text
engine = "chyt"
query = "SELECT 1 + 1 AS x"
state = "pending"
```

В логах Query Tracker были ошибки доступа к master:

```text
Error getting mount info for //sys/query_tracker/active_queries
Request timed out
method Execute
address ms-0.masters.default.svc.cluster.local:9010
```

И позже:

```text
Error generating fresh timestamps
method GenerateTimestamps
address ms-0.masters.default.svc.cluster.local:9010
```

Это тоже указывало на проблемы сетевого/DNS-доступа к YTsaurus master.

---

## 6. SPYT

Было состояние:

```text
spyt-cluster.service loaded failed failed Launch SPYT cluster
```

SPYT/Livy operation при этом была видна:

```bash
yt list-operations --state running --format json | jq '.operations[] | {id, type, title, state}'
```

Пример:

```json
{
  "id": "2fb1f0f7-acdced25-103e8-783d45ea",
  "type": "vanilla",
  "title": null,
  "state": "running"
}
```

`yt get-operation "$OP"` показывал много записей:

```text
job_type = "livy"
```

Airflow/Spark падал на подключении к Spark master:

```text
WARN StandaloneAppClient$ClientEndpoint:
Failed to connect to master end-1.exec-nodes.default.svc.cluster.local:27001
```

Проверка после восстановления DNS:

```bash
nc -vz end-0.exec-nodes.default.svc.cluster.local 27001 || true
nc -vz end-1.exec-nodes.default.svc.cluster.local 27001 || true
```

Результат:

```text
end-0:27001 -> Connection refused
end-1:27001 -> succeeded
```

Вывод: SPYT master был на `end-1:27001`. Для Airflow/Spark нужен стабильный DNS/route до Kubernetes pod/service сети.

---

## 7. Airflow

### DAG

```text
greenhub_silver_to_gold
```

Задачи:

```text
list_marts
make_batch_id
submit_gold_mart
mark_gold_runs
```

### submit_gold_mart

Airflow запускал SSH-команду на `vm1`:

```bash
bash /home/chig_k3s/git/repo/spyt/launch_silver_to_gold.sh daily_country_stats   > /home/chig_k3s/git/spyt-logs/silver_to_gold_daily_country_stats_<batch_id>.log 2>&1
```

Переменные:

```bash
export BATCH_ID=<uuid>
export YT_PROXY=***
export YT_USE_HOSTS=0
export DRIVER_HOST=10.130.0.24
export SPARK_LOCAL_IP=10.130.0.24
```

Ошибка:

```text
Failed to connect to master end-1.exec-nodes.default.svc.cluster.local:27001
```

Это относилось к SPYT/Spark connectivity, а не к CHYT.

### mark_gold_runs

Падение было по SeaweedFS/S3:

```text
ConnectTimeoutError:
http://10.130.0.35:8333/greenhub/_state/greenhub_gold_runs.json
```

Позже выяснилось, что VM с этим endpoint была недоступна/умерла. Это отдельная проблема, не CHYT.

Также был warning:

```text
AWS Connection (conn_id='seaweedfs_s3', conn_type='git') expected connection type 'aws', got 'git'
```

Лучше исправить connection type на `aws`, если используется `S3Hook`.

---

## 8. Kafka

Kafka напрямую не была финальным блокером CHYT.

Был активен:

```text
kafka-portforward.service loaded active running Kafka broker port-forward 9094
```

Проверки на будущее:

```bash
systemctl status kafka-portforward.service --no-pager
ss -ltnp | grep 9094 || true
nc -vz localhost 9094
```

---

## 9. CHYT: первая проблема — отсутствовал requests

Первые CHYT jobs падали так:

```text
Traceback (most recent call last):
  File "./clickhouse-trampoline", line 12, in <module>
    import requests
ModuleNotFoundError: No module named 'requests'
```

Временный фикс:

```bash
kubectl exec -n default end-0 -- sh -lc 'python3 -m pip install requests || pip3 install requests'
kubectl exec -n default end-1 -- sh -lc 'python3 -m pip install requests || pip3 install requests'
```

Проверка:

```bash
kubectl exec -n default end-0 -- sh -lc 'python3 -c "import requests; print(requests.__version__)"'
kubectl exec -n default end-1 -- sh -lc 'python3 -c "import requests; print(requests.__version__)"'
```

Результат:

```text
2.32.4
2.32.4
```

Важно: это не постоянный фикс. После пересоздания pod пакет исчезнет, если не добавить его в image/init/bootstrap.

---

## 10. CHYT: почему ресурсы не были корневой причиной

CHYT speclet:

```yson
{
    pool = "research";
    active = %true;
    instance_count = 1;
    cpu_limit = 16;
    memory_limit = 68719476736;
}
```

То есть:

```text
1 instance
16 CPU
64 GiB memory
```

В generated operation spec было около:

```text
cpu_limit = 16
memory_limit = 69793218560
```

Ресурсы были выставлены большими, чтобы исключить гипотезы:

- не хватает CPU;
- не хватает memory;
- pool не дает ресурсов;
- exec-node не принимает job;
- job умирает из-за лимитов.

Фактическая проблема была не в ресурсах, а в:

1. отсутствии `requests`;
2. c-ares DNS IPv6-only поведении;
3. FQDN в `cluster_connection`.

Фактический пример resource usage:

```text
end-0:
user_slots = 0
cpu = 0
user_memory = 0

end-1:
user_slots = 3
cpu = 7
user_memory = 16508780544
```

После стабилизации можно пробовать уменьшить CHYT:

```yson
{
    pool = "research";
    active = %true;
    instance_count = 1;
    cpu_limit = 4;
    memory_limit = 17179869184;
}
```

Но для production объем подбирать по реальным запросам.

---

## 11. CHYT: что именно починили в cluster_connection

Сначала был backup:

```bash
yt get //sys/@cluster_connection > /tmp/cluster_connection.before.yson
```

Получение IP:

```bash
MS_IP=$(getent hosts ms-0.masters.default.svc.cluster.local | awk '{print $1}' | head -1)
QT_IP=$(getent hosts qt-0.query-trackers.default.svc.cluster.local | awk '{print $1}' | head -1)
QA_IP=$(getent hosts qa-0.queue-agents.default.svc.cluster.local | awk '{print $1}' | head -1)
DS_IP=$(getent hosts ds-0.discovery.default.svc.cluster.local | awk '{print $1}' | head -1)

echo "MS_IP=$MS_IP QT_IP=$QT_IP QA_IP=$QA_IP DS_IP=$DS_IP"
```

Получилось:

```text
MS_IP=10.42.2.166
QT_IP=10.42.4.52
QA_IP=10.42.2.172
DS_IP=10.42.4.48
```

Фиксы master:

```bash
MS_IP=$(getent hosts ms-0.masters.default.svc.cluster.local | awk '{print $1}' | head -1)

yt set //sys/@cluster_connection/primary_master/addresses "["${MS_IP}:9010"]"
yt set //sys/@cluster_connection/primary_master/peers "[{address="${MS_IP}:9010"; voting=%true;}]"
yt set //sys/@cluster_connection/master_cache/addresses "["${MS_IP}:9010"]"
```

Фикс discovery:

```bash
DS_IP=$(getent hosts ds-0.discovery.default.svc.cluster.local | awk '{print $1}' | head -1)
yt set //sys/@cluster_connection/discovery_connection/addresses "["${DS_IP}:9020"]"
```

Ключевое рабочее состояние:

```text
primary_master.addresses = ["10.42.2.166:9010"]
primary_master.peers = [{address="10.42.2.166:9010"; voting=%true;}]
master_cache.addresses = ["10.42.2.166:9010"]
discovery_connection.addresses = ["10.42.4.48:9020"]
```

---

## 12. Перезапуск CHYT

Рабочая последовательность:

```bash
OP=$(yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_id 2>/dev/null | tr -d '"')
[ -n "$OP" ] && yt abort-op "$OP" || true

yt remove //sys/clickhouse/strawberry/ch_public --recursive --force || true

kubectl exec -n default deploy/strawberry-controller -- sh -lc '
cat > /tmp/chyt_public.yson <<EOF
{
    pool = "research";
    active = %true;
    instance_count = 1;
    cpu_limit = 16;
    memory_limit = 68719476736;
}
EOF

export YT_PROXY=http-proxies-lb.default.svc.cluster.local

/usr/bin/chyt-controller one-shot-run   --config-path /config/strawberry-controller.yson   --log-to-stderr   --family chyt   --alias ch_public   --speclet-path /tmp/chyt_public.yson
'
```

---

## 13. Проверка CHYT

```bash
OP=$(yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_id | tr -d '"')
echo "OP=$OP"

yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_state

yt list-jobs "$OP"   --include-runtime   --include-archive   --include-cypress   --limit 20   --format json | jq '.state_counts, [.jobs[] | {id,state,stderr_size,address,start_time,finish_time}]'

yt query --format json chyt "SELECT 1 + 1 AS x"
```

Финальный успешный вывод:

```text
OP=3324ad85-6cf22f9f-103e8-37004c7a
state_counts = {"running": 1}
```

И:

```json
{"x":2}
```

`yt_operation_state` мог еще показывать:

```text
"initializing"
```

Но если SQL проходит, clique уже usable. Persistent state может отставать от фактической регистрации в discovery.

---

## 14. Диагностические команды на будущее

### CHYT operation

```bash
OP=$(yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_id | tr -d '"')
echo "$OP"

yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state

yt list-jobs "$OP"   --include-runtime   --include-archive   --include-cypress   --limit 20   --format json | jq '.state_counts, [.jobs[] | {id,state,stderr_size,address,start_time,finish_time}]'
```

### stderr failed job

```bash
JOB=$(yt list-jobs "$OP"   --include-runtime   --include-archive   --include-cypress   --limit 50   --format json | jq -r '.jobs[] | select(.stderr_size > 1) | .id' | head -1)

echo "OP=$OP"
echo "JOB=$JOB"

yt get-job-stderr "$JOB" "$OP" | tail -300
```

В этой CLI порядок, который реально использовался:

```bash
yt get-job-stderr "$JOB" "$OP"
```

Если появляется `No such operation`, проверить порядок аргументов.

### Проверка процесса CHYT внутри exec-node

```bash
kubectl exec -n default end-1 -- sh -lc '
ps -ef | grep -E "ytserver-clickhouse|clickhouse-trampoline" | grep -v grep || true

PID=$(pgrep -f "^./ytserver-clickhouse" | head -1 || true)
echo "PID=$PID"

if [ -n "$PID" ]; then
  CWD=$(readlink /proc/$PID/cwd)
  echo "CWD=$CWD"

  grep -nEi "primary_master|master_cache|addresses|10\.42|ms-0|discovery|ch_public|health_checker|sample_table" "$CWD/config_patched.yson" | head -200 || true

  grep -nEi "Ares|Dns|sample_table|health|discovery|/chyt|ch_public|group|register|heartbeat|error|exception|failed|timeout|master"     "$CWD"/clickhouse.log "$CWD"/clickhouse.debug.log "$CWD"/clickhouse.error.log 2>/dev/null | tail -250 || true
fi
'
```

### DNS и connectivity

```bash
getent hosts ds-0.discovery.default.svc.cluster.local
nc -vz ds-0.discovery.default.svc.cluster.local 9020

kubectl exec -n default end-1 -- sh -lc '
getent hosts ds-0.discovery.default.svc.cluster.local
nc -vz ds-0.discovery.default.svc.cluster.local 9020 || true
getent hosts ms-0.masters.default.svc.cluster.local
nc -vz ms-0.masters.default.svc.cluster.local 9010 || true
'
```

### Smoke test

```bash
yt query --format json chyt "SELECT 1 + 1 AS x"
```

Ожидаемо:

```json
{"x":2}
```

---

## 15. Что сделать постоянным

### 15.1. Сохранить рабочее состояние

```bash
yt get //sys/@cluster_connection > ~/cluster_connection.chyt_working.yson
yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state > ~/chyt_ch_public_state_working.yson
```

### 15.2. Bootstrap для cluster_connection

```bash
#!/usr/bin/env bash
set -euo pipefail

MS_IP=$(getent hosts ms-0.masters.default.svc.cluster.local | awk '{print $1}' | head -1)
DS_IP=$(getent hosts ds-0.discovery.default.svc.cluster.local | awk '{print $1}' | head -1)

test -n "$MS_IP"
test -n "$DS_IP"

yt set //sys/@cluster_connection/primary_master/addresses "["${MS_IP}:9010"]"
yt set //sys/@cluster_connection/primary_master/peers "[{address="${MS_IP}:9010"; voting=%true;}]"
yt set //sys/@cluster_connection/master_cache/addresses "["${MS_IP}:9010"]"
yt set //sys/@cluster_connection/discovery_connection/addresses "["${DS_IP}:9020"]"

yt get //sys/@cluster_connection | grep -nEi '10\.42|primary_master|master_cache|discovery_connection'
```

Запускать после поднятия YTsaurus или после пересоздания service IP, если IP меняются.

### 15.3. Вшить `requests` в exec-node image

Ручной вариант временный:

```bash
kubectl exec -n default end-0 -- python3 -m pip install requests
kubectl exec -n default end-1 -- python3 -m pip install requests
```

Нужно одно из:

1. свой image exec-node с `requests`;
2. initContainer;
3. bootstrap/lifecycle hook;
4. исправление поставки `clickhouse-trampoline`, чтобы зависимости были в образе.

Проверка:

```bash
kubectl exec -n default end-0 -- sh -lc 'python3 -c "import requests; print(requests.__version__)"'
kubectl exec -n default end-1 -- sh -lc 'python3 -c "import requests; print(requests.__version__)"'
```

### 15.4. Airflow connection для S3

Исправить:

```text
conn_id = seaweedfs_s3
conn_type = aws
```

И проверить endpoint:

```bash
nc -vz 10.130.0.35 8333
curl -v http://10.130.0.35:8333/
```

### 15.5. SPYT master connectivity

Перед DAG:

```bash
getent hosts end-1.exec-nodes.default.svc.cluster.local
nc -vz end-1.exec-nodes.default.svc.cluster.local 27001
```

Если master может переезжать, не хардкодить `end-1`; получать актуальный endpoint из SPYT/YTsaurus discovery/operation.

---

## 16. Чего не делать снова

1. Не лечить `Group "/chyt/ch_public" does not exist` port-forward-ом.
2. Не считать, что если `getent` работает, то c-ares внутри CHYT тоже работает.
3. Не увеличивать ресурсы бесконечно, если stderr показывает DNS/heartbeat.
4. Не оставлять `pip install requests` внутри live pod как финальный production fix.
5. Не пересоздавать CHYT без backup `//sys/@cluster_connection`.
6. Не путать SeaweedFS timeout с CHYT-инцидентом.

---

## 17. Короткий финальный вывод

Корень CHYT проблемы:

```text
ytserver-clickhouse не мог зарегистрироваться в discovery,
потому что c-ares пытался резолвить Kubernetes FQDN как IPv6-only,
а DNS отдавал только IPv4.
```

Рабочий обход:

```text
заменить critical endpoints в //sys/@cluster_connection с FQDN на IPv4.
```

После этого:

```bash
yt query --format json chyt "SELECT 1 + 1 AS x"
```

вернул:

```json
{"x":2}
```


---

# Приложение C. Исходный общий runbook

# Project 392: YTsaurus / SPYT / CHYT / Airflow / DataLens runbook

Дата: 2026-05-10  
Среда: k3s-кластер `minisaurus`, YTsaurus, SPYT, CHYT, Airflow, SeaweedFS/S3, DataLens.  
Основная VM/точка входа: `vm1-k3s-ing-server`.  
Публичный endpoint YTsaurus HTTP proxy: `http://<PUBLIC_VM1_IP>:31103`.

> Реальные токены не сохраняются. Везде используй `<YT_TOKEN>`.

---

## 1. Финальный итог

К концу разбора:

- Airflow поднят.
- SPYT silver-to-gold дошел до рабочих запусков после фикса DNS/сети.
- CHYT clique `ch_public` заработала.
- Проверочный CHYT-запрос успешно вернул `{"x":2}`:

```bash
yt query --format json chyt "SELECT 1 + 1 AS x"
```

- YTsaurus HTTP proxy доступен снаружи через NodePort `31103`.
- Ответ `401 Client is missing credentials` снаружи — нормальный сетевой признак: порт открыт, прокси отвечает, нужна авторизация.
- Авторизованный HTTP API-запрос с OAuth-токеном работает.

---

## 2. Топология и важные адреса

### Kubernetes services

```bash
kubectl get svc -n default | grep -Ei 'http|proxy|yt|lb'
kubectl get svc -n default http-proxies-lb -o wide
kubectl get svc -n default http-proxies-lb -o yaml | sed -n '1,160p'
```

Ключевые сервисы:

```text
http-proxies-lb       NodePort   10.43.106.181   <none>   80:31103/TCP
ytsaurus-ui           NodePort   10.43.38.174    <none>   80:32118/TCP
```

`http-proxies-lb`:

```yaml
spec:
  clusterIP: 10.43.106.181
  ports:
  - name: http
    nodePort: 31103
    port: 80
    protocol: TCP
    targetPort: 80
  selector:
    yt_component: minisaurus-yt-http-proxy
  type: NodePort
```

### Внутренние компоненты

```text
http-proxies-lb.default.svc.cluster.local:80
hp-0.http-proxies.default.svc.cluster.local
ms-0.masters.default.svc.cluster.local:9010
qt-0.query-trackers.default.svc.cluster.local:9028
qa-0.queue-agents.default.svc.cluster.local:9030
ds-0.discovery.default.svc.cluster.local:9020
end-0.exec-nodes.default.svc.cluster.local:9029
end-1.exec-nodes.default.svc.cluster.local:9029
```

NodePort:

```text
vm1 public:  <PUBLIC_VM1_IP>:31103
vm1 private: 10.130.0.24:31103
vm1 local:   localhost:31103
```

---

## 3. Форматы команд: YSON vs JSON

### YT CLI часто ждет YSON

Правильно:

```bash
yt update-op-parameters "$OP" '{
    acl = [
        {
            action = allow;
            subjects = ["admin"; "root"; "users"];
            permissions = ["read"; "manage"];
        };
    ];
}'
```

Неправильно:

```bash
yt update-op-parameters "$OP" '{
  "acl": [
    {
      "action": "allow"
    }
  ]
}'
```

Ошибка:

```text
Unexpected token ":" of type Colon; expected types are ['Equals']
```

### HTTP API ждет JSON в `X-YT-Parameters`

Правильно:

```bash
curl -sS \
  -H "Authorization: OAuth ${YT_TOKEN}" \
  -H 'X-YT-Parameters: {"path":"//sys"}' \
  "http://<PUBLIC_VM1_IP>:31103/api/v4/list"
```

Неправильно:

```bash
curl -sS \
  -H "Authorization: OAuth ${YT_TOKEN}" \
  -H "X-YT-Parameters: {path=\"//sys\";}" \
  "http://<PUBLIC_VM1_IP>:31103/api/v4/list"
```

Ошибка:

```text
Unable to parse parameters from headers
Error parsing JSON
lexical error: invalid char in json text.
```

---

## 4. DNS на VM1

### Симптом

С `vm1` сначала не резолвились Kubernetes service FQDN:

```bash
getent hosts end-0.exec-nodes.default.svc.cluster.local
getent hosts end-1.exec-nodes.default.svc.cluster.local
getent hosts http-proxies-lb.default.svc.cluster.local

nc -vz end-0.exec-nodes.default.svc.cluster.local 27001 || true
nc -vz end-1.exec-nodes.default.svc.cluster.local 27001 || true
```

Ошибка:

```text
Temporary failure in name resolution
```

### Диагностика

```bash
ls -l /etc/resolv.conf
resolvectl status | sed -n '1,120p'

resolvectl query end-0.exec-nodes.default.svc.cluster.local
resolvectl query end-1.exec-nodes.default.svc.cluster.local
resolvectl query http-proxies-lb.default.svc.cluster.local
```

Рабочее состояние:

```text
/etc/resolv.conf -> /run/systemd/resolve/stub-resolv.conf

Global
  Current DNS Server: 10.43.0.10
  DNS Servers: 10.43.0.10
  DNS Domain: ~cluster.local
```

Проверка:

```bash
getent hosts end-0.exec-nodes.default.svc.cluster.local
getent hosts end-1.exec-nodes.default.svc.cluster.local
getent hosts http-proxies-lb.default.svc.cluster.local
```

Ожидаемо:

```text
10.42.1.170     end-0.exec-nodes.default.svc.cluster.local
10.42.2.176     end-1.exec-nodes.default.svc.cluster.local
10.43.106.181   http-proxies-lb.default.svc.cluster.local
```

### Вывод

Для CLI и Airflow-задач, которые запускаются с `vm1`, нужно, чтобы `*.svc.cluster.local` резолвился через CoreDNS `10.43.0.10`, а `cluster.local` шел в него как routing domain `~cluster.local`.

---

## 5. SPYT / Spark

### Симптом в Airflow

DAG:

```text
greenhub_silver_to_gold
```

Task:

```text
submit_gold_mart
```

Лог:

```text
WARN StandaloneAppClient$ClientEndpoint: Failed to connect to master end-1.exec-nodes.default.svc.cluster.local:27001
```

Airflow запускал SSH-команду на `vm1`:

```bash
export BATCH_ID=<uuid>
export YT_PROXY=***
export YT_USE_HOSTS=0
export DRIVER_HOST=10.130.0.24
export SPARK_LOCAL_IP=10.130.0.24

bash /home/chig_k3s/git/repo/spyt/launch_silver_to_gold.sh daily_country_stats \
  > /home/chig_k3s/git/spyt-logs/silver_to_gold_daily_country_stats_<BATCH_ID>.log 2>&1
```

### Диагностика

```bash
nc -vz end-0.exec-nodes.default.svc.cluster.local 27001 || true
nc -vz end-1.exec-nodes.default.svc.cluster.local 27001 || true
```

Рабочая картина после фикса DNS:

```text
end-0:27001 -> Connection refused
end-1:27001 -> succeeded
```

### Вывод

SPYT master был на `end-1:27001`. Для стабильности:

- `*.svc.cluster.local` должен резолвиться на `vm1`;
- `DRIVER_HOST` и `SPARK_LOCAL_IP` должны указывать на IP `vm1`, доступный из кластера: `10.130.0.24`;
- `YT_PROXY` должен указывать на рабочий YTsaurus HTTP proxy;
- если SPYT master переедет, endpoint нужно обновить.

---

## 6. Airflow и SeaweedFS/S3

### Что заработало

Airflow поднят, DAG `greenhub_silver_to_gold` начал выполнять:

```text
list_marts
make_batch_id
submit_gold_mart
mark_gold_runs
```

### Ошибка SeaweedFS/S3

На `mark_gold_runs`:

```text
ConnectTimeoutError: Connect timeout on endpoint URL:
"http://10.130.0.35:8333/greenhub/_state/greenhub_gold_runs.json"
```

Причина: VM/узел `10.130.0.35:8333` был недоступен/умер. Это не была проблема CHYT/SPYT/YTsaurus.

### Предупреждение Airflow connection

```text
AWS Connection (conn_id='seaweedfs_s3', conn_type='git') expected connection type 'aws', got 'git'
```

Нужно поправить Airflow Connection:

```text
conn_id: seaweedfs_s3
conn_type: aws
endpoint_url: http://<seaweedfs-host>:8333
```

---

## 7. CHYT проблема 1: нет Python-модуля `requests`

### Симптом

CHYT jobs падали:

```bash
OP=$(yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_id | tr -d '"')

yt list-jobs "$OP" \
  --include-runtime \
  --include-archive \
  --include-cypress \
  --limit 50 \
  --format json | jq '.state_counts'
```

Stderr:

```bash
JOB=<job-id>
yt get-job-stderr "$JOB" "$OP" | cat -A
```

Ошибка:

```text
Traceback (most recent call last):
  File "./clickhouse-trampoline", line 12, in <module>
    import requests
ModuleNotFoundError: No module named 'requests'
```

### Быстрый фикс

```bash
kubectl exec -n default end-0 -- sh -lc 'python3 -m pip install requests || pip3 install requests'
kubectl exec -n default end-1 -- sh -lc 'python3 -m pip install requests || pip3 install requests'
```

Проверка:

```bash
kubectl exec -n default end-0 -- sh -lc 'python3 -c "import requests; print(requests.__version__)"'
kubectl exec -n default end-1 -- sh -lc 'python3 -c "import requests; print(requests.__version__)"'
```

Ожидаемо:

```text
2.32.4
```

### Постоянное решение

`pip install` внутри pod — временно. После пересоздания pod пакет пропадет.

Нужно одно из:

1. custom image для exec-node с `requests`;
2. init container/entrypoint, который ставит `requests`;
3. исправленный CHYT artifact/launcher, где зависимость уже есть.

---

## 8. CHYT проблема 2: `Group "/chyt/ch_public" does not exist`

### Симптом

Operation была running, job был running, но query падал:

```bash
yt query --format json chyt "SELECT 1 + 1 AS x"
```

Ошибка:

```text
Ensure that the clique "ch_public" has started properly and its jobs are successfully running
    Group "/chyt/ch_public" does not exist
```

Порты CHYT при этом могли слушать:

```bash
nc -vz end-1.exec-nodes.default.svc.cluster.local 24578 || true
nc -vz end-1.exec-nodes.default.svc.cluster.local 24579 || true
nc -vz end-1.exec-nodes.default.svc.cluster.local 24580 || true
nc -vz end-1.exec-nodes.default.svc.cluster.local 24581 || true
```

`24579` — monitoring/orchid.  
`24578`, `24580`, `24581` — CHYT RPC/TCP/HTTP-порты.

### Диагностика orchid

```bash
curl -sS http://end-1.exec-nodes.default.svc.cluster.local:24579/orchid/service/state || true
curl -sS http://end-1.exec-nodes.default.svc.cluster.local:24579/orchid/alerts || true
curl -sS http://end-1.exec-nodes.default.svc.cluster.local:24579/orchid | head -80 || true
```

### Проверка процесса и логов внутри exec-node

```bash
kubectl exec -n default end-1 -- sh -lc '
echo "== CHYT processes =="
ps -ef | grep -E "ytserver-clickhouse|clickhouse-trampoline" | grep -v grep || true

PID=$(pgrep -f "^./ytserver-clickhouse" | head -1)
echo "YTCH_PID=$PID"

CWD=$(readlink /proc/$PID/cwd)
echo "CWD=$CWD"

echo "== sandbox files =="
ls -lah "$CWD" | sed -n "1,120p"

echo "== grep discovery/chyt/group errors =="
for f in "$CWD"/clickhouse.log "$CWD"/clickhouse.error.log "$CWD"/clickhouse.debug.log; do
  [ -f "$f" ] || continue
  echo "---- $f ----"
  grep -nEi "discovery|/chyt|ch_public|group|register|heartbeat|error|exception|failed|timeout|master|health|Ares|Dns" "$f" | tail -120 || true
done
'
```

---

## 9. CHYT корневая проблема: Ares DNS и IPv6-only lookup

### Симптом в stderr failed CHYT jobs

```bash
yt get-job-stderr <JOB_ID> "$OP" | tail -300
```

Ключевой фрагмент:

```text
Error reporting heartbeat 10 times on startup

There are not enough healthy servers known

Channel terminated
method          Heartbeat
address         ds-0.discovery.default.svc.cluster.local:9020
service         DiscoveryClientService

Ares DNS resolve failed for "ds-0.discovery.default.svc.cluster.local"
enable_ipv4     false
enable_ipv6     true

DNS server returned answer with no data
```

Ранее похожее было по master:

```text
Ares DNS resolve failed
HostName: ms-0.masters.default.svc.cluster.local
Options: {EnableIPv4: False, EnableIPv6: True}
```

### Почему это было странно

Из pod обычный resolver работал:

```bash
kubectl exec -n default end-1 -- sh -lc '
getent hosts ds-0.discovery.default.svc.cluster.local
nc -vz ds-0.discovery.default.svc.cluster.local 9020 || true
'
```

Результат:

```text
10.42.4.48 ds-0.discovery.default.svc.cluster.local
Connection to ds-0.discovery.default.svc.cluster.local 9020 port [tcp/*] succeeded!
```

То есть Kubernetes DNS и сеть были живы. Ломался именно DNS resolver внутри `ytserver-clickhouse`: он искал только IPv6 (`enable_ipv4=false`, `enable_ipv6=true`) в IPv4-only окружении.

### Фикс 1: заменить master FQDN на IP в `//sys/@cluster_connection`

Backup:

```bash
yt get //sys/@cluster_connection > /tmp/cluster_connection.before.yson
```

Получить IP:

```bash
MS_IP=$(getent hosts ms-0.masters.default.svc.cluster.local | awk '{print $1}' | head -1)
QT_IP=$(getent hosts qt-0.query-trackers.default.svc.cluster.local | awk '{print $1}' | head -1)
QA_IP=$(getent hosts qa-0.queue-agents.default.svc.cluster.local | awk '{print $1}' | head -1)
DS_IP=$(getent hosts ds-0.discovery.default.svc.cluster.local | awk '{print $1}' | head -1)

echo "MS_IP=$MS_IP QT_IP=$QT_IP QA_IP=$QA_IP DS_IP=$DS_IP"
```

Пример:

```text
MS_IP=10.42.2.166 QT_IP=10.42.4.52 QA_IP=10.42.2.172 DS_IP=10.42.4.48
```

Установить master IP:

```bash
yt set //sys/@cluster_connection/primary_master/addresses "[\"${MS_IP}:9010\"]"
yt set //sys/@cluster_connection/primary_master/peers "[{address=\"${MS_IP}:9010\"; voting=%true;}]"
```

### Фикс 2: заменить `master_cache` FQDN на IP

Проверка:

```bash
yt get //sys/@cluster_connection | grep -nEi 'ms-0|10\.42|master_cache|primary_master|query_tracker|queue_agent|discovery' | head -200
```

До фикса было:

```text
master_cache:
  addresses:
    ms-0.masters.default.svc.cluster.local:9010

primary_master:
  addresses:
    10.42.2.166:9010
```

Фикс:

```bash
MS_IP=$(getent hosts ms-0.masters.default.svc.cluster.local | awk '{print $1}' | head -1)

yt set //sys/@cluster_connection/master_cache/addresses "[\"${MS_IP}:9010\"]"
```

Проверка:

```bash
yt get //sys/@cluster_connection | grep -nEi 'ms-0|10\.42|master_cache|primary_master' | head -120
```

Ожидаемо:

```text
master_cache:
  addresses:
    10.42.2.166:9010

primary_master:
  addresses:
    10.42.2.166:9010
```

### При необходимости: заменить discovery FQDN на IP

Если снова будет падать heartbeat на discovery:

```bash
DS_IP=$(getent hosts ds-0.discovery.default.svc.cluster.local | awk '{print $1}' | head -1)

yt set //sys/@cluster_connection/discovery_connection/addresses "[\"${DS_IP}:9020\"]"
```

---

## 10. CHYT health checker и sample table

В CHYT config был health checker:

```text
health_checker:
  queries:
    - select * from `//sys/clickhouse/sample_table`
```

Но таблицы не было:

```bash
yt exists //sys/clickhouse/sample_table
```

Результат:

```text
false
```

Создать:

```bash
yt create map_node //sys/clickhouse --ignore-existing || true

yt create table //sys/clickhouse/sample_table \
  --ignore-existing \
  --attributes '{schema=[{name=x;type=int64;}];}' || true
```

Запись через YSON дала ошибку:

```bash
printf '{x=1;}\n' | yt write-table //sys/clickhouse/sample_table --format yson
```

Ошибка:

```text
YSON bindings required. Try to use other format or install bindings.
```

Лучше писать JSON:

```bash
printf '{"x":1}\n' | yt write-table //sys/clickhouse/sample_table --format json
```

---

## 11. CHYT restart runbook

### Получить текущий OP

```bash
OP=$(yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_id 2>/dev/null | tr -d '"')
echo "$OP"
```

### Остановить старое

```bash
[ -n "$OP" ] && yt abort-op "$OP" || true
yt remove //sys/clickhouse/strawberry/ch_public --recursive --force || true
```

### Запустить clique

```bash
kubectl exec -n default deploy/strawberry-controller -- sh -lc '
cat > /tmp/chyt_public.yson <<EOF
{
    pool = "research";
    active = %true;
    instance_count = 1;
    cpu_limit = 16;
    memory_limit = 68719476736;
}
EOF

export YT_PROXY=http-proxies-lb.default.svc.cluster.local

/usr/bin/chyt-controller one-shot-run \
  --config-path /config/strawberry-controller.yson \
  --log-to-stderr \
  --family chyt \
  --alias ch_public \
  --speclet-path /tmp/chyt_public.yson
'
```

### Проверить

```bash
OP=$(yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_id | tr -d '"')
echo "OP=$OP"

sleep 30

yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_state

yt list-jobs "$OP" \
  --include-runtime \
  --include-archive \
  --include-cypress \
  --limit 20 \
  --format json | jq '.state_counts, [.jobs[] | {id,state,stderr_size,address,start_time,finish_time}]'

yt query --format json chyt "SELECT 1 + 1 AS x"
```

Финально было:

```text
{"x":2}
```

Примечание: `yt_operation_state` может еще показывать `"initializing"`, но query уже работает.

---

## 12. Ресурсы CHYT и почему их поставили так много

Финальный speclet:

```bash
yt get //sys/clickhouse/strawberry/ch_public/speclet
```

```yson
{
    "memory_limit" = 68719476736;
    "family" = "chyt";
    "pool" = "research";
    "instance_count" = 1;
    "active" = %true;
    "cpu_limit" = 16;
    "stage" = "untracked";
}
```

Параметры:

```text
instance_count = 1
cpu_limit      = 16
memory_limit   = 68719476736 # 64 GiB
pool           = research
```

Почему так:

1. Нужно было быстро исключить ресурсное голодание.
2. `ytserver-clickhouse` тяжелый: у него memory pools, caches, reader memory, watchdog.
3. При маленьких ресурсах CHYT может не стартовать, падать по OOM или долго проходить health/startup.
4. Проблема оказалась не в ресурсах, а в DNS/Ares/cluster_connection, но большие лимиты помогли не смешивать проблемы.
5. `instance_count=1` — минимальный рабочий вариант для DataLens smoke-test.

После стабилизации можно пробовать уменьшить:

```yson
{
    pool = "research";
    active = %true;
    instance_count = 1;
    cpu_limit = 4;
    memory_limit = 17179869184; # 16 GiB
}
```

---

## 13. DataLens подключение

### Проверка публичного доступа

```bash
curl -v --max-time 10 http://<PUBLIC_VM1_IP>:31103/api/v4/list
```

Ожидаемый сетевой признак без токена:

```text
HTTP/1.1 401 Unauthorized
Client is missing credentials
```

### Токен

В shell:

```bash
export YT_TOKEN='<YT_TOKEN>'
```

В curl:

```bash
-H "Authorization: OAuth ${YT_TOKEN}"
```

В DataLens поле токена заполнять без кавычек:

```text
ytct-...
```

### Проверка токена через API

```bash
curl -sS \
  -H "Authorization: OAuth ${YT_TOKEN}" \
  -H 'X-YT-Parameters: {"path":"//sys/clickhouse/strawberry/ch_public/speclet"}' \
  "http://<PUBLIC_VM1_IP>:31103/api/v4/get"
```

Успешно:

```json
{"value":{"memory_limit":68719476736,"family":"chyt","pool":"research","instance_count":1,"active":true,"cpu_limit":16,"stage":"untracked"}}
```

### Clique alias

Проверка alias:

```bash
OP=$(yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_id | tr -d '"')

yt get-operation "$OP" --format json \
  | jq -r '
    .brief_spec.alias?,
    .spec.alias?,
    .full_spec.alias?,
    .provided_spec.alias?
  ' | sort -u
```

Результат:

```text
*ch_public
```

DataLens требует alias с `*`:

```text
*ch_public
```

Если указать без `*`, ошибка:

```text
SDK_REQUEST_ERROR
Clique alias name must begin with an asterisk (*)
```

### DataLens ошибка `/runtime_parameters/acl`

Ошибка:

```text
Invalid clique specification
Node /runtime_parameters has no child with key "acl"
```

Проверка:

```bash
yt get-operation "$OP" --format json \
  | jq '.runtime_parameters.acl, .spec.acl, .provided_spec.acl'
```

Было:

```json
null
[]
[]
```

Добавить operation ACL:

```bash
OP=$(yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_id | tr -d '"')

yt update-op-parameters "$OP" '{
    acl = [
        {
            action = allow;
            subjects = ["admin"; "root"; "users"];
            permissions = ["read"; "manage"];
        };
    ];
}'
```

Важно: для operation ACL нельзя `use`.

Ошибка с `use`:

```text
Only "read", "manage" and "administer" permissions are allowed in operation ACL,
got ["read";"use";"manage";]
```

Можно максимально широко:

```bash
yt update-op-parameters "$OP" '{
    acl = [
        {
            action = allow;
            subjects = ["admin"; "root"; "users"];
            permissions = ["read"; "manage"; "administer"];
        };
    ];
}'
```

Проверка:

```bash
yt get-operation "$OP" --format json | jq '.runtime_parameters.acl'
```

---

## 14. Таблица ошибок

| Ошибка | Где | Причина | Решение |
|---|---|---|---|
| `Temporary failure in name resolution` | `getent`, `nc`, `yt get-job-stderr` | VM1 не резолвила `*.svc.cluster.local` | systemd-resolved: DNS `10.43.0.10`, domain `~cluster.local` |
| `Failed to connect to master ...:27001` | Airflow SPYT | Spark master endpoint недоступен | Починить DNS, проверить `end-1:27001` |
| `ConnectTimeoutError http://10.130.0.35:8333` | Airflow S3 state | SeaweedFS VM умерла/недоступна | Поднять VM/сервис или заменить endpoint |
| `ModuleNotFoundError: No module named 'requests'` | CHYT job | В exec-node нет `requests` | `pip install requests`; постоянно — custom image/init |
| `Group "/chyt/ch_public" does not exist` | CHYT/DataLens | CHYT не зарегистрировался в discovery | Смотреть job stderr; фикс DNS/Ares/cluster_connection |
| `Ares DNS resolve failed ... enable_ipv4 false enable_ipv6 true` | CHYT job stderr | CHYT искал IPv6 в IPv4-only кластере | Заменить FQDN на IP в `//sys/@cluster_connection` |
| `Node //sys/clickhouse has no child with key "sample_table"` | CHYT health checker | Нет sample table | Создать `//sys/clickhouse/sample_table` |
| `YSON bindings required` | `write-table --format yson` | Нет Python YSON bindings | Использовать JSON или поставить `ytsaurus-yson` |
| `Command "version" is not registered` | `/api/v4/version` | Такой команды нет в API | Использовать `/api/v4/list` или `/api/v4/get` |
| `Unable to parse parameters from headers` | curl API | В `X-YT-Parameters` передан YSON | Передавать JSON |
| `Clique alias name must begin with an asterisk (*)` | DataLens | Alias без `*` | Указать `*ch_public` |
| `Node /runtime_parameters has no child with key "acl"` | DataLens | Нет operation runtime ACL | `yt update-op-parameters "$OP" '{ acl = [...] }'` |
| `Only "read", "manage" and "administer"...` | operation ACL | В ACL добавили `use` | Убрать `use` |
| `Unexpected token ":"` | `yt update-op-parameters` | Передан JSON вместо YSON | Использовать YSON |

---

## 15. Полный smoke-test после reboot/redeploy

```bash
# DNS
getent hosts http-proxies-lb.default.svc.cluster.local
getent hosts ms-0.masters.default.svc.cluster.local
getent hosts ds-0.discovery.default.svc.cluster.local

# YT proxy
yt list //sys | head

# CHYT op
OP=$(yt get //sys/clickhouse/strawberry/ch_public/@strawberry_persistent_state/yt_operation_id | tr -d '"')
echo "OP=$OP"

yt list-jobs "$OP" \
  --include-runtime \
  --include-archive \
  --include-cypress \
  --limit 20 \
  --format json | jq '.state_counts, [.jobs[] | {id,state,stderr_size,address,start_time,finish_time}]'

# CHYT query
yt query --format json chyt "SELECT 1 + 1 AS x"

# External HTTP proxy
curl -v --max-time 10 http://<PUBLIC_VM1_IP>:31103/api/v4/list

# Auth HTTP API
export YT_TOKEN='<YT_TOKEN>'
curl -sS \
  -H "Authorization: OAuth ${YT_TOKEN}" \
  -H 'X-YT-Parameters: {"path":"//sys/clickhouse/strawberry/ch_public/speclet"}' \
  "http://<PUBLIC_VM1_IP>:31103/api/v4/get"

# DataLens-related ACL
yt get-operation "$OP" --format json | jq '.runtime_parameters.acl'
```

---

## 16. Что закрепить постоянно

1. DNS на `vm1`:
   - CoreDNS `10.43.0.10`;
   - routing domain `~cluster.local`.

2. `requests` в exec-node:
   - не вручную через `pip`;
   - через image/init/entrypoint.

3. `//sys/@cluster_connection`:
   - `primary_master` и `master_cache` должны быть IP, если CHYT Ares продолжает ломаться на FQDN;
   - при повторных ошибках heartbeat заменить `discovery_connection` на IP.

4. CHYT sample table:
   - создать `//sys/clickhouse/sample_table`;
   - или убрать/поменять health checker query.

5. DataLens:
   - alias `*ch_public`;
   - token без кавычек;
   - operation ACL без `use`, только `read/manage/administer`;
   - публичный доступ к `31103` ограничить firewall/security group.

6. Ресурсы:
   - текущий CHYT 16 CPU / 64 GiB — стабилизационный режим;
   - после проверки DataLens можно снижать до 4-8 CPU / 16-32 GiB.

---

## 17. Executive summary

Самая тяжелая проблема CHYT была не в DataLens и не только в ресурсах. `ytserver-clickhouse` стартовал, но не мог корректно зарегистрироваться в discovery, потому что его Ares DNS resolver пытался резолвить внутренние Kubernetes FQDN только как IPv6 (`enable_ipv4=false`, `enable_ipv6=true`) в IPv4-only кластере. Из-за этого Query Tracker/DataLens видели:

```text
Group "/chyt/ch_public" does not exist
```

Реальный рабочий путь:

1. Починить DNS на `vm1` для `*.svc.cluster.local`.
2. Установить `requests` для `clickhouse-trampoline` в exec-node pods.
3. Заменить FQDN `primary_master` и `master_cache` в `//sys/@cluster_connection` на IP.
4. Перезапустить CHYT clique.
5. Проверить `yt query --format json chyt "SELECT 1 + 1 AS x"`.
6. Для DataLens использовать `*ch_public`, `<PUBLIC_VM1_IP>:31103`, OAuth token.
7. Если DataLens ругается на `/runtime_parameters/acl`, добавить operation ACL через `yt update-op-parameters`.

