# ytsaurus_k3s_ycloud
Data LakeHouse on YTsaurus tech

---

## `dags/` — DAG-и Airflow (gitSync subPath)

| Файл | DAG ID | schedule | Что делает |
|------|--------|----------|------------|
| `test_dag.py` | `test_dag` | None (manual) | Smoke-test для Airflow. Печатает `Airflow test DAG is working`. Запускается вручную для проверки готовности Airflow|
| `greenhub_s3_to_bronze.py` | `greenhub_s3_to_bronze` | None (manual) | Загружает parquet-файлы из S3 (SeaweedFS) в bronze-слой. Сверяет etag-журнал `_state/greenhub_load.json` в S3, обрабатывает только новые файлы. Через SSH на vm1 вызывает `spyt/launch_raw_to_bronze.sh`|
| `greenhub_bronze_to_silver.py` | `greenhub_bronze_to_silver` | None (manual) | Bronze fact + dims -> silver wide table `//home/silver_stage/greenhub_telemetry`. Через SSH на vm1 вызывает `spyt/launch_bronze_to_silver.sh`. Журнал в `_state/greenhub_silver_runs.json`|
| `greenhub_silver_to_gold.py` | `greenhub_silver_to_gold` | None (manual) | Silver -> 4 gold-витрины. Параметризован по mart-name. Через SSH вызывает `spyt/launch_silver_to_gold.sh <mart>`|
| `streaming_silver_yql_dag.py` | `streaming_silver_yql` | `*/5 * * * *` | Каждые 5 минут запускает 3 YQL-скрипта из `sql/streaming_silver/`. Bronze_t0/1/2 -> silver streaming_posts/comments/todos. Через SSH вызывает `scripts/run_yql_with_yt_log.sh`|

---

## `kafka/` — Streaming pipeline (Python)

| Файл | Что делает |
|------|------------|
| `api_to_kafka.py` | Producer. Каждые 30 сек. тянет с 3 endpoint с `jsonplaceholder.typicode.com` (posts/comments/todos), формирует `KafkaEvent` через `from_payload(...)`, шлёт батчем в `raw-events`. Использует `KafkaEventProducer`|
| `kafka_producer.py` | Класс `KafkaEventProducer` (обёртка над `confluent_kafka.Producer`). Идемпотентность включена (`enable.idempotence=true`, `acks=all`). Метод `send_many(events, key_fn)` шлёт с `key=stream_id:entity_id`, делает `flush()`|
| `kafka_event_schema.py` | `dataclass KafkaEvent`. Поля: source, stream_id, event_type, event_ts, entity_id, payload_json, ingest_batch_id, ingest_ts. Конструктор `from_payload(...)`|
| `kafka_consumer.py` | Класс `KafkaRawConsumer` (обёртка над `confluent_kafka.Consumer`). `enable.auto.commit=false`, `auto.offset.reset=earliest`. Методы: `subscribe()`, `poll_batch(batch_size, timeout)`, `commit()`|
| `kafka_to_raw.py` | Consumer Kafka -> YT raw. Маппит `stream_id` → `tablet_index` (s0→0, s1→1, s2→2). Батч 100 событий, `yt.insert_rows(RAW_TABLE, rows)`, после успеха `consumer.commit()`|
| `raw_to_bronze.py` | Цикл по 3 tablets: `pull_consumer` -> enrichment (добавляет queue_timestamp, processed_at) -> `insert_rows` в `bronze_t{N}` -> `advance_consumer`|
| `init_kafka.txt` | Инструкция (комментарий) как запустить consumers вручную через nohup|

---

## `spyt/` — SPYT/Spark jobs + launcher-ы

| Файл | Что делает |
|------|------------|
| `raw_to_bronze_greenhub.py` | PySpark job. Читает parquet из `s3a://greenhub/...`, разбирает на fact и dims, считает детерминированный `fact_uid = sha256()`. Пишет в `//home/bronze_stage/greenhub/fact_telemetry` и dim-таблицы через ytTable connector|
| `raw_yt_to_bronze_greenhub.py` | Альтернативная версия (читает не из S3, а из staging YT-таблицы)|
| `bronze_to_silver_greenhub.py` | PySpark job. Читает fact + все dim-таблицы из bronze, JOIN-ит, обогащает (флаги, метрики), дедупит по `fact_uid` через `row_number() over (partition by fact_uid order by ...)`. Записывает `//home/silver_stage/greenhub_telemetry`|
| `silver_to_gold_greenhub.py` | PySpark job. Строитgold-витрин daily_country_stats  из silver. Пересоздаёт таблицу полностью|
| `stuct_parquete.py` | Утилита для исследования структуры исходных parquet файлов GreenHub (printSchema)|
| `launch_raw_to_bronze.sh` | Bash-launcher для `raw_to_bronze_greenhub.py`. Активирует yt-env, экспортирует JAVA_HOME, YT_PROXY, SPARK_LOCAL_IP, S3_ACCESS_KEY, S3_SECRET_KEY, DRIVER_HOST. Вызывает `spark-submit-yt`. Принимает 2 аргумента: `<s3a-path> <part-index>` |
| `launch_raw_yt_to_bronze.sh` | Аналог launch_raw_to_bronze, но для версии из YT (не используется в текущем pipeline) |
| `launch_bronze_to_silver.sh` | Bash-launcher для `bronze_to_silver_greenhub.py`. Без аргументов (читает все bronze, пишет один silver)|
| `launch_silver_to_gold.sh` | Bash-launcher для `silver_to_gold_greenhub.py`|

---

## `sql/streaming_silver/` — YQL-скрипты для silver streaming

| Файл | Что делает |
|------|------------|
| `streaming_posts.sql` | Читает `//home/bronze_stage/bronze_t0`, фильтрует `source = "jsonplaceholder_posts"`. Парсит `Yson::ParseJson(payload_json)`. Достаёт поля id, userId, title, body. `INSERT INTO //home/silver_stage/streaming_posts WITH TRUNCATE`|
| `streaming_comments.sql` | То же из `bronze_t1`, source `jsonplaceholder_comments`. Поля: id, postId, name, email, body|
| `streaming_todos.sql` | То же из `bronze_t2`, source `jsonplaceholder_todos`. Поля: id, userId, title, completed|

---

## `scripts/` — служебные bash-скрипты

| Файл | Что делает | Кто вызывает |
|------|------------|---------------|
| `run_yql_with_yt_log.sh` | Запускает один YQL-скрипт через `yt query`, измеряет длительность, пишет audit-запись в `//home/ops_logs/airflow/streaming_silver_runs` (success/failed, started_at, duration, yt_query_id, message). | `streaming_silver_yql_dag` |
| `init_silver_tables.sh` | Создаёт `//home/silver_stage/streaming_*` (3 таблицы для streaming silver). | Вручную при инициализации |
| `init_ytsaurus_logs.sh` | Создаёт `//home/ops_logs/airflow/streaming_silver_runs` для аудита streaming silver. | Вручную при инициализации |
| `init_greenhub_ops_logs.sh` | Создаёт audit-таблицы для batch DAG: `//home/ops_logs/greenhub/file_loads`, `layer_runs`, `mart_runs`, `dag_runs`. | Вручную при инициализации |
| `deploy_airflow.sh` | Делает `git pull` в репо, запускает `init_ytsaurus_logs.sh` и `init_silver_tables.sh`, проверяет что DAG `streaming_silver_yql` появился в `airflow dags list`. | Вручную после обновлений |

---

## `YTsaurus/` — bash-скрипты создания таблиц

| Файл | Что делает |
|------|------------|
| `script_table.sh` | Создаёт **bronze для batch**: `//home/bronze_stage/greenhub/fact_telemetry` + 13 dim-таблиц (dim_device, dim_country, dim_timezone, dim_battery_state, dim_network_status, dim_charger, dim_health, dim_network_type, dim_mobile_network_type, dim_mobile_data_status, dim_mobile_data_activity, dim_wifi_status). Все статические, со schema|
| `create_silver_tables.sh` | Создаёт **silver для batch**: `//home/silver_stage/greenhub_telemetry` (wide table, 40+ полей)|
| `create_gold_tables.sh` | Создаёт **4 gold-витрины**: gold_daily_country_stats (MVP используется только она), gold_country_overview, gold_device_lifecycle, gold_hourly_battery_health|
| `silver_kafka.txt` | Создаёт **silver для streaming**: streaming_posts, streaming_comments, streaming_todos|

---

## `infra/` — конфигурация helm-чартов и инфраструктура

| Файл | Что делает |
|------|------------|
| `airflow-values.yaml` | Helm values для Airflow. CeleryExecutor, webserver NodePort 30081, postgres+redis embedded, dags через gitSync, default user (`AFL938ahZKal` / `Tye1lf0f3sdcXX`). Все компоненты на vm5|
| `kafka-values.yaml` | Helm values для Kafka. KRaft mode, controller+broker по 1 реплике, persistence 8Gi, listeners PLAINTEXT|
| `kafka-ui-values.yaml` | Helm values для kafka-ui (kafbat). NodePort 30808, подключение к `kafka.kafka.svc.cluster.local:9092`|
| `loki-values.yaml` | Helm values для Loki (логи). **Опционально**, ещё не подключено к Grafana|
| `kube-prometheus-values.yaml` | Helm values для kube-prometheus (метрики). **Опционально**|
| `alloy-values.yaml` | Helm values для Grafana Alloy (агент сбора логов в Loki). **Опционально**|
| `spyt_launch.sh` | Кладёт `/usr/local/bin/start-spyt-cluster.sh` с `spark-launch-yt` параметрами для SPYT-кластера (--disable-tmpfs, --abort-existing, и т.д.)|
| `systemd_spyt.sh` | Кладёт `/etc/systemd/system/spyt-cluster.service`. После него `systemctl enable spyt-cluster`|
| `systemd _port_forward.sh` | (Пустой / для backup) systemd unit для kubectl port-forward в фоне|
| `port_forward.sh` | Кладёт `kubectl port-forward` команды для YT UI 30254, Airflow 30081, Kafka UI 30808 в фон через nohup|
| `ssh.sh` | Bash-сниппет для SSH с локальной машины на vm1 через бастион|
| `ssh.ps1` | То же для PowerShell|
| `start-all.ps1` / `stop-all.ps1` | Скрипты для управления ВМ из локального Windows|


## Environment / .env

Эти секреты НЕ хранятся в git. Должны быть на vm1 либо в Airflow Variables.

| Имя | Где | Значение |
|-----|-----|----------|
| `YT_TOKEN` | `/home/chig_k3s/.yt/token` (chmod 600) | yt-токен из secret `ytadminsec` |
| `S3_ACCESS_KEY` | Airflow Variable + env vm1 | ключ SeaweedFS |
| `S3_SECRET_KEY` | Airflow Variable + env vm1 | секрет SeaweedFS |
| `K3S_TOKEN` | `/var/lib/rancher/k3s/server/node-token` на vm1 | для join worker-узлов |
| `GH_USER` / `GH_PAT` | Kubernetes Secret `airflow-git-credentials` в ns `airflow` | для приватного git-sync (если репо приватный) |
| SSH ключи | `~/.ssh/id_ed25519` на vm1 | для Airflow → vm1 SSH connection `vm1_ssh` |
| Airflow admin | в helm values `airflow-values.yaml` | `AFL938ahZKal` / `Tye1lf0f3sdcXX` |

---

## NodePorts (сводка)

| Порт | Сервис | Доступ |
|------|--------|--------|
| 31103 | YTsaurus HTTP proxy | внутренний + порт-форвард |
| 31104 | YTsaurus HTTP proxy (control) | внутренний |
| 31105 | YTsaurus RPC proxy | внутренний |
| 30254 | YTsaurus UI | публичный через vm1 |
| 30081 | Airflow webserver (см. airflow-values) | публичный через vm1 |
| 30808 | Kafka UI | публичный через vm1 |
| 6443 | k3s API | внутренний |


### ВМ

| Узел | Роль | Ресурсы | Примечание |
|---|---|---:|---|
| `vm1` | k3s control plane, точка управления, YT proxy/UI, CHYT/DataLens endpoint | 4 CPU / 8 GiB | Есть внешний IP |
| `vm2` | YTsaurus heavy / exec | 32 CPU / 128 GiB | label `yt-role=heavy` |
| `vm3` | YTsaurus heavy / exec | 32 CPU / 128 GiB | label `yt-role=heavy` |
| `vm4` | Storage | 8 CPU / 16 GiB | label `yt-role=storage` |
| `vm5` | Kafka | 16 CPU / 32 GiB | label `yt-role=kafka/airflow/monitoring` |

Дополнительно:

- SeaweedFS / S3: `10.130.0.35:8333`
- Внутренний IP `vm1`: `10.130.0.24`
- k3s API: `https://10.130.0.24:6443`
- YTsaurus proxy наружу через `vm1`: `http://localhost:31103`
- CHYT для DataLens: `<vm1_public_ip>:31103`, alias `*ch_public`

### Общая очередность

1. Подготовить ВМ, SSH и сеть.
2. Поднять k3s: сначала server на `vm1`, потом agents на `vm2..vm5`.
3. Проставить labels и DNS для `cluster.local`.
4. Установить окружение на `vm1`: Java, Python venv, Helm, cert-manager, repo.
5. Установить YTsaurus operator и создать YTsaurus cluster.
6. Создать базовые Cypress-директории и таблицы.
7. Увеличить ресурсы execNodes до запуска CHYT.
8. Поднять CHYT clique и применить persist patch.
9. Поднять Kafka и Kafka UI, создать topics, YT queue и dynamic bronze tables.
10. Поднять Airflow, заполнить Variables / Connections.
11. Поднять SPYT через systemd.
12. Поднять streaming consumers через systemd.
13. Запустить batch и streaming pipelines.
14. Подключить DataLens.

---

## 1. Подготовка ВМ и SSH

Проверить, что доступны все ВМ:

```bash
ssh -i ~/.ssh/key login@<vm1_public_ip>
ssh -i ~/.ssh/key login@<vm2_internal_ip>
ssh -i ~/.ssh/key login@<vm3_internal_ip>
ssh -i ~/.ssh/key login@<vm4_internal_ip>
ssh -i ~/.ssh/key login@<vm5_internal_ip>
```

Проверить сетевую связность между ВМ по внутренним IP.

**Критерий готовности**

- `ssh vm1` работает.
- `ssh vm2..vm5` с `vm1` работает.
- Внутри сети открыт доступ к `6443` для k3s API.

---

## 2. k3s cluster

### 2.1. Control plane на `vm1`

```bash
curl -sfL https://get.k3s.io | sh -s - \
  --write-kubeconfig-mode 644 \
  --disable traefik

sudo systemctl status k3s --no-pager | head -10
```

Настроить kubeconfig:

```bash
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $USER:$USER ~/.kube/config
chmod 600 ~/.kube/config

echo 'export KUBECONFIG=~/.kube/config' >> ~/.bashrc
export KUBECONFIG=~/.kube/config

kubectl get nodes -o wide
```

### 2.2. Worker nodes `vm2..vm5`

На `vm1` получить токен:

```bash
sudo cat /var/lib/rancher/k3s/server/node-token
```

На каждой worker-ВМ:

```bash
export K3S_URL="https://10.130.0.24:6443"
export K3S_TOKEN="<TOKEN_FROM_VM1>"

curl -sfL https://get.k3s.io | K3S_URL="$K3S_URL" K3S_TOKEN="$K3S_TOKEN" sh -

sudo systemctl status k3s-agent --no-pager | head -20
```

### 2.3. Labels для scheduling

Выполнять на `vm1`:

```bash
kubectl label node vm2-k3s-ing-server yt-role=heavy --overwrite
kubectl label node vm3-k3s-ing-server yt-role=heavy --overwrite
kubectl label node vm4-k3s-ing-server yt-role=storage --overwrite
kubectl label node vm5-k3s-ing-server yt-role=kafka/airflow/monitoring --overwrite

kubectl get nodes --show-labels
```

### 2.4. DNS для `cluster.local` на всех ВМ

Нужно, чтобы worker-узлы и процессы на хостах могли ходить в Kubernetes service DNS.

На каждой из 5 ВМ:

```bash
sudo mkdir -p /etc/systemd/resolved.conf.d

sudo tee /etc/systemd/resolved.conf.d/cluster-local.conf >/dev/null <<'EOF_DNS'
[Resolve]
DNS=10.43.0.10
Domains=~cluster.local
EOF_DNS

sudo systemctl restart systemd-resolved

getent hosts baltocdn.com
getent hosts hp-0.http-proxies.default.svc.cluster.local
```

**Критерий готовности**

```bash
kubectl get nodes -o wide
```

Ожидаемо: 5 узлов в статусе `Ready`.

Также на каждой ВМ:

```bash
getent hosts hp-0.http-proxies.default.svc.cluster.local
```

-> Должен вернуться IP Kubernetes service.

---

## 3. Базовое окружение на `vm1`

### 3.1. Системные пакеты

```bash
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk-headless python3-venv unzip uuid-runtime
java -version
```

### 3.2. Helm

```bash
curl -fsSL -o /tmp/get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 /tmp/get_helm.sh
sudo /tmp/get_helm.sh

helm version --short
```

### 3.3. cert-manager

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml
kubectl get crd | grep cert-manager
```

### 3.4. Python venv для YTsaurus / SPYT / streaming

```bash
python3 -m venv /home/chig_k3s/yt-env
source /home/chig_k3s/yt-env/bin/activate

pip install --upgrade pip
pip install ytsaurus-client ytsaurus-yson ytsaurus-spyt
pip install "pyspark>=3.5,<4.0"
pip install requests confluent-kafka

yt --version
ls /home/chig_k3s/yt-env/bin/ | grep -E "spark|spyt"
which spark-launch-yt
which spark-submit-yt
python -c "import pyspark; print(pyspark.__version__)"
```

-> PySpark: `3.5.x`, например `3.5.3`.

### 3.5. Git repo

```bash
git clone https://github.com/chiglo-k/ytsaurus_k3s_ycloud.git /home/chig_k3s/git/repo
cd /home/chig_k3s/git/repo
```

**Критерий готовности**

```bash
yt --version
spark-launch-yt --help | head
helm version --short
java -version
```

---

## 4. YTsaurus

### 4.1. Установка YTsaurus operator

```bash
helm install ytsaurus oci://ghcr.io/ytsaurus/ytop-chart \
  --version 0.31.0 \
  --namespace ytsaurus-operator \
  --create-namespace

kubectl get pods -n ytsaurus-operator
```

### 4.2. Secret с admin credentials

```bash
kubectl create secret generic ytadminsec \
  --from-literal=login=admin \
  --from-literal=password=passworf \
  --from-literal=token=$(openssl rand -hex 16) \
  -n default

kubectl get secret ytadminsec -n default
```

### 4.3. Создание YTsaurus cluster resource

Файл `minisaurus-cr.yaml` берётся из проекта / restore pack / README проекта. Перед применением удалить устаревшие поля, если они есть:

```bash
sed -i '/useIPv6:/d; /useIPv4:/d' ~/minisaurus-cr.yaml
kubectl apply -f ~/minisaurus-cr.yaml
```

Ждать готовности:

```bash
kubectl get pods -n default -w
```

Ориентир по pod-ам: `ms-0`, `ds-*`, `hp-0`, `sch-0`, `ca-0`, `ui-0`, `qt-0`, `yqla-0`, `qa-0`, `end-*`, `dnd-*`, `tnd-*` должны быть `Running 1/1`.

### 4.4. Token, proxy и Cypress-директории

```bash
TOKEN=$(kubectl get secret ytadminsec -n default -o jsonpath='{.data.token}' | base64 -d)
mkdir -p ~/.yt
echo $TOKEN > ~/.yt/token
chmod 600 ~/.yt/token

export YT_PROXY=http://localhost:31103
export YT_TOKEN=$(cat ~/.yt/token)

yt list /
```

Создать базовые директории:

```bash
for d in raw_stage bronze_stage silver_stage gold_stage \
         ops_logs ops_logs/greenhub bronze_stage/greenhub \
         spark spark/discovery; do
  yt create map_node //home/$d --ignore-existing
done
```

### 4.5. PATCH execNodes до запуска CHYT

Стандартных execNodes недостаточно для CHYT clique. Поднимаем до `24 CPU / 96 GiB`.

```bash
kubectl patch ytsaurus minisaurus -n default --type='json' -p='[
  {"op":"replace","path":"/spec/execNodes/0/resources/requests/cpu","value":"24"},
  {"op":"replace","path":"/spec/execNodes/0/resources/requests/memory","value":"96Gi"},
  {"op":"replace","path":"/spec/execNodes/0/resources/limits/cpu","value":"24"},
  {"op":"replace","path":"/spec/execNodes/0/resources/limits/memory","value":"96Gi"}
]'

sleep 60
kubectl get pods -n default | grep end-
```

**Критерий готовности**

```bash
yt list /
kubectl get pods -n default
```
-> YTsaurus pod-ы `Running`.

---

## 5. Таблицы YTsaurus

Выполнять из repo:

```bash
cd /home/chig_k3s/git/repo
```

### 5.1. Bronze batch tables

```bash
bash YTsaurus/script_table.sh
yt list //home/bronze_stage/greenhub
```

-> fact + dims, в текущей версии restore pack обычно 14-16 таблиц в зависимости от состава схем.

### 5.2. Silver batch table

```bash
bash YTsaurus/create_silver_tables.sh
yt list //home/silver_stage
```

-> `greenhub_telemetry`.

### 5.3. Gold marts

```bash
bash YTsaurus/create_gold_tables.sh
yt list //home/gold_stage
```

Ожидаемо: 4 gold-витрины.

### 5.4. Silver streaming tables

```bash
bash YTsaurus/silver_kafka.txt
yt list //home/silver_stage
```

-> `streaming_posts`, `streaming_comments`, `streaming_todos`.

### 5.5. Ops logs

```bash
bash scripts/init_ytsaurus_logs.sh
bash scripts/init_greenhub_ops_logs.sh

yt list //home/ops_logs
```


-> Все batch, streaming и ops tables видны через `yt list` и в YT UI.

---

## 6. CHYT

CHYT поднимать после YTsaurus и после patch execNodes.

### 6.1. CHYT CR

```bash
cat > ~/chyt-cr.yaml <<'EOF_CHYT'
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
EOF_CHYT

kubectl apply -f ~/chyt-cr.yaml
sleep 60

kubectl get pods -n default | grep strawberry
```

### 6.2. Persist patch / speclet

```bash
cd /home/chig_k3s/git/repo
bash scripts/chyt-persist.sh
```

Если используется restore pack:

```bash
bash restore_pack/scripts/chyt-persist.sh
```

Скрипт должен применить:

- `entrypointWrapper` в execNodes: `pip3 install requests certifi urllib3 idna` перед запуском контейнера;
- `jobImage = ghcr.io/ytsaurus/chyt:2.14.0-relwithdebinfo`;
- `controllerAgents.resources.limits = 4 CPU / 8 GiB`;
- `//sys/pool_trees/default/chyt/@strong_guarantee_resources = {cpu=18}`;
- `//sys/strawberry/chyt/ch_public/speclet` с `instance_cpu=18`, `instance_memory=65Gi`.

### 6.3. Проверка CHYT

```bash
sleep 60
yt clickhouse execute "SELECT 1+1" --alias ch_public --proxy localhost:31103
```

-> `2`.

-> CHYT clique отвечает на SQL-запросы через `yt clickhouse execute`.

---

## 7. Kafka и Kafka UI

### 7.1. Kafka Helm values

`infra/kafka-values.yaml` и `infra/kafka-ui-values.yaml` должны быть в repo.

Если нужно быстро создать/проверить values для Kafka UI:

```bash
mkdir -p ~/helm-values
cat > ~/helm-values/kafka-ui.yaml <<'EOF_KAFKA_UI'
nodeSelector:
  kubernetes.io/hostname: vm5-k3s-ing-server
service:
  type: NodePort
yamlApplicationConfig:
  kafka:
    clusters:
    - bootstrapServers: kafka.kafka.svc.cluster.local:9092
      name: ytsaurus-lab
EOF_KAFKA_UI
```

### 7.2. Установка Kafka

```bash
cd /home/chig_k3s/git/repo

helm install kafka oci://registry-1.docker.io/bitnamicharts/kafka \
  --version 32.4.3 \
  --namespace kafka \
  --create-namespace \
  -f infra/kafka-values.yaml

helm install kafka-ui kafbat-ui/kafka-ui \
  --namespace kafka \
  --version 1.6.4 \
  -f infra/kafka-ui-values.yaml

kubectl get pods -n kafka -w
```

Ждать `Running` для Kafka broker/controller и Kafka UI.

### 7.3. Topics, YT queue, consumer, dynamic bronze

Рекомендуемый вариант через restore script:

```bash
bash restore_pack/scripts/init-streaming.sh
```

Что делает скрипт:

- создаёт `__consumer_offsets` вручную;
- создаёт topic `raw-events`;
- создаёт dynamic table `//home/raw_stage/raw_events` с 3 tablets;
- создаёт consumer table `//home/raw_stage/raw_events_consumer`;
- регистрирует YT queue consumer;
- создаёт `//home/bronze_stage/bronze_t0`, `bronze_t1`, `bronze_t2`.

```bash
kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic __consumer_offsets \
  --partitions 50 --replication-factor 1 \
  --config cleanup.policy=compact

kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic raw-events \
  --partitions 1 --replication-factor 1
```

YT queue:

```bash
yt create table //home/raw_stage/raw_events --attributes '{
  dynamic=%true;
  schema=[
    {name=source; type=string};
    {name=stream_id; type=string};
    {name=event_type; type=string};
    {name=event_ts; type=string};
    {name=entity_id; type=string};
    {name=payload_json; type=string};
    {name=kafka_topic; type=string};
    {name=kafka_partition; type=int64};
    {name=kafka_offset; type=int64};
    {name=ingested_at; type=string};
    {name=batch_id; type=string};
  ]
}' --ignore-existing

yt unmount-table //home/raw_stage/raw_events 2>/dev/null || true
yt reshard-table //home/raw_stage/raw_events --tablet-count 3
yt mount-table //home/raw_stage/raw_events
```

Consumer table:

```bash
yt create table //home/raw_stage/raw_events_consumer --attributes '{
  dynamic=%true; treat_as_queue_consumer=%true;
  schema=[
    {name=queue_cluster; type=string; sort_order=ascending};
    {name=queue_path; type=string; sort_order=ascending};
    {name=partition_index; type=uint64; sort_order=ascending};
    {name=offset; type=uint64};
    {name=meta; type=any};
  ]
}' --ignore-existing

yt mount-table //home/raw_stage/raw_events_consumer

yt register-queue-consumer //home/raw_stage/raw_events //home/raw_stage/raw_events_consumer --vital true
```

Bronze dynamic tables:

```bash
for i in 0 1 2; do
  yt create table //home/bronze_stage/bronze_t$i --attributes '{
    dynamic=%true;
    schema=[
      {name=stream_id; type=string; sort_order=ascending};
      {name=row_index; type=int64; sort_order=ascending};
      {name=tablet_index; type=int64};
      {name=source; type=string};
      {name=event_type; type=string};
      {name=event_ts; type=string};
      {name=entity_id; type=string};
      {name=payload_json; type=string};
      {name=queue_timestamp; type=uint64};
      {name=processed_at; type=string};
      {name=batch_id; type=string};
    ]
  }' --ignore-existing
  yt mount-table //home/bronze_stage/bronze_t$i
done
```
--->

```bash
kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

yt get //home/raw_stage/raw_events/@tablet_count
```

--->

- topics: `__consumer_offsets`, `raw-events`;
- tablet count: `3`.

---

## 8. Airflow

### 8.1. Установка

```bash
kubectl create namespace airflow
```

Если repo приватный:

```bash
kubectl create secret generic airflow-git-credentials \
  -n airflow \
  --from-literal=GIT_SYNC_USERNAME='GH_USER' \
  --from-literal=GIT_SYNC_PASSWORD='GH_PAT'
```

Установка chart:

```bash
helm repo add apache-airflow https://airflow.apache.org/
helm repo update

helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --version 1.20.0 \
  -f infra/airflow-values.yaml

kubectl get pods -n airflow -w
```

### 8.2. Variables

В Airflow UI: `Admin -> Variables`.

| Name | Value | Используется |
|---|---|---|
| `YT_PROXY` | `http://localhost:31103` | все DAG |
| `S3_ACCESS_KEY` | значение из SeaweedFS | `s3_to_bronze` |
| `S3_SECRET_KEY` | значение из SeaweedFS | `s3_to_bronze`, `bronze_to_silver` |
| `GREENHUB_S3_BUCKET` | `greenhub` | `bronze_to_silver` |
| `GREENHUB_AWS_CONN_ID` | `seaweedfs_s3` | `bronze_to_silver` |
| `GREENHUB_SSH_CONN_ID` | `vm1_ssh` | `bronze_to_silver`, `silver_to_gold` |
| `GREENHUB_S3_STATE_KEY` | `_state/greenhub_load.json` | `s3_to_bronze`, `bronze_to_silver` |
| `GREENHUB_SILVER_LAUNCHER` | `/home/chig_k3s/git/repo/spyt/launch_bronze_to_silver.sh` | `bronze_to_silver` |
| `GREENHUB_GOLD_LAUNCHER` | `/home/chig_k3s/git/repo/spyt/launch_silver_to_gold.sh` | `silver_to_gold` |

### 8.3. Connections

В Airflow UI: `Admin -> Connections`.

| ID | Type | Параметры |
|---|---|---|
| `seaweedfs_s3` | Amazon Web Services | Host `http://10.130.0.35:8333`, login = `S3_ACCESS_KEY`, password = `S3_SECRET_KEY` |
| `vm1_ssh` | SSH | Host `10.130.0.24`, login `chig_k3s`, key из `.ssh/id_ed25519` |

### 8.4. Smoke test

В Airflow UI запустить `test_dag`.

**Критерий готовности**

- `test_dag` прошёл в `Success`.
- Остальные DAG-и появились в UI через gitSync.
- Основные DAG-и пока можно оставить `Paused` до запуска pipeline-ов.

---

## 9. SPYT

SPYT запускать после готовности YTsaurus, Cypress-директорий и execNodes patch.

### 9.1. Проверка Java / PySpark

```bash
sudo apt-get install -y openjdk-17-jdk-headless
java -version
ls -la /usr/lib/jvm/ | grep -E "java-17"

source /home/chig_k3s/yt-env/bin/activate
pip install "pyspark>=3.5,<4.0"
python -c "import pyspark; print(pyspark.__version__)"
```

### 9.2. Discovery paths

```bash
yt create map_node //home/spark --ignore-existing
yt create map_node //home/spark/discovery --ignore-existing
```

### 9.3. systemd autostart

```bash
cd /home/chig_k3s/git/repo

sudo bash infra/spyt_launch.sh
sudo bash infra/systemd_spyt.sh

sudo systemctl daemon-reload
sudo systemctl enable spyt-cluster.service
sudo systemctl start spyt-cluster.service
sudo systemctl status spyt-cluster.service
```

Параметры запуска внутри wrapper должны быть примерно такими:

```text
--worker-cores 5
--worker-memory 20G
--worker-num 1
--master-memory-limit 2G
--enable-livy
--disable-tmpfs
--abort-existing
--spyt-version 2.9.1
```

Важно: `--disable-tmpfs` нужен, потому что `aws-java-sdk-bundle` около 280 MB может не влезть в стандартный tmpfs 1 GiB, и executor падает на pulling jars.

### 9.4. Проверка SPYT

```bash
yt list //home/spark/discovery/main
sudo journalctl -u spyt-cluster -n 30
```

--->

- `spyt-cluster.service` active.
- В `//home/spark/discovery/main` есть discovery-файлы Spark master/worker.
- В journal есть сообщение вида `[SPYT] launched`.

---

## 10. Streaming consumers

Три Python-процесса в одном systemd unit:

1. `api_to_kafka`
2. `kafka_to_raw`
3. `raw_to_bronze`

### 10.1. Установка unit

```bash
cd /home/chig_k3s/git/repo
bash scripts/install-streaming-systemd.sh
```

Скрипт устанавливает:

- `/usr/local/bin/start-streaming-consumers.sh` — wrapper с watchdog;
- `/etc/systemd/system/greenhub-streaming-consumers.service`;
- зависимости `requests`, `confluent-kafka` в `yt-env`;
- fixed `kafka/raw_to_bronze.py` с `insert_rows` вместо `write_table`.

### 10.2. Запуск

```bash
sudo systemctl daemon-reload
sudo systemctl start greenhub-streaming-consumers.service
sleep 60
sudo systemctl status greenhub-streaming-consumers.service
```

### 10.3. Проверка логов и счётчиков

```bash
tail -10 /home/chig_k3s/main_d/logs/api_to_kafka.log
tail -10 /home/chig_k3s/main_d/logs/kafka_to_raw.log
tail -10 /home/chig_k3s/main_d/logs/raw_to_bronze.log
```

Ожидаемые признаки:

```text
api_to_kafka.log: cycle=N rows=800 grand_total=... sleeping=30s
kafka_to_raw.log: inserted_to_raw rows=100 ... batch_id=...
raw_to_bronze.log: processed total=300 t0=100 t1=100 t2=100 ...
```

Проверить строки:

```bash
yt select-rows "sum(1) as c from [//home/raw_stage/raw_events] group by 1" --format '<format=text>yson'
yt select-rows "sum(1) as c from [//home/bronze_stage/bronze_t0] group by 1" --format '<format=text>yson'
yt select-rows "sum(1) as c from [//home/bronze_stage/bronze_t1] group by 1" --format '<format=text>yson'
yt select-rows "sum(1) as c from [//home/bronze_stage/bronze_t2] group by 1" --format '<format=text>yson'
```

Проверить tablet cells:

```bash
yt list //sys/tablet_cells --attribute health --format '<format=text>yson' | head
```

**Критерий готовности**

- `greenhub-streaming-consumers.service` active.
- Счётчики в raw и bronze растут.
- Tablet cells в состоянии `good`.

---

## 11. Запуск batch pipeline-ов

Перед запуском положить файлы данных GreenHub в SeaweedFS bucket `greenhub/`.

### 11.1. S3 to Bronze

В Airflow UI запустить DAG:

```text
greenhub_s3_to_bronze
```

Проверка:

```bash
yt select-rows "count(*) from [//home/bronze_stage/greenhub/fact_telemetry]"
```

### 11.2. Bronze to Silver

В Airflow UI запустить DAG:

```text
greenhub_bronze_to_silver
```

Проверка:

```bash
yt select-rows "count(*) from [//home/silver_stage/greenhub_telemetry]"
```

### 11.3. Silver to Gold

В Airflow UI запустить DAG:

```text
greenhub_silver_to_gold
```

Проверка:

```bash
yt list //home/gold_stage
```

**Критерий готовности**

Batch DAG-и проходят в `Success`, данные появляются в bronze, silver и gold.

---

## 12. Streaming silver pipeline

В Airflow UI включить DAG:

```text
streaming_silver_yql
```

DAG должен запускаться каждые 5 минут.

Проверка:

```bash
yt select-rows "count(*) from [//home/silver_stage/streaming_posts]"
yt select-rows "count(*) from [//home/silver_stage/streaming_comments]"
yt select-rows "count(*) from [//home/silver_stage/streaming_todos]"
```

Audit:

```bash
yt select-rows "* from [//home/ops_logs/airflow/streaming_silver_runs] limit 10"
```

**Критерий готовности**

- `streaming_silver_yql` запускается по расписанию.
- Silver streaming таблицы наполняются.
- Audit log пишет строки в `//home/ops_logs/airflow/streaming_silver_runs`.

---

## 13. DataLens

В DataLens UI:

1. Создать connection типа `CHYT`.
2. Host: внешний IP `vm1`.
3. Port: `31103`.
4. Token: значение из `cat ~/.yt/token`.
5. Clique alias: `*ch_public`.
6. Проверить запрос:

```sql
SELECT count(*) FROM "//home/gold_stage/gold_daily_country_stats"
```

После проверки создать dataset и dashboard.

**Критерий готовности**

DataLens подключается к CHYT и читает gold-витрины.

---

## 14. Финальная проверка стенда

```bash
kubectl get pods -A
```

Не должно быть `CrashLoopBackOff`, `ImagePullBackOff`, постоянных `Pending`.

```bash
yt list /
```

Должен быть ответ от YTsaurus.

```bash
yt clickhouse execute "SELECT 1+1" --alias ch_public --proxy localhost:31103
```

--> `2`.

```bash
kubectl exec -n kafka kafka-controller-0 -- /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

-->  `__consumer_offsets`, `raw-events`.

```bash
sudo systemctl status spyt-cluster greenhub-streaming-consumers --no-pager
```

Проверить streaming:

```bash
yt select-rows "count(*) from [//home/raw_stage/raw_events]"
yt select-rows "count(*) from [//home/bronze_stage/bronze_t0]"
yt select-rows "count(*) from [//home/bronze_stage/bronze_t1]"
yt select-rows "count(*) from [//home/bronze_stage/bronze_t2]"
```

Проверить batch:

```bash
yt select-rows "count(*) from [//home/bronze_stage/greenhub/fact_telemetry]"
yt select-rows "count(*) from [//home/silver_stage/greenhub_telemetry]"
yt list //home/gold_stage
```


## 15. Возможные проблемы и решения

| Симптом | Диагностика | Решение |
|---|---|---|
| CHYT `clickhouse-trampoline` падает на `ImportError: requests` | `kubectl logs strawberry-...` | Запустить `scripts/chyt-persist.sh`, он добавляет `entrypointWrapper` с `pip3 install requests certifi urllib3 idna` |
| SPYT executor падает на pulling `aws-java-sdk-bundle` около 280 MB | Spark logs / journal `spyt-cluster` | Убедиться, что в `spark-launch-yt` есть `--disable-tmpfs` |
| Kafka consumer пишет `COORDINATOR_NOT_AVAILABLE` | `kafka-consumer-groups.sh --list` пуст или consumer не стартует | Создать `__consumer_offsets` вручную с `cleanup.policy=compact` |
| `write_table API is not supported` для dynamic table | `grep yt.write_table kafka/raw_to_bronze.py` | Использовать версию с `insert_rows`, обычно из `restore_pack/kafka/raw_to_bronze.py` |
| Tablet cells `failed` или `leader_recovery` под нагрузкой | `yt list //sys/tablet_cells --attribute health` | Остановить streaming consumers на 1-2 минуты, затем перезапустить; при повторе снизить producer rate или поднять `tnd` до 8 CPU / 32 GiB |
| Controller-agent OOM / snapshot timeout | `kubectl describe pod`, events `OOMKilled` | Поднять controller-agent до 4 CPU / 8 GiB через `chyt-persist.sh` |
| Airflow worker не может SSH на `vm1` | Test connection в Airflow UI | Проверить host `10.130.0.24`, login `chig_k3s`, приватный ключ и firewall на 22 |
| YT pod падает с OOMKilled | `kubectl describe pod <pod>` | Поднять limits через `kubectl patch ytsaurus ...` |
| Spark / ytTable connector не находит таблицу | Spark logs | Проверить `yt list <path>` и что таблица создана до запуска DAG |
| Worker / host не резолвит `*.svc.cluster.local` | `getent hosts hp-0.http-proxies.default.svc.cluster.local` | Применить DNS patch в `/etc/systemd/resolved.conf.d/cluster-local.conf` и restart `systemd-resolved` |

---

## 16. Мин. команд для восстановления

```bash
# 1. vm1: k3s server + kubeconfig
curl -sfL https://get.k3s.io | sh -s - --write-kubeconfig-mode 644 --disable traefik
mkdir -p ~/.kube && sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $USER:$USER ~/.kube/config && chmod 600 ~/.kube/config
export KUBECONFIG=~/.kube/config

# 2. vm2..vm5: k3s agent через K3S_URL + K3S_TOKEN

# 3. vm1: labels + DNS на всех ВМ
kubectl label node vm2-k3s-ing-server yt-role=heavy --overwrite
kubectl label node vm3-k3s-ing-server yt-role=heavy --overwrite
kubectl label node vm4-k3s-ing-server yt-role=storage --overwrite
kubectl label node vm5-k3s-ing-server yt-role=kafka --overwrite

# 4. vm1: Java, Helm, cert-manager, venv, repo
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk-headless python3-venv unzip uuid-runtime
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | sudo bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml
python3 -m venv /home/chig_k3s/yt-env
source /home/chig_k3s/yt-env/bin/activate
pip install --upgrade pip ytsaurus-client ytsaurus-yson ytsaurus-spyt "pyspark>=3.5,<4.0" requests confluent-kafka

git clone https://github.com/chiglo-k/ytsaurus_k3s_ycloud.git /home/chig_k3s/git/repo
cd /home/chig_k3s/git/repo

# 5. YTsaurus operator + secret + minisaurus CR
helm install ytsaurus oci://ghcr.io/ytsaurus/ytop-chart --version 0.31.0 --namespace ytsaurus-operator --create-namespace
kubectl create secret generic ytadminsec --from-literal=login=admin --from-literal=password=passworf --from-literal=token=$(openssl rand -hex 16) -n default
kubectl apply -f ~/minisaurus-cr.yaml

# 6. Token + Cypress + tables
export YT_PROXY=http://localhost:31103
export YT_TOKEN=$(kubectl get secret ytadminsec -n default -o jsonpath='{.data.token}' | base64 -d)
mkdir -p ~/.yt && echo $YT_TOKEN > ~/.yt/token && chmod 600 ~/.yt/token
bash YTsaurus/script_table.sh
bash YTsaurus/create_silver_tables.sh
bash YTsaurus/create_gold_tables.sh
bash YTsaurus/silver_kafka.txt
bash scripts/init_ytsaurus_logs.sh
bash scripts/init_greenhub_ops_logs.sh

# 7. execNodes patch + CHYT
bash scripts/chyt-persist.sh

# 8. Kafka + init streaming
helm install kafka oci://registry-1.docker.io/bitnamicharts/kafka --version 32.4.3 --namespace kafka --create-namespace -f infra/kafka-values.yaml
helm install kafka-ui kafbat-ui/kafka-ui --namespace kafka --version 1.6.4 -f infra/kafka-ui-values.yaml
bash restore_pack/scripts/init-streaming.sh

# 9. Airflow
kubectl create namespace airflow
helm repo add apache-airflow https://airflow.apache.org/
helm repo update
helm install airflow apache-airflow/airflow --namespace airflow --version 1.20.0 -f infra/airflow-values.yaml

# 10. SPYT + streaming systemd
sudo bash infra/spyt_launch.sh
sudo bash infra/systemd_spyt.sh
sudo systemctl daemon-reload
sudo systemctl enable --now spyt-cluster.service
bash scripts/install-streaming-systemd.sh
sudo systemctl start greenhub-streaming-consumers.service
```

