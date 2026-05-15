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
