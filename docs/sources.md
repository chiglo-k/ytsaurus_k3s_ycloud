vm1 — control plane, только kubectl/helm
vm2 — qt, yqla, strawberry, ui, dnd-2
vm3 — ms, hp, rp, ca, sch, qa, dnd-1
vm4 — ds, end, hp-control, rp-heavy, tnd, dnd-0
vm5 — зарезервирована под Airflow/Kafka


1. создаем вм
2. создаем ingress 

```
curl -sfL https://get.k3s.io | sh -s - server \
  --disable traefik \
  --disable servicelb \
  --node-name vm1
```

3. на остальных машинах подрубаем кубер с ключом

```
curl -sfL https://get.k3s.io | 
K3S_URL=https://10.130.0.24:6443 
K3S_TOKEN=<token> sh -s - agent --node-name vm{}
```

4. чек ап нод на ингресс машине

```
sudo kubectl get nodes
```

5. helm 

``` 
helm curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash 
```

5.1 check helm

``` 
helm version
```

5.2 taint

```
sudo kubectl taint nodes vm node-role.kubernetes.io/control-plane:NoSchedule
```

6. YTsaurus

```
sudo kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.11.0/cert-manager.yaml
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm install ytsaurus oci://ghcr.io/ytsaurus/ytop-chart --version 0.31.0 --namespace ytsaurus-operator --create-namespace
sudo kubectl get pods -n ytsaurus-operator
```

6.1 Созадем namespace 

```
sudo kubectl create namespace ytsaurus
```

6.2 Изменение конфига

```
curl -s https://raw.githubusercontent.com/ytsaurus/ytsaurus/refs/heads/main/yt/docs/code-examples/cluster-config/cluster_v1_local.yaml > ~/main_d/ytsaurus-cluster.yaml

nano ~/main_d/ytsaurus-cluster.yaml

sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl apply -f ~/main_d/ytsaurus-cluster.yaml
sudo kubectl get ytsaurus --watch
```

6.3. МОЙ косяк вм не изолировал, персборка

```
sudo kubectl taint nodes vm5 role=apps:NoSchedule
sudo kubectl drain vm5 --ignore-daemonsets --delete-emptydir-data
sudo bash -c 'echo "$(sudo kubectl get pod hp-0 -o jsonpath={.status.podIP}) hp-0.http-proxies.default.svc.cluster.local" >> /etc/hosts'
```

READY	контейнеры готовы / всего контейнеров в поде
STATUS	текущее состояние (Running/Pending/Init)
RESTARTS	сколько раз падал и перезапускался
AGE	сколько живёт
IP	внутренний IP пода в сети k8s
NODE	на какой ВМ запущен
Что за поды:

Pod
ms-0	master	мозг YTsaurus, хранит метаданные
ds-0	discovery	помогает компонентам находить друг друга
dnd-0/1/2	data node	хранят данные (чанки таблиц)
end-0	exec node	выполняет вычислительные джобы (SPYT)
hp-0	http proxy	HTTP точка входа в кластер
hp-control-0	http proxy control	прокси для системных операций
rp-0	rpc proxy	RPC точка входа
rp-heavy-0	rpc proxy heavy	для тяжёлых запросов
sch-0	scheduler	планировщик джобов
ca-0	controller agent	управляет операциями
tnd-0	tablet node	для dynamic tables (Kafka pipeline)
qa-0	queue agent	обслуживает очереди
qt-0	query tracker	отслеживает YQL запросы
yqla-0	YQL agent	выполняет YQL запросы
strawberry-controller	strawberry	управляет CHYT кликами
ytsaurus-ui-deployment	UI	веб-интерфейс Cypress\


7. SPYT/CHYT, тестовая таблица через YQL

```
sudo apt-get update && sudo apt install python3-pip --fix-missing 
```

```
cat >> ~/.bashrc << 'EOF'
export YT_PROXY=http://localhost:31103
export YT_TOKEN=
alias yt="~/yt-env/bin/yt"
EOF
source ~/.bashrc


echo 'alias yt="~/yt-env/bin/yt"' >> ~/.bashrc && source ~/.bashrc
```


yt list //raw_stage - файлы/таблицы
yt create table //raw_stage/test - создание таблиц
yt write-table //raw_stage/test — запись данных
yt read-table //raw_stage/test — читть данные
yt select-rows "* from [//raw_stage/test]" — SQL запросы


```
yt create table //home/raw_stage/test \
  --attr '{schema=[{name=id;type=int64};{name=name;type=string};{name=value;type=double}]}'
```


8. ИНФРА

8.1 Airflow

```
helm repo add apache-airflow https://airflow.apache.org

nano ~/main_d/airflow-values.yaml 


webserver:
  tolerations:
    - key: role
      value: apps
      effect: NoSchedule
  nodeSelector:
    kubernetes.io/hostname: vm5
  service:
    type: NodePort

scheduler:
  tolerations:
    - key: role
      value: apps
      effect: NoSchedule
  nodeSelector:
    kubernetes.io/hostname: vm5

workers:
  tolerations:
    - key: role
      value: apps
      effect: NoSchedule
  nodeSelector:
    kubernetes.io/hostname: vm5

triggerer:
  tolerations:
    - key: role
      value: apps
      effect: NoSchedule
  nodeSelector:
    kubernetes.io/hostname: vm5

dagProcessor:
  tolerations:
    - key: role
      value: apps
      effect: NoSchedule
  nodeSelector:
    kubernetes.io/hostname: vm5

statsd:
  tolerations:
    - key: role
      value: apps
      effect: NoSchedule
  nodeSelector:
    kubernetes.io/hostname: vm5

redis:
  tolerations:
    - key: role
      value: apps
      effect: NoSchedule
  nodeSelector:
    kubernetes.io/hostname: vm5

postgresql:
  primary:
    tolerations:
      - key: role
        value: apps
        effect: NoSchedule
    nodeSelector:
      kubernetes.io/hostname: vm5

createUserJob:
  tolerations:
    - key: role
      value: apps
      effect: NoSchedule
  nodeSelector:
    kubernetes.io/hostname: vm5

migrateDatabaseJob:
  tolerations:
    - key: role
      value: apps
      effect: NoSchedule
  nodeSelector:
    kubernetes.io/hostname: vm5
```

```
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  --values ~/main_d/airflow-values.yaml \
  --timeout 10m

```

### Надо с sudo сразу устанавливать, иначе он начинает ругаться

```
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm repo add apache-airflow https://airflow.apache.org
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm repo update

sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  --values ~/main_d/airflow-values.yaml
```

```
sudo kubectl describe pod airflow-redis-0 -n airflow | grep -A 10 Events
```

```
sudo kubectl create secret generic airflow-git-credentials \
  -n airflow \
  --from-literal=GIT_SYNC_USERNAME='chiglo-k' \
  --from-literal=GIT_SYNC_PASSWORD='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'


  ```
  sudo kubectl get svc -n airflow
  ```


### Kafka

```
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm repo add kafka-ui https://provectus.github.io/kafka-ui-charts
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm repo update
```

```
image:
  registry: docker.io
  repository: bitnamilegacy/kafka
  tag: 4.0.0-debian-12-r10

kraft:
  enabled: true

zookeeper:
  enabled: false

controller:
  replicaCount: 1
  nodeSelector:
    kubernetes.io/hostname: vm5
  tolerations:
    - key: role
      operator: Equal
      value: apps
      effect: NoSchedule
  persistence:
    enabled: true
    size: 8Gi

broker:
  replicaCount: 1
  nodeSelector:
    kubernetes.io/hostname: vm5
  tolerations:
    - key: role
      operator: Equal
      value: apps
      effect: NoSchedule
  persistence:
    enabled: true
    size: 20Gi

listeners:
  client:
    protocol: PLAINTEXT
  controller:
    protocol: PLAINTEXT
  interbroker:
    protocol: PLAINTEXT

externalAccess:
  enabled: false
```
```
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm upgrade kafka oci://registry-1.docker.io/bitnamicharts/kafka \
  -n kafka \
  -f ~/main_d/kafka-values.yaml \
  --timeout 15m
```

```
nano ~/main_d/kafka-ui-values.yaml

yamlApplicationConfig:
  kafka:
    clusters:
      - name: ytsaurus-lab
        bootstrapServers: kafka.kafka.svc.cluster.local:9092

service:
  type: NodePort

nodeSelector:
  kubernetes.io/hostname: vm5

tolerations:
  - key: role
    operator: Equal
    value: apps
    effect: NoSchedule

```

```
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm repo add kafka-ui https://provectus.github.io/kafka-ui-charts
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm repo update
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm upgrade --install kafka-ui kafka-ui/kafka-ui \
  -n kafka \
  -f ~/main_d/kafka-ui-values.yaml \
  --timeout 10m

sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm upgrade kafka oci://registry-1.docker.io/bitnamicharts/kafka \
  -n kafka \
  -f ~/main_d/kafka-values.yaml \
  -f ~/main_d/kafka-external-values.yaml
```
```
teesting
sudo kubectl run kafka-client --restart='Never' --image docker.io/bitnamilegacy/kafka:4.0.0-debian-12-r10 --namespace kafka --command -- sleep infinity
sudo kubectl exec --tty -i kafka-client --namespace kafka -- bash
kafka-topics.sh --bootstrap-server kafka.kafka.svc.cluster.local:9092 --create --topic test --partitions 1 --replication-factor 1
kafka-topics.sh --bootstrap-server kafka.kafka.svc.cluster.local:9092 --list
kafka-console-producer.sh --bootstrap-server kafka.kafka.svc.cluster.local:9092 --topic test
{"id":1,"event":"hello","ts":"2026-04-25T05:20:00Z"}
```

### Consumer YTsaurus

```
yt create table //home/queue/events_queue --attributes '{
  dynamic=%true;
  schema=[
    {name=source;type=string};
    {name=stream_id;type=string};
    {name=event_type;type=string};
    {name=event_ts;type=string};
    {name=entity_id;type=string};
    {name=payload_json;type=string};
    {name=ingest_batch_id;type=string};
    {name="$timestamp";type=uint64}
  ]
}'
```

```
chig_k3s@vm1-k3s-ing-server:~/main_d$ yt unmount-table //home/queue/events_queue --sync
chig_k3s@vm1-k3s-ing-server:~/main_d$ yt reshard-table //home/queue/events_queue --tablet-count 3
chig_k3s@vm1-k3s-ing-server:~/main_d$ yt mount-table //home/queue/events_queue --sync
chig_k3s@vm1-k3s-ing-server:~/main_d$ yt get //home/queue/events_queue/@tablet_count
3u
chig_k3s@vm1-k3s-ing-server:~/main_d$ yt get //home/queue/events_queue/@tablets
[
    {
        "index" = 0;
        "performance_counters" = {
            "dynamic_row_read_count" = 0;
            "dynamic_row_read_rate" = 0.;
            "dynamic_row_read_10m_rate" = 0.;
            "dynamic_row_read_1h_rate" = 0.;
            "dynamic_row_read_data_weight_count" = 0;
            "dynamic_row_read_data_weight_rate" = 0.;
            "dynamic_row_read_data_weight_10m_rate" = 0.;
            "dynamic_row_read_data_weight_1h_rate" = 0.;
            "dynamic_row_lookup_count" = 0;
            "dynamic_row_lookup_rate" = 0.;
            "dynamic_row_lookup_10m_rate" = 0.;
            "dynamic_row_lookup_1h_rate" = 0.;
            "dynamic_row_lookup_data_weight_count" = 0;
            "dynamic_row_lookup_data_weight_rate" = 0.;
            "dynamic_row_lookup_data_weight_10m_rate" = 0.;
            "dynamic_row_lookup_data_weight_1h_rate" = 0.;
            "dynamic_row_write_count" = 0;
            "dynamic_row_write_rate" = 0.;
            "dynamic_row_write_10m_rate" = 0.;
            "dynamic_row_write_1h_rate" = 0.;
            "dynamic_row_write_data_weight_count" = 0;
            "dynamic_row_write_data_weight_rate" = 0.;
            "dynamic_row_write_data_weight_10m_rate" = 0.;
            "dynamic_row_write_data_weight_1h_rate" = 0.;
            "dynamic_row_delete_count" = 0;
            "dynamic_row_delete_rate" = 0.;
            "dynamic_row_delete_10m_rate" = 0.;
            "dynamic_row_delete_1h_rate" = 0.;
            "static_chunk_row_read_count" = 0;
            "static_chunk_row_read_rate" = 0.;
            "static_chunk_row_read_10m_rate" = 0.;
            "static_chunk_row_read_1h_rate" = 0.;
            "static_chunk_row_read_data_weight_count" = 0;
            "static_chunk_row_read_data_weight_rate" = 0.;
            "static_chunk_row_read_data_weight_10m_rate" = 0.;
            "static_chunk_row_read_data_weight_1h_rate" = 0.;
            "static_chunk_row_lookup_count" = 0;
            "static_chunk_row_lookup_rate" = 0.;
            "static_chunk_row_lookup_10m_rate" = 0.;
            "static_chunk_row_lookup_1h_rate" = 0.;
            "static_chunk_row_lookup_data_weight_count" = 0;
            "static_chunk_row_lookup_data_weight_rate" = 0.;
            "static_chunk_row_lookup_data_weight_10m_rate" = 0.;
            "static_chunk_row_lookup_data_weight_1h_rate" = 0.;
            "compaction_data_weight_count" = 0;
            "compaction_data_weight_rate" = 0.;
            "compaction_data_weight_10m_rate" = 0.;
            "compaction_data_weight_1h_rate" = 0.;
            "partitioning_data_weight_count" = 0;
            "partitioning_data_weight_rate" = 0.;
            "partitioning_data_weight_10m_rate" = 0.;
            "partitioning_data_weight_1h_rate" = 0.;
            "lookup_error_count" = 0;
            "lookup_error_rate" = 0.;
            "lookup_error_10m_rate" = 0.;
            "lookup_error_1h_rate" = 0.;
            "write_error_count" = 0;
            "write_error_rate" = 0.;
            "write_error_10m_rate" = 0.;
            "write_error_1h_rate" = 0.;
            "lookup_cpu_time_count" = 0;
            "lookup_cpu_time_rate" = 0.;
            "lookup_cpu_time_10m_rate" = 0.;
            "lookup_cpu_time_1h_rate" = 0.;
            "select_cpu_time_count" = 0;
            "select_cpu_time_rate" = 0.;
            "select_cpu_time_10m_rate" = 0.;
            "select_cpu_time_1h_rate" = 0.;
            "static_hunk_chunk_row_read_data_weight_count" = 0;
            "static_hunk_chunk_row_read_data_weight_rate" = 0.;
            "static_hunk_chunk_row_read_data_weight_10m_rate" = 0.;
            "static_hunk_chunk_row_read_data_weight_1h_rate" = 0.;
            "static_hunk_chunk_row_lookup_data_weight_count" = 0;
            "static_hunk_chunk_row_lookup_data_weight_rate" = 0.;
            "static_hunk_chunk_row_lookup_data_weight_10m_rate" = 0.;
            "static_hunk_chunk_row_lookup_data_weight_1h_rate" = 0.;
        };
        "trimmed_row_count" = 0;
        "flushed_row_count" = 0;
        "state" = "mounted";
        "last_commit_timestamp" = 0u;
        "statistics" = {
            "chunk_count" = 0;
            "compressed_data_size" = 0;
            "disk_space" = 0;
            "disk_space_per_medium" = {
                "default" = 0;
            };
            "dynamic_memory_pool_size" = 0;
            "hunk_compressed_data_size" = 0;
            "hunk_uncompressed_data_size" = 0;
            "memory_size" = 0;
            "overlapping_store_count" = 0;
            "partition_count" = 0;
            "preload_completed_store_count" = 0;
            "preload_failed_store_count" = 0;
            "preload_pending_store_count" = 0;
            "store_count" = 0;
            "tablet_count" = 1;
            "tablet_count_per_memory_mode" = {
                "none" = 1;
                "compressed" = 0;
                "uncompressed" = 0;
            };
            "uncompressed_data_size" = 0;
            "unmerged_row_count" = 0;
        };
        "tablet_id" = "9-10c9f-102be-2b3fe0da";
        "cell_id" = "1-3dd-102bc-912e7bf1";
        "cell_leader_address" = "tnd-0.tablet-nodes.default.svc.cluster.local:9022";
        "mount_time" = "2026-04-25T06:09:17.162238Z";
        "mount_revision" = 38654774553u;
        "error_count" = 0;
        "replication_error_count" = 0;
    };
    {
        "index" = 1;
        "performance_counters" = {
            "dynamic_row_read_count" = 0;
            "dynamic_row_read_rate" = 0.;
            "dynamic_row_read_10m_rate" = 0.;
            "dynamic_row_read_1h_rate" = 0.;
            "dynamic_row_read_data_weight_count" = 0;
            "dynamic_row_read_data_weight_rate" = 0.;
            "dynamic_row_read_data_weight_10m_rate" = 0.;
            "dynamic_row_read_data_weight_1h_rate" = 0.;
            "dynamic_row_lookup_count" = 0;
            "dynamic_row_lookup_rate" = 0.;
            "dynamic_row_lookup_10m_rate" = 0.;
            "dynamic_row_lookup_1h_rate" = 0.;
            "dynamic_row_lookup_data_weight_count" = 0;
            "dynamic_row_lookup_data_weight_rate" = 0.;
            "dynamic_row_lookup_data_weight_10m_rate" = 0.;
            "dynamic_row_lookup_data_weight_1h_rate" = 0.;
            "dynamic_row_write_count" = 0;
            "dynamic_row_write_rate" = 0.;
            "dynamic_row_write_10m_rate" = 0.;
            "dynamic_row_write_1h_rate" = 0.;
            "dynamic_row_write_data_weight_count" = 0;
            "dynamic_row_write_data_weight_rate" = 0.;
            "dynamic_row_write_data_weight_10m_rate" = 0.;
            "dynamic_row_write_data_weight_1h_rate" = 0.;
            "dynamic_row_delete_count" = 0;
            "dynamic_row_delete_rate" = 0.;
            "dynamic_row_delete_10m_rate" = 0.;
            "dynamic_row_delete_1h_rate" = 0.;
            "static_chunk_row_read_count" = 0;
            "static_chunk_row_read_rate" = 0.;
            "static_chunk_row_read_10m_rate" = 0.;
            "static_chunk_row_read_1h_rate" = 0.;
            "static_chunk_row_read_data_weight_count" = 0;
            "static_chunk_row_read_data_weight_rate" = 0.;
            "static_chunk_row_read_data_weight_10m_rate" = 0.;
            "static_chunk_row_read_data_weight_1h_rate" = 0.;
            "static_chunk_row_lookup_count" = 0;
            "static_chunk_row_lookup_rate" = 0.;
            "static_chunk_row_lookup_10m_rate" = 0.;
            "static_chunk_row_lookup_1h_rate" = 0.;
            "static_chunk_row_lookup_data_weight_count" = 0;
            "static_chunk_row_lookup_data_weight_rate" = 0.;
            "static_chunk_row_lookup_data_weight_10m_rate" = 0.;
            "static_chunk_row_lookup_data_weight_1h_rate" = 0.;
            "compaction_data_weight_count" = 0;
            "compaction_data_weight_rate" = 0.;
            "compaction_data_weight_10m_rate" = 0.;
            "compaction_data_weight_1h_rate" = 0.;
            "partitioning_data_weight_count" = 0;
            "partitioning_data_weight_rate" = 0.;
            "partitioning_data_weight_10m_rate" = 0.;
            "partitioning_data_weight_1h_rate" = 0.;
            "lookup_error_count" = 0;
            "lookup_error_rate" = 0.;
            "lookup_error_10m_rate" = 0.;
            "lookup_error_1h_rate" = 0.;
            "write_error_count" = 0;
            "write_error_rate" = 0.;
            "write_error_10m_rate" = 0.;
            "write_error_1h_rate" = 0.;
            "lookup_cpu_time_count" = 0;
            "lookup_cpu_time_rate" = 0.;
            "lookup_cpu_time_10m_rate" = 0.;
            "lookup_cpu_time_1h_rate" = 0.;
            "select_cpu_time_count" = 0;
            "select_cpu_time_rate" = 0.;
            "select_cpu_time_10m_rate" = 0.;
            "select_cpu_time_1h_rate" = 0.;
            "static_hunk_chunk_row_read_data_weight_count" = 0;
            "static_hunk_chunk_row_read_data_weight_rate" = 0.;
            "static_hunk_chunk_row_read_data_weight_10m_rate" = 0.;
            "static_hunk_chunk_row_read_data_weight_1h_rate" = 0.;
            "static_hunk_chunk_row_lookup_data_weight_count" = 0;
            "static_hunk_chunk_row_lookup_data_weight_rate" = 0.;
            "static_hunk_chunk_row_lookup_data_weight_10m_rate" = 0.;
            "static_hunk_chunk_row_lookup_data_weight_1h_rate" = 0.;
        };
        "trimmed_row_count" = 0;
        "flushed_row_count" = 0;
        "state" = "mounted";
        "last_commit_timestamp" = 0u;
        "statistics" = {
            "chunk_count" = 0;
            "compressed_data_size" = 0;
            "disk_space" = 0;
            "disk_space_per_medium" = {
                "default" = 0;
            };
            "dynamic_memory_pool_size" = 0;
            "hunk_compressed_data_size" = 0;
            "hunk_uncompressed_data_size" = 0;
            "memory_size" = 0;
            "overlapping_store_count" = 0;
            "partition_count" = 0;
            "preload_completed_store_count" = 0;
            "preload_failed_store_count" = 0;
            "preload_pending_store_count" = 0;
            "store_count" = 0;
            "tablet_count" = 1;
            "tablet_count_per_memory_mode" = {
                "none" = 1;
                "compressed" = 0;
                "uncompressed" = 0;
            };
            "uncompressed_data_size" = 0;
            "unmerged_row_count" = 0;
        };
        "tablet_id" = "9-10c9f-102be-d9102da1";
        "cell_id" = "1-3dd-102bc-912e7bf1";
        "cell_leader_address" = "tnd-0.tablet-nodes.default.svc.cluster.local:9022";
        "mount_time" = "2026-04-25T06:09:17.162238Z";
        "mount_revision" = 38654774553u;
        "error_count" = 0;
        "replication_error_count" = 0;
    };
    {
        "index" = 2;
        "performance_counters" = {
            "dynamic_row_read_count" = 0;
            "dynamic_row_read_rate" = 0.;
            "dynamic_row_read_10m_rate" = 0.;
            "dynamic_row_read_1h_rate" = 0.;
            "dynamic_row_read_data_weight_count" = 0;
            "dynamic_row_read_data_weight_rate" = 0.;
            "dynamic_row_read_data_weight_10m_rate" = 0.;
            "dynamic_row_read_data_weight_1h_rate" = 0.;
            "dynamic_row_lookup_count" = 0;
            "dynamic_row_lookup_rate" = 0.;
            "dynamic_row_lookup_10m_rate" = 0.;
            "dynamic_row_lookup_1h_rate" = 0.;
            "dynamic_row_lookup_data_weight_count" = 0;
            "dynamic_row_lookup_data_weight_rate" = 0.;
            "dynamic_row_lookup_data_weight_10m_rate" = 0.;
            "dynamic_row_lookup_data_weight_1h_rate" = 0.;
            "dynamic_row_write_count" = 0;
            "dynamic_row_write_rate" = 0.;
            "dynamic_row_write_10m_rate" = 0.;
            "dynamic_row_write_1h_rate" = 0.;
            "dynamic_row_write_data_weight_count" = 0;
            "dynamic_row_write_data_weight_rate" = 0.;
            "dynamic_row_write_data_weight_10m_rate" = 0.;
            "dynamic_row_write_data_weight_1h_rate" = 0.;
            "dynamic_row_delete_count" = 0;
            "dynamic_row_delete_rate" = 0.;
            "dynamic_row_delete_10m_rate" = 0.;
            "dynamic_row_delete_1h_rate" = 0.;
            "static_chunk_row_read_count" = 0;
            "static_chunk_row_read_rate" = 0.;
            "static_chunk_row_read_10m_rate" = 0.;
            "static_chunk_row_read_1h_rate" = 0.;
            "static_chunk_row_read_data_weight_count" = 0;
            "static_chunk_row_read_data_weight_rate" = 0.;
            "static_chunk_row_read_data_weight_10m_rate" = 0.;
            "static_chunk_row_read_data_weight_1h_rate" = 0.;
            "static_chunk_row_lookup_count" = 0;
            "static_chunk_row_lookup_rate" = 0.;
            "static_chunk_row_lookup_10m_rate" = 0.;
            "static_chunk_row_lookup_1h_rate" = 0.;
            "static_chunk_row_lookup_data_weight_count" = 0;
            "static_chunk_row_lookup_data_weight_rate" = 0.;
            "static_chunk_row_lookup_data_weight_10m_rate" = 0.;
            "static_chunk_row_lookup_data_weight_1h_rate" = 0.;
            "compaction_data_weight_count" = 0;
            "compaction_data_weight_rate" = 0.;
            "compaction_data_weight_10m_rate" = 0.;
            "compaction_data_weight_1h_rate" = 0.;
            "partitioning_data_weight_count" = 0;
            "partitioning_data_weight_rate" = 0.;
            "partitioning_data_weight_10m_rate" = 0.;
            "partitioning_data_weight_1h_rate" = 0.;
            "lookup_error_count" = 0;
            "lookup_error_rate" = 0.;
            "lookup_error_10m_rate" = 0.;
            "lookup_error_1h_rate" = 0.;
            "write_error_count" = 0;
            "write_error_rate" = 0.;
            "write_error_10m_rate" = 0.;
            "write_error_1h_rate" = 0.;
            "lookup_cpu_time_count" = 0;
            "lookup_cpu_time_rate" = 0.;
            "lookup_cpu_time_10m_rate" = 0.;
            "lookup_cpu_time_1h_rate" = 0.;
            "select_cpu_time_count" = 0;
            "select_cpu_time_rate" = 0.;
            "select_cpu_time_10m_rate" = 0.;
            "select_cpu_time_1h_rate" = 0.;
            "static_hunk_chunk_row_read_data_weight_count" = 0;
            "static_hunk_chunk_row_read_data_weight_rate" = 0.;
            "static_hunk_chunk_row_read_data_weight_10m_rate" = 0.;
            "static_hunk_chunk_row_read_data_weight_1h_rate" = 0.;
            "static_hunk_chunk_row_lookup_data_weight_count" = 0;
            "static_hunk_chunk_row_lookup_data_weight_rate" = 0.;
            "static_hunk_chunk_row_lookup_data_weight_10m_rate" = 0.;
            "static_hunk_chunk_row_lookup_data_weight_1h_rate" = 0.;
        };
        "trimmed_row_count" = 0;
        "flushed_row_count" = 0;
        "state" = "mounted";
        "last_commit_timestamp" = 0u;
        "statistics" = {
            "chunk_count" = 0;
            "compressed_data_size" = 0;
            "disk_space" = 0;
            "disk_space_per_medium" = {
                "default" = 0;
            };
            "dynamic_memory_pool_size" = 0;
            "hunk_compressed_data_size" = 0;
            "hunk_uncompressed_data_size" = 0;
            "memory_size" = 0;
            "overlapping_store_count" = 0;
            "partition_count" = 0;
            "preload_completed_store_count" = 0;
            "preload_failed_store_count" = 0;
            "preload_pending_store_count" = 0;
            "store_count" = 0;
            "tablet_count" = 1;
            "tablet_count_per_memory_mode" = {
                "none" = 1;
                "compressed" = 0;
                "uncompressed" = 0;
            };
            "uncompressed_data_size" = 0;
            "unmerged_row_count" = 0;
        };
        "tablet_id" = "9-10c9f-102be-8097659c";
        "cell_id" = "1-3dd-102bc-912e7bf1";
        "cell_leader_address" = "tnd-0.tablet-nodes.default.svc.cluster.local:9022";
        "mount_time" = "2026-04-25T06:09:17.162238Z";
        "mount_revision" = 38654774553u;
        "error_count" = 0;
        "replication_error_count" = 0;
    };
]
```

```
YT_PROXY=http://localhost:31103 YT_USE_HOSTS=0 \
/home/chig_k3s/yt-env/bin/yt create table //home/raw_stage/raw_events --attributes '{
  dynamic=%true;
  tablet_count=3;
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
    {name="$timestamp"; type=uint64};
  ];
}'
YT_PROXY=http://localhost:31103 YT_USE_HOSTS=0 \
/home/chig_k3s/yt-env/bin/yt set //home/raw_stage/raw_events/@treat_as_queue_consumer false

YT_PROXY=http://localhost:31103 YT_USE_HOSTS=0 \
/home/chig_k3s/yt-env/bin/yt set //home/raw_stage/raw_events/@queue_agent_stage "production"

YT_PROXY=http://localhost:31103 YT_USE_HOSTS=0 \
/home/chig_k3s/yt-env/bin/yt mount-table //home/raw_stage/raw_events


yt create table //home/bronze_stage/bronze_t0 --attributes '{
  schema=[
    {name=source;type=string};
    {name=stream_id;type=string};
    {name=event_type;type=string};
    {name=event_ts;type=string};
    {name=entity_id;type=string};
    {name=payload_json;type=string};
    {name=queue_timestamp;type=uint64};
    {name=tablet_index;type=int64};
    {name=row_index;type=int64};
    {name=processed_at;type=string};
    {name=batch_id;type=string}
  ]
}'

yt create table //home/bronze_stage/bronze_t1 --attributes '{
  schema=[
    {name=source;type=string};
    {name=stream_id;type=string};
    {name=event_type;type=string};
    {name=event_ts;type=string};
    {name=entity_id;type=string};
    {name=payload_json;type=string};
    {name=queue_timestamp;type=uint64};
    {name=tablet_index;type=int64};
    {name=row_index;type=int64};
    {name=processed_at;type=string};
    {name=batch_id;type=string}
  ]
}'

yt create table //home/bronze_stage/bronze_t2 --attributes '{
  schema=[
    {name=source;type=string};
    {name=stream_id;type=string};
    {name=event_type;type=string};
    {name=event_ts;type=string};
    {name=entity_id;type=string};
    {name=payload_json;type=string};
    {name=queue_timestamp;type=uint64};
    {name=tablet_index;type=int64};
    {name=row_index;type=int64};
    {name=processed_at;type=string};
    {name=batch_id;type=string}
  ]
}'
```

```
yt get //home/raw_stage/bronze_t0/@row_count
yt get //home/raw_stage/bronze_t1/@row_count
yt get //home/raw_stage/bronze_t2/@row_count

cat ~/main_d/consumers/offsets.json

yt select-rows '* from [//home/queue/events_queue] where [$tablet_index] = 0 limit 5' --format json
yt select-rows '* from [//home/queue/events_queue] where [$tablet_index] = 1 limit 5' --format json
yt select-rows '* from [//home/queue/events_queue] where [$tablet_index] = 2 limit 5' --format json
```



### Prometheus

```
grafana:
  service:
    type: NodePort
  nodeSelector:
    kubernetes.io/hostname: vm5
  tolerations:
    - key: role
      operator: Equal
      value: apps
      effect: NoSchedule

prometheus:
  service:
    type: NodePort
  prometheusSpec:
    nodeSelector:
      kubernetes.io/hostname: vm5
    tolerations:
      - key: role
        operator: Equal
        value: apps
        effect: NoSchedule

alertmanager:
  service:
    type: NodePort
  alertmanagerSpec:
    nodeSelector:
      kubernetes.io/hostname: vm5
    tolerations:
      - key: role
        operator: Equal
        value: apps
        effect: NoSchedule

kube-state-metrics:
  nodeSelector:
    kubernetes.io/hostname: vm5
  tolerations:
    - key: role
      operator: Equal
      value: apps
      effect: NoSchedule

prometheus-node-exporter:
  tolerations:
    - operator: Exists
```
```
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm repo update

sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
  -n monitoring \
  --create-namespace \
  -f ~/main_d/monitoring-values.yaml \
  --timeout 15m

sudo kubectl get pods -n monitoring -o wide
sudo kubectl get svc -n monitoring
```

### Loki

```
deploymentMode: SingleBinary

singleBinary:
  replicas: 1
  nodeSelector:
    kubernetes.io/hostname: vm5
  tolerations:
    - key: role
      operator: Equal
      value: apps
      effect: NoSchedule

loki:
  auth_enabled: false

gateway:
  enabled: false

backend:
  replicas: 0
read:
  replicas: 0
write:
  replicas: 0

chunksCache:
  enabled: false

resultsCache:
  enabled: false

monitoring:
  dashboards:
    enabled: false
  rules:
    enabled: false
  serviceMonitor:
    enabled: false
```
```
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm repo add grafana-community https://grafana-community.github.io/helm-charts
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm repo update

sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm upgrade --install loki grafana-community/loki \
  -n loki \
  --create-namespace \
  -f ~/main_d/loki-values.yaml \
  --timeout 15m

sudo kubectl get pods -n loki -o wide
sudo kubectl get svc -n loki
```

### Alloy

```
controller:
  type: daemonset

alloy:
  configMap:
    create: true
    content: |
      discovery.kubernetes "pods" {
        role = "pod"
      }

      discovery.relabel "pod_logs" {
        targets = discovery.kubernetes.pods.targets

        rule {
          source_labels = ["__meta_kubernetes_namespace"]
          target_label  = "namespace"
        }

        rule {
          source_labels = ["__meta_kubernetes_pod_name"]
          target_label  = "pod"
        }

        rule {
          source_labels = ["__meta_kubernetes_pod_container_name"]
          target_label  = "container"
        }

        rule {
          source_labels = ["__meta_kubernetes_pod_node_name"]
          target_label  = "node"
        }
      }

      loki.source.kubernetes "pods" {
        targets    = discovery.relabel.pod_logs.output
        forward_to = [loki.write.default.receiver]
      }

      loki.write "default" {
        endpoint {
          url = "http://loki.loki.svc.cluster.local:3100/loki/api/v1/push"
        }
      }

serviceMonitor:
  enabled: false

nodeSelector:
  kubernetes.io/hostname: vm5

tolerations:
  - key: role
    operator: Equal
    value: apps
    effect: NoSchedule
```
```
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm repo add grafana https://grafana.github.io/helm-charts
sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm repo update

sudo KUBECONFIG=/etc/rancher/k3s/k3s.yaml helm upgrade --install alloy grafana/alloy \
  -n alloy \
  --create-namespace \
  -f ~/main_d/alloy-values.yaml \
  --timeout 15m
```

```
python3 -m venv ~/main_d/venvs/kafka-env
source ~/main_d/venvs/kafka-env/bin/activate
pip install --upgrade pip
pip install confluent-kafka requests
```
CODE -->

```
source ~/main_d/venvs/kafka-env/bin/activate

export KAFKA_BOOTSTRAP_SERVERS=10.130.0.27:30092
export KAFKA_TOPIC="raw-events"

python3 ~/main_d/consumers/api_to_kafka.py
```


```
from confluent_kafka import Consumer
import json

class KafkaRawConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        self.topic = topic
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

    def subscribe(self):
        def on_assign(consumer, partitions):
            print(f"partition_assigned: {partitions}")

        def on_revoke(consumer, partitions):
            print(f"partition_revoked: {partitions}")

        self.consumer.subscribe([self.topic], on_assign=on_assign, on_revoke=on_revoke)

    def poll_batch(self, batch_size: int = 100, timeout: float = 1.0):
        rows = []
        while len(rows) < batch_size:
            msg = self.consumer.poll(timeout)
            if msg is None:
                if rows:
                    break
                else:
                    continue
            if msg.error():
                print(f"poll error: {msg.error()}")
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"json decode error: {e}")
                continue
            rows.append((msg, payload))
        return rows

    def commit(self):
        self.consumer.commit(asynchronous=False)

    def close(self):
        self.consumer.close()
```
```
export KAFKA_BOOTSTRAP_SERVERS=10.130.0.27:30092
export POLL_SECONDS=30
python3 ~/main_d/consumers/api_to_kafka.py

export KAFKA_GROUP_ID=raw-consumer-v12
export KAFKA_BOOTSTRAP_SERVERS=10.130.0.27:30092
export YT_PROXY=http://localhost:31103
export YT_USE_HOSTS=0
python3 ~/main_d/consumers/kafka_to_raw.py
```
