# ytsaurus_k3s_ycloud
Data LakeHouse on YTsaurus tech

**Вначале**:

1. Подгоотвить вм; 

2. Выбрать метод аутентификации (логин,ssh в работе);

3. yt клиент подготовить;

4. python env;

## Поднять k3s  (control plane)

ВМ с внешним IP через которую будет управление кластером

```bash
curl -sfL https://get.k3s.io | sh -s - \
--write-kubeconfig-mode 644 \
--disable traefik sudo systemctl status k3s --no-pager | head -10
```

### kubeconfig + проверка

```bash
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $USER:$USER ~/.kube/config
chmod 600 ~/.kube/config
echo 'export KUBECONFIG=~/.kube/config' >> ~/.bashrc
export KUBECONFIG=~/.kube/config
kubectl get nodes -o wide
```


### Подключение worker

Подключение через ВМ (contol plane)   `ssh -i ~/.ssh/key login@IP(внутрениий IP ВМ для воркера)

Токен для воркеров - `sudo cat /var/lib/rancher/k3s/server/node-token`

```bash
export K3S_URL="https://IP(contol plane):6443"
export K3S_TOKEN="token" 
curl -sfL https://get.k3s.io | K3S_URL="$K3S_URL" K3S_TOKEN="$K3S_TOKEN" sh -
sudo systemctl status k3s-agent --no-pager | head -20
```

### Теги

```bash
kubectl label node vm2-k3s-ing-server yt-role=heavy --overwrite 
kubectl label node vm3-k3s-ing-server yt-role=heavy --overwrite 
kubectl label node vm4-k3s-ing-server yt-role=storage --overwrite 
kubectl label node vm5-k3s-ing-server yt-role=kafka/airflow/monitor –overwrite

kubectl get nodes --show-labels
```

### DNS (чтобы воркеры, их поды, могли идти в «мир» через 1ВМ контролку)

```bash
sudo mkdir -p /etc/systemd/resolved.conf.d

sudo tee /etc/systemd/resolved.conf.d/cluster-local.conf >/dev/null <<'EOF'
[Resolve]
DNS=10.43.0.10
Domains=~cluster.local
EOF

sudo systemctl restart systemd-resolved

getent hosts baltocdn.com
getent hosts hp-0.http-proxies.default.svc.cluster.local
```
```python
for node in vm2-k3s-ing-server vm3-k3s-ing-server vm4-k3s-ing-server vm5-k3s-ing-server; do
  echo "=== $node ==="
  kubectl run "dnsfix-$(echo $node | cut -d- -f1)" \
    --image=ubuntu \
    --restart=Never \
    --rm -i --tty=false \
    --overrides="{
      \"spec\": {
        \"nodeName\": \"$node\",
        \"hostPID\": true,
        \"hostNetwork\": true,
        \"containers\": [{
          \"name\": \"dnsfix\",
          \"image\": \"ubuntu\",
          \"command\": [\"/bin/sh\",\"-c\",\"nsenter -t 1 -m -u -i -n -- /bin/sh -c 'mkdir -p /etc/systemd/resolved.conf.d && cat > /etc/systemd/resolved.conf.d/cluster-local.conf <<EOF\n[Resolve]\nDNS=10.43.0.10\nDomains=~cluster.local\nEOF\nsystemctl restart systemd-resolved && getent hosts hp-0.http-proxies.default.svc.cluster.local && echo DONE'\"],
          \"securityContext\": {\"privileged\": true}
        }]
      }
    }" \
    -n default 2>&1 | tail -5
  echo ""
done
```

### Установка helm

```bash
curl -fsSL -o /tmp/get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 /tmp/get_helm.sh
sudo /tmp/get_helm.sh

helm version --short
```

## YTsaurus

#### cert-manager 

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml
kubectl get crd | grep cert-manager
```
#### venv

```
python3 -m venv /home/chig_k3s/yt-env
source /home/chig_k3s/yt-env/bin/activate
pip install --upgrade pip
pip install ytsaurus-client ytsaurus-yson
yt --version
```
### helm YTsaurus

```bash
helm install ytsaurus oci://ghcr.io/ytsaurus/ytop-chart \
  --version 0.31.0 \
  --namespace ytsaurus-operator \
  --create-namespace

kubectl get pods -n ytsaurus-operator
```

### manifest для Custom Resource

```yaml
cat > ~/minisaurus-cr.yaml <<'EOF'
apiVersion: cluster.ytsaurus.tech/v1
kind: Ytsaurus
metadata:
  name: minisaurus
  namespace: default
spec:
  coreImage: ghcr.io/ytsaurus/ytsaurus:stable-25.2.1-relwithdebinfo
  uiImage: ghcr.io/ytsaurus/ui:stable
  adminCredentials:
    name: ytadminsec

  isManaged: true
  enableFullUpdate: false
  useIPv6: false
  useIPv4: true

  discovery:
    instanceCount: 1
    resources:
      requests: { cpu: 100m, memory: 256Mi }
      limits:   { cpu: 1,    memory: 1Gi }

  primaryMasters:
    instanceCount: 1
    cellTag: 1
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - key: yt-role
              operator: In
              values: [heavy]
    resources:
      requests: { cpu: 500m, memory: 2Gi }
      limits:   { cpu: 2,    memory: 4Gi }
    locations:
    - locationType: MasterChangelogs
      path: /yt/master-data/master-changelogs
    - locationType: MasterSnapshots
      path: /yt/master-data/master-snapshots
    volumeClaimTemplates:
    - metadata: { name: master-data }
      spec:
        accessModes: [ReadWriteOnce]
        resources: { requests: { storage: 10Gi } }
    volumeMounts:
    - name: master-data
      mountPath: /yt/master-data

  httpProxies:
  - role: default
    instanceCount: 1
    serviceType: NodePort
    httpNodePort: 31103
    resources:
      requests: { cpu: 200m, memory: 512Mi }
      limits:   { cpu: 2,    memory: 2Gi }
  - role: control
    instanceCount: 1
    serviceType: NodePort
    httpNodePort: 31104
    resources:
      requests: { cpu: 200m, memory: 512Mi }
      limits:   { cpu: 1,    memory: 1Gi }

  rpcProxies:
  - role: default
    instanceCount: 1
    serviceType: NodePort
    nodePort: 31105
    resources:
      requests: { cpu: 200m, memory: 512Mi }
      limits:   { cpu: 2,    memory: 2Gi }
  - role: heavy
    instanceCount: 1
    serviceType: NodePort
    nodePort: 31106
    resources:
      requests: { cpu: 200m, memory: 512Mi }
      limits:   { cpu: 2,    memory: 2Gi }

  dataNodes:
  - instanceCount: 3
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - key: yt-role
              operator: In
              values: [heavy, storage]
      podAntiAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchExpressions:
            - key: app.kubernetes.io/component
              operator: In
              values: [data-node]
          topologyKey: kubernetes.io/hostname
    resources:
      requests: { cpu: 500m, memory: 2Gi }
      limits:   { cpu: 4,    memory: 8Gi }
    locations:
    - locationType: ChunkStore
      path: /yt/node-data/chunk-store
      lowWatermark: 1073741824
    volumeClaimTemplates:
    - metadata: { name: node-data }
      spec:
        accessModes: [ReadWriteOnce]
        resources: { requests: { storage: 70Gi } }
    volumeMounts:
    - name: node-data
      mountPath: /yt/node-data

  execNodes:
  - instanceCount: 2
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - key: yt-role
              operator: In
              values: [heavy]
      podAntiAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchExpressions:
            - key: app.kubernetes.io/component
              operator: In
              values: [exec-node]
          topologyKey: kubernetes.io/hostname
    resources:
      requests: { cpu: 1, memory: 4Gi }
      limits:   { cpu: 8, memory: 16Gi }
    locations:
    - locationType: ChunkCache
      path: /yt/node-data/chunk-cache
    - locationType: Slots
      path: /yt/node-data/slots
    volumeClaimTemplates:
    - metadata: { name: node-data }
      spec:
        accessModes: [ReadWriteOnce]
        resources: { requests: { storage: 20Gi } }
    volumeMounts:
    - name: node-data
      mountPath: /yt/node-data

  tabletNodes:
  - instanceCount: 2
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - key: yt-role
              operator: In
              values: [heavy]
      podAntiAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchExpressions:
            - key: app.kubernetes.io/component
              operator: In
              values: [tablet-node]
          topologyKey: kubernetes.io/hostname
    resources:
      requests: { cpu: 500m, memory: 4Gi }
      limits:   { cpu: 4,    memory: 16Gi }

  schedulers:
    instanceCount: 1
    resources:
      requests: { cpu: 100m, memory: 500Mi }
      limits:   { cpu: 2,    memory: 4Gi }

  controllerAgents:
    instanceCount: 1
    resources:
      requests: { cpu: 100m, memory: 500Mi }
      limits:   { cpu: 2,    memory: 4Gi }

  ui:
    image: ghcr.io/ytsaurus/ui:stable
    instanceCount: 1
    serviceType: NodePort
    resources:
      requests: { cpu: 100m, memory: 200Mi }
      limits:   { cpu: 1,    memory: 1Gi }

  queryTrackers:
    image: ghcr.io/ytsaurus/query-tracker:0.0.11
    instanceCount: 1
    resources:
      requests: { cpu: 100m, memory: 256Mi }
      limits:   { cpu: 1,    memory: 2Gi }

  yqlAgents:
    image: ghcr.io/ytsaurus/query-tracker:0.0.11
    instanceCount: 1
    resources:
      requests: { cpu: 100m, memory: 256Mi }
      limits:   { cpu: 1,    memory: 2Gi }

  queueAgents:
    instanceCount: 1
    resources:
      requests: { cpu: 100m, memory: 256Mi }
      limits:   { cpu: 1,    memory: 1Gi }

  strawberry:
    image: ghcr.io/ytsaurus/strawberry:0.0.15
    resources:
      requests: { cpu: 100m, memory: 100Mi }
      limits:   { cpu: 1,    memory: 1Gi }
EOF
```

### creds YTsaurus

```bash
kubectl create secret generic ytadminsec \
  --from-literal=login=admin \
  --from-literal=password=passworf \
  --from-literal=token=$(openssl rand -hex 16) \
  -n default

kubectl get secret ytadminsec -n default
```

#### patch

Проблемы с полями Ipv4/Ipv6

```bash
sed -i '/useIPv6:/d; /useIPv4:/d' ~/minisaurus-cr.yaml
grep -c "useIP" ~/minisaurus-cr.yaml   # --> 0 !
kubectl apply -f ~/minisaurus-cr.yaml 
```

#### token YTsaurus + ui node

```bash
TOKEN=$(kubectl get secret ytadminsec -n default -o jsonpath='{.data.token}' | base64 -d)
echo "TOKEN=$TOKEN"
echo $TOKEN > ~/.yt/token
mkdir -p ~/.yt
echo $TOKEN > ~/.yt/token
chmod 600 ~/.yt/token

# Endpoint http-proxies через NodePort
kubectl get svc -n default | grep -i ui
```
##### Тестовый коннект

```bash
export YT_PROXY=http://localhost:31103
export YT_TOKEN=$(cat ~/.yt/token)

yt list / 2>&1 | head -10 # externIP:30254 у меня здесь.
```

##### Cypress тест

```python
for d in raw_stage bronze_stage silver_stage gold_stage ops_logs ops_logs/greenhub bronze_stage/greenhub; do
  yt create map_node //home/$d --ignore-existing
done

yt list //home
yt list //home/ops_logs
```

### Создать таблицы

```bash

bash script_table.sh
yt list //home/bronze_stage/greenhub

bash create_silver_tables.sh
yt list //home/silver_stage

bash create_gold_tables.sh
yt list //home/gold_stage

bash silver_kafka.txt
yt list //home/silver_stage

```
## Kafka

```yaml
mkdir -p ~/helm-values

cat > ~/helm-values/kafka.yaml <<'EOF'
broker:
  nodeSelector:
    kubernetes.io/hostname: vm5-k3s-ing-server
  persistence:
    enabled: true
    size: 20Gi
  replicaCount: 1
controller:
  nodeSelector:
    kubernetes.io/hostname: vm5-k3s-ing-server
  persistence:
    enabled: true
    size: 8Gi
  replicaCount: 1
externalAccess:
  broker:
    service:
      externalIPs:
      - 10.130.0.27
      nodePorts:
      - 30092
      type: NodePort
  controller:
    forceExpose: false
    service:
      loadBalancerIPs:
      - 10.130.0.27
      type: LoadBalancer
  enabled: true
image:
  registry: docker.io
  repository: bitnamilegacy/kafka
  tag: 4.0.0-debian-12-r10
kraft:
  enabled: true
listeners:
  client:
    protocol: PLAINTEXT
  controller:
    protocol: PLAINTEXT
  external:
    protocol: PLAINTEXT
  interbroker:
    protocol: PLAINTEXT
zookeeper:
  enabled: false
EOF

cat > ~/helm-values/kafka-ui.yaml <<'EOF'
nodeSelector:
  kubernetes.io/hostname: vm5-k3s-ing-server
service:
  type: NodePort
yamlApplicationConfig:
  kafka:
    clusters:
    - bootstrapServers: kafka.kafka.svc.cluster.local:9092
      name: ytsaurus-lab
EOF
```

### helm kafka

```bash
helm install kafka oci://registry-1.docker.io/bitnamicharts/kafka \
  --version 32.4.3 \
  --namespace kafka \
  --create-namespace \
  -f ~/helm-values/kafka.yaml

# Смотреть pods
kubectl get pods -n kafka -w
# Ctrl+C когда controller и broker Running
```

### kafka UI

```bash
helm install kafka-ui kafbat-ui/kafka-ui \
  --namespace kafka \
  --version 1.6.4 \
  -f ~/helm-values/kafka-ui.yaml

kubectl get pods -n kafka -w
```

## Airflow

```yaml
cat > ~/helm-values/airflow.yaml <<'EOF'
# === Node placement ===
# Airflow целиком на vm5 (как было)
apiServer:
  nodeSelector:
    kubernetes.io/hostname: vm5-k3s-ing-server
  service:
    type: NodePort
createUserJob:
  defaultUser:
    email: chiglok.tech@gmail.com
    enabled: true
    firstName: Askold
    lastName: Apet
    password: Tye1lf0f3sdcXX
    role: Admin
    username: AFL938ahZKal
  nodeSelector:
    kubernetes.io/hostname: vm5-k3s-ing-server
dagProcessor:
  nodeSelector:
    kubernetes.io/hostname: vm5-k3s-ing-server
migrateDatabaseJob:
  nodeSelector:
    kubernetes.io/hostname: vm5-k3s-ing-server
postgresql:
  primary:
    nodeSelector:
      kubernetes.io/hostname: vm5-k3s-ing-server
    persistence:
      size: 5Gi   # ограничиваем чтобы не разрасся
redis:
  nodeSelector:
    kubernetes.io/hostname: vm5-k3s-ing-server
scheduler:
  nodeSelector:
    kubernetes.io/hostname: vm5-k3s-ing-server
statsd:
  nodeSelector:
    kubernetes.io/hostname: vm5-k3s-ing-server
triggerer:
  nodeSelector:
    kubernetes.io/hostname: vm5-k3s-ing-server
webserver:
  nodeSelector:
    kubernetes.io/hostname: vm5-k3s-ing-server
  service:
    type: NodePort
workers:
  nodeSelector:
    kubernetes.io/hostname: vm5-k3s-ing-server

# === DAGs from GitHub ===
dags:
  gitSync:
    branch: main
    credentialsSecret: airflow-git-credentials
    enabled: true
    period: 30s
    repo: https://github.com/chiglo-k/ytsaurus_k3s_ycloud.git
    subPath: dags
  persistence:
    enabled: false
EOF
```

### helm Airflow

Если репозиторий закрыт
----------------------------
```bash
kubectl create namespace airflow

kubectl create secret generic airflow-git-credentials \
  -n airflow \
  --from-literal=GIT_SYNC_USERNAME='GH_USER' \
  --from-literal=GIT_SYNC_PASSWORD='GH_PAT'
```
-----------------------------


```bash
helm repo add apache-airflow https://airflow.apache.org/
helm repo update

helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --version 1.20.0 \
  -f ~/helm-values/airflow.yaml

sleep 500
kubectl get pods -n airflow -w
```

## SPYT

```bash
source /home/chig_k3s/yt-env/bin/activate
pip install ytsaurus-spyt

#проверка! вне ytsaurus поэтому прям его надо отлавливать
ls /home/chig_k3s/yt-env/bin/ | grep -E "spark|spyt"
which spark-launch-yt
which spark-submit-yt
```
Отдельно JAVA/PySpark

```bash
sudo apt-get install -y openjdk-17-jdk-headless
java -version
ls -la /usr/lib/jvm/ | grep -E "java-17"
```
```bash
source /home/chig_k3s/yt-env/bin/activate
pip install "pyspark>=3.5,<4.0"

python -c "import pyspark; print(pyspark.__version__)"
```

Поднимаем SPYT release в Cypress

```
yt create map_node //home/spark --ignore-existing
yt create map_node //home/spark/discovery --ignore-existing
```

#### PATCH

Exec nodes //

```bash
kubectl patch ytsaurus minisaurus -n default --type='json' -p='[
  {"op": "replace", "path": "/spec/execNodes/0/resources/requests/cpu", "value": "24"},
  {"op": "replace", "path": "/spec/execNodes/0/resources/requests/memory", "value": "96Gi"},
  {"op": "replace", "path": "/spec/execNodes/0/resources/limits/cpu", "value": "24"},
  {"op": "replace", "path": "/spec/execNodes/0/resources/limits/memory", "value": "96Gi"}
]'
```

Запуск кластера

```bash
spark-launch-yt \
  --proxy localhost:31103 \
  --pool research \
  --discovery-path //home/spark/discovery/main \
  --worker-cores 2 \
  --worker-num 2 \
  --worker-memory 3G \
  --tmpfs-limit 1G \
  --params '{spark_conf={"spark.driver.host"="10.130.0.24"}}'
```

