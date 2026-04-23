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
