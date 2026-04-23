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
```

