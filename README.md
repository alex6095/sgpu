# SGPU

**SGVR GPU** / **S**imple **GPU** monitor for the lab's MLXP H200 nodes —
check the GPUs before you launch a pod.

- Every GPU process is attributed to its **pod and owner** (not just a PID).
- **In-pod TUI** (`kubectl exec -it`) — smooth like nvitop, scroll/sort/filter.
- **Usage stats 24/7**: per-owner GPU-hours, awards, GitHub-style activity
  calendar, idle-allocation warnings (KST).
- Shared **storage (pv-01/pv-02) usage** at a glance.
- Monitor pod is read-only, always-on, and requests **no GPU**.

## Install

```bash
uvx sgpu            # run without installing (uv)
pipx install sgpu   # or pipx
pip install sgpu    # or plain pip (WSL/Ubuntu: add --user --break-system-packages)
```

Needs `kubectl` configured for the MLXP namespace
([kubectl setup](#kubectl-setup-linuxwsl)).

## Use

```text
sgpu               interactive TUI      sgpu stats [days]  usage report + awards
sgpu once          one-shot dashboard   sgpu apps          processes + owners
sgpu watch [sec]   dumb-terminal loop   sgpu nvitop        raw nvitop
sgpu pods|smi|gpustat|json|health|version|--help
```

TUI keys: `j/k` scroll · `Tab` pane · `s` sort · `o` owner filter · `p` pause · `q` quit.
Options: `-n` namespace, `--pod`, `-r` refresh, `--no-color`. Env: `SGPU_NAMESPACE`, `SGPU_POD`.

### Zero-install (kubectl only)

```bash
kubectl exec -it -n p-sgvr-node-02 sangmin-gpu-monitor -- python3 /opt/gpu-monitor/tui.py
kubectl exec -n p-sgvr-node-02 sangmin-gpu-monitor -- curl -fsS http://127.0.0.1:8080/table
```

Endpoints on `:8080`: `/table /apps /json /stats /pods /smi /topo /gpustat
/health /version /stats/files /stats/raw?date=` (`?color=1&cols=N&ascii=1`).

## Stats

Sampled every 15 s around the clock into raw JSONL (full fidelity — future
tools can recompute anything), gzipped + rolled up daily, stored on the
shared volume at `pv-01/sangmin/sgpu`. Retention 365 d, capped at 2 GB.
`sgpu stats 30` shows the leaderboard, awards, daily activity calendar and
KST hour heatmap. Raw export: `/stats/raw?date=YYYYMMDD`.

> The monitor pod must stay running for stats to accumulate — it is designed
> to (tini init, `restartPolicy: Always`, no GPU held).

## Deploy / operate

Image push works **from anywhere** via the registry's public endpoint
(`sgvr-registry.kr.ncr.ntruss.com`); the cluster pulls via the private
endpoint (`vnxb4cz3.kr.private-ncr.ntruss.com`, in-cluster only — preferred
per the MLXP guide). API key: NCP console → Access Management.

```bash
docker login sgvr-registry.kr.ncr.ntruss.com
docker build -f docker/Dockerfile.gpu-monitor -t sgvr-registry.kr.ncr.ntruss.com/sangmin/gpu-monitor:TAG .
docker push sgvr-registry.kr.ncr.ntruss.com/sangmin/gpu-monitor:TAG

kubectl delete pod sangmin-gpu-monitor -n p-sgvr-node-02 --ignore-not-found
kubectl apply -f k8s/gpu-monitor.yaml   # pods are immutable: delete + apply
kubectl wait --for=condition=Ready pod/sangmin-gpu-monitor -n p-sgvr-node-02 --timeout=180s
```

Optional (enables the pod-allocation view + idle stats; kubelet syncs it in
within a minute, no restart):

```bash
kubectl -n p-sgvr-node-02 create secret generic sgpu-kubeconfig --from-file=config=$HOME/.kube/config
```

> Anyone with exec access to the monitor pod can read that token — fine
> inside a trusting lab namespace; use your least-privileged kubeconfig.

## kubectl setup (Linux/WSL)

```bash
mkdir -p ~/.local/bin ~/.kube
V=$(curl -fsSL https://dl.k8s.io/release/stable.txt)
curl -fsSL -o ~/.local/bin/kubectl "https://dl.k8s.io/release/${V}/bin/linux/amd64/kubectl" && chmod +x ~/.local/bin/kubectl
cp /path/to/sgvr-node-02-kubeconfig.yaml ~/.kube/config && chmod 600 ~/.kube/config
kubectl get pods -n p-sgvr-node-02   # connectivity test
```

## Development

```bash
SGPU_MOCK=1 python3 tools/gpu-monitor/server.py   # full pipeline, no GPU needed
SGPU_MOCK=1 python3 tools/gpu-monitor/tui.py
python3 -m unittest discover -s tests
```

How it works: `sgpu` (thin Python client) → `kubectl exec` → monitor pod
(privileged, hostPID) where server.py renders everything; process→pod
attribution reads `/proc/<pid>/environ` (`HOSTNAME` = pod name), owner =
pod-name prefix. Known limits: pods overriding `spec.hostname` and MPS show
as `?`.

Troubleshooting: broken terminal after a dropped TUI → `reset` · frozen TUI
→ rerun `sgpu` · garbled bars → Windows Terminal or `--no-color`.
