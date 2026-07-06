# SGPU

**SGVR GPU** / **S**imple **GPU** monitor — a zero-fuss way to check the
MLXP H200 GPU nodes before you decide whether to launch a pod.

- **Zero-install**: anyone with `kubectl` gets the full dashboard and TUI.
- **Who is using what**: every GPU process is attributed to its Kubernetes
  pod and owner (`yoonki-ume-...` → `yoonki`), including `nvitop`-style bars.
- **Usage accounting**: per-owner GPU-hours, utilization, memory efficiency,
  idle-allocation warnings and a KST time-of-day heatmap, recorded 24/7.
- The monitor pod is read-only and requests **no GPU** (CPU/memory only).

## Zero-install usage (kubectl only)

No install needed — these work for every lab member as-is:

```bash
# Interactive TUI (scroll, sort, owner filter — like nvitop, plus pod owners)
kubectl exec -it -n p-sgvr-node-02 sangmin-gpu-monitor -- python3 /opt/gpu-monitor/tui.py

# One-shot dashboard
kubectl exec -n p-sgvr-node-02 sangmin-gpu-monitor -- curl -fsS http://127.0.0.1:8080/table

# Raw nvitop / nvidia-smi
kubectl exec -it -n p-sgvr-node-02 sangmin-gpu-monitor -- nvitop
kubectl exec -n p-sgvr-node-02 sangmin-gpu-monitor -- nvidia-smi

# Usage report (last 7 days)
kubectl exec -n p-sgvr-node-02 sangmin-gpu-monitor -- curl -fsS "http://127.0.0.1:8080/stats?days=7"
```

The `sgpu` command below is just a short wrapper around these.

## Install the `sgpu` command

Requirements: `kubectl` configured for the MLXP namespace (see
[Linux / WSL kubectl setup](#linux--wsl-kubectl-setup) if starting fresh).

**Windows** (PowerShell, from this repo):

```powershell
.\scripts\install-sgpu.ps1     # creates %USERPROFILE%\bin\sgpu.cmd
```

**Linux / WSL / macOS**:

```bash
./scripts/install-sgpu.sh      # symlinks to ~/.local/bin/sgpu
```

## Commands

```text
sgpu               interactive TUI (default)
sgpu once          one-shot dashboard
sgpu watch [sec]   simple refresh loop (dumb terminals / CI)
sgpu apps          GPU process table with pod owners
sgpu stats [days]  per-owner usage report (default 7 days)
sgpu nvitop        raw nvitop TUI
sgpu pods          GPU-requesting pods (JSON)
sgpu smi           raw nvidia-smi
sgpu gpustat       gpustat output
sgpu json          full snapshot (JSON, schema 2)
sgpu health | version | help
```

Options (both platforms): namespace `-n/-Namespace`, pod `--pod/-Pod`,
refresh `-r/-Refresh`, `--no-color/-NoColor`. Env vars: `SGPU_NAMESPACE`,
`SGPU_POD`.

### TUI keys

```text
j/k or ↑/↓   scroll        Tab   switch pane (processes / pods)
PgUp/PgDn    page          s     cycle sort (gpu / mem / owner)
g/G          top/bottom    o     cycle owner filter
p            pause         r     force refresh
q            quit
```

The refresh loop runs **inside the pod**, so the TUI is smooth from any
client — there is no per-frame kubectl round trip.

## Usage statistics

The monitor samples every GPU, process (with pod/owner attribution) and
GPU-requesting pod every 15 seconds, around the clock:

- **Raw samples**: `samples-YYYYMMDD.jsonl` (UTC days) — full fidelity
  (util, SM% per process, memory, power, temperature, pod allocation), so
  future analysis tools can recompute any statistic from raw data.
  Yesterday's file is gzipped and summarized into `rollup-YYYYMMDD.json`
  on rollover. Retention 365 days, total size capped at 2 GB.
- **Report** (`sgpu stats [days]`): per-owner leaderboard (GPU-hours,
  average SM%/util, peak memory, allocated-vs-idle hours), low-utilization
  warnings, pods that hold GPUs idle right now, and a KST hour-of-day
  activity heatmap.
- **Raw export**: `/stats/raw?date=YYYYMMDD` (NDJSON) and `/stats/files`.

Storage is a node-local PVC (`sgpu-data`, storage class `local-path`,
mounted at `/var/lib/sgpu`), so history survives pod restarts. (`hostPath`
is denied by the cluster's kyverno policy.)

## HTTP endpoints (inside the pod, `:8080`)

```text
/table   dashboard (plain by default; ?color=1, ?cols=N, ?ascii=1)
/apps    process table          /pods    pods JSON
/json    snapshot (schema 2)    /stats   usage report (?days=N&format=json)
/smi /topo /gpustat /health /version /stats/files /stats/raw?date=
```

## Deploy / operate the monitor pod

The NCR registry (`vnxb4cz3.kr.private-ncr.ntruss.com`) resolves to a
**private IP** — it is reachable only from inside the cluster network, so
build and push from a dind pod, not from your laptop:

```bash
IMG=vnxb4cz3.kr.private-ncr.ntruss.com/sangmin/gpu-monitor:nvml-580.126.16-v5
DIND="kubectl exec -n p-sgvr-node-02 sangmin-ulr-v2-dind-dev -c docker-cli --"

# 1. Copy the (tiny) build context into the dind pod and build
tar cf - docker tools .dockerignore | kubectl exec -i -n p-sgvr-node-02 \
  sangmin-ulr-v2-dind-dev -c docker-cli -- \
  sh -c 'rm -rf /tmp/sgpu-build && mkdir -p /tmp/sgpu-build && tar xf - -C /tmp/sgpu-build'
$DIND sh -c "cd /tmp/sgpu-build && docker build -f docker/Dockerfile.gpu-monitor -t $IMG ."

# 2. Login (interactive), push, then REMOVE the stored credential
kubectl exec -it -n p-sgvr-node-02 sangmin-ulr-v2-dind-dev -c docker-cli -- \
  docker login vnxb4cz3.kr.private-ncr.ntruss.com
$DIND docker push "$IMG"
$DIND docker logout vnxb4cz3.kr.private-ncr.ntruss.com

# 3. Pods are immutable — recreate to roll out
kubectl delete pod sangmin-gpu-monitor -n p-sgvr-node-02 --ignore-not-found
kubectl apply -f k8s/gpu-monitor.yaml
kubectl wait --for=condition=Ready pod/sangmin-gpu-monitor -n p-sgvr-node-02 --timeout=180s
```

### Enable the pod-allocation view (recommended, one command)

The pod's own service account cannot list pods (403), so out of the box the
dashboard attributes only *running processes*. To also see **allocated
pods** (including "holds GPUs but runs nothing"), give the server a
kubeconfig:

```bash
kubectl -n p-sgvr-node-02 create secret generic sgpu-kubeconfig \
  --from-file=config=$HOME/.kube/config
```

No pod restart needed — kubelet syncs the optional secret within a minute.

> **Security note**: anyone with `exec` access to the monitor pod can read
> that token. This is acceptable inside a trusting lab namespace; use the
> least-privileged kubeconfig you have.

## How it works

```text
┌─ your machine ──────────┐   ┌─ monitor pod (privileged, hostPID, no GPU req) ─┐
│ sgpu  (thin wrapper)    │   │ server.py: NVML snapshots + /proc attribution   │
│  └─ kubectl exec ──────────▶│   ├─ /table /json /stats ... (renders ANSI)     │
│  └─ kubectl exec -it ──────▶│ tui.py (curses, reads local /json)              │
└─────────────────────────┘   │ statsdb: 15s JSONL samples → rollups (hostPath) │
                              └──────────────────────────────────────────────────┘
```

Process → pod attribution reads `/proc/<pid>/environ` (`HOSTNAME` is the pod
name; hostPID makes GPU PIDs visible), with a cgroup-UID fallback. The owner
is the pod-name prefix before the first `-`/`_`. Known limits: pods that
override `spec.hostname`, and MPS-shared contexts, may attribute to `?` or
to the MPS server pod.

## Linux / WSL kubectl setup

If the machine does not have `kubectl` yet (fresh WSL Ubuntu, for example):

```bash
mkdir -p ~/.local/bin
V=$(curl -fsSL https://dl.k8s.io/release/stable.txt)
curl -fsSL -o ~/.local/bin/kubectl "https://dl.k8s.io/release/${V}/bin/linux/amd64/kubectl"
chmod +x ~/.local/bin/kubectl
```

Install the kubeconfig (the file already contains a bearer token, so no
extra login is needed):

```bash
mkdir -p ~/.kube
cp /path/to/sgvr-node-02-kubeconfig.yaml ~/.kube/config
chmod 600 ~/.kube/config
kubectl get pods -n p-sgvr-node-02   # connectivity test
```

In WSL, the Windows Downloads path is `/mnt/c/Users/<you>/Downloads/...`.

## Development

```bash
# Everything runs without a GPU in mock mode:
SGPU_MOCK=1 python3 tools/gpu-monitor/server.py    # then curl :8080/table
SGPU_MOCK=1 python3 tools/gpu-monitor/tui.py
python3 -m unittest discover -s tests              # stats pipeline tests
```

## Troubleshooting

- **Terminal looks broken after the TUI** (dropped connection): run `reset`.
- **TUI frozen**: long `kubectl exec` sessions can be cut by load balancers —
  just rerun `sgpu`.
- **Garbled box characters on Windows**: use Windows Terminal, or add
  `?ascii=1` / use `sgpu once -NoColor`.
- **`pod allocation view disabled`** in the dashboard: create the
  `sgpu-kubeconfig` secret (see above).
