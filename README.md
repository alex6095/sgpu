# SGPU

**SGVR GPU** / **Simple GPU** monitor for the lab's MLXP H200 nodes.
Check GPU ownership, utilization, storage, and usage history before launching
another Kubernetes pod.

<p align="center">
  <img src="docs/images/sgpu-hero.svg" alt="SGPU live dashboard" width="100%">
</p>

- Every GPU process is attributed to its **pod and owner**, not just a PID.
- **In-pod TUI** via `kubectl exec -it`: smooth refresh, scrolling, sorting,
  owner filtering, and a stats screen.
- **Usage stats 24/7**: per-owner GPU-hours, awards, KST activity heatmaps,
  and idle-allocation warnings.
- Shared **storage (pv-01/pv-02) usage** at a glance.
- Monitor pod is read-only, always-on, and requests **no GPU**.

## Install

```bash
uvx sgpu            # run without installing (uv)
pipx install sgpu   # or pipx
pip install sgpu    # or plain pip (WSL/Ubuntu: add --user --break-system-packages)
```

Needs `kubectl` with an MLXP kubeconfig ([setup](#kubectl-setup-linuxwsl)).
One kubeconfig covers **both** H200 servers — the two downloads
(`sgvr-node-01`/`-02`) share the same token and contexts, so either file
works for every node.

## Use

```text
sgpu               interactive TUI      sgpu stats [days]  usage report + awards
sgpu once          one-shot dashboard   sgpu apps          processes + owners
sgpu watch [sec]   dumb-terminal loop   sgpu nvitop        raw nvitop
sgpu pods|smi|gpustat|json|health|version|--help
```

### Pick a node

MLXP has two H200 servers (`p-sgvr-node-01`, `p-sgvr-node-02`):

```text
sgpu -n 1 once        node-01   (shorthand for p-sgvr-node-01)
sgpu -n 2 once        node-02
sgpu --all once       survey both nodes at once (any text command)
sgpu once             uses your current kubectl context's namespace
```

TUI keys:

```text
j/k       scroll
Tab       switch pane
s         sort
o         owner filter
p         pause
t         stats screen
h/d/w/m   stats axis: hour/day/week/month
a         cycle stats axis
r         refresh
q         quit
```

Options: `-n` namespace, `--pod`, `-r` refresh, `--no-color`.
Env: `SGPU_NAMESPACE`, `SGPU_POD`.

## Screenshots

### Process Attribution

<p align="center">
  <img src="docs/images/sgpu-processes.svg" alt="SGPU process attribution" width="100%">
</p>

## Zero Install

Anyone with `kubectl` access can use the monitor pod without installing
`sgpu`.

```bash
kubectl exec -it -n p-sgvr-node-01 sangmin-gpu-monitor -- python3 /opt/gpu-monitor/tui.py
kubectl exec -n p-sgvr-node-01 sangmin-gpu-monitor -- curl -fsS http://127.0.0.1:8080/table
```

Endpoints on `:8080`:

```text
/table /apps /json /stats /pods /smi /topo /gpustat /health /version
/stats/files /stats/raw?date=YYYYMMDD
```

Text endpoints support `?color=1&cols=N&ascii=1`.

## Stats

SGPU samples every 15 seconds around the clock into raw JSONL, gzips and rolls
up daily summaries, and stores the results on the shared volume at
`pv-01/sangmin/sgpu`.

Retention defaults to 365 days and is capped at 2 GB. `sgpu stats 30` shows
leaderboards, awards, daily activity, and KST hour heatmaps.

<p align="center">
  <img src="docs/images/sgpu-stats.svg" alt="SGPU stats report" width="100%">
</p>

> The monitor pod must stay running for stats to accumulate. It is designed to
> do that with tini init, `restartPolicy: Always`, and no GPU allocation.

## Deploy / Operate

The monitor runs from a **public image** (`docker.io/alex6095/sgpu-monitor`),
so no registry login or pull secret is needed. Deploy one pod per node —
always pass `-n` (a bare `kubectl apply` would hit your current context's
namespace):

```bash
# For each node namespace (p-sgvr-node-01 and/or p-sgvr-node-02):
kubectl apply -n p-sgvr-node-01 -f k8s/gpu-monitor.yaml
kubectl wait --for=condition=Ready pod/sangmin-gpu-monitor -n p-sgvr-node-01 --timeout=180s
```

Pods are immutable, so to roll out a new image: `kubectl delete pod
sangmin-gpu-monitor -n <ns>` then `apply` again.

Optional, for the pod-allocation view and idle stats (kubelet syncs it in
within a minute, no restart; use the same `-n`):

```bash
kubectl -n p-sgvr-node-01 create secret generic sgpu-kubeconfig --from-file=config=$HOME/.kube/config
```

> Anyone with exec access to the monitor pod can read that token. This is fine
> inside a trusting lab namespace; use a least-privileged kubeconfig.

<details>
<summary>Maintainer: build & publish the image</summary>

```bash
docker build -f docker/Dockerfile.gpu-monitor -t docker.io/alex6095/sgpu-monitor:X.Y.Z .
docker push docker.io/alex6095/sgpu-monitor:X.Y.Z   # keep the repo public
```

Bump the tag on every change — never repush a tag (`imagePullPolicy:
IfNotPresent` would keep a node's cached layer). The NVIDIA driver
(580.126.16) is pinned in the image; if a node runs a different driver the
server degrades to `source=nvidia-smi` or `/health` 503 instead of crashing.
</details>

## kubectl Setup (Linux/WSL)

```bash
mkdir -p ~/.local/bin ~/.kube
V=$(curl -fsSL https://dl.k8s.io/release/stable.txt)
curl -fsSL -o ~/.local/bin/kubectl "https://dl.k8s.io/release/${V}/bin/linux/amd64/kubectl" && chmod +x ~/.local/bin/kubectl
cp /path/to/sgvr-node-01-kubeconfig.yaml ~/.kube/config && chmod 600 ~/.kube/config
# Either node's kubeconfig works for both — pick the node with `sgpu -n 1|2`.
kubectl get pods -n p-sgvr-node-02   # connectivity test
```

## Development

```bash
SGPU_MOCK=1 python3 tools/gpu-monitor/server.py   # full pipeline, no GPU needed
SGPU_MOCK=1 python3 tools/gpu-monitor/tui.py
python3 -m unittest discover -s tests
python3 tools/render_readme_images.py
```

How it works: `sgpu` is a thin Python client. It uses `kubectl exec` to reach
the monitor pod, where `server.py` renders the dashboard. Process-to-pod
attribution reads `/proc/<pid>/environ` (`HOSTNAME` = pod name), and owner is
inferred from the pod-name prefix.

Known limits: pods overriding `spec.hostname` and MPS may show as `?`.

Troubleshooting:

```text
broken terminal after dropped TUI -> reset
frozen TUI                         -> rerun sgpu
garbled bars                       -> Windows Terminal or --no-color
```
