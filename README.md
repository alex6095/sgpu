# sgpu

`sgpu` is a small local command for checking MLXP H200 GPU status before you
decide whether to launch a GPU pod.

It hides the noisy parts:

- uses `kubectl exec` by default, so no local port-forward is needed
- can use an optional `kubectl port-forward` tunnel when you want lower-latency HTTP access
- shows a live terminal dashboard
- combines `nvidia-smi` process rows with Kubernetes pod owner/age/GPU request
- does not require a GPU allocation for the monitor pod

The monitor pod is still read-only. It requests CPU/memory only.

## Requirements

Local machine:

- `kubectl` configured for the MLXP namespace
- access to namespace `p-sgvr-node-02`
- Windows PowerShell 5+ or Linux/macOS Bash
- Linux/macOS also needs `curl` and `python3`

Cluster side:

- `sangmin-gpu-monitor` pod and service deployed from `k8s/gpu-monitor.yaml`
- image `vnxb4cz3.kr.private-ncr.ntruss.com/sangmin/gpu-monitor:nvml-580.126.16-v4`

## Windows Install

From this folder:

```powershell
.\scripts\install-sgpu.ps1
```

Open a new terminal, then:

```powershell
sgpu once
sgpu
```

The installer creates:

```text
%USERPROFILE%\bin\sgpu.cmd
```

and adds `%USERPROFILE%\bin` to the user PATH if needed.

## Linux / WSL kubectl setup

If the machine does not have `kubectl` yet (fresh WSL Ubuntu, for example):

```bash
mkdir -p ~/.local/bin
V=$(curl -fsSL https://dl.k8s.io/release/stable.txt)
curl -fsSL -o ~/.local/bin/kubectl "https://dl.k8s.io/release/${V}/bin/linux/amd64/kubectl"
chmod +x ~/.local/bin/kubectl
# ~/.local/bin is on PATH by default in Ubuntu login shells.
```

Install the kubeconfig (the file already contains a bearer token, so no extra
login is needed):

```bash
mkdir -p ~/.kube
cp /path/to/sgvr-node-02-kubeconfig.yaml ~/.kube/config
chmod 600 ~/.kube/config
kubectl get pods -n p-sgvr-node-02   # connectivity test
```

In WSL, the Windows Downloads path is `/mnt/c/Users/<you>/Downloads/...`.

## Linux Install

From this folder:

```bash
chmod +x ./scripts/install-sgpu.sh ./bin/sgpu
./scripts/install-sgpu.sh
```

If `~/.local/bin` is not on PATH:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

Then:

```bash
sgpu once
sgpu
```

## Commands

```text
sgpu          live dashboard
sgpu once     one-shot dashboard
sgpu smi      raw nvidia-smi
sgpu apps     process and pod owner view
sgpu gpustat  gpustat text output
sgpu url      local URLs
sgpu status   local tunnel status
sgpu stop     stop local kubectl port-forward
```

Refresh interval:

```powershell
sgpu -Refresh 1
sgpu top 1
sgpu once -Refresh 5
```

Linux:

```bash
sgpu -r 1
sgpu top 1
SGPU_INTERVAL=5 sgpu once
```

Transport:

```powershell
# Default: no local tunnel.
sgpu
sgpu once

# Optional faster local HTTP tunnel.
sgpu -UsePortForward
sgpu once -UsePortForward
sgpu stop
```

Linux:

```bash
# Default: no local tunnel.
sgpu
sgpu once

# Optional faster local HTTP tunnel.
sgpu --port-forward
SGPU_TRANSPORT=port-forward sgpu once
sgpu stop
```

Local URLs are available only when port-forward transport is selected:

```text
http://127.0.0.1:18080/table
http://127.0.0.1:18080/smi
http://127.0.0.1:18080/apps
```

## Smooth Rendering

The live dashboard does not call `clear` on every refresh. It uses ANSI cursor
movement, hides the cursor, writes into the terminal alternate screen, and
clears each physical line before rewriting it. Long lines are clipped to the
current terminal width to avoid automatic wrapping. It also avoids writing a
final newline at the bottom row, which prevents one-frame terminal scroll
artifacts. This avoids the heavy flicker and overlap artifacts from full-screen
clears in Windows Terminal and WSL terminals.

## Configuration

Windows options:

```powershell
sgpu once -Namespace p-sgvr-node-02 -Service sangmin-gpu-monitor -Port 18080
sgpu top -Refresh 1
sgpu top -UsePortForward -Refresh 1
```

Linux environment variables:

```bash
SGPU_NAMESPACE=p-sgvr-node-02
SGPU_SERVICE=sangmin-gpu-monitor
SGPU_PORT=18080
SGPU_INTERVAL=2
```

## LAN Sharing

Default binding is local-only:

```text
127.0.0.1:18080
```

For a trusted network only:

```powershell
sgpu top -UsePortForward -ShareOnLan
```

or on Linux:

```bash
SGPU_TRANSPORT=port-forward SGPU_SHARE_ON_LAN=1 sgpu
```

This binds the local port-forward to `0.0.0.0`. Firewall and network routing
still apply.

## Deploy Monitor Pod

If the monitor pod is missing:

```powershell
kubectl apply -f .\k8s\gpu-monitor.yaml
kubectl wait --for=condition=Ready pod/sangmin-gpu-monitor -n p-sgvr-node-02 --timeout=180s
```

Check that it does not reserve a GPU:

```powershell
kubectl get pod sangmin-gpu-monitor -n p-sgvr-node-02 -o jsonpath="{.spec.containers[0].resources.requests}"
```

Expected request does not include `nvidia.com/gpu`.

## No Install Kubectl Usage

Anyone with `kubectl` access can use the monitor pod without installing `sgpu`.

One-shot compact table:

```bash
kubectl exec -n p-sgvr-node-02 sangmin-gpu-monitor -- \
  bash -lc 'curl -fsS http://127.0.0.1:8080/table'
```

Raw `nvidia-smi`:

```bash
kubectl exec -n p-sgvr-node-02 sangmin-gpu-monitor -- nvidia-smi
```

Processes:

```bash
kubectl exec -n p-sgvr-node-02 sangmin-gpu-monitor -- \
  bash -lc 'curl -fsS http://127.0.0.1:8080/apps'
```

Linux live view without installing `sgpu`:

```bash
watch -n 2 "kubectl exec -n p-sgvr-node-02 sangmin-gpu-monitor -- bash -lc 'curl -fsS http://127.0.0.1:8080/table'"
```

Windows PowerShell live view without installing `sgpu`:

```powershell
while ($true) {
  Clear-Host
  kubectl exec -n p-sgvr-node-02 sangmin-gpu-monitor -- bash -lc 'curl -fsS http://127.0.0.1:8080/table'
  Start-Sleep -Seconds 2
}
```

The no-install commands show the monitor container view. `sgpu` adds the local
`kubectl get pods` owner/age table on top.

## Notes

- `kubectl port-forward` is not treated as a permanent daemon. `sgpu` health
  checks it and recreates it when needed, but only when port-forward transport
  is explicitly selected.
- The monitor pod uses `hostPID: true` so `nvidia-smi` can show compute PIDs.
- Kubernetes pod owner/age/GPU request rows are added by the local `sgpu`
  command using your local `kubectl` permission.
- The owner column is inferred from the pod name prefix before `-` or `_`.
