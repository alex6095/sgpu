# sgpu — thin client for the in-pod sgpu monitor.
# All rendering happens server-side; this script only shells out to kubectl.
param(
    [Parameter(Position = 0)]
    [ValidateSet("top", "once", "watch", "apps", "stats", "nvitop", "pods",
                 "smi", "gpustat", "json", "health", "version", "help")]
    [string]$Command = "top",
    [Parameter(Position = 1)]
    [int]$Arg = 0,                    # days for stats, seconds for watch
    [Alias("Refresh", "Every")]
    [int]$Interval = 2,
    [string]$Namespace = $(if ($env:SGPU_NAMESPACE) { $env:SGPU_NAMESPACE } else { "p-sgvr-node-02" }),
    [string]$Pod = $(if ($env:SGPU_POD) { $env:SGPU_POD } else { "sangmin-gpu-monitor" }),
    [switch]$NoColor
)

$ErrorActionPreference = "Continue"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

function Show-Usage {
    Write-Host @"
usage: sgpu [command] [options]

commands:
  (none) | top     interactive TUI (scroll, sort, owner filter)
  once             one-shot dashboard
  watch [sec]      simple refresh loop (for dumb terminals)
  apps             GPU process table with pod owners
  stats [days]     per-owner usage report (default 7 days)
  nvitop           raw nvitop TUI
  pods | smi | gpustat | json | health | version

options:
  -Namespace NS    kubernetes namespace   (default p-sgvr-node-02)
  -Pod NAME        monitor pod name       (default sangmin-gpu-monitor)
  -Refresh SEC     TUI/watch refresh interval (default 2)
  -NoColor         disable ANSI colors
"@
}

function Show-FailureHint([string]$Output) {
    Write-Host "sgpu: cannot reach monitor pod '$Pod' in namespace '$Namespace'"
    $tail = ($Output -split "`n" | Select-Object -Last 3) -join "`n"
    Write-Host $tail
    if ($Output -match "(?i)not ?found") {
        Write-Host "hint: the monitor pod is not running. Deploy it with:"
        Write-Host "  kubectl apply -f https://raw.githubusercontent.com/alex6095/sgpu/main/k8s/gpu-monitor.yaml"
    } elseif ($Output -match "(?i)unauthorized|forbidden|credentials") {
        Write-Host "hint: check your access: kubectl auth can-i get pods -n $Namespace"
    }
}

function Get-FromPod([string]$Path) {
    $output = & kubectl exec -n $Namespace $Pod -- curl -fsS "http://127.0.0.1:8080$Path" 2>&1
    $text = ($output | ForEach-Object { $_.ToString() }) -join "`n"
    if ($LASTEXITCODE -ne 0) {
        Show-FailureHint $text
        return $null
    }
    return $text
}

function Get-ColorParam {
    if ($NoColor -or [Console]::IsOutputRedirected) { return "color=0" }
    return "color=1"
}

function Get-ColsParam {
    try {
        $width = [Console]::WindowWidth
        if ($width -lt 40) { $width = 120 }
    } catch { $width = 120 }
    return "cols=$width"
}

function Invoke-Interactive([string[]]$PodCommand) {
    if ([Console]::IsOutputRedirected -or [Console]::IsInputRedirected) {
        # No TTY to hand over — degrade to a one-shot dashboard.
        $text = Get-FromPod "/table?color=0&$(Get-ColsParam)"
        if ($null -ne $text) { Write-Output $text }
        return
    }
    & kubectl exec -it -n $Namespace $Pod -- @PodCommand
}

function Invoke-WatchLoop {
    $esc = [char]27
    $seconds = if ($Arg -gt 0) { $Arg } else { $Interval }
    [Console]::Write("${esc}[?1049h${esc}[?25l")
    try {
        while ($true) {
            $frame = Get-FromPod "/table?$(Get-ColorParam)&$(Get-ColsParam)"
            if ($null -eq $frame) { $frame = "sgpu: fetch failed; retrying..." }
            [Console]::Write("${esc}[H" + $frame + "`n${esc}[0J")
            Start-Sleep -Seconds $seconds
        }
    } finally {
        [Console]::Write("${esc}[?25h${esc}[?1049l")
    }
}

function Write-PodText([string]$Path) {
    $text = Get-FromPod $Path
    if ($null -ne $text) { Write-Output $text }
}

switch ($Command) {
    "help"    { Show-Usage }
    "top"     { Invoke-Interactive @("python3", "/opt/gpu-monitor/tui.py", "$Interval") }
    "nvitop"  { Invoke-Interactive @("nvitop") }
    "once"    { Write-PodText "/table?$(Get-ColorParam)&$(Get-ColsParam)" }
    "watch"   { Invoke-WatchLoop }
    "apps"    { Write-PodText "/apps?$(Get-ColorParam)&$(Get-ColsParam)" }
    "stats"   { $days = if ($Arg -gt 0) { $Arg } else { 7 }
                Write-PodText "/stats?days=$days&$(Get-ColorParam)&$(Get-ColsParam)" }
    "pods"    { Write-PodText "/pods" }
    "smi"     { Write-PodText "/smi" }
    "gpustat" { Write-PodText "/gpustat" }
    "json"    { Write-PodText "/json" }
    "health"  { Write-PodText "/health" }
    "version" { Write-PodText "/version" }
}
