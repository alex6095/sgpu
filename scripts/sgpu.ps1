param(
    [Parameter(Position = 0)]
    [ValidateSet("top", "once", "smi", "apps", "pods", "gpustat", "url", "status", "stop", "open")]
    [string]$Command = "top",
    [Parameter(Position = 1)]
    [Alias("Refresh", "Every")]
    [int]$Interval = 2,
    [int]$Port = 18080,
    [string]$Namespace = "p-sgvr-node-02",
    [string]$Service = "sangmin-gpu-monitor",
    [string]$Pod = "sangmin-gpu-monitor",
    [switch]$UseExec,
    [switch]$UsePortForward,
    [switch]$NoColor,
    [switch]$ShareOnLan
)

$ErrorActionPreference = "Stop"

$scriptDir = $PSScriptRoot
$repoRoot = Split-Path -Parent $scriptDir
$forwardScript = Join-Path $scriptDir "gpu-monitor-local.ps1"
$baseUrl = "http://127.0.0.1:${Port}"
$transport = "exec"
if ($UsePortForward -or $env:SGPU_TRANSPORT -in @("port-forward", "pf", "tunnel")) {
    $transport = "port-forward"
}
if ($UseExec -or $env:SGPU_TRANSPORT -eq "exec") {
    $transport = "exec"
}
$script:MonitorNode = $null
$script:PodTable = ""
$script:PodTableAt = [datetime]::MinValue
$validCommands = @("top", "once", "smi", "apps", "pods", "gpustat", "url", "status", "stop", "open")

if ($args.Count -gt 0 -and $Command -eq "top" -and $validCommands -contains $args[0]) {
    $Command = [string]$args[0]
    if ($args.Count -gt 1 -and [string]$args[1] -match "^\d+$") {
        $Interval = [int]$args[1]
    }
}

if ($Command -in @("url", "open")) {
    $transport = "port-forward"
}

function Ensure-Tunnel {
    if ($transport -eq "exec") {
        return
    }
    $args = @("-Namespace", $Namespace, "-Service", $Service, "-LocalPort", $Port)
    if ($ShareOnLan) {
        $args += "-ShareOnLan"
    }
    & powershell -NoProfile -ExecutionPolicy Bypass -File $forwardScript @args | Out-Null
}

function Stop-Tunnel {
    & powershell -NoProfile -ExecutionPolicy Bypass -File $forwardScript -Namespace $Namespace -Service $Service -LocalPort $Port -Stop
}

function Show-Status {
    & powershell -NoProfile -ExecutionPolicy Bypass -File $forwardScript -Namespace $Namespace -Service $Service -LocalPort $Port -Status
}

function Get-Text([string]$Path) {
    if ($transport -eq "exec") {
        $url = "http://127.0.0.1:8080$Path"
        $output = & kubectl exec -n $Namespace $Pod -- bash -lc "curl -fsS '$url'" 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw (($output | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine)
        }
        return (($output | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine)
    }
    try {
        return (Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl$Path" -TimeoutSec 5).Content
    } catch {
        Ensure-Tunnel
        return (Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl$Path" -TimeoutSec 8).Content
    }
}

function Get-MonitorNode {
    if ($script:MonitorNode) {
        return $script:MonitorNode
    }
    try {
        $script:MonitorNode = kubectl get pod $Pod -n $Namespace -o jsonpath='{.spec.nodeName}'
    } catch {
        $script:MonitorNode = ""
    }
    return $script:MonitorNode
}

function Get-PodAge([string]$StartTime) {
    if (-not $StartTime) {
        return "?"
    }
    try {
        $started = ([datetime]::Parse($StartTime)).ToUniversalTime()
        $span = [datetime]::UtcNow - $started
    } catch {
        return "?"
    }
    if ($span.TotalDays -ge 1) {
        return ("{0}d{1}h" -f [int]$span.TotalDays, $span.Hours)
    }
    if ($span.TotalHours -ge 1) {
        return ("{0}h{1}m" -f [int]$span.TotalHours, $span.Minutes)
    }
    return ("{0}m" -f [int][Math]::Max(0, $span.TotalMinutes))
}

function Get-OwnerPrefix([string]$Name) {
    if (-not $Name) {
        return "?"
    }
    return (($Name -replace "_", "-") -split "-", 2)[0]
}

function Get-GpuRequest($Pod) {
    $gpu = 0
    foreach ($container in $Pod.spec.containers) {
        $value = $null
        if ($container.resources.requests.'nvidia.com/gpu') {
            $value = $container.resources.requests.'nvidia.com/gpu'
        } elseif ($container.resources.limits.'nvidia.com/gpu') {
            $value = $container.resources.limits.'nvidia.com/gpu'
        }
        if ($value) {
            $gpu += [int]$value
        }
    }
    return $gpu
}

function Get-KubePodTable {
    if (((Get-Date) - $script:PodTableAt).TotalSeconds -lt 10 -and $script:PodTable) {
        return $script:PodTable
    }
    try {
        $node = Get-MonitorNode
        $pods = kubectl get pods -n $Namespace -o json | ConvertFrom-Json
        $rows = @()
        foreach ($pod in $pods.items) {
            if ($pod.status.phase -notin @("Running", "Pending")) {
                continue
            }
            if ($node -and $pod.spec.nodeName -ne $node) {
                continue
            }
            $gpu = Get-GpuRequest $pod
            if ($gpu -le 0) {
                continue
            }
            $rows += [pscustomobject]@{
                Owner = Get-OwnerPrefix $pod.metadata.name
                GPU = $gpu
                Age = Get-PodAge $pod.status.startTime
                Phase = $pod.status.phase
                Pod = $pod.metadata.name
            }
        }
        $lines = New-Object System.Collections.Generic.List[string]
        $lines.Add("")
        $lines.Add("Kubernetes GPU pods from local kubectl")
        $lines.Add("OWNER    GPU  AGE     PHASE    POD")
        $lines.Add("-------  ---  ------  -------  ----------------------------------------")
        if ($rows.Count -eq 0) {
            $lines.Add("(no Running/Pending GPU-requesting pods found)")
        } else {
            foreach ($row in ($rows | Sort-Object Owner, Pod)) {
                $lines.Add(("{0,-7}  {1,3}  {2,-6}  {3,-7}  {4}" -f $row.Owner.Substring(0, [Math]::Min(7, $row.Owner.Length)), $row.GPU, $row.Age, $row.Phase, $row.Pod))
            }
        }
        $script:PodTable = ($lines -join [Environment]::NewLine)
        $script:PodTableAt = Get-Date
        return $script:PodTable
    } catch {
        return "`nKubernetes GPU pods from local kubectl: unavailable ($($_.Exception.Message))"
    }
}

function Print-Text([string]$Path) {
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    Write-Output (Get-Text $Path)
}

function Print-Dashboard {
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    Write-Output (Get-DashboardText)
}

function Get-DashboardText {
    $parts = @(
        (Get-Text "/table").TrimEnd(),
        (Get-KubePodTable).TrimEnd()
    )
    return ($parts -join ([Environment]::NewLine + [Environment]::NewLine))
}

function Start-LiveDashboard {
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    $esc = [char]27
    [Console]::Write("${esc}[?1049h${esc}[?25l${esc}[2J${esc}[H")
    try {
        while ($true) {
            try {
                $frame = (Get-DashboardText).TrimEnd() +
                    [Environment]::NewLine +
                    [Environment]::NewLine +
                    "refresh=${Interval}s  transport=${transport}  commands: sgpu once | sgpu smi | sgpu apps | sgpu stop" +
                    [Environment]::NewLine
                Render-Frame $frame
            } catch {
                Render-Frame "sgpu: $($_.Exception.Message)"
            }
            Start-Sleep -Seconds $Interval
        }
    } finally {
        [Console]::Write("${esc}[?25h${esc}[?1049l")
    }
}

function Get-TerminalWidth {
    try {
        $width = [Console]::WindowWidth
        if ($width -lt 20) {
            return 80
        }
        return $width
    } catch {
        return 100
    }
}

function Get-TerminalHeight {
    try {
        $height = [Console]::WindowHeight
        if ($height -lt 8) {
            return 24
        }
        return $height
    } catch {
        return 30
    }
}

function Limit-Line([string]$Line, [int]$Width) {
    $max = [Math]::Max(1, $Width - 1)
    if ($Line.Length -le $max) {
        return $Line
    }
    if ($max -le 1) {
        return ""
    }
    return $Line.Substring(0, $max - 1) + ">"
}

function Color-Code([string]$Name) {
    if ($NoColor -or [Console]::IsOutputRedirected) {
        return ""
    }
    $esc = [char]27
    switch ($Name) {
        "reset" { return "${esc}[0m" }
        "bold" { return "${esc}[1m" }
        "dim" { return "${esc}[2m" }
        "cyan" { return "${esc}[36m" }
        "green" { return "${esc}[32m" }
        "yellow" { return "${esc}[33m" }
        "red" { return "${esc}[31m" }
        "magenta" { return "${esc}[35m" }
        default { return "" }
    }
}

function Style-Line([string]$Line) {
    if ($NoColor) {
        return $Line
    }
    $reset = Color-Code "reset"
    if (-not $reset) {
        return $Line
    }
    if ($Line -match "^(SGPU\s+)?MLXP GPU monitor") {
        return "$(Color-Code "bold")$(Color-Code "cyan")$Line$reset"
    }
    if ($Line -match "^(NVIDIA compute processes|Kubernetes GPU pods)") {
        return "$(Color-Code "bold")$(Color-Code "magenta")$Line$reset"
    }
    if ($Line -match "^(GPU  Name|OWNER\s+GPU|GPU UUID)") {
        return "$(Color-Code "bold")$Line$reset"
    }
    if ($Line -match "^-{3,}") {
        return "$(Color-Code "dim")$Line$reset"
    }
    if ($Line -match "^\s*\d+\s+NVIDIA" -and $Line -match "\s+(\d+)%") {
        $util = [int]$Matches[1]
        if ($util -ge 90) {
            return "$(Color-Code "red")$Line$reset"
        }
        if ($util -ge 50) {
            return "$(Color-Code "yellow")$Line$reset"
        }
        return "$(Color-Code "green")$Line$reset"
    }
    if ($Line -match "^refresh=") {
        return "$(Color-Code "dim")$Line$reset"
    }
    return $Line
}

function Render-Frame([string]$Frame) {
    $esc = [char]27
    $width = Get-TerminalWidth
    $height = Get-TerminalHeight
    $maxRows = [Math]::Max(1, $height - 1)
    $lines = ($Frame -replace "`r", "") -split "`n"
    $rowCount = [Math]::Min($lines.Count, $maxRows)
    [Console]::Write("${esc}[H")
    for ($i = 0; $i -lt $rowCount; $i++) {
        $line = $lines[$i]
        if ($line -match "^MLXP GPU monitor") {
            $line = "SGPU  $line"
        }
        [Console]::Write("${esc}[2K")
        [Console]::Write((Style-Line (Limit-Line $line $width)))
        if ($i -lt ($rowCount - 1)) {
            [Console]::Write([Environment]::NewLine)
        }
    }
    [Console]::Write("${esc}[J")
}

if ($Command -eq "stop") {
    Stop-Tunnel
    exit 0
}

if ($Command -eq "status") {
    if ($transport -eq "exec") {
        Write-Host "sgpu default transport=exec; no local port-forward is used for top/once/smi/apps."
        Show-Status
        exit 0
    }
    Show-Status
    exit 0
}

Ensure-Tunnel

switch ($Command) {
    "url" {
        Write-Host "$baseUrl/table"
        Write-Host "$baseUrl/smi"
        Write-Host "$baseUrl/apps"
    }
    "open" {
        Start-Process "$baseUrl/table"
    }
    "once" {
        Print-Dashboard
    }
    "smi" {
        Print-Text "/smi"
    }
    "apps" {
        Print-Text "/apps"
    }
    "pods" {
        Print-Text "/pods"
    }
    "gpustat" {
        Print-Text "/gpustat"
    }
    "top" {
        Start-LiveDashboard
    }
}
