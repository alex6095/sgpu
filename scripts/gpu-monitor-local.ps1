param(
    [string]$Namespace = "p-sgvr-node-02",
    [string]$Service = "sangmin-gpu-monitor",
    [int]$LocalPort = 18080,
    [switch]$ShareOnLan,
    [switch]$Stop,
    [switch]$Status,
    [switch]$Open
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$workDir = Join-Path $repoRoot "work"
$pidFile = Join-Path $workDir "gpu-monitor-port-forward.pid"
$addressFile = Join-Path $workDir "gpu-monitor-port-forward.address"
$outLog = Join-Path $workDir "gpu-monitor-port-forward.out.log"
$errLog = Join-Path $workDir "gpu-monitor-port-forward.err.log"
$localUrl = "http://127.0.0.1:${LocalPort}"
$desiredAddress = if ($ShareOnLan) { "0.0.0.0" } else { "127.0.0.1" }

function Get-ForwardProcess {
    if (-not (Test-Path $pidFile)) {
        return Find-ForwardProcess
    }
    $rawPid = (Get-Content $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if (-not $rawPid) {
        return Find-ForwardProcess
    }
    try {
        $proc = Get-Process -Id ([int]$rawPid) -ErrorAction SilentlyContinue
        if ($proc) {
            return $proc
        }
        return Find-ForwardProcess
    } catch {
        return Find-ForwardProcess
    }
}

function Find-ForwardProcess {
    $patternPort = "${LocalPort}:8080"
    try {
        $match = Get-CimInstance Win32_Process -Filter "name='kubectl.exe'" -ErrorAction SilentlyContinue |
            Where-Object {
                $_.CommandLine -like "*port-forward*" -and
                $_.CommandLine -like "*svc/$Service*" -and
                $_.CommandLine -like "*$patternPort*" -and
                $_.CommandLine -like "*$Namespace*"
            } |
            Select-Object -First 1
        if ($match) {
            return Get-Process -Id $match.ProcessId -ErrorAction SilentlyContinue
        }
    } catch {
        return $null
    }
    return $null
}

function Test-Health {
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "$localUrl/health" -TimeoutSec 2
        return $response.StatusCode -eq 200
    } catch {
        return $false
    }
}

function Show-Urls {
    Write-Host ""
    Write-Host "Local GPU monitor:"
    Write-Host "  $localUrl/table"
    Write-Host "  $localUrl/smi"
    Write-Host "  $localUrl/gpustat"
    Write-Host "  $localUrl/apps"
    Write-Host ""
    Write-Host "Stop:"
    Write-Host "  sgpu stop"
    Write-Host "  .\scripts\gpu-monitor-local.ps1 -Stop"
}

New-Item -ItemType Directory -Force $workDir | Out-Null

if ($Stop) {
    $proc = Get-ForwardProcess
    if ($proc) {
        Stop-Process -Id $proc.Id -Force
        Write-Host "Stopped kubectl port-forward pid=$($proc.Id)"
    } else {
        Write-Host "No running gpu monitor port-forward found."
    }
    Remove-Item -Force $pidFile, $addressFile, $outLog, $errLog -ErrorAction SilentlyContinue
    exit 0
}

if ($Status) {
    $proc = Get-ForwardProcess
    if ($proc) {
        Write-Host "kubectl port-forward pid=$($proc.Id) is running."
        if (Test-Path $addressFile) {
            Write-Host "address=$(Get-Content $addressFile | Select-Object -First 1)"
        }
        Write-Host "health=$((Test-Health))"
        Show-Urls
    } else {
        Write-Host "kubectl port-forward is not running."
    }
    exit 0
}

kubectl get service $Service -n $Namespace | Out-Host
kubectl get pod $Service -n $Namespace -o wide | Out-Host

$existing = Get-ForwardProcess
$existingAddress = if (Test-Path $addressFile) { Get-Content $addressFile | Select-Object -First 1 } else { "" }
if ($existing -and (Test-Health) -and $existingAddress -eq $desiredAddress) {
    Set-Content -Path $pidFile -Value $existing.Id -Encoding ASCII
    Set-Content -Path $addressFile -Value $desiredAddress -Encoding ASCII
    Write-Host "Reusing existing kubectl port-forward pid=$($existing.Id)"
    Show-Urls
    if ($Open) {
        Start-Process "$localUrl/smi"
    }
    exit 0
}

if ($existing) {
    Stop-Process -Id $existing.Id -Force
    Remove-Item -Force $pidFile, $addressFile, $outLog, $errLog -ErrorAction SilentlyContinue
}

if (Test-Health) {
    $detected = Find-ForwardProcess
    if ($detected) {
        Set-Content -Path $pidFile -Value $detected.Id -Encoding ASCII
        Set-Content -Path $addressFile -Value $desiredAddress -Encoding ASCII
        Write-Host "Reusing detected kubectl port-forward pid=$($detected.Id)"
        Show-Urls
        if ($Open) {
            Start-Process "$localUrl/table"
        }
        exit 0
    }
}

$args = @(
    "-n", $Namespace,
    "port-forward",
    "svc/$Service",
    "${LocalPort}:8080",
    "--address", $desiredAddress
)

$proc = Start-Process -FilePath "kubectl" `
    -ArgumentList $args `
    -RedirectStandardOutput $outLog `
    -RedirectStandardError $errLog `
    -WindowStyle Hidden `
    -PassThru

Set-Content -Path $pidFile -Value $proc.Id -Encoding ASCII
Set-Content -Path $addressFile -Value $desiredAddress -Encoding ASCII

$deadline = (Get-Date).AddSeconds(12)
while ((Get-Date) -lt $deadline) {
    if (Test-Health) {
        break
    }
    Start-Sleep -Milliseconds 500
}

if (-not (Test-Health)) {
    Write-Host "Port-forward started but health check failed. Logs:"
    Get-Content $outLog, $errLog -ErrorAction SilentlyContinue | Select-Object -Last 80
    throw "GPU monitor local forwarding is not healthy."
}

Show-Urls

if ($ShareOnLan) {
    Write-Host ""
    Write-Host "LAN sharing is enabled. Other computers on the reachable network can try:"
    Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Where-Object { $_.IPAddress -notlike "127.*" -and $_.PrefixOrigin -ne "WellKnown" } |
        Select-Object -ExpandProperty IPAddress |
        ForEach-Object { Write-Host "  http://${_}:${LocalPort}/table" }
    Write-Host ""
    Write-Host "Only use -ShareOnLan on trusted networks; Windows Firewall may still block it."
}

if ($Open) {
    Start-Process "$localUrl/table"
}
