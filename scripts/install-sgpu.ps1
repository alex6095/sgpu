param(
    [string]$InstallDir = "$HOME\bin"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$sgpuScript = Join-Path $repoRoot "scripts\sgpu.ps1"

if (-not (Test-Path $sgpuScript)) {
    throw "Cannot find $sgpuScript"
}

New-Item -ItemType Directory -Force $InstallDir | Out-Null

$cmdPath = Join-Path $InstallDir "sgpu.cmd"
$cmd = @"
@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "$sgpuScript" %*
"@
Set-Content -Path $cmdPath -Value $cmd -Encoding ASCII

$currentUserPath = [Environment]::GetEnvironmentVariable("Path", "User")
$pathParts = @()
if ($currentUserPath) {
    $pathParts = $currentUserPath -split ";"
}

$alreadyInPath = $pathParts | Where-Object { $_.TrimEnd("\") -ieq $InstallDir.TrimEnd("\") }
if (-not $alreadyInPath) {
    $newPath = if ($currentUserPath) { "$currentUserPath;$InstallDir" } else { $InstallDir }
    [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
    $env:Path = "$env:Path;$InstallDir"
    Write-Host "Added $InstallDir to the user PATH. New terminals will see sgpu automatically."
}

Write-Host "Installed: $cmdPath"
Write-Host ""
Write-Host "Try:"
Write-Host "  sgpu once"
Write-Host "  sgpu"
Write-Host "  sgpu smi"
Write-Host "  sgpu stop"
