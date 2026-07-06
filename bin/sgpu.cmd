@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0..\scripts\sgpu.ps1" %*
