# scripts/run_all.ps1
# 用 venv 的 python 直接跑每个脚本，无需 Activate.ps1
$ErrorActionPreference = "Stop"

# 脚本目录（scripts）与项目根
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjDir   = (Get-Item $ScriptDir).Parent.FullName
Set-Location $ProjDir

# venv python
$py = Join-Path $ProjDir ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { throw "找不到虚拟环境 Python：$py" }

# 日志
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$logDir = Join-Path $ProjDir "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$log = Join-Path $logDir "run_$ts.log"

function RunPy([string]$script, [string]$args="") {
  Write-Host ">> $script $args" -ForegroundColor Cyan
  & $py $script $args 2>&1 | Tee-Object -FilePath $log -Append
  if ($LASTEXITCODE -ne 0) { throw "命令失败：$script $args" }
}

RunPy "scripts\build_merged.py"
RunPy "scripts\build_baseline.py"
RunPy "scripts\build_slope.py"
RunPy "scripts\score_alerts.py"
RunPy "scripts\plot_baseline_alerts.py"
RunPy "scripts\make_report.py"

Write-Host "完成，日志：$log" -ForegroundColor Green
