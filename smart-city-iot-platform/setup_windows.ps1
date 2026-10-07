# One-time Windows setup for the Smart City IoT project.
# Run from the project folder in PowerShell:
#     Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#     .\setup_windows.ps1
$ErrorActionPreference = "Stop"

Write-Host "`n[1/5] Checking Python 3.11 ..." -ForegroundColor Cyan
$pyOk = $true
try { & py -3.11 --version; if ($LASTEXITCODE -ne 0) { $pyOk = $false } } catch { $pyOk = $false }
if (-not $pyOk) {
    Write-Host "Python 3.11 not found. Install it from https://www.python.org/downloads/release/python-3119/ (tick 'Add to PATH') and re-run." -ForegroundColor Red
    exit 1
}

Write-Host "`n[2/5] Checking Java 17 ..." -ForegroundColor Cyan
try { & java -version } catch {
    Write-Host "Java not found. Install Temurin JDK 17 from https://adoptium.net/temurin/releases/?version=17 (tick 'Set JAVA_HOME') and re-run." -ForegroundColor Red
    exit 1
}
if (-not $env:JAVA_HOME) {
    $javaExe = (Get-Command java).Source
    $javaHome = Split-Path (Split-Path $javaExe -Parent) -Parent
    [Environment]::SetEnvironmentVariable("JAVA_HOME", $javaHome, "User")
    $env:JAVA_HOME = $javaHome
    Write-Host "JAVA_HOME set to $javaHome"
}

Write-Host "`n[3/5] Installing Hadoop winutils (needed by Spark to write files on Windows) ..." -ForegroundColor Cyan
$hadoopHome = Join-Path $env:USERPROFILE "hadoop"
$bin = Join-Path $hadoopHome "bin"
New-Item -ItemType Directory -Force -Path $bin | Out-Null
$base = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin"
foreach ($f in @("winutils.exe", "hadoop.dll")) {
    $dest = Join-Path $bin $f
    if (-not (Test-Path $dest)) { Invoke-WebRequest "$base/$f" -OutFile $dest }
}
[Environment]::SetEnvironmentVariable("HADOOP_HOME", $hadoopHome, "User")
$env:HADOOP_HOME = $hadoopHome
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($userPath -notlike "*$bin*") {
    [Environment]::SetEnvironmentVariable("Path", "$userPath;$bin", "User")
}
$env:Path = "$env:Path;$bin"
Write-Host "HADOOP_HOME = $hadoopHome"

Write-Host "`n[4/5] Creating virtual environment .venv and installing packages ..." -ForegroundColor Cyan
if (-not (Test-Path ".venv")) { & py -3.11 -m venv .venv }
& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt

Write-Host "`n[5/5] Smoke test: start Spark and write a Parquet file ..." -ForegroundColor Cyan
& .\.venv\Scripts\python.exe -c "from src.spark_utils import get_spark; s=get_spark('smoke'); s.range(5).write.mode('overwrite').parquet('output/_smoke'); print('Spark', s.version, 'OK'); s.stop()"

Write-Host "`nSetup complete. Close this window, open a NEW PowerShell in the project folder, then run:" -ForegroundColor Green
Write-Host "    .\.venv\Scripts\Activate.ps1"
