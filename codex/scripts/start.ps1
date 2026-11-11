param(
    [Parameter(Mandatory=$true)][string]$State,
    [int]$Port = 0,
    [string]$TokenEnv = 'WORKER_LOCAL_TOKEN',
    [string]$Binary
)
$ErrorActionPreference = 'Stop'
# Optional local credential loader; plain environment variables also work.
if (Test-Path 'D:\project\env\env.ps1') { . 'D:\project\env\env.ps1' }
if (-not $Binary) { $Binary = Join-Path $PSScriptRoot '..\target\debug\codex-native-worker.exe' }
& $Binary serve --state $State --port $Port --token-env $TokenEnv
exit $LASTEXITCODE
