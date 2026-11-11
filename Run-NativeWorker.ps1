param(
    [Parameter(Mandatory=$true)][ValidateSet('pi','codex')][string]$Engine,
    [Parameter(Mandatory=$true)][string]$State,
    [string]$Project,
    [string]$ProjectId='project',
    [int]$Port=0,
    [switch]$Initialize,
    [string]$CodexExecutable,
    [string]$CodexHostExecutable,
    [string]$Model,
    [string]$ModelBaseUrl
)
$ErrorActionPreference='Stop'
# Optional local credential loader: dot-source a file that exports WORKER_LOCAL_TOKEN / MODEL_*,
# or set them directly in the environment. Plain inherited env vars also work.
if (Test-Path 'D:\project\env\env.ps1') { . 'D:\project\env\env.ps1' }
if($Model){$env:MODEL_DEFAULT_MODEL=$Model}
if($ModelBaseUrl){$env:MODEL_BASE_URL=$ModelBaseUrl}
if([string]::IsNullOrWhiteSpace($env:WORKER_LOCAL_TOKEN) -or $env:WORKER_LOCAL_TOKEN.Length -lt 24){throw 'Configure WORKER_LOCAL_TOKEN in the environment (minimum 24 characters).'}
if($Port -lt 0 -or $Port -gt 65535){throw 'Port must be between 0 and 65535.'}
$nativeState=[System.IO.Path]::GetFullPath($State)
if($Initialize -and [string]::IsNullOrWhiteSpace($Project)){throw 'Initialize requires an explicit registered Git project root.'}
if($Initialize){
    foreach($nativeMarker in @('registry.json','ledger.sqlite','worker.sqlite')){
        if(Test-Path -LiteralPath (Join-Path $nativeState $nativeMarker)){throw 'State is already initialized. Start without Initialize; use a fresh state directory for a different binding.'}
    }
}
if($Initialize -and ([string]::IsNullOrWhiteSpace($env:MODEL_DEFAULT_MODEL) -or [string]::IsNullOrWhiteSpace($env:MODEL_BASE_URL))){throw 'Explicit model and model base URL are required.'}
if($Engine -eq 'pi'){
    $nativeNode=(Get-Command node -ErrorAction Stop).Source
    $nativeMain=Join-Path $PSScriptRoot 'pi\dist\host\main.js'
    if(-not (Test-Path -LiteralPath $nativeMain -PathType Leaf)){throw 'Build workers/pi with npm ci and npm run build first.'}
    if($Initialize){
        & $nativeNode $nativeMain init --state $nativeState --project $Project --project-id $ProjectId
        if($LASTEXITCODE -ne 0){throw 'Pi initialization failed.'}
    }
    & $nativeNode $nativeMain serve --state $nativeState --port $Port --token-env WORKER_LOCAL_TOKEN
}else{
    if(-not $CodexHostExecutable){
        $nativeCandidates=@('codex\target\release\codex-native-worker.exe','codex\target\debug\codex-native-worker.exe','codex\target\x86_64-pc-windows-gnu\release\codex-native-worker.exe','codex\target\x86_64-pc-windows-gnu\debug\codex-native-worker.exe')
        foreach($nativeCandidate in $nativeCandidates){$nativePath=Join-Path $PSScriptRoot $nativeCandidate;if(Test-Path -LiteralPath $nativePath -PathType Leaf){$CodexHostExecutable=$nativePath;break}}
    }
    if(-not $CodexHostExecutable -or -not (Test-Path -LiteralPath $CodexHostExecutable -PathType Leaf)){throw 'Build workers/codex or supply CodexHostExecutable first.'}
    if($Initialize){
        if(-not $CodexExecutable){$CodexExecutable=(Get-Command codex -ErrorAction Stop).Source}
        & $CodexHostExecutable init --state $nativeState --executable $CodexExecutable --project $Project --project-id $ProjectId --model $env:MODEL_DEFAULT_MODEL --base-url $env:MODEL_BASE_URL
        if($LASTEXITCODE -ne 0){throw 'Codex initialization failed.'}
    }
    & $CodexHostExecutable serve --state $nativeState --port $Port --token-env WORKER_LOCAL_TOKEN
}
if($LASTEXITCODE -ne 0){throw 'Native host exited with an error.'}
