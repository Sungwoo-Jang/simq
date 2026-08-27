[CmdletBinding()]
param(
    [switch]$Keep,
    [switch]$SkipBuild,
    [switch]$ForceUnsupportedCgroup
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$clusterName = 'simq-m10'
$contextName = "kind-$clusterName"
$kindVersion = 'v0.20.0'
$kindChecksum = 'aa49e245e201583884fa079b64d8b648b2ef93edadea9d6dcca127114d87e5ca'
$nodeImage = 'kindest/node:v1.27.3@sha256:3966ac761ae0136263ffdb6cfd4db23ef8a83cba8a463690e98317add2c9ba72'
$toolDir = Join-Path $projectRoot ".cache\tools\kind\$kindVersion"
$kindBinary = Join-Path $toolDir 'kind.exe'
$artifactDir = Join-Path $projectRoot '.cache\integration-kind'
$requiredToolchain = 'go1.26.7'
$localToolchainRoot = Join-Path $projectRoot ".cache\toolchains\$requiredToolchain\go"

if ($env:SIMQ_GO_ROOT) {
    $toolchainRoot = $env:SIMQ_GO_ROOT
} elseif (Test-Path -LiteralPath (Join-Path $localToolchainRoot 'bin\go.exe')) {
    $toolchainRoot = $localToolchainRoot
} else {
    $goCommand = Get-Command go -ErrorAction Stop
    $toolchainRoot = (& $goCommand.Source env GOROOT).Trim()
}
$goBinary = Join-Path $toolchainRoot 'bin\go.exe'
if ((& $goBinary env GOVERSION).Trim() -ne $requiredToolchain) {
    throw "Go toolchain mismatch: kind integration requires $requiredToolchain"
}
& docker info *> $null
if ($LASTEXITCODE -ne 0) { throw 'Docker Desktop or another Docker engine must be running.' }
$cgroupVersion = (& docker info --format '{{.CgroupVersion}}').Trim()
if ($cgroupVersion -eq '1' -and -not $ForceUnsupportedCgroup) {
    throw @'
This Windows Docker engine uses cgroup v1. Its nested kubelet cgroups cannot
start the pinned kind node on this workstation. Enable the WSL2/cgroup v2
Docker backend and retry, or pass -ForceUnsupportedCgroup only to re-run the
known-failing host-capability probe. This is an environment limitation, not
Kubernetes recovery evidence.
'@
}
$kubectl = (Get-Command kubectl -ErrorAction Stop).Source

New-Item -ItemType Directory -Force -Path $toolDir, $artifactDir | Out-Null
if (-not (Test-Path -LiteralPath $kindBinary)) {
    Invoke-WebRequest -Uri "https://github.com/kubernetes-sigs/kind/releases/download/$kindVersion/kind-windows-amd64" -OutFile $kindBinary
}
$actualChecksum = (Get-FileHash -LiteralPath $kindBinary -Algorithm SHA256).Hash.ToLowerInvariant()
if ($actualChecksum -ne $kindChecksum) {
    throw "kind checksum mismatch: got $actualChecksum"
}

$env:GOROOT = $toolchainRoot
$env:GOCACHE = Join-Path $projectRoot '.cache\go-build'
$env:GOMODCACHE = Join-Path $projectRoot '.cache\go-mod'
$env:GOTMPDIR = Join-Path $projectRoot '.cache\go-tmp'
$env:SIMQ_KIND_CONTEXT = $contextName
New-Item -ItemType Directory -Force -Path $env:GOCACHE, $env:GOMODCACHE, $env:GOTMPDIR | Out-Null

$completed = $false
Push-Location $projectRoot
try {
    $existing = @(& $kindBinary get clusters)
    if ($existing -contains $clusterName) {
        & $kindBinary delete cluster --name $clusterName
        if ($LASTEXITCODE -ne 0) { throw "failed to delete stale kind cluster $clusterName" }
    }
    if (-not $SkipBuild) {
        & docker build -t simq:kind .
        if ($LASTEXITCODE -ne 0) { throw 'failed to build simq:kind image' }
    }
    & $kindBinary create cluster --name $clusterName --image $nodeImage --config deployments/kubernetes/kind/cluster.yml --wait 180s
    if ($LASTEXITCODE -ne 0) { throw 'failed to create kind cluster' }
    & $kindBinary load docker-image simq:kind --name $clusterName
    if ($LASTEXITCODE -ne 0) { throw 'failed to load SimQ image into kind' }
    & $kubectl --context $contextName apply -f deployments/kubernetes/kind/simq.yml
    if ($LASTEXITCODE -ne 0) { throw 'failed to apply SimQ StatefulSet' }

    $deadline = (Get-Date).AddMinutes(3)
    do {
        $running = @(& $kubectl --context $contextName -n simq-m10 get pods -l app=simq --field-selector=status.phase=Running -o name 2>$null)
        if ($running.Count -eq 3) { break }
        Start-Sleep -Milliseconds 500
    } while ((Get-Date) -lt $deadline)
    if ($running.Count -ne 3) { throw 'three SimQ Pods did not reach Running phase' }

    & $goBinary test -tags=kindintegration -count=1 -v ./tests/integration -run '^TestKindStatefulSetLeaderReplacement$'
    if ($LASTEXITCODE -ne 0) { throw 'kind leader-replacement scenario failed' }
    $completed = $true
} finally {
    & $kubectl --context $contextName -n simq-m10 get all,pvc -o wide 2>&1 | Out-File -FilePath (Join-Path $artifactDir 'resources.txt') -Encoding utf8
    & $kubectl --context $contextName -n simq-m10 describe pods 2>&1 | Out-File -FilePath (Join-Path $artifactDir 'pods.txt') -Encoding utf8
    foreach ($pod in @('simq-0', 'simq-1', 'simq-2')) {
        & $kubectl --context $contextName -n simq-m10 logs $pod --all-containers 2>&1 | Out-File -FilePath (Join-Path $artifactDir "$pod.log") -Encoding utf8
    }
    if (-not $Keep) {
        & $kindBinary delete cluster --name $clusterName
    } else {
        Write-Host "kind cluster retained; delete it with: $kindBinary delete cluster --name $clusterName"
    }
    Pop-Location
}

if (-not $completed) { exit 1 }
Write-Host "kind leader-replacement test passed; diagnostics: $artifactDir"
