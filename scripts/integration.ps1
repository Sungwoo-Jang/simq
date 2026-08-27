[CmdletBinding()]
param(
    [switch]$Keep,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$composeFile = Join-Path $projectRoot 'deployments\local\compose.integration.yml'
$projectName = 'simq-integration'
$artifactDir = Join-Path $projectRoot '.cache\integration'
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
if (-not (Test-Path -LiteralPath $goBinary)) {
    throw "Go was not found below $toolchainRoot"
}
if ((& $goBinary env GOVERSION).Trim() -ne $requiredToolchain) {
    throw "Go toolchain mismatch: integration tests require $requiredToolchain"
}

& docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw 'Docker Desktop or another Docker engine must be running.'
}
& docker compose version *> $null
if ($LASTEXITCODE -ne 0) {
    throw 'Docker Compose v2 is required.'
}

New-Item -ItemType Directory -Force -Path $artifactDir | Out-Null
$env:GOROOT = $toolchainRoot
$env:GOCACHE = Join-Path $projectRoot '.cache\go-build'
$env:GOMODCACHE = Join-Path $projectRoot '.cache\go-mod'
$env:GOTMPDIR = Join-Path $projectRoot '.cache\go-tmp'
$env:SIMQ_INTEGRATION_COMPOSE_FILE = $composeFile
$env:SIMQ_INTEGRATION_PROJECT = $projectName
$env:SIMQ_INTEGRATION_URLS = 'http://127.0.0.1:19324,http://127.0.0.1:19325,http://127.0.0.1:19326'
$env:SIMQ_INTEGRATION_ADMIN_TOKEN = 'local-integration-admin'
New-Item -ItemType Directory -Force -Path $env:GOCACHE, $env:GOMODCACHE, $env:GOTMPDIR | Out-Null

$compose = @('compose', '-p', $projectName, '-f', $composeFile)
$testFailed = $false
Push-Location $projectRoot
try {
    & docker @compose down --volumes --remove-orphans
    if ($LASTEXITCODE -ne 0) { throw 'failed to reset the integration project' }
    $up = @('up', '-d')
    if (-not $SkipBuild) { $up += '--build' }
    & docker @compose @up
    if ($LASTEXITCODE -ne 0) { throw 'failed to start the integration cluster' }

    & $goBinary test -tags=integration -count=1 -v ./tests/integration
    if ($LASTEXITCODE -ne 0) {
        $testFailed = $true
        throw 'integration tests failed'
    }
} finally {
    & docker @compose ps --all | Out-File -FilePath (Join-Path $artifactDir 'compose-ps.txt') -Encoding utf8
    & docker @compose logs --no-color | Out-File -FilePath (Join-Path $artifactDir 'compose.log') -Encoding utf8
    if (-not $Keep) {
        & docker @compose down --volumes --remove-orphans
    } else {
        Write-Host "cluster retained; stop it with: docker compose -p $projectName -f `"$composeFile`" down --volumes"
    }
    Pop-Location
}

if ($testFailed) { exit 1 }
Write-Host "integration tests passed; diagnostics: $artifactDir"
