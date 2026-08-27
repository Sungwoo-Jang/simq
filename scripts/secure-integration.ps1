[CmdletBinding()]
param(
    [switch]$Keep,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$composeFile = Join-Path $projectRoot 'deployments\local\compose.secure-integration.yml'
$projectName = 'simq-secure-integration'
$artifactDir = Join-Path $projectRoot '.cache\integration-secure'
$fixtureDir = Join-Path $artifactDir 'fixtures'
$backupDir = Join-Path $artifactDir 'backup'
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
    throw "Go toolchain mismatch: secure integration requires $requiredToolchain"
}
& docker info *> $null
if ($LASTEXITCODE -ne 0) { throw 'Docker Desktop or another Docker engine must be running.' }
& docker compose version *> $null
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose v2 is required.' }

New-Item -ItemType Directory -Force -Path $artifactDir, $fixtureDir, $backupDir | Out-Null
$env:GOROOT = $toolchainRoot
$env:GOCACHE = Join-Path $projectRoot '.cache\go-build'
$env:GOMODCACHE = Join-Path $projectRoot '.cache\go-mod'
$env:GOTMPDIR = Join-Path $projectRoot '.cache\go-tmp'
$env:SIMQ_SECURE_FIXTURE_DIR = $fixtureDir
$env:SIMQ_SECURE_COMPOSE_FILE = $composeFile
New-Item -ItemType Directory -Force -Path $env:GOCACHE, $env:GOMODCACHE, $env:GOTMPDIR | Out-Null

$compose = @('compose', '-p', $projectName, '-f', $composeFile)
$volumes = @('n1', 'n2', 'n3')
$completed = $false
Push-Location $projectRoot
try {
    & docker @compose down --volumes --remove-orphans
    if ($LASTEXITCODE -ne 0) { throw 'failed to reset secure integration resources' }

    & $goBinary run ./tests/integration/fixture generate -output $fixtureDir
    if ($LASTEXITCODE -ne 0) { throw 'failed to generate ephemeral security fixtures' }
    & docker @compose config --quiet
    if ($LASTEXITCODE -ne 0) { throw 'secure Compose configuration is invalid' }

    $up = @('up', '-d')
    if (-not $SkipBuild) { $up += '--build' }
    & docker @compose @up
    if ($LASTEXITCODE -ne 0) { throw 'failed to start secure multi-shard cluster' }

    & $goBinary test -tags=secureintegration -count=1 -v ./tests/integration -run '^TestSecureMultiShardChaos$'
    if ($LASTEXITCODE -ne 0) { throw 'secure chaos scenario failed' }
    & $goBinary test -tags=secureintegration -count=1 -v ./tests/integration -run '^TestDisasterRecoveryPrepare$'
    if ($LASTEXITCODE -ne 0) { throw 'disaster-recovery preparation failed' }

    & docker @compose logs --no-color | Out-File -FilePath (Join-Path $artifactDir 'before-backup.log') -Encoding utf8
    & docker @compose stop n1 n2 n3
    if ($LASTEXITCODE -ne 0) { throw 'failed to stop writers before backup' }
    foreach ($node in $volumes) {
        $volumeName = "$projectName-$node-data"
        & docker run --rm -v "${volumeName}:/source:ro" -v "${backupDir}:/backup" alpine:3.23 sh -ec "cd /source && tar -czf /backup/$node.tgz ."
        if ($LASTEXITCODE -ne 0) { throw "failed to archive $volumeName" }
    }

    & docker @compose down --volumes --remove-orphans
    if ($LASTEXITCODE -ne 0) { throw 'failed to delete source volumes before restore' }
    foreach ($node in $volumes) {
        $volumeName = "$projectName-$node-data"
        & docker volume create --label "com.docker.compose.project=$projectName" --label "com.docker.compose.volume=${node}-data" $volumeName *> $null
        if ($LASTEXITCODE -ne 0) { throw "failed to recreate $volumeName" }
        & docker run --rm -v "${volumeName}:/target" -v "${backupDir}:/backup:ro" alpine:3.23 sh -ec "cd /target && tar -xzf /backup/$node.tgz"
        if ($LASTEXITCODE -ne 0) { throw "failed to restore $volumeName" }
    }

    & docker @compose up -d --no-build
    if ($LASTEXITCODE -ne 0) { throw 'failed to start restored cluster' }
    & $goBinary test -tags=secureintegration -count=1 -v ./tests/integration -run '^TestDisasterRecoveryVerify$'
    if ($LASTEXITCODE -ne 0) { throw 'restored cluster verification failed' }
    $completed = $true
} finally {
    & docker @compose ps --all | Out-File -FilePath (Join-Path $artifactDir 'compose-ps.txt') -Encoding utf8
    & docker @compose logs --no-color | Out-File -FilePath (Join-Path $artifactDir 'compose.log') -Encoding utf8
    if (-not $Keep) {
        & docker @compose down --volumes --remove-orphans
        $resolvedArtifact = [IO.Path]::GetFullPath($artifactDir).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
        $resolvedFixture = [IO.Path]::GetFullPath($fixtureDir)
        if (-not $resolvedFixture.StartsWith($resolvedArtifact, [StringComparison]::OrdinalIgnoreCase)) {
            throw "refusing to delete fixture path outside $artifactDir"
        }
        if (Test-Path -LiteralPath $resolvedFixture) {
            Remove-Item -LiteralPath $resolvedFixture -Recurse -Force
        }
    } else {
        Write-Host "restored cluster retained; stop it with: docker compose -p $projectName -f `"$composeFile`" down --volumes"
    }
    Pop-Location
}

if (-not $completed) { exit 1 }
Write-Host "secure chaos and disaster-recovery tests passed; diagnostics: $artifactDir"
