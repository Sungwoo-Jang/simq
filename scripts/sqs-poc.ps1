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
$artifactDir = Join-Path $projectRoot '.cache\sqs-poc'
$reportPath = Join-Path $artifactDir 'simq-report.json'
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
    throw "Go toolchain mismatch: SQS PoC requires $requiredToolchain"
}

& docker info *> $null
if ($LASTEXITCODE -ne 0) { throw 'Docker Desktop or another Docker engine must be running.' }
& docker compose version *> $null
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose v2 is required.' }

$env:GOROOT = $toolchainRoot
$env:GOCACHE = Join-Path $projectRoot '.cache\go-build'
$env:GOMODCACHE = Join-Path $projectRoot '.cache\go-mod'
$env:GOTMPDIR = Join-Path $projectRoot '.cache\go-tmp'
New-Item -ItemType Directory -Force -Path $artifactDir, $env:GOCACHE, $env:GOMODCACHE, $env:GOTMPDIR | Out-Null

$compose = @('compose', '-p', $projectName, '-f', $composeFile)
$urls = @('http://127.0.0.1:19324', 'http://127.0.0.1:19325', 'http://127.0.0.1:19326')
Push-Location $projectRoot
try {
    & docker @compose down --volumes --remove-orphans
    if ($LASTEXITCODE -ne 0) { throw 'failed to reset the PoC cluster' }
    $up = @('up', '-d')
    if (-not $SkipBuild) { $up += '--build' }
    & docker @compose @up
    if ($LASTEXITCODE -ne 0) { throw 'failed to start the PoC cluster' }

    $deadline = (Get-Date).AddMinutes(2)
    $pending = @($urls)
    while ($pending.Count -ne 0 -and (Get-Date) -lt $deadline) {
        $next = @()
        foreach ($url in $pending) {
            try {
                $response = Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 -Uri "$url/healthz"
                if ($response.StatusCode -ne 200) { $next += $url }
            } catch {
                $next += $url
            }
        }
        $pending = $next
        if ($pending.Count -ne 0) { Start-Sleep -Milliseconds 250 }
    }
    if ($pending.Count -ne 0) { throw 'the three-node PoC cluster did not become healthy' }

    Push-Location (Join-Path $projectRoot 'poc\sqssemantics')
    try {
        $output = & $goBinary run ./cmd/sqs-poc --backend simq --simq-endpoints ($urls -join ',')
        $pocExitCode = $LASTEXITCODE
        $output | Set-Content -LiteralPath $reportPath -Encoding utf8
        if ($pocExitCode -ne 0) { throw 'SQS semantics PoC failed' }
    } finally {
        Pop-Location
    }

    $report = Get-Content -Raw -LiteralPath $reportPath | ConvertFrom-Json
    if ($report.status -ne 'passed' -or $report.scenarios.Count -ne 6 -or @($report.scenarios | Where-Object status -ne 'passed').Count -ne 0) {
        throw 'SQS semantics report did not contain six passing scenarios'
    }
    $sensitivePattern = '-----BEGIN ([A-Z0-9]+ )?PRIVATE KEY-----|gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,}|eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.'
    if ((Get-Content -Raw -LiteralPath $reportPath) -match $sensitivePattern) {
        throw 'sensitive-value pattern detected in PoC report'
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

Write-Host "SQS semantics PoC passed; redacted report: $reportPath"
