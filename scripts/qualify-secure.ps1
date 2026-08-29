[CmdletBinding()]
param(
    [ValidateRange(1, 10)]
    [int]$Iterations = 2,
    [ValidateRange(60, 3600)]
    [int]$IterationTimeoutSeconds = 600
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$runner = Join-Path $PSScriptRoot 'secure-integration.ps1'
$sourceDir = Join-Path $projectRoot '.cache\integration-secure'
$artifactDir = Join-Path $projectRoot '.cache\qualification\secure'
$expectedArtifactDir = [IO.Path]::GetFullPath((Join-Path $projectRoot '.cache\qualification\secure'))
if ([IO.Path]::GetFullPath($artifactDir) -ne $expectedArtifactDir) {
    throw "refusing to reset unexpected qualification path: $artifactDir"
}
if (Test-Path -LiteralPath $artifactDir) {
    Remove-Item -LiteralPath $artifactDir -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $artifactDir | Out-Null

function Copy-QualificationDiagnostics([int]$Iteration) {
    $destination = Join-Path $artifactDir "iteration-$Iteration"
    New-Item -ItemType Directory -Force -Path $destination | Out-Null
    foreach ($name in @('before-backup.log', 'compose.log', 'compose-ps.txt')) {
        $source = Join-Path $sourceDir $name
        if (Test-Path -LiteralPath $source) {
            Copy-Item -LiteralPath $source -Destination (Join-Path $destination $name)
        }
    }
    return $destination
}

$started = Get-Date
$shell = (Get-Process -Id $PID).Path
for ($iteration = 1; $iteration -le $Iterations; $iteration++) {
    $iterationStarted = Get-Date
    $process = Start-Process -FilePath $shell -ArgumentList @('-NoProfile', '-File', $runner) -NoNewWindow -PassThru
    $finished = $process.WaitForExit($IterationTimeoutSeconds * 1000)
    if (-not $finished) {
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
        & docker compose -p simq-secure-integration -f (Join-Path $projectRoot 'deployments\local\compose.secure-integration.yml') down --volumes --remove-orphans *> $null
    }
    $destination = Copy-QualificationDiagnostics $iteration
    $duration = [int]((Get-Date) - $iterationStarted).TotalSeconds
    if (-not $finished) {
        @('status=failed', 'reason=timeout', "duration_seconds=$duration") | Out-File (Join-Path $destination 'result.txt') -Encoding utf8
        throw "secure qualification iteration $iteration exceeded ${IterationTimeoutSeconds}s"
    }
    if ($process.ExitCode -ne 0) {
        @('status=failed', "exit_code=$($process.ExitCode)", "duration_seconds=$duration") | Out-File (Join-Path $destination 'result.txt') -Encoding utf8
        throw "secure qualification iteration $iteration failed with exit code $($process.ExitCode)"
    }
    @('status=passed', "duration_seconds=$duration") | Out-File (Join-Path $destination 'result.txt') -Encoding utf8
}

$totalDuration = [int]((Get-Date) - $started).TotalSeconds
@(
    '### Secure operational qualification'
    ''
    '- Result: passed'
    "- Iterations: $Iterations"
    "- Total duration: ${totalDuration}s"
    '- Per iteration: TLS 1.3/OIDC/two-shard partition, FIFO failover, and offline three-volume restore'
) | Out-File (Join-Path $artifactDir 'summary.md') -Encoding utf8
Write-Host "secure operational qualification passed $Iterations iterations in ${totalDuration}s"
