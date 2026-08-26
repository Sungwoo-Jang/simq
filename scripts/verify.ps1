[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
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
$gofmtBinary = Join-Path $toolchainRoot 'bin\gofmt.exe'
if (-not (Test-Path -LiteralPath $goBinary) -or -not (Test-Path -LiteralPath $gofmtBinary)) {
    throw "Go toolchain binaries were not found below $toolchainRoot"
}

$actualToolchain = (& $goBinary env GOVERSION).Trim()
if ($actualToolchain -ne $requiredToolchain) {
    throw "Go toolchain mismatch: got $actualToolchain, want $requiredToolchain"
}

$env:GOROOT = $toolchainRoot
$env:GOCACHE = Join-Path $projectRoot '.cache\go-build'
$env:GOMODCACHE = Join-Path $projectRoot '.cache\go-mod'
$env:GOTMPDIR = Join-Path $projectRoot '.cache\go-tmp'
$buildOutput = Join-Path $projectRoot '.cache\bin\simq.exe'
New-Item -ItemType Directory -Force -Path $env:GOCACHE, $env:GOMODCACHE, $env:GOTMPDIR, (Split-Path $buildOutput) | Out-Null

$goFiles = Get-ChildItem -LiteralPath $projectRoot -Recurse -File -Filter '*.go' |
    Where-Object {
        $_.FullName -notlike "$projectRoot\vendor\*" -and
        $_.FullName -notlike "$projectRoot\.cache\*" -and
        $_.FullName -notlike "$projectRoot\.git\*"
    }
$unformatted = @($goFiles | ForEach-Object { & $gofmtBinary -l $_.FullName })
if ($unformatted.Count -ne 0) {
    throw "Unformatted Go files:`n$($unformatted -join "`n")"
}

Write-Host "using $(& $goBinary version)"
Push-Location $projectRoot
try {
    & $goBinary vet ./...
    if ($LASTEXITCODE -ne 0) { throw 'go vet failed' }

    # On Windows os.Process.Kill uses TerminateProcess. The test reopens the same
    # bbolt file and proves the same commit-before-crash recovery boundary that
    # SIGKILL exercises on Unix.
    & $goBinary test -count=1 ./cmd/simq -run '^TestSIGKILLRestartRecoversHTTPState$'
    if ($LASTEXITCODE -ne 0) { throw 'process-crash recovery test failed' }

    & $goBinary test ./...
    if ($LASTEXITCODE -ne 0) { throw 'go test failed' }

    & $goBinary test -race ./...
    if ($LASTEXITCODE -ne 0) { throw 'go test -race failed' }

    & $goBinary build -buildvcs=false -o $buildOutput ./cmd/simq
    if ($LASTEXITCODE -ne 0) { throw 'go build failed' }
} finally {
    Pop-Location
}
