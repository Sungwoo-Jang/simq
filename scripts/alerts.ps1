[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$image = 'prom/prometheus@sha256:69f5241418838263316593f7274a304b095c40bcf22e57272865da91bd60a8ac'

& docker info *> $null
if ($LASTEXITCODE -ne 0) { throw 'Docker Desktop or another Docker engine must be running.' }
& docker run --rm --entrypoint /bin/promtool -v "${projectRoot}:/work:ro" -w /work/monitoring/prometheus $image test rules simq-alerts.test.yml
if ($LASTEXITCODE -ne 0) { throw 'Prometheus alert-rule tests failed' }
