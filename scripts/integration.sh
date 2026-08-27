#!/usr/bin/env sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose_file="$project_root/deployments/local/compose.integration.yml"
project_name=simq-integration
artifact_dir="$project_root/.cache/integration"
required_toolchain=go1.26.7
keep=${SIMQ_INTEGRATION_KEEP:-false}

if [ -x "$project_root/.cache/toolchains/$required_toolchain/go/bin/go" ]; then
  go_binary="$project_root/.cache/toolchains/$required_toolchain/go/bin/go"
else
  go_binary=$(command -v go)
fi
if [ "$($go_binary env GOVERSION)" != "$required_toolchain" ]; then
  echo "Go toolchain mismatch: integration tests require $required_toolchain" >&2
  exit 1
fi
docker info >/dev/null
docker compose version >/dev/null

mkdir -p "$artifact_dir" "$project_root/.cache/go-build" "$project_root/.cache/go-mod" "$project_root/.cache/go-tmp"
export GOROOT=$($go_binary env GOROOT)
export GOCACHE="$project_root/.cache/go-build"
export GOMODCACHE="$project_root/.cache/go-mod"
export GOTMPDIR="$project_root/.cache/go-tmp"
export SIMQ_INTEGRATION_COMPOSE_FILE="$compose_file"
export SIMQ_INTEGRATION_PROJECT="$project_name"
export SIMQ_INTEGRATION_URLS=http://127.0.0.1:19324,http://127.0.0.1:19325,http://127.0.0.1:19326
export SIMQ_INTEGRATION_ADMIN_TOKEN=local-integration-admin

compose() {
  docker compose -p "$project_name" -f "$compose_file" "$@"
}
cleanup() {
  compose ps --all >"$artifact_dir/compose-ps.txt" 2>&1 || true
  compose logs --no-color >"$artifact_dir/compose.log" 2>&1 || true
  if [ "$keep" != true ]; then
    compose down --volumes --remove-orphans || true
  fi
}
trap cleanup EXIT INT TERM

cd "$project_root"
compose down --volumes --remove-orphans
compose up -d --build
"$go_binary" test -tags=integration -count=1 -v ./tests/integration
echo "integration tests passed; diagnostics: $artifact_dir"
