#!/usr/bin/env sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose_file="$project_root/deployments/local/compose.secure-integration.yml"
project_name=simq-secure-integration
artifact_dir="$project_root/.cache/integration-secure"
fixture_dir="$artifact_dir/fixtures"
backup_dir="$artifact_dir/backup"
required_toolchain=go1.26.7
keep=${SIMQ_INTEGRATION_KEEP:-false}

if [ -x "$project_root/.cache/toolchains/$required_toolchain/go/bin/go" ]; then
  go_binary="$project_root/.cache/toolchains/$required_toolchain/go/bin/go"
else
  go_binary=$(command -v go)
fi
if [ "$($go_binary env GOVERSION)" != "$required_toolchain" ]; then
  echo "Go toolchain mismatch: secure integration requires $required_toolchain" >&2
  exit 1
fi
docker info >/dev/null
docker compose version >/dev/null

mkdir -p "$artifact_dir" "$fixture_dir" "$backup_dir" "$project_root/.cache/go-build" "$project_root/.cache/go-mod" "$project_root/.cache/go-tmp"
export GOROOT=$($go_binary env GOROOT)
export GOCACHE="$project_root/.cache/go-build"
export GOMODCACHE="$project_root/.cache/go-mod"
export GOTMPDIR="$project_root/.cache/go-tmp"
export SIMQ_SECURE_FIXTURE_DIR="$fixture_dir"
export SIMQ_SECURE_COMPOSE_FILE="$compose_file"

compose() {
  docker compose -p "$project_name" -f "$compose_file" "$@"
}
cleanup() {
  compose ps --all >"$artifact_dir/compose-ps.txt" 2>&1 || true
  compose logs --no-color >"$artifact_dir/compose.log" 2>&1 || true
  if [ "$keep" != true ]; then
    compose down --volumes --remove-orphans || true
    case "$fixture_dir" in
      "$project_root/.cache/integration-secure/fixtures") rm -rf -- "$fixture_dir" ;;
      *) echo "refusing to delete unexpected fixture path: $fixture_dir" >&2 ;;
    esac
  fi
}
trap cleanup EXIT INT TERM

cd "$project_root"
compose down --volumes --remove-orphans
"$go_binary" run ./tests/integration/fixture generate -output "$fixture_dir"
compose config --quiet
compose up -d --build
"$go_binary" test -tags=secureintegration -count=1 -v ./tests/integration -run '^TestSecureMultiShardChaos$'
"$go_binary" test -tags=secureintegration -count=1 -v ./tests/integration -run '^TestDisasterRecoveryPrepare$'
compose logs --no-color >"$artifact_dir/before-backup.log"
compose stop n1 n2 n3

for node in n1 n2 n3; do
  volume_name="$project_name-$node-data"
  docker run --rm -v "$volume_name:/source:ro" -v "$backup_dir:/backup" alpine:3.23 sh -ec "cd /source && tar -czf /backup/$node.tgz ."
done
compose down --volumes --remove-orphans
for node in n1 n2 n3; do
  volume_name="$project_name-$node-data"
  docker volume create --label "com.docker.compose.project=$project_name" --label "com.docker.compose.volume=$node-data" "$volume_name" >/dev/null
  docker run --rm -v "$volume_name:/target" -v "$backup_dir:/backup:ro" alpine:3.23 sh -ec "cd /target && tar -xzf /backup/$node.tgz"
done
compose up -d --no-build
"$go_binary" test -tags=secureintegration -count=1 -v ./tests/integration -run '^TestDisasterRecoveryVerify$'
echo "secure chaos and disaster-recovery tests passed; diagnostics: $artifact_dir"
