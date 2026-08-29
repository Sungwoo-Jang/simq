#!/usr/bin/env sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose_file="$project_root/deployments/local/compose.integration.yml"
project_name=simq-integration
artifact_dir="$project_root/.cache/sqs-poc"
report_path="$artifact_dir/simq-report.json"
required_toolchain=go1.26.7
keep=${SIMQ_SQS_POC_KEEP:-false}

if [ -x "$project_root/.cache/toolchains/$required_toolchain/go/bin/go" ]; then
  go_binary="$project_root/.cache/toolchains/$required_toolchain/go/bin/go"
else
  go_binary=$(command -v go)
fi
if [ "$($go_binary env GOVERSION)" != "$required_toolchain" ]; then
  echo "Go toolchain mismatch: SQS PoC requires $required_toolchain" >&2
  exit 1
fi
docker info >/dev/null
docker compose version >/dev/null

mkdir -p "$artifact_dir" "$project_root/.cache/go-build" "$project_root/.cache/go-mod" "$project_root/.cache/go-tmp"
export GOROOT=$($go_binary env GOROOT)
export GOCACHE="$project_root/.cache/go-build"
export GOMODCACHE="$project_root/.cache/go-mod"
export GOTMPDIR="$project_root/.cache/go-tmp"

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

deadline=$(( $(date +%s) + 120 ))
while :; do
  healthy=true
  for port in 19324 19325 19326; do
    if ! curl --fail --silent --show-error --max-time 2 "http://127.0.0.1:$port/healthz" >/dev/null; then
      healthy=false
    fi
  done
  [ "$healthy" = true ] && break
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo 'the three-node PoC cluster did not become healthy' >&2
    exit 1
  fi
  sleep 1
done

cd "$project_root/poc/sqssemantics"
"$go_binary" run ./cmd/sqs-poc --backend simq \
  --simq-endpoints http://127.0.0.1:19324,http://127.0.0.1:19325,http://127.0.0.1:19326 \
  >"$report_path"

"$project_root/scripts/assert-safe-artifacts.sh" .cache/sqs-poc
echo "SQS semantics PoC passed; redacted report: $report_path"
