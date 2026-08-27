#!/usr/bin/env sh
set -eu

umask 077
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
runner="$project_root/scripts/secure-integration.sh"
source_dir="$project_root/.cache/integration-secure"
artifact_dir="$project_root/.cache/qualification/secure"
iterations=${SIMQ_SECURE_QUALIFICATION_ITERATIONS:-2}
iteration_timeout=${SIMQ_SECURE_QUALIFICATION_TIMEOUT:-10m}

case "$iterations" in
  ''|*[!0-9]*) echo 'SIMQ_SECURE_QUALIFICATION_ITERATIONS must be an integer from 1 through 10' >&2; exit 2 ;;
esac
if [ "$iterations" -lt 1 ] || [ "$iterations" -gt 10 ]; then
  echo 'SIMQ_SECURE_QUALIFICATION_ITERATIONS must be from 1 through 10' >&2
  exit 2
fi

case "$artifact_dir" in
  "$project_root/.cache/qualification/secure") rm -rf -- "$artifact_dir" ;;
  *) echo "refusing to reset unexpected qualification path: $artifact_dir" >&2; exit 2 ;;
esac
mkdir -p "$artifact_dir"

copy_diagnostics() {
  destination=$1
  mkdir -p "$destination"
  for name in before-backup.log compose.log compose-ps.txt; do
    if [ -f "$source_dir/$name" ]; then
      cp "$source_dir/$name" "$destination/$name"
    fi
  done
}

started=$(date +%s)
iteration=1
while [ "$iteration" -le "$iterations" ]; do
  destination="$artifact_dir/iteration-$iteration"
  iteration_started=$(date +%s)
  if timeout --signal=TERM --kill-after=30s "$iteration_timeout" "$runner"; then
    copy_diagnostics "$destination"
    duration=$(( $(date +%s) - iteration_started ))
    printf 'status=passed\nduration_seconds=%s\n' "$duration" >"$destination/result.txt"
  else
    status=$?
    copy_diagnostics "$destination"
    duration=$(( $(date +%s) - iteration_started ))
    printf 'status=failed\nexit_code=%s\nduration_seconds=%s\n' "$status" "$duration" >"$destination/result.txt"
    docker compose -p simq-secure-integration -f "$project_root/deployments/local/compose.secure-integration.yml" down --volumes --remove-orphans >/dev/null 2>&1 || true
    echo "secure qualification iteration $iteration failed with exit code $status" >&2
    exit "$status"
  fi
  iteration=$((iteration + 1))
done

duration=$(( $(date +%s) - started ))
cat >"$artifact_dir/summary.md" <<EOF
### Secure operational qualification

- Result: passed
- Iterations: $iterations
- Total duration: ${duration}s
- Per iteration: TLS 1.3/OIDC/two-shard partition, FIFO failover, and offline three-volume restore
EOF
echo "secure operational qualification passed $iterations iterations in ${duration}s"
