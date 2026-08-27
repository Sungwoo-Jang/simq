#!/usr/bin/env sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
image=prom/prometheus@sha256:69f5241418838263316593f7274a304b095c40bcf22e57272865da91bd60a8ac

docker info >/dev/null
docker run --rm --entrypoint /bin/promtool -v "$project_root:/work:ro" -w /work/monitoring/prometheus "$image" test rules simq-alerts.test.yml
