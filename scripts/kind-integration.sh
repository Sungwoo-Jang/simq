#!/usr/bin/env sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cluster_name=simq-m10
context_name=kind-simq-m10
kind_version=v0.20.0
kind_checksum=513a7213d6d3332dd9ef27c24dab35e5ef10a04fa27274fe1c14d8a246493ded
kubectl_version=v1.27.3
kubectl_checksum=fba6c062e754a120bc8105cde1344de200452fe014a8759e06e4eec7ed258a09
node_image=kindest/node:v1.27.3@sha256:3966ac761ae0136263ffdb6cfd4db23ef8a83cba8a463690e98317add2c9ba72
tool_dir="$project_root/.cache/tools/kind/$kind_version"
kind_binary="$tool_dir/kind"
kubectl_binary="$tool_dir/kubectl"
artifact_dir="$project_root/.cache/integration-kind"
required_toolchain=go1.26.7
keep=${SIMQ_INTEGRATION_KEEP:-false}

if [ -x "$project_root/.cache/toolchains/$required_toolchain/go/bin/go" ]; then
  go_binary="$project_root/.cache/toolchains/$required_toolchain/go/bin/go"
else
  go_binary=$(command -v go)
fi
if [ "$($go_binary env GOVERSION)" != "$required_toolchain" ]; then
  echo "Go toolchain mismatch: kind integration requires $required_toolchain" >&2
  exit 1
fi
docker info >/dev/null
mkdir -p "$tool_dir" "$artifact_dir" "$project_root/.cache/go-build" "$project_root/.cache/go-mod" "$project_root/.cache/go-tmp"
if [ ! -x "$kind_binary" ]; then
  curl -fsSL "https://github.com/kubernetes-sigs/kind/releases/download/$kind_version/kind-linux-amd64" -o "$kind_binary"
  chmod +x "$kind_binary"
fi
printf '%s  %s\n' "$kind_checksum" "$kind_binary" | sha256sum -c -
if [ ! -x "$kubectl_binary" ]; then
  curl -fsSL "https://dl.k8s.io/release/$kubectl_version/bin/linux/amd64/kubectl" -o "$kubectl_binary"
  chmod +x "$kubectl_binary"
fi
printf '%s  %s\n' "$kubectl_checksum" "$kubectl_binary" | sha256sum -c -

export GOROOT=$($go_binary env GOROOT)
export GOCACHE="$project_root/.cache/go-build"
export GOMODCACHE="$project_root/.cache/go-mod"
export GOTMPDIR="$project_root/.cache/go-tmp"
export SIMQ_KIND_CONTEXT="$context_name"
export SIMQ_KUBECTL="$kubectl_binary"

completed=false

cleanup() {
  "$kubectl_binary" --context "$context_name" -n simq-m10 get all,pvc -o wide >"$artifact_dir/resources.txt" 2>&1 || true
  "$kubectl_binary" --context "$context_name" -n simq-m10 describe pods >"$artifact_dir/pods.txt" 2>&1 || true
  for pod in simq-0 simq-1 simq-2; do
    "$kubectl_binary" --context "$context_name" -n simq-m10 logs "$pod" --all-containers >"$artifact_dir/$pod.log" 2>&1 || true
  done
  printf 'status=%s\nkind=%s\nkubectl=%s\nnode_image=%s\n' "$completed" "$kind_version" "$kubectl_version" "$node_image" >"$artifact_dir/result.txt"
  if [ "$keep" != true ]; then
    "$kind_binary" delete cluster --name "$cluster_name" || true
  fi
}
trap cleanup EXIT INT TERM

cd "$project_root"
if "$kind_binary" get clusters | grep -Fx "$cluster_name" >/dev/null; then
  "$kind_binary" delete cluster --name "$cluster_name"
fi
docker build -t simq:kind .
"$kind_binary" create cluster --name "$cluster_name" --image "$node_image" --config deployments/kubernetes/kind/cluster.yml --wait 180s
"$kind_binary" load docker-image simq:kind --name "$cluster_name"
"$kubectl_binary" --context "$context_name" apply -f deployments/kubernetes/kind/simq.yml

deadline=$(( $(date +%s) + 180 ))
while [ "$("$kubectl_binary" --context "$context_name" -n simq-m10 get pods -l app=simq --field-selector=status.phase=Running -o name 2>/dev/null | wc -l | tr -d ' ')" -ne 3 ]; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "three SimQ Pods did not reach Running phase" >&2
    exit 1
  fi
  sleep 1
done
"$go_binary" test -tags=kindintegration -count=1 -v ./tests/integration -run '^TestKindStatefulSetLeaderReplacement$'
completed=true
echo "kind leader-replacement test passed; diagnostics: $artifact_dir"
