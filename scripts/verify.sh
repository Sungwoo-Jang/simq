#!/bin/sh

set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$project_root"

export GOCACHE="$project_root/.cache/go-build"
export GOTMPDIR="$project_root/.cache/go-tmp"
build_output="$project_root/.cache/bin/simq"
mkdir -p "$GOCACHE" "$GOTMPDIR" "$(dirname -- "$build_output")"

required_toolchain=go1.26.7
toolchain_root=$(GOTOOLCHAIN="$required_toolchain" go env GOROOT)
go_binary="$toolchain_root/bin/go"
gofmt_binary="$toolchain_root/bin/gofmt"

actual_toolchain=$($go_binary env GOVERSION)
if [ "$actual_toolchain" != "$required_toolchain" ]; then
	printf 'Go toolchain mismatch: got %s, want %s\n' "$actual_toolchain" "$required_toolchain" >&2
	exit 1
fi

unformatted=$(find . -type f -name '*.go' -not -path './vendor/*' -exec "$gofmt_binary" -l {} +)
if [ -n "$unformatted" ]; then
	printf 'unformatted Go files:\n%s\n' "$unformatted" >&2
	exit 1
fi

printf 'using %s\n' "$($go_binary version)"
$go_binary vet ./...
$go_binary test -count=1 ./cmd/simq -run '^TestSIGKILLRestartRecoversHTTPState$'
$go_binary test ./...
$go_binary test -race ./...
$go_binary build -buildvcs=false -o "$build_output" ./cmd/simq
