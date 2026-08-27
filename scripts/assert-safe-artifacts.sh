#!/usr/bin/env sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ "$#" -eq 0 ]; then
  echo 'usage: assert-safe-artifacts.sh PATH...' >&2
  exit 2
fi

pattern='-----BEGIN ([A-Z0-9]+ )?PRIVATE KEY-----|gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,}|SIMQ_[A-Z0-9_]*TOKEN=|SIMQ_ENCRYPTION_KEYS=|eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.'
for requested in "$@"; do
  case "$requested" in
    "$project_root/.cache/"*) path=$requested ;;
    .cache/*) path="$project_root/$requested" ;;
    *) echo "refusing to inspect artifact path outside .cache: $requested" >&2; exit 2 ;;
  esac
  if [ -e "$path" ] && grep -R -I -E -q -- "$pattern" "$path"; then
    echo "sensitive-value pattern detected below artifact path; upload is blocked: $requested" >&2
    exit 1
  fi
done
echo 'artifact safety scan passed'
