#!/bin/sh
set -eu
# Toolchain may be isolated with CARGO_HOME/RUSTUP_HOME and PATH; no installation.
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ "$(uname -s)" != Darwin ] || [ "$(uname -m)" != arm64 ]; then
  echo ENGINE_PLATFORM_UNQUALIFIED >&2; exit 1
fi
command=build
release=true
if [ "${1:-}" = --test ]; then command=test; release=false; shift; fi
if [ "${1:-}" = --release ]; then shift; fi
if [ "${1:-}" = --debug ]; then release=false; shift; fi
if [ "$release" = true ]; then set -- --release "$@"; fi
exec "${CARGO:-cargo}" "$command" --locked --manifest-path "$script_dir/../Cargo.toml" "$@"
