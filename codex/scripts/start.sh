#!/bin/sh
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ "$#" -lt 1 ]; then echo 'Usage: start.sh <state> [serve options]' >&2; exit 2; fi
state=$1; shift
# Model/control credentials must already be injected by the caller.
exec "${NWF_CODEX_HOST_EXECUTABLE:-$script_dir/../target/release/codex-native-worker}" serve --state "$state" "$@"
