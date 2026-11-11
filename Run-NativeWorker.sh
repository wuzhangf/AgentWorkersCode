#!/bin/bash
# macOS entry point. Credentials are inherited from the caller's approved loader.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: Run-NativeWorker.sh --engine pi|codex --state DIR [options]
  --initialize --project GIT_ROOT [--project-id ID]
  --port PORT                         Default: an ephemeral loopback port
  --model MODEL --model-base-url URL   Or inherit MODEL_DEFAULT_MODEL/MODEL_BASE_URL
  --codex-executable FILE              Pinned app-server, required for Codex init
  --codex-host-executable FILE         Built Rust Worker host
  --codex-tool-profile PROFILE         macos-runner-v1 (default), macos-in-process-v1 or native
  --pi-execution-profile PROFILE       macos-runner-v1 (default), macos-in-process-v1 or native
  --command-runner FILE                Codex Runner main.mjs; defaults to the bundled service

Load credentials into the environment before calling this entry point.
WORKER_LOCAL_TOKEN must have at least 24 characters. No credential is written here.
EOF
}
fail() { printf '%s\n' "$1" >&2; exit 1; }
value() { [ "$#" -ge 2 ] && [ -n "$2" ] || fail "OPTION_VALUE_REQUIRED: $1"; }

engine='' state='' project='' project_id='project' port='0' initialize=0
codex_executable='' codex_host='' codex_tool_profile='macos-runner-v1' pi_execution_profile='macos-runner-v1' command_runner=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    --engine) value "$@"; engine="$2"; shift 2 ;;
    --state) value "$@"; state="$2"; shift 2 ;;
    --project) value "$@"; project="$2"; shift 2 ;;
    --project-id) value "$@"; project_id="$2"; shift 2 ;;
    --port) value "$@"; port="$2"; shift 2 ;;
    --model) value "$@"; export MODEL_DEFAULT_MODEL="$2"; shift 2 ;;
    --model-base-url) value "$@"; export MODEL_BASE_URL="$2"; shift 2 ;;
    --codex-executable) value "$@"; codex_executable="$2"; shift 2 ;;
    --codex-host-executable) value "$@"; codex_host="$2"; shift 2 ;;
    --codex-tool-profile) value "$@"; codex_tool_profile="$2"; shift 2 ;;
    --pi-execution-profile) value "$@"; pi_execution_profile="$2"; shift 2 ;;
    --command-runner) value "$@"; command_runner="$2"; shift 2 ;;
    --initialize) initialize=1; shift ;;
    --help|-h) usage; exit 0 ;;
    *) fail "UNKNOWN_OPTION: $1" ;;
  esac
done
[ "$(uname -s)" = 'Darwin' ] || fail 'MACOS_REQUIRED'
case "$engine" in pi|codex) ;; *) fail 'ENGINE_REQUIRED: pi or codex' ;; esac
case "$codex_tool_profile" in native|macos-in-process-v1|macos-runner-v1) ;; *) fail 'INVALID_CODEX_TOOL_PROFILE' ;; esac
case "$pi_execution_profile" in native|macos-in-process-v1|macos-runner-v1) ;; *) fail 'INVALID_PI_EXECUTION_PROFILE' ;; esac
[ -n "$state" ] || fail 'STATE_REQUIRED'
case "$port" in ''|*[!0-9]*) fail 'INVALID_PORT' ;; esac
[ "${#port}" -le 5 ] && [ "$port" -le 65535 ] || fail 'INVALID_PORT'
control_token="${WORKER_LOCAL_TOKEN:-}"
[ "${#control_token}" -ge 24 ] || fail 'WORKER_LOCAL_TOKEN_REQUIRED: minimum 24 characters'
unset control_token

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)"
if [ -z "$command_runner" ]; then command_runner="$script_dir/command-runner/main.mjs"; fi
if [ "$initialize" -eq 1 ]; then
  [ -n "$project" ] || fail 'INITIALIZE_REQUIRES_PROJECT'
  [ -n "${MODEL_DEFAULT_MODEL:-}" ] && [ -n "${MODEL_BASE_URL:-}" ] || fail 'EXPLICIT_MODEL_BINDING_REQUIRED'
  for marker in registry.json ledger.sqlite worker.sqlite; do
    [ ! -e "$state/$marker" ] || fail 'LOCAL_STATE_ALREADY_INITIALIZED: start without --initialize'
  done
fi

if [ "$engine" = 'pi' ]; then
  native_node="$(command -v node)" || fail 'NODE_REQUIRED'
  native_main="$script_dir/pi/dist/host/main.js"
  [ -f "$native_main" ] || fail 'BUILD_PI_FIRST: npm ci && npm run build in workers/pi'
  if [ "$initialize" -eq 1 ]; then
    "$native_node" "$native_main" init --state "$state" --project "$project" --project-id "$project_id" --execution-profile "$pi_execution_profile"
  fi
  exec "$native_node" "$native_main" serve --state "$state" --port "$port" --token-env WORKER_LOCAL_TOKEN --execution-profile "$pi_execution_profile"
fi

if [ -z "$codex_host" ]; then
  for candidate in "$script_dir/codex/target/release/codex-native-worker" "$script_dir/codex/target/debug/codex-native-worker"; do
    if [ -x "$candidate" ]; then codex_host="$candidate"; break; fi
  done
fi
[ -n "$codex_host" ] && [ -x "$codex_host" ] || fail 'BUILD_CODEX_HOST_FIRST: or supply --codex-host-executable'
if [ "$initialize" -eq 1 ]; then
  [ -n "$codex_executable" ] && [ -x "$codex_executable" ] || fail 'PINNED_CODEX_EXECUTABLE_REQUIRED'
  init_args=(init --state "$state" --executable "$codex_executable" --project "$project" --project-id "$project_id" --model "$MODEL_DEFAULT_MODEL" --base-url "$MODEL_BASE_URL" --tool-profile "$codex_tool_profile")
  if [ "$codex_tool_profile" = 'macos-runner-v1' ]; then
    [ -f "$command_runner" ] || fail 'COMMAND_RUNNER_REQUIRED'
    init_args+=(--command-runner "$command_runner")
  fi
  "$codex_host" "${init_args[@]}"
fi
exec "$codex_host" serve --state "$state" --port "$port" --token-env WORKER_LOCAL_TOKEN
