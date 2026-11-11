#!/usr/bin/env bash
# office-demo one-click launcher (macOS/Linux). Same semantics as demo.cmd.
cd "$(dirname "$0")"

if [ -z "$MODEL_API_KEY" ] && [ -f .local/env.sh ]; then
  . .local/env.sh
fi
if [ -z "$MODEL_API_KEY" ]; then
  echo "[demo] MODEL_API_KEY is not set."
  echo "[demo] Export it, or create .local/env.sh that sets it, then run again."
  exit 1
fi

exec node runner.mjs
