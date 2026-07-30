#!/bin/bash
set -euo pipefail

if ! command -v codex >/dev/null 2>&1; then
  echo "fatal: codex CLI is missing from the worker image" >&2
  exit 1
fi

echo "codex CLI ready: $(codex --version)"
caddy start --config /app/Caddyfile --adapter caddyfile
exec bun run /app/server.ts
