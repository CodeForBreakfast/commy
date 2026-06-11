#!/bin/sh
# Launch the commy MCP server with bun on PATH — no Nix required (comms-ip4q).
#
# A consumer installs the plugin from the git marketplace; Claude Code clones
# the marketplace repo but installs no JS deps, so a fresh checkout has no
# node_modules and the workspace symlink `@commy/mcp` does not exist yet. This
# launcher stages the workspace deps once, idempotently, then execs the server.
#
# Two invariants this preserves:
#   - The server must be claude's DIRECT child so its stdin is claude's pipe
#     (comms-hfhm): the final `exec` replaces this shell, so bun inherits the
#     pid and the stdio — no grandchild, no orphaned ~200MB server on disconnect.
#   - The one-time stage must not race across concurrently-booting sessions
#     (comms-ae3: an unguarded connect-time `bun install` raced to EEXIST). A
#     portable mkdir mutex + a re-check inside the lock makes the stage safe
#     even if two sessions cold-start at once. `mkdir` (not `flock`) so the
#     guard works on macOS too, where util-linux `flock` is absent.
#
# Plain POSIX sh — no bashisms, integer `sleep` — so the launcher imposes no
# shell of our choosing on the consumer (dash/ash/busybox/bash all run it).
# The frozen marketplace copy ships node_modules pre-staged (publish-marketplace
# stages and installs the workspace), so fleet seats skip the install branch
# entirely and fall straight through to the exec.

set -eu

: "${CLAUDE_PLUGIN_ROOT:?CLAUDE_PLUGIN_ROOT must be set by Claude Code}"

ROOT="${CLAUDE_PLUGIN_ROOT}"
WORKSPACE="$(cd "${ROOT}/../.." && pwd)"
ENTRYPOINT="${ROOT}/node_modules/@commy/mcp/server.ts"

if [ ! -e "${ENTRYPOINT}" ]; then
  LOCK="${WORKSPACE}/.commy-install.lock"
  until mkdir "${LOCK}" 2>/dev/null; do sleep 1; done
  trap 'rmdir "${LOCK}" 2>/dev/null || true' EXIT
  if [ ! -e "${ENTRYPOINT}" ]; then
    (cd "${WORKSPACE}" && bun install --frozen-lockfile --ignore-scripts) >&2
  fi
  rmdir "${LOCK}"
  trap - EXIT
fi

exec bun --cwd="${ROOT}" node_modules/@commy/mcp/server.ts
