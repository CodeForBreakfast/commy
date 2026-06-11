#!/usr/bin/env bash
# Invoke bun via the plugin's own Nix flake so PreToolUse hooks work in
# Claude Code sessions where bun is not on the host PATH (comms-f9n).
#
# On first run after install (or after `nix store --gc` evicted the
# pinned bun), this lazy-builds a GC-root symlink at
# $CLAUDE_PLUGIN_ROOT/.bun-result -> /nix/store/...-bun via the same flake
# the MCP server uses (path:$CLAUDE_PLUGIN_ROOT#default — comms-94r).
# Every call thereafter is a direct exec, equivalent to having bun on PATH.

set -euo pipefail

: "${CLAUDE_PLUGIN_ROOT:?CLAUDE_PLUGIN_ROOT must be set by Claude Code}"

RESULT="${CLAUDE_PLUGIN_ROOT}/.bun-result"

if [ ! -x "${RESULT}/bin/bun" ]; then
  nix build --out-link "${RESULT}" "path:${CLAUDE_PLUGIN_ROOT}" >&2
fi

exec "${RESULT}/bin/bun" "$@"
