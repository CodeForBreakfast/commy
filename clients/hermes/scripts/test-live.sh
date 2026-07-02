#!/usr/bin/env bash
#
# Run the live end-to-end suite for the commy Hermes adapter against the
# REAL Zulip realm. This is the Python mirror of the TS
# `test:live` script: the default `scripts/test.sh` excludes `-m live`, this
# one targets it explicitly.
#
# Builds the same isolated env as `test.sh` (hermes-agent + mcp via the dev
# group), then runs only the `live`-marked tests. The suite is also env-gated
# inside the file — without ZULIP_SITE / ZULIP_MINTER_* / ZULIP_LIVE_CHANNEL_NAME
# it skips silently.
#
# The live tests spawn `bun packages/mcp/server.ts`, so `bun` must be on PATH
# and the commy checkout (COMMY_SERVER_DIR, default: repo root) must
# have `node_modules` (`bun install`). Requires `uv` on PATH.
set -euo pipefail

cd "$(dirname "$0")/.."

HERMES_SPEC="${HERMES_SPEC:-hermes-agent>=0.12,<1}"

uv venv --clear
uv sync --group dev
uv pip install --no-deps "${HERMES_SPEC}"
uv run --no-sync ruff check .
uv run --no-sync pytest -q -m live
