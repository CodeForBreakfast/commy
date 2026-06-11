#!/usr/bin/env bash
#
# Build the isolated test environment for the commy Hermes adapter and
# run lint + the faithful registration test.
#
# `hermes-agent` is installed with `--no-deps`: the host Hermes provides it at
# pod runtime, so we exercise the real registration machinery without pulling
# its dependency tree (anthropic, openai, firecrawl, edge-tts, ...). The
# registration import path only needs pyyaml beyond stdlib.
#
# Requires `uv` on PATH.
set -euo pipefail

cd "$(dirname "$0")/.."

# Tested against hermes-agent 0.15.2 (PyPI latest). Pin to the confirmed pod
# Hermes version once the image lane is pinned.
HERMES_SPEC="${HERMES_SPEC:-hermes-agent>=0.12,<1}"

uv venv --clear
uv sync --group dev
uv pip install --no-deps "${HERMES_SPEC}"
uv run --no-sync ruff check .
uv run --no-sync pytest -q
