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

# The upper bound is load-bearing, not caution. `--no-deps` means the host
# Hermes is installed without its tree, so any dependency its import chain
# grows becomes a collection error here. 0.19.0 grew one (`requests`, reached
# via gateway -> agent.turn_context -> agent.model_metadata) and turned the
# gate red on a repo nothing had changed. Raise this bound deliberately, with
# the suite green, rather than letting PyPI's latest decide.
# Green against hermes-agent 0.18.2.
HERMES_SPEC="${HERMES_SPEC:-hermes-agent>=0.12,<0.19}"

uv venv --clear
uv sync --group dev
uv pip install --no-deps "${HERMES_SPEC}"
uv run --no-sync ruff check .
uv run --no-sync pytest -q
