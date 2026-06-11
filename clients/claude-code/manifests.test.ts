import { expect, test } from 'bun:test'

import { PLUGIN_VERSION } from '@commy/mcp/mcp-server'
import pluginManifest from './.claude-plugin/plugin.json'
import packageManifest from './package.json'

const SEMVER_SHAPE = /^\d+\.\d+\.\d+$/

/**
 * Six sites, one truth. `.claude-plugin/plugin.json` is what Claude
 * Code reads for plugin discovery; the plugin's `package.json` is the
 * Node artefact bun consumes for installs and scripts; the `mcp`
 * package's `package.json` is the universal MCP server's published
 * version; `mcp-server.ts` exports `PLUGIN_VERSION` for the MCP
 * `initialize` handshake; the Hermes adapter's `pyproject.toml` and
 * `commy/plugin.yaml` are what the homelab pod's image build
 * pins its flake input to (the tag carrying this version is what
 * `CodeForBreakfast/hermes-agent` rebuilds against). When any drifts,
 * `claude plugin update`, MCP clients, or the pod image see a stale
 * version — see journal `Agent Comms 08:21` (ass-220u follow-up:
 * 0.0.6 bump that initially missed plugin.json).
 *
 * Enforce parity at the unit-test bar so a partial bump can't silently
 * land again. The plugin lives in `clients/claude-code`; the `mcp`
 * package.json is across the workspace boundary, read via the same
 * `Bun.resolveSync` resolution the codebase already uses (mirrors
 * hooks-manifest.test.ts). The Hermes manifests live in the sibling
 * `clients/hermes` Python project — read as text and parsed via Bun's
 * native `TOML` / `YAML` (no new deps, no module-resolution coupling
 * to a non-TS package). Releases go through the `release-plugin`
 * skill, which edits all six sites together.
 */

const mcpPackageManifest = (await Bun.file(
  Bun.resolveSync('@commy/mcp/package.json', import.meta.dir),
).json()) as { readonly version: string }

const hermesPyproject = Bun.TOML.parse(
  await Bun.file(new URL('../hermes/pyproject.toml', import.meta.url)).text(),
) as { readonly project: { readonly version: string } }

const hermesPluginManifest = Bun.YAML.parse(
  await Bun.file(new URL('../hermes/commy/plugin.yaml', import.meta.url)).text(),
) as { readonly version: string }

test('plugin.json and the plugin package.json declare the same version', () => {
  expect(pluginManifest.version).toBe(packageManifest.version)
})

test('the mcp package.json version matches plugin.json', () => {
  expect(mcpPackageManifest.version).toBe(pluginManifest.version)
})

test('mcp-server.ts PLUGIN_VERSION matches plugin.json', () => {
  expect(PLUGIN_VERSION).toBe(pluginManifest.version)
})

test('the hermes pyproject.toml version matches plugin.json', () => {
  expect(hermesPyproject.project.version).toBe(pluginManifest.version)
})

test('the hermes plugin.yaml version matches plugin.json', () => {
  expect(hermesPluginManifest.version).toBe(pluginManifest.version)
})

test('plugin.json version matches semver MAJOR.MINOR.PATCH shape', () => {
  expect(pluginManifest.version).toMatch(SEMVER_SHAPE)
})

test('plugin package.json version matches semver MAJOR.MINOR.PATCH shape', () => {
  expect(packageManifest.version).toMatch(SEMVER_SHAPE)
})

test('mcp package.json version matches semver MAJOR.MINOR.PATCH shape', () => {
  expect(mcpPackageManifest.version).toMatch(SEMVER_SHAPE)
})

test('mcp-server.ts PLUGIN_VERSION matches semver MAJOR.MINOR.PATCH shape', () => {
  expect(PLUGIN_VERSION).toMatch(SEMVER_SHAPE)
})

test('hermes pyproject.toml version matches semver MAJOR.MINOR.PATCH shape', () => {
  expect(hermesPyproject.project.version).toMatch(SEMVER_SHAPE)
})

test('hermes plugin.yaml version matches semver MAJOR.MINOR.PATCH shape', () => {
  expect(hermesPluginManifest.version).toMatch(SEMVER_SHAPE)
})
