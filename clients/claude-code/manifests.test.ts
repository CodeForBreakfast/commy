import { expect, test } from 'bun:test'

import { PLUGIN_VERSION } from '@commy/mcp/mcp-server'
import { RELEASE_VERSION_SHAPE } from '../../scripts/release-detection.ts'
import pluginManifest from './.claude-plugin/plugin.json'
import mcpConfig from './.mcp.json'
import packageManifest from './package.json'

// pyproject.toml and uv.lock both record the PEP 440 normal form, which
// spells 0.24.0-rc.1 as 0.24.0rc1. A plain X.Y.Z has no '-rc.' to replace,
// so it passes through unchanged.
const toPep440 = (version: string): string => version.replace('-rc.', 'rc')

/**
 * Eight sites, one truth. `.claude-plugin/plugin.json` is what Claude
 * Code reads for plugin discovery; the plugin's `package.json` is the
 * Node artefact bun consumes for installs and scripts; the `mcp`
 * package's `package.json` is the universal MCP server's published
 * version; `mcp-server.ts` exports `PLUGIN_VERSION` for the MCP
 * `initialize` handshake; the Hermes adapter's `pyproject.toml` and
 * `commy/plugin.yaml` are what the pod's image build
 * pins its flake input to (the tag carrying this version is what
 * `CodeForBreakfast/hermes-agent` rebuilds against); the Hermes
 * `pyproject.toml` version and its `uv.lock` self-entry are both set by
 * `uv version <version>` (the hermes gate fails on a stale lock); the
 * `.mcp.json` launcher pins the published server at the same version,
 * so the plugin and the server it starts move as one artefact. When
 * any drifts, `claude plugin update`, MCP clients, or the pod image
 * see a stale version.
 *
 * Enforce parity at the unit-test bar so a partial bump can't silently
 * land again. The plugin lives in `clients/claude-code`; the `mcp`
 * package.json is across the workspace boundary, read via the same
 * `Bun.resolveSync` resolution the codebase already uses (mirrors
 * hooks-manifest.test.ts). The Hermes manifests live in the sibling
 * `clients/hermes` Python project — read as text and parsed via Bun's
 * native `TOML` / `YAML` (no new deps, no module-resolution coupling
 * to a non-TS package). `commy/plugin.yaml` and `.mcp.json` are
 * hand-edited; `pyproject.toml` and `uv.lock` are set together by
 * `uv version`, never hand-edited — see `docs/releasing.md` for the
 * worker release flow that drives all eight.
 */

const LAUNCHER_PIN = /^@codeforbreakfast\/commy-mcp@(.+)$/
const launcherPinnedVersion = mcpConfig.mcpServers['commy'].args
  .map((arg) => LAUNCHER_PIN.exec(arg)?.[1])
  .find((version) => version !== undefined)

const mcpPackageManifest = (await Bun.file(
  Bun.resolveSync('@commy/mcp/package.json', import.meta.dir),
).json()) as { readonly version: string }

const hermesPyproject = Bun.TOML.parse(
  await Bun.file(new URL('../hermes/pyproject.toml', import.meta.url)).text(),
) as { readonly project: { readonly version: string } }

const hermesPluginManifest = Bun.YAML.parse(
  await Bun.file(new URL('../hermes/commy/plugin.yaml', import.meta.url)).text(),
) as { readonly version: string }

const hermesLock = Bun.TOML.parse(
  await Bun.file(new URL('../hermes/uv.lock', import.meta.url)).text(),
) as { readonly package: ReadonlyArray<{ readonly name: string; readonly version: string }> }

const hermesLockSelfEntry = hermesLock.package.find((pkg) => pkg.name === 'commy-hermes')

test('plugin.json and the plugin package.json declare the same version', () => {
  expect(pluginManifest.version).toBe(packageManifest.version)
})

test('the mcp package.json version matches plugin.json', () => {
  expect(mcpPackageManifest.version).toBe(pluginManifest.version)
})

test('mcp-server.ts PLUGIN_VERSION matches plugin.json', () => {
  expect(PLUGIN_VERSION).toBe(pluginManifest.version)
})

test('the hermes pyproject.toml version matches plugin.json (uv version was run)', () => {
  expect(hermesPyproject.project.version).toBe(toPep440(pluginManifest.version))
})

test('the hermes plugin.yaml version matches plugin.json', () => {
  expect(hermesPluginManifest.version).toBe(pluginManifest.version)
})

test('the hermes uv.lock self-entry version matches plugin.json (uv version was run)', () => {
  expect(hermesLockSelfEntry?.version).toBe(toPep440(pluginManifest.version))
})

test('the .mcp.json launcher pins the published server at the plugin.json version', () => {
  expect(launcherPinnedVersion).toBe(pluginManifest.version)
})

test('plugin.json version matches the release version shape', () => {
  expect(pluginManifest.version).toMatch(RELEASE_VERSION_SHAPE)
})

test('plugin package.json version matches the release version shape', () => {
  expect(packageManifest.version).toMatch(RELEASE_VERSION_SHAPE)
})

test('mcp package.json version matches the release version shape', () => {
  expect(mcpPackageManifest.version).toMatch(RELEASE_VERSION_SHAPE)
})

test('mcp-server.ts PLUGIN_VERSION matches the release version shape', () => {
  expect(PLUGIN_VERSION).toMatch(RELEASE_VERSION_SHAPE)
})

test('hermes pyproject.toml version matches the release version shape', () => {
  expect(hermesPyproject.project.version).toMatch(RELEASE_VERSION_SHAPE)
})

test('hermes plugin.yaml version matches the release version shape', () => {
  expect(hermesPluginManifest.version).toMatch(RELEASE_VERSION_SHAPE)
})
