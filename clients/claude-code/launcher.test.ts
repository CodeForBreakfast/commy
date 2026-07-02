import { expect, test } from 'bun:test'

import pluginManifest from './.claude-plugin/plugin.json'
import mcpConfig from './.mcp.json'

/**
 * The plugin launches its stdio MCP server via `npx @codeforbreakfast/commy-mcp`
 * — node-only, no bun on the consumer's PATH. The published npm
 * package carries the `@codeforbreakfast` scope because `@commy` is taken on npm;
 * everything else stays `commy`.
 *
 * Two contracts these tests pin:
 *
 *  - `cwd` is `${CLAUDE_PLUGIN_ROOT}`. npx resolves a package from its cwd's
 *    `node_modules` (walking up) before the registry, so an install whose
 *    frozen marketplace stages the bundle there runs its local copy with zero
 *    registry hits (the local-run guarantee), while a consumer with no
 *    such local install resolves the published build from npm. The name on the
 *    command line is bare — no `@<version>` — because a command-line version pin
 *    forces a registry round-trip and defeats the local override.
 *
 *  - The server must be claude's child with claude's pipe as its stdin so it
 *    exits when claude disconnects (orphan-leak). npx does not
 *    exec-through, but it inherits stdio to the server, so claude's pipe reaches
 *    the server directly; the runtime proof that the server exits on that pipe's
 *    EOF lives in `packages/mcp/disconnect-exit.test.ts`.
 */

const commyServer = mcpConfig.mcpServers['commy']
const configText = JSON.stringify(mcpConfig)

test('the launcher runs the published bundle via npx — no bun, no nix, no launch.sh', () => {
  expect(commyServer.command).toBe('npx')
  expect(configText).not.toContain('launch.sh')
  expect(configText).not.toContain('nix')
  expect(configText).not.toMatch(/\bbun\b/)
})

test('npx is pointed at the bare @codeforbreakfast/commy-mcp package — no version pin on the command line', () => {
  expect(commyServer.args).toContain('-y')
  expect(commyServer.args).toContain('@codeforbreakfast/commy-mcp')
  // A `@<version>` suffix would force a registry hit and defeat the local
  // override such installs rely on — the version pin lives in the staged
  // dependency, never on the command line.
  const pinned = commyServer.args.find((arg) => /@codeforbreakfast\/commy-mcp@/.test(arg))
  expect(pinned).toBeUndefined()
})

test('cwd anchors npx resolution at the plugin root so the local override wins', () => {
  expect(commyServer.cwd).toBe('${CLAUDE_PLUGIN_ROOT}')
})

test('the realm credentials and subscription env are still threaded through', () => {
  expect(Object.keys(commyServer.env)).toEqual([
    'ZULIP_SITE',
    'ZULIP_MINTER_EMAIL',
    'ZULIP_MINTER_API_KEY',
    'COMMY_SUBSCRIBE',
    'COMMY_CATCHUP_WINDOW_SECONDS',
    'npm_config_min_release_age',
  ])
})

/**
 * npm's `min-release-age` soaks a freshly-published release so a compromised
 * one can be caught before a consumer auto-pulls it — it guards the publisher's
 * own not-yet-vetted code. Waiving it is therefore a trust decision about the
 * publisher, which the publisher itself can safely make (no detection window is
 * needed for code you authored and released). The launcher exposes an optional
 * knob for that: COMMY_NPM_MIN_RELEASE_AGE=0 waives the soak for commy;
 * everyone else leaves it unset. commy ships as a zero-dependency bundle
 * (scripts/assemble-npm-package.ts inlines every dep into one server.js), so the
 * waiver is scoped to exactly this one publisher — no transitive tree rides
 * along — and the consumer otherwise can't install a fresh release until it ages
 * past their window (ENOVERSIONS).
 *
 * The safety property is that an unset knob must
 * not weaken the consumer's own setting: an empty/absent env var leaves the
 * consumer's `.npmrc min-release-age` untouched (npm ignores an empty env var
 * rather than overriding the file), and only an explicit `0` waives the soak.
 * That hinges on the userConfig being OPTIONAL with no default, so an
 * unconfigured consumer threads nothing.
 */
test('the npm release-age knob is threaded from an optional userConfig, off by default', () => {
  expect(commyServer.env.npm_config_min_release_age).toBe(
    `\${user_config.COMMY_NPM_MIN_RELEASE_AGE}`,
  )
  expect(pluginManifest.userConfig.COMMY_NPM_MIN_RELEASE_AGE.required).toBe(false)
  expect(pluginManifest.userConfig.COMMY_NPM_MIN_RELEASE_AGE).not.toHaveProperty('default')
})
