import { expect, test } from 'bun:test'

import mcpConfig from './.mcp.json'

/**
 * The plugin launches its stdio MCP server via `npx @codeforbreakfast/commy-mcp`
 * (comms-iw8w.5) — node-only, no bun on the consumer's PATH. The published npm
 * package carries the `@codeforbreakfast` scope because `@commy` is taken on npm
 * (commy-naming-codeforbreakfast-only-on-clash); everything else stays `commy`.
 *
 * Two contracts these tests pin:
 *
 *  - `cwd` is `${CLAUDE_PLUGIN_ROOT}`. npx resolves a package from its cwd's
 *    `node_modules` (walking up) before the registry, so fleet seats whose
 *    frozen marketplace stages the bundle there run their LOCAL copy with zero
 *    registry hits (the comms-2mx local-run guarantee), while a consumer with no
 *    such local install resolves the published build from npm. The name on the
 *    command line is BARE — no `@<version>` — because a command-line version pin
 *    forces a registry round-trip and defeats the local override.
 *
 *  - The server must be claude's child with claude's pipe as its stdin so it
 *    exits when claude disconnects (comms-hfhm orphan-leak). npx does not
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
  // override fleet seats rely on — the version pin lives in the staged
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
  ])
})
