import { expect, test } from 'bun:test'
import { decodeDisplayNameSync } from '@commy/core/ports'
import { type MemoryAdapter, memoryAdapter } from '@commy/memory/adapter'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import { Effect, Option, type Scope } from 'effect'
import type { ProjectSlug } from './bootstrap.ts'
import { sanitiseProjectSlug } from './bootstrap.ts'
import { createEphemeralIdentityCache } from './identity-cache.ts'
import { buildMcpServer } from './mcp-server.ts'
import { createNarrowSet } from './narrow-set.ts'
import { registerTools } from './tools.ts'

const slug = (raw: string): ProjectSlug => {
  const result = sanitiseProjectSlug(raw)
  if (Option.isNone(result)) throw new Error(`slug helper: sanitises to nothing: ${raw}`)
  return result.value
}

interface SessionRig {
  readonly client: Client
  readonly adapter: MemoryAdapter
}

const buildSessionRig = (
  options: {
    readonly idleReleaseMs?: number
    readonly projectForCwd?: (cwd: string | undefined) => Effect.Effect<ProjectSlug | undefined>
  } = {},
): Effect.Effect<SessionRig, never, Scope.Scope> =>
  Effect.gen(function* () {
    const idleReleaseMs = options.idleReleaseMs ?? 60 * 60 * 1000
    const adapter = yield* memoryAdapter()
    const identityCache = createEphemeralIdentityCache({
      acquire: adapter.identity.acquire,
      release: adapter.identity.release,
      idleReleaseMs,
    })
    const narrowSet = createNarrowSet()
    const server = buildMcpServer()
    const toolsCache = registerTools(server, {
      adapter,
      identityCache,
      narrowSet,
      ...(options.projectForCwd !== undefined ? { projectForCwd: options.projectForCwd } : {}),
    })
    toolsCache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))

    const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair()
    const client = new Client(
      { name: 'commy-session-test', version: '0.0.0' },
      { capabilities: {} },
    )
    yield* Effect.promise(() =>
      Promise.all([server.connect(serverTransport), client.connect(clientTransport)]),
    )
    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        await client.close()
        await server.close()
      }),
    )

    return { client, adapter }
  })

// UUID-shaped test session ids. The brand demands UUID format (comms-uqf);
// these are valid UUIDs whose leading 8 hex chars give readable bot-name
// suffixes (`cc-aaaaaaaa`, `cc-bbbbbbbb`, etc.).
const SID_A = 'aaaaaaaa-0000-4000-8000-000000000001'
const SID_B = 'bbbbbbbb-0000-4000-8000-000000000002'
const SID_X_PASSIVE = 'cccccccc-0000-4000-8000-000000000003'
const SID_FIRST_TIME = 'dddddddd-0000-4000-8000-000000000004'
const SID_CONTINUITY = 'c001c001-0000-4000-8000-000000000005'
const SID_PRE_CLEAR = 'c1eac1ea-0000-4000-8000-000000000006'

test('post with session_id mints cc-<sid-prefix> on the ephemeral cache', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildSessionRig()
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: 'home',
              body: 'first attribution from session A',
              session_id: SID_A,
            },
          }),
        )
        expect(result.isError).toBeFalsy()
        const current = yield* rig.adapter.identity.currentIdentity()
        expect(current.name).toBe(decodeDisplayNameSync('cc-aaaaaaaa'))
      }),
    ),
  ))

test('current_identity with session_id reads the bound entry passively', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildSessionRig()
        // First, an attribution call to mint.
        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: 'home',
              body: 'mint-trigger',
              session_id: SID_X_PASSIVE,
            },
          }),
        )

        const passive = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'current_identity',
            arguments: { session_id: SID_X_PASSIVE },
          }),
        )
        const sc = passive.structuredContent as {
          state: string
          identity: { name: string } | null
        }
        expect(sc.state).toBe('bound')
        expect(sc.identity?.name).toBe('cc-cccccccc')
      }),
    ),
  ))

test('current_identity for an unknown session_id returns unbound without minting', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildSessionRig()
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'current_identity',
            arguments: { session_id: SID_FIRST_TIME },
          }),
        )
        expect(result.structuredContent).toEqual({ state: 'unbound', identity: null })
        // Adapter must not have acquired anything from a passive call.
        const exit = yield* Effect.exit(rig.adapter.identity.currentIdentity())
        expect(exit._tag).toBe('Failure')
      }),
    ),
  ))

test('current_identity with a non-UUID session_id returns unbound (comms-uqf)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // Regression for the comms-uqf class of bug: a model-guessed session_id
        // string (or a non-CC client supplying a non-UUID like `homelab-iphone-...`)
        // must NOT mint a malformed `cc-<garbage>` identity. `readSessionId` now
        // validates UUID shape via the SessionId brand; non-UUID inputs route to
        // the unbound stub exactly as if session_id were missing.
        const rig = yield* buildSessionRig()
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'current_identity',
            arguments: { session_id: 'homelab-iphone-vpn-debug' },
          }),
        )
        expect(result.structuredContent).toEqual({ state: 'unbound', identity: null })
        const exit = yield* Effect.exit(rig.adapter.identity.currentIdentity())
        expect(exit._tag).toBe('Failure')
      }),
    ),
  ))

test('post with a non-UUID session_id is rejected — no malformed cc-* identity minted (comms-uqf)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // Same defence at the attribution-producing boundary. Without the brand,
        // `homelab-` would slice to suffix `homelab-` and mint `cc-<project>-homelab-`
        // — the exact symptom that produced `cc-homelab-homelab-` in the wild.
        const rig = yield* buildSessionRig()
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'post',
                arguments: {
                  channel_name: 'home',
                  body: 'will not be sent',
                  session_id: 'homelab-iphone-vpn-debug',
                },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        expect(error.message).toMatch(/session_id|ephemeral/i)
        // The unbound refusal now carries a discriminator (comms-spj3.17): the
        // tagged error's name reaches the MCP edge reshape, so the client sees
        // `UnboundEphemeralSession: …` rather than the bare message the old
        // `name === 'Error'` skip produced.
        expect(error.message).toContain('UnboundEphemeralSession')
        const exit = yield* Effect.exit(rig.adapter.identity.currentIdentity())
        expect(exit._tag).toBe('Failure')
      }),
    ),
  ))

test('post with a new session_id after one already bound releases the prior identity', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildSessionRig()
        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: 'home',
              body: 'session A first attribution',
              session_id: SID_A,
            },
          }),
        )
        const firstBound = yield* rig.adapter.identity.currentIdentity()
        expect(firstBound.name).toBe(decodeDisplayNameSync('cc-aaaaaaaa'))

        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: 'home',
              body: 'session B first attribution',
              session_id: SID_B,
            },
          }),
        )
        const secondBound = yield* rig.adapter.identity.currentIdentity()
        expect(secondBound.name).toBe(decodeDisplayNameSync('cc-bbbbbbbb'))
        // The prior identity must no longer be the active binding on the adapter.
        expect(secondBound.id).not.toBe(firstBound.id)
      }),
    ),
  ))

test('post without session_id rejects even when a prior session bound an identity (comms-67j)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // Tightened contract: an undefined session_id MUST NOT reuse the active
        // slot. Pre-tightening, a missing/dropped session_id post-/clear leaked
        // the previous conversation's identity into the new conversation. The
        // active slot stays put for legitimate continuation calls that DO pass
        // the original session_id.
        const rig = yield* buildSessionRig()
        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: 'home',
              body: 'first call WITH sid',
              session_id: SID_CONTINUITY,
            },
          }),
        )
        const before = yield* rig.adapter.identity.currentIdentity()
        expect(before.name).toBe(decodeDisplayNameSync('cc-c001c001'))

        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'post',
                arguments: { channel_name: 'home', body: 'follow-up WITHOUT sid' },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        expect(error.message).toMatch(/session_id|ephemeral/i)

        // The active slot survives the undefined-sid call — a subsequent
        // legitimate call with the original sid still reaches the same binding.
        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: 'home',
              body: 'follow-up WITH original sid',
              session_id: SID_CONTINUITY,
            },
          }),
        )
        const after = yield* rig.adapter.identity.currentIdentity()
        expect(after.id).toBe(before.id)
      }),
    ),
  ))

test('current_identity without session_id returns unbound, even when a prior session bound an identity (comms-67j)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // Regression for the /clear leak: after a session has bound an
        // identity, a current_identity call from a *different* conversation
        // whose hook fails to inject session_id must NOT surface the prior
        // session's seat. Reads unbound — the fresh conversation will mint
        // its own seat on its first attribution call.
        const rig = yield* buildSessionRig()
        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: 'home',
              body: 'pre-clear attribution',
              session_id: SID_PRE_CLEAR,
            },
          }),
        )
        const bound = yield* rig.adapter.identity.currentIdentity()
        expect(bound.name).toBe(decodeDisplayNameSync('cc-c1eac1ea'))

        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'current_identity',
            arguments: {},
          }),
        )
        expect(result.structuredContent).toEqual({ state: 'unbound', identity: null })
      }),
    ),
  ))

test('post without session_id and no prior binding rejects with an instructive error', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildSessionRig()
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'post',
                arguments: { channel_name: 'home', body: 'cold start, no hook' },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        expect(error.message).toMatch(/session_id|ephemeral/i)
      }),
    ),
  ))

test('post with session_id + cwd mints cc-<project>-<sid-prefix> derived from cwd (ass-v7b4)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildSessionRig({
          projectForCwd: (cwd) =>
            Effect.succeed(
              cwd === '/home/x/assistant'
                ? Option.getOrUndefined(sanitiseProjectSlug('assistant'))
                : undefined,
            ),
        })
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: 'home',
              body: 'first attribution from session with project',
              session_id: SID_A,
              cwd: '/home/x/assistant',
            },
          }),
        )
        expect(result.isError).toBeFalsy()
        const current = yield* rig.adapter.identity.currentIdentity()
        expect(current.name).toBe(decodeDisplayNameSync('cc-assistant-aaaaaaaa'))
      }),
    ),
  ))

test('two sessions in different cwds mint two different project prefixes (ass-v7b4)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // This is the bug reproducer: today, every minted name carries the
        // plugin's own location regardless of where the calling session is.
        // The fix routes the calling session's cwd through projectForCwd so
        // each session's minted name reflects its own project.
        const cwdToSlug: Record<string, ProjectSlug> = {
          '/home/x/brewlife': slug('brewlife'),
          '/home/x/homelab': slug('homelab'),
        }
        const rig = yield* buildSessionRig({
          projectForCwd: (cwd) => Effect.succeed(cwd === undefined ? undefined : cwdToSlug[cwd]),
        })
        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: 'home',
              body: 'brewlife session first attribution',
              session_id: 'b7e71ba4-0000-4000-8000-000000000007',
              cwd: '/home/x/brewlife',
            },
          }),
        )
        const brewBound = yield* rig.adapter.identity.currentIdentity()
        expect(brewBound.name).toBe(decodeDisplayNameSync('cc-brewlife-b7e71ba4'))

        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: 'home',
              body: 'homelab session first attribution',
              session_id: '40e1aaaa-0000-4000-8000-000000000008',
              cwd: '/home/x/homelab',
            },
          }),
        )
        const homeBound = yield* rig.adapter.identity.currentIdentity()
        expect(homeBound.name).toBe(decodeDisplayNameSync('cc-homelab-40e1aaaa'))
      }),
    ),
  ))

test('post with cwd from a non-project directory falls back to bare cc-<8> (ass-v7b4)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // projectForCwd returns undefined when cwd is not in a known repo.
        // The minted name must NOT inherit the plugin's own location.
        const rig = yield* buildSessionRig({ projectForCwd: () => Effect.succeed(undefined) })
        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: 'home',
              body: 'from a /tmp session',
              session_id: '7a9e5ee5-0000-4000-8000-000000000009',
              cwd: '/tmp',
            },
          }),
        )
        const current = yield* rig.adapter.identity.currentIdentity()
        expect(current.name).toBe(decodeDisplayNameSync('cc-7a9e5ee5'))
      }),
    ),
  ))
