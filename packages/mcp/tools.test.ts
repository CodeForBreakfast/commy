import { expect, test } from 'bun:test'
import { rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  decodeBotNameSync,
  decodeChannelNameSync,
  decodeMessageBodySync,
  decodeThreadNameSync,
  HistoryError,
  type IdentityError,
  type UnknownIdentity,
} from '@commy/core/ports'
import { type MemoryAdapter, memoryAdapter } from '@commy/memory/adapter'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import { Effect, type Scope } from 'effect'
import type { EnsureBound } from './ensure-bound.ts'
import { createEnsureBound } from './ensure-bound.ts'
import type { IdentityCache } from './identity-cache.ts'
import { createSingleIdentityCache } from './identity-cache.ts'
import { buildMcpServer } from './mcp-server.ts'
import type { NarrowSet } from './narrow-set.ts'
import { createNarrowSet } from './narrow-set.ts'
import type { ToolsCache } from './tools.ts'
import { registerTools } from './tools.ts'

type MemoryAdapterRig = MemoryAdapter

const buildDeps = (
  adapter: MemoryAdapterRig,
): Effect.Effect<{
  ensureBound: EnsureBound<UnknownIdentity | IdentityError>
  identityCache: IdentityCache
  narrowSet: NarrowSet
}> =>
  createEnsureBound({
    acquire: adapter.identity.acquire,
    name: decodeBotNameSync('test-bot'),
  }).pipe(
    Effect.map((ensureBound) => ({
      ensureBound,
      identityCache: createSingleIdentityCache({ ensureBound }),
      narrowSet: createNarrowSet(),
    })),
  )

interface ConnectedRig {
  readonly adapter: MemoryAdapterRig
  readonly ensureBound: EnsureBound<UnknownIdentity | IdentityError>
  readonly narrowSet: NarrowSet
  readonly cache: ToolsCache
  readonly client: Client
}

type ExtraToolDeps = Partial<{
  downloadFile: (
    urlPath: string,
  ) => Effect.Effect<{ filePath: string; contentType: string; size: number }>
  upload: (path: string) => Effect.Effect<{ reference: string; filename: string; size: number }>
  canEditMessages: boolean
}>

/**
 * Mount server + client around an already-built adapter and deps. Used by
 * withRig / withRigAndCache and by bespoke tests that monkey-patch the
 * adapter before mounting. Releases client+server when the scope closes.
 */
const mountAndConnect = (
  adapter: MemoryAdapterRig,
  deps: {
    identityCache: IdentityCache
    narrowSet: NarrowSet
    ensureBound: EnsureBound<UnknownIdentity | IdentityError>
  },
  extra: ExtraToolDeps = {},
): Effect.Effect<ConnectedRig, never, Scope.Scope> =>
  Effect.gen(function* () {
    const server = buildMcpServer()
    const cache = registerTools(server, {
      adapter,
      identityCache: deps.identityCache,
      narrowSet: deps.narrowSet,
      ...extra,
    })
    const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair()
    const client = new Client({ name: 'commy-tools-test', version: '0.0.0' }, { capabilities: {} })
    yield* Effect.promise(() =>
      Promise.all([server.connect(serverTransport), client.connect(clientTransport)]),
    )
    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        await client.close()
        await server.close()
      }),
    )
    return {
      adapter,
      ensureBound: deps.ensureBound,
      narrowSet: deps.narrowSet,
      cache,
      client,
    }
  })

const withRig = <E>(
  setup: (
    adapter: MemoryAdapterRig,
    ensureBound: EnsureBound<UnknownIdentity | IdentityError>,
  ) => Effect.Effect<void, E>,
): Effect.Effect<ConnectedRig, E, Scope.Scope> =>
  Effect.gen(function* () {
    const adapter = yield* memoryAdapter()
    const deps = yield* buildDeps(adapter)
    yield* setup(adapter, deps.ensureBound)
    return yield* mountAndConnect(adapter, deps)
  })

const withRigAndCache = <E>(
  setup: (
    adapter: MemoryAdapterRig,
    cache: ToolsCache,
    ensureBound: EnsureBound<UnknownIdentity | IdentityError>,
  ) => Effect.Effect<void, E>,
): Effect.Effect<ConnectedRig, E, Scope.Scope> =>
  Effect.gen(function* () {
    const adapter = yield* memoryAdapter()
    const deps = yield* buildDeps(adapter)
    const rig = yield* mountAndConnect(adapter, deps)
    yield* setup(adapter, rig.cache, deps.ensureBound)
    return rig
  })

test('tools/list advertises current_identity with optional session_id', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((_adapter, ensureBound) => ensureBound().pipe(Effect.asVoid))
        const result = yield* Effect.promise(() => rig.client.listTools())
        const tool = result.tools.find((t) => t.name === 'current_identity')
        expect(tool).toBeDefined()
        expect(tool?.description).toBeTruthy()
        expect(tool?.inputSchema).toMatchObject({
          type: 'object',
          properties: {
            session_id: { type: 'string', description: expect.any(String) },
          },
          additionalProperties: false,
        })
        // session_id is optional (not in required[]).
        const inputSchema = tool?.inputSchema as { required?: ReadonlyArray<string> }
        expect(inputSchema.required ?? []).not.toContain('session_id')
      }),
    ),
  ))

test('tools/list advertises edit_message when the substrate allows editing', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((_adapter, ensureBound) => ensureBound().pipe(Effect.asVoid))
        const result = yield* Effect.promise(() => rig.client.listTools())
        expect(result.tools.map((t) => t.name)).toContain('edit_message')
      }),
    ),
  ))

test('tools/list omits edit_message when the substrate has editing disabled', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* memoryAdapter()
        const deps = yield* buildDeps(adapter)
        const rig = yield* mountAndConnect(adapter, deps, { canEditMessages: false })
        const result = yield* Effect.promise(() => rig.client.listTools())
        expect(result.tools.map((t) => t.name)).not.toContain('edit_message')
        // Only edit_message is gated — the rest of the surface is untouched.
        expect(result.tools.map((t) => t.name)).toContain('post')
      }),
    ),
  ))

test('a gated-off edit_message is not callable through the back door', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* memoryAdapter()
        const deps = yield* buildDeps(adapter)
        const rig = yield* mountAndConnect(adapter, deps, { canEditMessages: false })
        // Withheld from the list means withheld from dispatch too: a client
        // that calls it anyway (stale list, hand-written request) is refused
        // by the protocol as an unknown tool, not handed to the adapter.
        const outcome = yield* Effect.promise(() =>
          rig.client
            .callTool({ name: 'edit_message', arguments: { message_id: '1', body: 'nope' } })
            .then(() => 'dispatched' as const)
            .catch((error: unknown) => (error instanceof Error ? error.message : String(error))),
        )
        expect(outcome).not.toBe('dispatched')
        expect(outcome).toContain('edit_message')
      }),
    ),
  ))

/**
 * The boot-time sample is a snapshot, so the tool list has to be able to
 * move after connect — otherwise a seat that connected while editing was
 * on carries a tool that cannot work until it restarts, and a seat that
 * connected while it was off never regains one that now can.
 */
test('setEditingAvailable withdraws edit_message from an already-connected session', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* memoryAdapter()
        const deps = yield* buildDeps(adapter)
        const rig = yield* mountAndConnect(adapter, deps, { canEditMessages: true })
        const before = yield* Effect.promise(() => rig.client.listTools())
        expect(before.tools.map((t) => t.name)).toContain('edit_message')

        rig.cache.setEditingAvailable(false)

        const after = yield* Effect.promise(() => rig.client.listTools())
        expect(after.tools.map((t) => t.name)).not.toContain('edit_message')
        expect(after.tools.map((t) => t.name)).toContain('post')
      }),
    ),
  ))

test('setEditingAvailable restores edit_message when the realm switches it back on', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* memoryAdapter()
        const deps = yield* buildDeps(adapter)
        const rig = yield* mountAndConnect(adapter, deps, { canEditMessages: false })
        expect(
          (yield* Effect.promise(() => rig.client.listTools())).tools.map((t) => t.name),
        ).not.toContain('edit_message')

        rig.cache.setEditingAvailable(true)

        expect(
          (yield* Effect.promise(() => rig.client.listTools())).tools.map((t) => t.name),
        ).toContain('edit_message')
      }),
    ),
  ))

/**
 * Dispatch reads the same source as the list. A tool withdrawn after
 * connect must stop being callable, or the withdrawal is cosmetic and a
 * client working from its pre-change list still reaches the adapter.
 */
test('a tool withdrawn after connect is no longer callable', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* memoryAdapter()
        const deps = yield* buildDeps(adapter)
        const rig = yield* mountAndConnect(adapter, deps, { canEditMessages: true })

        rig.cache.setEditingAvailable(false)

        const outcome = yield* Effect.promise(() =>
          rig.client
            .callTool({ name: 'edit_message', arguments: { message_id: '1', body: 'nope' } })
            .then(() => 'dispatched' as const)
            .catch((error: unknown) => (error instanceof Error ? error.message : String(error))),
        )
        expect(outcome).not.toBe('dispatched')
        expect(outcome).toContain('edit_message')
      }),
    ),
  ))

test('current_identity returns the bound identity after ensureBound resolves', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((_adapter, ensureBound) => ensureBound().pipe(Effect.asVoid))
        const expected = yield* rig.adapter.identity.currentIdentity()
        const result = yield* Effect.promise(() =>
          rig.client.callTool({ name: 'current_identity', arguments: {} }),
        )
        expect(result.isError).toBeFalsy()
        expect(result.structuredContent).toEqual({
          state: 'bound',
          identity: {
            id: expected.id,
            name: expected.name,
            kind: expected.kind,
          },
          recent_threads: [],
        })
      }),
    ),
  ))

test('current_identity before acquire returns the unbound sentinel (passive, never triggers acquire)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig(() => Effect.void)
        const result = yield* Effect.promise(() =>
          rig.client.callTool({ name: 'current_identity', arguments: {} }),
        )
        expect(result.isError).toBeFalsy()
        expect(result.structuredContent).toEqual({ state: 'unbound', identity: null })
        // The passive accessor must not have triggered acquire.
        expect(rig.ensureBound.current()).toBeUndefined()
      }),
    ),
  ))

test('current_identity flips to bound after a post triggers ensureBound', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache) =>
          Effect.gen(function* () {
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        const before = yield* Effect.promise(() =>
          rig.client.callTool({ name: 'current_identity', arguments: {} }),
        )
        expect(before.structuredContent).toEqual({ state: 'unbound', identity: null })

        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'first attribution' },
          }),
        )

        const after = yield* Effect.promise(() =>
          rig.client.callTool({ name: 'current_identity', arguments: {} }),
        )
        const sc = after.structuredContent as {
          state: string
          identity: { id: string; name: string; kind: string } | null
        }
        expect(sc.state).toBe('bound')
        expect(sc.identity?.name).toBe('test-bot')
      }),
    ),
  ))

test('current_identity includes recent_threads showing where the bot posted', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            cache.rememberChannel(yield* adapter.seedChannel('project-x').pipe(Effect.orDie))
            yield* ensureBound()
            yield* adapter.publisher.post(
              decodeChannelNameSync('project-x'),
              decodeMessageBodySync('first response'),
              { thread: decodeThreadNameSync('bug-report') },
            )
            yield* adapter.publisher.post(
              decodeChannelNameSync('project-x'),
              decodeMessageBodySync('second response'),
              { thread: decodeThreadNameSync('feature-request') },
            )
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({ name: 'current_identity', arguments: {} }),
        )
        expect(result.isError).toBeFalsy()
        const sc = result.structuredContent as {
          state: string
          identity: unknown
          recent_threads: ReadonlyArray<{
            channel: string
            thread: string
            last_post_ts: number
            last_post_body: string
          }>
        }
        expect(sc.recent_threads).toBeDefined()
        expect(sc.recent_threads).toHaveLength(2)
        const [first, second] = sc.recent_threads
        expect(first?.channel).toBe('project-x')
        expect(first?.thread).toBe('feature-request')
        expect(first?.last_post_body).toBe('second response')
        expect(second?.thread).toBe('bug-report')
        expect(second?.last_post_body).toBe('first response')
      }),
    ),
  ))

test('current_identity fails soft when the recent_threads enrichment throws — still returns bound', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const base = yield* memoryAdapter()
        const adapter: MemoryAdapterRig = {
          ...base,
          history: {
            ...base.history,
            recentThreads: () =>
              Effect.fail(
                new HistoryError({
                  operation: 'recentThreads',
                  cause: new Error('ZulipApiError: unknown user 473'),
                }),
              ),
          },
        }
        const deps = yield* buildDeps(adapter)
        yield* deps.ensureBound()
        const rig = yield* mountAndConnect(adapter, deps)

        const result = yield* Effect.promise(() =>
          rig.client.callTool({ name: 'current_identity', arguments: {} }),
        )
        expect(result.isError).toBeFalsy()
        const sc = result.structuredContent as {
          state: string
          identity: { id: string; name: string; kind: string } | null
          recent_threads?: unknown
        }
        expect(sc.state).toBe('bound')
        expect(sc.identity?.name).toBe('test-bot')
        expect(sc.recent_threads).toBeUndefined()
      }),
    ),
  ))

test('calling an unknown tool surfaces a protocol error', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((_adapter, ensureBound) => ensureBound().pipe(Effect.asVoid))
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () => rig.client.callTool({ name: 'no_such_tool', arguments: {} }),
            catch: (e) => e as { message: string },
          }),
        )
        expect(error.message).toContain('no_such_tool')
      }),
    ),
  ))

test('tools/list advertises resolve with a name argument', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((_adapter, ensureBound) => ensureBound().pipe(Effect.asVoid))
        const result = yield* Effect.promise(() => rig.client.listTools())
        const tool = result.tools.find((t) => t.name === 'resolve')
        expect(tool).toBeDefined()
        expect(tool?.inputSchema).toEqual({
          type: 'object',
          properties: { name: { type: 'string', description: expect.any(String) } },
          required: ['name'],
          additionalProperties: false,
        })
      }),
    ),
  ))

test('resolve returns the matching identity by name', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((adapter, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            yield* adapter.seedAgent('peer-bot').pipe(Effect.orDie)
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'resolve',
            arguments: { name: 'peer-bot' },
          }),
        )
        expect(result.isError).toBeFalsy()
        expect(result.structuredContent).toMatchObject({
          identity: { name: 'peer-bot', kind: 'agent' },
        })
      }),
    ),
  ))

test('resolve returns identity=null when nothing matches', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((_adapter, ensureBound) => ensureBound().pipe(Effect.asVoid))
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'resolve',
            arguments: { name: 'no-such-name' },
          }),
        )
        expect(result.isError).toBeFalsy()
        expect(result.structuredContent).toEqual({ identity: null })
      }),
    ),
  ))

test('list_agents returns only agent-kind identities seeded in the directory', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((adapter, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            yield* adapter.seedAgent('peer-a').pipe(Effect.orDie)
            yield* adapter.seedAgent('peer-b').pipe(Effect.orDie)
            yield* adapter.seedHuman('carol').pipe(Effect.orDie)
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({ name: 'list_agents', arguments: {} }),
        )
        expect(result.isError).toBeFalsy()
        const sc = result.structuredContent as {
          identities: Array<{ name: string; kind: string }>
        }
        const names = sc.identities.map((i) => i.name).sort()
        expect(names).toContain('peer-a')
        expect(names).toContain('peer-b')
        expect(names).not.toContain('carol')
        for (const identity of sc.identities) {
          expect(identity.kind).toBe('agent')
        }
      }),
    ),
  ))

test('presence reads cache after list_agents populates it', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((adapter, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            yield* adapter.seedAgent('peer-a').pipe(Effect.orDie)
          }),
        )
        const listed = yield* Effect.promise(() =>
          rig.client.callTool({ name: 'list_agents', arguments: {} }),
        )
        const sc = listed.structuredContent as { identities: Array<{ id: string }> }
        const peer = sc.identities.find(
          (i) =>
            // peer-a is the seeded agent
            i.id.length > 0,
        )
        expect(peer).toBeDefined()
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'presence',
            arguments: { identity_id: peer?.id ?? '' },
          }),
        )
        expect(result.isError).toBeFalsy()
        const psc = result.structuredContent as { presence: string }
        expect(['online', 'idle', 'offline']).toContain(psc.presence)
      }),
    ),
  ))

test('presence reads cache after resolve populates it', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((adapter, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            yield* adapter.seedAgent('peer-b').pipe(Effect.orDie)
          }),
        )
        const resolved = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'resolve',
            arguments: { name: 'peer-b' },
          }),
        )
        const rsc = resolved.structuredContent as { identity: { id: string } | null }
        const peerId = rsc.identity?.id ?? ''
        expect(peerId.length).toBeGreaterThan(0)
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'presence',
            arguments: { identity_id: peerId },
          }),
        )
        expect(result.isError).toBeFalsy()
        const psc = result.structuredContent as { presence: string }
        expect(['online', 'idle', 'offline']).toContain(psc.presence)
      }),
    ),
  ))

test('presence for an unknown id surfaces UnknownIdentity', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((_adapter, ensureBound) => ensureBound().pipe(Effect.asVoid))
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'presence',
                arguments: { identity_id: 'never-cached-or-seeded' },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        expect(error.message).toContain('UnknownIdentity')
      }),
    ),
  ))

test('list_humans returns only human-kind identities', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((adapter, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            yield* adapter.seedAgent('peer-a').pipe(Effect.orDie)
            yield* adapter.seedHuman('carol').pipe(Effect.orDie)
            yield* adapter.seedHuman('dave').pipe(Effect.orDie)
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({ name: 'list_humans', arguments: {} }),
        )
        expect(result.isError).toBeFalsy()
        const sc = result.structuredContent as {
          identities: Array<{ name: string; kind: string }>
        }
        const names = sc.identities.map((i) => i.name).sort()
        expect(names).toContain('carol')
        expect(names).toContain('dave')
        expect(names).not.toContain('peer-a')
        for (const identity of sc.identities) {
          expect(identity.kind).toBe('human')
        }
      }),
    ),
  ))

test('list_channels returns every channel seeded in the substrate', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((adapter, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            yield* adapter.seedChannel('home').pipe(Effect.orDie)
            yield* adapter.seedChannel('general').pipe(Effect.orDie)
            yield* adapter.seedChannel('commy').pipe(Effect.orDie)
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({ name: 'list_channels', arguments: {} }),
        )
        expect(result.isError).toBeFalsy()
        const sc = result.structuredContent as { channels: Array<{ id: string; name: string }> }
        const names = sc.channels.map((c) => c.name).sort()
        expect(names).toEqual(['commy', 'general', 'home'])
        for (const channel of sc.channels) {
          expect(typeof channel.id).toBe('string')
          expect(channel.id.length).toBeGreaterThan(0)
        }
      }),
    ),
  ))

test('read_channel returns posted messages within the given range', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
            cache.rememberChannel(channelRef)
            yield* adapter.publisher.post(channelRef.name, decodeMessageBodySync('hello one'))
            yield* adapter.publisher.post(channelRef.name, decodeMessageBodySync('hello two'))
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'read_channel',
            arguments: { channel_name: 'home' },
          }),
        )
        expect(result.isError).toBeFalsy()
        const sc = result.structuredContent as { messages: Array<{ body: string }> }
        const bodies = sc.messages.map((m) => m.body).sort()
        expect(bodies).toEqual(['hello one', 'hello two'])
      }),
    ),
  ))

test('read_channel honours the limit argument', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
            cache.rememberChannel(channelRef)
            yield* adapter.publisher.post(channelRef.name, decodeMessageBodySync('one'))
            yield* adapter.publisher.post(channelRef.name, decodeMessageBodySync('two'))
            yield* adapter.publisher.post(channelRef.name, decodeMessageBodySync('three'))
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'read_channel',
            arguments: { channel_name: 'home', limit: 2 },
          }),
        )
        const sc = result.structuredContent as { messages: Array<{ body: string }> }
        expect(sc.messages).toHaveLength(2)
      }),
    ),
  ))

test('resolve_thread then unresolve_thread flip a thread’s resolved status, surfaced via read_thread', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
            cache.rememberChannel(channelRef)
            yield* adapter.publisher.post(channelRef.name, decodeMessageBodySync('in design'), {
              thread: decodeThreadNameSync('design'),
            })
          }),
        )
        const readResolved = (): Promise<{ thread: { resolved: boolean } | null }> =>
          rig.client
            .callTool({
              name: 'read_thread',
              arguments: { channel_name: 'home', thread: 'design' },
            })
            .then((r) => {
              const sc = r.structuredContent as {
                messages: Array<{ thread: { resolved: boolean } | null }>
              }
              const message = sc.messages[0]
              if (message === undefined) throw new Error('expected the design thread readable')
              return message
            })

        const resolveResult = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'resolve_thread',
            arguments: { channel_name: 'home', thread: 'design' },
          }),
        )
        expect(resolveResult.isError).toBeFalsy()
        expect((yield* Effect.promise(readResolved)).thread?.resolved).toBe(true)

        const unresolveResult = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'unresolve_thread',
            arguments: { channel_name: 'home', thread: 'design' },
          }),
        )
        expect(unresolveResult.isError).toBeFalsy()
        expect((yield* Effect.promise(readResolved)).thread?.resolved).toBe(false)
      }),
    ),
  ))

// The tool pair over the port's Option: the wire has no null to write, so an
// empty `description` is how a caller clears one, and a channel with none
// reads back as `null` rather than an empty string.
test('get_channel_description / set_channel_description round-trip, and empty clears', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        const readDescription = (): Promise<string | null> =>
          rig.client
            .callTool({ name: 'get_channel_description', arguments: { channel_name: 'home' } })
            .then((r) => (r.structuredContent as { description: string | null }).description)

        expect(yield* Effect.promise(readDescription)).toBeNull()

        const charter = 'Where the home crowd coordinates.'
        const setResult = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'set_channel_description',
            arguments: { channel_name: 'home', description: charter },
          }),
        )
        expect(setResult.isError).toBeFalsy()
        expect(yield* Effect.promise(readDescription)).toBe(charter)

        const clearResult = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'set_channel_description',
            arguments: { channel_name: 'home', description: '' },
          }),
        )
        expect(clearResult.isError).toBeFalsy()
        expect(yield* Effect.promise(readDescription)).toBeNull()
      }),
    ),
  ))

test('set_channel_description surfaces a substrate refusal as a tool error, storing nothing', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'set_channel_description',
                arguments: { channel_name: 'home', description: 'first line\nsecond line' },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        // The refusal reaches the caller naming both the failure and the fix,
        // rather than a generic write error.
        expect(error.message).toContain('ChannelDescriptionRejected')
        expect(error.message).toContain('single line')

        const after = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'get_channel_description',
            arguments: { channel_name: 'home' },
          }),
        )
        expect((after.structuredContent as { description: string | null }).description).toBeNull()
      }),
    ),
  ))

test('post returns a clickable permalink for the new message', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'hi', thread: 'topic-a' },
          }),
        )
        expect(result.isError).toBeFalsy()
        const sc = result.structuredContent as {
          message_id: string
          channel_id: string
          permalink: string
        }
        expect(sc.permalink).toBe(
          `memory://commy/channel/${sc.channel_id}/topic/topic-a/near/${sc.message_id}`,
        )
      }),
    ),
  ))

test('read_channel decorates each message with message and channel permalinks', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
            cache.rememberChannel(channelRef)
            yield* adapter.publisher.post(channelRef.name, decodeMessageBodySync('hello one'))
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'read_channel',
            arguments: { channel_name: 'home' },
          }),
        )
        expect(result.isError).toBeFalsy()
        const sc = result.structuredContent as {
          messages: Array<{
            id: string
            channel: { id: string; permalink: string }
            permalink: string
          }>
        }
        const message = sc.messages[0]
        expect(message?.channel.permalink).toBe(`memory://commy/channel/${message?.channel.id}`)
        expect(message?.permalink).toBe(
          `memory://commy/channel/${message?.channel.id}/near/${message?.id}`,
        )
      }),
    ),
  ))

test('list_channels decorates each channel with a permalink', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((adapter, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            yield* adapter.seedChannel('home').pipe(Effect.orDie)
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({ name: 'list_channels', arguments: {} }),
        )
        expect(result.isError).toBeFalsy()
        const sc = result.structuredContent as {
          channels: Array<{ id: string; permalink: string }>
        }
        const channel = sc.channels[0]
        expect(channel?.permalink).toBe(`memory://commy/channel/${channel?.id}`)
      }),
    ),
  ))

test('message_link returns the cached permalink for a known message id', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        const posted = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'hi', thread: 'topic-a' },
          }),
        )
        const post = posted.structuredContent as { message_id: string; permalink: string }
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'message_link',
            arguments: { message_id: post.message_id },
          }),
        )
        expect(result.isError).toBeFalsy()
        expect((result.structuredContent as { permalink: string }).permalink).toBe(post.permalink)
      }),
    ),
  ))

test('message_link builds a permalink from a channel hint for an uncached id', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        // A post only to learn the channel's numeric id; the linked id is a
        // different, uncached one so the channel-hint build path is exercised.
        const posted = yield* Effect.promise(() =>
          rig.client.callTool({ name: 'post', arguments: { channel_name: 'home', body: 'seed' } }),
        )
        const channelId = (posted.structuredContent as { channel_id: string }).channel_id
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'message_link',
            arguments: { message_id: '999', channel_name: 'home', thread: 'topic-a' },
          }),
        )
        expect(result.isError).toBeFalsy()
        expect((result.structuredContent as { permalink: string | null }).permalink).toBe(
          `memory://commy/channel/${channelId}/topic/topic-a/near/999`,
        )
      }),
    ),
  ))

test('subscribe with channel:<name> calls inbox.subscribe with a matching ChannelName', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const subscribed: Array<unknown> = []
        const adapter = yield* memoryAdapter()
        // wrap inbox.subscribe to capture targets
        const originalSubscribe = adapter.inbox.subscribe.bind(adapter.inbox)
        adapter.inbox.subscribe = (target) =>
          Effect.sync(() => {
            subscribed.push(target)
          }).pipe(Effect.flatMap(() => originalSubscribe(target)))
        const deps = yield* buildDeps(adapter)
        const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
        const rig = yield* mountAndConnect(adapter, deps)

        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'subscribe',
            arguments: { target: 'channel:home' },
          }),
        )
        expect(result.isError).toBeFalsy()
        expect(subscribed).toHaveLength(1)
        expect(subscribed[0]).toBe(channelRef.name)
        // narrowSet is updated alongside the substrate call.
        expect(rig.narrowSet.size()).toBe(1)
      }),
    ),
  ))

test('subscribe with malformed target surfaces a protocol error', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((_adapter, ensureBound) => ensureBound().pipe(Effect.asVoid))
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'subscribe',
                arguments: { target: 'not-a-valid-token' },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        expect(error.message).toContain('SubscribeTokenError')
      }),
    ),
  ))

test('unsubscribe routes through inbox.unsubscribe with the parsed target', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const unsubscribed: Array<unknown> = []
        const adapter = yield* memoryAdapter()
        const originalUnsubscribe = adapter.inbox.unsubscribe.bind(adapter.inbox)
        adapter.inbox.unsubscribe = (target) =>
          Effect.sync(() => {
            unsubscribed.push(target)
          }).pipe(Effect.flatMap(() => originalUnsubscribe(target)))
        const deps = yield* buildDeps(adapter)
        const rig = yield* mountAndConnect(adapter, deps)

        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'unsubscribe',
            arguments: { target: 'mentions' },
          }),
        )
        expect(result.isError).toBeFalsy()
        expect(unsubscribed).toEqual(['mentions'])
      }),
    ),
  ))

test('post returns message_id + channel_name and posts via the adapter', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
            cache.rememberChannel(channelRef)
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'hello from tool' },
          }),
        )
        expect(result.isError).toBeFalsy()
        const sc = result.structuredContent as {
          message_id: string
          channel_name: string
          channel_id: string
          thread: { name: string } | null
        }
        expect(sc.message_id.length).toBeGreaterThan(0)
        expect(sc.channel_name).toBe('home')
        expect(sc.thread).toBeNull()
        const channelRef = yield* rig.adapter.seedChannel('home').pipe(Effect.orDie)
        const history = yield* rig.adapter.history.readChannel(channelRef.name, {})
        expect(history.map((m) => m.body)).toContain(decodeMessageBodySync('hello from tool'))
      }),
    ),
  ))

test('post supports thread arg', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'thread reply', thread: 'payments' },
          }),
        )
        const sc = result.structuredContent as { thread: { name: string } | null }
        expect(sc.thread).toEqual({ name: 'payments' })
      }),
    ),
  ))

test('post to a thread auto-subscribes the poster to that thread', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const subscribed: Array<unknown> = []
        const adapter = yield* memoryAdapter()
        const originalSubscribe = adapter.inbox.subscribe.bind(adapter.inbox)
        adapter.inbox.subscribe = (target) =>
          Effect.sync(() => {
            subscribed.push(target)
          }).pipe(Effect.flatMap(() => originalSubscribe(target)))
        const deps = yield* buildDeps(adapter)
        const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
        const rig = yield* mountAndConnect(adapter, deps)
        rig.cache.rememberChannel(channelRef)

        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'opening message', thread: 'topic-X' },
          }),
        )
        expect(result.isError).toBeFalsy()
        expect(subscribed).toEqual([
          expect.objectContaining({
            channel: 'home',
            thread: 'topic-X',
          }),
        ])
        expect(rig.narrowSet.size()).toBe(1)
      }),
    ),
  ))

test('post without a thread does NOT auto-subscribe', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const subscribed: Array<unknown> = []
        const adapter = yield* memoryAdapter()
        const originalSubscribe = adapter.inbox.subscribe.bind(adapter.inbox)
        adapter.inbox.subscribe = (target) =>
          Effect.sync(() => {
            subscribed.push(target)
          }).pipe(Effect.flatMap(() => originalSubscribe(target)))
        const deps = yield* buildDeps(adapter)
        const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
        const rig = yield* mountAndConnect(adapter, deps)
        rig.cache.rememberChannel(channelRef)

        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'channel-level post' },
          }),
        )
        expect(result.isError).toBeFalsy()
        expect(subscribed).toEqual([])
        expect(rig.narrowSet.size()).toBe(0)
      }),
    ),
  ))

test('posting to the same thread twice subscribes only once (idempotency)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const subscribed: Array<unknown> = []
        const adapter = yield* memoryAdapter()
        const originalSubscribe = adapter.inbox.subscribe.bind(adapter.inbox)
        adapter.inbox.subscribe = (target) =>
          Effect.sync(() => {
            subscribed.push(target)
          }).pipe(Effect.flatMap(() => originalSubscribe(target)))
        const deps = yield* buildDeps(adapter)
        const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
        const rig = yield* mountAndConnect(adapter, deps)
        rig.cache.rememberChannel(channelRef)

        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'first', thread: 'topic-X' },
          }),
        )
        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'second', thread: 'topic-X' },
          }),
        )
        expect(rig.narrowSet.size()).toBe(1)
        void subscribed
      }),
    ),
  ))

test('react after a prior post hits the MessageRef cache without channel_name', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        const posted = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'reactable' },
          }),
        )
        const messageId = (posted.structuredContent as { message_id: string }).message_id
        const reacted = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'react',
            arguments: { message_id: messageId, emoji: 'tada' },
          }),
        )
        expect(reacted.isError).toBeFalsy()
      }),
    ),
  ))

test('react with explicit channel_name works on a cache-miss id', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
            cache.rememberChannel(channelRef)
            // post directly via adapter — bypasses the plugin's MessageRef cache
            yield* adapter.publisher.post(
              channelRef.name,
              decodeMessageBodySync('pre-existing message'),
            )
          }),
        )
        // We don't know the adapter-allocated message id externally, so reach in.
        const homeRef = yield* rig.adapter.seedChannel('home').pipe(Effect.orDie)
        const history = yield* rig.adapter.history.readChannel(homeRef.name, {})
        const messageId = history[0]?.ref.id
        expect(messageId).toBeDefined()
        const reacted = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'react',
            arguments: { message_id: String(messageId), emoji: 'tada', channel_name: 'home' },
          }),
        )
        expect(reacted.isError).toBeFalsy()
      }),
    ),
  ))

test('react with cache miss and no channel_name surfaces UnknownMessage', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((_adapter, ensureBound) => ensureBound().pipe(Effect.asVoid))
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'react',
                arguments: { message_id: 'made-up-id', emoji: 'tada' },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        expect(error.message).toContain('UnknownMessage')
      }),
    ),
  ))

test('react with a malformed emoji arg yields a typed ParseError tool error, not a crash', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // MCP args are untrusted: a colon-wrapped emoji shortcode fails the
        // Emoji brand decode. Its ParseError must thread through runEdge as a
        // typed tool-error response rather than surfacing as a defect/crash.
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        const posted = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'react target' },
          }),
        )
        const messageId = (posted.structuredContent as { message_id: string }).message_id
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'react',
                arguments: { message_id: messageId, emoji: ':smile:' },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        expect(error.message).toContain('ParseError')
      }),
    ),
  ))

test('edit_message rewrites the body, visible via history.readChannel', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        const posted = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'original' },
          }),
        )
        const messageId = (posted.structuredContent as { message_id: string }).message_id
        const edited = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'edit_message',
            arguments: { message_id: messageId, body: 'replacement' },
          }),
        )
        expect(edited.isError).toBeFalsy()
        const homeRef = yield* rig.adapter.seedChannel('home').pipe(Effect.orDie)
        const history = yield* rig.adapter.history.readChannel(homeRef.name, {})
        expect(history.map((m) => m.body)).toContain(decodeMessageBodySync('replacement'))
        expect(history.map((m) => m.body)).not.toContain(decodeMessageBodySync('original'))
      }),
    ),
  ))

test('edit_message with explicit channel_name works on a cache-miss id', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
            cache.rememberChannel(channelRef)
            yield* adapter.publisher.post(channelRef.name, decodeMessageBodySync('pre-existing'))
          }),
        )
        const homeRef = yield* rig.adapter.seedChannel('home').pipe(Effect.orDie)
        const history = yield* rig.adapter.history.readChannel(homeRef.name, {})
        const messageId = history[0]?.ref.id
        expect(messageId).toBeDefined()
        const edited = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'edit_message',
            arguments: { message_id: String(messageId), body: 'amended', channel_name: 'home' },
          }),
        )
        expect(edited.isError).toBeFalsy()
        const afterRef = yield* rig.adapter.seedChannel('home').pipe(Effect.orDie)
        const after = yield* rig.adapter.history.readChannel(afterRef.name, {})
        expect(after[0]?.body).toBe(decodeMessageBodySync('amended'))
      }),
    ),
  ))

test('edit_message with cache miss and no channel_name surfaces UnknownMessage', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((_adapter, ensureBound) => ensureBound().pipe(Effect.asVoid))
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'edit_message',
                arguments: { message_id: 'made-up-id', body: 'nope' },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        expect(error.message).toContain('UnknownMessage')
      }),
    ),
  ))

test('unreact mirrors react: routes through publisher.unreact', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        const posted = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'unreact-me' },
          }),
        )
        const messageId = (posted.structuredContent as { message_id: string }).message_id
        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'react',
            arguments: { message_id: messageId, emoji: 'tada' },
          }),
        )
        const unreacted = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'unreact',
            arguments: { message_id: messageId, emoji: 'tada' },
          }),
        )
        expect(unreacted.isError).toBeFalsy()
      }),
    ),
  ))

test('post into an existing thread (other agent posted first) auto-subscribes the poster', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const subscribed: Array<unknown> = []
        const adapter = yield* memoryAdapter()
        const originalSubscribe = adapter.inbox.subscribe.bind(adapter.inbox)
        adapter.inbox.subscribe = (target) =>
          Effect.sync(() => {
            subscribed.push(target)
          }).pipe(Effect.flatMap(() => originalSubscribe(target)))
        const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
        // Seed the thread with a prior message from a different bot so the topic
        // exists before our agent participates. ensureBound below acquires a
        // separate name ("test-bot") — we acquire-then-release a peer first.
        yield* adapter.identity.acquire(decodeBotNameSync('peer-bot'))
        yield* adapter.publisher.post(
          channelRef.name,
          decodeMessageBodySync('opening line from peer'),
          {
            thread: decodeThreadNameSync('joint-topic'),
          },
        )
        yield* adapter.identity.release()

        const deps = yield* buildDeps(adapter)
        const rig = yield* mountAndConnect(adapter, deps)
        rig.cache.rememberChannel(channelRef)

        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: 'home',
              body: 'replying into existing topic',
              thread: 'joint-topic',
            },
          }),
        )
        expect(result.isError).toBeFalsy()
        expect(subscribed).toEqual([
          expect.objectContaining({
            channel: 'home',
            thread: 'joint-topic',
          }),
        ])
        expect(rig.narrowSet.size()).toBe(1)
      }),
    ),
  ))

test('react to a message in a thread auto-subscribes the reactor', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const subscribed: Array<unknown> = []
        const adapter = yield* memoryAdapter()
        const originalSubscribe = adapter.inbox.subscribe.bind(adapter.inbox)
        adapter.inbox.subscribe = (target) =>
          Effect.sync(() => {
            subscribed.push(target)
          }).pipe(Effect.flatMap(() => originalSubscribe(target)))
        const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
        // Pre-existing thread message authored by a peer — bypasses the tool so
        // no post-side auto-sub fires before the react under test.
        yield* adapter.identity.acquire(decodeBotNameSync('peer-bot'))
        const targetRef = yield* adapter.publisher.post(
          channelRef.name,
          decodeMessageBodySync('reactable in thread'),
          {
            thread: decodeThreadNameSync('payments'),
          },
        )
        yield* adapter.identity.release()

        const deps = yield* buildDeps(adapter)
        const rig = yield* mountAndConnect(adapter, deps)
        rig.cache.rememberChannel(channelRef)

        const reacted = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'react',
            arguments: {
              message_id: String(targetRef.id),
              emoji: 'tada',
              channel_name: 'home',
              thread: 'payments',
            },
          }),
        )
        expect(reacted.isError).toBeFalsy()
        expect(subscribed).toEqual([
          expect.objectContaining({
            channel: 'home',
            thread: 'payments',
          }),
        ])
        expect(rig.narrowSet.size()).toBe(1)
      }),
    ),
  ))

test('react to a top-level (no-thread) message does NOT auto-subscribe', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const subscribed: Array<unknown> = []
        const adapter = yield* memoryAdapter()
        const originalSubscribe = adapter.inbox.subscribe.bind(adapter.inbox)
        adapter.inbox.subscribe = (target) =>
          Effect.sync(() => {
            subscribed.push(target)
          }).pipe(Effect.flatMap(() => originalSubscribe(target)))
        const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
        yield* adapter.identity.acquire(decodeBotNameSync('peer-bot'))
        const targetRef = yield* adapter.publisher.post(
          channelRef.name,
          decodeMessageBodySync('top-level reactable'),
        )
        yield* adapter.identity.release()

        const deps = yield* buildDeps(adapter)
        const rig = yield* mountAndConnect(adapter, deps)
        rig.cache.rememberChannel(channelRef)

        const reacted = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'react',
            arguments: {
              message_id: String(targetRef.id),
              emoji: 'tada',
              channel_name: 'home',
            },
          }),
        )
        expect(reacted.isError).toBeFalsy()
        expect(subscribed).toEqual([])
        expect(rig.narrowSet.size()).toBe(0)
      }),
    ),
  ))

test('reacting to the same thread twice subscribes only once (idempotency)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const subscribed: Array<unknown> = []
        const adapter = yield* memoryAdapter()
        const originalSubscribe = adapter.inbox.subscribe.bind(adapter.inbox)
        adapter.inbox.subscribe = (target) =>
          Effect.sync(() => {
            subscribed.push(target)
          }).pipe(Effect.flatMap(() => originalSubscribe(target)))
        const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
        yield* adapter.identity.acquire(decodeBotNameSync('peer-bot'))
        const a = yield* adapter.publisher.post(channelRef.name, decodeMessageBodySync('one'), {
          thread: decodeThreadNameSync('payments'),
        })
        const b = yield* adapter.publisher.post(channelRef.name, decodeMessageBodySync('two'), {
          thread: decodeThreadNameSync('payments'),
        })
        yield* adapter.identity.release()

        const deps = yield* buildDeps(adapter)
        const rig = yield* mountAndConnect(adapter, deps)
        rig.cache.rememberChannel(channelRef)

        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'react',
            arguments: {
              message_id: String(a.id),
              emoji: 'tada',
              channel_name: 'home',
              thread: 'payments',
            },
          }),
        )
        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'react',
            arguments: {
              message_id: String(b.id),
              emoji: 'plus_one',
              channel_name: 'home',
              thread: 'payments',
            },
          }),
        )
        expect(rig.narrowSet.size()).toBe(1)
        void subscribed
      }),
    ),
  ))

test('unreact does not change subscription state (no unsub-on-disengage)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const subscribed: Array<unknown> = []
        const unsubscribed: Array<unknown> = []
        const adapter = yield* memoryAdapter()
        const originalSubscribe = adapter.inbox.subscribe.bind(adapter.inbox)
        const originalUnsubscribe = adapter.inbox.unsubscribe.bind(adapter.inbox)
        adapter.inbox.subscribe = (target) =>
          Effect.sync(() => {
            subscribed.push(target)
          }).pipe(Effect.flatMap(() => originalSubscribe(target)))
        adapter.inbox.unsubscribe = (target) =>
          Effect.sync(() => {
            unsubscribed.push(target)
          }).pipe(Effect.flatMap(() => originalUnsubscribe(target)))
        const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
        yield* adapter.identity.acquire(decodeBotNameSync('peer-bot'))
        const targetRef = yield* adapter.publisher.post(
          channelRef.name,
          decodeMessageBodySync('reactable'),
          {
            thread: decodeThreadNameSync('payments'),
          },
        )
        yield* adapter.identity.release()

        const deps = yield* buildDeps(adapter)
        const rig = yield* mountAndConnect(adapter, deps)
        rig.cache.rememberChannel(channelRef)

        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'react',
            arguments: {
              message_id: String(targetRef.id),
              emoji: 'tada',
              channel_name: 'home',
              thread: 'payments',
            },
          }),
        )
        expect(rig.narrowSet.size()).toBe(1)
        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'unreact',
            arguments: {
              message_id: String(targetRef.id),
              emoji: 'tada',
              channel_name: 'home',
              thread: 'payments',
            },
          }),
        )
        expect(rig.narrowSet.size()).toBe(1)
        expect(unsubscribed).toEqual([])
        void subscribed
      }),
    ),
  ))

test('read_thread returns only messages in the given thread', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
            cache.rememberChannel(channelRef)
            yield* adapter.publisher.post(
              channelRef.name,
              decodeMessageBodySync('top-level message'),
            )
            yield* adapter.publisher.post(
              channelRef.name,
              decodeMessageBodySync('thread message'),
              {
                thread: decodeThreadNameSync('payments'),
              },
            )
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'read_thread',
            arguments: { channel_name: 'home', thread: 'payments' },
          }),
        )
        const sc = result.structuredContent as { messages: Array<{ body: string }> }
        expect(sc.messages.map((m) => m.body)).toEqual(['thread message'])
      }),
    ),
  ))

test('read_thread schema uses thread not thread_name', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((_adapter, ensureBound) => ensureBound().pipe(Effect.asVoid))
        const result = yield* Effect.promise(() => rig.client.listTools())
        const tool = result.tools.find((t) => t.name === 'read_thread')
        expect(tool).toBeDefined()
        const props = tool?.inputSchema.properties as Record<string, unknown>
        expect(props['thread']).toBeDefined()
        expect(props['thread_name']).toBeUndefined()
        const required = (tool?.inputSchema as { required?: ReadonlyArray<string> }).required ?? []
        expect(required).toContain('thread')
        expect(required).not.toContain('thread_name')
      }),
    ),
  ))

test('post with unknown argument rejects instead of silently dropping', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'post',
                arguments: { channel_name: 'home', body: 'hello', thread_name: 'oops' },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        expect(error.message).toContain('thread_name')
      }),
    ),
  ))

test('post with a non-string required arg rejects via the typed args decode', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // The handler decodes the raw MCP args through a typed Schema.Struct
        // once at its boundary rather than reaching in with args['k'] + typeof
        // checks. A wrong-typed required field (channel_name as a number) must
        // be rejected by that decode rather than flowing into the body as an
        // unchecked value.
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'post',
                arguments: { channel_name: 123, body: 'hello' },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        // The decode failure surfaces as a tool error threaded through runEdge,
        // not a crash; it names the offending field.
        expect(error.message).toContain('channel_name')
      }),
    ),
  ))

test('post with valid args decodes to the typed shape and posts', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // Happy path for the typed args decode: a well-formed args object with
        // both required strings and an optional thread decodes cleanly and the
        // post lands with the parsed values.
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: { channel_name: 'home', body: 'typed', thread: 'topic-typed' },
          }),
        )
        expect(result.isError).toBeFalsy()
        const sc = result.structuredContent as {
          channel_name: string
          thread: { name: string } | null
        }
        expect(sc.channel_name).toBe('home')
        expect(sc.thread).toEqual({ name: 'topic-typed' })
      }),
    ),
  ))

test('react with unknown argument rejects', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRigAndCache((adapter, cache, ensureBound) =>
          Effect.gen(function* () {
            yield* ensureBound()
            cache.rememberChannel(yield* adapter.seedChannel('home').pipe(Effect.orDie))
            const channelRef = yield* adapter.seedChannel('home').pipe(Effect.orDie)
            const ref = yield* adapter.publisher.post(channelRef.name, decodeMessageBodySync('msg'))
            cache.rememberMessage(ref)
          }),
        )
        const homeRef = yield* rig.adapter.seedChannel('home').pipe(Effect.orDie)
        const history = yield* rig.adapter.history.readChannel(homeRef.name, {})
        const messageId = history[0]?.ref.id
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'react',
                arguments: { message_id: String(messageId), emoji: 'tada', bogus_field: true },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        expect(error.message).toContain('bogus_field')
      }),
    ),
  ))

// --- download_file ---

interface DownloadRig {
  readonly client: Client
}

const withDownloadRig = (
  downloadFile: (
    urlPath: string,
  ) => Effect.Effect<{ filePath: string; contentType: string; size: number }>,
): Effect.Effect<DownloadRig, never, Scope.Scope> =>
  Effect.gen(function* () {
    const adapter = yield* memoryAdapter()
    const deps = yield* buildDeps(adapter)
    yield* deps.ensureBound().pipe(Effect.orDie)
    const rig = yield* mountAndConnect(adapter, deps, { downloadFile })
    return { client: rig.client }
  })

test('tools/list advertises download_file when downloadFile dep is provided', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withDownloadRig(() =>
          Effect.succeed({
            filePath: '/tmp/stub',
            contentType: 'application/octet-stream',
            size: 0,
          }),
        )
        const result = yield* Effect.promise(() => rig.client.listTools())
        const tool = result.tools.find((t) => t.name === 'download_file')
        expect(tool).toBeDefined()
        expect(tool?.inputSchema).toMatchObject({
          type: 'object',
          properties: {
            url_path: { type: 'string' },
          },
          required: ['url_path'],
          additionalProperties: false,
        })
      }),
    ),
  ))

test('tools/list does not advertise download_file when downloadFile dep is missing', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((_adapter, ensureBound) => ensureBound().pipe(Effect.asVoid))
        const result = yield* Effect.promise(() => rig.client.listTools())
        const tool = result.tools.find((t) => t.name === 'download_file')
        expect(tool).toBeUndefined()
      }),
    ),
  ))

test('download_file returns the file path, content type, and size from the callback', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const pngHeader = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])
        const tmpPath = join(tmpdir(), `download-file-test-${Date.now()}.png`)
        writeFileSync(tmpPath, pngHeader)
        yield* Effect.addFinalizer(() => Effect.sync(() => rmSync(tmpPath, { force: true })))
        const rig = yield* withDownloadRig((urlPath) =>
          Effect.sync(() => {
            expect(urlPath).toBe('/user_uploads/2/56/image.png')
            return { filePath: tmpPath, contentType: 'image/png', size: pngHeader.byteLength }
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'download_file',
            arguments: { url_path: '/user_uploads/2/56/image.png' },
          }),
        )
        expect(result.isError).toBeFalsy()
        const content = result.structuredContent as {
          file_path: string
          content_type: string
          size: number
        }
        expect(content.content_type).toBe('image/png')
        expect(content.size).toBe(8)
        expect(content.file_path).toBe(tmpPath)
      }),
    ),
  ))

test('download_file rejects paths not starting with /user_uploads/', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withDownloadRig(() =>
          Effect.succeed({
            filePath: '/tmp/stub',
            contentType: 'application/octet-stream',
            size: 0,
          }),
        )
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'download_file',
                arguments: { url_path: '/api/v1/messages' },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        // The reject is a typed ParseError threaded through runEdge,
        // not a defect/crash; its message names the user_uploads constraint.
        expect(error.message).toContain('ParseError')
        expect(error.message).toContain('user_uploads')
      }),
    ),
  ))

// --- upload_file ---

interface UploadRig {
  readonly client: Client
}

const withUploadRig = (
  upload: (path: string) => Effect.Effect<{ reference: string; filename: string; size: number }>,
): Effect.Effect<UploadRig, never, Scope.Scope> =>
  Effect.gen(function* () {
    const adapter = yield* memoryAdapter()
    const deps = yield* buildDeps(adapter)
    yield* deps.ensureBound().pipe(Effect.orDie)
    const rig = yield* mountAndConnect(adapter, deps, { upload })
    return { client: rig.client }
  })

test('tools/list advertises upload_file when the upload dep is provided', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withUploadRig(() =>
          Effect.succeed({
            reference: '[x](/u)',
            filename: 'x',
            size: 0,
          }),
        )
        const result = yield* Effect.promise(() => rig.client.listTools())
        const tool = result.tools.find((t) => t.name === 'upload_file')
        expect(tool).toBeDefined()
        expect(tool?.inputSchema).toMatchObject({
          type: 'object',
          properties: {
            path: { type: 'string' },
          },
          required: ['path'],
          additionalProperties: false,
        })
      }),
    ),
  ))

test('tools/list does not advertise upload_file when the upload dep is missing', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withRig((_adapter, ensureBound) => ensureBound().pipe(Effect.asVoid))
        const result = yield* Effect.promise(() => rig.client.listTools())
        const tool = result.tools.find((t) => t.name === 'upload_file')
        expect(tool).toBeUndefined()
      }),
    ),
  ))

test('upload_file passes the path through and returns reference, filename, and size', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withUploadRig((path) =>
          Effect.sync(() => {
            expect(path).toBe('/tmp/chart.png')
            return {
              reference: '[chart.png](/user_uploads/1/ab/chart.png)',
              filename: 'chart.png',
              size: 42,
            }
          }),
        )
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'upload_file',
            arguments: { path: '/tmp/chart.png' },
          }),
        )
        expect(result.isError).toBeFalsy()
        const content = result.structuredContent as {
          reference: string
          filename: string
          size: number
        }
        expect(content.reference).toBe('[chart.png](/user_uploads/1/ab/chart.png)')
        expect(content.filename).toBe('chart.png')
        expect(content.size).toBe(42)
      }),
    ),
  ))

test('upload_file rejects a relative path', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* withUploadRig(() =>
          Effect.succeed({
            reference: '[x](/u)',
            filename: 'x',
            size: 0,
          }),
        )
        const error = yield* Effect.flip(
          Effect.tryPromise({
            try: () =>
              rig.client.callTool({
                name: 'upload_file',
                arguments: { path: 'chart.png' },
              }),
            catch: (e) => e as { message: string },
          }),
        )
        expect(error.message).toContain('absolute')
      }),
    ),
  ))
