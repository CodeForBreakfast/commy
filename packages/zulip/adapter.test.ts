import { afterEach, beforeEach, expect, test } from 'bun:test'
import type { ChannelRef, Identity, InboundEvent, MessageRef } from '@codeforbreakfast/core/ports'
import {
  DirectoryError,
  decodeBotNameSync,
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeEmojiSync,
  decodeIdentityIdSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  decodeTimestampSync,
  IdentityError,
  PublisherError,
  UnknownChannel,
  type UnknownIdentity,
} from '@codeforbreakfast/core/ports'
import { FetchHttpClient, HttpClient } from '@effect/platform'
import { Cause, Duration, Effect, Exit, Option, Queue, Redacted, type Scope, Stream } from 'effect'
import type { ZulipAdapter, ZulipAdapterConfig } from './adapter.ts'
import { attachmentReference, zulipAdapter as zulipAdapterRaw } from './adapter.ts'
import { ApiKey, BotEmail, decodeUserUploadPathSync, RealmUrl, ZulipApiError } from './http.ts'
import type { TestRealm } from './test-server.ts'
import { startTestRealm } from './test-server.ts'

let realm: TestRealm

beforeEach(() => {
  realm = startTestRealm()
})

afterEach(async () => {
  await realm.stop()
})

const HERMES = {
  user_id: 9,
  email: 'hermes-agent-bot@example.com',
  full_name: 'hermes-agent',
  is_bot: true,
  is_active: true,
  role: 400,
} as const

const GRAEME = {
  user_id: 5,
  email: 'graeme@example.com',
  full_name: 'Graeme Foster',
  is_bot: false,
  is_active: true,
  role: 100,
} as const

const RIQ = {
  user_id: 11,
  email: 'riq-bot@example.com',
  full_name: 'riq6r230',
  is_bot: true,
  is_active: true,
  role: 400,
} as const

// Raw directory member behind the `bobHuman` Identity (id '2'). User-scoped
// calls resolve the ZulipUserRef through the directory, so the target must be
// a seeded member (comms-7ee).
const BOB = {
  user_id: 2,
  email: 'bob@example.com',
  full_name: 'bob',
  is_bot: false,
  is_active: true,
  role: 100,
} as const

const httpClient = Effect.runSync(HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer)))

// makeZulipHttp reads HttpClient from context (comms-0m8), so the adapter
// Effect carries `HttpClient` in R. Provide the real fetch-backed client
// here so every call site keeps a `never` requirements channel.
const zulipAdapter = (config: ZulipAdapterConfig): Effect.Effect<ZulipAdapter> =>
  zulipAdapterRaw(config).pipe(Effect.provideService(HttpClient.HttpClient, httpClient))

const makeConfig = (): Effect.Effect<ZulipAdapterConfig> =>
  Effect.gen(function* () {
    return {
      realmUrl: yield* RealmUrl(realm.url),
      minterEmail: yield* BotEmail('minter@example.com'),
      minterApiKey: Redacted.make(yield* ApiKey('minter-key')),
    }
  }).pipe(Effect.orDie)

const seedUsers = (members: ReadonlyArray<unknown>): void => {
  realm.handle('GET', '/api/v1/users', () => ({
    body: { result: 'success', members },
  }))
}

const seedRegenerate = (userId: number, apiKey = 'fresh-key'): void => {
  realm.handle('POST', `/api/v1/bots/${userId}/api_key/regenerate`, () => ({
    body: { result: 'success', api_key: apiKey },
  }))
}

const seedMint = (userId = 9999, apiKey = 'minted-key'): void => {
  // Match real Zulip's response shape — POST /bots returns
  // user_id + api_key but NOT email. The adapter reconstructs the
  // bot's delivery email from <short_name>-bot@<realm_host>.
  realm.handle('POST', '/api/v1/bots', () => ({
    body: { result: 'success', api_key: apiKey, user_id: userId },
  }))
}

// Realm host the test ZulipHttp will see — used to verify the
// adapter's client-side email construction. The test server binds
// to localhost so the realmUrl host is always "localhost".
const realmHost = (): string => new URL(realm.url).hostname

const seedDeactivate = (userId: number): void => {
  realm.handle('DELETE', `/api/v1/bots/${userId}`, () => ({
    body: { result: 'success' },
  }))
}

const buildAdapter = (
  configOverrides: Partial<ZulipAdapterConfig> = {},
  acquireName = decodeBotNameSync('hermes-agent'),
): Effect.Effect<ZulipAdapter, IdentityError | UnknownIdentity> =>
  Effect.gen(function* () {
    seedUsers([HERMES])
    seedRegenerate(HERMES.user_id)
    const config = yield* makeConfig()
    const adapter = yield* zulipAdapter({ ...config, ...configOverrides })
    yield* adapter.identity.acquire(acquireName)
    return adapter
  })

const findRequest = (method: string, pathname: string) => {
  const req = realm.captured.find((r) => r.method === method && r.url.pathname === pathname)
  if (req === undefined) throw new Error(`no captured ${method} ${pathname}`)
  return req
}

// Mirror inbox.events() into an unbounded Queue under the caller's Scope.
// The forked Stream.runDrain fiber lives for the scope's lifetime, so
// scope close interrupts it — which aborts any in-flight long-poll via the
// HttpClient's AbortSignal. Tests that need iter-shaped semantics use
// `yield* Queue.take(queue)` per substrate event.
const eventQueue = (
  adapter: ZulipAdapter,
): Effect.Effect<Queue.Queue<InboundEvent>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const queue = yield* Queue.unbounded<InboundEvent>()
    yield* Effect.forkScoped(
      adapter.inbox.events().pipe(
        Stream.tap((event) => Queue.offer(queue, event)),
        Stream.runDrain,
      ),
    )
    return queue
  })

test('identity.acquire on an existing bot regenerates its API key and binds', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([HERMES])
      seedRegenerate(HERMES.user_id, 'rotated-key')
      const config = yield* makeConfig()
      const adapter = yield* zulipAdapter(config)
      const result = yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      expect(result.identity).toEqual({
        id: decodeIdentityIdSync('9'),
        name: decodeDisplayNameSync('hermes-agent'),
        kind: 'agent',
      })
      expect(result.credentials).toEqual({
        substrate: 'zulip',
        realmUrl: config.realmUrl,
        email: yield* BotEmail(HERMES.email).pipe(Effect.orDie),
        apiKey: yield* ApiKey('rotated-key').pipe(Effect.orDie),
      })
      expect(findRequest('POST', `/api/v1/bots/${HERMES.user_id}/api_key/regenerate`)).toBeDefined()
    }),
  ))

test('identity.acquire on a name with no existing bot mints fresh via POST /bots', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([GRAEME])
      seedMint(42, 'fresh-mint-key')
      const config = yield* makeConfig()
      const adapter = yield* zulipAdapter(config)
      const result = yield* adapter.identity.acquire(decodeBotNameSync('fresh-bot'))
      expect(result.identity).toEqual({
        id: decodeIdentityIdSync('42'),
        name: decodeDisplayNameSync('fresh-bot'),
        kind: 'agent',
      })
      expect(result.credentials).toEqual({
        substrate: 'zulip',
        realmUrl: config.realmUrl,
        email: yield* BotEmail(`fresh-bot-bot@${realmHost()}`).pipe(Effect.orDie),
        apiKey: yield* ApiKey('fresh-mint-key').pipe(Effect.orDie),
      })
      const mintReq = findRequest('POST', '/api/v1/bots')
      const params = new URLSearchParams(mintReq.body)
      expect(params.get('full_name')).toBe('fresh-bot')
      expect(params.get('short_name')).toBe('fresh-bot')
      expect(params.get('bot_type')).toBe('1')
    }),
  ))

test('identity.acquire on a deactivated bot reactivates and regenerates (does NOT mint)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const deactivated = { ...HERMES, is_active: false }
      seedUsers([deactivated])
      realm.handle('POST', `/api/v1/users/${HERMES.user_id}/reactivate`, () => ({
        body: { result: 'success' },
      }))
      seedRegenerate(HERMES.user_id, 'rotated-key')
      const adapter = yield* zulipAdapter(yield* makeConfig())
      const result = yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      expect(result.identity.id).toEqual(decodeIdentityIdSync(String(HERMES.user_id)))
      expect(findRequest('POST', `/api/v1/users/${HERMES.user_id}/reactivate`)).toBeDefined()
      expect(findRequest('POST', `/api/v1/bots/${HERMES.user_id}/api_key/regenerate`)).toBeDefined()
      // Must not have minted — the deactivated email is still reserved on
      // the realm; mint would fail with EmailAlreadyInUseError.
      expect(
        realm.captured.find((r) => r.method === 'POST' && r.url.pathname === '/api/v1/bots'),
      ).toBeUndefined()
    }),
  ))

test('identity.acquire sanitises short_name by lowercasing and replacing non-email chars', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([])
      seedMint(50, 'k')
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('gas-town/witness'))
      const params = new URLSearchParams(findRequest('POST', '/api/v1/bots').body)
      expect(params.get('full_name')).toBe('gas-town/witness')
      expect(params.get('short_name')).toBe('gas-town-witness')
    }),
  ))

test('identity.acquire rejects with ZulipApiError when /users lookup fails', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', '/api/v1/users', () => ({
        body: { result: 'error', msg: 'Invalid API key', code: 'BAD_API_KEY' },
        init: { status: 401 },
      }))
      const adapter = yield* zulipAdapter(yield* makeConfig())
      const err = yield* Effect.flip(adapter.identity.acquire(decodeBotNameSync('hermes-agent')))
      expect(err).toBeInstanceOf(IdentityError)
      expect((err as { cause: unknown }).cause).toBeInstanceOf(ZulipApiError)
    }),
  ))

test('zulipAdapter construction does NOT call /users — the call is deferred to acquire', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([HERMES])
      yield* zulipAdapter(yield* makeConfig())
      const usersCalls = realm.captured.filter(
        (r) => r.method === 'GET' && r.url.pathname === '/api/v1/users',
      )
      expect(usersCalls).toHaveLength(0)
    }),
  ))

test('currentIdentity before acquire throws — port is unauthenticated at construction', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* zulipAdapter(yield* makeConfig())
      const exit = yield* Effect.exit(adapter.identity.currentIdentity())
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  ))

test('identity.acquire on the bound name is idempotent and skips a second /users round-trip', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([HERMES])
      seedRegenerate(HERMES.user_id)
      const adapter = yield* zulipAdapter(yield* makeConfig())
      const first = yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      const second = yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      expect(second.identity.id).toEqual(first.identity.id)
      const usersCalls = realm.captured.filter(
        (r) => r.method === 'GET' && r.url.pathname === '/api/v1/users',
      )
      expect(usersCalls).toHaveLength(1)
      const regenCalls = realm.captured.filter(
        (r) =>
          r.method === 'POST' &&
          r.url.pathname === `/api/v1/bots/${HERMES.user_id}/api_key/regenerate`,
      )
      expect(regenCalls).toHaveLength(1)
    }),
  ))

test('identity.acquire with a different name on a bound adapter rejects', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      const exit = yield* Effect.exit(adapter.identity.acquire(decodeBotNameSync('someone-else')))
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  ))

test('identity.release on a bound adapter deactivates the bot via DELETE /bots/{id}', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([HERMES])
      seedRegenerate(HERMES.user_id)
      seedDeactivate(HERMES.user_id)
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      yield* adapter.identity.release()
      expect(findRequest('DELETE', `/api/v1/bots/${HERMES.user_id}`)).toBeDefined()
    }),
  ))

test('identity.release clears the binding — currentIdentity then throws', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedDeactivate(HERMES.user_id)
      const adapter = yield* buildAdapter()
      yield* adapter.identity.release()
      const exit = yield* Effect.exit(adapter.identity.currentIdentity())
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  ))

test('identity.release on an unauthenticated adapter is a no-op', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.identity.release()
      const deletes = realm.captured.filter(
        (r) => r.method === 'DELETE' && r.url.pathname.startsWith('/api/v1/bots/'),
      )
      expect(deletes).toHaveLength(0)
    }),
  ))

test('identity.release is best-effort — deactivate failure does not throw', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([HERMES])
      seedRegenerate(HERMES.user_id)
      realm.handle('DELETE', `/api/v1/bots/${HERMES.user_id}`, () => ({
        body: { result: 'error', msg: 'kaboom', code: 'INTERNAL_SERVER_ERROR' },
        init: { status: 500 },
      }))
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      yield* adapter.identity.release()
      const exit = yield* Effect.exit(adapter.identity.currentIdentity())
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  ))

test('identity.release({ persistent: true }) does NOT deactivate the bot', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([HERMES])
      seedRegenerate(HERMES.user_id)
      seedDeactivate(HERMES.user_id)
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      yield* adapter.identity.release({ persistent: true })
      const deletes = realm.captured.filter(
        (r) => r.method === 'DELETE' && r.url.pathname.startsWith('/api/v1/bots/'),
      )
      expect(deletes).toHaveLength(0)
    }),
  ))

test('identity.release({ persistent: true }) still clears the binding — currentIdentity throws', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([HERMES])
      seedRegenerate(HERMES.user_id)
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      yield* adapter.identity.release({ persistent: true })
      const exit = yield* Effect.exit(adapter.identity.currentIdentity())
      expect(Exit.isFailure(exit)).toBe(true)
    }),
  ))

test('identity.release (ephemeral default) still deactivates the bot via DELETE /bots/{id}', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([HERMES])
      seedRegenerate(HERMES.user_id)
      seedDeactivate(HERMES.user_id)
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      yield* adapter.identity.release({ persistent: false })
      expect(findRequest('DELETE', `/api/v1/bots/${HERMES.user_id}`)).toBeDefined()
    }),
  ))

test('identity.acquire surfaces a reactivate failure with the real cause, not a generic credential error', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const deactivated = { ...HERMES, is_active: false }
      seedUsers([deactivated])
      // A Member (non-admin) minter calling POST /users/{id}/reactivate gets
      // UNAUTHORIZED_PRINCIPAL ("Must be an organization administrator"), 400.
      realm.handle('POST', `/api/v1/users/${HERMES.user_id}/reactivate`, () => ({
        body: {
          result: 'error',
          msg: 'Must be an organization administrator',
          code: 'UNAUTHORIZED_PRINCIPAL',
        },
        init: { status: 400 },
      }))
      seedRegenerate(HERMES.user_id)
      const adapter = yield* zulipAdapter(yield* makeConfig())
      const err = yield* Effect.flip(adapter.identity.acquire(decodeBotNameSync('hermes-agent')))
      expect(err).toBeInstanceOf(IdentityError)
      // The surfaced message must name the real cause (reactivate forbidden /
      // minter not admin) so the next debugger is not sent chasing a phantom
      // credential bug.
      expect(err.message).toContain('reactivate')
      expect(err.message.toLowerCase()).toContain('administrator')
      // The regenerate must never have fired — reactivate failed first.
      expect(
        realm.captured.find(
          (r) =>
            r.method === 'POST' &&
            r.url.pathname === `/api/v1/bots/${HERMES.user_id}/api_key/regenerate`,
        ),
      ).toBeUndefined()
    }),
  ))

test('identity.acquire after release rebinds via a fresh /users round-trip', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedDeactivate(HERMES.user_id)
      const adapter = yield* buildAdapter()
      yield* adapter.identity.release()
      // After release, the regenerate seed is still in place — re-acquire
      // takes the regenerate path again.
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      const usersCalls = realm.captured.filter(
        (r) => r.method === 'GET' && r.url.pathname === '/api/v1/users',
      )
      expect(usersCalls).toHaveLength(2)
    }),
  ))

test('identity.resolve returns Identity for a known full_name', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', '/api/v1/users', () => ({
        body: { result: 'success', members: [HERMES, GRAEME, RIQ] },
      }))
      seedRegenerate(HERMES.user_id)
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      const found = yield* adapter.identity.resolve('Graeme Foster')
      expect(found).toEqual(
        Option.some({
          id: decodeIdentityIdSync('5'),
          name: decodeDisplayNameSync('Graeme Foster'),
          kind: 'human',
        }),
      )
    }),
  ))

test('identity.resolve returns undefined for an unknown full_name', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', '/api/v1/users', () => ({
        body: { result: 'success', members: [HERMES, GRAEME] },
      }))
      seedRegenerate(HERMES.user_id)
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      const found = yield* adapter.identity.resolve('Nobody')
      expect(found).toEqual(Option.none())
    }),
  ))

test('directory.listAgents returns only is_bot=true users', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', '/api/v1/users', () => ({
        body: { result: 'success', members: [HERMES, GRAEME, RIQ] },
      }))
      seedRegenerate(HERMES.user_id)
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      const agents = yield* adapter.directory.listAgents()
      expect(agents).toEqual([
        {
          id: decodeIdentityIdSync('9'),
          name: decodeDisplayNameSync('hermes-agent'),
          kind: 'agent',
        },
        { id: decodeIdentityIdSync('11'), name: decodeDisplayNameSync('riq6r230'), kind: 'agent' },
      ])
    }),
  ))

test('directory.listHumans returns only is_bot=false users', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', '/api/v1/users', () => ({
        body: { result: 'success', members: [HERMES, GRAEME, RIQ] },
      }))
      seedRegenerate(HERMES.user_id)
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      const humans = yield* adapter.directory.listHumans()
      expect(humans).toEqual([
        {
          id: decodeIdentityIdSync('5'),
          name: decodeDisplayNameSync('Graeme Foster'),
          kind: 'human',
        },
      ])
    }),
  ))

test('directory.listHumans surfaces a /users fetch failure as DirectoryError (cause preserved)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      realm.handle('GET', '/api/v1/users', () => ({
        body: { result: 'error', msg: 'internal server error' },
        init: { status: 500 },
      }))
      const error = yield* Effect.flip(adapter.directory.listHumans())
      expect(error).toBeInstanceOf(DirectoryError)
      expect(error.operation).toBe('listHumans')
      expect(error.cause).toBeInstanceOf(ZulipApiError)
    }),
  ))

test('directory.listChannels surfaces a /streams fetch failure as DirectoryError (cause preserved)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      realm.handle('GET', '/api/v1/streams', () => ({
        body: { result: 'error', msg: 'internal server error' },
        init: { status: 500 },
      }))
      const error = yield* Effect.flip(adapter.directory.listChannels())
      expect(error).toBeInstanceOf(DirectoryError)
      expect(error.operation).toBe('listChannels')
      expect(error.cause).toBeInstanceOf(ZulipApiError)
    }),
  ))

test('directory.listAgents excludes inactive users', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', '/api/v1/users', () => ({
        body: {
          result: 'success',
          members: [HERMES, { ...RIQ, is_active: false }],
        },
      }))
      seedRegenerate(HERMES.user_id)
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      const agents = yield* adapter.directory.listAgents()
      expect(agents.map((a) => a.id)).toEqual([decodeIdentityIdSync('9')])
    }),
  ))

test('directory.listAgents surfaces a /users fetch failure as DirectoryError (cause preserved)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      realm.handle('GET', '/api/v1/users', () => ({
        body: { result: 'error', msg: 'internal server error' },
        init: { status: 500 },
      }))
      const error = yield* Effect.flip(adapter.directory.listAgents())
      expect(error).toBeInstanceOf(DirectoryError)
      expect(error.operation).toBe('listAgents')
      expect(error.cause).toBeInstanceOf(ZulipApiError)
    }),
  ))

const generalChannel: ChannelRef = {
  id: decodeChannelIdSync('1234'),
  name: decodeChannelNameSync('general'),
}
const aliceBot: Identity = {
  id: decodeIdentityIdSync('1'),
  name: decodeDisplayNameSync('alice'),
  kind: 'agent',
}
const bobHuman: Identity = {
  id: decodeIdentityIdSync('2'),
  name: decodeDisplayNameSync('bob'),
  kind: 'human',
}

const seedSendMessage = (id: number): void => {
  // publisher.post now pre-flights against GET /streams so unknown channels
  // surface as UnknownChannel instead of being silently routed to
  // Notification Bot. Helper bundles both stubs because every caller posts
  // to `generalChannel`.
  realm.handle('GET', '/api/v1/streams', () => ({
    body: {
      result: 'success',
      streams: [{ stream_id: 1234, name: 'general' }],
    },
  }))
  realm.handle('POST', '/api/v1/messages', () => ({
    body: { result: 'success', id },
  }))
}

test('publisher.post sends type=channel + to=channel.name + content; defaults topic to "(no topic)" when no thread', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedSendMessage(42)
      const adapter = yield* buildAdapter()
      yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('hello world'))
      const params = new URLSearchParams(findRequest('POST', '/api/v1/messages').body)
      expect(params.get('type')).toBe('channel')
      expect(params.get('to')).toBe('general')
      expect(params.get('content')).toBe('hello world')
      expect(params.get('topic')).toBe('(no topic)')
    }),
  ))

test('publisher.post returns a MessageRef built from the response id', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedSendMessage(99)
      const adapter = yield* buildAdapter()
      const ref = yield* adapter.publisher.post(
        generalChannel,
        decodeMessageBodySync('hello world'),
      )
      expect(ref.id).toEqual(decodeMessageIdSync('99'))
      expect(ref.channel).toEqual(generalChannel)
      expect(ref.thread).toBeUndefined()
    }),
  ))

test('publisher.post with thread sends topic and threads the returned MessageRef', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedSendMessage(100)
      const adapter = yield* buildAdapter()
      const ref = yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('hi'), {
        thread: { name: decodeThreadNameSync('ass-zsd9') },
      })
      const params = new URLSearchParams(findRequest('POST', '/api/v1/messages').body)
      expect(params.get('topic')).toBe('ass-zsd9')
      expect(ref.thread).toEqual({ name: decodeThreadNameSync('ass-zsd9') })
    }),
  ))

test('publisher.post leaves body unchanged when opts.mentions is set — mentions[] is metadata-only', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedSendMessage(101)
      const adapter = yield* buildAdapter()
      yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('wake up'), {
        mentions: [aliceBot, bobHuman],
      })
      const params = new URLSearchParams(findRequest('POST', '/api/v1/messages').body)
      expect(params.get('content')).toBe('wake up')
    }),
  ))

test('publisher.post with body-only mention markup posts body verbatim', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedSendMessage(103)
      const adapter = yield* buildAdapter()
      yield* adapter.publisher.post(
        generalChannel,
        decodeMessageBodySync('hey @**alice** look'),
        {},
      )
      const params = new URLSearchParams(findRequest('POST', '/api/v1/messages').body)
      expect(params.get('content')).toBe('hey @**alice** look')
    }),
  ))

// Regression for comms-izp: body containing `@**Name**` + mentions[] for the
// same identity used to double-render the mention. Under the metadata-only
// contract the adapter never folds mentions[] into body, so this case posts
// body verbatim with exactly one rendered @-mention.
test('publisher.post with both body markup AND opts.mentions does not double-render', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedSendMessage(104)
      const adapter = yield* buildAdapter()
      yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('@**alice** wake up'), {
        mentions: [aliceBot],
      })
      const params = new URLSearchParams(findRequest('POST', '/api/v1/messages').body)
      expect(params.get('content')).toBe('@**alice** wake up')
    }),
  ))

test('publisher.post drops opts.replyTo silently (Zulip has no in-topic reply primitive)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedSendMessage(102)
      const adapter = yield* buildAdapter()
      const parent: MessageRef = {
        id: decodeMessageIdSync('1'),
        channel: generalChannel,
        thread: { name: decodeThreadNameSync('ass-zsd9') },
      }
      yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('still here'), {
        replyTo: parent,
      })
      const params = new URLSearchParams(findRequest('POST', '/api/v1/messages').body)
      expect(params.get('content')).toBe('still here')
      expect(params.has('reply_to')).toBe(false)
    }),
  ))

// Pre-flight invariants for the publisher.post path — substrate-specific
// because Zulip silently routes "channel doesn't exist" to Notification Bot
// DMs and returns a success-shaped reply. The adapter must catch this with
// a GET /streams check before issuing POST /messages so callers see
// UnknownChannel instead of a silent void-send.
test('publisher.post pre-flights GET /streams before issuing POST /messages', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedSendMessage(42)
      const adapter = yield* buildAdapter()
      yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('hello world'))
      const streamsIndex = realm.captured.findIndex(
        (r) => r.method === 'GET' && r.url.pathname === '/api/v1/streams',
      )
      const messagesIndex = realm.captured.findIndex(
        (r) => r.method === 'POST' && r.url.pathname === '/api/v1/messages',
      )
      expect(streamsIndex).toBeGreaterThanOrEqual(0)
      expect(messagesIndex).toBeGreaterThanOrEqual(0)
      expect(streamsIndex).toBeLessThan(messagesIndex)
    }),
  ))

test('publisher.post refreshes /streams once on cache miss before throwing UnknownChannel', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      let streamsCallCount = 0
      realm.handle('GET', '/api/v1/streams', () => {
        streamsCallCount++
        return { body: { result: 'success', streams: [] } }
      })
      const adapter = yield* buildAdapter()
      const error = yield* Effect.flip(
        adapter.publisher.post(generalChannel, decodeMessageBodySync('should fail')),
      )
      expect(error).toBeInstanceOf(UnknownChannel)
      expect(streamsCallCount).toBe(2)
    }),
  ))

test('publisher.post does not issue POST /messages when pre-flight rejects the channel', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', '/api/v1/streams', () => ({
        body: { result: 'success', streams: [] },
      }))
      realm.handle('POST', '/api/v1/messages', () => ({
        body: { result: 'success', id: 999 },
      }))
      const adapter = yield* buildAdapter()
      const error = yield* Effect.flip(
        adapter.publisher.post(generalChannel, decodeMessageBodySync('should fail')),
      )
      expect(error).toBeInstanceOf(UnknownChannel)
      expect(
        realm.captured.find((r) => r.method === 'POST' && r.url.pathname === '/api/v1/messages'),
      ).toBeUndefined()
    }),
  ))

// Typed failure channel for publisher.post (comms-oalxg). A substrate failure
// on either the pre-flight `/streams` fetch or the `POST /messages` call is
// wrapped in a typed PublisherError carrying the cause (core stays
// substrate-agnostic — it never names ZulipApiError); an unknown channel is a
// tagged UnknownChannel; calling before acquire is a defect, not a typed
// failure (the bound-creds invariant is the caller's bug to fix).
test('publisher.post wraps a POST /messages failure as a PublisherError (cause preserved)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', '/api/v1/streams', () => ({
        body: { result: 'success', streams: [{ stream_id: 1234, name: 'general' }] },
      }))
      realm.handle('POST', '/api/v1/messages', () => ({
        body: { result: 'error', msg: 'internal server error' },
        init: { status: 500 },
      }))
      const adapter = yield* buildAdapter()
      const error = yield* Effect.flip(
        adapter.publisher.post(generalChannel, decodeMessageBodySync('boom')),
      )
      expect(error).toBeInstanceOf(PublisherError)
      if (error instanceof PublisherError) {
        expect(error.operation).toBe('post')
        expect(error.cause).toBeInstanceOf(ZulipApiError)
        // The cause's detail survives to the MCP edge — `${name}: ${message}`
        // stays informative instead of regressing to a bare "PublisherError:".
        expect(error.message).toBe('internal server error')
      }
    }),
  ))

test('publisher.post fails with a tagged UnknownChannel on an unknown channel', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', '/api/v1/streams', () => ({
        body: { result: 'success', streams: [] },
      }))
      const adapter = yield* buildAdapter()
      const error = yield* Effect.flip(
        adapter.publisher.post(generalChannel, decodeMessageBodySync('should fail')),
      )
      expect(error).toBeInstanceOf(UnknownChannel)
      if (error instanceof UnknownChannel) {
        expect(error._tag).toBe('UnknownChannel')
        expect(error.message).toContain('general')
      }
    }),
  ))

test('publisher.edit PATCHes /messages/{id} with the new content', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('PATCH', '/api/v1/messages/42', () => ({
        body: { result: 'success' },
      }))
      const adapter = yield* buildAdapter()
      const target: MessageRef = { id: decodeMessageIdSync('42'), channel: generalChannel }
      yield* adapter.publisher.edit(target, decodeMessageBodySync('replacement body'))
      const req = findRequest('PATCH', '/api/v1/messages/42')
      expect(req.method).toBe('PATCH')
      const params = new URLSearchParams(req.body)
      expect(params.get('content')).toBe('replacement body')
    }),
  ))

test('publisher.edit propagates ZulipApiError on permission failure', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('PATCH', '/api/v1/messages/42', () => ({
        body: { result: 'error', msg: 'not allowed', code: 'BAD_REQUEST' },
        init: { status: 400 },
      }))
      const adapter = yield* buildAdapter()
      const target: MessageRef = { id: decodeMessageIdSync('42'), channel: generalChannel }
      const err = yield* Effect.flip(adapter.publisher.edit(target, decodeMessageBodySync('nope')))
      expect(err).toBeInstanceOf(PublisherError)
      expect((err as { cause: unknown }).cause).toBeInstanceOf(ZulipApiError)
    }),
  ))

test('publisher.react POSTs /messages/{id}/reactions with emoji_name', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('POST', '/api/v1/messages/42/reactions', () => ({
        body: { result: 'success' },
      }))
      const adapter = yield* buildAdapter()
      const target: MessageRef = { id: decodeMessageIdSync('42'), channel: generalChannel }
      yield* adapter.publisher.react(target, decodeEmojiSync('thumbs_up'))
      const params = new URLSearchParams(findRequest('POST', '/api/v1/messages/42/reactions').body)
      expect(params.get('emoji_name')).toBe('thumbs_up')
    }),
  ))

test('publisher.unreact DELETEs /messages/{id}/reactions with emoji_name', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('DELETE', '/api/v1/messages/42/reactions', () => ({
        body: { result: 'success' },
      }))
      const adapter = yield* buildAdapter()
      const target: MessageRef = { id: decodeMessageIdSync('42'), channel: generalChannel }
      yield* adapter.publisher.unreact(target, decodeEmojiSync('thumbs_up'))
      const req = findRequest('DELETE', '/api/v1/messages/42/reactions')
      const params = new URLSearchParams(req.body)
      expect(req.method).toBe('DELETE')
      expect(params.get('emoji_name')).toBe('thumbs_up')
    }),
  ))

const seedMessages = (messages: ReadonlyArray<Record<string, unknown>>): void => {
  realm.handle('GET', '/api/v1/messages', () => ({
    body: {
      result: 'success',
      messages,
      anchor: 0,
      found_anchor: false,
      found_newest: true,
      found_oldest: false,
      history_limited: false,
    },
  }))
}

test('history.readChannel narrows by channel and maps each message to the port shape', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, GRAEME])
      seedMessages([
        {
          id: 555,
          sender_id: 5,
          sender_full_name: 'Graeme Foster',
          stream_id: 1234,
          display_recipient: 'general',
          subject: 'lobby',
          content: 'hi all',
          timestamp: 1715000000,
        },
      ])
      const messages = yield* adapter.history.readChannel(generalChannel, { limit: 50 })
      expect(messages).toHaveLength(1)
      expect(messages[0]).toEqual({
        ref: {
          id: decodeMessageIdSync('555'),
          channel: generalChannel,
          thread: { name: decodeThreadNameSync('lobby') },
        },
        sender: {
          id: decodeIdentityIdSync('5'),
          name: decodeDisplayNameSync('Graeme Foster'),
          kind: 'human',
        },
        body: decodeMessageBodySync('hi all'),
        ts: decodeTimestampSync(1715000000),
        mentions: [],
        reactions: [],
      })
    }),
  ))

test('history.readChannel sends narrow=[channel] with anchor=newest + num_before=range.limit', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, GRAEME])
      seedMessages([])
      yield* adapter.history.readChannel(generalChannel, { limit: 25 })
      const req = findRequest('GET', '/api/v1/messages')
      expect(req.url.searchParams.get('anchor')).toBe('newest')
      expect(req.url.searchParams.get('num_before')).toBe('25')
      expect(req.url.searchParams.get('num_after')).toBe('0')
      const narrow = JSON.parse(req.url.searchParams.get('narrow') ?? 'null') as unknown
      expect(narrow).toEqual([{ operator: 'channel', operand: 'general' }])
    }),
  ))

test('history.readChannel resolves bot senders to kind=agent via the user directory', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, GRAEME])
      seedMessages([
        {
          id: 600,
          sender_id: 9,
          sender_full_name: 'hermes-agent',
          stream_id: 1234,
          display_recipient: 'general',
          subject: 'lobby',
          content: 'reporting in',
          timestamp: 1715000100,
        },
      ])
      const [msg] = yield* adapter.history.readChannel(generalChannel, { limit: 10 })
      expect(msg?.sender.kind).toBe('agent')
    }),
  ))

test('history.readChannel resolves deactivated bot senders to kind=agent', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      const deactivatedBot = { ...RIQ, is_active: false }
      seedUsers([HERMES, GRAEME, deactivatedBot])
      seedMessages([
        {
          id: 700,
          sender_id: deactivatedBot.user_id,
          sender_full_name: deactivatedBot.full_name,
          stream_id: 1234,
          display_recipient: 'general',
          subject: 'lobby',
          content: 'old hermes message',
          timestamp: 1714000000,
        },
      ])
      const [msg] = yield* adapter.history.readChannel(generalChannel, { limit: 10 })
      expect(msg?.sender.kind).toBe('agent')
      expect(msg?.sender.id).toBe(decodeIdentityIdSync(String(deactivatedBot.user_id)))
    }),
  ))

test('history.readChannel filters by range.since (epoch seconds, inclusive)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, GRAEME])
      seedMessages([
        {
          id: 1,
          sender_id: 5,
          sender_full_name: 'Graeme Foster',
          stream_id: 1234,
          display_recipient: 'general',
          subject: 'a',
          content: 'old',
          timestamp: 1714000000,
        },
        {
          id: 2,
          sender_id: 5,
          sender_full_name: 'Graeme Foster',
          stream_id: 1234,
          display_recipient: 'general',
          subject: 'a',
          content: 'mid',
          timestamp: 1715000000,
        },
        {
          id: 3,
          sender_id: 5,
          sender_full_name: 'Graeme Foster',
          stream_id: 1234,
          display_recipient: 'general',
          subject: 'a',
          content: 'new',
          timestamp: 1716000000,
        },
      ])
      const messages = yield* adapter.history.readChannel(generalChannel, {
        since: decodeTimestampSync(1715000000),
        limit: 50,
      })
      expect(messages.map((m) => m.body)).toEqual([
        decodeMessageBodySync('mid'),
        decodeMessageBodySync('new'),
      ])
    }),
  ))

test('history.readChannel filters by range.until (epoch seconds, inclusive)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, GRAEME])
      seedMessages([
        {
          id: 1,
          sender_id: 5,
          sender_full_name: 'Graeme Foster',
          stream_id: 1234,
          display_recipient: 'general',
          subject: 'a',
          content: 'old',
          timestamp: 1714000000,
        },
        {
          id: 2,
          sender_id: 5,
          sender_full_name: 'Graeme Foster',
          stream_id: 1234,
          display_recipient: 'general',
          subject: 'a',
          content: 'mid',
          timestamp: 1715000000,
        },
        {
          id: 3,
          sender_id: 5,
          sender_full_name: 'Graeme Foster',
          stream_id: 1234,
          display_recipient: 'general',
          subject: 'a',
          content: 'new',
          timestamp: 1716000000,
        },
      ])
      const messages = yield* adapter.history.readChannel(generalChannel, {
        until: decodeTimestampSync(1715000000),
        limit: 50,
      })
      expect(messages.map((m) => m.body)).toEqual([
        decodeMessageBodySync('old'),
        decodeMessageBodySync('mid'),
      ])
    }),
  ))

test('history.readThread narrows by both channel and topic', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, GRAEME])
      seedMessages([])
      yield* adapter.history.readThread(generalChannel, decodeThreadNameSync('ass-zsd9'), {
        limit: 10,
      })
      const req = findRequest('GET', '/api/v1/messages')
      const narrow = JSON.parse(req.url.searchParams.get('narrow') ?? 'null') as unknown
      expect(narrow).toEqual([
        { operator: 'channel', operand: 'general' },
        { operator: 'topic', operand: 'ass-zsd9' },
      ])
    }),
  ))

test('history.readChannel with no limit defaults num_before to 100', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, GRAEME])
      seedMessages([])
      yield* adapter.history.readChannel(generalChannel, {})
      const req = findRequest('GET', '/api/v1/messages')
      expect(req.url.searchParams.get('num_before')).toBe('100')
    }),
  ))

test('history.recentThreads queries by sender and deduplicates per thread (comms-esu)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, GRAEME])
      seedMessages([
        {
          id: 101,
          sender_id: HERMES.user_id,
          sender_full_name: HERMES.full_name,
          stream_id: 1234,
          display_recipient: 'project-x',
          subject: 'feature-request',
          content: 'latest reply',
          timestamp: 1715000200,
        },
        {
          id: 100,
          sender_id: HERMES.user_id,
          sender_full_name: HERMES.full_name,
          stream_id: 1234,
          display_recipient: 'project-x',
          subject: 'feature-request',
          content: 'earlier reply',
          timestamp: 1715000100,
        },
        {
          id: 99,
          sender_id: HERMES.user_id,
          sender_full_name: HERMES.full_name,
          stream_id: 5678,
          display_recipient: 'ops',
          subject: 'deploy-issue',
          content: 'investigating',
          timestamp: 1715000050,
        },
      ])
      const threads = yield* adapter.history.recentThreads(
        decodeIdentityIdSync(String(HERMES.user_id)),
      )
      expect(threads).toHaveLength(2)
      expect(threads[0]).toEqual({
        channel: decodeChannelNameSync('project-x'),
        thread: decodeThreadNameSync('feature-request'),
        lastPostTs: decodeTimestampSync(1715000200),
        lastPostBody: decodeMessageBodySync('latest reply'),
      })
      expect(threads[1]).toEqual({
        channel: decodeChannelNameSync('ops'),
        thread: decodeThreadNameSync('deploy-issue'),
        lastPostTs: decodeTimestampSync(1715000050),
        lastPostBody: decodeMessageBodySync('investigating'),
      })
      const req = findRequest('GET', '/api/v1/messages')
      const narrow = JSON.parse(req.url.searchParams.get('narrow') ?? 'null') as ReadonlyArray<{
        operator: string
        operand: unknown
      }>
      // Zulip's `sender` narrow operand must be the INTEGER user id. A numeric
      // string ("9") is rejected as BAD_NARROW "unknown user 9" (comms-wpp).
      expect(narrow).toEqual([{ operator: 'sender', operand: HERMES.user_id }])
      expect(typeof narrow[0]?.operand).toBe('number')
    }),
  ))

test('history.recentThreads short-circuits to [] when the sender is not a known directory member (comms-7ee)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES])
      // identity '999' resolves to no ZulipUserRef, so there is no integer the
      // `sender` narrow could use — return [] rather than query with an operand
      // Zulip would reject.
      const threads = yield* adapter.history.recentThreads(decodeIdentityIdSync('999'))
      expect(threads).toEqual([])
      expect(realm.captured.some((r) => r.url.pathname === '/api/v1/messages')).toBe(false)
    }),
  ))

const seedPresence = (userId: string, aggregatedStatus: 'active' | 'idle' | 'offline'): void => {
  realm.handle('GET', `/api/v1/users/${userId}/presence`, () => ({
    body: {
      result: 'success',
      msg: '',
      server_timestamp: 1715000000,
      presence: {
        aggregated: { status: aggregatedStatus, timestamp: 1715000000 },
      },
    },
  }))
}

test('directory.presence maps aggregated status=active to online', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedPresence(bobHuman.id, 'active')
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, BOB])
      const presence = yield* adapter.directory.presence(bobHuman)
      expect(presence).toBe('online')
    }),
  ))

test('directory.presence maps aggregated status=idle to idle', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedPresence(bobHuman.id, 'idle')
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, BOB])
      const presence = yield* adapter.directory.presence(bobHuman)
      expect(presence).toBe('idle')
    }),
  ))

test('directory.presence maps aggregated status=offline to offline', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedPresence(bobHuman.id, 'offline')
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, BOB])
      const presence = yield* adapter.directory.presence(bobHuman)
      expect(presence).toBe('offline')
    }),
  ))

test('directory.presence returns offline when user has never set presence (Zulip 400 BAD_REQUEST)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', `/api/v1/users/${bobHuman.id}/presence`, () => ({
        body: { result: 'error', msg: `No presence data for ${bobHuman.id}`, code: 'BAD_REQUEST' },
        init: { status: 400 },
      }))
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, BOB])
      const presence = yield* adapter.directory.presence(bobHuman)
      expect(presence).toBe('offline')
    }),
  ))

test('directory.presence returns offline when the user no longer exists (Zulip 400 BAD_REQUEST: No such user)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', `/api/v1/users/${bobHuman.id}/presence`, () => ({
        body: { result: 'error', msg: 'No such user', code: 'BAD_REQUEST' },
        init: { status: 400 },
      }))
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, BOB])
      const presence = yield* adapter.directory.presence(bobHuman)
      expect(presence).toBe('offline')
    }),
  ))

test('directory.presence surfaces non-BAD_REQUEST errors as DirectoryError (cause preserved)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      // A non-BAD_REQUEST error must propagate (only the benign "no presence
      // data" 400 is swallowed to offline). Use a 500 rather than a 429 — the
      // adapter now rides out 429s internally (comms-nbz), so a 429 is no
      // longer an error the presence path surfaces.
      realm.handle('GET', `/api/v1/users/${bobHuman.id}/presence`, () => ({
        body: { result: 'error', msg: 'internal server error', code: 'INTERNAL_ERROR' },
        init: { status: 500 },
      }))
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, BOB])
      const error = yield* Effect.flip(adapter.directory.presence(bobHuman))
      expect(error).toBeInstanceOf(DirectoryError)
      expect(error.cause).toBeInstanceOf(ZulipApiError)
    }),
  ))

test('directory.presence short-circuits to offline for an identity that is not a known directory member (comms-7ee)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES])
      const stranger: Identity = {
        id: decodeIdentityIdSync('999'),
        name: decodeDisplayNameSync('stranger'),
        kind: 'human',
      }
      const presence = yield* adapter.directory.presence(stranger)
      expect(presence).toBe('offline')
      // No ZulipUserRef means no resolvable user — don't fire a doomed presence GET.
      expect(realm.captured.some((r) => r.url.pathname === '/api/v1/users/999/presence')).toBe(
        false,
      )
    }),
  ))

test("directory.presence returns 'unknown' for an agent identity without reading Zulip presence (comms-1mnb)", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      // Zulip presence is human-only by design (POST /users/me/presence is
      // @human_users_only), so a bot has no presence concept. Reading it would
      // 400 and collapse to 'offline', which lies about a bot we simply cannot
      // know. Short-circuit agents to 'unknown' before any directory lookup or
      // presence GET — even for a bot that IS a known directory member.
      seedPresence('11', 'active')
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, RIQ])
      const riqBot: Identity = {
        id: decodeIdentityIdSync('11'),
        name: decodeDisplayNameSync('riq6r230'),
        kind: 'agent',
      }
      const presence = yield* adapter.directory.presence(riqBot)
      expect(presence).toBe('unknown')
      expect(realm.captured.some((r) => r.url.pathname === '/api/v1/users/11/presence')).toBe(false)
    }),
  ))

// ─── comms-fpa: bots never write their own presence ─────────────────────────
// Zulip's POST /users/me/presence is @human_users_only, so a bot self-presence
// write is structurally impossible (it 400s with "does not accept bot
// requests"). The adapter therefore exposes no presence-write path. This guards
// against the bot self-presence heartbeat (the deleted comms-6li feature) being
// wired back in. The presence READ — directory.presence, above — stays: a bot
// reading a human's presence is supported.

test('a bound adapter never writes its own presence (comms-fpa)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      const presencePosts = realm.captured.filter(
        (r) => r.method === 'POST' && r.url.pathname === '/api/v1/users/me/presence',
      )
      expect(presencePosts).toHaveLength(0)
      yield* Effect.promise(() => adapter.close())
    }),
  ))

const seedRegisterOk = (queueId = 'q1', lastEventId = 0): void => {
  realm.handle('POST', '/api/v1/register', () => ({
    body: { result: 'success', queue_id: queueId, last_event_id: lastEventId },
  }))
}

const seedSubscribeOk = (channelName: string): void => {
  realm.handle('POST', '/api/v1/users/me/subscriptions', () => ({
    body: {
      result: 'success',
      subscribed: { 'hermes-agent-bot@example.com': [channelName] },
      already_subscribed: {},
      unauthorized: [],
    },
  }))
  // subscribe() also POSTs /register to satisfy the port's readiness
  // contract (subscribe resolved → events() observes subsequent posts).
  seedRegisterOk()
}

const seedUnsubscribeOk = (channelName: string): void => {
  realm.handle('DELETE', '/api/v1/users/me/subscriptions', () => ({
    body: { result: 'success', removed: [channelName], not_removed: [] },
  }))
}

test('inbox.subscribe(channel) POSTs /users/me/subscriptions with [{ name }]', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedSubscribeOk('general')
      const adapter = yield* buildAdapter()
      yield* adapter.inbox.subscribe(generalChannel)
      const req = findRequest('POST', '/api/v1/users/me/subscriptions')
      const params = new URLSearchParams(req.body)
      const subs = JSON.parse(params.get('subscriptions') ?? '[]') as unknown
      expect(subs).toEqual([{ name: 'general' }])
    }),
  ))

test('inbox.subscribe is a no-op for already_subscribed', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('POST', '/api/v1/users/me/subscriptions', () => ({
        body: {
          result: 'success',
          subscribed: {},
          already_subscribed: { 'hermes-agent-bot@example.com': ['general'] },
          unauthorized: [],
        },
      }))
      seedRegisterOk()
      const adapter = yield* buildAdapter()
      yield* adapter.inbox.subscribe(generalChannel)
    }),
  ))

test('inbox.subscribe with thread subscribes to its underlying channel', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedSubscribeOk('general')
      const adapter = yield* buildAdapter()
      yield* adapter.inbox.subscribe({
        channel: generalChannel,
        thread: { name: decodeThreadNameSync('design') },
      })
      const req = findRequest('POST', '/api/v1/users/me/subscriptions')
      const subs = JSON.parse(new URLSearchParams(req.body).get('subscriptions') ?? '[]') as unknown
      expect(subs).toEqual([{ name: 'general' }])
    }),
  ))

test('inbox.subscribe with mentions target does not call /users/me/subscriptions', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedRegisterOk()
      const adapter = yield* buildAdapter()
      yield* adapter.inbox.subscribe('mentions')
      expect(
        realm.captured.find(
          (r) => r.method === 'POST' && r.url.pathname === '/api/v1/users/me/subscriptions',
        ),
      ).toBeUndefined()
    }),
  ))

test('inbox.unsubscribe(channel) DELETEs /users/me/subscriptions with the stream name', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUnsubscribeOk('general')
      const adapter = yield* buildAdapter()
      yield* adapter.inbox.unsubscribe(generalChannel)
      const req = findRequest('DELETE', '/api/v1/users/me/subscriptions')
      const params = new URLSearchParams(req.body)
      const subs = JSON.parse(params.get('subscriptions') ?? '[]') as unknown
      expect(subs).toEqual(['general'])
    }),
  ))

test('inbox.unsubscribe with mentions target does not call /users/me/subscriptions', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      yield* adapter.inbox.unsubscribe('mentions')
      expect(
        realm.captured.find(
          (r) => r.method === 'DELETE' && r.url.pathname === '/api/v1/users/me/subscriptions',
        ),
      ).toBeUndefined()
    }),
  ))

interface RawZulipEvent {
  readonly id: number
  readonly type: string
  readonly [key: string]: unknown
}

const seedEventBatches = (batches: ReadonlyArray<ReadonlyArray<RawZulipEvent>>): void => {
  let idx = 0
  realm.handle('GET', '/api/v1/events', () => {
    const events = batches[idx] ?? []
    if (idx < batches.length) idx += 1
    return { body: { result: 'success', events } }
  })
}

const aZulipMessage = (
  overrides: Partial<{
    id: number
    sender_id: number
    sender_full_name: string
    stream_id: number
    display_recipient: string
    subject: string
    content: string
    timestamp: number
  }> = {},
): Record<string, unknown> => ({
  id: 100,
  sender_id: 5,
  sender_full_name: 'Graeme Foster',
  stream_id: 1234,
  display_recipient: 'general',
  subject: 'lobby',
  content: 'hello',
  timestamp: 1715000000,
  ...overrides,
})

test('inbox.events first .next() yields a message-posted InboundEvent for a Zulip message event', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* buildAdapter()
        seedUsers([HERMES, GRAEME])
        seedRegisterOk('queue-1', 0)
        seedEventBatches([[{ id: 1, type: 'message', message: aZulipMessage(), flags: [] }]])
        const queue = yield* eventQueue(adapter)
        const event = yield* Queue.take(queue)
        expect(event).toEqual({
          kind: 'message-posted',
          message: {
            ref: {
              id: decodeMessageIdSync('100'),
              channel: { id: decodeChannelIdSync('1234'), name: decodeChannelNameSync('general') },
              thread: { name: decodeThreadNameSync('lobby') },
            },
            sender: {
              id: decodeIdentityIdSync('5'),
              name: decodeDisplayNameSync('Graeme Foster'),
              kind: 'human',
            },
            body: decodeMessageBodySync('hello'),
            ts: decodeTimestampSync(1715000000),
            mentions: [],
            reactions: [],
          },
        })
      }),
    ),
  ))

test('inbox.events surfaces mention-received in addition to message-posted when the bound bot is mentioned in the content', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* buildAdapter()
        seedUsers([HERMES, GRAEME])
        seedRegisterOk('queue-1', 0)
        seedEventBatches([
          [
            {
              id: 7,
              type: 'message',
              message: aZulipMessage({ content: '@**hermes-agent** wake up' }),
              flags: [],
            },
          ],
        ])
        const queue = yield* eventQueue(adapter)
        const first = yield* Queue.take(queue)
        const second = yield* Queue.take(queue)
        expect(first.kind).toBe('message-posted')
        expect(second.kind).toBe('mention-received')
        if (second.kind === 'mention-received') {
          expect(second.mentions.map((i: Identity) => i.name)).toEqual([
            decodeDisplayNameSync('hermes-agent'),
          ])
        }
      }),
    ),
  ))

test('inbox.events advances last_event_id between long-poll calls', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* buildAdapter()
        seedUsers([HERMES, GRAEME])
        seedRegisterOk('queue-1', 0)
        seedEventBatches([
          [
            {
              id: 5,
              type: 'message',
              message: aZulipMessage({ id: 100, content: 'first' }),
              flags: [],
            },
          ],
          [
            {
              id: 11,
              type: 'message',
              message: aZulipMessage({ id: 200, content: 'second' }),
              flags: [],
            },
          ],
        ])
        const queue = yield* eventQueue(adapter)
        yield* Queue.take(queue)
        yield* Queue.take(queue)
        const eventCalls = realm.captured.filter(
          (r) => r.method === 'GET' && r.url.pathname === '/api/v1/events',
        )
        expect(eventCalls.length).toBeGreaterThanOrEqual(2)
        const [first, second] = eventCalls
        if (first === undefined || second === undefined) throw new Error('expected 2 event calls')
        expect(first.url.searchParams.get('last_event_id')).toBe('0')
        expect(second.url.searchParams.get('last_event_id')).toBe('5')
      }),
    ),
  ))

test('inbox.events ignores Zulip heartbeat events but advances last_event_id', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* buildAdapter()
        seedUsers([HERMES, GRAEME])
        seedRegisterOk('queue-1', 0)
        seedEventBatches([
          [{ id: 17, type: 'heartbeat' }],
          [{ id: 18, type: 'message', message: aZulipMessage(), flags: [] }],
        ])
        const queue = yield* eventQueue(adapter)
        const event = yield* Queue.take(queue)
        expect(event.kind).toBe('message-posted')
        const eventCalls = realm.captured.filter(
          (r) => r.method === 'GET' && r.url.pathname === '/api/v1/events',
        )
        expect(eventCalls.length).toBeGreaterThanOrEqual(2)
        const secondCall = eventCalls[1]
        if (secondCall === undefined) throw new Error('expected 2 event calls')
        expect(secondCall.url.searchParams.get('last_event_id')).toBe('17')
      }),
    ),
  ))

test('inbox.events re-registers and resumes when /events returns BAD_EVENT_QUEUE_ID', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const adapter = yield* buildAdapter()
        seedUsers([HERMES, GRAEME])
        let registerCalls = 0
        realm.handle('POST', '/api/v1/register', () => {
          registerCalls += 1
          return {
            body: { result: 'success', queue_id: `q${registerCalls}`, last_event_id: 0 },
          }
        })
        let eventsCalls = 0
        realm.handle('GET', '/api/v1/events', () => {
          eventsCalls += 1
          if (eventsCalls === 1) {
            return {
              body: { result: 'error', code: 'BAD_EVENT_QUEUE_ID', msg: 'queue expired' },
              init: { status: 400 },
            }
          }
          return {
            body: {
              result: 'success',
              events: [{ id: 1, type: 'message', message: aZulipMessage(), flags: [] }],
            },
          }
        })
        const queue = yield* eventQueue(adapter)
        const event = yield* Queue.take(queue)
        expect(event.kind).toBe('message-posted')
        expect(registerCalls).toBe(2)
      }),
    ),
  ))

test('inbox.events backfills the gap via inbox.replay() on BAD_EVENT_QUEUE_ID and marks events replayed=true (comms-jnn)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // End-to-end through the real adapter: live message at ts=1000 sets the
        // watermark, BAD_EVENT_QUEUE_ID dies the queue, the iterator calls
        // adapter.replay(since=1000) which hits /messages and returns a message
        // posted at ts=1500 during the dead window, and a fresh live message at
        // ts=2000 follows on the new queue. The middle event must surface with
        // replayed=true; the live ones must not.
        const adapter = yield* buildAdapter()
        seedUsers([HERMES, GRAEME])
        let registerCalls = 0
        realm.handle('POST', '/api/v1/register', () => {
          registerCalls += 1
          return {
            body: { result: 'success', queue_id: `q${registerCalls}`, last_event_id: 0 },
          }
        })
        realm.handle('GET', '/api/v1/messages', () => ({
          body: {
            result: 'success',
            messages: [
              {
                id: 150,
                sender_id: GRAEME.user_id,
                sender_full_name: GRAEME.full_name,
                stream_id: 1234,
                display_recipient: 'general',
                subject: 'lobby',
                content: 'posted during the gap',
                timestamp: 1500,
                flags: [],
              },
            ],
            anchor: 0,
            found_anchor: false,
            found_newest: true,
            found_oldest: false,
            history_limited: false,
          },
        }))
        let eventsCalls = 0
        realm.handle('GET', '/api/v1/events', () => {
          eventsCalls += 1
          if (eventsCalls === 1) {
            return {
              body: {
                result: 'success',
                events: [
                  {
                    id: 1,
                    type: 'message',
                    message: aZulipMessage({ id: 100, timestamp: 1000 }),
                    flags: [],
                  },
                ],
              },
            }
          }
          if (eventsCalls === 2) {
            return {
              body: { result: 'error', code: 'BAD_EVENT_QUEUE_ID', msg: 'queue expired' },
              init: { status: 400 },
            }
          }
          return {
            body: {
              result: 'success',
              events: [
                {
                  id: 1,
                  type: 'message',
                  message: aZulipMessage({ id: 200, timestamp: 2000 }),
                  flags: [],
                },
              ],
            },
          }
        })
        const queue = yield* eventQueue(adapter)
        const collected: InboundEvent[] = []
        for (let i = 0; i < 3; i += 1) {
          collected.push(yield* Queue.take(queue))
        }
        expect(collected).toHaveLength(3)
        expect(collected[0]?.kind).toBe('message-posted')
        if (collected[0]?.kind === 'message-posted') {
          expect(collected[0].replayed).toBeUndefined()
          expect(String(collected[0].message.ref.id)).toBe('100')
        }
        expect(collected[1]?.kind).toBe('message-posted')
        if (collected[1]?.kind === 'message-posted') {
          expect(collected[1].replayed).toBe(true)
          expect(String(collected[1].message.ref.id)).toBe('150')
          expect(collected[1].message.body).toBe(decodeMessageBodySync('posted during the gap'))
        }
        expect(collected[2]?.kind).toBe('message-posted')
        if (collected[2]?.kind === 'message-posted') {
          expect(collected[2].replayed).toBeUndefined()
          expect(String(collected[2].message.ref.id)).toBe('200')
        }
        expect(registerCalls).toBe(2)
      }),
    ),
  ))

// Per-test wall clock (comms-9tik). Unlike the fakeHttp iterator tests in
// events.test.ts — which use a 2s fail-fast wall because their fake resolves
// instantly — this test drives the REAL in-process Bun.serve realm across
// ~7 sequential round-trips (users, two register/events pairs, a replay, a
// final live poll). Every wait here is condition-gated (`Queue.take` blocks on
// the forked drain producing an event); there is NO artificial sleep or
// Schedule backoff in the path — BAD_EVENT_QUEUE_ID is handled inline without
// retry — so TestClock has nothing to virtualise. The cost is irreducible real
// local I/O plus fiber scheduling, all sharing one starved event loop. In
// isolation the body runs ~60ms, but under a full-parallel `bun run check`
// (34 test files) that real work stretched to 5002ms and tripped bun's default
// 5s ceiling by 2ms. 30s clears pathological contention with wide margin while
// still failing honestly if the pump ever genuinely hangs (infinite long-poll
// loop, or scope-close abort never firing).
const GAP_REPLAY_TEST_TIMEOUT_MS = 30_000

test(
  'inbox.events persists the gap-replay watermark across iterator instances so reconnect-then-BAD_EVENT_QUEUE_ID still backfills (comms-4au)',
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        // event-pump auto-reconnect (comms-ynb) restarts the iterator on
        // transient errors, but the gap-replay watermark used to live in
        // the iterator's closure — iter2's first poll had no watermark to
        // anchor the replay (comms-jnn), and the very poll where replay
        // mattered most was the one that skipped it. The watermark now
        // lives at the adapter so iter2 inherits iter1's last-seen ts and
        // BAD_EVENT_QUEUE_ID on the new iterator fires the replay.
        const adapter = yield* buildAdapter()
        seedUsers([HERMES, GRAEME])
        let registerCalls = 0
        realm.handle('POST', '/api/v1/register', () => {
          registerCalls += 1
          return {
            body: { result: 'success', queue_id: `q${registerCalls}`, last_event_id: 0 },
          }
        })
        realm.handle('GET', '/api/v1/messages', () => ({
          body: {
            result: 'success',
            messages: [
              {
                id: 150,
                sender_id: GRAEME.user_id,
                sender_full_name: GRAEME.full_name,
                stream_id: 1234,
                display_recipient: 'general',
                subject: 'lobby',
                content: 'posted during the reconnect gap',
                timestamp: 1500,
                flags: [],
              },
            ],
            anchor: 0,
            found_anchor: false,
            found_newest: true,
            found_oldest: false,
            history_limited: false,
          },
        }))
        // Sequence:
        //   call 1 (queue1) — one live message at ts=1000 (advances watermark)
        //   call 2 (queue1) — Stream.runDrain pulls eagerly past the consumer's
        //                     first take; long-poll forever so the BAD_QUEUE
        //                     meant for queue2 is not consumed by queue1's race.
        //                     queue1's scope close interrupts the consumer fiber
        //                     and unwinds this fetch via AbortSignal.
        //   call 3 (queue2) — BAD_EVENT_QUEUE_ID — queue2's first poll hits the
        //                     replay path.
        //   call 4+ (queue2) — live ts=2000 on the re-registered queue
        let eventsCalls = 0
        realm.handle('GET', '/api/v1/events', async () => {
          eventsCalls += 1
          if (eventsCalls === 1) {
            return {
              body: {
                result: 'success',
                events: [
                  {
                    id: 1,
                    type: 'message',
                    message: aZulipMessage({ id: 100, timestamp: 1000 }),
                    flags: [],
                  },
                ],
              },
            }
          }
          if (eventsCalls === 2) {
            await new Promise<void>(() => {})
            return { body: { result: 'success', events: [] } }
          }
          if (eventsCalls === 3) {
            return {
              body: { result: 'error', code: 'BAD_EVENT_QUEUE_ID', msg: 'queue expired' },
              init: { status: 400 },
            }
          }
          return {
            body: {
              result: 'success',
              events: [
                {
                  id: 1,
                  type: 'message',
                  message: aZulipMessage({ id: 200, timestamp: 2000 }),
                  flags: [],
                },
              ],
            },
          }
        })

        // queue1: pull a live message at ts=1000 (advances the watermark),
        // then close the scope to mimic the pump dropping the consumer after
        // a transient error.
        yield* Effect.scoped(
          Effect.gen(function* () {
            const queue1 = yield* eventQueue(adapter)
            const first = yield* Queue.take(queue1)
            expect(first.kind).toBe('message-posted')
            if (first.kind === 'message-posted') {
              expect(first.message.ts).toBe(decodeTimestampSync(1000))
              expect(first.replayed).toBeUndefined()
            }
          }),
        )

        // queue2: the pump-side reconnect. First poll hits BAD_EVENT_QUEUE_ID
        // because the queue was GC'd during the backoff window. The persisted
        // watermark anchors the replay at since=1000, so the message posted
        // during the gap (ts=1500) surfaces as replayed=true before the next
        // live event (ts=2000) on the freshly-registered queue.
        yield* Effect.scoped(
          Effect.gen(function* () {
            const queue2 = yield* eventQueue(adapter)
            const collected: InboundEvent[] = []
            for (let i = 0; i < 2; i += 1) {
              collected.push(yield* Queue.take(queue2))
            }
            expect(collected).toHaveLength(2)
            expect(collected[0]?.kind).toBe('message-posted')
            if (collected[0]?.kind === 'message-posted') {
              expect(collected[0].replayed).toBe(true)
              expect(collected[0].message.ts).toBe(decodeTimestampSync(1500))
              expect(collected[0].message.body).toBe(
                decodeMessageBodySync('posted during the reconnect gap'),
              )
            }
            expect(collected[1]?.kind).toBe('message-posted')
            if (collected[1]?.kind === 'message-posted') {
              expect(collected[1].replayed).toBeUndefined()
              expect(collected[1].message.ts).toBe(decodeTimestampSync(2000))
            }
          }),
        )
      }),
    ),
  GAP_REPLAY_TEST_TIMEOUT_MS,
)

test('inbox.events sleeps and retries when /events returns 429 RATE_LIMIT_HIT (comms-9wi)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // Pre-comms-9wi: a single 429 from /events bubbled out of the iterator,
        // was caught by the event-pump, and process.exit(1)'d the whole MCP
        // server — disconnecting every subscribed bot in lock-step. Recovery
        // must mirror the BAD_EVENT_QUEUE_ID pattern: honour the retry-after
        // value Zulip emits and let the outer loop try again.
        const adapter = yield* buildAdapter()
        seedUsers([HERMES, GRAEME])
        seedRegisterOk('queue-1', 0)
        let eventsCalls = 0
        realm.handle('GET', '/api/v1/events', () => {
          eventsCalls += 1
          if (eventsCalls === 1) {
            return {
              body: {
                result: 'error',
                code: 'RATE_LIMIT_HIT',
                msg: 'API usage exceeded rate limit',
                'retry-after': 0,
              },
              init: { status: 429 },
            }
          }
          return {
            body: {
              result: 'success',
              events: [{ id: 1, type: 'message', message: aZulipMessage(), flags: [] }],
            },
          }
        })
        const queue = yield* eventQueue(adapter)
        const event = yield* Queue.take(queue)
        expect(event.kind).toBe('message-posted')
        expect(eventsCalls).toBe(2)
      }),
    ),
  ))

test('inbox.events sleeps and retries when /register returns 429 RATE_LIMIT_HIT (comms-9wi)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // Same recovery semantics on the registration round-trip — the
        // initial /register is fired lazily inside the iterator when
        // subscribe() has not yet ensured a queue. A 429 there must not
        // escape the iterator either.
        const adapter = yield* buildAdapter()
        seedUsers([HERMES, GRAEME])
        let registerCalls = 0
        realm.handle('POST', '/api/v1/register', () => {
          registerCalls += 1
          if (registerCalls === 1) {
            return {
              body: {
                result: 'error',
                code: 'RATE_LIMIT_HIT',
                msg: 'API usage exceeded rate limit',
                'retry-after': 0,
              },
              init: { status: 429 },
            }
          }
          return { body: { result: 'success', queue_id: 'q1', last_event_id: 0 } }
        })
        seedEventBatches([[{ id: 1, type: 'message', message: aZulipMessage(), flags: [] }]])
        const queue = yield* eventQueue(adapter)
        const event = yield* Queue.take(queue)
        expect(event.kind).toBe('message-posted')
        expect(registerCalls).toBe(2)
      }),
    ),
  ))

test('inbox.events long-poll fiber interrupts cleanly when the consumer scope closes (comms-spj3.8)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      // A hung Zulip long-poll must not pin the pump on shutdown — fiber
      // interrupt has to abort the in-flight fetch and let the Effect.scoped
      // boundary return. The handler hangs forever; scope close interrupts
      // the underlying Stream fiber; the @effect/platform HttpClient signals
      // abort via AbortController so the fetch unwinds. Test passes iff the
      // outer scope close completes — if interrupt leaks, bun's per-test
      // timeout fires.
      const adapter = yield* buildAdapter()
      seedUsers([HERMES])
      seedRegisterOk('queue-1', 0)
      realm.handle('GET', '/api/v1/events', async () => {
        await new Promise<void>(() => {})
        return { body: { result: 'success', events: [] } }
      })
      yield* Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* eventQueue(adapter)
          // Let the long-poll get into flight before the scope unwinds.
          yield* Effect.sleep(Duration.millis(20))
          void queue
        }),
      )
      expect(
        realm.captured.some((r) => r.method === 'GET' && r.url.pathname === '/api/v1/events'),
      ).toBe(true)
    }),
  ))

test('inbox.replay(since) returns message-posted events for messages with ts >= since', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES, GRAEME])
      realm.handle('GET', '/api/v1/messages', () => ({
        body: {
          result: 'success',
          messages: [
            {
              id: 1,
              sender_id: 5,
              sender_full_name: 'Graeme Foster',
              stream_id: 100,
              display_recipient: 'general',
              subject: 'lobby',
              content: 'old',
              timestamp: 1000,
            },
            {
              id: 2,
              sender_id: 5,
              sender_full_name: 'Graeme Foster',
              stream_id: 100,
              display_recipient: 'general',
              subject: 'lobby',
              content: 'new',
              timestamp: 3000,
            },
          ],
          anchor: 0,
          found_anchor: false,
          found_newest: true,
          found_oldest: false,
          history_limited: false,
        },
      }))
      const events = yield* adapter.inbox.replay(decodeTimestampSync(2000))
      expect(events).toHaveLength(1)
      expect(events[0]?.kind).toBe('message-posted')
      if (events[0]?.kind === 'message-posted') {
        expect(events[0].message.body).toBe(decodeMessageBodySync('new'))
        expect(events[0].message.ref.channel.name).toEqual(decodeChannelNameSync('general'))
      }
    }),
  ))

test('inbox.replay calls /messages with anchor=newest and a generous num_before', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES])
      realm.handle('GET', '/api/v1/messages', () => ({
        body: {
          result: 'success',
          messages: [],
          anchor: 0,
          found_anchor: false,
          found_newest: true,
          found_oldest: false,
          history_limited: false,
        },
      }))
      yield* adapter.inbox.replay(decodeTimestampSync(0))
      const req = findRequest('GET', '/api/v1/messages')
      expect(req.url.searchParams.get('anchor')).toBe('newest')
      expect(Number(req.url.searchParams.get('num_before'))).toBeGreaterThanOrEqual(100)
      expect(req.url.searchParams.get('num_after')).toBe('0')
    }),
  ))

// The replay schema accepts the stream-message shape only — DMs in
// `/messages` responses have no stream_id, a recipient-array
// `display_recipient`, and an empty subject, so they explode the Zod
// parse. Asking Zulip to filter at the source keeps the parser strict.
test('inbox.replay narrows /messages to exclude DMs so PMs in minter history do not crash the parser', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES])
      realm.handle('GET', '/api/v1/messages', () => ({
        body: {
          result: 'success',
          messages: [],
          anchor: 0,
          found_anchor: false,
          found_newest: true,
          found_oldest: false,
          history_limited: false,
        },
      }))
      yield* adapter.inbox.replay(decodeTimestampSync(0))
      const req = findRequest('GET', '/api/v1/messages')
      const narrowRaw = req.url.searchParams.get('narrow')
      if (narrowRaw === null) throw new Error('expected narrow param on /messages request')
      const narrow = JSON.parse(narrowRaw) as ReadonlyArray<unknown>
      const excludesDms = narrow.some((entry) => {
        if (typeof entry !== 'object' || entry === null) return false
        const o = entry as Record<string, unknown>
        return o['operator'] === 'is' && o['operand'] === 'dm' && o['negated'] === true
      })
      expect(excludesDms).toBe(true)
    }),
  ))

test('inbox.replay surfaces mention-received alongside message-posted when flags include "mentioned"', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      seedUsers([HERMES])
      realm.handle('GET', '/api/v1/messages', () => ({
        body: {
          result: 'success',
          messages: [
            {
              id: 1,
              sender_id: 9,
              sender_full_name: 'hermes-agent',
              stream_id: 100,
              display_recipient: 'general',
              subject: 'lobby',
              content: '@**hermes-agent** wake up',
              timestamp: 5000,
              flags: ['mentioned'],
            },
          ],
          anchor: 0,
          found_anchor: false,
          found_newest: true,
          found_oldest: false,
          history_limited: false,
        },
      }))
      const events = yield* adapter.inbox.replay(decodeTimestampSync(0))
      expect(events.map((e) => e.kind)).toEqual(['message-posted', 'mention-received'])
    }),
  ))

test('inbox.events register includes narrow=[["is","mentioned"]] when subscribed mentions', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      // subscribe('mentions') eagerly registers the events queue (the
      // readiness contract — events() must see anything posted after
      // subscribe resolves). The narrow assertion lives here so the queue
      // used by events() is exclusive to mentions.
      const adapter = yield* buildAdapter()
      seedUsers([HERMES])
      seedRegisterOk('queue-1', 0)
      yield* adapter.inbox.subscribe('mentions')
      const reg = realm.captured.find(
        (r) => r.method === 'POST' && r.url.pathname === '/api/v1/register',
      )
      if (reg === undefined) throw new Error('expected captured POST /api/v1/register')
      const params = new URLSearchParams(reg.body)
      const narrow = JSON.parse(params.get('narrow') ?? 'null') as unknown
      expect(narrow).toEqual([['is', 'mentioned']])
    }),
  ))

// ─── ass-220u: pre-acquire surfaces run on minter creds ────────────────────

const decodeBasicAuth = (header: string | null): { email: string; apiKey: string } => {
  if (header === null || !header.startsWith('Basic ')) {
    throw new Error(`expected Basic auth, got ${header ?? '<absent>'}`)
  }
  const decoded = Buffer.from(header.slice('Basic '.length), 'base64').toString('utf-8')
  const idx = decoded.indexOf(':')
  if (idx < 0) throw new Error(`malformed basic auth payload: ${decoded}`)
  return { email: decoded.slice(0, idx), apiKey: decoded.slice(idx + 1) }
}

const minterAuth = { email: 'minter@example.com', apiKey: 'minter-key' }

test('history.readChannel runs pre-acquire and routes via minter creds', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([HERMES, GRAEME])
      realm.handle('GET', '/api/v1/messages', () => ({
        body: { result: 'success', messages: [] },
      }))
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.history.readChannel(generalChannel, {})
      const req = findRequest('GET', '/api/v1/messages')
      expect(decodeBasicAuth(req.headers.get('Authorization'))).toEqual(minterAuth)
    }),
  ))

test('directory.listAgents runs pre-acquire and routes via minter creds', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([HERMES, GRAEME])
      const adapter = yield* zulipAdapter(yield* makeConfig())
      const agents = yield* adapter.directory.listAgents()
      expect(agents.map((a) => a.name)).toEqual([decodeDisplayNameSync('hermes-agent')])
      const usersCalls = realm.captured.filter(
        (r) => r.method === 'GET' && r.url.pathname === '/api/v1/users',
      )
      expect(usersCalls).not.toHaveLength(0)
      for (const call of usersCalls) {
        expect(decodeBasicAuth(call.headers.get('Authorization'))).toEqual(minterAuth)
      }
    }),
  ))

// The presence read path runs for humans only (agents short-circuit to
// 'unknown', comms-1mnb), so the minter-cred routing assertion uses a human.
test('directory.presence runs pre-acquire and routes via minter creds', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([GRAEME])
      realm.handle('GET', `/api/v1/users/${GRAEME.user_id}/presence`, () => ({
        body: { result: 'success', presence: { aggregated: { status: 'active' } } },
      }))
      const adapter = yield* zulipAdapter(yield* makeConfig())
      const presence = yield* adapter.directory.presence({
        id: decodeIdentityIdSync(String(GRAEME.user_id)),
        name: decodeDisplayNameSync(GRAEME.full_name),
        kind: 'human',
      })
      expect(presence).toBe('online')
      const req = findRequest('GET', `/api/v1/users/${GRAEME.user_id}/presence`)
      expect(decodeBasicAuth(req.headers.get('Authorization'))).toEqual(minterAuth)
    }),
  ))

test('inbox.subscribe runs pre-acquire and routes /users/me/subscriptions via minter creds', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedSubscribeOk('general')
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.inbox.subscribe(generalChannel)
      const subReq = findRequest('POST', '/api/v1/users/me/subscriptions')
      expect(decodeBasicAuth(subReq.headers.get('Authorization'))).toEqual(minterAuth)
      // The /register that arms the events queue must also be minter-creds —
      // the queue belongs to the minter so lurking sessions share it.
      const regReq = findRequest('POST', '/api/v1/register')
      expect(decodeBasicAuth(regReq.headers.get('Authorization'))).toEqual(minterAuth)
    }),
  ))

test('publisher.post still requires acquire — pre-acquire call dies on the "not acquired" invariant', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedUsers([])
      const adapter = yield* zulipAdapter(yield* makeConfig())
      const exit = yield* Effect.exit(
        adapter.publisher.post(generalChannel, decodeMessageBodySync('hello')),
      )
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        expect(Cause.isDie(exit.cause)).toBe(true)
        expect(String(Cause.squash(exit.cause))).toMatch(/not acquired/)
      }
    }),
  ))

test('publisher.post after acquire uses BOUND bot creds, not minter creds', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', '/api/v1/streams', () => ({
        body: {
          result: 'success',
          streams: [{ stream_id: 1234, name: 'general' }],
        },
      }))
      realm.handle('POST', '/api/v1/messages', () => ({
        body: { result: 'success', id: 555 },
      }))
      const adapter = yield* buildAdapter() // acquires hermes-agent → rotates key to "fresh-key"
      yield* adapter.publisher.post(
        generalChannel,
        decodeMessageBodySync('attribution-producing message'),
      )
      const req = findRequest('POST', '/api/v1/messages')
      const auth = decodeBasicAuth(req.headers.get('Authorization'))
      expect(auth.email).toBe(HERMES.email)
      expect(auth.apiKey).toBe('fresh-key')
      // And it must NOT be minter creds.
      expect(auth).not.toEqual(minterAuth)
    }),
  ))

const seedStreamsList = (
  streams: ReadonlyArray<{ readonly stream_id: number; readonly name: string }>,
): void => {
  realm.handle('GET', '/api/v1/streams', () => ({
    body: { result: 'success', streams },
  }))
}

test('reconcileMinterSubscriptions GETs /streams filtered to public-not-subscribed', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedStreamsList([])
      seedUsers([])
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.reconcileMinterSubscriptions()
      const req = findRequest('GET', '/api/v1/streams')
      expect(req.url.searchParams.get('include_public')).toBe('true')
      expect(req.url.searchParams.get('include_subscribed')).toBe('false')
      yield* Effect.promise(() => adapter.close())
    }),
  ))

test('reconcileMinterSubscriptions returns empty added when the realm reports no gap', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedStreamsList([])
      seedUsers([])
      const adapter = yield* zulipAdapter(yield* makeConfig())
      const report = yield* adapter.reconcileMinterSubscriptions()
      expect(report).toEqual({ added: [], error: undefined })
      expect(
        realm.captured.find(
          (r) => r.method === 'POST' && r.url.pathname === '/api/v1/users/me/subscriptions',
        ),
      ).toBeUndefined()
      yield* Effect.promise(() => adapter.close())
    }),
  ))

test('reconcileMinterSubscriptions batches every unsubscribed public stream into one POST', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedStreamsList([
        { stream_id: 11, name: 'commy' },
        { stream_id: 12, name: 'assistant' },
        { stream_id: 13, name: 'homelab' },
      ])
      realm.handle('POST', '/api/v1/users/me/subscriptions', () => ({
        body: {
          result: 'success',
          subscribed: { 'minter@example.com': ['commy', 'assistant', 'homelab'] },
          already_subscribed: {},
          unauthorized: [],
        },
      }))
      seedUsers([])
      const adapter = yield* zulipAdapter(yield* makeConfig())
      const report = yield* adapter.reconcileMinterSubscriptions()
      expect(report.added).toEqual([
        decodeChannelNameSync('commy'),
        decodeChannelNameSync('assistant'),
        decodeChannelNameSync('homelab'),
      ])
      expect(report.error).toBeUndefined()
      const post = findRequest('POST', '/api/v1/users/me/subscriptions')
      const subs = JSON.parse(
        new URLSearchParams(post.body).get('subscriptions') ?? '[]',
      ) as unknown
      expect(subs).toEqual([{ name: 'commy' }, { name: 'assistant' }, { name: 'homelab' }])
      yield* Effect.promise(() => adapter.close())
    }),
  ))

test('reconcileMinterSubscriptions reports only the streams the realm confirms as newly subscribed', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedStreamsList([
        { stream_id: 11, name: 'commy' },
        { stream_id: 12, name: 'homelab' },
      ])
      // Race: another reconciler already subscribed `homelab` between
      // our list and post. Zulip puts it under already_subscribed and the
      // reconciler's report mirrors that.
      realm.handle('POST', '/api/v1/users/me/subscriptions', () => ({
        body: {
          result: 'success',
          subscribed: { 'minter@example.com': ['commy'] },
          already_subscribed: { 'minter@example.com': ['homelab'] },
          unauthorized: [],
        },
      }))
      seedUsers([])
      const adapter = yield* zulipAdapter(yield* makeConfig())
      const report = yield* adapter.reconcileMinterSubscriptions()
      expect(report.added).toEqual([decodeChannelNameSync('commy')])
      expect(report.error).toBeUndefined()
      yield* Effect.promise(() => adapter.close())
    }),
  ))

test('reconcileMinterSubscriptions captures list failure without throwing', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', '/api/v1/streams', () => ({
        status: 500,
        body: { result: 'error', msg: 'realm unreachable' },
      }))
      seedUsers([])
      const adapter = yield* zulipAdapter(yield* makeConfig())
      const report = yield* adapter.reconcileMinterSubscriptions()
      expect(report.added).toEqual([])
      expect(report.error).toBe('realm unreachable')
      expect(
        realm.captured.find(
          (r) => r.method === 'POST' && r.url.pathname === '/api/v1/users/me/subscriptions',
        ),
      ).toBeUndefined()
      yield* Effect.promise(() => adapter.close())
    }),
  ))

test('reconcileMinterSubscriptions routes via minter creds', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      seedStreamsList([{ stream_id: 11, name: 'commy' }])
      realm.handle('POST', '/api/v1/users/me/subscriptions', () => ({
        body: {
          result: 'success',
          subscribed: { 'minter@example.com': ['commy'] },
          already_subscribed: {},
          unauthorized: [],
        },
      }))
      seedUsers([])
      const adapter = yield* zulipAdapter(yield* makeConfig())
      yield* adapter.reconcileMinterSubscriptions()
      const listReq = findRequest('GET', '/api/v1/streams')
      const postReq = findRequest('POST', '/api/v1/users/me/subscriptions')
      expect(decodeBasicAuth(listReq.headers.get('Authorization'))).toEqual(minterAuth)
      expect(decodeBasicAuth(postReq.headers.get('Authorization'))).toEqual(minterAuth)
      yield* Effect.promise(() => adapter.close())
    }),
  ))

// --- attachmentReference (comms-nsa) ---

test('attachmentReference renders a Zulip markdown link, filename as text and url as target', () => {
  expect(
    attachmentReference({
      filename: 'chart.png',
      url: decodeUserUploadPathSync('/user_uploads/1/ab/chart.png'),
    }),
  ).toBe('[chart.png](/user_uploads/1/ab/chart.png)')
})

test('downloadFile rejects an unbranded urlPath at the type level (comms-m1y)', () => {
  // comms-39q dropped this proof believing method-param bivariance let a bare
  // string through `downloadFile(urlPath: UserUploadPath)`. It does not:
  // bivariance only loosens function-type assignability, never a direct
  // call-site argument check — so the UserUploadPath brand bites here even
  // though downloadFile is declared with method shorthand on the
  // intersection-typed ZulipAdapter. If this @ts-expect-error stops firing,
  // the brand has genuinely been weakened.
  const proof = (adapter: ZulipAdapter): void => {
    // @ts-expect-error — urlPath must be UserUploadPath, not string
    void adapter.downloadFile('raw-unbranded-path')
    void adapter.downloadFile(decodeUserUploadPathSync('/user_uploads/1/ab/x.png'))
  }
  expect(proof).toBeTypeOf('function')
})

// --- UserUploadPath brand (comms-39q) ---

test('attachmentReference refuses an unbranded url at the type level (comms-39q)', () => {
  // UploadResult.url is a UserUploadPath brand: a bare string from any
  // source must not reach a consumer that expects a checked upload path.
  // If this @ts-expect-error stops erroring, the brand has been weakened
  // and an arbitrary string could masquerade as a /user_uploads/ path.
  // @ts-expect-error — url must be UserUploadPath, not string
  attachmentReference({ filename: 'chart.png', url: '/user_uploads/1/ab/chart.png' })
  // Sanity: minting the brand compiles fine.
  attachmentReference({
    filename: 'chart.png',
    url: decodeUserUploadPathSync('/user_uploads/1/ab/chart.png'),
  })
})

test('UserUploadPath accepts a /user_uploads/ path', () => {
  expect(decodeUserUploadPathSync('/user_uploads/1/ab/chart.png')).toBe(
    decodeUserUploadPathSync('/user_uploads/1/ab/chart.png'),
  )
})

test('UserUploadPath rejects a path not under /user_uploads/', () => {
  expect(() => decodeUserUploadPathSync('/api/v1/messages')).toThrow(/user_uploads/)
})
