import { expect, test } from 'bun:test'
import type { ChannelRef, Identity, MessageRef } from '@commy/core/ports'
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
  ThreadPermalinkSchema,
  UnknownChannel,
  type UnknownIdentity,
} from '@commy/core/ports'
import { effectTest } from '@commy/testing/effect-test'
import { makeStubHttpClient, type StubHttpClient } from '@commy/testing/stub-http-client'
import { HttpClient } from '@effect/platform'
import { Cause, Effect, Exit, Option, Redacted } from 'effect'
import type { ZulipAdapter, ZulipAdapterConfig } from './adapter.ts'
import { attachmentReference, zulipAdapter as zulipAdapterRaw } from './adapter.ts'
import { ApiKey, BotEmail, decodeUserUploadPathSync, RealmUrl, ZulipApiError } from './http.ts'

// Adapter-internal logic exercised on the owned-fake stub HttpClient — no
// `Bun.serve`, no real socket. These are the request-shape / error-mapping
// unit tests: they inspect the captured `HttpClientRequest`s (params, narrow
// JSON, auth headers, call counts, paths) and the typed-error wrapping that
// the substrate-agnostic live contract (`contract.ts`) deliberately never
// asserts. The behavioural round-trips the contract does prove are not
// duplicated here. The long-poll / reconnect logic lives in
// `adapter-events.test.ts`; the genuine real-socket teardown is the Tier-3
// residue.

const HERMES = {
  user_id: 9,
  email: 'hermes-agent-bot@example.com',
  full_name: 'hermes-agent',
  is_bot: true,
  is_active: true,
  role: 400,
} as const

const MAINTAINER = {
  user_id: 5,
  email: 'user@example.com',
  full_name: 'Robin Reyes',
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
// a seeded member.
const BOB = {
  user_id: 2,
  email: 'bob@example.com',
  full_name: 'bob',
  is_bot: false,
  is_active: true,
  role: 100,
} as const

const REALM_URL = 'https://zulip.example.com'

// makeZulipHttp reads HttpClient from context, so the adapter
// Effect carries `HttpClient` in R. Provide the owned-fake stub client here so
// every call site keeps a `never` requirements channel.
const zulipAdapter = (
  stub: StubHttpClient,
  config: ZulipAdapterConfig,
): Effect.Effect<ZulipAdapter> =>
  zulipAdapterRaw(config).pipe(Effect.provideService(HttpClient.HttpClient, stub.client))

const makeConfig = (): Effect.Effect<ZulipAdapterConfig> =>
  Effect.gen(function* () {
    return {
      realmUrl: yield* RealmUrl(REALM_URL),
      minterEmail: yield* BotEmail('minter@example.com'),
      minterApiKey: Redacted.make(yield* ApiKey('minter-key')),
    }
  }).pipe(Effect.orDie)

const seedUsers = (stub: StubHttpClient, members: ReadonlyArray<unknown>): Effect.Effect<void> =>
  stub.respond('GET', '/api/v1/users', { body: { result: 'success', members } })

const seedRegenerate = (
  stub: StubHttpClient,
  userId: number,
  apiKey = 'fresh-key',
): Effect.Effect<void> =>
  stub.respond('POST', `/api/v1/bots/${userId}/api_key/regenerate`, {
    body: { result: 'success', api_key: apiKey },
  })

const seedMint = (
  stub: StubHttpClient,
  userId = 9999,
  apiKey = 'minted-key',
): Effect.Effect<void> =>
  // Match real Zulip's response shape — POST /bots returns
  // user_id + api_key but not email. The adapter reconstructs the
  // bot's delivery email from <short_name>-bot@<realm_host>.
  stub.respond('POST', '/api/v1/bots', {
    body: { result: 'success', api_key: apiKey, user_id: userId },
  })

// Realm host the test ZulipHttp will see — used to verify the adapter's
// client-side email construction.
const realmHost = (): string => new URL(REALM_URL).hostname

const seedDeactivate = (stub: StubHttpClient, userId: number): Effect.Effect<void> =>
  stub.respond('DELETE', `/api/v1/bots/${userId}`, { body: { result: 'success' } })

const buildAdapter = (
  stub: StubHttpClient,
  configOverrides: Partial<ZulipAdapterConfig> = {},
  acquireName = decodeBotNameSync('hermes-agent'),
): Effect.Effect<ZulipAdapter, IdentityError | UnknownIdentity> =>
  Effect.gen(function* () {
    yield* seedUsers(stub, [HERMES])
    yield* seedRegenerate(stub, HERMES.user_id)
    const config = yield* makeConfig()
    const adapter = yield* zulipAdapter(stub, { ...config, ...configOverrides })
    yield* adapter.identity.acquire(acquireName)
    return adapter
  })

const findRequest = (stub: StubHttpClient, method: string, pathname: string) =>
  stub.captured.pipe(
    Effect.map((reqs) => {
      const req = reqs.find((r) => r.method === method && r.url.pathname === pathname)
      if (req === undefined) throw new Error(`no captured ${method} ${pathname}`)
      return req
    }),
  )

effectTest('identity.acquire on an existing bot regenerates its API key and binds', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedUsers(stub, [HERMES])
    yield* seedRegenerate(stub, HERMES.user_id, 'rotated-key')
    const config = yield* makeConfig()
    const adapter = yield* zulipAdapter(stub, config)
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
    expect(
      yield* findRequest(stub, 'POST', `/api/v1/bots/${HERMES.user_id}/api_key/regenerate`),
    ).toBeDefined()
  }),
)

effectTest('identity.acquire on a name with no existing bot mints fresh via POST /bots', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedUsers(stub, [MAINTAINER])
    yield* seedMint(stub, 42, 'fresh-mint-key')
    const config = yield* makeConfig()
    const adapter = yield* zulipAdapter(stub, config)
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
    const mintReq = yield* findRequest(stub, 'POST', '/api/v1/bots')
    const params = new URLSearchParams(mintReq.body)
    expect(params.get('full_name')).toBe('fresh-bot')
    expect(params.get('short_name')).toBe('fresh-bot')
    expect(params.get('bot_type')).toBe('1')
  }),
)

effectTest(
  'identity.acquire on a deactivated bot reactivates and regenerates (does NOT mint)',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const deactivated = { ...HERMES, is_active: false }
      yield* seedUsers(stub, [deactivated])
      yield* stub.respond('POST', `/api/v1/users/${HERMES.user_id}/reactivate`, {
        body: { result: 'success' },
      })
      yield* seedRegenerate(stub, HERMES.user_id, 'rotated-key')
      const adapter = yield* zulipAdapter(stub, yield* makeConfig())
      const result = yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      expect(result.identity.id).toEqual(decodeIdentityIdSync(String(HERMES.user_id)))
      expect(
        yield* findRequest(stub, 'POST', `/api/v1/users/${HERMES.user_id}/reactivate`),
      ).toBeDefined()
      expect(
        yield* findRequest(stub, 'POST', `/api/v1/bots/${HERMES.user_id}/api_key/regenerate`),
      ).toBeDefined()
      // Must not have minted — the deactivated email is still reserved on
      // the realm; mint would fail with EmailAlreadyInUseError.
      const reqs = yield* stub.captured
      expect(
        reqs.find((r) => r.method === 'POST' && r.url.pathname === '/api/v1/bots'),
      ).toBeUndefined()
    }),
)

effectTest(
  'identity.acquire sanitises short_name by lowercasing and replacing non-email chars',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub, [])
      yield* seedMint(stub, 50, 'k')
      const adapter = yield* zulipAdapter(stub, yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('gas-town/witness'))
      const params = new URLSearchParams((yield* findRequest(stub, 'POST', '/api/v1/bots')).body)
      expect(params.get('full_name')).toBe('gas-town/witness')
      expect(params.get('short_name')).toBe('gas-town-witness')
    }),
)

// --- Attach mode: bind a pre-provisioned persona via a supplied
// stable api key without regenerating it, so many sessions/processes can share
// one identity (the Discord-style single-identity model) with no acquire
// collision. Gated on config.attachIdentity matching the acquired name; every
// other acquire keeps the normal mint/regenerate behaviour.

const attachConfig = (name: string, apiKey: string): Effect.Effect<ZulipAdapterConfig> =>
  Effect.gen(function* () {
    return {
      ...(yield* makeConfig()),
      attachIdentity: {
        name: decodeBotNameSync(name),
        apiKey: Redacted.make(yield* ApiKey(apiKey).pipe(Effect.orDie)),
      },
    }
  })

effectTest(
  'identity.acquire ATTACHES to a provisioned persona via the supplied key — no regenerate, no mint',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub, [HERMES])
      // Seed neither regenerate nor mint: the attach path must
      // not call them, and an accidental call surfaces as an unstubbed-request
      // failure rather than passing silently.
      const config = yield* attachConfig('hermes-agent', 'stable-provided-key')
      const adapter = yield* zulipAdapter(stub, config)
      const result = yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      expect(result.identity).toEqual({
        id: decodeIdentityIdSync(String(HERMES.user_id)),
        name: decodeDisplayNameSync('hermes-agent'),
        kind: 'agent',
      })
      expect(result.credentials).toEqual({
        substrate: 'zulip',
        realmUrl: config.realmUrl,
        email: yield* BotEmail(HERMES.email).pipe(Effect.orDie),
        // The supplied key, verbatim — not a rotated one.
        apiKey: yield* ApiKey('stable-provided-key').pipe(Effect.orDie),
      })
      const reqs = yield* stub.captured
      expect(
        reqs.find(
          (r) =>
            r.method === 'POST' &&
            r.url.pathname === `/api/v1/bots/${HERMES.user_id}/api_key/regenerate`,
        ),
      ).toBeUndefined()
      expect(
        reqs.find((r) => r.method === 'POST' && r.url.pathname === '/api/v1/bots'),
      ).toBeUndefined()
    }),
)

effectTest(
  'identity.acquire attach fails clearly when the persona is not provisioned on the realm',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub, [MAINTAINER])
      const adapter = yield* zulipAdapter(stub, yield* attachConfig('hermes-agent', 'k'))
      const err = yield* Effect.flip(adapter.identity.acquire(decodeBotNameSync('hermes-agent')))
      expect(err).toBeInstanceOf(IdentityError)
    }),
)

// With no stable key supplied, acquire takes the normal mint/regenerate path.
effectTest(
  'identity.acquire with NO attachIdentity regenerates as before — non-attach consumers untouched',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub, [HERMES])
      yield* seedRegenerate(stub, HERMES.user_id, 'rotated-key')
      const adapter = yield* zulipAdapter(stub, yield* makeConfig())
      const result = yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      expect(result.credentials['apiKey']).toEqual(yield* ApiKey('rotated-key').pipe(Effect.orDie))
      expect(
        yield* findRequest(stub, 'POST', `/api/v1/bots/${HERMES.user_id}/api_key/regenerate`),
      ).toBeDefined()
    }),
)

// The attach config must only fire for the persona it names — acquiring any
// other name regenerates as usual, so a per-persona key can never leak into an
// unrelated consumer's binding.
effectTest(
  'identity.acquire of a name other than attachIdentity.name keeps the mint/regenerate path',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub, [HERMES])
      yield* seedRegenerate(stub, HERMES.user_id, 'rotated-key')
      const adapter = yield* zulipAdapter(stub, yield* attachConfig('other-persona', 'unused-key'))
      const result = yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      expect(result.credentials['apiKey']).toEqual(yield* ApiKey('rotated-key').pipe(Effect.orDie))
      expect(
        yield* findRequest(stub, 'POST', `/api/v1/bots/${HERMES.user_id}/api_key/regenerate`),
      ).toBeDefined()
    }),
)

effectTest('identity.acquire rejects with ZulipApiError when /users lookup fails', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/users', {
      body: { result: 'error', msg: 'Invalid API key', code: 'BAD_API_KEY' },
      status: 401,
    })
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    const err = yield* Effect.flip(adapter.identity.acquire(decodeBotNameSync('hermes-agent')))
    expect(err).toBeInstanceOf(IdentityError)
    expect((err as { cause: unknown }).cause).toBeInstanceOf(ZulipApiError)
  }),
)

effectTest('zulipAdapter construction does NOT call /users — the call is deferred to acquire', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedUsers(stub, [HERMES])
    yield* zulipAdapter(stub, yield* makeConfig())
    const reqs = yield* stub.captured
    const usersCalls = reqs.filter((r) => r.method === 'GET' && r.url.pathname === '/api/v1/users')
    expect(usersCalls).toHaveLength(0)
  }),
)

effectTest('currentIdentity before acquire throws — port is unauthenticated at construction', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    const exit = yield* Effect.exit(adapter.identity.currentIdentity())
    expect(Exit.isFailure(exit)).toBe(true)
  }),
)

effectTest(
  'identity.acquire on the bound name is idempotent and skips a second /users round-trip',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub, [HERMES])
      yield* seedRegenerate(stub, HERMES.user_id)
      const adapter = yield* zulipAdapter(stub, yield* makeConfig())
      const first = yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      const second = yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      expect(second.identity.id).toEqual(first.identity.id)
      const reqs = yield* stub.captured
      const usersCalls = reqs.filter(
        (r) => r.method === 'GET' && r.url.pathname === '/api/v1/users',
      )
      expect(usersCalls).toHaveLength(1)
      const regenCalls = reqs.filter(
        (r) =>
          r.method === 'POST' &&
          r.url.pathname === `/api/v1/bots/${HERMES.user_id}/api_key/regenerate`,
      )
      expect(regenCalls).toHaveLength(1)
    }),
)

effectTest('identity.release on a bound adapter deactivates the bot via DELETE /bots/{id}', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedUsers(stub, [HERMES])
    yield* seedRegenerate(stub, HERMES.user_id)
    yield* seedDeactivate(stub, HERMES.user_id)
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
    yield* adapter.identity.release()
    expect(yield* findRequest(stub, 'DELETE', `/api/v1/bots/${HERMES.user_id}`)).toBeDefined()
  }),
)

effectTest('identity.release clears the binding — currentIdentity then throws', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedDeactivate(stub, HERMES.user_id)
    const adapter = yield* buildAdapter(stub)
    yield* adapter.identity.release()
    const exit = yield* Effect.exit(adapter.identity.currentIdentity())
    expect(Exit.isFailure(exit)).toBe(true)
  }),
)

effectTest('identity.release on an unauthenticated adapter is a no-op', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    yield* adapter.identity.release()
    const reqs = yield* stub.captured
    const deletes = reqs.filter(
      (r) => r.method === 'DELETE' && r.url.pathname.startsWith('/api/v1/bots/'),
    )
    expect(deletes).toHaveLength(0)
  }),
)

effectTest('identity.release is best-effort — deactivate failure does not throw', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedUsers(stub, [HERMES])
    yield* seedRegenerate(stub, HERMES.user_id)
    yield* stub.respond('DELETE', `/api/v1/bots/${HERMES.user_id}`, {
      body: { result: 'error', msg: 'kaboom', code: 'INTERNAL_SERVER_ERROR' },
      status: 500,
    })
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
    yield* adapter.identity.release()
    const exit = yield* Effect.exit(adapter.identity.currentIdentity())
    expect(Exit.isFailure(exit)).toBe(true)
  }),
)

effectTest('identity.release({ persistent: true }) does NOT deactivate the bot', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedUsers(stub, [HERMES])
    yield* seedRegenerate(stub, HERMES.user_id)
    yield* seedDeactivate(stub, HERMES.user_id)
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
    yield* adapter.identity.release({ persistent: true })
    const reqs = yield* stub.captured
    const deletes = reqs.filter(
      (r) => r.method === 'DELETE' && r.url.pathname.startsWith('/api/v1/bots/'),
    )
    expect(deletes).toHaveLength(0)
  }),
)

effectTest(
  'identity.release({ persistent: true }) still clears the binding — currentIdentity throws',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub, [HERMES])
      yield* seedRegenerate(stub, HERMES.user_id)
      const adapter = yield* zulipAdapter(stub, yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      yield* adapter.identity.release({ persistent: true })
      const exit = yield* Effect.exit(adapter.identity.currentIdentity())
      expect(Exit.isFailure(exit)).toBe(true)
    }),
)

effectTest(
  'identity.release (ephemeral default) still deactivates the bot via DELETE /bots/{id}',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub, [HERMES])
      yield* seedRegenerate(stub, HERMES.user_id)
      yield* seedDeactivate(stub, HERMES.user_id)
      const adapter = yield* zulipAdapter(stub, yield* makeConfig())
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      yield* adapter.identity.release({ persistent: false })
      expect(yield* findRequest(stub, 'DELETE', `/api/v1/bots/${HERMES.user_id}`)).toBeDefined()
    }),
)

effectTest(
  'identity.acquire surfaces a reactivate failure with the real cause, not a generic credential error',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const deactivated = { ...HERMES, is_active: false }
      yield* seedUsers(stub, [deactivated])
      // A Member (non-admin) minter calling POST /users/{id}/reactivate gets
      // UNAUTHORIZED_PRINCIPAL ("Must be an organization administrator"), 400.
      yield* stub.respond('POST', `/api/v1/users/${HERMES.user_id}/reactivate`, {
        body: {
          result: 'error',
          msg: 'Must be an organization administrator',
          code: 'UNAUTHORIZED_PRINCIPAL',
        },
        status: 400,
      })
      yield* seedRegenerate(stub, HERMES.user_id)
      const adapter = yield* zulipAdapter(stub, yield* makeConfig())
      const err = yield* Effect.flip(adapter.identity.acquire(decodeBotNameSync('hermes-agent')))
      expect(err).toBeInstanceOf(IdentityError)
      // The surfaced message must name the real cause (reactivate forbidden /
      // minter not admin) so the next debugger is not sent chasing a phantom
      // credential bug.
      expect(err.message).toContain('reactivate')
      expect(err.message.toLowerCase()).toContain('administrator')
      // The regenerate must never have fired — reactivate failed first.
      const reqs = yield* stub.captured
      expect(
        reqs.find(
          (r) =>
            r.method === 'POST' &&
            r.url.pathname === `/api/v1/bots/${HERMES.user_id}/api_key/regenerate`,
        ),
      ).toBeUndefined()
    }),
)

effectTest('identity.acquire after release rebinds via a fresh /users round-trip', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedDeactivate(stub, HERMES.user_id)
    const adapter = yield* buildAdapter(stub)
    yield* adapter.identity.release()
    // After release, the regenerate seed is still in place — re-acquire
    // takes the regenerate path again.
    yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
    const reqs = yield* stub.captured
    const usersCalls = reqs.filter((r) => r.method === 'GET' && r.url.pathname === '/api/v1/users')
    expect(usersCalls).toHaveLength(2)
  }),
)

effectTest('directory.listAgents returns only is_bot=true users', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/users', {
      body: { result: 'success', members: [HERMES, MAINTAINER, RIQ] },
    })
    yield* seedRegenerate(stub, HERMES.user_id)
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
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
)

effectTest('directory.listHumans returns only is_bot=false users', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/users', {
      body: { result: 'success', members: [HERMES, MAINTAINER, RIQ] },
    })
    yield* seedRegenerate(stub, HERMES.user_id)
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
    const humans = yield* adapter.directory.listHumans()
    expect(humans).toEqual([
      {
        id: decodeIdentityIdSync('5'),
        name: decodeDisplayNameSync('Robin Reyes'),
        kind: 'human',
      },
    ])
  }),
)

effectTest(
  'directory.listHumans surfaces a /users fetch failure as DirectoryError (cause preserved)',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* stub.respond('GET', '/api/v1/users', {
        body: { result: 'error', msg: 'internal server error' },
        status: 500,
      })
      const error = yield* Effect.flip(adapter.directory.listHumans())
      expect(error).toBeInstanceOf(DirectoryError)
      expect(error.operation).toBe('listHumans')
      expect(error.cause).toBeInstanceOf(ZulipApiError)
    }),
)

effectTest(
  'directory.listChannels surfaces a /streams fetch failure as DirectoryError (cause preserved)',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* stub.respond('GET', '/api/v1/streams', {
        body: { result: 'error', msg: 'internal server error' },
        status: 500,
      })
      const error = yield* Effect.flip(adapter.directory.listChannels())
      expect(error).toBeInstanceOf(DirectoryError)
      expect(error.operation).toBe('listChannels')
      expect(error.cause).toBeInstanceOf(ZulipApiError)
    }),
)

effectTest('directory.listAgents excludes inactive users', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/users', {
      body: {
        result: 'success',
        members: [HERMES, { ...RIQ, is_active: false }],
      },
    })
    yield* seedRegenerate(stub, HERMES.user_id)
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
    const agents = yield* adapter.directory.listAgents()
    expect(agents.map((a) => a.id)).toEqual([decodeIdentityIdSync('9')])
  }),
)

effectTest(
  'directory.listAgents surfaces a /users fetch failure as DirectoryError (cause preserved)',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* stub.respond('GET', '/api/v1/users', {
        body: { result: 'error', msg: 'internal server error' },
        status: 500,
      })
      const error = yield* Effect.flip(adapter.directory.listAgents())
      expect(error).toBeInstanceOf(DirectoryError)
      expect(error.operation).toBe('listAgents')
      expect(error.cause).toBeInstanceOf(ZulipApiError)
    }),
)

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

const seedSendMessage = (stub: StubHttpClient, id: number): Effect.Effect<void> =>
  // publisher.post now pre-flights against GET /streams so unknown channels
  // surface as UnknownChannel instead of being silently routed to
  // Notification Bot. Helper bundles both stubs because every caller posts
  // to `generalChannel`.
  Effect.gen(function* () {
    yield* stub.respond('GET', '/api/v1/streams', {
      body: {
        result: 'success',
        streams: [{ stream_id: 1234, name: 'general' }],
      },
    })
    yield* stub.respond('POST', '/api/v1/messages', {
      body: { result: 'success', id },
    })
  })

effectTest(
  'publisher.post sends type=channel + to=channel.name + content; defaults topic to "(no topic)" when no thread',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedSendMessage(stub, 42)
      const adapter = yield* buildAdapter(stub)
      yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('hello world'))
      const params = new URLSearchParams(
        (yield* findRequest(stub, 'POST', '/api/v1/messages')).body,
      )
      expect(params.get('type')).toBe('channel')
      expect(params.get('to')).toBe('general')
      expect(params.get('content')).toBe('hello world')
      expect(params.get('topic')).toBe('(no topic)')
    }),
)

effectTest('publisher.post returns a MessageRef built from the response id', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedSendMessage(stub, 99)
    const adapter = yield* buildAdapter(stub)
    const ref = yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('hello world'))
    expect(ref.id).toEqual(decodeMessageIdSync('99'))
    expect(ref.channel).toMatchObject(generalChannel)
    expect(ref.thread).toEqual(Option.none())
  }),
)

effectTest('publisher.post with thread sends topic and threads the returned MessageRef', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedSendMessage(stub, 100)
    const adapter = yield* buildAdapter(stub)
    const ref = yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('hi'), {
      thread: decodeThreadNameSync('planning'),
    })
    const params = new URLSearchParams((yield* findRequest(stub, 'POST', '/api/v1/messages')).body)
    expect(params.get('topic')).toBe('planning')
    expect(Option.map(ref.thread, (t) => t.name)).toEqual(
      Option.some(decodeThreadNameSync('planning')),
    )
  }),
)

effectTest('publisher.post hands back message and channel permalinks on the returned ref', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedSendMessage(stub, 99)
    const adapter = yield* buildAdapter(stub)
    const ref = yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('hello world'))
    expect(ref.permalink).toBe('https://zulip.example.com/#narrow/channel/1234-general/near/99')
    expect(ref.channel.permalink).toBe('https://zulip.example.com/#narrow/channel/1234-general')
  }),
)

effectTest('publisher.post threads the permalink through the topic when a thread is set', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedSendMessage(stub, 100)
    const adapter = yield* buildAdapter(stub)
    const ref = yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('hi'), {
      thread: decodeThreadNameSync('planning'),
    })
    expect(ref.permalink).toBe(
      'https://zulip.example.com/#narrow/channel/1234-general/topic/planning/near/100',
    )
    expect(Option.map(ref.thread, (t) => t.permalink)).toEqual(
      Option.some(
        ThreadPermalinkSchema.make(
          'https://zulip.example.com/#narrow/channel/1234-general/topic/planning',
        ),
      ),
    )
  }),
)

effectTest('publisher.post permalink uses the public host header when one is configured', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedSendMessage(stub, 7)
    const adapter = yield* buildAdapter(stub, { hostHeader: 'public.zulip.test' })
    const ref = yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('hi'))
    expect(ref.permalink).toBe('https://public.zulip.test/#narrow/channel/1234-general/near/7')
  }),
)

effectTest(
  'publisher.post leaves body unchanged when opts.mentions is set — mentions[] is metadata-only',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedSendMessage(stub, 101)
      const adapter = yield* buildAdapter(stub)
      yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('wake up'), {
        mentions: [aliceBot, bobHuman],
      })
      const params = new URLSearchParams(
        (yield* findRequest(stub, 'POST', '/api/v1/messages')).body,
      )
      expect(params.get('content')).toBe('wake up')
    }),
)

effectTest('publisher.post with body-only mention markup posts body verbatim', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedSendMessage(stub, 103)
    const adapter = yield* buildAdapter(stub)
    yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('hey @**alice** look'), {})
    const params = new URLSearchParams((yield* findRequest(stub, 'POST', '/api/v1/messages')).body)
    expect(params.get('content')).toBe('hey @**alice** look')
  }),
)

// A body containing `@**Name**` + mentions[] for the same identity: under the
// metadata-only contract the adapter never folds mentions[] into body, so this
// case posts body verbatim with exactly one rendered @-mention.
effectTest('publisher.post with both body markup AND opts.mentions does not double-render', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedSendMessage(stub, 104)
    const adapter = yield* buildAdapter(stub)
    yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('@**alice** wake up'), {
      mentions: [aliceBot],
    })
    const params = new URLSearchParams((yield* findRequest(stub, 'POST', '/api/v1/messages')).body)
    expect(params.get('content')).toBe('@**alice** wake up')
  }),
)

effectTest(
  'publisher.post drops opts.replyTo silently (Zulip has no in-topic reply primitive)',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedSendMessage(stub, 102)
      const adapter = yield* buildAdapter(stub)
      const parent: MessageRef = {
        id: decodeMessageIdSync('1'),
        channel: generalChannel,
        thread: Option.some({
          name: decodeThreadNameSync('planning'),
          permalink: ThreadPermalinkSchema.make(
            'https://zulip.example.com/#narrow/channel/1234-general/topic/planning',
          ),
        }),
      }
      yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('still here'), {
        replyTo: parent,
      })
      const params = new URLSearchParams(
        (yield* findRequest(stub, 'POST', '/api/v1/messages')).body,
      )
      expect(params.get('content')).toBe('still here')
      expect(params.has('reply_to')).toBe(false)
    }),
)

// Pre-flight invariants for the publisher.post path — substrate-specific
// because Zulip silently routes "channel doesn't exist" to Notification Bot
// DMs and returns a success-shaped reply. The adapter must catch this with
// a GET /streams check before issuing POST /messages so callers see
// UnknownChannel instead of a silent void-send.
effectTest('publisher.post pre-flights GET /streams before issuing POST /messages', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedSendMessage(stub, 42)
    const adapter = yield* buildAdapter(stub)
    yield* adapter.publisher.post(generalChannel, decodeMessageBodySync('hello world'))
    const reqs = yield* stub.captured
    const streamsIndex = reqs.findIndex(
      (r) => r.method === 'GET' && r.url.pathname === '/api/v1/streams',
    )
    const messagesIndex = reqs.findIndex(
      (r) => r.method === 'POST' && r.url.pathname === '/api/v1/messages',
    )
    expect(streamsIndex).toBeGreaterThanOrEqual(0)
    expect(messagesIndex).toBeGreaterThanOrEqual(0)
    expect(streamsIndex).toBeLessThan(messagesIndex)
  }),
)

effectTest(
  'publisher.post refreshes /streams once on cache miss before throwing UnknownChannel',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/streams', { body: { result: 'success', streams: [] } })
      const adapter = yield* buildAdapter(stub)
      const error = yield* Effect.flip(
        adapter.publisher.post(generalChannel, decodeMessageBodySync('should fail')),
      )
      expect(error).toBeInstanceOf(UnknownChannel)
      const reqs = yield* stub.captured
      const streamsCallCount = reqs.filter(
        (r) => r.method === 'GET' && r.url.pathname === '/api/v1/streams',
      ).length
      expect(streamsCallCount).toBe(2)
    }),
)

effectTest('publisher.post does not issue POST /messages when pre-flight rejects the channel', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/streams', { body: { result: 'success', streams: [] } })
    yield* stub.respond('POST', '/api/v1/messages', { body: { result: 'success', id: 999 } })
    const adapter = yield* buildAdapter(stub)
    const error = yield* Effect.flip(
      adapter.publisher.post(generalChannel, decodeMessageBodySync('should fail')),
    )
    expect(error).toBeInstanceOf(UnknownChannel)
    const reqs = yield* stub.captured
    expect(
      reqs.find((r) => r.method === 'POST' && r.url.pathname === '/api/v1/messages'),
    ).toBeUndefined()
  }),
)

// Typed failure channel for publisher.post. A substrate failure
// on either the pre-flight `/streams` fetch or the `POST /messages` call is
// wrapped in a typed PublisherError carrying the cause (core stays
// substrate-agnostic — it never names ZulipApiError); an unknown channel is a
// tagged UnknownChannel; calling before acquire is a defect, not a typed
// failure (the bound-creds invariant is the caller's bug to fix).
effectTest(
  'publisher.post wraps a POST /messages failure as a PublisherError (cause preserved)',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/streams', {
        body: { result: 'success', streams: [{ stream_id: 1234, name: 'general' }] },
      })
      yield* stub.respond('POST', '/api/v1/messages', {
        body: { result: 'error', msg: 'internal server error' },
        status: 500,
      })
      const adapter = yield* buildAdapter(stub)
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
)

effectTest('publisher.post fails with a tagged UnknownChannel on an unknown channel', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/streams', { body: { result: 'success', streams: [] } })
    const adapter = yield* buildAdapter(stub)
    const error = yield* Effect.flip(
      adapter.publisher.post(generalChannel, decodeMessageBodySync('should fail')),
    )
    expect(error).toBeInstanceOf(UnknownChannel)
    if (error instanceof UnknownChannel) {
      expect(error._tag).toBe('UnknownChannel')
      expect(error.message).toContain('general')
    }
  }),
)

effectTest('publisher.edit PATCHes /messages/{id} with the new content', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('PATCH', '/api/v1/messages/42', { body: { result: 'success' } })
    const adapter = yield* buildAdapter(stub)
    const target: MessageRef = {
      id: decodeMessageIdSync('42'),
      channel: generalChannel,
      thread: Option.none(),
    }
    yield* adapter.publisher.edit(target, decodeMessageBodySync('replacement body'))
    const req = yield* findRequest(stub, 'PATCH', '/api/v1/messages/42')
    expect(req.method).toBe('PATCH')
    const params = new URLSearchParams(req.body)
    expect(params.get('content')).toBe('replacement body')
  }),
)

effectTest('publisher.edit propagates ZulipApiError on permission failure', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('PATCH', '/api/v1/messages/42', {
      body: { result: 'error', msg: 'not allowed', code: 'BAD_REQUEST' },
      status: 400,
    })
    const adapter = yield* buildAdapter(stub)
    const target: MessageRef = {
      id: decodeMessageIdSync('42'),
      channel: generalChannel,
      thread: Option.none(),
    }
    const err = yield* Effect.flip(adapter.publisher.edit(target, decodeMessageBodySync('nope')))
    expect(err).toBeInstanceOf(PublisherError)
    expect((err as { cause: unknown }).cause).toBeInstanceOf(ZulipApiError)
  }),
)

effectTest('publisher.react POSTs /messages/{id}/reactions with emoji_name', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('POST', '/api/v1/messages/42/reactions', { body: { result: 'success' } })
    const adapter = yield* buildAdapter(stub)
    const target: MessageRef = {
      id: decodeMessageIdSync('42'),
      channel: generalChannel,
      thread: Option.none(),
    }
    yield* adapter.publisher.react(target, decodeEmojiSync('thumbs_up'))
    const params = new URLSearchParams(
      (yield* findRequest(stub, 'POST', '/api/v1/messages/42/reactions')).body,
    )
    expect(params.get('emoji_name')).toBe('thumbs_up')
  }),
)

effectTest('publisher.unreact DELETEs /messages/{id}/reactions with emoji_name', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('DELETE', '/api/v1/messages/42/reactions', { body: { result: 'success' } })
    const adapter = yield* buildAdapter(stub)
    const target: MessageRef = {
      id: decodeMessageIdSync('42'),
      channel: generalChannel,
      thread: Option.none(),
    }
    yield* adapter.publisher.unreact(target, decodeEmojiSync('thumbs_up'))
    const req = yield* findRequest(stub, 'DELETE', '/api/v1/messages/42/reactions')
    const params = new URLSearchParams(req.body)
    expect(req.method).toBe('DELETE')
    expect(params.get('emoji_name')).toBe('thumbs_up')
  }),
)

const seedMessages = (
  stub: StubHttpClient,
  messages: ReadonlyArray<Record<string, unknown>>,
): Effect.Effect<void> =>
  stub.respond('GET', '/api/v1/messages', {
    body: {
      result: 'success',
      messages,
      anchor: 0,
      found_anchor: false,
      found_newest: true,
      found_oldest: false,
      history_limited: false,
    },
  })

effectTest('history.readChannel narrows by channel and maps each message to the port shape', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    yield* seedMessages(stub, [
      {
        id: 555,
        sender_id: 5,
        sender_full_name: 'Robin Reyes',
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
        channel: {
          ...generalChannel,
          permalink: 'https://zulip.example.com/#narrow/channel/1234-general',
        },
        thread: Option.some({
          name: decodeThreadNameSync('lobby'),
          permalink: ThreadPermalinkSchema.make(
            'https://zulip.example.com/#narrow/channel/1234-general/topic/lobby',
          ),
        }),
        permalink: 'https://zulip.example.com/#narrow/channel/1234-general/topic/lobby/near/555',
      },
      sender: {
        id: decodeIdentityIdSync('5'),
        name: decodeDisplayNameSync('Robin Reyes'),
        kind: 'human',
      },
      body: decodeMessageBodySync('hi all'),
      ts: decodeTimestampSync(1715000000),
      mentions: [],
      reactions: [],
    })
  }),
)

effectTest(
  'history.readChannel maps reactions, resolving reactors via the directory and falling back for unknown ids',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* seedUsers(stub, [HERMES, MAINTAINER])
      yield* seedMessages(stub, [
        {
          id: 555,
          sender_id: 5,
          sender_full_name: 'Robin Reyes',
          stream_id: 1234,
          display_recipient: 'general',
          subject: 'lobby',
          content: 'hi all',
          timestamp: 1715000000,
          reactions: [
            { user_id: 5, emoji_name: 'thumbs_up' },
            { user_id: 999, emoji_name: 'thumbs_up' },
            { user_id: 5, emoji_name: 'tada' },
          ],
        },
      ])
      const messages = yield* adapter.history.readChannel(generalChannel, { limit: 50 })
      const maintainer: Identity = {
        id: decodeIdentityIdSync('5'),
        name: decodeDisplayNameSync('Robin Reyes'),
        kind: 'human',
      }
      const unknownReactor: Identity = {
        id: decodeIdentityIdSync('999'),
        name: decodeDisplayNameSync('user-999'),
        kind: 'human',
      }
      expect(messages[0]?.reactions).toEqual([
        { emoji: decodeEmojiSync('thumbs_up'), by: [maintainer, unknownReactor] },
        { emoji: decodeEmojiSync('tada'), by: [maintainer] },
      ])
    }),
)

effectTest('history.messagePermalink builds a link from a channel hint via the streams cache', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    yield* stub.respond('GET', '/api/v1/streams', {
      body: { result: 'success', streams: [{ stream_id: 1234, name: 'general' }] },
    })
    const link = yield* adapter.history.messagePermalink(decodeMessageIdSync('77'), {
      channel: decodeChannelNameSync('general'),
      thread: decodeThreadNameSync('topic-x'),
    })
    expect(link).toEqual(
      Option.some('https://zulip.example.com/#narrow/channel/1234-general/topic/topic-x/near/77'),
    )
  }),
)

effectTest('history.messagePermalink fetches the message by id when no hint is given', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    yield* seedMessages(stub, [
      {
        id: 77,
        sender_id: 5,
        sender_full_name: 'Robin Reyes',
        stream_id: 1234,
        display_recipient: 'general',
        subject: 'lobby',
        content: 'hi',
        timestamp: 1715000000,
      },
    ])
    const link = yield* adapter.history.messagePermalink(decodeMessageIdSync('77'))
    expect(link).toEqual(
      Option.some('https://zulip.example.com/#narrow/channel/1234-general/topic/lobby/near/77'),
    )
  }),
)

effectTest(
  'history.readChannel sends narrow=[channel] with anchor=newest + num_before=range.limit',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* seedUsers(stub, [HERMES, MAINTAINER])
      yield* seedMessages(stub, [])
      yield* adapter.history.readChannel(generalChannel, { limit: 25 })
      const req = yield* findRequest(stub, 'GET', '/api/v1/messages')
      expect(req.url.searchParams.get('anchor')).toBe('newest')
      expect(req.url.searchParams.get('num_before')).toBe('25')
      expect(req.url.searchParams.get('num_after')).toBe('0')
      const narrow = JSON.parse(req.url.searchParams.get('narrow') ?? 'null') as unknown
      expect(narrow).toEqual([{ operator: 'channel', operand: 'general' }])
    }),
)

effectTest('history.readChannel resolves bot senders to kind=agent via the user directory', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    yield* seedMessages(stub, [
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
)

effectTest('history.readChannel resolves deactivated bot senders to kind=agent', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    const deactivatedBot = { ...RIQ, is_active: false }
    yield* seedUsers(stub, [HERMES, MAINTAINER, deactivatedBot])
    yield* seedMessages(stub, [
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
)

effectTest('history.readChannel filters by range.since (epoch seconds, inclusive)', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    yield* seedMessages(stub, [
      {
        id: 1,
        sender_id: 5,
        sender_full_name: 'Robin Reyes',
        stream_id: 1234,
        display_recipient: 'general',
        subject: 'a',
        content: 'old',
        timestamp: 1714000000,
      },
      {
        id: 2,
        sender_id: 5,
        sender_full_name: 'Robin Reyes',
        stream_id: 1234,
        display_recipient: 'general',
        subject: 'a',
        content: 'mid',
        timestamp: 1715000000,
      },
      {
        id: 3,
        sender_id: 5,
        sender_full_name: 'Robin Reyes',
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
)

effectTest('history.readChannel filters by range.until (epoch seconds, inclusive)', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    yield* seedMessages(stub, [
      {
        id: 1,
        sender_id: 5,
        sender_full_name: 'Robin Reyes',
        stream_id: 1234,
        display_recipient: 'general',
        subject: 'a',
        content: 'old',
        timestamp: 1714000000,
      },
      {
        id: 2,
        sender_id: 5,
        sender_full_name: 'Robin Reyes',
        stream_id: 1234,
        display_recipient: 'general',
        subject: 'a',
        content: 'mid',
        timestamp: 1715000000,
      },
      {
        id: 3,
        sender_id: 5,
        sender_full_name: 'Robin Reyes',
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
)

effectTest('history.readThread narrows by both channel and topic', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    yield* seedMessages(stub, [])
    yield* adapter.history.readThread(generalChannel, decodeThreadNameSync('planning'), {
      limit: 10,
    })
    const req = yield* findRequest(stub, 'GET', '/api/v1/messages')
    const narrow = JSON.parse(req.url.searchParams.get('narrow') ?? 'null') as unknown
    expect(narrow).toEqual([
      { operator: 'channel', operand: 'general' },
      { operator: 'topic', operand: 'planning' },
    ])
  }),
)

effectTest('history.readChannel with no limit defaults num_before to 100', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    yield* seedMessages(stub, [])
    yield* adapter.history.readChannel(generalChannel, {})
    const req = yield* findRequest(stub, 'GET', '/api/v1/messages')
    expect(req.url.searchParams.get('num_before')).toBe('100')
  }),
)

effectTest('history.recentThreads queries by sender and deduplicates per thread', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    yield* seedMessages(stub, [
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
    const req = yield* findRequest(stub, 'GET', '/api/v1/messages')
    const narrow = JSON.parse(req.url.searchParams.get('narrow') ?? 'null') as ReadonlyArray<{
      operator: string
      operand: unknown
    }>
    // Zulip's `sender` narrow operand must be the integer user id. A numeric
    // string ("9") is rejected as BAD_NARROW "unknown user 9".
    expect(narrow).toEqual([{ operator: 'sender', operand: HERMES.user_id }])
    expect(typeof narrow[0]?.operand).toBe('number')
  }),
)

effectTest(
  'history.recentThreads short-circuits to [] when the sender is not a known directory member',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* seedUsers(stub, [HERMES])
      // identity '999' resolves to no ZulipUserRef, so there is no integer the
      // `sender` narrow could use — return [] rather than query with an operand
      // Zulip would reject.
      const threads = yield* adapter.history.recentThreads(decodeIdentityIdSync('999'))
      expect(threads).toEqual([])
      const reqs = yield* stub.captured
      expect(reqs.some((r) => r.url.pathname === '/api/v1/messages')).toBe(false)
    }),
)

const seedPresence = (
  stub: StubHttpClient,
  userId: string,
  aggregatedStatus: 'active' | 'idle' | 'offline',
): Effect.Effect<void> =>
  stub.respond('GET', `/api/v1/users/${userId}/presence`, {
    body: {
      result: 'success',
      msg: '',
      server_timestamp: 1715000000,
      presence: {
        aggregated: { status: aggregatedStatus, timestamp: 1715000000 },
      },
    },
  })

effectTest('directory.presence maps aggregated status=active to online', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedPresence(stub, bobHuman.id, 'active')
    const adapter = yield* buildAdapter(stub)
    yield* seedUsers(stub, [HERMES, BOB])
    const presence = yield* adapter.directory.presence(bobHuman)
    expect(presence).toBe('online')
  }),
)

effectTest('directory.presence maps aggregated status=idle to idle', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedPresence(stub, bobHuman.id, 'idle')
    const adapter = yield* buildAdapter(stub)
    yield* seedUsers(stub, [HERMES, BOB])
    const presence = yield* adapter.directory.presence(bobHuman)
    expect(presence).toBe('idle')
  }),
)

effectTest('directory.presence maps aggregated status=offline to offline', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedPresence(stub, bobHuman.id, 'offline')
    const adapter = yield* buildAdapter(stub)
    yield* seedUsers(stub, [HERMES, BOB])
    const presence = yield* adapter.directory.presence(bobHuman)
    expect(presence).toBe('offline')
  }),
)

effectTest(
  'directory.presence returns offline when user has never set presence (Zulip 400 BAD_REQUEST)',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', `/api/v1/users/${bobHuman.id}/presence`, {
        body: { result: 'error', msg: `No presence data for ${bobHuman.id}`, code: 'BAD_REQUEST' },
        status: 400,
      })
      const adapter = yield* buildAdapter(stub)
      yield* seedUsers(stub, [HERMES, BOB])
      const presence = yield* adapter.directory.presence(bobHuman)
      expect(presence).toBe('offline')
    }),
)

effectTest(
  'directory.presence returns offline when the user no longer exists (Zulip 400 BAD_REQUEST: No such user)',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', `/api/v1/users/${bobHuman.id}/presence`, {
        body: { result: 'error', msg: 'No such user', code: 'BAD_REQUEST' },
        status: 400,
      })
      const adapter = yield* buildAdapter(stub)
      yield* seedUsers(stub, [HERMES, BOB])
      const presence = yield* adapter.directory.presence(bobHuman)
      expect(presence).toBe('offline')
    }),
)

effectTest(
  'directory.presence surfaces non-BAD_REQUEST errors as DirectoryError (cause preserved)',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      // A non-BAD_REQUEST error must propagate (only the benign "no presence
      // data" 400 is swallowed to offline). Use a 500 rather than a 429 — the
      // adapter now rides out 429s internally, so a 429 is no
      // longer an error the presence path surfaces.
      yield* stub.respond('GET', `/api/v1/users/${bobHuman.id}/presence`, {
        body: { result: 'error', msg: 'internal server error', code: 'INTERNAL_ERROR' },
        status: 500,
      })
      const adapter = yield* buildAdapter(stub)
      yield* seedUsers(stub, [HERMES, BOB])
      const error = yield* Effect.flip(adapter.directory.presence(bobHuman))
      expect(error).toBeInstanceOf(DirectoryError)
      expect(error.cause).toBeInstanceOf(ZulipApiError)
    }),
)

effectTest(
  'directory.presence short-circuits to offline for an identity that is not a known directory member',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* seedUsers(stub, [HERMES])
      const stranger: Identity = {
        id: decodeIdentityIdSync('999'),
        name: decodeDisplayNameSync('stranger'),
        kind: 'human',
      }
      const presence = yield* adapter.directory.presence(stranger)
      expect(presence).toBe('offline')
      // No ZulipUserRef means no resolvable user — don't fire a doomed presence GET.
      const reqs = yield* stub.captured
      expect(reqs.some((r) => r.url.pathname === '/api/v1/users/999/presence')).toBe(false)
    }),
)

effectTest(
  "directory.presence returns 'unknown' for an agent identity without reading Zulip presence",
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      // Zulip presence is human-only by design (POST /users/me/presence is
      // @human_users_only), so a bot has no presence concept. Reading it would
      // 400 and collapse to 'offline', which lies about a bot we simply cannot
      // know. Short-circuit agents to 'unknown' before any directory lookup or
      // presence GET — even for a bot that IS a known directory member.
      yield* seedPresence(stub, '11', 'active')
      const adapter = yield* buildAdapter(stub)
      yield* seedUsers(stub, [HERMES, RIQ])
      const riqBot: Identity = {
        id: decodeIdentityIdSync('11'),
        name: decodeDisplayNameSync('riq6r230'),
        kind: 'agent',
      }
      const presence = yield* adapter.directory.presence(riqBot)
      expect(presence).toBe('unknown')
      const reqs = yield* stub.captured
      expect(reqs.some((r) => r.url.pathname === '/api/v1/users/11/presence')).toBe(false)
    }),
)

// ─── bots never write their own presence ─────────────────────────
// Zulip's POST /users/me/presence is @human_users_only, so a bot self-presence
// write is structurally impossible (it 400s with "does not accept bot
// requests"). The adapter therefore exposes no presence-write path. This guards
// against the bot self-presence heartbeat being wired back in. The presence
// read — directory.presence, above — stays: a bot
// reading a human's presence is supported.

effectTest('a bound adapter never writes its own presence', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    const reqs = yield* stub.captured
    const presencePosts = reqs.filter(
      (r) => r.method === 'POST' && r.url.pathname === '/api/v1/users/me/presence',
    )
    expect(presencePosts).toHaveLength(0)
    yield* Effect.promise(() => adapter.close())
  }),
)

const seedRegisterOk = (
  stub: StubHttpClient,
  queueId = 'q1',
  lastEventId = 0,
): Effect.Effect<void> =>
  stub.respond('POST', '/api/v1/register', {
    body: { result: 'success', queue_id: queueId, last_event_id: lastEventId },
  })

const seedSubscribeOk = (stub: StubHttpClient, channelName: string): Effect.Effect<void> =>
  Effect.gen(function* () {
    yield* stub.respond('POST', '/api/v1/users/me/subscriptions', {
      body: {
        result: 'success',
        subscribed: { 'hermes-agent-bot@example.com': [channelName] },
        already_subscribed: {},
        unauthorized: [],
      },
    })
    // subscribe() also POSTs /register to satisfy the port's readiness
    // contract (subscribe resolved → events() observes subsequent posts).
    yield* seedRegisterOk(stub)
  })

const seedUnsubscribeOk = (stub: StubHttpClient, channelName: string): Effect.Effect<void> =>
  stub.respond('DELETE', '/api/v1/users/me/subscriptions', {
    body: { result: 'success', removed: [channelName], not_removed: [] },
  })

effectTest('inbox.subscribe(channel) POSTs /users/me/subscriptions with [{ name }]', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedSubscribeOk(stub, 'general')
    const adapter = yield* buildAdapter(stub)
    yield* adapter.inbox.subscribe(generalChannel)
    const req = yield* findRequest(stub, 'POST', '/api/v1/users/me/subscriptions')
    const params = new URLSearchParams(req.body)
    const subs = JSON.parse(params.get('subscriptions') ?? '[]') as unknown
    expect(subs).toEqual([{ name: 'general' }])
  }),
)

effectTest('inbox.subscribe is a no-op for already_subscribed', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('POST', '/api/v1/users/me/subscriptions', {
      body: {
        result: 'success',
        subscribed: {},
        already_subscribed: { 'hermes-agent-bot@example.com': ['general'] },
        unauthorized: [],
      },
    })
    yield* seedRegisterOk(stub)
    const adapter = yield* buildAdapter(stub)
    yield* adapter.inbox.subscribe(generalChannel)
  }),
)

effectTest('inbox.subscribe with thread subscribes to its underlying channel', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedSubscribeOk(stub, 'general')
    const adapter = yield* buildAdapter(stub)
    yield* adapter.inbox.subscribe({
      channel: generalChannel,
      thread: decodeThreadNameSync('design'),
    })
    const req = yield* findRequest(stub, 'POST', '/api/v1/users/me/subscriptions')
    const subs = JSON.parse(new URLSearchParams(req.body).get('subscriptions') ?? '[]') as unknown
    expect(subs).toEqual([{ name: 'general' }])
  }),
)

effectTest('inbox.subscribe with mentions target does not call /users/me/subscriptions', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedRegisterOk(stub)
    const adapter = yield* buildAdapter(stub)
    yield* adapter.inbox.subscribe('mentions')
    const reqs = yield* stub.captured
    expect(
      reqs.find((r) => r.method === 'POST' && r.url.pathname === '/api/v1/users/me/subscriptions'),
    ).toBeUndefined()
  }),
)

effectTest('inbox.subscribe(mentions) twice registers the events queue only once', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedRegisterOk(stub)
    const adapter = yield* buildAdapter(stub)
    yield* adapter.inbox.subscribe('mentions')
    yield* adapter.inbox.subscribe('mentions')
    const registers = (yield* stub.captured).filter(
      (r) => r.method === 'POST' && r.url.pathname === '/api/v1/register',
    )
    expect(registers).toHaveLength(1)
  }),
)

// ensureQueueRegistered is a read-decide-effectful-write on the
// minter-scoped inboxRef. Two subscribe() calls can interleave — both read
// registration=None, both POST /register — double-registering the events queue
// (one leaks, GC'd by Zulip's TTL). Reachable in production: a single
// process-singleton adapter, no MCP-side serialization, parallel subscribe tool
// calls in one agent turn. The SynchronizedRef.modifyEffect holds the lock
// across registerQueue so the second call observes the first's write and skips.
effectTest('concurrent inbox.subscribe(mentions) calls register the events queue only once', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedRegisterOk(stub)
    const adapter = yield* buildAdapter(stub)
    yield* Effect.all([adapter.inbox.subscribe('mentions'), adapter.inbox.subscribe('mentions')], {
      concurrency: 2,
    })
    const registers = (yield* stub.captured).filter(
      (r) => r.method === 'POST' && r.url.pathname === '/api/v1/register',
    )
    expect(registers).toHaveLength(1)
  }),
)

effectTest('inbox.subscribe flipping mentions -> all re-registers the events queue', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedSubscribeOk(stub, 'general')
    const adapter = yield* buildAdapter(stub)
    yield* adapter.inbox.subscribe('mentions')
    yield* adapter.inbox.subscribe(generalChannel)
    const registers = (yield* stub.captured).filter(
      (r) => r.method === 'POST' && r.url.pathname === '/api/v1/register',
    )
    expect(registers).toHaveLength(2)
  }),
)

effectTest('inbox.unsubscribe(channel) DELETEs /users/me/subscriptions with the stream name', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedUnsubscribeOk(stub, 'general')
    const adapter = yield* buildAdapter(stub)
    yield* adapter.inbox.unsubscribe(generalChannel)
    const req = yield* findRequest(stub, 'DELETE', '/api/v1/users/me/subscriptions')
    const params = new URLSearchParams(req.body)
    const subs = JSON.parse(params.get('subscriptions') ?? '[]') as unknown
    expect(subs).toEqual(['general'])
  }),
)

effectTest('inbox.unsubscribe with mentions target does not call /users/me/subscriptions', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    yield* adapter.inbox.unsubscribe('mentions')
    const reqs = yield* stub.captured
    expect(
      reqs.find(
        (r) => r.method === 'DELETE' && r.url.pathname === '/api/v1/users/me/subscriptions',
      ),
    ).toBeUndefined()
  }),
)

effectTest('inbox.replay(since) returns message-posted events for messages with ts >= since', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    yield* stub.respond('GET', '/api/v1/messages', {
      body: {
        result: 'success',
        messages: [
          {
            id: 1,
            sender_id: 5,
            sender_full_name: 'Robin Reyes',
            stream_id: 100,
            display_recipient: 'general',
            subject: 'lobby',
            content: 'old',
            timestamp: 1000,
          },
          {
            id: 2,
            sender_id: 5,
            sender_full_name: 'Robin Reyes',
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
    })
    const events = yield* adapter.inbox.replay(decodeTimestampSync(2000))
    expect(events).toHaveLength(1)
    expect(events[0]?.kind).toBe('message-posted')
    if (events[0]?.kind === 'message-posted') {
      expect(events[0].message.body).toBe(decodeMessageBodySync('new'))
      expect(events[0].message.ref.channel.name).toEqual(decodeChannelNameSync('general'))
    }
  }),
)

effectTest('inbox.replay calls /messages with anchor=newest and a generous num_before', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const adapter = yield* buildAdapter(stub)
    yield* seedUsers(stub, [HERMES])
    yield* stub.respond('GET', '/api/v1/messages', {
      body: {
        result: 'success',
        messages: [],
        anchor: 0,
        found_anchor: false,
        found_newest: true,
        found_oldest: false,
        history_limited: false,
      },
    })
    yield* adapter.inbox.replay(decodeTimestampSync(0))
    const req = yield* findRequest(stub, 'GET', '/api/v1/messages')
    expect(req.url.searchParams.get('anchor')).toBe('newest')
    expect(Number(req.url.searchParams.get('num_before'))).toBeGreaterThanOrEqual(100)
    expect(req.url.searchParams.get('num_after')).toBe('0')
  }),
)

// The replay schema accepts the stream-message shape only — DMs in
// `/messages` responses have no stream_id, a recipient-array
// `display_recipient`, and an empty subject, so they explode the Zod
// parse. Asking Zulip to filter at the source keeps the parser strict.
effectTest(
  'inbox.replay narrows /messages to exclude DMs so PMs in minter history do not crash the parser',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* seedUsers(stub, [HERMES])
      yield* stub.respond('GET', '/api/v1/messages', {
        body: {
          result: 'success',
          messages: [],
          anchor: 0,
          found_anchor: false,
          found_newest: true,
          found_oldest: false,
          history_limited: false,
        },
      })
      yield* adapter.inbox.replay(decodeTimestampSync(0))
      const req = yield* findRequest(stub, 'GET', '/api/v1/messages')
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
)

effectTest(
  'inbox.replay surfaces mention-received alongside message-posted when flags include "mentioned"',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* seedUsers(stub, [HERMES])
      yield* stub.respond('GET', '/api/v1/messages', {
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
      })
      const events = yield* adapter.inbox.replay(decodeTimestampSync(0))
      expect(events.map((e) => e.kind)).toEqual(['message-posted', 'mention-received'])
    }),
)

effectTest(
  'inbox.events register includes narrow=[["is","mentioned"]] when subscribed mentions',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      // subscribe('mentions') eagerly registers the events queue (the
      // readiness contract — events() must see anything posted after
      // subscribe resolves). The narrow assertion lives here so the queue
      // used by events() is exclusive to mentions.
      const adapter = yield* buildAdapter(stub)
      yield* seedUsers(stub, [HERMES])
      yield* seedRegisterOk(stub, 'queue-1', 0)
      yield* adapter.inbox.subscribe('mentions')
      const reqs = yield* stub.captured
      const reg = reqs.find((r) => r.method === 'POST' && r.url.pathname === '/api/v1/register')
      if (reg === undefined) throw new Error('expected captured POST /api/v1/register')
      const params = new URLSearchParams(reg.body)
      const narrow = JSON.parse(params.get('narrow') ?? 'null') as unknown
      expect(narrow).toEqual([['is', 'mentioned']])
    }),
)

// ─── pre-acquire surfaces run on minter creds ────────────────────

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

effectTest('history.readChannel runs pre-acquire and routes via minter creds', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    yield* stub.respond('GET', '/api/v1/messages', {
      body: { result: 'success', messages: [] },
    })
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    yield* adapter.history.readChannel(generalChannel, {})
    const req = yield* findRequest(stub, 'GET', '/api/v1/messages')
    expect(decodeBasicAuth(req.headers.get('Authorization'))).toEqual(minterAuth)
  }),
)

effectTest('directory.listAgents runs pre-acquire and routes via minter creds', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    const agents = yield* adapter.directory.listAgents()
    expect(agents.map((a) => a.name)).toEqual([decodeDisplayNameSync('hermes-agent')])
    const reqs = yield* stub.captured
    const usersCalls = reqs.filter((r) => r.method === 'GET' && r.url.pathname === '/api/v1/users')
    expect(usersCalls).not.toHaveLength(0)
    for (const call of usersCalls) {
      expect(decodeBasicAuth(call.headers.get('Authorization'))).toEqual(minterAuth)
    }
  }),
)

// The presence read path runs for humans only (agents short-circuit to
// 'unknown'), so the minter-cred routing assertion uses a human.
effectTest('directory.presence runs pre-acquire and routes via minter creds', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedUsers(stub, [MAINTAINER])
    yield* stub.respond('GET', `/api/v1/users/${MAINTAINER.user_id}/presence`, {
      body: { result: 'success', presence: { aggregated: { status: 'active' } } },
    })
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    const presence = yield* adapter.directory.presence({
      id: decodeIdentityIdSync(String(MAINTAINER.user_id)),
      name: decodeDisplayNameSync(MAINTAINER.full_name),
      kind: 'human',
    })
    expect(presence).toBe('online')
    const req = yield* findRequest(stub, 'GET', `/api/v1/users/${MAINTAINER.user_id}/presence`)
    expect(decodeBasicAuth(req.headers.get('Authorization'))).toEqual(minterAuth)
  }),
)

effectTest(
  'inbox.subscribe runs pre-acquire and routes /users/me/subscriptions via minter creds',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedSubscribeOk(stub, 'general')
      const adapter = yield* zulipAdapter(stub, yield* makeConfig())
      yield* adapter.inbox.subscribe(generalChannel)
      const subReq = yield* findRequest(stub, 'POST', '/api/v1/users/me/subscriptions')
      expect(decodeBasicAuth(subReq.headers.get('Authorization'))).toEqual(minterAuth)
      // The /register that arms the events queue must also be minter-creds —
      // the queue belongs to the minter so lurking sessions share it.
      const regReq = yield* findRequest(stub, 'POST', '/api/v1/register')
      expect(decodeBasicAuth(regReq.headers.get('Authorization'))).toEqual(minterAuth)
    }),
)

effectTest(
  'publisher.post still requires acquire — pre-acquire call dies on the "not acquired" invariant',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub, [])
      const adapter = yield* zulipAdapter(stub, yield* makeConfig())
      const exit = yield* Effect.exit(
        adapter.publisher.post(generalChannel, decodeMessageBodySync('hello')),
      )
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        expect(Cause.isDie(exit.cause)).toBe(true)
        expect(String(Cause.squash(exit.cause))).toMatch(/not acquired/)
      }
    }),
)

effectTest('publisher.post after acquire uses BOUND bot creds, not minter creds', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/streams', {
      body: {
        result: 'success',
        streams: [{ stream_id: 1234, name: 'general' }],
      },
    })
    yield* stub.respond('POST', '/api/v1/messages', {
      body: { result: 'success', id: 555 },
    })
    const adapter = yield* buildAdapter(stub) // acquires hermes-agent → rotates key to "fresh-key"
    yield* adapter.publisher.post(
      generalChannel,
      decodeMessageBodySync('attribution-producing message'),
    )
    const req = yield* findRequest(stub, 'POST', '/api/v1/messages')
    const auth = decodeBasicAuth(req.headers.get('Authorization'))
    expect(auth.email).toBe(HERMES.email)
    expect(auth.apiKey).toBe('fresh-key')
    // And it must not be minter creds.
    expect(auth).not.toEqual(minterAuth)
  }),
)

const seedStreamsList = (
  stub: StubHttpClient,
  streams: ReadonlyArray<{ readonly stream_id: number; readonly name: string }>,
): Effect.Effect<void> =>
  stub.respond('GET', '/api/v1/streams', {
    body: { result: 'success', streams },
  })

effectTest('reconcileMinterSubscriptions GETs /streams filtered to public-not-subscribed', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedStreamsList(stub, [])
    yield* seedUsers(stub, [])
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    yield* adapter.reconcileMinterSubscriptions()
    const req = yield* findRequest(stub, 'GET', '/api/v1/streams')
    expect(req.url.searchParams.get('include_public')).toBe('true')
    expect(req.url.searchParams.get('include_subscribed')).toBe('false')
    yield* Effect.promise(() => adapter.close())
  }),
)

effectTest('reconcileMinterSubscriptions returns empty added when the realm reports no gap', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedStreamsList(stub, [])
    yield* seedUsers(stub, [])
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    const report = yield* adapter.reconcileMinterSubscriptions()
    expect(report).toEqual({ added: [], error: undefined })
    const reqs = yield* stub.captured
    expect(
      reqs.find((r) => r.method === 'POST' && r.url.pathname === '/api/v1/users/me/subscriptions'),
    ).toBeUndefined()
    yield* Effect.promise(() => adapter.close())
  }),
)

effectTest(
  'reconcileMinterSubscriptions batches every unsubscribed public stream into one POST',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedStreamsList(stub, [
        { stream_id: 11, name: 'commy' },
        { stream_id: 12, name: 'myproject-a' },
        { stream_id: 13, name: 'myproject-b' },
      ])
      yield* stub.respond('POST', '/api/v1/users/me/subscriptions', {
        body: {
          result: 'success',
          subscribed: { 'minter@example.com': ['commy', 'myproject-a', 'myproject-b'] },
          already_subscribed: {},
          unauthorized: [],
        },
      })
      yield* seedUsers(stub, [])
      const adapter = yield* zulipAdapter(stub, yield* makeConfig())
      const report = yield* adapter.reconcileMinterSubscriptions()
      expect(report.added).toEqual([
        decodeChannelNameSync('commy'),
        decodeChannelNameSync('myproject-a'),
        decodeChannelNameSync('myproject-b'),
      ])
      expect(report.error).toBeUndefined()
      const post = yield* findRequest(stub, 'POST', '/api/v1/users/me/subscriptions')
      const subs = JSON.parse(
        new URLSearchParams(post.body).get('subscriptions') ?? '[]',
      ) as unknown
      expect(subs).toEqual([{ name: 'commy' }, { name: 'myproject-a' }, { name: 'myproject-b' }])
      yield* Effect.promise(() => adapter.close())
    }),
)

effectTest(
  'reconcileMinterSubscriptions reports only the streams the realm confirms as newly subscribed',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* seedStreamsList(stub, [
        { stream_id: 11, name: 'commy' },
        { stream_id: 12, name: 'myproject-b' },
      ])
      // Race: another reconciler already subscribed `myproject-b` between
      // our list and post. Zulip puts it under already_subscribed and the
      // reconciler's report mirrors that.
      yield* stub.respond('POST', '/api/v1/users/me/subscriptions', {
        body: {
          result: 'success',
          subscribed: { 'minter@example.com': ['commy'] },
          already_subscribed: { 'minter@example.com': ['myproject-b'] },
          unauthorized: [],
        },
      })
      yield* seedUsers(stub, [])
      const adapter = yield* zulipAdapter(stub, yield* makeConfig())
      const report = yield* adapter.reconcileMinterSubscriptions()
      expect(report.added).toEqual([decodeChannelNameSync('commy')])
      expect(report.error).toBeUndefined()
      yield* Effect.promise(() => adapter.close())
    }),
)

effectTest('reconcileMinterSubscriptions captures list failure without throwing', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/streams', {
      status: 500,
      body: { result: 'error', msg: 'realm unreachable' },
    })
    yield* seedUsers(stub, [])
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    const report = yield* adapter.reconcileMinterSubscriptions()
    expect(report.added).toEqual([])
    expect(report.error).toBe('realm unreachable')
    const reqs = yield* stub.captured
    expect(
      reqs.find((r) => r.method === 'POST' && r.url.pathname === '/api/v1/users/me/subscriptions'),
    ).toBeUndefined()
    yield* Effect.promise(() => adapter.close())
  }),
)

effectTest('reconcileMinterSubscriptions routes via minter creds', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedStreamsList(stub, [{ stream_id: 11, name: 'commy' }])
    yield* stub.respond('POST', '/api/v1/users/me/subscriptions', {
      body: {
        result: 'success',
        subscribed: { 'minter@example.com': ['commy'] },
        already_subscribed: {},
        unauthorized: [],
      },
    })
    yield* seedUsers(stub, [])
    const adapter = yield* zulipAdapter(stub, yield* makeConfig())
    yield* adapter.reconcileMinterSubscriptions()
    const listReq = yield* findRequest(stub, 'GET', '/api/v1/streams')
    const postReq = yield* findRequest(stub, 'POST', '/api/v1/users/me/subscriptions')
    expect(decodeBasicAuth(listReq.headers.get('Authorization'))).toEqual(minterAuth)
    expect(decodeBasicAuth(postReq.headers.get('Authorization'))).toEqual(minterAuth)
    yield* Effect.promise(() => adapter.close())
  }),
)

// --- attachmentReference ---

test('attachmentReference renders a Zulip markdown link, filename as text and url as target', () => {
  expect(
    attachmentReference({
      filename: 'chart.png',
      url: decodeUserUploadPathSync('/user_uploads/1/ab/chart.png'),
    }),
  ).toBe('[chart.png](/user_uploads/1/ab/chart.png)')
})

test('downloadFile rejects an unbranded urlPath at the type level', () => {
  // Method-param bivariance does not let a bare string through
  // `downloadFile(urlPath: UserUploadPath)`: bivariance only loosens
  // function-type assignability, never a direct call-site argument check — so
  // the UserUploadPath brand bites here even though downloadFile is declared
  // with method shorthand on the intersection-typed ZulipAdapter. If the
  // suppression below stops firing, the brand has genuinely been weakened.
  const proof = (adapter: ZulipAdapter): void => {
    // @ts-expect-error — urlPath must be UserUploadPath, not string
    void adapter.downloadFile('raw-unbranded-path')
    void adapter.downloadFile(decodeUserUploadPathSync('/user_uploads/1/ab/x.png'))
  }
  expect(proof).toBeTypeOf('function')
})

// --- UserUploadPath brand ---

test('attachmentReference refuses an unbranded url at the type level', () => {
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
