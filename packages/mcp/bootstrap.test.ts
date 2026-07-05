import { describe, expect, test } from 'bun:test'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { MessageInbox, SubscriptionTarget } from '@commy/core/ports'
import {
  decodeBotNameSync,
  decodeChannelNameSync,
  decodeThreadNameSync,
  InboxError,
} from '@commy/core/ports'
import { ApiKey, BotEmail, RealmUrl } from '@commy/zulip/http'
import { NodeContext } from '@effect/platform-node'
import { Effect, Option, Redacted, Stream } from 'effect'
import type { ParsedEnv, ProjectSlug, SessionId } from './bootstrap.ts'
import {
  composeBotName,
  deriveProject,
  EnvConfigError,
  InRepo,
  NotInRepo,
  parseBotName,
  parseEnv,
  parseSessionId,
  readBootSessionId,
  readGitContext,
  sanitiseProjectSlug,
  subscribeFromEnv,
} from './bootstrap.ts'
import { createNarrowSet } from './narrow-set.ts'
import { SubscribeTokenError } from './subscribe-parser.ts'
import { testConfigProviderLayer } from './test-platform.ts'

/**
 * `parseEnv` reads the ambient ConfigProvider; the tests
 * set it per-call from a fixture env map via `testConfigProviderLayer`,
 * mirroring how the app edge sets it from `process.env`.
 */
const parse = (env: Record<string, string | undefined>): Effect.Effect<ParsedEnv, EnvConfigError> =>
  parseEnv.pipe(Effect.provide(testConfigProviderLayer(env)))

const baseRequiredEnvEffect: Effect.Effect<ParsedEnv> = Effect.gen(function* () {
  const realmUrl = yield* RealmUrl('https://zulip.example.com')
  const minterEmail = yield* BotEmail('minter-bot@zulip.example.com')
  const minterApiKey = Redacted.make(yield* ApiKey('kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk1'))
  return { realmUrl, minterEmail, minterApiKey }
}).pipe(Effect.orDie)

const expectParseEnvError = (
  env: Record<string, string | undefined>,
): Effect.Effect<EnvConfigError, ParsedEnv> =>
  Effect.gen(function* () {
    const err = yield* Effect.flip(parse(env))
    expect(err).toBeInstanceOf(EnvConfigError)
    return err
  })

const fullEnv = {
  ZULIP_SITE: 'https://zulip.example.com',
  ZULIP_MINTER_EMAIL: 'minter-bot@zulip.example.com',
  ZULIP_MINTER_API_KEY: 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk1',
  COMMY_BOT_NAME: 'myproject-concierge',
  COMMY_SUBSCRIBE: 'channel:home,channel:llm-feed',
} as const

test('parseEnv fails with EnvConfigError when all required vars are missing', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const err = yield* expectParseEnvError({})
      expect(err.message).toContain('ZULIP_SITE')
      expect(err.message).toContain('ZULIP_MINTER_EMAIL')
      expect(err.message).toContain('ZULIP_MINTER_API_KEY')
    }),
  ))

test('parseEnv fails with EnvConfigError when only ZULIP_SITE is missing', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const { ZULIP_SITE: _zulipSite, ...rest } = fullEnv
      const err = yield* expectParseEnvError(rest)
      expect(err.message).toContain('ZULIP_SITE')
      expect(err.message).not.toContain('ZULIP_MINTER_EMAIL')
      expect(err.message).not.toContain('ZULIP_MINTER_API_KEY')
    }),
  ))

test('parseEnv fails with EnvConfigError when only ZULIP_MINTER_EMAIL is missing', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const { ZULIP_MINTER_EMAIL: _email, ...rest } = fullEnv
      const err = yield* expectParseEnvError(rest)
      expect(err.message).toContain('ZULIP_MINTER_EMAIL')
      expect(err.message).not.toContain('ZULIP_SITE')
      expect(err.message).not.toContain('ZULIP_MINTER_API_KEY')
    }),
  ))

test('parseEnv fails with EnvConfigError when only ZULIP_MINTER_API_KEY is missing', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const { ZULIP_MINTER_API_KEY: _apiKey, ...rest } = fullEnv
      const err = yield* expectParseEnvError(rest)
      expect(err.message).toContain('ZULIP_MINTER_API_KEY')
      expect(err.message).not.toContain('ZULIP_SITE')
      expect(err.message).not.toContain('ZULIP_MINTER_EMAIL')
    }),
  ))

test('parseEnv fails with EnvConfigError when ZULIP_SITE is not a valid URL', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const err = yield* expectParseEnvError({ ...fullEnv, ZULIP_SITE: 'not a url' })
      expect(err.message).toContain('ZULIP_SITE')
      expect(err.message.toLowerCase()).toMatch(/url|parseable|protocol/)
    }),
  ))

test('parseEnv rejects required vars present but empty (ZULIP_SITE)', () =>
  Effect.runPromise(expectParseEnvError({ ...fullEnv, ZULIP_SITE: '' }).pipe(Effect.asVoid)))

test('parseEnv rejects required vars present but empty (ZULIP_MINTER_EMAIL)', () =>
  Effect.runPromise(
    expectParseEnvError({ ...fullEnv, ZULIP_MINTER_EMAIL: '' }).pipe(Effect.asVoid),
  ))

test('parseEnv rejects required vars present but empty (ZULIP_MINTER_API_KEY)', () =>
  Effect.runPromise(
    expectParseEnvError({ ...fullEnv, ZULIP_MINTER_API_KEY: '' }).pipe(Effect.asVoid),
  ))

test('parseEnv rejects optional vars present but empty (COMMY_BOT_NAME)', () =>
  Effect.runPromise(expectParseEnvError({ ...fullEnv, COMMY_BOT_NAME: '' }).pipe(Effect.asVoid)))

test('parseEnv treats empty COMMY_SUBSCRIBE as unset (user_config-backed)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({ ...fullEnv, COMMY_SUBSCRIBE: '' })
      expect('subscribe' in parsed).toBe(false)
    }),
  ))

// biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — testing placeholder rejection
test('parseEnv treats unsubstituted ${user_config.COMMY_SUBSCRIBE} as unset', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({
        ...fullEnv,
        // biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — testing placeholder rejection
        COMMY_SUBSCRIBE: '${user_config.COMMY_SUBSCRIBE}',
      })
      expect('subscribe' in parsed).toBe(false)
    }),
  ))

// biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — testing placeholder rejection
test('parseEnv rejects COMMY_SUBSCRIBE when value is a non-user_config ${...} placeholder', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const err = yield* expectParseEnvError({
        ...fullEnv,
        // biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — testing placeholder rejection
        COMMY_SUBSCRIBE: '${SUBSCRIBE_TARGETS}',
      })
      expect(err.message).toContain('COMMY_SUBSCRIBE')
      expect(err.message.toLowerCase()).toMatch(/substitut|placeholder/)
    }),
  ))

// --- COMMY_* env config ---
// Optional COMMY_* keys are read from the canonical form; a value that is
// present but invalid fails loudly rather than being silently ignored.

const requiredOnlyEnv = {
  ZULIP_SITE: 'https://zulip.example.com',
  ZULIP_MINTER_EMAIL: 'minter-bot@zulip.example.com',
  ZULIP_MINTER_API_KEY: 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk1',
} as const

test('parseEnv reads botName from canonical COMMY_BOT_NAME', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({ ...requiredOnlyEnv, COMMY_BOT_NAME: 'canonical-bot' })
      expect(parsed.botName).toBe(decodeBotNameSync('canonical-bot'))
    }),
  ))

test('parseEnv rejects a set-but-invalid COMMY_BOT_NAME with a substrate-safe message', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const err = yield* expectParseEnvError({
        ...requiredOnlyEnv,
        COMMY_BOT_NAME: 'Has Spaces',
      })
      expect(err.message).toContain('COMMY_BOT_NAME')
      expect(err.message).toContain('must be substrate-safe')
      expect(err.message).toContain('Has Spaces')
    }),
  ))

test('parseEnv reads subscribe from canonical COMMY_SUBSCRIBE', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({ ...requiredOnlyEnv, COMMY_SUBSCRIBE: 'channel:home' })
      expect(parsed.subscribe).toBe('channel:home')
    }),
  ))

test('parseEnv reads project from canonical COMMY_PROJECT', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({ ...requiredOnlyEnv, COMMY_PROJECT: 'canonproj' })
      expect(parsed.project as string | undefined).toBe('canonproj')
    }),
  ))

test('parseEnv reads catchupWindowSeconds from canonical COMMY_CATCHUP_WINDOW_SECONDS', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({ ...requiredOnlyEnv, COMMY_CATCHUP_WINDOW_SECONDS: '3600' })
      expect(parsed.catchupWindowSeconds).toBe(3600)
    }),
  ))

// biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — testing placeholder rejection
test('parseEnv rejects ZULIP_SITE when value is an unsubstituted ${user_config.X} placeholder', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const err = yield* expectParseEnvError({
        ...fullEnv,
        // biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — testing placeholder rejection
        ZULIP_SITE: '${user_config.ZULIP_SITE}',
      })
      expect(err.message).toContain('ZULIP_SITE')
      expect(err.message.toLowerCase()).toMatch(/substitut|placeholder/)
    }),
  ))

test('parseEnv returns required-only result when no optionals set', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const env = {
        ZULIP_SITE: fullEnv.ZULIP_SITE,
        ZULIP_MINTER_EMAIL: fullEnv.ZULIP_MINTER_EMAIL,
        ZULIP_MINTER_API_KEY: fullEnv.ZULIP_MINTER_API_KEY,
      }
      const parsed = yield* parse(env)
      expect(parsed.realmUrl).toBe(fullEnv.ZULIP_SITE as typeof parsed.realmUrl)
      expect(parsed.minterEmail).toBe(fullEnv.ZULIP_MINTER_EMAIL as typeof parsed.minterEmail)
      expect(Redacted.value(parsed.minterApiKey)).toBe(
        fullEnv.ZULIP_MINTER_API_KEY as Redacted.Redacted.Value<typeof parsed.minterApiKey>,
      )
      expect('botName' in parsed).toBe(false)
      expect('subscribe' in parsed).toBe(false)
    }),
  ))

test('parseEnv returns all fields when every var is present', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse(fullEnv)
      expect(parsed.realmUrl).toBe(fullEnv.ZULIP_SITE as typeof parsed.realmUrl)
      expect(parsed.minterEmail).toBe(fullEnv.ZULIP_MINTER_EMAIL as typeof parsed.minterEmail)
      expect(Redacted.value(parsed.minterApiKey)).toBe(
        fullEnv.ZULIP_MINTER_API_KEY as Redacted.Redacted.Value<typeof parsed.minterApiKey>,
      )
      expect(parsed.botName).toBe(decodeBotNameSync(fullEnv.COMMY_BOT_NAME))
      expect(parsed.subscribe).toBe(fullEnv.COMMY_SUBSCRIBE)
    }),
  ))

// Attach mode: COMMY_BOT_API_KEY supplies the stable key for the
// COMMY_BOT_NAME persona, so the server binds it without regenerating.
const BOT_API_KEY = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2'

test('parseEnv reads attachIdentity from COMMY_BOT_NAME + COMMY_BOT_API_KEY', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({ ...fullEnv, COMMY_BOT_API_KEY: BOT_API_KEY })
      expect(parsed.attachIdentity?.name).toBe(decodeBotNameSync(fullEnv.COMMY_BOT_NAME))
      expect(parsed.attachIdentity).toBeDefined()
      if (parsed.attachIdentity !== undefined) {
        expect(Redacted.value(parsed.attachIdentity.apiKey)).toBe(
          BOT_API_KEY as Redacted.Redacted.Value<typeof parsed.attachIdentity.apiKey>,
        )
        // The supplied key is masked everywhere it could be logged.
        expect(String(parsed.attachIdentity.apiKey)).toBe('<redacted>')
      }
      expect(JSON.stringify(parsed)).not.toContain(BOT_API_KEY)
    }),
  ))

test('parseEnv rejects COMMY_BOT_API_KEY without COMMY_BOT_NAME', () =>
  Effect.runPromise(
    expectParseEnvError({
      ...fullEnv,
      COMMY_BOT_NAME: undefined,
      COMMY_BOT_API_KEY: BOT_API_KEY,
    }).pipe(Effect.asVoid),
  ))

test('parseEnv with COMMY_BOT_NAME but no COMMY_BOT_API_KEY yields no attachIdentity (mint path)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse(fullEnv)
      expect('attachIdentity' in parsed).toBe(false)
    }),
  ))

test('parseEnv wraps the minter api key as Redacted so the secret never logs', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse(fullEnv)
      const raw = fullEnv.ZULIP_MINTER_API_KEY

      expect(Redacted.isRedacted(parsed.minterApiKey)).toBe(true)
      // The raw secret must NOT appear in any rendering of the value.
      expect(String(parsed.minterApiKey)).toBe('<redacted>')
      expect(JSON.stringify(parsed.minterApiKey)).toBe('"<redacted>"')
      expect(`${parsed.minterApiKey}`).not.toContain(raw)
      // Nor when the whole parsed-env struct is stringified.
      expect(JSON.stringify(parsed)).not.toContain(raw)
      // The raw header value is still recoverable at the auth boundary.
      expect(Redacted.value(parsed.minterApiKey)).toBe(
        raw as Redacted.Redacted.Value<typeof parsed.minterApiKey>,
      )
    }),
  ))

test('parseEnv ignores unrelated env vars', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({
        ZULIP_SITE: fullEnv.ZULIP_SITE,
        ZULIP_MINTER_EMAIL: fullEnv.ZULIP_MINTER_EMAIL,
        ZULIP_MINTER_API_KEY: fullEnv.ZULIP_MINTER_API_KEY,
        PATH: '/usr/bin',
        HOME: '/home/user',
      })
      expect('botName' in parsed).toBe(false)
    }),
  ))

// ─── parseSessionId ───────────────────────────────────────
//
// SessionId is a branded type — only `parseSessionId` can mint one. The
// validating constructor demands UUID format so a model-guessed string
// (`my-session-...`, `cc-mysess...`, or any other garbage that leaked
// through a missing hook + non-CC client's "any per-conversation string"
// loophole) cannot reach `composeBotName` and mint a malformed
// `cc-<project>-<garbage>`
// identity.

const SAMPLE_UUID = '98a364ab-ea1f-4eaa-9a97-bacbe68c581f'

test('parseSessionId returns a SessionId for a valid UUID', () => {
  const sid = parseSessionId(SAMPLE_UUID)
  expect(Option.isSome(sid)).toBe(true)
  // Branded type still equals the raw string at runtime.
  expect(Option.getOrUndefined(sid) as unknown as string).toBe(SAMPLE_UUID)
})

test('parseSessionId accepts uppercase UUIDs', () => {
  expect(Option.isSome(parseSessionId(SAMPLE_UUID.toUpperCase()))).toBe(true)
})

test('parseSessionId rejects a non-UUID string', () => {
  expect(Option.isNone(parseSessionId('my-debug-session'))).toBe(true)
})

test('parseSessionId rejects a UUID prefix that the model might leak', () => {
  expect(Option.isNone(parseSessionId('my-session-'))).toBe(true)
  expect(Option.isNone(parseSessionId('cc-mysess'))).toBe(true)
  expect(Option.isNone(parseSessionId('myproject'))).toBe(true)
})

test('parseSessionId rejects a hex string without UUID dashes', () => {
  expect(Option.isNone(parseSessionId('abcdef1234567890abcdef1234567890'))).toBe(true)
})

test('parseSessionId rejects an empty string', () => {
  expect(Option.isNone(parseSessionId(''))).toBe(true)
})

test('parseSessionId rejects non-string inputs', () => {
  expect(Option.isNone(parseSessionId(undefined))).toBe(true)
  expect(Option.isNone(parseSessionId(null))).toBe(true)
  expect(Option.isNone(parseSessionId(123))).toBe(true)
  expect(Option.isNone(parseSessionId({}))).toBe(true)
})

const sid = (raw: string): SessionId => {
  const result = parseSessionId(raw)
  if (Option.isNone(result)) {
    throw new Error(`bootstrap.test sid helper: not a UUID: ${raw}`)
  }
  return result.value
}

const slug = (raw: string): ProjectSlug => {
  const result = sanitiseProjectSlug(raw)
  if (Option.isNone(result)) {
    throw new Error(`bootstrap.test slug helper: sanitises to nothing: ${raw}`)
  }
  return result.value
}

test('composeBotName produces cc-<8> when project is undefined', () => {
  expect(composeBotName({ sessionId: sid(SAMPLE_UUID) })).toBe(decodeBotNameSync('cc-98a364ab'))
})

test('composeBotName refuses an unbranded string at the type level', () => {
  // The whole point of the SessionId brand: a bare `string` from
  // `args['session_id']` must not reach `composeBotName`. The compiler
  // catches this — if this @ts-expect-error stops erroring, the brand
  // has been weakened and this class of bug is reintroducible.
  // @ts-expect-error — sessionId must be SessionId, not string
  composeBotName({ sessionId: 'my-debug-session' })
  // Sanity: the same call with a parsed SessionId compiles fine.
  composeBotName({ sessionId: sid(SAMPLE_UUID) })
})

test('composeBotName refuses an unbranded project string at the type level', () => {
  // The ProjectSlug brand: a bare `string` from COMMY_PROJECT
  // must not reach `composeBotName` without passing through
  // `sanitiseProjectSlug`. The compiler catches this — if the
  // ts-expect-error below stops erroring, the brand has been weakened.
  // @ts-expect-error — project must be ProjectSlug, not string
  composeBotName({ sessionId: sid(SAMPLE_UUID), project: 'raw-unsanitised' })
  composeBotName({ sessionId: sid(SAMPLE_UUID), project: slug('myproject') })
})

// ─── parseBotName ────────────────────────────────────────
//
// BotName is a branded type — `parseBotName` validates the substrate-safe
// shape (lowercase ASCII, digits, dashes, underscores; starts with letter;
// max 40 chars). `composeBotName` is the ephemeral mint point. Together
// they ensure no unvalidated string reaches `IdentityPort.acquire`.

test('parseBotName accepts a valid substrate-safe name', () => {
  const name = parseBotName('myproject-concierge')
  expect(Option.isSome(name)).toBe(true)
  expect(Option.getOrUndefined(name)).toBe(decodeBotNameSync('myproject-concierge'))
})

test('parseBotName accepts names with underscores and digits', () => {
  expect(Option.isSome(parseBotName('cc-my_project-abcdef12'))).toBe(true)
})

test('parseBotName rejects empty strings', () => {
  expect(Option.isNone(parseBotName(''))).toBe(true)
})

test('parseBotName rejects names starting with a digit', () => {
  expect(Option.isNone(parseBotName('123-bot'))).toBe(true)
})

test('parseBotName rejects names starting with a dash', () => {
  expect(Option.isNone(parseBotName('-bot'))).toBe(true)
})

test('parseBotName rejects uppercase characters', () => {
  expect(Option.isNone(parseBotName('Myproject'))).toBe(true)
})

test('parseBotName rejects spaces', () => {
  expect(Option.isNone(parseBotName('my bot'))).toBe(true)
})

test('parseBotName rejects names longer than 40 chars', () => {
  expect(Option.isNone(parseBotName('a'.repeat(41)))).toBe(true)
  expect(Option.isSome(parseBotName('a'.repeat(40)))).toBe(true)
})

test('IdentityPort.acquire refuses an unbranded string at the type level', () => {
  // The BotName brand: a bare `string` must not reach `acquire` without
  // going through `composeBotName` or `parseBotName`. The compiler catches
  // this — if the @ts-expect-error below stops erroring, the brand has
  // been weakened and unvalidated strings can flow into the identity pipeline.
  const fakePort = {
    acquire: async (_name: import('@commy/core/ports').BotName) => ({}),
  }
  // @ts-expect-error — name must be BotName, not string
  fakePort.acquire('raw-unbranded-string')
  fakePort.acquire(decodeBotNameSync('branded-value'))
})

test('parseEnv rejects COMMY_BOT_NAME that fails substrate-safe validation', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* expectParseEnvError({ ...fullEnv, COMMY_BOT_NAME: 'Has Spaces' })
      yield* expectParseEnvError({ ...fullEnv, COMMY_BOT_NAME: '123-starts-digit' })
    }),
  ))

test('parseEnv sanitises COMMY_PROJECT at parse time', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({ ...fullEnv, COMMY_PROJECT: 'My_Project' })
      expect(parsed.project as string | undefined).toBe('my-project')
    }),
  ))

test('composeBotName produces cc-<project>-<8> when both are present', () => {
  expect(
    composeBotName({
      sessionId: sid('abcdef12-3456-4789-89ab-cdef01234567'),
      project: slug('myproject'),
    }),
  ).toBe(decodeBotNameSync('cc-myproject-abcdef12'))
})

test('composeBotName slices session id to 8 chars', () => {
  // UUIDs are always 36 chars; the slice always takes the leading 8 hex chars.
  expect(composeBotName({ sessionId: sid('12345678-9abc-4def-8123-456789abcdef') })).toBe(
    decodeBotNameSync('cc-12345678'),
  )
})

test('composeBotName stays within the 24-char budget for max project length', () => {
  const name = composeBotName({
    sessionId: sid('abcdef12-3456-4789-89ab-cdef01234567'),
    project: slug('myproject-ab'), // 12 chars (the budget)
  })
  expect(name.length).toBeLessThanOrEqual(24)
  expect(name).toBe(decodeBotNameSync('cc-myproject-ab-abcdef12'))
})

// sanitiseProjectSlug assertions cast to `string | undefined` because
// `ProjectSlug` is a branded type — the `.toBe()` overload
// rejects a bare `string` as the expected value. The cast is safe: the
// brand is phantom; at runtime it IS a string.
const slugStr = (raw: string): string | undefined =>
  Option.getOrUndefined(sanitiseProjectSlug(raw)) as string | undefined

test('sanitiseProjectSlug lowercases the input', () => {
  expect(slugStr('MyProject')).toBe('myproject')
})

test('sanitiseProjectSlug replaces underscores and slashes with dashes', () => {
  expect(slugStr('my_project')).toBe('my-project')
  expect(slugStr('foo/bar')).toBe('foo-bar')
})

test('sanitiseProjectSlug truncates to 12 chars', () => {
  expect(slugStr('abcdefghijklmnop')).toBe('abcdefghijkl')
})

test('sanitiseProjectSlug replaces chars outside [a-z0-9-] with a dash', () => {
  expect(slugStr('foo.bar!baz')).toBe('foo-bar-baz')
})

test('sanitiseProjectSlug collapses consecutive dashes', () => {
  expect(slugStr('foo--bar')).toBe('foo-bar')
})

test('sanitiseProjectSlug strips leading and trailing dashes', () => {
  expect(slugStr('-foo-')).toBe('foo')
})

test('sanitiseProjectSlug trims trailing dash produced by truncation', () => {
  expect(slugStr('hello-world-x')).toBe('hello-world')
})

test('sanitiseProjectSlug returns undefined when the result is empty', () => {
  expect(slugStr('')).toBeUndefined()
  expect(slugStr('---')).toBeUndefined()
})

test('sanitiseProjectSlug returns undefined when the result starts with a digit', () => {
  expect(slugStr('123abc')).toBeUndefined()
})

test('sanitiseProjectSlug preserves digits and dashes inside the slug', () => {
  expect(slugStr('myproject-b-3')).toBe('myproject-b')
  expect(slugStr('a1b2c3')).toBe('a1b2c3')
})

// deriveProject returns ProjectSlug | undefined; cast to
// string | undefined for assertion readability (same as slugStr above).
const deriveStr = (deps: Parameters<typeof deriveProject>[0]): string | undefined =>
  Option.getOrUndefined(Effect.runSync(deriveProject(deps))) as string | undefined

test('deriveProject prefers explicit COMMY_PROJECT env value', () => {
  const result = deriveStr({
    project: slug('override'),
    cwd: '/home/x/anything',
    readGitContext: () =>
      Effect.succeed(
        InRepo({ gitRoot: '/home/x/anything', remoteBasename: Option.some('remote-name') }),
      ),
  })
  expect(result).toBe('override')
})

test('deriveProject returns a pre-sanitised ProjectSlug verbatim', () => {
  const result = deriveStr({
    project: slug('My_Project'),
    cwd: '/home/x',
    readGitContext: () => Effect.succeed(NotInRepo()),
  })
  expect(result).toBe('my-project')
})

test('deriveProject falls back to git remote basename when env value absent', () => {
  const result = deriveStr({
    cwd: '/home/x/myproject',
    readGitContext: () =>
      Effect.succeed(
        InRepo({ gitRoot: '/home/x/myproject', remoteBasename: Option.some('myproject') }),
      ),
  })
  expect(result).toBe('myproject')
})

test('deriveProject sanitises the git remote basename (mid-word truncation acceptable)', () => {
  const result = deriveStr({
    cwd: '/home/x/foo',
    readGitContext: () =>
      Effect.succeed(
        InRepo({ gitRoot: '/home/x/foo', remoteBasename: Option.some('My-Project_v2') }),
      ),
  })
  expect(result).toBe('my-project-v')
})

test('deriveProject falls back to git-root basename when no remote', () => {
  const result = deriveStr({
    cwd: '/home/x/myproject/scripts',
    readGitContext: () =>
      Effect.succeed(InRepo({ gitRoot: '/home/x/myproject', remoteBasename: Option.none() })),
  })
  expect(result).toBe('myproject')
})

test('deriveProject returns undefined when cwd is not in a git repo', () => {
  const result = deriveStr({
    cwd: '/tmp',
    readGitContext: () => Effect.succeed(NotInRepo()),
  })
  expect(result).toBeUndefined()
})

test('parseEnv treats COMMY_PROJECT that sanitises to nothing as unset', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({ ...fullEnv, COMMY_PROJECT: '---' })
      expect('project' in parsed).toBe(false)
    }),
  ))

test('parseEnv reads COMMY_PROJECT into parsed.project', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({ ...fullEnv, COMMY_PROJECT: 'myproject' })
      expect(parsed.project as string | undefined).toBe('myproject')
    }),
  ))

test('parseEnv rejects COMMY_PROJECT when empty', () =>
  Effect.runPromise(expectParseEnvError({ ...fullEnv, COMMY_PROJECT: '' }).pipe(Effect.asVoid)))

test('parseEnv rejects COMMY_PROJECT when value is an unsubstituted placeholder', () =>
  Effect.runPromise(
    // biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — testing placeholder rejection
    expectParseEnvError({ ...fullEnv, COMMY_PROJECT: '${user_config.X}' }).pipe(Effect.asVoid),
  ))

test('parseEnv leaves project absent when COMMY_PROJECT is unset', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse(fullEnv)
      expect('project' in parsed).toBe(false)
    }),
  ))

test('parseEnv reads COMMY_CATCHUP_WINDOW_SECONDS into parsed.catchupWindowSeconds', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({ ...fullEnv, COMMY_CATCHUP_WINDOW_SECONDS: '3600' })
      expect(parsed.catchupWindowSeconds).toBe(3600)
    }),
  ))

test('parseEnv accepts COMMY_CATCHUP_WINDOW_SECONDS=0 (disable catch-up)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({ ...fullEnv, COMMY_CATCHUP_WINDOW_SECONDS: '0' })
      expect(parsed.catchupWindowSeconds).toBe(0)
    }),
  ))

test('parseEnv treats empty COMMY_CATCHUP_WINDOW_SECONDS as unset', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({ ...fullEnv, COMMY_CATCHUP_WINDOW_SECONDS: '' })
      expect('catchupWindowSeconds' in parsed).toBe(false)
    }),
  ))

// biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — testing placeholder rejection
test('parseEnv treats unsubstituted ${user_config.COMMY_CATCHUP_WINDOW_SECONDS} as unset', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse({
        ...fullEnv,
        // biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — testing placeholder rejection
        COMMY_CATCHUP_WINDOW_SECONDS: '${user_config.COMMY_CATCHUP_WINDOW_SECONDS}',
      })
      expect('catchupWindowSeconds' in parsed).toBe(false)
    }),
  ))

test('parseEnv rejects COMMY_CATCHUP_WINDOW_SECONDS that is not a non-negative integer', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* expectParseEnvError({ ...fullEnv, COMMY_CATCHUP_WINDOW_SECONDS: 'banana' })
      yield* expectParseEnvError({ ...fullEnv, COMMY_CATCHUP_WINDOW_SECONDS: '-30' })
      yield* expectParseEnvError({ ...fullEnv, COMMY_CATCHUP_WINDOW_SECONDS: '3.5' })
    }),
  ))

test('parseEnv leaves catchupWindowSeconds absent when env var is unset', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const parsed = yield* parse(fullEnv)
      expect('catchupWindowSeconds' in parsed).toBe(false)
    }),
  ))

interface FakeInbox {
  readonly inbox: MessageInbox
  readonly calls: { readonly subscribed: SubscriptionTarget[] }
}

const buildFakeInbox = (options: { readonly rejectOn?: number } = {}): FakeInbox => {
  const subscribed: SubscriptionTarget[] = []
  let calls = 0
  const inbox: MessageInbox = {
    subscribe: (target) =>
      Effect.suspend(() => {
        calls += 1
        if (options.rejectOn !== undefined && calls === options.rejectOn) {
          return Effect.fail(
            new InboxError({
              operation: 'subscribe',
              cause: new Error(`fake-inbox rejected call #${calls}`),
            }),
          )
        }
        subscribed.push(target)
        return Effect.void
      }),
    unsubscribe: () => Effect.void,
    events: () => Stream.empty,
    replay: () => Effect.succeed([]),
  }
  return { inbox, calls: { subscribed } }
}

test('subscribeFromEnv is a no-op when COMMY_SUBSCRIBE is unset', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fake = buildFakeInbox()
      const narrowSet = createNarrowSet()
      const baseRequiredEnv = yield* baseRequiredEnvEffect
      yield* subscribeFromEnv(fake.inbox, narrowSet, baseRequiredEnv)
      expect(fake.calls.subscribed).toEqual([])
      expect(narrowSet.size()).toBe(0)
    }),
  ))

test('subscribeFromEnv subscribes to each comma-separated token in order', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fake = buildFakeInbox()
      const narrowSet = createNarrowSet()
      const baseRequiredEnv = yield* baseRequiredEnvEffect
      const parsed: ParsedEnv = {
        ...baseRequiredEnv,
        subscribe: 'channel:home,thread:home/payments,mentions',
      }
      yield* subscribeFromEnv(fake.inbox, narrowSet, parsed)
      expect(fake.calls.subscribed).toEqual([
        decodeChannelNameSync('home'),
        {
          channel: decodeChannelNameSync('home'),
          thread: decodeThreadNameSync('payments'),
        },
        'mentions',
      ])
      expect(narrowSet.size()).toBe(3)
    }),
  ))

test('subscribeFromEnv trims whitespace around each token', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fake = buildFakeInbox()
      const narrowSet = createNarrowSet()
      const baseRequiredEnv = yield* baseRequiredEnvEffect
      const parsed: ParsedEnv = {
        ...baseRequiredEnv,
        subscribe: ' channel:home , channel:llm-feed ',
      }
      yield* subscribeFromEnv(fake.inbox, narrowSet, parsed)
      expect(fake.calls.subscribed).toEqual([
        decodeChannelNameSync('home'),
        decodeChannelNameSync('llm-feed'),
      ])
      expect(narrowSet.size()).toBe(2)
    }),
  ))

test('subscribeFromEnv aborts on a malformed token (and stops before later tokens)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fake = buildFakeInbox()
      const narrowSet = createNarrowSet()
      const baseRequiredEnv = yield* baseRequiredEnvEffect
      const parsed: ParsedEnv = {
        ...baseRequiredEnv,
        subscribe: 'channel:home,bogus:thing,channel:llm-feed',
      }
      const err = yield* Effect.flip(subscribeFromEnv(fake.inbox, narrowSet, parsed))
      expect(err).toBeInstanceOf(SubscribeTokenError)
      expect(fake.calls.subscribed).toEqual([decodeChannelNameSync('home')])
      expect(narrowSet.size()).toBe(1)
    }),
  ))

test('subscribeFromEnv propagates inbox.subscribe rejections and stops calling', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fake = buildFakeInbox({ rejectOn: 2 })
      const narrowSet = createNarrowSet()
      const baseRequiredEnv = yield* baseRequiredEnvEffect
      const parsed: ParsedEnv = {
        ...baseRequiredEnv,
        subscribe: 'channel:home,channel:llm-feed,channel:third',
      }
      const err = yield* Effect.flip(subscribeFromEnv(fake.inbox, narrowSet, parsed))
      expect(err.message).toContain('fake-inbox rejected call #2')
      expect(fake.calls.subscribed).toEqual([decodeChannelNameSync('home')])
      // Narrow set advanced both intents before the rejection — the substrate
      // call is what failed, not the local set add. Partial state is acceptable
      // since the boot orchestrator decides whether to release-and-exit.
      expect(narrowSet.size()).toBe(2)
    }),
  ))

test('subscribeFromEnv ignores empty list yielded by a trailing comma', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fake = buildFakeInbox()
      const narrowSet = createNarrowSet()
      const baseRequiredEnv = yield* baseRequiredEnvEffect
      const parsed: ParsedEnv = {
        ...baseRequiredEnv,
        subscribe: 'channel:home,',
      }
      const err = yield* Effect.flip(subscribeFromEnv(fake.inbox, narrowSet, parsed))
      expect(err).toBeInstanceOf(SubscribeTokenError)
      expect(fake.calls.subscribed).toEqual([decodeChannelNameSync('home')])
      expect(narrowSet.size()).toBe(1)
    }),
  ))

test('subscribeFromEnv returns parsed intents in token order (for downstream catch-up)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fake = buildFakeInbox()
      const narrowSet = createNarrowSet()
      const baseRequiredEnv = yield* baseRequiredEnvEffect
      const parsed: ParsedEnv = {
        ...baseRequiredEnv,
        subscribe: 'channel:home,thread:home/payments,mentions',
      }
      const intents = yield* subscribeFromEnv(fake.inbox, narrowSet, parsed)
      expect(intents).toEqual([
        { kind: 'channel', channelName: decodeChannelNameSync('home') },
        {
          kind: 'thread',
          channelName: decodeChannelNameSync('home'),
          threadName: decodeThreadNameSync('payments'),
        },
        { kind: 'mentions' },
      ])
    }),
  ))

test('subscribeFromEnv returns an empty array when COMMY_SUBSCRIBE is unset', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fake = buildFakeInbox()
      const narrowSet = createNarrowSet()
      const baseRequiredEnv = yield* baseRequiredEnvEffect
      const intents = yield* subscribeFromEnv(fake.inbox, narrowSet, baseRequiredEnv)
      expect(intents).toEqual([])
    }),
  ))

// Deliberate node-runtime pass: the default `readGitContext`
// now shells out via the `@effect/platform` command executor instead of
// `Bun.spawnSync`. CI runs the suite under bun, so exercise the real probe
// against a temp git repo while providing the node `CommandExecutor`
// (NodeContext.layer) — the production runtime path — rather than the
// injected fakes the deriveProject tests use.
describe('readGitContext on the node command executor', () => {
  const runProbe = (cwd: string) =>
    Effect.runPromise(readGitContext(cwd).pipe(Effect.provide(NodeContext.layer)))

  const withTempDir = async (run: (dir: string) => Promise<void>): Promise<void> => {
    const dir = mkdtempSync(join(tmpdir(), 'commy-gitctx-'))
    try {
      await run(dir)
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  }

  test('reads gitRoot and the origin remote basename for a real repo', () =>
    withTempDir(async (dir) => {
      execFileSync('git', ['init', '-q'], { cwd: dir })
      execFileSync('git', ['remote', 'add', 'origin', 'https://example.com/acme/widgets.git'], {
        cwd: dir,
      })
      const context = await runProbe(dir)
      if (context._tag !== 'InRepo') throw new Error(`expected InRepo, got ${context._tag}`)
      expect(context.gitRoot.length).toBeGreaterThan(0)
      expect(Option.getOrUndefined(context.remoteBasename)).toBe('widgets')
    }))

  test('reports InRepo with no remote basename when origin is absent', () =>
    withTempDir(async (dir) => {
      execFileSync('git', ['init', '-q'], { cwd: dir })
      const context = await runProbe(dir)
      if (context._tag !== 'InRepo') throw new Error(`expected InRepo, got ${context._tag}`)
      expect(Option.isNone(context.remoteBasename)).toBe(true)
    }))

  test('reports NotInRepo for a directory outside any git repo', () =>
    withTempDir(async (dir) => {
      expect(await runProbe(dir)).toEqual(NotInRepo())
    }))
})

describe('readBootSessionId (boot-env session-id feed source)', () => {
  const SID = '33333333-3333-4333-8333-333333333333'
  const read = (env: Record<string, string | undefined>): Effect.Effect<Option.Option<SessionId>> =>
    readBootSessionId.pipe(Effect.provide(testConfigProviderLayer(env)))

  test('CLAUDE_CODE_SESSION_ID present and UUID-shaped → Some(minted sid)', () =>
    Effect.gen(function* () {
      const result = yield* read({ CLAUDE_CODE_SESSION_ID: SID })
      expect(Option.isSome(result)).toBe(true)
      expect(Option.getOrThrow(result)).toBe(Option.getOrThrow(parseSessionId(SID)))
    }).pipe(Effect.runPromise))

  test('CLAUDE_CODE_SESSION_ID absent → None (boot feeder is a harmless no-op)', () =>
    Effect.gen(function* () {
      expect(Option.isNone(yield* read({}))).toBe(true)
    }).pipe(Effect.runPromise))

  test('CLAUDE_CODE_SESSION_ID present but not UUID-shaped → None', () =>
    Effect.gen(function* () {
      expect(Option.isNone(yield* read({ CLAUDE_CODE_SESSION_ID: 'not-a-uuid' }))).toBe(true)
    }).pipe(Effect.runPromise))
})
