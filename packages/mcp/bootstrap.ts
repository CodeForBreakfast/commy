import type { BotName, InboxError, MessageInbox } from '@commy/core/ports'
import { decodeBotNameSync } from '@commy/core/ports'
import type { ZulipAdapter } from '@commy/zulip/adapter'
import { zulipAdapter } from '@commy/zulip/adapter'
import type {
  ApiKey as ApiKeyType,
  BotEmail as BotEmailType,
  RealmUrl as RealmUrlType,
} from '@commy/zulip/http'
import { apiKeySchema, botEmailSchema, realmUrlSchema } from '@commy/zulip/http'
import type { HttpClient } from '@effect/platform'
import {
  Config,
  ConfigError,
  Context,
  Data,
  Effect,
  Either,
  Layer,
  Option,
  ParseResult,
  type Redacted,
  type Schema,
} from 'effect'
import type { NarrowSet } from './narrow-set.ts'
import type { SubscribeIntent, SubscribeTokenError } from './subscribe-parser.ts'
import { intentToTarget, parseSubscribeTarget } from './subscribe-parser.ts'

export class EnvConfigError extends Data.TaggedError('EnvConfigError')<{
  readonly message: string
  readonly issues: ReadonlyArray<string>
}> {}

const envConfigError = (issues: ReadonlyArray<string>): EnvConfigError =>
  new EnvConfigError({
    issues,
    message: `commy plugin env config invalid:\n  - ${issues.join('\n  - ')}`,
  })

declare const ProjectSlugBrand: unique symbol
/**
 * Sanitised project slug (comms-tud). Brand carries the invariant "we ran
 * `sanitiseProjectSlug`" — lowercase ASCII letters, digits, and `-`; starts
 * with a letter; capped at 12 chars. The single mint point is
 * `sanitiseProjectSlug`. Without the brand, an unsanitised string from
 * `COMMY_PROJECT` could flow through `composeBotName` and mint a
 * name that exceeds the 24-char budget or contains invalid characters.
 * See comms-uqf for the worked example of parse-don't-validate via brands.
 */
export type ProjectSlug = string & { readonly [ProjectSlugBrand]: never }

declare const SessionIdBrand: unique symbol
/**
 * Per-conversation session identifier (comms-uqf). Brand fences off the
 * `string` channel: only `parseSessionId` can mint one, and only from a
 * UUID-shaped raw value. Without the brand, an unvalidated string from
 * `args['session_id']` could flow all the way into `composeBotName` and
 * mint a malformed `cc-<project>-<garbage>` identity — which is exactly
 * what happened when a missing PreToolUse hook left a non-CC client's
 * `session_id` arg in place (see comms-uqf root-cause notes).
 */
export type SessionId = string & { readonly [SessionIdBrand]: never }

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

/**
 * Validating constructor for `SessionId`. Returns `Option.none()` for any
 * input that isn't a UUID-shaped string. The single mint point — every
 * call site that needs a `SessionId` must come through here.
 *
 * UUID-shape is the tightening: Claude Code's `CLAUDE_CODE_SESSION_ID`
 * is a UUID, so the hook-injected path always passes; non-CC MCP clients
 * must supply a UUID (e.g. via `crypto.randomUUID()`), which is a small
 * ask in exchange for making malformed identities unrepresentable
 * downstream.
 */
export const parseSessionId = (raw: unknown): Option.Option<SessionId> => {
  if (typeof raw !== 'string') return Option.none()
  if (!UUID_RE.test(raw)) return Option.none()
  return Option.some(raw as SessionId)
}

export type { BotName } from '@commy/core/ports'

const BOT_NAME_RE = /^[a-z][a-z0-9_-]*$/

/**
 * Validating constructor for `BotName` from an env-var value
 * (`COMMY_BOT_NAME`). Returns `Option.none()` for any input that
 * isn't substrate-safe: lowercase ASCII letters, digits, dashes, and
 * underscores; starts with a letter; capped at 40 chars. The single
 * env-path mint point — `composeBotName` is the ephemeral-path mint
 * point.
 */
export const parseBotName = (raw: string): Option.Option<BotName> => {
  if (raw.length === 0 || raw.length > 40) return Option.none()
  if (!BOT_NAME_RE.test(raw)) return Option.none()
  return Option.some(decodeBotNameSync(raw))
}

export interface ParsedEnv {
  readonly realmUrl: RealmUrlType
  readonly minterEmail: BotEmailType
  readonly minterApiKey: Redacted.Redacted<ApiKeyType>
  readonly botName?: BotName
  readonly subscribe?: string
  readonly sessionId?: SessionId
  readonly project?: ProjectSlug
  /**
   * Window (in seconds) for the boot-time channel/thread catch-up
   * applied on a persistent-bot restart (comms-3wl). When unset the
   * server applies a default; set to 0 to disable the catch-up.
   */
  readonly catchupWindowSeconds?: number
}

const placeholderShape = /^\$\{[^}]+\}$/
const userConfigPlaceholderShape = /^\$\{user_config\.[^}]+\}$/

const placeholderMessage = (key: string, raw?: string): string =>
  `${key} value is an unsubstituted placeholder${raw === undefined ? '' : ` (${raw})`} — ` +
  `the host did not expand it. ` +
  `Claude Code only substitutes \${user_config.X} (and \${CLAUDE_PLUGIN_ROOT} in args) ` +
  `in .mcp.json; host-env vars like \${CLAUDE_CODE_SESSION_ID} must be inherited from ` +
  `the parent environment instead. See ass-ukz6 for context.`

/**
 * A host-env placeholder the host failed to expand is a genuine misconfig:
 * reject it as `InvalidData` carrying the diagnostic wording so it surfaces in
 * the `EnvConfigError`. Applied to every key whose value the host is meant to
 * have substituted.
 */
const rejectPlaceholder = (key: string): ((self: Config.Config<string>) => Config.Config<string>) =>
  Config.validate({
    message: placeholderMessage(key),
    validation: (value: string) => !placeholderShape.test(value),
  })

const renderConfigError = (error: ConfigError.ConfigError): ReadonlyArray<string> => {
  const leaf = (path: ReadonlyArray<string>, message: string): string =>
    path.length === 0 ? message : `${path.join('.')}: ${message}`
  const walk = (node: ConfigError.ConfigError): ReadonlyArray<string> => {
    switch (node._op) {
      case 'And':
      case 'Or':
        return [...walk(node.left), ...walk(node.right)]
      default:
        return [leaf(node.path, node.message)]
    }
  }
  return walk(error)
}

/**
 * Required brand field: read the raw string under `key`, reject an
 * unsubstituted host-env placeholder, then decode through the FOUNDATION brand
 * schema. A missing key surfaces as `MissingData`; a malformed value as
 * `InvalidData` carrying the schema's formatted message under `key`.
 */
const requiredBrand = <Brand extends string, I extends string>(
  key: string,
  schema: Schema.Schema<I, string>,
  brand: (value: I) => Brand,
): Config.Config<Brand> => {
  const decode = ParseResult.decodeUnknownEither(schema)
  return Config.string(key).pipe(
    rejectPlaceholder(key),
    Config.mapOrFail((value) =>
      decode(value).pipe(
        Either.mapBoth({
          onLeft: (issue) =>
            ConfigError.InvalidData([key], ParseResult.TreeFormatter.formatIssueSync(issue)),
          onRight: brand,
        }),
      ),
    ),
  )
}

/**
 * Strict non-empty, non-placeholder string under `key`. A missing key is
 * `MissingData`; an empty or placeholder value is `InvalidData`.
 */
const nonEmptyString = (key: string): Config.Config<string> =>
  Config.string(key).pipe(
    Config.validate({
      message: `${key} must not be empty when set`,
      validation: (value: string) => value.length > 0,
    }),
    rejectPlaceholder(key),
  )

/**
 * Optional value that, when present, must be non-empty and not a placeholder.
 * A missing key is `None`; an empty or placeholder value is a real failure.
 * Used for `COMMY_BOT_NAME` and `COMMY_PROJECT`, where the operator supplied
 * something deliberate and a blank or unexpanded value is a misconfig.
 */
const optionalNonEmpty = (key: string): Config.Config<Option.Option<string>> =>
  Config.option(nonEmptyString(key))

/**
 * Optional value wired through Claude Code `userConfig`. When the operator
 * hasn't set the field, the substituted value can arrive as either an empty
 * string or the literal `${user_config.KEY}` placeholder — the plugins
 * reference doesn't pin behaviour down for unset optional fields, so accept
 * both as "unset" (`None`) rather than failing loudly. Genuine misconfigs —
 * host-env placeholders like `${CLAUDE_CODE_SESSION_ID}` that the host doesn't
 * substitute — still reject.
 */
const userConfigString = (key: string): Config.Config<string> =>
  Config.string(key).pipe(
    Config.mapOrFail((value) => {
      if (value.length === 0 || userConfigPlaceholderShape.test(value)) {
        return Either.left(ConfigError.MissingData([key], `${key} is unset`))
      }
      if (placeholderShape.test(value)) {
        return Either.left(ConfigError.InvalidData([key], placeholderMessage(key, value)))
      }
      return Either.right(value)
    }),
  )

const optionalUserConfig = (key: string): Config.Config<Option.Option<string>> =>
  Config.option(userConfigString(key))

const optionalNonNegativeInt = (key: string): Config.Config<Option.Option<number>> =>
  optionalUserConfig(key).pipe(
    Config.mapOrFail((option) =>
      Option.match(option, {
        onNone: () => Either.right(Option.none<number>()),
        onSome: (raw) => {
          const parsed = Number(raw)
          return Number.isInteger(parsed) && parsed >= 0
            ? Either.right(Option.some(parsed))
            : Either.left(
                ConfigError.InvalidData(
                  [key],
                  `${key} must be a non-negative integer — received: ${raw}`,
                ),
              )
        },
      }),
    ),
  )

/**
 * `CLAUDE_CODE_SESSION_ID` is a host-env var (not `userConfig`): empty or
 * unset is `None`, an unsubstituted placeholder is a misconfig, and a present
 * value must be UUID-shaped (the `SessionId` brand invariant, comms-uqf).
 */
const optionalSessionId = (key: string): Config.Config<Option.Option<SessionId>> =>
  Config.option(
    Config.string(key).pipe(
      Config.mapOrFail((value) => {
        if (value.length === 0) {
          return Either.left(ConfigError.MissingData([key], `${key} is unset`))
        }
        if (placeholderShape.test(value)) {
          return Either.left(ConfigError.InvalidData([key], placeholderMessage(key, value)))
        }
        return Option.match(parseSessionId(value), {
          onNone: () =>
            Either.left(
              ConfigError.InvalidData([key], `${key} must be a UUID — received: ${value}`),
            ),
          onSome: (sessionId) => Either.right(sessionId),
        })
      }),
    ),
  )

/**
 * `COMMY_BOT_NAME` must be substrate-safe (lowercase ASCII, digits, dashes,
 * underscores; starts with a letter; max 40 chars) — the `BotName` brand
 * invariant (comms-0zo).
 */
const optionalBotName = (key: string): Config.Config<Option.Option<BotName>> =>
  optionalNonEmpty(key).pipe(
    Config.mapOrFail((option) =>
      Option.match(option, {
        onNone: () => Either.right(Option.none<BotName>()),
        onSome: (raw) =>
          Option.match(parseBotName(raw), {
            onNone: () =>
              Either.left(
                ConfigError.InvalidData(
                  [key],
                  `${key} must be substrate-safe (lowercase ASCII, digits, dashes, underscores; starts with letter; max 40 chars) — received: ${raw}`,
                ),
              ),
            onSome: (botName) => Either.right(Option.some(botName)),
          }),
      }),
    ),
  )

const envConfig: Config.Config<ParsedEnv> = Config.all({
  realmUrl: requiredBrand('ZULIP_SITE', realmUrlSchema, (v): RealmUrlType => v as RealmUrlType),
  minterEmail: requiredBrand(
    'ZULIP_MINTER_EMAIL',
    botEmailSchema,
    (v): BotEmailType => v as BotEmailType,
  ),
  minterApiKey: Config.redacted(
    requiredBrand('ZULIP_MINTER_API_KEY', apiKeySchema, (v): ApiKeyType => v as ApiKeyType),
  ),
  botName: optionalBotName('COMMY_BOT_NAME'),
  subscribe: optionalUserConfig('COMMY_SUBSCRIBE'),
  project: optionalNonEmpty('COMMY_PROJECT'),
  catchupWindowSeconds: optionalNonNegativeInt('COMMY_CATCHUP_WINDOW_SECONDS'),
  sessionId: optionalSessionId('CLAUDE_CODE_SESSION_ID'),
}).pipe(
  Config.map((raw) => {
    const project = Option.flatMap(raw.project, sanitiseProjectSlug)
    return {
      realmUrl: raw.realmUrl,
      minterEmail: raw.minterEmail,
      minterApiKey: raw.minterApiKey,
      ...Option.match(raw.botName, { onNone: () => ({}), onSome: (botName) => ({ botName }) }),
      ...Option.match(raw.subscribe, {
        onNone: () => ({}),
        onSome: (subscribe) => ({ subscribe }),
      }),
      ...Option.match(raw.sessionId, {
        onNone: () => ({}),
        onSome: (sessionId) => ({ sessionId }),
      }),
      ...Option.match(project, { onNone: () => ({}), onSome: (slug) => ({ project: slug }) }),
      ...Option.match(raw.catchupWindowSeconds, {
        onNone: () => ({}),
        onSome: (catchupWindowSeconds) => ({ catchupWindowSeconds }),
      }),
    }
  }),
)

/**
 * Parse the plugin's env into `ParsedEnv`, reading the ambient
 * ConfigProvider set at the boot edge — `PlatformLive` in production
 * (`ConfigProvider.fromEnv()` over `process.env`), a fixture
 * `ConfigProvider.fromMap` layer in tests. A missing required var
 * surfaces as `MissingData`, a malformed one as `InvalidData`; both
 * render into the `EnvConfigError`.
 */
export const parseEnv: Effect.Effect<ParsedEnv, EnvConfigError> = envConfig.pipe(
  Effect.mapError((error) => envConfigError(renderConfigError(error))),
)

/**
 * Per ass-j3i8 / ass-js5u: project slugs are lowercase ASCII letters,
 * digits, and `-`; start with a letter; capped at 12 chars (so that
 * `cc-<project>-<8>` fits the 24-char overall budget). Returns the
 * sanitised slug, or `Option.none()` if the input collapses to something
 * unusable (empty, leading-digit, all-punctuation).
 */
export const sanitiseProjectSlug = (raw: string): Option.Option<ProjectSlug> => {
  const lowered = raw.toLowerCase()
  const dashed = lowered.replace(/[/_]/g, '-')
  const filtered = dashed.replace(/[^a-z0-9-]/g, '-')
  const collapsed = filtered.replace(/-+/g, '-')
  const trimmed = collapsed.replace(/^-+|-+$/g, '')
  if (trimmed.length === 0) return Option.none()
  const truncated = trimmed.slice(0, 12).replace(/-+$/, '')
  if (truncated.length === 0) return Option.none()
  if (!/^[a-z]/.test(truncated)) return Option.none()
  return Option.some(truncated as ProjectSlug)
}

/**
 * Compose the bot name for an ephemeral Claude Code session.
 *
 * - `cc-<project>-<first-8-of-sessionId>` when `project` is provided
 *   (assumed already sanitised — call `sanitiseProjectSlug` first).
 * - `cc-<first-8-of-sessionId>` when `project` is undefined.
 *
 * `sessionId` is the branded `SessionId` type (comms-uqf): only
 * `parseSessionId` can mint one, and only from a UUID-shaped raw
 * value. UUIDs are 36 chars with hex/dashes — the first-8 slice is
 * always 8 hex chars, so the resulting suffix can never carry an
 * unvalidated string smuggled in from `args['session_id']`.
 */
export const composeBotName = (args: {
  readonly sessionId: SessionId
  readonly project?: ProjectSlug
}): BotName => {
  const suffix = args.sessionId.slice(0, 8)
  if (args.project === undefined) return decodeBotNameSync(`cc-${suffix}`)
  return decodeBotNameSync(`cc-${args.project}-${suffix}`)
}

/**
 * Git context for a cwd, as needed by `deriveProject`. The default
 * implementation shells out to `git -C <cwd>`; tests inject a fake.
 */
/**
 * Git context for a cwd, modelled as a tagged union so the
 * gitRoot-present-iff-inRepo invariant is structural rather than
 * conventional: `InRepo` always carries a `gitRoot` (and an optional
 * remote basename); `NotInRepo` carries no git fields. The illegal
 * "not in repo but has a gitRoot" / "in repo without a gitRoot"
 * combinations are unrepresentable.
 */
export type GitContext = Data.TaggedEnum<{
  InRepo: { readonly gitRoot: string; readonly remoteBasename: Option.Option<string> }
  // The empty-payload variant of a Data.TaggedEnum is `{}` by construction —
  // the canonical shape from the data.mdx "Union of Tagged Structs" idiom.
  // biome-ignore lint/complexity/noBannedTypes: tagged-enum empty variant payload
  NotInRepo: {}
}>

export const { InRepo, NotInRepo, $match: matchGitContext } = Data.taggedEnum<GitContext>()

export interface DeriveProjectDeps {
  /** Value of `COMMY_PROJECT` from env, already sanitised. */
  readonly project?: ProjectSlug
  /** Process cwd at plugin boot. */
  readonly cwd: string
  /** Git-context probe. Injected in tests; defaults to a real `git` shell-out at runtime. */
  readonly readGitContext: (cwd: string) => GitContext
}

const basename = (path: string): string => {
  const trimmed = path.replace(/\/+$/, '')
  const idx = trimmed.lastIndexOf('/')
  return idx === -1 ? trimmed : trimmed.slice(idx + 1)
}

/**
 * Hybrid derivation of the project slug (ass-js5u). Precedence:
 *   1. `COMMY_PROJECT` env var (most reliable, opted-in per
 *      devshell / `.envrc`).
 *   2. Git remote origin basename (stable across worktree paths,
 *      misses non-repo projects).
 *   3. Git root basename (covers local-only repos; better than cwd
 *      basename, which would mis-identify `~/assistant/scripts/` as
 *      `scripts`).
 *   4. `undefined` — non-project cwds (`/tmp`, `$HOME`) fall through
 *      to bare `cc-<8>`.
 *
 * The env value, when set, is authoritative: if it sanitises to
 * nothing usable, we return `undefined` rather than falling back —
 * the operator's explicit choice wins over auto-derivation.
 */
export const deriveProject = (deps: DeriveProjectDeps): Option.Option<ProjectSlug> => {
  if (deps.project !== undefined) {
    return Option.some(deps.project)
  }
  return matchGitContext(deps.readGitContext(deps.cwd), {
    NotInRepo: () => Option.none(),
    InRepo: ({ gitRoot, remoteBasename }) =>
      Option.match(
        Option.filter(remoteBasename, (name) => name.length > 0),
        {
          onSome: sanitiseProjectSlug,
          onNone: () =>
            gitRoot.length > 0 ? sanitiseProjectSlug(basename(gitRoot)) : Option.none(),
        },
      ),
  })
}

/**
 * Default git-context probe — shells out to `git -C <cwd>`. Returns
 * `inRepo: false` if `rev-parse --show-toplevel` exits non-zero; the
 * remote basename is left `undefined` if `remote get-url origin` fails.
 */
export const readGitContext = (cwd: string): GitContext => {
  const toplevel = Bun.spawnSync({
    cmd: ['git', '-C', cwd, 'rev-parse', '--show-toplevel'],
    stderr: 'ignore',
  })
  if (toplevel.exitCode !== 0) return NotInRepo()
  const gitRoot = toplevel.stdout.toString().trim()
  const remote = Bun.spawnSync({
    cmd: ['git', '-C', cwd, 'remote', 'get-url', 'origin'],
    stderr: 'ignore',
  })
  if (remote.exitCode !== 0) {
    return InRepo({ gitRoot, remoteBasename: Option.none() })
  }
  const url = remote.stdout.toString().trim()
  const tail = url.split('/').pop()
  const remoteBasename = tail === undefined ? undefined : tail.replace(/\.git$/, '') || undefined
  return InRepo({ gitRoot, remoteBasename: Option.fromNullable(remoteBasename) })
}

/**
 * The full driven surface `main` composes against (comms-spj3.39): the
 * universal `AgentComms` aggregate plus the Zulip-shaped boot extras
 * (reconcile / download / upload / close). Lives in the plugin — core
 * stays substrate-neutral, and `registerTools` keeps its narrower
 * `AgentComms` dependency via structural subtyping. Request-time DI
 * (methods carrying `R = HttpClient`) is deferred to comms-7v3.
 */
export class SubstrateAdapter extends Context.Tag('SubstrateAdapter')<
  SubstrateAdapter,
  ZulipAdapter
>() {}

/**
 * Wrap an adapter-construction Effect as the `SubstrateAdapter` layer,
 * registering `close()` as a scope finalizer so substrate teardown
 * drops out of `main` and fires after the program's own finalizers
 * (pump-cancel, identity release) when the program scope unwinds.
 */
export const substrateAdapterLayer = <E, R>(
  make: Effect.Effect<ZulipAdapter, E, R>,
): Layer.Layer<SubstrateAdapter, E, R> =>
  Layer.scoped(
    SubstrateAdapter,
    Effect.tap(make, (adapter) => Effect.addFinalizer(() => Effect.promise(() => adapter.close()))),
  )

/**
 * Production substrate layer: construct the Zulip adapter from the
 * app-edge config and bind it to {@link SubstrateAdapter}, with
 * `close()` as a scope finalizer. V1 is hard-wired to Zulip; substrate
 * selection becomes pluggable once a second driven adapter exists.
 */
export const ZulipAdapterLive: Layer.Layer<
  SubstrateAdapter,
  EnvConfigError,
  HttpClient.HttpClient
> = substrateAdapterLayer(
  Effect.gen(function* () {
    const parsed = yield* parseEnv
    return yield* zulipAdapter({
      realmUrl: parsed.realmUrl,
      minterEmail: parsed.minterEmail,
      minterApiKey: parsed.minterApiKey,
    })
  }),
)

/**
 * Apply each token in `COMMY_SUBSCRIBE` to the plugin-layer
 * narrow set AND the substrate via `inbox.subscribe`. Empty / unset
 * env → no work. Malformed token or substrate rejection aborts
 * immediately — partial state made before the failure remains in
 * place.
 *
 * Two sinks on purpose:
 *   1. `narrowSet` is the consumer-side filter the event pump uses
 *      to tee only matching events to the MCP host (ass-220u).
 *   2. `inbox.subscribe` keeps the substrate side wired so the
 *      adapter actually receives events. For Zulip this calls
 *      `/users/me/subscriptions` against the minter, ensuring the
 *      stream is in the minter's queue. The boot-time reconciler
 *      (ass-6a77) covers most streams; this per-session call still
 *      handles streams created after the plugin booted.
 */
export const subscribeFromEnv = (
  inbox: MessageInbox,
  narrowSet: NarrowSet,
  parsed: ParsedEnv,
): Effect.Effect<ReadonlyArray<SubscribeIntent>, SubscribeTokenError | InboxError> => {
  if (parsed.subscribe === undefined) return Effect.succeed([])
  return Effect.forEach(parsed.subscribe.split(','), (raw) =>
    parseSubscribeTarget(raw.trim()).pipe(
      Effect.tap((intent) => Effect.sync(() => narrowSet.add(intent))),
      Effect.flatMap((intent) =>
        intentToTarget(intent).pipe(
          Effect.flatMap((target) => inbox.subscribe(target)),
          Effect.as(intent),
        ),
      ),
    ),
  )
}
