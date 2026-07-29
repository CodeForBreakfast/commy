import type { BotName, InboxError, MessageInbox } from '@commy/core/ports'
import { decodeBotNameSync } from '@commy/core/ports'
import type { ZulipAdapter } from '@commy/zulip/adapter'
import { zulipAdapter } from '@commy/zulip/adapter'
import { MAX_QUEUE_TIMEOUT_SECS } from '@commy/zulip/events'
import type {
  ApiKey as ApiKeyType,
  BotEmail as BotEmailType,
  RealmUrl as RealmUrlType,
} from '@commy/zulip/http'
import { apiKeySchema, botEmailSchema, realmUrlSchema } from '@commy/zulip/http'
import { Command, type CommandExecutor, type HttpClient } from '@effect/platform'
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
  Redacted,
  Schema,
  String as Str,
} from 'effect'
import type { NarrowSet } from './narrow-set.ts'
import { buildQueueStateHooks } from './queue-state-hooks.ts'
import { QueueStateStoreTag } from './queue-state-store.ts'
import { ResumeOutcome as ResumeOutcomeTag } from './resume-outcome.ts'
import { bindThrough, SessionBinder as SessionBinderTag } from './session-binder.ts'
import { SessionId as SessionIdTag } from './session-id.ts'
import type { SubscribeIntent, SubscribeTokenError } from './subscribe-parser.ts'
import { intentToTarget, parseSubscribeTarget } from './subscribe-parser.ts'

export class EnvConfigError extends Data.TaggedError('EnvConfigError')<{
  readonly message: string
  readonly issues: ReadonlyArray<string>
}> {}

export const envConfigError = (issues: ReadonlyArray<string>): EnvConfigError =>
  new EnvConfigError({
    issues,
    message: `commy plugin env config invalid:\n  - ${issues.join('\n  - ')}`,
  })

declare const ProjectSlugBrand: unique symbol
/**
 * Sanitised project slug. Brand carries the invariant "we ran
 * `sanitiseProjectSlug`" — lowercase ASCII letters, digits, and `-`; starts
 * with a letter; capped at 12 chars. The single mint point is
 * `sanitiseProjectSlug`. Without the brand, an unsanitised string from
 * `COMMY_PROJECT` could flow through `composeBotName` and mint a
 * name that exceeds the 24-char budget or contains invalid characters.
 */
export type ProjectSlug = string & { readonly [ProjectSlugBrand]: never }

/**
 * Per-conversation session identifier. Brand fences off the
 * `string` channel: only `parseSessionId` can mint one, and only from a
 * UUID-shaped raw value. Without the brand, an unvalidated string from
 * `args['session_id']` could flow all the way into `composeBotName` and
 * mint a malformed `cc-<project>-<garbage>` identity.
 *
 * UUID-shape is the tightening: Claude Code's `CLAUDE_CODE_SESSION_ID`
 * is a UUID, so the hook-injected path always passes; non-CC MCP clients
 * must supply a UUID (e.g. via `crypto.randomUUID()`), which is a small
 * ask in exchange for making malformed identities unrepresentable
 * downstream.
 */
const SessionIdSchema = Schema.UUID.pipe(Schema.brand('SessionId'))
export type SessionId = typeof SessionIdSchema.Type

/**
 * Validating constructor for `SessionId`. Returns `Option.none()` for any
 * input that isn't a UUID-shaped string. The single mint point — every
 * call site that needs a `SessionId` must come through here.
 */
export const parseSessionId: (raw: unknown) => Option.Option<SessionId> =
  Schema.decodeUnknownOption(SessionIdSchema)

/**
 * Boot-time session-id feed source. Claude Code injects
 * `CLAUDE_CODE_SESSION_ID` into the MCP child's env at spawn — verified on CC
 * 2.1.201: it is inherited into the child `process.env`, NOT substituted into
 * `.mcp.json` via `${…}` (the host does not do that, cc#2065). Read it once
 * at boot and mint a `SessionId`, so the shared session-id `Deferred` can be
 * filled with zero agent action — the case a resumed listen-only seat needs,
 * where restore must fire before the agent does anything. A missing var (a
 * non-CC host, or a CC that stops injecting it) or a non-UUID value yields
 * `Option.none()`: the boot feeder becomes a harmless no-op and the
 * per-tool-call feeders still cover any acting seat. Reads the ambient
 * ConfigProvider, so it never fails — an absent key is `None`, not an error.
 */
export const readBootSessionId: Effect.Effect<Option.Option<SessionId>> = Effect.orElseSucceed(
  Effect.map(
    Config.option(Config.string('CLAUDE_CODE_SESSION_ID')),
    Option.flatMap(parseSessionId),
  ),
  () => Option.none<SessionId>(),
)

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
  /**
   * Attach mode. Present when both `COMMY_BOT_NAME` and
   * `COMMY_BOT_API_KEY` are set: the server binds the named persona using the
   * supplied stable key (no regenerate), letting many sessions share one
   * identity. The key is `Redacted` so it never logs.
   */
  readonly attachIdentity?: {
    readonly name: BotName
    readonly apiKey: Redacted.Redacted<ApiKeyType>
  }
  readonly subscribe?: string
  readonly project?: ProjectSlug
  /**
   * Window (in seconds) for the boot-time channel/thread catch-up
   * applied on a persistent-bot restart. When unset the
   * server applies a default; set to 0 to disable the catch-up.
   */
  readonly catchupWindowSeconds?: number
  /**
   * Idle-timeout (in seconds) sent as `idle_queue_timeout` on an ephemeral
   * session's `/register`, governing how long Zulip keeps the events queue
   * alive between polls. Always present — `COMMY_QUEUE_IDLE_TIMEOUT_SECS`
   * defaults to 24h and is clamped to Zulip's 7-day maximum.
   */
  readonly queueIdleTimeoutSecs: number
  /**
   * Operator-set base directory (`COMMY_DOWNLOAD_DIR`) that `download_file`
   * roots its per-download temp directory in, so the fetched attachment lands
   * somewhere the caller can Read (its own scratchpad) rather than `$TMPDIR`.
   * Unset → `$TMPDIR` fallback. Existence is validated once at boot.
   */
  readonly downloadDir?: string
}

const placeholderShape = /^\$\{[^}]+\}$/
const userConfigPlaceholderShape = /^\$\{user_config\.[^}]+\}$/

const placeholderMessage = (key: string, raw?: string): string =>
  `${key} value is an unsubstituted placeholder${raw === undefined ? '' : ` (${raw})`} — ` +
  `the host did not expand it. ` +
  `Claude Code only substitutes \${user_config.X} (and \${CLAUDE_PLUGIN_ROOT} in args) ` +
  `in .mcp.json; host-env vars like \${CLAUDE_CODE_SESSION_ID} must be inherited from ` +
  `the parent environment instead.`

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
 * unsubstituted host-env placeholder, then decode through the foundation brand
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
 * both as "unset" (`None`). Both readings are safe because the plugin manifest
 * writes these to the {@link USER_CONFIG_SUFFIX} key, which nothing but the
 * manifest ever sets: "empty here" can only mean the host substituted nothing.
 * Genuine misconfigs — host-env placeholders like `${CLAUDE_CODE_SESSION_ID}`
 * that the host doesn't substitute — still reject.
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

/**
 * Where `clients/claude-code/.mcp.json` writes a `${user_config.KEY}`
 * substitution: `KEY_USER_CONFIG`, never the bare `KEY`.
 *
 * `.mcp.json` is static JSON with no way to omit a key conditionally, so every
 * declared `user_config` field is substituted and written into the child env
 * whether or not the operator supplied it. Written to the bare name, an
 * unsupplied optional field therefore lands as an empty string that OVERRIDES
 * whatever the operator set by other means — a systemd unit, a pane env, a nix
 * module — rather than deferring to it. Giving the manifest its own key space
 * makes that impossible by construction: the plugin can only ever clobber a
 * name it alone owns.
 */
const USER_CONFIG_SUFFIX = '_USER_CONFIG'

/**
 * Optional operator-supplied value, read from the two paths that can carry one:
 * the plugin manifest's `KEY_USER_CONFIG` first, then an inherited bare `KEY`.
 *
 * The precedence direction is safe because of how the two artefacts ship, not
 * because of anything visible in this file. The MCP server goes out via
 * `npx @codeforbreakfast/commy-mcp` with no version spec, so it floats to npm
 * latest at every seat start; the plugin manifest ships via a pinned
 * marketplace ref that moves only when an operator runs `/plugin`. The server
 * therefore always moves first and the manifest lags arbitrarily. New server +
 * old manifest is the skew that will happen: the manifest still writes the bare
 * name, this read finds nothing under the suffixed one and falls through to the
 * inherited value — which is the clobbered-empty string, i.e. no worse than
 * before the fix. The inverse skew, which would ignore a working operator's
 * plugin config, cannot occur because the manifest cannot outrun the server.
 * Reverse the precedence and that reasoning no longer holds.
 */
const optionalUserConfig = (key: string): Config.Config<Option.Option<string>> =>
  Config.option(
    userConfigString(`${key}${USER_CONFIG_SUFFIX}`).pipe(
      Config.orElseIf({
        // Only "unset" falls through. A genuine misconfig under the suffixed
        // key — an unsubstituted host-env placeholder — still fails loudly
        // rather than being masked by whatever the bare name happens to hold.
        if: ConfigError.isMissingDataOnly,
        orElse: () => userConfigString(key),
      }),
    ),
  )

const QUEUE_IDLE_TIMEOUT_DEFAULT_SECS = 86400

/**
 * `COMMY_QUEUE_IDLE_TIMEOUT_SECS` — how many seconds an ephemeral session's
 * events queue survives without a poll before Zulip garbage-collects it, sent
 * as `idle_queue_timeout` on `/register`. Unset (or empty / placeholder) falls
 * back to 24h; a set value is clamped to Zulip's 7-day
 * {@link MAX_QUEUE_TIMEOUT_SECS} ceiling. A non-positive or non-integer value
 * is rejected rather than silently coerced.
 */
const queueIdleTimeoutSecs = (key: string): Config.Config<number> =>
  optionalUserConfig(key).pipe(
    Config.mapOrFail((option) =>
      Option.match(option, {
        onNone: () => Either.right(QUEUE_IDLE_TIMEOUT_DEFAULT_SECS),
        onSome: (raw) => {
          const parsed = Number(raw)
          return Number.isInteger(parsed) && parsed > 0
            ? Either.right(Math.min(parsed, MAX_QUEUE_TIMEOUT_SECS))
            : Either.left(
                ConfigError.InvalidData(
                  [key],
                  `${key} must be a positive integer — received: ${raw}`,
                ),
              )
        },
      }),
    ),
  )

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
 * `COMMY_BOT_NAME` must be substrate-safe (lowercase ASCII, digits, dashes,
 * underscores; starts with a letter; max 40 chars) — the `BotName` brand
 * invariant.
 */
const optionalBotName = (key: string): Config.Config<Option.Option<BotName>> =>
  optionalNonEmpty(key).pipe(
    Config.mapOrFail((option) =>
      Option.match(option, {
        onNone: () => Either.right(Option.none<BotName>()),
        onSome: (raw) =>
          Either.fromOption(parseBotName(raw), () =>
            ConfigError.InvalidData(
              [key],
              `${key} must be substrate-safe (lowercase ASCII, digits, dashes, underscores; starts with letter; max 40 chars) — received: ${raw}`,
            ),
          ).pipe(Either.map(Option.some)),
      }),
    ),
  )

/**
 * Optional, `Redacted`-wrapped api key under `key` (attach mode).
 * Missing ⇒ `None`; empty / placeholder / malformed ⇒ `InvalidData`, mirroring
 * `optionalBotName`. The supplied secret is masked the same way the minter key
 * is, so it never lands in a log line.
 */
const optionalApiKey = (
  key: string,
): Config.Config<Option.Option<Redacted.Redacted<ApiKeyType>>> => {
  const decode = ParseResult.decodeUnknownEither(apiKeySchema)
  return optionalNonEmpty(key).pipe(
    Config.mapOrFail((option) =>
      Option.match(option, {
        onNone: () => Either.right(Option.none<Redacted.Redacted<ApiKeyType>>()),
        onSome: (raw) =>
          decode(raw).pipe(
            Either.mapBoth({
              onLeft: (issue) =>
                ConfigError.InvalidData([key], ParseResult.TreeFormatter.formatIssueSync(issue)),
              onRight: (value) => Option.some(Redacted.make(value as ApiKeyType)),
            }),
          ),
      }),
    ),
  )
}

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
  botApiKey: optionalApiKey('COMMY_BOT_API_KEY'),
  subscribe: optionalUserConfig('COMMY_SUBSCRIBE'),
  project: optionalNonEmpty('COMMY_PROJECT'),
  catchupWindowSeconds: optionalNonNegativeInt('COMMY_CATCHUP_WINDOW_SECONDS'),
  queueIdleTimeoutSecs: queueIdleTimeoutSecs('COMMY_QUEUE_IDLE_TIMEOUT_SECS'),
  downloadDir: optionalNonEmpty('COMMY_DOWNLOAD_DIR'),
}).pipe(
  Config.mapOrFail((raw) => {
    // Attach mode needs both halves: a key with no name has no identity to bind
    // to, so reject it rather than silently ignore the supplied secret.
    if (Option.isSome(raw.botApiKey) && Option.isNone(raw.botName)) {
      return Either.left(
        ConfigError.InvalidData(
          ['COMMY_BOT_API_KEY'],
          'COMMY_BOT_API_KEY requires COMMY_BOT_NAME — the key identifies which provisioned persona to attach to',
        ),
      )
    }
    const project = Option.flatMap(raw.project, sanitiseProjectSlug)
    return Either.right({
      realmUrl: raw.realmUrl,
      minterEmail: raw.minterEmail,
      minterApiKey: raw.minterApiKey,
      queueIdleTimeoutSecs: raw.queueIdleTimeoutSecs,
      ...Option.match(raw.botName, { onNone: () => ({}), onSome: (botName) => ({ botName }) }),
      ...(Option.isSome(raw.botName) && Option.isSome(raw.botApiKey)
        ? { attachIdentity: { name: raw.botName.value, apiKey: raw.botApiKey.value } }
        : {}),
      ...Option.match(raw.subscribe, {
        onNone: () => ({}),
        onSome: (subscribe) => ({ subscribe }),
      }),
      ...Option.match(project, { onNone: () => ({}), onSome: (slug) => ({ project: slug }) }),
      ...Option.match(raw.catchupWindowSeconds, {
        onNone: () => ({}),
        onSome: (catchupWindowSeconds) => ({ catchupWindowSeconds }),
      }),
      ...Option.match(raw.downloadDir, {
        onNone: () => ({}),
        onSome: (downloadDir) => ({ downloadDir }),
      }),
    })
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
 * Project slugs are lowercase ASCII letters,
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
  if (Str.isEmpty(trimmed)) return Option.none()
  const truncated = trimmed.slice(0, 12).replace(/-+$/, '')
  if (Str.isEmpty(truncated)) return Option.none()
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
 * `sessionId` is the branded `SessionId` type: only
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
  /**
   * Git-context probe. Injected in tests; defaults to a real `git` shell-out
   * at runtime. The default ({@link readGitContext}) requires a
   * `CommandExecutor`; `server.ts` provides it once from context, so the
   * probe seen here is already executor-satisfied (`R = never`).
   */
  readonly readGitContext: (cwd: string) => Effect.Effect<GitContext>
}

const basename = (path: string): string => {
  const trimmed = path.replace(/\/+$/, '')
  const idx = trimmed.lastIndexOf('/')
  return idx === -1 ? trimmed : trimmed.slice(idx + 1)
}

/**
 * Hybrid derivation of the project slug. Precedence:
 *   1. `COMMY_PROJECT` env var (most reliable, opted-in per
 *      devshell / `.envrc`).
 *   2. Git remote origin basename (stable across worktree paths,
 *      misses non-repo projects).
 *   3. Git root basename (covers local-only repos; better than cwd
 *      basename, which would mis-identify `~/myproject/scripts/` as
 *      `scripts`).
 *   4. `undefined` — non-project cwds (`/tmp`, `$HOME`) fall through
 *      to bare `cc-<8>`.
 *
 * The env value, when set, is authoritative: if it sanitises to
 * nothing usable, we return `undefined` rather than falling back —
 * the operator's explicit choice wins over auto-derivation.
 */
export const deriveProject = (
  deps: DeriveProjectDeps,
): Effect.Effect<Option.Option<ProjectSlug>> => {
  if (deps.project !== undefined) {
    return Effect.succeed(Option.some(deps.project))
  }
  return Effect.map(deps.readGitContext(deps.cwd), (context) =>
    matchGitContext(context, {
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
    }),
  )
}

/**
 * Trimmed stdout of `git -C <cwd> <args…>`, on the Effect-native command
 * executor. A non-zero git exit (e.g. not a repo, no origin remote) yields
 * empty stdout — git's diagnostics go to its piped, undrained stderr — and a
 * spawn failure (no `git` on PATH) is caught to the same empty string. So the
 * caller reads "no output" as the single failure signal.
 */
const gitStdout = (
  cwd: string,
  args: ReadonlyArray<string>,
): Effect.Effect<string, never, CommandExecutor.CommandExecutor> =>
  Command.make('git', '-C', cwd, ...args).pipe(
    Command.stderr('pipe'),
    Command.string,
    Effect.map((out) => out.trim()),
    Effect.catchAll(() => Effect.succeed('')),
  )

/**
 * Default git-context probe — shells out to `git -C <cwd>` via the
 * `@effect/platform` command executor (`server.ts` provides the node
 * executor). Returns `NotInRepo` when `rev-parse --show-toplevel` produces no
 * toplevel; the remote basename is left `undefined` when `remote get-url
 * origin` produces no url.
 */
export const readGitContext = (
  cwd: string,
): Effect.Effect<GitContext, never, CommandExecutor.CommandExecutor> =>
  Effect.gen(function* () {
    const gitRoot = yield* gitStdout(cwd, ['rev-parse', '--show-toplevel'])
    if (gitRoot.length === 0) return NotInRepo()
    const url = yield* gitStdout(cwd, ['remote', 'get-url', 'origin'])
    const tail = url.split('/').pop()
    const remoteBasename = tail === undefined ? undefined : tail.replace(/\.git$/, '') || undefined
    return InRepo({ gitRoot, remoteBasename: Option.fromNullable(remoteBasename) })
  })

/**
 * The full driven surface `main` composes against: the
 * universal `AgentComms` aggregate plus the Zulip-shaped boot extras
 * (reconcile / download / upload / close). Lives in the plugin — core
 * stays substrate-neutral, and `registerTools` keeps its narrower
 * `AgentComms` dependency via structural subtyping. Request-time DI
 * (methods carrying `R = HttpClient`) is deferred.
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
  HttpClient.HttpClient | QueueStateStoreTag | SessionIdTag | ResumeOutcomeTag | SessionBinderTag
> = substrateAdapterLayer(
  Effect.gen(function* () {
    const parsed = yield* parseEnv
    // Queue-state resume hooks: EPHEMERAL seats only (`botName` unset). The
    // write half registers with the configured idle timeout and persists
    // `{queueId, lastEventId}` to the per-session store; the read half
    // (`resumeQueue`) reads it back on boot so a resumed seat resume-polls the
    // surviving queue instead of registering fresh. Persistent bots pass no
    // hooks and keep the server's default queue window.
    const queueHooks =
      parsed.botName === undefined
        ? buildQueueStateHooks({
            store: yield* QueueStateStoreTag,
            session: yield* SessionIdTag,
            idleTimeoutSecs: parsed.queueIdleTimeoutSecs,
            resumeOutcome: yield* ResumeOutcomeTag,
          })
        : undefined
    return yield* zulipAdapter({
      realmUrl: parsed.realmUrl,
      minterEmail: parsed.minterEmail,
      minterApiKey: parsed.minterApiKey,
      // The mint seam. A write reaching for a bound credential resolves this;
      // nothing else decides whether this seat needs an identity.
      bindOnDemand: bindThrough(yield* SessionBinderTag),
      ...(parsed.attachIdentity === undefined ? {} : { attachIdentity: parsed.attachIdentity }),
      ...(queueHooks === undefined
        ? {}
        : {
            queueIdleTimeoutSecs: queueHooks.queueIdleTimeoutSecs,
            onQueueRegister: queueHooks.onQueueRegister,
            onQueueAdvance: queueHooks.onQueueAdvance,
            resumeQueue: queueHooks.resumeQueue,
            onResumeOutcome: queueHooks.onResumeOutcome,
          }),
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
 *      to tee only matching events to the MCP host.
 *   2. `inbox.subscribe` keeps the substrate side wired so the
 *      adapter actually receives events. For Zulip this calls
 *      `/users/me/subscriptions` against the minter, ensuring the
 *      stream is in the minter's queue. The boot-time reconciler
 *      covers most streams; this per-session call still
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
      Effect.tap((intent) =>
        Effect.sync(() => narrowSet.add(intent)).pipe(
          Effect.andThen(inbox.subscribe(intentToTarget(intent))),
        ),
      ),
    ),
  )
}
