import type {
  AgentComms,
  ChannelName,
  ChannelRef,
  Identity,
  IdentityId,
  Message,
  MessageId,
  MessageRef,
  PostOpts,
  ThreadName,
  Timestamp,
} from '@commy/core/ports'
import {
  ChannelPermalinkSchema,
  ChannelRefSchema,
  decodeChannelId,
  decodeChannelName,
  decodeEmoji,
  decodeIdentityId,
  decodeMessageBody,
  decodeMessageId,
  decodeThreadName,
  decodeTimestamp,
  ObservedThreadSchema,
} from '@commy/core/ports'
import type { UserUploadPath, ZulipApiError } from '@commy/zulip/http'
import { decodeUserUploadPath } from '@commy/zulip/http'
import type { PlatformError } from '@effect/platform/Error'
import type { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import {
  Cause,
  Data,
  Effect,
  Exit,
  Option,
  type ParseResult,
  Predicate,
  Record,
  Schema,
} from 'effect'
import type { ProjectSlug, SessionId } from './bootstrap.ts'
import { parseSessionId } from './bootstrap.ts'
import type { IdentityCache } from './identity-cache.ts'
import type { NarrowSet } from './narrow-set.ts'
import { intentToTarget, parseSubscribeTarget } from './subscribe-parser.ts'

interface ToolInputSchema {
  readonly type: 'object'
  readonly properties: Readonly<Record<string, unknown>>
  readonly required?: ReadonlyArray<string>
  readonly additionalProperties: false
}

interface ToolDef {
  readonly name: string
  readonly description: string
  readonly inputSchema: ToolInputSchema
  readonly handler: (args: Readonly<Record<string, unknown>>) => Promise<unknown>
}

const emptyObjectSchema: ToolInputSchema = {
  type: 'object',
  properties: {},
  additionalProperties: false,
}

/**
 * Run an Effect at the MCP tool boundary. On failure this throws the
 * original error from the cause rather than the `FiberFailure` wrapper
 * `Effect.runPromise` would reject with, so the CallTool error handler
 * serializes a clean `<ClassName>: <message>` (e.g. `DirectoryError: …`)
 * instead of `(FiberFailure) DirectoryError: …`.
 */
const runEdge = async <A, E>(effect: Effect.Effect<A, E>): Promise<A> => {
  const exit = await Effect.runPromiseExit(effect)
  if (Exit.isSuccess(exit)) {
    return exit.value
  }
  throw Cause.squash(exit.cause)
}

interface SerializedIdentity {
  readonly id: string
  readonly name: string
  readonly kind: string
}

const identityShape = (identity: Identity): SerializedIdentity => ({
  id: identity.id,
  name: identity.name,
  kind: identity.kind,
})

export interface ToolsCache {
  rememberIdentity(identity: Identity): void
  rememberChannel(channel: ChannelRef): void
  rememberMessage(ref: MessageRef): void
}

/**
 * Dependencies for tool registration. The plugin wires these at boot
 * (see `server.ts:main`); tests build them by hand.
 *
 * `identityCache` resolves a session-aware `EnsureBound` for each
 * attribution-producing call. In persistent mode it wraps a singleton
 * (`createSingleIdentityCache`); in ephemeral mode it holds an at-most-
 * one slot keyed by `session_id` (`createEphemeralIdentityCache`) and
 * release-then-acquires across session transitions. The
 * attribution-producing tools (`post`/`edit_message`/`react`/`unreact`)
 * and the passive `current_identity` read all route through it.
 *
 * `narrowSet` is the consumer-side filter for the inbound event
 * pump. `subscribe` / `unsubscribe` mutate it so the pump tees only
 * intended events to the MCP host. The substrate-side call
 * (`inbox.subscribe` / `inbox.unsubscribe`) handles streams created
 * after the plugin booted; the boot-time minter reconciler
 * covers the rest.
 */
export interface RegisterToolsDeps {
  readonly adapter: AgentComms
  readonly identityCache: IdentityCache
  readonly narrowSet: NarrowSet
  /**
   * Resolve a project slug from the calling session's cwd.
   * Called at every attribution-producing tool call; the result is
   * passed to `identityCache.ensureBoundFor(sid, project)` so the
   * minted `cc-<project>-<8>` name reflects the *calling* session's
   * project rather than the plugin's own location. Wired at boot in
   * `server.ts` from `COMMY_PROJECT` (operator override) and
   * the git probe; defaults to a constant `undefined` resolver when
   * omitted (tests that don't care about per-session derivation).
   */
  readonly projectForCwd?: (cwd: string | undefined) => Effect.Effect<ProjectSlug | undefined>
  /**
   * Restore (or seed) this session's narrow set on its first `subscribe`/
   * `unsubscribe` — memoised once per session_id in `server.ts`, so it runs
   * before any persistence write and the store's presence stays a true resume
   * signal. Omitted in tests that don't exercise persistence;
   * when absent, subscribe/unsubscribe behave exactly as before.
   */
  readonly ensureSessionSubscriptions?: (
    sessionId: SessionId,
    project: ProjectSlug | undefined,
  ) => Effect.Effect<void>
  /**
   * Persist the current narrow set under the session_id after a
   * `subscribe`/`unsubscribe` mutation. Best-effort — never
   * fails the tool call. Omitted in tests that don't exercise persistence.
   */
  readonly persistSessionSubscriptions?: (sessionId: SessionId) => Effect.Effect<void>
  readonly downloadFile?: (
    urlPath: UserUploadPath,
  ) => Effect.Effect<
    { filePath: string; contentType: string; size: number },
    ZulipApiError | PlatformError
  >
  readonly upload?: (
    path: string,
  ) => Effect.Effect<
    { reference: string; filename: string; size: number },
    ZulipApiError | ParseResult.ParseError | PlatformError
  >
}

/**
 * Wire shape for `current_identity`. Pre-acquire ephemeral sessions
 * return `{ state: 'unbound', identity: null }`; once `ensureBound`
 * has resolved (eagerly in persistent mode, lazily on first
 * post/react/unreact) the bound identity surfaces. Callers
 * discriminate on `state` — a single field is both the discriminator
 * and the "did this session attribute anything yet" signal.
 */
type CurrentIdentityResult =
  | {
      readonly state: 'bound'
      readonly identity: SerializedIdentity
      readonly recent_threads?: ReadonlyArray<{
        readonly channel: string
        readonly thread: string
        readonly last_post_ts: number
        readonly last_post_body: string
      }>
    }
  | { readonly state: 'unbound'; readonly identity: null }

interface InternalCache extends ToolsCache {
  readonly identityById: Map<IdentityId, Identity>
  readonly channelByName: Map<ChannelName, ChannelRef>
  readonly messageById: Map<MessageId, MessageRef>
}

const createCache = (): InternalCache => {
  const identityById = new Map<IdentityId, Identity>()
  const channelByName = new Map<ChannelName, ChannelRef>()
  const messageById = new Map<MessageId, MessageRef>()
  return {
    identityById,
    channelByName,
    messageById,
    rememberIdentity: (i) => {
      identityById.set(i.id, i)
    },
    rememberChannel: (c) => {
      channelByName.set(c.name, c)
      // Refs that arrive via inbound events / messages also imply the channel
    },
    rememberMessage: (m) => {
      messageById.set(m.id, m)
      channelByName.set(m.channel.name, m.channel)
    },
  }
}

class UnknownIdentityError extends Data.TaggedError('UnknownIdentity')<{ readonly id: string }> {
  override get message(): string {
    return `no identity cached for id=${this.id}`
  }
}

class UnknownMessageError extends Data.TaggedError('UnknownMessage')<{ readonly id: string }> {
  override get message(): string {
    return `no MessageRef cached for id=${this.id}; pass channel_name (and thread for thread-scoped messages) to reconstruct`
  }
}

// A reconstructed ref is an *address* target (react/edit/unreact/reply): we
// know its id and channel but observed no permalink, so `thread` is `none` —
// the topic name, when needed for sticky engagement, travels as a separate
// address argument rather than through this observation facet.
const reconstructMessageRef = (
  cache: InternalCache,
  rawId: string,
  channelNameArg: string | undefined,
): Effect.Effect<MessageRef, UnknownMessageError | ParseResult.ParseError> =>
  Effect.gen(function* () {
    const id = yield* decodeMessageId(rawId)
    const cached = cache.messageById.get(id)
    if (cached !== undefined) return cached
    if (channelNameArg === undefined) {
      return yield* new UnknownMessageError({ id: rawId })
    }
    const channel = yield* addressChannel(cache, yield* decodeChannelName(channelNameArg))
    return { id, channel, thread: Option.none() }
  })

const resolveMentions = (
  cache: InternalCache,
  mentionIds: ReadonlyArray<string>,
): Effect.Effect<ReadonlyArray<Identity>, UnknownIdentityError | ParseResult.ParseError> =>
  Effect.forEach(mentionIds, (raw) =>
    Effect.gen(function* () {
      const identity = cache.identityById.get(yield* decodeIdentityId(raw))
      if (identity === undefined) {
        return yield* new UnknownIdentityError({ id: raw })
      }
      return identity
    }),
  )

const encodeThreadFacet = Schema.encode(Schema.OptionFromNullOr(ObservedThreadSchema))
const encodeChannel = Schema.encode(ChannelRefSchema)
const decodeChannelPermalink = Schema.decode(ChannelPermalinkSchema)

const messageShape = (m: Message): Effect.Effect<Record<string, unknown>, ParseResult.ParseError> =>
  Effect.all({
    channel: encodeChannel(m.ref.channel),
    thread: encodeThreadFacet(m.ref.thread),
  }).pipe(
    Effect.map(({ channel, thread }) => ({
      id: m.ref.id,
      channel,
      thread,
      permalink: m.ref.permalink ?? null,
      sender: identityShape(m.sender),
      body: m.body,
      ts: m.ts,
      mentions: m.mentions.map(identityShape),
      reactions: m.reactions.map((r) => ({
        emoji: r.emoji,
        by: r.by.map(identityShape),
      })),
    })),
  )

// The channel facet of an *address* MessageRef — the react/edit/unreact/reply
// target rebuilt from a bare id (reconstructMessageRef). When the channel has
// been observed this session it comes straight from cache with its real id and
// permalink. Otherwise only the name is known, so id and permalink are
// name-derived placeholders: transparently placeholders, never a plausible URL,
// and never surfaced — `channelByName` is read only here to rebuild address
// refs (display permalinks come from the adapter, not this cache). The real fix
// is the message-address split (comms-e6yi), which drops the observation
// channel from an address entirely; this placeholder is an accepted transient
// owned by that bead.
const addressChannel = (
  cache: InternalCache,
  name: ChannelName,
): Effect.Effect<ChannelRef, ParseResult.ParseError> =>
  Effect.gen(function* () {
    const cached = cache.channelByName.get(name)
    if (cached !== undefined) return cached
    return {
      id: yield* decodeChannelId(name),
      name,
      permalink: yield* decodeChannelPermalink(name),
    }
  })

const parseRange = (range: {
  readonly since?: number | undefined
  readonly until?: number | undefined
  readonly limit?: number | undefined
}): Effect.Effect<
  { readonly since?: Timestamp; readonly until?: Timestamp; readonly limit?: number },
  ParseResult.ParseError
> =>
  Effect.gen(function* () {
    const out: { since?: Timestamp; until?: Timestamp; limit?: number } = {}
    if (range.since !== undefined) out.since = yield* decodeTimestamp(range.since)
    if (range.until !== undefined) out.until = yield* decodeTimestamp(range.until)
    if (range.limit !== undefined) out.limit = range.limit
    return out
  })

const rangeSchemaFields = {
  since: { type: 'number', description: 'Inclusive lower bound in epoch seconds' },
  until: { type: 'number', description: 'Inclusive upper bound in epoch seconds' },
  limit: { type: 'number', description: 'Hard cap on returned messages' },
} as const

/**
 * Typed parse of the raw MCP `arguments` object, decoded once at the top of
 * each handler's Effect rather than reached into with `args['k']` + per-field
 * `typeof` checks. The struct schemas below are the single source
 * of truth for each tool's parsed-args shape; a wrong-type or missing required
 * field surfaces as a `ParseError` threaded through `runEdge` (the same typed
 * tool-error path the brand decoders already use), never an ad-hoc throw.
 *
 * These parse the *wire shape* (string/number/array). Domain brands
 * (`ChannelName`, `MessageBody`, `Emoji`, …) are still minted from the parsed
 * fields inside the handler via the `decode*` decoders in `core/ports.ts`.
 *
 * The MCP-advertised `inputSchema` on each `ToolDef` stays hand-written: it is
 * what `tools/list` exposes to clients and what the central unknown-argument
 * guard in `registerTools` checks against. The schemas here govern the
 * handler-side parse only.
 */
const RangeArgs = {
  since: Schema.optional(Schema.Number),
  until: Schema.optional(Schema.Number),
  limit: Schema.optional(Schema.Number),
}

const ResolveArgs = Schema.Struct({ name: Schema.String })
const PostArgs = Schema.Struct({
  channel_name: Schema.String,
  body: Schema.String,
  thread: Schema.optional(Schema.String),
  mentions: Schema.optional(Schema.Array(Schema.String)),
  reply_to: Schema.optional(Schema.String),
})
const EditMessageArgs = Schema.Struct({
  message_id: Schema.String,
  body: Schema.String,
  channel_name: Schema.optional(Schema.String),
  thread: Schema.optional(Schema.String),
})
const ReactArgs = Schema.Struct({
  message_id: Schema.String,
  emoji: Schema.String,
  channel_name: Schema.optional(Schema.String),
  thread: Schema.optional(Schema.String),
})
const SubscribeArgs = Schema.Struct({ target: Schema.String })
const ReadChannelArgs = Schema.Struct({ channel_name: Schema.String, ...RangeArgs })
const ReadThreadArgs = Schema.Struct({
  channel_name: Schema.String,
  thread: Schema.String,
  ...RangeArgs,
})
const MessageLinkArgs = Schema.Struct({
  message_id: Schema.String,
  channel_name: Schema.optional(Schema.String),
  thread: Schema.optional(Schema.String),
})
const PresenceArgs = Schema.Struct({ identity_id: Schema.String })
const DownloadFileArgs = Schema.Struct({ url_path: Schema.String })
const UploadFileArgs = Schema.Struct({ path: Schema.String })

const buildToolDefs = (deps: RegisterToolsDeps, cache: InternalCache): ReadonlyArray<ToolDef> => {
  const { adapter, identityCache, narrowSet } = deps
  const projectForCwd = deps.projectForCwd ?? (() => Effect.succeed(undefined))
  const sessionIdField = {
    type: 'string',
    description:
      "Per-conversation identifier (UUID). In Claude Code, the plugin's PreToolUse hook injects this from the harness session id; non-CC MCP clients must supply a UUID (e.g. via crypto.randomUUID()). Anything that fails UUID validation is treated as missing — the server returns the unbound-stub error rather than minting a malformed cc-* identity. Drives ephemeral identity minting.",
  } as const
  const cwdField = {
    type: 'string',
    description:
      "Calling session's working directory. In Claude Code, the PreToolUse hook injects this from the harness cwd; non-CC MCP clients may supply their own. Drives the project component of ephemeral cc-<project>-<8> names — without this the minted name falls back to bare cc-<8> (or the operator-forced COMMY_PROJECT slug).",
  } as const
  // SessionId is a branded type — `parseSessionId` is the single mint point.
  // A non-UUID raw value (model-guessed string, hook misfire) returns
  // `undefined` and routes through the cache's unbound stub, so unvalidated
  // strings cannot reach `composeBotName` and produce a malformed
  // `cc-<project>-<garbage>` identity. The hook injects session_id from CC's
  // session UUID, which always passes; non-CC clients without a UUID fail
  // loudly at parse time rather than silently minting garbage downstream.
  const readSessionId = (args: Readonly<Record<string, unknown>>): SessionId | undefined =>
    Option.getOrUndefined(parseSessionId(args['session_id']))
  const readCwd = (args: Readonly<Record<string, unknown>>): string | undefined => {
    const raw = args['cwd']
    return Predicate.isString(raw) ? raw : undefined
  }
  const projectForArgs = (
    args: Readonly<Record<string, unknown>>,
  ): Effect.Effect<ProjectSlug | undefined> => projectForCwd(readCwd(args))
  // Sticky engagement: active participation in a thread —
  // posting or reacting — implies interest in its replies. Idempotent;
  // narrow-set add is set-backed and inbox.subscribe mirrors the explicit
  // subscribe tool. No-op for refs without a thread (top-level channel
  // messages don't get sticky thread behaviour).
  const stickyThreadEngagement = async (
    channel: ChannelRef,
    threadName: Option.Option<ThreadName>,
  ): Promise<void> => {
    if (Option.isNone(threadName)) return
    const intent = {
      kind: 'thread' as const,
      channelName: channel.name,
      threadName: threadName.value,
    }
    narrowSet.add(intent)
    await runEdge(adapter.inbox.subscribe(intentToTarget(intent)))
  }
  const coreTools: ReadonlyArray<ToolDef> = [
    {
      name: 'current_identity',
      description:
        'Return the identity this session is bound to. Passive — never triggers acquire. Returns {state: "unbound", identity: null} for ephemeral sessions that have not yet performed an attribution-producing action; {state: "bound", identity: {id, name, kind}, recent_threads?} once acquire has resolved. recent_threads is a best-effort orientation list of {channel, thread, last_post_ts, last_post_body} for the threads this identity most recently posted in; it is omitted when the enrichment lookup fails — the binding self-check itself never fails on it.',
      inputSchema: {
        type: 'object',
        properties: { session_id: sessionIdField, cwd: cwdField },
        additionalProperties: false,
      },
      handler: async (args): Promise<CurrentIdentityResult> => {
        const sessionId = readSessionId(args)
        const ensureBound = await runEdge(
          Effect.flatMap(projectForArgs(args), (project) =>
            identityCache.ensureBoundFor(sessionId, project),
          ),
        )
        const current = ensureBound.current()
        if (current === undefined) {
          return { state: 'unbound', identity: null }
        }
        cache.rememberIdentity(current.identity)
        const bound = { state: 'bound' as const, identity: identityShape(current.identity) }
        try {
          const threads = await runEdge(adapter.history.recentThreads(current.identity.id))
          return {
            ...bound,
            recent_threads: threads.map((t) => ({
              channel: t.channel,
              thread: t.thread,
              last_post_ts: t.lastPostTs,
              last_post_body: t.lastPostBody,
            })),
          }
        } catch {
          // recent_threads is a best-effort orientation enrichment; a binding
          // self-check must never hard-fail on the round-trip. Omit it on any
          // error rather than propagating.
          return bound
        }
      },
    },
    {
      name: 'resolve',
      description:
        'Look up an identity by display name. Returns {identity} or {identity: null} when no match.',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string', description: 'Display name to resolve' },
        },
        required: ['name'],
        additionalProperties: false,
      },
      handler: async (args) => {
        const resolved = await runEdge(
          Schema.decodeUnknown(ResolveArgs)(args).pipe(
            Effect.flatMap(({ name }) => adapter.identity.resolve(name)),
          ),
        )
        return Option.match(resolved, {
          onNone: () => ({ identity: null }),
          onSome: (identity) => {
            cache.rememberIdentity(identity)
            return { identity: identityShape(identity) }
          },
        })
      },
    },
    {
      name: 'list_agents',
      description: 'List all agent-kind identities the substrate is aware of.',
      inputSchema: emptyObjectSchema,
      handler: async () => {
        const identities = await runEdge(adapter.directory.listAgents())
        for (const i of identities) cache.rememberIdentity(i)
        return { identities: identities.map(identityShape) }
      },
    },
    {
      name: 'list_humans',
      description: 'List all human-kind identities the substrate is aware of.',
      inputSchema: emptyObjectSchema,
      handler: async () => {
        const identities = await runEdge(adapter.directory.listHumans())
        for (const i of identities) cache.rememberIdentity(i)
        return { identities: identities.map(identityShape) }
      },
    },
    {
      name: 'list_channels',
      description:
        'List every channel the substrate is aware of, returning {id, name, permalink} for each. Use this for discovery instead of guessing channel names — posting to a non-existent channel throws UnknownChannel. When you show a human a channel, link it by its permalink rather than a bare name.',
      inputSchema: emptyObjectSchema,
      handler: async () => {
        const channels = await runEdge(adapter.directory.listChannels())
        for (const c of channels) cache.rememberChannel(c)
        return { channels: await runEdge(Effect.forEach(channels, (c) => encodeChannel(c))) }
      },
    },
    {
      name: 'post',
      description:
        'Post a message to a channel by name. Optional thread (topic), mentions (identity ids the bot has seen), and reply_to (message id). To ping someone on Zulip, write the @**Name** markup inline in body where you want it rendered — the mentions array is notification metadata only and does not modify body. Returns {message_id, channel_id, channel_name, thread, permalink}. When you show a human this message afterwards, link it by the returned permalink — never a bare name or message id.',
      inputSchema: {
        type: 'object',
        properties: {
          channel_name: { type: 'string', description: 'Channel to post into' },
          body: {
            type: 'string',
            description:
              'Message body, written verbatim. Include @**Name** markup inline to trigger Zulip @-mention pings; the mentions[] field is metadata-only and will not add markup for you.',
          },
          thread: { type: 'string', description: 'Thread / topic name (optional)' },
          mentions: {
            type: 'array',
            items: { type: 'string' },
            description:
              'Identity ids to notify (metadata-only — does not modify body). Useful for substrates with a separate notification primitive. Ids must be cached via resolve/list_*/inbound events first.',
          },
          reply_to: {
            type: 'string',
            description: 'Message id of a prior message to reply to',
          },
          session_id: sessionIdField,
          cwd: cwdField,
        },
        required: ['channel_name', 'body'],
        additionalProperties: false,
      },
      handler: async (args) => {
        await runEdge(
          Effect.flatMap(projectForArgs(args), (project) =>
            identityCache.ensureBoundFor(readSessionId(args), project),
          ).pipe(Effect.flatMap((ensureBound) => ensureBound())),
        )
        const ref = await runEdge(
          Effect.gen(function* () {
            const { channel_name, body, thread, mentions, reply_to } =
              yield* Schema.decodeUnknown(PostArgs)(args)
            const channel = yield* decodeChannelName(channel_name)
            const messageBody = yield* decodeMessageBody(body)
            const opts: { -readonly [K in keyof PostOpts]: PostOpts[K] } = {}
            if (thread !== undefined) {
              opts.thread = yield* decodeThreadName(thread)
            }
            if (mentions !== undefined) {
              opts.mentions = yield* resolveMentions(cache, mentions)
            }
            if (reply_to !== undefined) {
              opts.replyTo = yield* reconstructMessageRef(cache, reply_to, channel_name)
            }
            return yield* adapter.publisher.post(channel, messageBody, opts)
          }),
        )
        cache.rememberMessage(ref)
        await stickyThreadEngagement(
          ref.channel,
          Option.map(ref.thread, (t) => t.name),
        )
        return {
          message_id: ref.id,
          channel_id: ref.channel.id,
          channel_name: ref.channel.name,
          thread: Option.match(ref.thread, {
            onNone: () => null,
            onSome: (t) => ({ name: t.name }),
          }),
          permalink: ref.permalink ?? null,
        }
      },
    },
    {
      name: 'edit_message',
      description:
        'Replace the body of a prior message. The bound identity must be the original sender. Cache-hit MessageRefs need only message_id; cache misses require channel_name (and thread for thread-scoped messages). Body is written verbatim — same @**Name** markup rules as post.',
      inputSchema: {
        type: 'object',
        properties: {
          message_id: { type: 'string', description: 'Message id to edit' },
          body: {
            type: 'string',
            description:
              'Replacement body, written verbatim. Include @**Name** markup inline to trigger Zulip @-mention pings.',
          },
          channel_name: { type: 'string', description: 'Channel name (required on cache miss)' },
          thread: {
            type: 'string',
            description: 'Thread / topic name (for thread-scoped messages)',
          },
          session_id: sessionIdField,
          cwd: cwdField,
        },
        required: ['message_id', 'body'],
        additionalProperties: false,
      },
      handler: async (args) => {
        await runEdge(
          Effect.flatMap(projectForArgs(args), (project) =>
            identityCache.ensureBoundFor(readSessionId(args), project),
          ).pipe(Effect.flatMap((ensureBound) => ensureBound())),
        )
        const ref = await runEdge(
          Effect.gen(function* () {
            const { message_id, body, channel_name } =
              yield* Schema.decodeUnknown(EditMessageArgs)(args)
            const target = yield* reconstructMessageRef(cache, message_id, channel_name)
            yield* adapter.publisher.edit(target, yield* decodeMessageBody(body))
            return target
          }),
        )
        cache.rememberMessage(ref)
        return {}
      },
    },
    {
      name: 'react',
      description:
        'Add an emoji reaction to a message. Cache-hit MessageRefs need only message_id; cache misses require channel_name (and thread for thread-scoped messages).',
      inputSchema: {
        type: 'object',
        properties: {
          message_id: { type: 'string', description: 'Message id to react to' },
          emoji: {
            type: 'string',
            description: 'Substrate-native emoji name (no surrounding colons)',
          },
          channel_name: { type: 'string', description: 'Channel name (required on cache miss)' },
          thread: {
            type: 'string',
            description: 'Thread / topic name (for thread-scoped messages)',
          },
          session_id: sessionIdField,
          cwd: cwdField,
        },
        required: ['message_id', 'emoji'],
        additionalProperties: false,
      },
      handler: async (args) => {
        await runEdge(
          Effect.flatMap(projectForArgs(args), (project) =>
            identityCache.ensureBoundFor(readSessionId(args), project),
          ).pipe(Effect.flatMap((ensureBound) => ensureBound())),
        )
        const { ref, threadName } = await runEdge(
          Effect.gen(function* () {
            const { message_id, emoji, channel_name, thread } =
              yield* Schema.decodeUnknown(ReactArgs)(args)
            const target = yield* reconstructMessageRef(cache, message_id, channel_name)
            yield* adapter.publisher.react(target, yield* decodeEmoji(emoji))
            // A cache-hit target carries the observed thread; a cache-miss one
            // does not, so fall back to the caller-supplied topic name.
            const argThread = yield* Effect.transposeMapOption(
              Option.fromNullable(thread),
              decodeThreadName,
            )
            const threadName = Option.orElse(
              Option.map(target.thread, (t) => t.name),
              () => argThread,
            )
            return { ref: target, threadName }
          }),
        )
        cache.rememberMessage(ref)
        await stickyThreadEngagement(ref.channel, threadName)
        return {}
      },
    },
    {
      name: 'unreact',
      description: 'Remove an emoji reaction from a message. Same MessageRef resolution as react.',
      inputSchema: {
        type: 'object',
        properties: {
          message_id: { type: 'string', description: 'Message id to remove reaction from' },
          emoji: { type: 'string', description: 'Substrate-native emoji name' },
          channel_name: { type: 'string', description: 'Channel name (required on cache miss)' },
          thread: { type: 'string', description: 'Thread / topic name' },
          session_id: sessionIdField,
          cwd: cwdField,
        },
        required: ['message_id', 'emoji'],
        additionalProperties: false,
      },
      handler: async (args) => {
        await runEdge(
          Effect.flatMap(projectForArgs(args), (project) =>
            identityCache.ensureBoundFor(readSessionId(args), project),
          ).pipe(Effect.flatMap((ensureBound) => ensureBound())),
        )
        await runEdge(
          Effect.gen(function* () {
            const { message_id, emoji, channel_name } = yield* Schema.decodeUnknown(ReactArgs)(args)
            const ref = yield* reconstructMessageRef(cache, message_id, channel_name)
            yield* adapter.publisher.unreact(ref, yield* decodeEmoji(emoji))
          }),
        )
        return {}
      },
    },
    {
      name: 'subscribe',
      description:
        'Subscribe the bot to a substrate target. Token syntax: "channel:<name>", "thread:<channel>/<thread>", "new-topics:<channel>", or "mentions".',
      inputSchema: {
        type: 'object',
        properties: {
          target: {
            type: 'string',
            description:
              'Subscribe-target token: "channel:<name>", "thread:<channel>/<thread>", "new-topics:<channel>", or "mentions"',
          },
          session_id: sessionIdField,
          cwd: cwdField,
        },
        required: ['target'],
        additionalProperties: false,
      },
      handler: async (args) => {
        await runEdge(
          Effect.gen(function* () {
            const { target: raw } = yield* Schema.decodeUnknown(SubscribeArgs)(args)
            const intent = yield* parseSubscribeTarget(raw)
            const sessionId = readSessionId(args)
            // Restore (or seed) this session's set before the first mutation, so
            // the snapshot persisted below captures the full live set and the
            // store's presence stays a true resume signal.
            if (sessionId !== undefined && deps.ensureSessionSubscriptions !== undefined) {
              yield* deps.ensureSessionSubscriptions(sessionId, yield* projectForArgs(args))
            }
            // Two sinks (see bootstrap.subscribeFromEnv): the consumer-side
            // narrow tells the event pump to tee matching events through;
            // the substrate-side call subscribes the minter to streams the
            // boot-time reconciler didn't have a chance to cover.
            narrowSet.add(intent)
            yield* adapter.inbox.subscribe(intentToTarget(intent))
            if (sessionId !== undefined && deps.persistSessionSubscriptions !== undefined) {
              yield* deps.persistSessionSubscriptions(sessionId)
            }
          }),
        )
        return {}
      },
    },
    {
      name: 'unsubscribe',
      description: 'Unsubscribe the bot from a substrate target. Same token syntax as subscribe.',
      inputSchema: {
        type: 'object',
        properties: {
          target: {
            type: 'string',
            description:
              'Subscribe-target token: "channel:<name>", "thread:<channel>/<thread>", "new-topics:<channel>", or "mentions"',
          },
          session_id: sessionIdField,
          cwd: cwdField,
        },
        required: ['target'],
        additionalProperties: false,
      },
      handler: async (args) => {
        await runEdge(
          Effect.gen(function* () {
            const { target: raw } = yield* Schema.decodeUnknown(SubscribeArgs)(args)
            const intent = yield* parseSubscribeTarget(raw)
            const sessionId = readSessionId(args)
            if (sessionId !== undefined && deps.ensureSessionSubscriptions !== undefined) {
              yield* deps.ensureSessionSubscriptions(sessionId, yield* projectForArgs(args))
            }
            narrowSet.remove(intent)
            yield* adapter.inbox.unsubscribe(intentToTarget(intent))
            if (sessionId !== undefined && deps.persistSessionSubscriptions !== undefined) {
              yield* deps.persistSessionSubscriptions(sessionId)
            }
          }),
        )
        return {}
      },
    },
    {
      name: 'read_channel',
      description:
        'Read recent messages from a channel by name. Returns {messages: Message[]} bounded by optional since/until/limit. Each message carries a clickable permalink (and channel.permalink / thread.permalink) — when you cite one of these to a human, render it as that permalink, not a bare name or id.',
      inputSchema: {
        type: 'object',
        properties: {
          channel_name: { type: 'string', description: 'Channel name to read from' },
          ...rangeSchemaFields,
        },
        required: ['channel_name'],
        additionalProperties: false,
      },
      handler: async (args) => {
        const messages = await runEdge(
          Effect.gen(function* () {
            const parsed = yield* Schema.decodeUnknown(ReadChannelArgs)(args)
            const channel = yield* decodeChannelName(parsed.channel_name)
            return yield* adapter.history.readChannel(channel, yield* parseRange(parsed))
          }),
        )
        for (const m of messages) {
          cache.rememberChannel(m.ref.channel)
          cache.rememberIdentity(m.sender)
        }
        return { messages: await runEdge(Effect.forEach(messages, messageShape)) }
      },
    },
    {
      name: 'read_thread',
      description:
        'Read recent messages from a thread (topic) within a channel. Returns {messages: Message[]} bounded by optional since/until/limit. Each message carries a clickable permalink (and channel.permalink / thread.permalink) — when you cite one of these to a human, render it as that permalink, not a bare name or id.',
      inputSchema: {
        type: 'object',
        properties: {
          channel_name: { type: 'string', description: 'Channel the thread lives in' },
          thread: { type: 'string', description: 'Thread / topic name within the channel' },
          ...rangeSchemaFields,
        },
        required: ['channel_name', 'thread'],
        additionalProperties: false,
      },
      handler: async (args) => {
        const messages = await runEdge(
          Effect.gen(function* () {
            const parsed = yield* Schema.decodeUnknown(ReadThreadArgs)(args)
            const channel = yield* decodeChannelName(parsed.channel_name)
            return yield* adapter.history.readThread(
              channel,
              yield* decodeThreadName(parsed.thread),
              yield* parseRange(parsed),
            )
          }),
        )
        for (const m of messages) {
          cache.rememberChannel(m.ref.channel)
          cache.rememberIdentity(m.sender)
        }
        return { messages: await runEdge(Effect.forEach(messages, messageShape)) }
      },
    },
    {
      name: 'message_link',
      description:
        'Return the canonical clickable permalink for a message id: {permalink}. Use when you hold only an id (e.g. one cited elsewhere) and want the URL without re-deriving the narrow format — messages from post/read already carry a permalink. A message just posted or read is resolved from cache; otherwise pass channel_name (and thread) to build the link directly, or omit them to look the message up by id. Returns {permalink: null} when the message cannot be resolved.',
      inputSchema: {
        type: 'object',
        properties: {
          message_id: { type: 'string', description: 'Message id to link to' },
          channel_name: {
            type: 'string',
            description: 'Channel the message lives in (lets the link be built without a lookup)',
          },
          thread: {
            type: 'string',
            description: 'Thread / topic the message lives in (sharpens the link to the topic)',
          },
        },
        required: ['message_id'],
        additionalProperties: false,
      },
      handler: async (args) => {
        const permalink = await runEdge(
          Effect.gen(function* () {
            const parsed = yield* Schema.decodeUnknown(MessageLinkArgs)(args)
            const id = yield* decodeMessageId(parsed.message_id)
            const cached = cache.messageById.get(id)
            if (cached?.permalink !== undefined) return cached.permalink
            const hint =
              parsed.channel_name === undefined
                ? undefined
                : {
                    channel: yield* decodeChannelName(parsed.channel_name),
                    ...(parsed.thread === undefined
                      ? {}
                      : { thread: yield* decodeThreadName(parsed.thread) }),
                  }
            return Option.getOrNull(yield* adapter.history.messagePermalink(id, hint))
          }),
        )
        return { permalink }
      },
    },
    {
      name: 'presence',
      description:
        "Return presence for the given identity id: online/idle/offline for humans, or 'unknown' for agents (bots have no presence concept — Zulip presence is human-only). Identity must already be cached via list_agents, list_humans, resolve, or current_identity — call one of those first when given a name.",
      inputSchema: {
        type: 'object',
        properties: {
          identity_id: {
            type: 'string',
            description: 'Identity id from a prior list/resolve/current_identity call',
          },
        },
        required: ['identity_id'],
        additionalProperties: false,
      },
      handler: async (args) => {
        const presence = await runEdge(
          Effect.gen(function* () {
            const { identity_id } = yield* Schema.decodeUnknown(PresenceArgs)(args)
            const identity = cache.identityById.get(yield* decodeIdentityId(identity_id))
            if (identity === undefined) {
              return yield* new UnknownIdentityError({ id: identity_id })
            }
            return yield* adapter.directory.presence(identity)
          }),
        )
        return { presence }
      },
    },
  ]
  const optionalTools: ToolDef[] = []
  if (deps.downloadFile !== undefined) {
    const download = deps.downloadFile
    optionalTools.push({
      name: 'download_file',
      description:
        'Download a Zulip user upload by its /user_uploads/... path (visible in message bodies). Writes the file to a temp path and returns {file_path, content_type, size}. Use the Read tool on the returned file_path to view images.',
      inputSchema: {
        type: 'object',
        properties: {
          url_path: {
            type: 'string',
            description:
              'The /user_uploads/... path from the message body (e.g. /user_uploads/2/56/image.png)',
          },
        },
        required: ['url_path'],
        additionalProperties: false,
      },
      handler: async (args) => {
        const result = await runEdge(
          Schema.decodeUnknown(DownloadFileArgs)(args).pipe(
            Effect.flatMap(({ url_path }) => decodeUserUploadPath(url_path)),
            Effect.flatMap(download),
          ),
        )
        return {
          file_path: result.filePath,
          content_type: result.contentType,
          size: result.size,
        }
      },
    } satisfies ToolDef)
  }
  if (deps.upload !== undefined) {
    const upload = deps.upload
    optionalTools.push({
      name: 'upload_file',
      description:
        'Upload a local file to the realm so it can be referenced in a message. Takes an absolute path, returns {reference, filename, size}. The `reference` is a ready-to-embed string — drop it into a post body wherever you want the attachment to render (same as writing @**Name** markup yourself).',
      inputSchema: {
        type: 'object',
        properties: {
          path: {
            type: 'string',
            description: 'Absolute path to the local file to upload (e.g. /tmp/chart.png)',
          },
        },
        required: ['path'],
        additionalProperties: false,
      },
      handler: async (args) => {
        const { path } = await runEdge(Schema.decodeUnknown(UploadFileArgs)(args))
        if (!path.startsWith('/')) {
          throw new Error(`upload_file: path must be absolute — received: ${path}`)
        }
        const result = await runEdge(upload(path))
        return {
          reference: result.reference,
          filename: result.filename,
          size: result.size,
        }
      },
    } satisfies ToolDef)
  }
  return optionalTools.length === 0 ? coreTools : [...coreTools, ...optionalTools]
}

export const registerTools = (server: Server, deps: RegisterToolsDeps): ToolsCache => {
  const cache = createCache()
  const defs = buildToolDefs(deps, cache)
  const byName = new Map(defs.map((def) => [def.name, def]))

  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: defs.map((def) => ({
      name: def.name,
      description: def.description,
      inputSchema: def.inputSchema,
    })),
  }))

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const def = byName.get(request.params.name)
    if (def === undefined) {
      throw new Error(`unknown tool: ${request.params.name}`)
    }
    const args = (request.params.arguments ?? {}) as Readonly<Record<string, unknown>>
    const knownKeys = new Set(Record.keys(def.inputSchema.properties))
    const unknownKeys = Record.keys(args).filter((k) => !knownKeys.has(k))
    if (unknownKeys.length > 0) {
      throw new Error(
        `${def.name}: unknown argument(s): ${unknownKeys.join(', ')}. Valid arguments: ${[...knownKeys].join(', ')}`,
      )
    }
    try {
      const structuredContent = await def.handler(args)
      return {
        content: [{ type: 'text' as const, text: JSON.stringify(structuredContent) }],
        structuredContent: structuredContent as Record<string, unknown>,
      }
    } catch (e) {
      // Surface the port/parser exception class name so the client sees
      // "<ClassName>: <message>" rather than the bare message — gives
      // consumers a code-like discriminator without leaving the JSON-RPC
      // message channel.
      if (Predicate.isError(e) && e.name !== 'Error' && !e.message.startsWith(`${e.name}:`)) {
        throw new Error(`${e.name}: ${e.message}`)
      }
      throw e
    }
  })

  return {
    rememberIdentity: cache.rememberIdentity,
    rememberChannel: cache.rememberChannel,
    rememberMessage: cache.rememberMessage,
  }
}
