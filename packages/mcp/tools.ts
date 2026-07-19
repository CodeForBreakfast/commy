import type {
  AgentComms,
  ChannelDescription,
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
  decodeChannelDescription,
  decodeChannelId,
  decodeChannelName,
  decodeEmoji,
  decodeIdentityId,
  decodeMessageBody,
  decodeMessageId,
  decodeThreadName,
  decodeTimestamp,
  Mention,
  MessagePermalinkSchema,
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

/**
 * Wire shape of a mention. `type` discriminates the three things a mention can
 * be: a named person, one of the substrate's whole-audience wildcards, or a
 * named group. A reader that only understands `type: 'user'` still sees that
 * the other forms happened rather than seeing an empty list — the failure this
 * shape exists to end.
 */
type SerializedMention =
  | ({ readonly type: 'user' } & SerializedIdentity)
  | { readonly type: 'channel-wildcard' }
  | { readonly type: 'topic-wildcard' }
  | { readonly type: 'group'; readonly name: string }

const mentionShape = Mention.$match({
  UserMention: (m): SerializedMention => ({ type: 'user', ...identityShape(m.identity) }),
  ChannelWildcardMention: (): SerializedMention => ({ type: 'channel-wildcard' }),
  TopicWildcardMention: (): SerializedMention => ({ type: 'topic-wildcard' }),
  GroupMention: (m): SerializedMention => ({ type: 'group', name: m.name }),
})

/**
 * The remembering half of what registration hands back — everything the
 * pump needs to feed observed identities, channels and messages into the
 * lookup the tools resolve against.
 */
export interface ToolsMemory {
  rememberIdentity(identity: Identity): void
  rememberChannel(channel: ChannelRef): void
  rememberMessage(ref: MessageRef): void
}

export interface ToolsCache extends ToolsMemory {
  /**
   * Rebuild the tool list against a new value of the realm-wide editing
   * switch. `canEditMessages` on the deps is only the value sampled at
   * boot; an administrator can move it under a connected seat, and this is
   * how the seat catches up without reconnecting.
   *
   * Rebuilding rather than mutating one entry keeps a single construction
   * path — the list a caller sees after this is the list `buildToolDefs`
   * would have produced had the new value been the boot sample. Dispatch
   * reads the same rebuilt source, so a withdrawn tool stops being callable
   * rather than merely disappearing from the listing.
   *
   * Emitting `notifications/tools/list_changed` is the caller's job (see
   * `server.ts`): registration owns the list, the server owns the wire.
   */
  setEditingAvailable(available: boolean): void
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
   * Persist the current narrow set after a `subscribe`/`unsubscribe`
   * mutation. Id-blind: the session_id is never stamped on these calls, so
   * the persist polls the shared session-id deferred internally and writes
   * only when the id is already known — no argument crosses this seam.
   * Best-effort — never fails the tool call. A bare lazy Effect (not a
   * thunk): each `yield*` re-polls the deferred and re-reads the current
   * snapshot at execution time. Omitted in tests that don't exercise
   * persistence.
   */
  readonly persistSessionSubscriptions?: Effect.Effect<void>
  /**
   * Hand a PreToolUse-stamped session_id to the shared session-id `Deferred`
   * (comms-k7cv). Every hooked tool (post/edit_message/react/unreact/
   * current_identity) feeds it; the feed is idempotent (first-writer-wins), so
   * the first hooked call of the MCP child fills the deferred for every awaiter.
   * The boot feeder in `server.ts` fills it earlier when the host injects
   * `CLAUDE_CODE_SESSION_ID`; this per-call feed covers a host that doesn't.
   * Omitted in tests that don't exercise the session-id latch.
   */
  readonly feedSessionId?: (sessionId: SessionId) => Effect.Effect<void>
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
  /**
   * Whether the substrate permitted editing when the caller sampled
   * `MessagePublisher.editingAvailable` at connect. `false` withholds
   * `edit_message` from the tool list entirely, so a seat on a realm with
   * editing switched off is never offered a tool that cannot work.
   *
   * A plain boolean, resolved by the caller: registration branches on a
   * capability, never on a substrate. Omitted means "assume available" —
   * see the fail-open note in `server.ts` where it is sampled.
   */
  readonly canEditMessages?: boolean
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

interface InternalCache extends ToolsMemory {
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
// know its id and channel but observed no permalink, so `thread` is `none` and
// `permalink` is a name-derived placeholder (the id itself) — transparently a
// placeholder, never a plausible URL, and never surfaced (display permalinks
// come from the adapter, not this rebuilt address). The topic name, when needed
// for sticky engagement, travels as a separate address argument rather than
// through this observation facet. The real fix is the message-address split
// (comms-e6yi), which drops the observation facets from an address entirely;
// this placeholder is an accepted transient owned by that bead.
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
    return { id, channel, thread: Option.none(), permalink: yield* decodeMessagePermalink(id) }
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
const encodeMessagePermalink = Schema.encode(MessagePermalinkSchema)
const decodeChannelPermalink = Schema.decode(ChannelPermalinkSchema)
const decodeMessagePermalink = Schema.decode(MessagePermalinkSchema)

const messageShape = (m: Message): Effect.Effect<Record<string, unknown>, ParseResult.ParseError> =>
  Effect.all({
    channel: encodeChannel(m.ref.channel),
    thread: encodeThreadFacet(m.ref.thread),
    permalink: encodeMessagePermalink(m.ref.permalink),
  }).pipe(
    Effect.map(({ channel, thread, permalink }) => ({
      id: m.ref.id,
      channel,
      thread,
      permalink,
      sender: identityShape(m.sender),
      body: m.body,
      ts: m.ts,
      mentions: m.mentions.map(mentionShape),
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
const ThreadResolutionArgs = Schema.Struct({
  channel_name: Schema.String,
  thread: Schema.String,
})
const ChannelDescriptionArgs = Schema.Struct({
  channel_name: Schema.String,
})
const SetChannelDescriptionArgs = Schema.Struct({
  channel_name: Schema.String,
  description: Schema.String,
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
  // Feed the shared session-id deferred (comms-k7cv), guarded on the same
  // UUID brand as minting so a malformed id never reaches it. A no-op when the
  // dep is absent (tests) or the arg didn't parse.
  const feedSession = (sessionId: SessionId | undefined): Effect.Effect<void> =>
    sessionId === undefined || deps.feedSessionId === undefined
      ? Effect.void
      : deps.feedSessionId(sessionId)
  const readCwd = (args: Readonly<Record<string, unknown>>): string | undefined => {
    const raw = args['cwd']
    return Predicate.isString(raw) ? raw : undefined
  }
  const projectForArgs = (
    args: Readonly<Record<string, unknown>>,
  ): Effect.Effect<ProjectSlug | undefined> => projectForCwd(readCwd(args))
  // The single binding choke-point every PreToolUse-stamped tool routes
  // through: feed the session_id, then resolve the ephemeral identity for it.
  // The returned `ensureBound` thunk IS the acquire — the attribution tools
  // invoke it, `current_identity` leaves it (a passive read that still feeds).
  const ensureBoundForArgs = (args: Readonly<Record<string, unknown>>) =>
    Effect.gen(function* () {
      const sessionId = readSessionId(args)
      yield* feedSession(sessionId)
      return yield* identityCache.ensureBoundFor(sessionId, yield* projectForArgs(args))
    })
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
        const ensureBound = await runEdge(ensureBoundForArgs(args))
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
        await runEdge(ensureBoundForArgs(args).pipe(Effect.flatMap((ensureBound) => ensureBound())))
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
          permalink: ref.permalink,
        }
      },
    },
    {
      name: 'edit_message',
      description:
        'Replace the body of a prior message. Three walls the substrate cannot lift refuse an edit — all surface as a typed MessageEditRefused, and the recovery for any of them is to re-post rather than edit: (1) AUTHORSHIP — only the original sender may edit, so a cross-session ephemeral seat can NEVER edit a message a prior seat posted, at any age; a decision anchor that outlives its authoring session is permanently uneditable. (2) EDIT-WINDOW — the realm caps how long even the original sender may edit (message_content_edit_limit_seconds; often minutes), after which the edit is refused by age alone. (3) EDITING-DISABLED — the realm has message editing turned off entirely (allow_message_editing), so nothing on it is ever editable by anyone. This tool is withheld from the tool list on such a realm, so seeing it at all means editing was on when this session connected; you can still hit this wall if an administrator switched editing off mid-session. Cache-hit MessageRefs need only message_id; cache misses require channel_name (and thread for thread-scoped messages). Body is written verbatim — same @**Name** markup rules as post.',
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
        await runEdge(ensureBoundForArgs(args).pipe(Effect.flatMap((ensureBound) => ensureBound())))
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
        await runEdge(ensureBoundForArgs(args).pipe(Effect.flatMap((ensureBound) => ensureBound())))
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
        await runEdge(ensureBoundForArgs(args).pipe(Effect.flatMap((ensureBound) => ensureBound())))
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
        'Subscribe the bot to a substrate target. Token syntax: "<channel>" for a whole channel, "<channel>/<thread>" for one topic in it, or "new-topics:<channel>" for the first message of each new topic. Mentions of the bot always arrive and need no subscription.',
      inputSchema: {
        type: 'object',
        properties: {
          target: {
            type: 'string',
            description:
              'Subscribe-target token: "<channel>", "<channel>/<thread>", or "new-topics:<channel>"',
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
            // Feed the shared session-id deferred first: subscribe is a feeder,
            // and the session-bound store's restore/persist below await this
            // deferred to resolve their id-keyed path. Without this a
            // subscribe-first resumed seat (no boot-env id) would park on the
            // await — the deaf-resume case this reactive core exists to close.
            yield* feedSession(sessionId)
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
            yield* Effect.sync(() => narrowSet.add(intent)).pipe(
              Effect.andThen(adapter.inbox.subscribe(intentToTarget(intent))),
            )
            // Persist is id-blind (this call carries no session_id): it polls
            // the shared deferred internally and writes only when the id is
            // already known, so it fires here without an id argument.
            if (deps.persistSessionSubscriptions !== undefined) {
              yield* deps.persistSessionSubscriptions
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
              'Subscribe-target token: "<channel>", "<channel>/<thread>", or "new-topics:<channel>"',
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
            // Feed the shared session-id deferred first (see subscribe): the
            // session-bound store's restore/persist below await it.
            yield* feedSession(sessionId)
            if (sessionId !== undefined && deps.ensureSessionSubscriptions !== undefined) {
              yield* deps.ensureSessionSubscriptions(sessionId, yield* projectForArgs(args))
            }
            yield* Effect.sync(() => narrowSet.remove(intent)).pipe(
              Effect.andThen(adapter.inbox.unsubscribe(intentToTarget(intent))),
            )
            // Persist is id-blind (see subscribe): it polls the shared deferred
            // internally and writes only when the id is already known.
            if (deps.persistSessionSubscriptions !== undefined) {
              yield* deps.persistSessionSubscriptions
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
      name: 'resolve_thread',
      description:
        "Mark a thread resolved (channel_name + thread, like read_thread). Idempotent — resolving an already-resolved thread is a no-op. Resolution is a status kept separate from the thread name; read it back as a message's thread.resolved via read_thread / read_channel. Use unresolve_thread to clear it.",
      inputSchema: {
        type: 'object',
        properties: {
          channel_name: { type: 'string', description: 'Channel the thread lives in' },
          thread: { type: 'string', description: 'Thread / topic name within the channel' },
        },
        required: ['channel_name', 'thread'],
        additionalProperties: false,
      },
      handler: async (args) => {
        await runEdge(
          Effect.gen(function* () {
            const parsed = yield* Schema.decodeUnknown(ThreadResolutionArgs)(args)
            yield* adapter.publisher.resolveThread(
              yield* decodeChannelName(parsed.channel_name),
              yield* decodeThreadName(parsed.thread),
            )
          }),
        )
        return {}
      },
    },
    {
      name: 'unresolve_thread',
      description:
        "Clear a thread's resolved status (channel_name + thread, like read_thread). Idempotent — unresolving a thread that is not resolved is a no-op. The inverse of resolve_thread.",
      inputSchema: {
        type: 'object',
        properties: {
          channel_name: { type: 'string', description: 'Channel the thread lives in' },
          thread: { type: 'string', description: 'Thread / topic name within the channel' },
        },
        required: ['channel_name', 'thread'],
        additionalProperties: false,
      },
      handler: async (args) => {
        await runEdge(
          Effect.gen(function* () {
            const parsed = yield* Schema.decodeUnknown(ThreadResolutionArgs)(args)
            yield* adapter.publisher.unresolveThread(
              yield* decodeChannelName(parsed.channel_name),
              yield* decodeThreadName(parsed.thread),
            )
          }),
        )
        return {}
      },
    },
    {
      name: 'get_channel_description',
      description:
        "Read a channel's standing description — the short statement of what the channel is for, which a project keeps as its charter. Returns {description}, null when nobody has set one. Distinct from the messages in the channel: it is channel-level state, and read_channel will not show it.",
      inputSchema: {
        type: 'object',
        properties: {
          channel_name: { type: 'string', description: 'Channel to read the description of' },
        },
        required: ['channel_name'],
        additionalProperties: false,
      },
      handler: async (args) => {
        const description = await runEdge(
          Effect.gen(function* () {
            const parsed = yield* Schema.decodeUnknown(ChannelDescriptionArgs)(args)
            return yield* adapter.directory.channelDescription(
              yield* decodeChannelName(parsed.channel_name),
            )
          }),
        )
        return { description: Option.getOrNull(description) }
      },
    },
    {
      name: 'set_channel_description',
      description:
        "Set a channel's standing description, replacing whatever it says now. Pass an empty description to clear it. Idempotent — writing the text already there changes nothing. What you write is read back verbatim: a description the substrate cannot store as given (too long, or spanning multiple lines) is refused with an error saying so, never silently trimmed or reflowed. Fails with a clear error when the bound identity lacks permission to edit the channel.",
      inputSchema: {
        type: 'object',
        properties: {
          channel_name: { type: 'string', description: 'Channel to describe' },
          description: {
            type: 'string',
            description: "The channel's new description; empty string clears it",
          },
        },
        required: ['channel_name', 'description'],
        additionalProperties: false,
      },
      handler: async (args) => {
        await runEdge(
          Effect.gen(function* () {
            const parsed = yield* Schema.decodeUnknown(SetChannelDescriptionArgs)(args)
            // An empty string is how a caller clears a description over a wire
            // that has no null — the port models "undescribed" as Option.none.
            const description =
              parsed.description.length === 0
                ? Option.none<ChannelDescription>()
                : Option.some(yield* decodeChannelDescription(parsed.description))
            yield* adapter.publisher.setChannelDescription(
              yield* decodeChannelName(parsed.channel_name),
              description,
            )
          }),
        )
        return {}
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
            if (cached !== undefined) return cached.permalink
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
        'Download a Zulip user upload by its /user_uploads/... path (visible in message bodies). Writes the file into a fresh temp directory and returns {file_path, content_type, size}. The temp directory is created under the operator-set COMMY_DOWNLOAD_DIR when configured (so the file lands in a directory you can Read), otherwise under $TMPDIR. Use the Read tool on the returned file_path to view images.',
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
  // A capability the substrate reports as switched off is withheld rather
  // than advertised-and-refused. Only the realm-wide switch gates: the
  // edit-window and original-sender walls are per-message, so a realm with a
  // 60-second window still has a usable `edit_message`.
  const availableTools =
    deps.canEditMessages === false
      ? coreTools.filter((def) => def.name !== 'edit_message')
      : coreTools
  return optionalTools.length === 0 ? availableTools : [...availableTools, ...optionalTools]
}

export const registerTools = (server: Server, deps: RegisterToolsDeps): ToolsCache => {
  const cache = createCache()
  // The handlers below are registered once and live for the connection, so
  // they read the current list through this holder rather than closing over
  // the boot-time one. Both handlers read the same holder, which is what
  // keeps the listing and dispatch from disagreeing after a rebuild.
  const buildFor = (canEditMessages: boolean | undefined) => {
    const defs = buildToolDefs(
      canEditMessages === undefined ? deps : { ...deps, canEditMessages },
      cache,
    )
    return { defs, byName: new Map(defs.map((def) => [def.name, def])) }
  }
  let registry = buildFor(deps.canEditMessages)

  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: registry.defs.map((def) => ({
      name: def.name,
      description: def.description,
      inputSchema: def.inputSchema,
    })),
  }))

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const def = registry.byName.get(request.params.name)
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
    setEditingAvailable: (available) => {
      registry = buildFor(available)
    },
  }
}
