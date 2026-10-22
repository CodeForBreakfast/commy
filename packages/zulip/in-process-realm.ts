/**
 * A stateful Zulip realm behind the `HttpClient` port. Where
 * `stub-http-client.ts` answers each request from a canned table, this keeps
 * the state the Zulip adapter's calls share — a post reads back, a reaction
 * shows on its message, an event reaches a registered queue — so the adapter
 * can run end to end with no socket and no live realm.
 *
 * Each request is authenticated by its Basic credentials, so the realm knows
 * which user acts, and delivery follows Zulip's rules for who receives what. A
 * request the realm does not model is refused rather than answered with a
 * guess.
 *
 * Test-only — never imported by production code.
 */

import { webRequestOf } from '@commy/testing/stub-http-client'
import { HttpClient, HttpClientResponse } from '@effect/platform'
import {
  Array as Arr,
  Clock,
  Data,
  Duration,
  Effect,
  Either,
  Encoding,
  HashMap,
  HashSet,
  Option,
  Order,
  Predicate,
  Ref,
  Schema,
  Stream,
  SubscriptionRef,
} from 'effect'
import type {
  ApiKey as ApiKeyType,
  BotEmail as BotEmailType,
  RealmUrl as RealmUrlType,
} from './http.ts'
import { ApiKey, BotEmail, RealmUrl } from './http.ts'
import { RESOLVED_TOPIC_PREFIX } from './resolved-topic.ts'

/** Zulip's `Stream.MAX_DESCRIPTION_LENGTH`. */
export const CHANNEL_DESCRIPTION_LIMIT = 1024

/** Zulip's default `max_message_length`, which `/register` reports as realm state. */
export const MESSAGE_LENGTH_LIMIT = 10_000
/** Zulip's `MAX_TOPIC_NAME_LENGTH`. A longer topic is stored cut short with an ellipsis. */
export const TOPIC_LIMIT = 60
const ELLIPSIS = '...'

const truncateTopic = (topic: string): string =>
  topic.length > TOPIC_LIMIT ? `${topic.slice(0, TOPIC_LIMIT - ELLIPSIS.length)}${ELLIPSIS}` : topic

export type PresenceStatus = 'active' | 'idle' | 'offline'

export interface InProcessRealm {
  /** Reaches this realm. Provide it as the adapter's `HttpClient`. */
  readonly client: HttpClient.HttpClient
  readonly url: RealmUrlType
  /** An organization administrator, for the adapter's minter credentials. */
  readonly minter: { readonly email: BotEmailType; readonly apiKey: ApiKeyType }
  /** A channel nobody is subscribed to, or the one already named so. */
  readonly addChannel: (
    name: string,
  ) => Effect.Effect<{ readonly streamId: number; readonly name: string }>
  /** An active member of the realm, returning its user id. */
  readonly addUser: (fullName: string, kind: 'human' | 'bot') => Effect.Effect<number>
  /** Deactivate the member with this user id, as an administrator would. */
  readonly deactivateUser: (userId: number) => Effect.Effect<void>
  readonly setPresence: (userId: number, status: PresenceStatus) => Effect.Effect<void>
}

const REALM_HOST = 'zulip.example.com'

/**
 * How long an idle `GET /events` holds before it answers with a heartbeat.
 * Zulip holds for about a minute; the realm answers sooner so an idle poll
 * costs a test nothing.
 */
const HEARTBEAT_AFTER = Duration.seconds(10)

/** Zulip's wildcards, matched case-sensitively as `zerver/lib/mention.py` does. */
const CHANNEL_WILDCARDS: ReadonlySet<string> = new Set(['all', 'everyone', 'stream', 'channel'])
const TOPIC_WILDCARD = 'topic'

type UserKind = 'human' | 'bot' | 'notification-bot'

interface RealmUser {
  readonly userId: number
  readonly email: string
  readonly fullName: string
  readonly kind: UserKind
  readonly isAdmin: boolean
  readonly isActive: boolean
  readonly apiKey: string
  readonly ownerId: Option.Option<number>
}

interface RealmChannel {
  readonly streamId: number
  readonly name: string
  readonly description: string
  readonly subscribers: HashSet.HashSet<number>
}

interface RealmReaction {
  readonly userId: number
  readonly emojiName: string
}

interface RealmMessage {
  readonly id: number
  readonly senderId: number
  readonly streamId: number
  readonly topic: string
  readonly content: string
  /** Rendered when the message is sent or edited, and stored, as Zulip does. */
  readonly rendered: string
  readonly mentioned: HashSet.HashSet<number>
  readonly timestamp: number
  readonly reactions: ReadonlyArray<RealmReaction>
  /** Zulip's `UserMessage` rows: who received the message, or reacted to it. */
  readonly receivedBy: HashSet.HashSet<number>
}

type QueuedEvent = { readonly id: number; readonly type: string } & Readonly<
  Record<string, unknown>
>

interface EventQueue {
  readonly queueId: string
  readonly ownerId: number
  /** None subscribes the queue to every event type. */
  readonly eventTypes: Option.Option<ReadonlySet<string>>
  readonly lastIssued: number
  readonly pending: ReadonlyArray<QueuedEvent>
}

interface Upload {
  readonly data: Uint8Array
  readonly contentType: string
}

interface RealmState {
  readonly users: HashMap.HashMap<number, RealmUser>
  readonly channels: HashMap.HashMap<number, RealmChannel>
  readonly messages: HashMap.HashMap<number, RealmMessage>
  readonly queues: HashMap.HashMap<string, EventQueue>
  readonly uploads: HashMap.HashMap<string, Upload>
  readonly presence: HashMap.HashMap<number, PresenceStatus>
}

class RealmRefusal extends Data.TaggedError('RealmRefusal')<{
  readonly status: number
  readonly code: string
  readonly msg: string
}> {}

const refusal = (msg: string, code = 'BAD_REQUEST', status = 400): RealmRefusal =>
  new RealmRefusal({ status, code, msg })

const refuse = (msg: string, code?: string, status?: number): Effect.Effect<never, RealmRefusal> =>
  Effect.fail(refusal(msg, code, status))

const ok = (fields: Readonly<Record<string, unknown>> = {}): Response =>
  Response.json({ result: 'success', msg: '', ...fields })

const refusalResponse = (r: RealmRefusal): Response =>
  Response.json({ result: 'error', msg: r.msg, code: r.code }, { status: r.status })

const sameName = (a: string, b: string): boolean => a.toLowerCase() === b.toLowerCase()

const isBot = (u: RealmUser): boolean => u.kind !== 'human'

const shortNameOf = (fullName: string): string =>
  fullName
    .toLowerCase()
    .replace(/[^a-z0-9._-]/g, '-')
    .replace(/^-+|-+$/g, '') || 'user'

const findUser = (state: RealmState, p: (u: RealmUser) => boolean): Option.Option<RealmUser> =>
  Arr.findFirst(HashMap.values(state.users), p)

const findChannel = (
  state: RealmState,
  p: (c: RealmChannel) => boolean,
): Option.Option<RealmChannel> => Arr.findFirst(HashMap.values(state.channels), p)

const channelNamed = (state: RealmState, name: string): Option.Option<RealmChannel> =>
  findChannel(state, (c) => sameName(c.name, name))

const activeIds = (state: RealmState, ids: HashSet.HashSet<number>): HashSet.HashSet<number> =>
  HashSet.filter(ids, (id) => HashMap.get(state.users, id).pipe(Option.exists((u) => u.isActive)))

const userPayload = (u: RealmUser) => ({
  user_id: u.userId,
  email: u.email,
  full_name: u.fullName,
  is_bot: isBot(u),
  is_active: u.isActive,
  is_admin: u.isAdmin,
  bot_type: u.kind === 'bot' ? 1 : null,
  bot_owner_id: Option.getOrNull(u.ownerId),
})

const escapeHtml = (text: string): string =>
  text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')

const CODE = /```[\s\S]*?```|`[^`\n]*`/g
const MENTION = /@(_?)\*\*([^*]+)\*\*/g
const DISAMBIGUATED = /^(.*)\|(\d+)$/

interface Rendering {
  readonly html: string
  /** Users the rendering notifies by name — the source of Zulip's `mentioned` flag. */
  readonly mentioned: HashSet.HashSet<number>
}

const mentionedUser = (state: RealmState, token: string): Option.Option<RealmUser> => {
  const disambiguated = DISAMBIGUATED.exec(token)
  if (disambiguated !== null) {
    return HashMap.get(state.users, Number(disambiguated[2])).pipe(
      Option.filter((u) => disambiguated[1] === '' || u.fullName === disambiguated[1]),
    )
  }
  // Zulip keys its lookup on the lowercased name from an unordered query, so
  // the last row wins; the highest id stands in for it here.
  const named = Arr.filter(Arr.fromIterable(HashMap.values(state.users)), (u) =>
    sameName(u.fullName, token),
  )
  return Arr.last(
    Arr.sort(
      named,
      Order.mapInput(Order.number, (u: RealmUser) => u.userId),
    ),
  )
}

/**
 * Zulip's rendering of a body, as far as the adapter reads it: mention spans
 * outside code, each one classed the way `zerver/lib/markdown` classes it. A
 * deactivated user's mention renders silent whatever its author wrote.
 */
const render = (state: RealmState, content: string): Rendering => {
  let mentioned = HashSet.empty<number>()
  const renderMention = (whole: string, silent: string, token: string): string => {
    if (silent === '' && CHANNEL_WILDCARDS.has(token)) {
      return `<span class="user-mention channel-wildcard-mention" data-user-id="*">@${escapeHtml(token)}</span>`
    }
    if (silent === '' && token === TOPIC_WILDCARD) {
      return `<span class="topic-mention">@${escapeHtml(token)}</span>`
    }
    return Option.match(mentionedUser(state, token), {
      onNone: () => escapeHtml(whole),
      onSome: (u) => {
        const name = escapeHtml(u.fullName)
        if (silent !== '' || !u.isActive) {
          return `<span class="user-mention silent" data-user-id="${u.userId}">${name}</span>`
        }
        mentioned = HashSet.add(mentioned, u.userId)
        return `<span class="user-mention" data-user-id="${u.userId}">@${name}</span>`
      },
    })
  }
  const renderText = (text: string): string => {
    let out = ''
    let at = 0
    for (const match of text.matchAll(MENTION)) {
      out += escapeHtml(text.slice(at, match.index))
      out += renderMention(match[0], match[1] ?? '', match[2] ?? '')
      at = match.index + match[0].length
    }
    return out + escapeHtml(text.slice(at))
  }
  let html = ''
  let at = 0
  for (const match of content.matchAll(CODE)) {
    html += renderText(content.slice(at, match.index))
    html += `<code>${escapeHtml(match[0].replace(/^`+|`+$/g, ''))}</code>`
    at = match.index + match[0].length
  }
  html += renderText(content.slice(at))
  return { html: `<p>${html}</p>`, mentioned }
}

const wants = (queue: EventQueue, type: string): boolean =>
  Option.match(queue.eventTypes, { onNone: () => true, onSome: (types) => types.has(type) })

const enqueue = (
  queue: EventQueue,
  event: Readonly<Record<string, unknown>> & { type: string },
) => {
  const id = queue.lastIssued + 1
  return { ...queue, lastIssued: id, pending: [...queue.pending, { ...event, id }] }
}

/** Hand each queue whose owner is in `audience` the event built for that owner. */
const deliver = (
  state: RealmState,
  audience: HashSet.HashSet<number>,
  type: string,
  eventFor: (ownerId: number) => Readonly<Record<string, unknown>>,
): RealmState => ({
  ...state,
  queues: HashMap.map(state.queues, (q) =>
    HashSet.has(audience, q.ownerId) && wants(q, type)
      ? enqueue(q, { ...eventFor(q.ownerId), type })
      : q,
  ),
})

const reactionPayload = (r: RealmReaction) => ({
  user_id: r.userId,
  emoji_name: r.emojiName,
  emoji_code: r.emojiName,
  reaction_type: 'unicode_emoji',
})

const messagePayload = (state: RealmState, m: RealmMessage, markdown: boolean) => {
  const sender = HashMap.unsafeGet(state.users, m.senderId)
  const channel = HashMap.unsafeGet(state.channels, m.streamId)
  return {
    id: m.id,
    type: 'stream',
    sender_id: sender.userId,
    sender_full_name: sender.fullName,
    sender_email: sender.email,
    stream_id: channel.streamId,
    display_recipient: channel.name,
    subject: m.topic,
    content: markdown ? m.rendered : m.content,
    content_type: markdown ? 'text/html' : 'text/x-markdown',
    timestamp: m.timestamp,
    reactions: m.reactions.map(reactionPayload),
  }
}

const flagsFor = (m: RealmMessage, userId: number): ReadonlyArray<string> => [
  ...(userId === m.senderId ? ['read'] : []),
  ...(HashSet.has(m.mentioned, userId) ? ['mentioned'] : []),
  ...(HashSet.has(m.receivedBy, userId) ? [] : ['historical']),
]

/**
 * Store a channel message and deliver it. Its recipients are the channel's
 * active subscribers and any active default bot it mentions — the rule in
 * `zerver/actions/message_send.py`, which gives a mentioned bot the message
 * even when the bot is not subscribed.
 */
const storeMessage = (
  state: RealmState,
  message: Omit<RealmMessage, 'rendered' | 'mentioned' | 'receivedBy' | 'reactions'>,
): RealmState => {
  const rendering = render(state, message.content)
  const channel = HashMap.unsafeGet(state.channels, message.streamId)
  const mentionedBots = HashSet.filter(rendering.mentioned, (id) =>
    HashMap.get(state.users, id).pipe(Option.exists((u) => u.kind === 'bot')),
  )
  const stored: RealmMessage = {
    ...message,
    rendered: rendering.html,
    mentioned: rendering.mentioned,
    reactions: [],
    receivedBy: activeIds(state, HashSet.union(channel.subscribers, mentionedBots)),
  }
  const next = { ...state, messages: HashMap.set(state.messages, stored.id, stored) }
  return deliver(next, stored.receivedBy, 'message', (ownerId) => ({
    message: messagePayload(next, stored, false),
    flags: flagsFor(stored, ownerId),
  }))
}

/**
 * Who hears about a reaction on a message in a public channel: its current
 * active subscribers, and everyone holding a `UserMessage` row for it
 * (`event_recipient_ids_for_action_on_messages`).
 */
const reactionAudience = (state: RealmState, m: RealmMessage): HashSet.HashSet<number> =>
  activeIds(
    state,
    HashSet.union(m.receivedBy, HashMap.unsafeGet(state.channels, m.streamId).subscribers),
  )

interface UploadedFile {
  readonly filename: string
  readonly contentType: string
  readonly data: Uint8Array
}

/** The first file in a multipart body, which is the one Zulip stores. */
const firstFile = (web: Request): Effect.Effect<Option.Option<UploadedFile>> =>
  Effect.promise(async () => {
    for (const value of (await web.formData()).values()) {
      if (!Predicate.isString(value)) {
        return Option.some({
          filename: value.name,
          contentType: value.type,
          data: new Uint8Array(await value.arrayBuffer()),
        })
      }
    }
    return Option.none()
  })

interface Call {
  readonly caller: RealmUser
  readonly params: URLSearchParams
  /** The path's captured groups. */
  readonly path: ReadonlyArray<string>
  readonly file: Option.Option<UploadedFile>
}

type Handler = (call: Call) => Effect.Effect<Response, RealmRefusal>

interface Route {
  readonly method: string
  readonly path: RegExp
  readonly handle: Handler
}

const param = (call: Call, name: string): Option.Option<string> =>
  Option.fromNullable(call.params.get(name))

const requiredParam = (call: Call, name: string): Effect.Effect<string, RealmRefusal> =>
  Option.match(param(call, name), {
    onNone: () => refuse(`Missing '${name}' argument`, 'REQUEST_VARIABLE_MISSING'),
    onSome: Effect.succeed,
  })

const intParam = (call: Call, name: string): Effect.Effect<number, RealmRefusal> =>
  requiredParam(call, name).pipe(
    Effect.flatMap((raw) =>
      /^-?\d+$/.test(raw) ? Effect.succeed(Number(raw)) : refuse(`Invalid ${name}`),
    ),
  )

const jsonParam = <A, I>(
  call: Call,
  name: string,
  schema: Schema.Schema<A, I>,
): Effect.Effect<Option.Option<A>, RealmRefusal> =>
  Option.match(param(call, name), {
    onNone: () => Effect.succeedNone,
    onSome: (raw) =>
      Schema.decodeUnknown(Schema.parseJson(schema))(raw).pipe(
        Effect.asSome,
        Effect.mapError(() => refusal(`Invalid ${name}`)),
      ),
  })

const requiredJsonParam = <A, I>(
  call: Call,
  name: string,
  schema: Schema.Schema<A, I>,
): Effect.Effect<A, RealmRefusal> =>
  jsonParam(call, name, schema).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => refuse(`Missing '${name}' argument`, 'REQUEST_VARIABLE_MISSING'),
        onSome: Effect.succeed,
      }),
    ),
  )

const pathId = (call: Call): number => Number(call.path[0])

const narrowTermSchema = Schema.Struct({
  operator: Schema.String,
  operand: Schema.Union(Schema.String, Schema.Number),
  negated: Schema.optional(Schema.Boolean),
})

type NarrowTerm = Schema.Schema.Type<typeof narrowTermSchema>

/**
 * A narrow as a predicate over stored messages. The operators are the ones
 * the adapter sends; any other is refused rather than matched loosely.
 */
const compileNarrow = (
  state: RealmState,
  terms: ReadonlyArray<NarrowTerm>,
): Either.Either<(m: RealmMessage) => boolean, RealmRefusal> =>
  Either.all(
    terms.map((term): Either.Either<(m: RealmMessage) => boolean, RealmRefusal> => {
      const operand = String(term.operand)
      const positive = ((): Either.Either<(m: RealmMessage) => boolean, RealmRefusal> => {
        switch (term.operator) {
          case 'channel':
          case 'stream': {
            const channel = Predicate.isNumber(term.operand)
              ? HashMap.get(state.channels, term.operand)
              : channelNamed(state, operand)
            return Option.match(channel, {
              onNone: () =>
                Either.left(refusal(`Invalid narrow operator: unknown channel`, 'BAD_NARROW')),
              onSome: (c) => Either.right((m: RealmMessage) => m.streamId === c.streamId),
            })
          }
          case 'topic':
            return Either.right((m: RealmMessage) => sameName(m.topic, operand))
          case 'sender':
            return Either.right((m: RealmMessage) =>
              Predicate.isNumber(term.operand)
                ? m.senderId === term.operand
                : HashMap.get(state.users, m.senderId).pipe(
                    Option.exists((u) => sameName(u.email, operand)),
                  ),
            )
          case 'is':
            return operand === 'dm' || operand === 'private'
              ? Either.right(() => false)
              : Either.left(refusal(`Invalid narrow operand: is:${operand}`, 'BAD_NARROW'))
          default:
            return Either.left(refusal(`Invalid narrow operator: ${term.operator}`, 'BAD_NARROW'))
        }
      })()
      return Either.map(positive, (p) => (term.negated === true ? (m: RealmMessage) => !p(m) : p))
    }),
  ).pipe(Either.map((ps) => (m: RealmMessage) => ps.every((p) => p(m))))

const byId = Order.mapInput(Order.number, (m: RealmMessage) => m.id)

/**
 * `GET /messages`: the `num_before` messages before the anchor, the anchor
 * itself, then `num_after` after it, always in ascending id order as Zulip
 * returns them. `anchor=newest` reads the newest `num_before`.
 */
const getMessages =
  (stateRef: SubscriptionRef.SubscriptionRef<RealmState>): Handler =>
  (call) =>
    Effect.gen(function* () {
      const state = yield* SubscriptionRef.get(stateRef)
      const terms = yield* jsonParam(call, 'narrow', Schema.Array(narrowTermSchema))
      const matches = yield* compileNarrow(
        state,
        Option.getOrElse(terms, () => []),
      )
      const rawAnchor = yield* requiredParam(call, 'anchor')
      const anchor =
        rawAnchor === 'newest'
          ? Number.POSITIVE_INFINITY
          : /^\d+$/.test(rawAnchor)
            ? Number(rawAnchor)
            : yield* refuse(`Invalid anchor`)
      const numBefore = yield* intParam(call, 'num_before')
      const numAfter = yield* intParam(call, 'num_after')
      const markdown = Option.getOrElse(param(call, 'apply_markdown'), () => 'true') === 'true'
      const matched = Arr.sort(
        Arr.filter(Arr.fromIterable(HashMap.values(state.messages)), matches),
        byId,
      )
      const before = matched.filter((m) => m.id < anchor)
      const at = matched.filter((m) => m.id === anchor)
      const after = matched.filter((m) => m.id > anchor)
      const from = Math.max(0, before.length - numBefore)
      const page = [...before.slice(from), ...at, ...after.slice(0, numAfter)]
      return ok({
        messages: page.map((m) => ({
          ...messagePayload(state, m, markdown),
          flags: flagsFor(m, call.caller.userId),
        })),
        found_anchor: at.length > 0,
        found_oldest: from === 0,
        found_newest: numAfter >= after.length,
        history_limited: false,
      })
    })

const subscriptionsSchema = Schema.Array(
  Schema.Struct({ name: Schema.String, description: Schema.optional(Schema.String) }),
)

const eventTypesSchema = Schema.Array(Schema.String)

const isResolved = (topic: string): boolean => topic.startsWith(RESOLVED_TOPIC_PREFIX)
const unresolvedName = (topic: string): string =>
  isResolved(topic) ? topic.slice(RESOLVED_TOPIC_PREFIX.length) : topic

// This follows Zulip's sanitize_name. Unicode word characters survive, so a
// stored path can hold characters that a URL percent-encodes.
const sanitizeUploadName = (name: string): string => {
  const value = name
    .normalize('NFKC')
    .replace(/[^\p{L}\p{N}_\s.-]/gu, '')
    .trim()
    .replace(/[-\s]+/gu, '-')
  return value === '' || value === '.' || value === '..' ? 'uploaded-file' : value
}

export const makeInProcessRealm: Effect.Effect<InProcessRealm> = Effect.gen(function* () {
  const serial = yield* Ref.make(0)
  const nextId = Ref.updateAndGet(serial, (n) => n + 1)
  const newKey = nextId.pipe(Effect.map((n) => `key${n}`))
  const now = Clock.currentTimeMillis.pipe(Effect.map((ms) => Math.floor(ms / 1000)))

  const stateRef = yield* SubscriptionRef.make<RealmState>({
    users: HashMap.empty(),
    channels: HashMap.empty(),
    messages: HashMap.empty(),
    queues: HashMap.empty(),
    uploads: HashMap.empty(),
    presence: HashMap.empty(),
  })

  const newUser = (
    fullName: string,
    kind: UserKind,
    options: { readonly email: string; readonly isAdmin: boolean; readonly ownerId?: number },
  ): Effect.Effect<RealmUser> =>
    Effect.all([nextId, newKey]).pipe(
      Effect.map(
        ([userId, apiKey]): RealmUser => ({
          userId,
          email: options.email,
          fullName,
          kind,
          isAdmin: options.isAdmin,
          isActive: true,
          apiKey,
          ownerId: Option.fromNullable(options.ownerId),
        }),
      ),
    )

  const putUser = (state: RealmState, user: RealmUser): RealmState => ({
    ...state,
    users: HashMap.set(state.users, user.userId, user),
  })

  const deactivate = (state: RealmState, user: RealmUser): RealmState => ({
    ...putUser(state, { ...user, isActive: false }),
    // Deactivation drops the user's event queues.
    queues: HashMap.filter(state.queues, (q) => q.ownerId !== user.userId),
  })

  const createUser = (
    fullName: string,
    kind: UserKind,
    options: { readonly email: string; readonly isAdmin: boolean; readonly ownerId?: number },
  ): Effect.Effect<RealmUser> =>
    newUser(fullName, kind, options).pipe(
      Effect.tap((user) => SubscriptionRef.update(stateRef, (s) => putUser(s, user))),
    )

  const minter = yield* createUser('Minter', 'human', {
    email: `minter@${REALM_HOST}`,
    isAdmin: true,
  })
  const notificationBot = yield* createUser('Notification Bot', 'notification-bot', {
    email: 'notification-bot@zulip.com',
    isAdmin: false,
  })

  const modify = <A>(
    f: (state: RealmState) => Effect.Effect<readonly [A, RealmState], RealmRefusal>,
  ): Effect.Effect<A, RealmRefusal> => SubscriptionRef.modifyEffect(stateRef, f)

  const update = (
    f: (state: RealmState) => Effect.Effect<RealmState, RealmRefusal>,
  ): Effect.Effect<Response, RealmRefusal> =>
    modify((s) => f(s).pipe(Effect.map((next) => [ok(), next] as const)))

  const requireMessage = (
    state: RealmState,
    id: number,
  ): Effect.Effect<RealmMessage, RealmRefusal> =>
    Option.match(HashMap.get(state.messages, id), {
      onNone: () => refuse('Invalid message(s)'),
      onSome: Effect.succeed,
    })

  const requireChannel = (
    state: RealmState,
    id: number,
  ): Effect.Effect<RealmChannel, RealmRefusal> =>
    Option.match(HashMap.get(state.channels, id), {
      onNone: () => refuse('Invalid channel ID'),
      onSome: Effect.succeed,
    })

  /** Zulip's `access_bot_by_id`: the bot's owner or an administrator may act on it. */
  const accessBot = (
    state: RealmState,
    caller: RealmUser,
    id: number,
  ): Effect.Effect<RealmUser, RealmRefusal> =>
    Option.match(HashMap.get(state.users, id).pipe(Option.filter((u) => u.kind === 'bot')), {
      onNone: () => refuse('No such bot'),
      onSome: (bot) =>
        caller.isAdmin || Option.contains(bot.ownerId, caller.userId)
          ? Effect.succeed(bot)
          : refuse('Insufficient permission'),
    })

  const putChannel = (state: RealmState, channel: RealmChannel): RealmState => ({
    ...state,
    channels: HashMap.set(state.channels, channel.streamId, channel),
  })

  const putMessage = (state: RealmState, message: RealmMessage): RealmState => ({
    ...state,
    messages: HashMap.set(state.messages, message.id, message),
  })

  const ownQueue = (
    state: RealmState,
    caller: RealmUser,
    queueId: string,
  ): Effect.Effect<EventQueue, RealmRefusal> =>
    Option.match(
      HashMap.get(state.queues, queueId).pipe(Option.filter((q) => q.ownerId === caller.userId)),
      {
        onNone: () => refuse(`Bad event queue ID: ${queueId}`, 'BAD_EVENT_QUEUE_ID'),
        onSome: Effect.succeed,
      },
    )

  /** Zulip's notice in a topic whose resolution just changed. */
  const resolutionNotice = (
    state: RealmState,
    caller: RealmUser,
    message: RealmMessage,
    topic: string,
    resolved: boolean,
  ): Effect.Effect<RealmState> =>
    Effect.all([nextId, now]).pipe(
      Effect.map(([id, timestamp]) =>
        storeMessage(state, {
          id,
          senderId: notificationBot.userId,
          streamId: message.streamId,
          topic,
          content: `@_**${caller.fullName}|${caller.userId}** has marked this topic as ${resolved ? 'resolved' : 'unresolved'}.`,
          timestamp,
        }),
      ),
    )

  const editMessage: Handler = (call) =>
    update((state) =>
      Effect.gen(function* () {
        const message = yield* requireMessage(state, pathId(call))
        const content = param(call, 'content')
        const topic = param(call, 'topic')
        if (Option.isSome(content) && message.senderId !== call.caller.userId) {
          return yield* refuse("You don't have permission to edit this message")
        }
        const edited = Option.match(content, {
          onNone: () => state,
          onSome: (body) => {
            const rendering = render(state, body)
            return putMessage(state, {
              ...message,
              content: body,
              rendered: rendering.html,
              mentioned: rendering.mentioned,
            })
          },
        })
        if (Option.isNone(topic)) return edited
        const newTopic = topic.value
        const mode = Option.getOrElse(param(call, 'propagate_mode'), () => 'change_one')
        const moves = (m: RealmMessage): boolean =>
          m.id === message.id ||
          (m.streamId === message.streamId &&
            sameName(m.topic, message.topic) &&
            (mode === 'change_all' || (mode === 'change_later' && m.id > message.id)))
        const renamed: RealmState = {
          ...edited,
          messages: HashMap.map(edited.messages, (m) => (moves(m) ? { ...m, topic: newTopic } : m)),
        }
        const resolving = isResolved(newTopic) && sameName(unresolvedName(newTopic), message.topic)
        const unresolving =
          isResolved(message.topic) && sameName(unresolvedName(message.topic), newTopic)
        return resolving || unresolving
          ? yield* resolutionNotice(renamed, call.caller, message, newTopic, resolving)
          : renamed
      }),
    )

  const react =
    (op: 'add' | 'remove'): Handler =>
    (call) =>
      update((state) =>
        Effect.gen(function* () {
          const message = yield* requireMessage(state, pathId(call))
          const emojiName = yield* requiredParam(call, 'emoji_name')
          const reaction: RealmReaction = { userId: call.caller.userId, emojiName }
          const held = message.reactions.some(
            (r) => r.userId === reaction.userId && r.emojiName === emojiName,
          )
          if (op === 'add' && held) {
            return yield* refuse('Reaction already exists.', 'REACTION_ALREADY_EXISTS')
          }
          if (op === 'remove' && !held) {
            return yield* refuse("Reaction doesn't exist.", 'REACTION_DOES_NOT_EXIST')
          }
          const reacted: RealmMessage = {
            ...message,
            reactions:
              op === 'add'
                ? [...message.reactions, reaction]
                : message.reactions.filter(
                    (r) => !(r.userId === reaction.userId && r.emojiName === emojiName),
                  ),
            // Reacting gives a non-recipient a historical UserMessage row.
            receivedBy: HashSet.add(message.receivedBy, reaction.userId),
          }
          const next = putMessage(state, reacted)
          return deliver(next, reactionAudience(next, reacted), 'reaction', () => ({
            op,
            message_id: reacted.id,
            ...reactionPayload(reaction),
          }))
        }),
      )

  const pollEvents: Handler = (call) =>
    Effect.gen(function* () {
      const queueId = yield* requiredParam(call, 'queue_id')
      const lastEventId = yield* intParam(call, 'last_event_id')
      const caller = call.caller
      // Acknowledge what the client has seen, as Zulip prunes on each poll.
      yield* modify((s) =>
        ownQueue(s, caller, queueId).pipe(
          Effect.map((q) => {
            const pruned = { ...q, pending: q.pending.filter((e) => e.id > lastEventId) }
            return [undefined, { ...s, queues: HashMap.set(s.queues, queueId, pruned) }] as const
          }),
        ),
      )
      const ready = (
        s: RealmState,
      ): Option.Option<Either.Either<ReadonlyArray<QueuedEvent>, RealmRefusal>> =>
        Option.match(
          HashMap.get(s.queues, queueId).pipe(Option.filter((q) => q.ownerId === caller.userId)),
          {
            onNone: () =>
              Option.some(
                Either.left(refusal(`Bad event queue ID: ${queueId}`, 'BAD_EVENT_QUEUE_ID')),
              ),
            onSome: (q) => {
              const events = q.pending.filter((e) => e.id > lastEventId)
              return Arr.isNonEmptyReadonlyArray(events)
                ? Option.some(Either.right(events))
                : Option.none()
            },
          },
        )
      const heartbeat = modify((s) =>
        ownQueue(s, caller, queueId).pipe(
          Effect.map((q) => {
            const beaten = enqueue(q, { type: 'heartbeat' })
            return [
              beaten.pending.slice(-1),
              { ...s, queues: HashMap.set(s.queues, queueId, beaten) },
            ] as const
          }),
        ),
      )
      const waited = yield* stateRef.changes.pipe(
        Stream.filterMap(ready),
        Stream.runHead,
        Effect.timeoutOption(HEARTBEAT_AFTER),
        Effect.map(Option.flatten),
      )
      const events = yield* Option.match(waited, {
        onNone: () => heartbeat,
        onSome: Either.match({ onLeft: Effect.fail, onRight: Effect.succeed }),
      })
      return ok({ events })
    })

  const routes: ReadonlyArray<Route> = [
    {
      method: 'GET',
      path: /^\/api\/v1\/users$/,
      handle: (call) =>
        Effect.gen(function* () {
          const state = yield* SubscriptionRef.get(stateRef)
          const ids = yield* jsonParam(call, 'user_ids', Schema.Array(Schema.Int))
          const members = Arr.fromIterable(HashMap.values(state.users)).filter((u) =>
            Option.match(ids, {
              onNone: () => true,
              onSome: (wanted) => wanted.includes(u.userId),
            }),
          )
          return ok({ members: members.map(userPayload) })
        }),
    },
    {
      method: 'POST',
      path: /^\/api\/v1\/bots$/,
      handle: (call) =>
        Effect.gen(function* () {
          const fullName = yield* requiredParam(call, 'full_name')
          const shortName = yield* requiredParam(call, 'short_name')
          if (Option.getOrElse(param(call, 'bot_type'), () => '1') !== '1') {
            return yield* refuse('in-process realm models default bots only')
          }
          const email = `${shortName}-bot@${REALM_HOST}`
          const bot = yield* newUser(fullName, 'bot', {
            email,
            isAdmin: false,
            ownerId: call.caller.userId,
          })
          yield* modify((state) =>
            Option.isSome(findUser(state, (u) => sameName(u.email, email)))
              ? refuse('Username already in use')
              : Option.isSome(findUser(state, (u) => u.isActive && sameName(u.fullName, fullName)))
                ? refuse('Name is already in use!')
                : Effect.succeed([undefined, putUser(state, bot)] as const),
          )
          return ok({ user_id: bot.userId, api_key: bot.apiKey })
        }),
    },
    {
      method: 'POST',
      path: /^\/api\/v1\/bots\/(\d+)\/api_key\/regenerate$/,
      handle: (call) =>
        newKey.pipe(
          Effect.flatMap((apiKey) =>
            modify((state) =>
              accessBot(state, call.caller, pathId(call)).pipe(
                Effect.map(
                  (bot) => [ok({ api_key: apiKey }), putUser(state, { ...bot, apiKey })] as const,
                ),
              ),
            ),
          ),
        ),
    },
    {
      method: 'DELETE',
      path: /^\/api\/v1\/bots\/(\d+)$/,
      handle: (call) =>
        update((state) =>
          accessBot(state, call.caller, pathId(call)).pipe(
            Effect.map((bot) => deactivate(state, bot)),
          ),
        ),
    },
    {
      method: 'POST',
      path: /^\/api\/v1\/users\/(\d+)\/reactivate$/,
      handle: (call) =>
        update((state) =>
          Effect.gen(function* () {
            if (!call.caller.isAdmin) return yield* refuse('Must be an organization administrator')
            const user = yield* Option.match(HashMap.get(state.users, pathId(call)), {
              onNone: () => refuse('No such user'),
              onSome: Effect.succeed,
            })
            if (user.isActive) return yield* refuse('User is already active')
            return putUser(state, { ...user, isActive: true })
          }),
        ),
    },
    {
      method: 'GET',
      path: /^\/api\/v1\/users\/([^/]+)\/presence$/,
      handle: (call) =>
        Effect.gen(function* () {
          const state = yield* SubscriptionRef.get(stateRef)
          const key = call.path[0] ?? ''
          const user = yield* Option.match(
            /^\d+$/.test(key)
              ? HashMap.get(state.users, Number(key))
              : findUser(state, (u) => sameName(u.email, key)),
            { onNone: () => refuse('No such user'), onSome: Effect.succeed },
          )
          if (isBot(user)) return yield* refuse('Presence is not supported for bot users.')
          const status = yield* Option.match(HashMap.get(state.presence, user.userId), {
            onNone: () => refuse(`No presence data for ${user.email}`),
            onSome: Effect.succeed,
          })
          const timestamp = yield* now
          return ok({ presence: { aggregated: { status, timestamp } } })
        }),
    },
    {
      method: 'GET',
      path: /^\/api\/v1\/streams$/,
      handle: () =>
        SubscriptionRef.get(stateRef).pipe(
          Effect.map((state) =>
            ok({
              streams: Arr.fromIterable(HashMap.values(state.channels)).map((c) => ({
                stream_id: c.streamId,
                name: c.name,
                description: c.description,
                invite_only: false,
              })),
            }),
          ),
        ),
    },
    {
      method: 'GET',
      path: /^\/api\/v1\/streams\/(\d+)$/,
      handle: (call) =>
        SubscriptionRef.get(stateRef).pipe(
          Effect.flatMap((state) => requireChannel(state, pathId(call))),
          Effect.map((c) =>
            ok({ stream: { stream_id: c.streamId, name: c.name, description: c.description } }),
          ),
        ),
    },
    {
      method: 'PATCH',
      path: /^\/api\/v1\/streams\/(\d+)$/,
      handle: (call) =>
        update((state) =>
          Effect.gen(function* () {
            const channel = yield* requireChannel(state, pathId(call))
            const description = yield* Option.match(param(call, 'description'), {
              onNone: () => refuse('in-process realm models channel description updates only'),
              onSome: Effect.succeed,
            })
            if (description.length > CHANNEL_DESCRIPTION_LIMIT) {
              return yield* refuse(
                `description is too long (limit: ${CHANNEL_DESCRIPTION_LIMIT} characters)`,
              )
            }
            // Zulip stores a description on one line.
            return putChannel(state, { ...channel, description: description.replace(/\n/g, ' ') })
          }),
        ),
    },
    {
      method: 'GET',
      path: /^\/api\/v1\/users\/me\/subscriptions$/,
      handle: (call) =>
        SubscriptionRef.get(stateRef).pipe(
          Effect.map((state) =>
            ok({
              subscriptions: Arr.fromIterable(HashMap.values(state.channels))
                .filter((c) => HashSet.has(c.subscribers, call.caller.userId))
                .map((c) => ({ stream_id: c.streamId, name: c.name, description: c.description })),
            }),
          ),
        ),
    },
    {
      method: 'POST',
      path: /^\/api\/v1\/users\/me\/subscriptions$/,
      handle: (call) =>
        Effect.gen(function* () {
          if (Option.isSome(param(call, 'principals'))) {
            return yield* refuse('in-process realm models subscribing oneself only')
          }
          const wanted = yield* requiredJsonParam(call, 'subscriptions', subscriptionsSchema)
          const caller = call.caller.userId
          // Subscribing to a channel that does not exist creates it, as Zulip does.
          const created = yield* Effect.forEach(wanted, (w) =>
            nextId.pipe(Effect.map((streamId) => ({ ...w, streamId }))),
          )
          return yield* modify((state) => {
            let next = state
            const subscribed: string[] = []
            const already: string[] = []
            for (const w of created) {
              const channel = Option.getOrElse(channelNamed(next, w.name), () => ({
                streamId: w.streamId,
                name: w.name,
                description: w.description ?? '',
                subscribers: HashSet.empty<number>(),
              }))
              ;(HashSet.has(channel.subscribers, caller) ? already : subscribed).push(channel.name)
              next = putChannel(next, {
                ...channel,
                subscribers: HashSet.add(channel.subscribers, caller),
              })
            }
            return Effect.succeed([
              ok({
                subscribed: { [String(caller)]: subscribed },
                already_subscribed: { [String(caller)]: already },
              }),
              next,
            ] as const)
          })
        }),
    },
    {
      method: 'DELETE',
      path: /^\/api\/v1\/users\/me\/subscriptions$/,
      handle: (call) =>
        Effect.gen(function* () {
          const names = yield* requiredJsonParam(call, 'subscriptions', Schema.Array(Schema.String))
          const caller = call.caller.userId
          return yield* modify((state) =>
            Effect.gen(function* () {
              const channels = yield* Effect.forEach(names, (name) =>
                Option.match(channelNamed(state, name), {
                  onNone: () => refuse(`Channel(s) (${name}) do not exist`),
                  onSome: Effect.succeed,
                }),
              )
              const removed = channels.filter((c) => HashSet.has(c.subscribers, caller))
              const next = removed.reduce(
                (s, c) =>
                  putChannel(s, { ...c, subscribers: HashSet.remove(c.subscribers, caller) }),
                state,
              )
              return [
                ok({
                  removed: removed.map((c) => c.name),
                  not_removed: channels.filter((c) => !removed.includes(c)).map((c) => c.name),
                }),
                next,
              ] as const
            }),
          )
        }),
    },
    {
      method: 'POST',
      path: /^\/api\/v1\/messages$/,
      handle: (call) =>
        Effect.gen(function* () {
          const type = yield* requiredParam(call, 'type')
          if (type !== 'channel' && type !== 'stream') {
            return yield* refuse('in-process realm models channel messages only')
          }
          const to = yield* requiredParam(call, 'to')
          const topic = truncateTopic(Option.getOrElse(param(call, 'topic'), () => ''))
          const content = yield* requiredParam(call, 'content')
          if (topic.trim() === '') return yield* refuse('Topics are required in this organization.')
          if (content.trim() === '') return yield* refuse('Message must not be empty')
          const [id, timestamp] = yield* Effect.all([nextId, now])
          return yield* modify((state) =>
            Option.match(
              /^\d+$/.test(to) ? HashMap.get(state.channels, Number(to)) : channelNamed(state, to),
              {
                onNone: () => refuse(`Channel '${to}' does not exist`, 'STREAM_DOES_NOT_EXIST'),
                onSome: (channel) =>
                  Effect.succeed([
                    ok({ id }),
                    storeMessage(state, {
                      id,
                      senderId: call.caller.userId,
                      streamId: channel.streamId,
                      topic,
                      content,
                      timestamp,
                    }),
                  ] as const),
              },
            ),
          )
        }),
    },
    { method: 'GET', path: /^\/api\/v1\/messages$/, handle: getMessages(stateRef) },
    {
      method: 'POST',
      path: /^\/api\/v1\/messages\/render$/,
      handle: (call) =>
        Effect.all([requiredParam(call, 'content'), SubscriptionRef.get(stateRef)]).pipe(
          Effect.map(([content, state]) => ok({ rendered: render(state, content).html })),
        ),
    },
    {
      method: 'GET',
      path: /^\/api\/v1\/messages\/(\d+)$/,
      handle: (call) =>
        SubscriptionRef.get(stateRef).pipe(
          Effect.flatMap((state) =>
            requireMessage(state, pathId(call)).pipe(
              Effect.map((message) => ok({ message: messagePayload(state, message, false) })),
            ),
          ),
        ),
    },
    { method: 'PATCH', path: /^\/api\/v1\/messages\/(\d+)$/, handle: editMessage },
    { method: 'POST', path: /^\/api\/v1\/messages\/(\d+)\/reactions$/, handle: react('add') },
    { method: 'DELETE', path: /^\/api\/v1\/messages\/(\d+)\/reactions$/, handle: react('remove') },
    {
      method: 'POST',
      path: /^\/api\/v1\/register$/,
      handle: (call) =>
        Effect.gen(function* () {
          const narrow = yield* jsonParam(call, 'narrow', Schema.Array(Schema.Unknown))
          if (Option.exists(narrow, Arr.isNonEmptyReadonlyArray)) {
            return yield* refuse('in-process realm does not model queue narrows')
          }
          if (Option.contains(param(call, 'apply_markdown'), 'true')) {
            return yield* refuse('in-process realm delivers raw message content only')
          }
          const eventTypes = yield* jsonParam(call, 'event_types', eventTypesSchema)
          const fetchTypes = Option.orElse(
            yield* jsonParam(call, 'fetch_event_types', eventTypesSchema),
            () => eventTypes,
          )
          const queueId = `queue${yield* nextId}`
          const queue: EventQueue = {
            queueId,
            ownerId: call.caller.userId,
            eventTypes: Option.map(eventTypes, (types) => new Set(types)),
            lastIssued: -1,
            pending: [],
          }
          yield* SubscriptionRef.update(stateRef, (s) => ({
            ...s,
            queues: HashMap.set(s.queues, queueId, queue),
          }))
          const realmState = Option.match(fetchTypes, {
            onNone: () => true,
            onSome: (types) => types.includes('realm'),
          })
          return ok({
            queue_id: queueId,
            last_event_id: queue.lastIssued,
            ...(realmState
              ? { realm_allow_message_editing: true, max_message_length: MESSAGE_LENGTH_LIMIT }
              : {}),
          })
        }),
    },
    {
      method: 'DELETE',
      path: /^\/api\/v1\/events$/,
      handle: (call) =>
        requiredParam(call, 'queue_id').pipe(
          Effect.flatMap((queueId) =>
            update((state) =>
              ownQueue(state, call.caller, queueId).pipe(
                Effect.as({ ...state, queues: HashMap.remove(state.queues, queueId) }),
              ),
            ),
          ),
        ),
    },
    { method: 'GET', path: /^\/api\/v1\/events$/, handle: pollEvents },
    {
      method: 'POST',
      path: /^\/api\/v1\/user_uploads$/,
      handle: (call) =>
        Effect.gen(function* () {
          if (Option.isNone(call.file)) return yield* refuse('You must specify a file to upload')
          const file = call.file.value
          const filename = sanitizeUploadName(file.filename)
          const url = `/user_uploads/2/${yield* nextId}/${filename}`
          yield* SubscriptionRef.update(stateRef, (s) => ({
            ...s,
            uploads: HashMap.set(s.uploads, url, {
              data: file.data,
              contentType: file.contentType === '' ? 'application/octet-stream' : file.contentType,
            }),
          }))
          return ok({ uri: url, url, filename })
        }),
    },
    {
      method: 'GET',
      path: /^(\/user_uploads\/.+)$/,
      handle: (call) =>
        SubscriptionRef.get(stateRef).pipe(
          Effect.flatMap((state) =>
            Option.match(HashMap.get(state.uploads, call.path[0] ?? ''), {
              onNone: () => refuse('File not found', 'NOT_FOUND', 404),
              onSome: (upload) =>
                Effect.succeed(
                  new Response(upload.data, { headers: { 'content-type': upload.contentType } }),
                ),
            }),
          ),
        ),
    },
  ]

  const authenticate = (
    state: RealmState,
    header: string | null,
  ): Either.Either<RealmUser, RealmRefusal> => {
    const unauthorized = refusal('Invalid API key', 'INVALID_API_KEY', 401)
    if (header === null || !header.startsWith('Basic ')) return Either.left(unauthorized)
    return Encoding.decodeBase64String(header.slice('Basic '.length)).pipe(
      Either.mapLeft(() => unauthorized),
      Either.flatMap((decoded) => {
        const colon = decoded.indexOf(':')
        const email = decoded.slice(0, colon)
        const key = decoded.slice(colon + 1)
        return Option.match(
          findUser(state, (u) => sameName(u.email, email) && u.apiKey === key),
          {
            onNone: () => Either.left(unauthorized),
            onSome: (u) =>
              u.isActive
                ? Either.right(u)
                : Either.left(refusal('Account is deactivated', 'USER_DEACTIVATED', 401)),
          },
        )
      }),
    )
  }

  const serve = (web: Request): Effect.Effect<Response> =>
    Effect.gen(function* () {
      const url = new URL(web.url)
      const pathname = Either.getOrElse(
        Encoding.decodeUriComponent(url.pathname),
        () => url.pathname,
      )
      const contentType = web.headers.get('content-type') ?? ''
      const multipart = contentType.startsWith('multipart/form-data')
      const file = multipart ? yield* firstFile(web) : Option.none()
      const body = multipart ? '' : yield* Effect.promise(() => web.text())
      const params = new URLSearchParams(url.search)
      for (const [k, v] of new URLSearchParams(body)) params.append(k, v)
      const state = yield* SubscriptionRef.get(stateRef)
      return yield* Effect.gen(function* () {
        const route = yield* Either.fromOption(
          Arr.findFirst(routes, (r) => r.method === web.method && r.path.test(pathname)),
          () =>
            refusal(
              `in-process realm has no handler for ${web.method} ${pathname}`,
              'NOT_FOUND',
              404,
            ),
        )
        const caller = yield* authenticate(state, web.headers.get('authorization'))
        return yield* route.handle({
          caller,
          params,
          path: (route.path.exec(pathname) ?? []).slice(1),
          file,
        })
      }).pipe(Effect.catchTag('RealmRefusal', (r) => Effect.succeed(refusalResponse(r))))
    })

  const client = HttpClient.make((request, url) =>
    serve(webRequestOf(request, url)).pipe(
      Effect.map((response) => HttpClientResponse.fromWeb(request, response)),
    ),
  )

  return {
    client,
    url: yield* RealmUrl(`https://${REALM_HOST}`).pipe(Effect.orDie),
    minter: {
      email: yield* BotEmail(minter.email).pipe(Effect.orDie),
      apiKey: yield* ApiKey(minter.apiKey).pipe(Effect.orDie),
    },
    addChannel: (name) =>
      nextId.pipe(
        Effect.flatMap((streamId) =>
          SubscriptionRef.modify(stateRef, (state) => {
            const channel = Option.getOrElse(channelNamed(state, name), () => ({
              streamId,
              name,
              description: '',
              subscribers: HashSet.empty<number>(),
            }))
            return [{ streamId: channel.streamId, name: channel.name }, putChannel(state, channel)]
          }),
        ),
      ),
    addUser: (fullName, kind) =>
      createUser(
        fullName,
        kind,
        kind === 'bot'
          ? {
              email: `${shortNameOf(fullName)}-bot@${REALM_HOST}`,
              isAdmin: false,
              ownerId: minter.userId,
            }
          : { email: `${shortNameOf(fullName)}@${REALM_HOST}`, isAdmin: false },
      ).pipe(Effect.map((u) => u.userId)),
    deactivateUser: (userId) =>
      SubscriptionRef.update(stateRef, (s) =>
        HashMap.get(s.users, userId).pipe(
          Option.match({ onNone: () => s, onSome: (user) => deactivate(s, user) }),
        ),
      ),
    setPresence: (userId, status) =>
      SubscriptionRef.update(stateRef, (s) => ({
        ...s,
        presence: HashMap.set(s.presence, userId, status),
      })),
  }
})
