/**
 * Stateful Zulip-shaped fixture for end-to-end behaviour tests.
 *
 * test-server.ts is the dumb HTTP plumbing — per-request handlers seeded
 * by the test, so each unit test pins exactly one round trip. This file
 * sits on top: a small in-memory Zulip realm that persists messages
 * between POST and GET, tracks reactions, and serves /users + /users/me
 * + presence consistently. The contract suite drives it for behavioural
 * round-trip tests where the unit-style fixture would force the test
 * to embody adapter-internal API details.
 *
 * Test-only — never imported by production code.
 */

import { Predicate } from 'effect'
import type { CapturedRequest, TestRealm } from './test-server.ts'
import { startTestRealm } from './test-server.ts'

export interface ZulipMember {
  readonly user_id: number
  readonly email: string
  readonly full_name: string
  readonly is_bot: boolean
  readonly is_active: boolean
}

interface StoredMessage {
  readonly id: number
  readonly sender_id: number
  readonly sender_full_name: string
  readonly stream_id: number
  readonly display_recipient: string
  readonly subject: string
  readonly content: string
  readonly timestamp: number
}

interface StreamRecord {
  readonly stream_id: number
  readonly name: string
}

export interface StatefulRealm {
  readonly url: string
  readonly captured: ReadonlyArray<CapturedRequest>
  readonly addMember: (member: ZulipMember) => void
  readonly addStream: (name: string) => StreamRecord
  readonly setSelf: (userId: number) => void
  readonly setPresence: (userId: number, status: 'active' | 'idle' | 'offline') => void
  readonly stop: () => Promise<void>
}

const NARROW_OPERATORS = new Set(['channel', 'stream', 'topic'])

interface NarrowFilter {
  readonly operator: string
  readonly operand: string
}

const parseNarrow = (raw: string | null): ReadonlyArray<NarrowFilter> => {
  if (raw === null) return []
  const parsed: unknown = JSON.parse(raw)
  if (!Array.isArray(parsed)) return []
  const filters: NarrowFilter[] = []
  for (const item of parsed) {
    if (
      Predicate.hasProperty(item, 'operator') &&
      Predicate.hasProperty(item, 'operand') &&
      Predicate.isString(item.operator) &&
      Predicate.isString(item.operand)
    ) {
      const op = item.operator
      if (NARROW_OPERATORS.has(op)) {
        filters.push({ operator: op, operand: item.operand })
      }
    }
  }
  return filters
}

const matchesNarrow = (msg: StoredMessage, filters: ReadonlyArray<NarrowFilter>): boolean =>
  filters.every((f) => {
    if (f.operator === 'channel' || f.operator === 'stream') {
      return msg.display_recipient === f.operand
    }
    if (f.operator === 'topic') {
      return msg.subject === f.operand
    }
    return true
  })

const MENTION_PATTERN = /@\*\*([^*]+)\*\*/g

const containsMentionOf = (content: string, fullName: string): boolean => {
  for (const match of content.matchAll(MENTION_PATTERN)) {
    if (match[1] === fullName) return true
  }
  return false
}

type QueueMode = 'all' | 'mentions'

const parseRegisterMode = (rawNarrow: string | null): QueueMode => {
  if (rawNarrow === null) return 'all'
  let parsed: unknown
  try {
    parsed = JSON.parse(rawNarrow)
  } catch {
    return 'all'
  }
  if (!Array.isArray(parsed)) return 'all'
  for (const item of parsed) {
    if (Array.isArray(item) && item.length === 2 && item[0] === 'is' && item[1] === 'mentioned') {
      return 'mentions'
    }
  }
  return 'all'
}

const parseSubscriptionsBody = (body: string): ReadonlyArray<string> => {
  const params = new URLSearchParams(body)
  const raw = params.get('subscriptions')
  if (raw === null) return []
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch {
    return []
  }
  if (!Array.isArray(parsed)) return []
  const names: string[] = []
  for (const item of parsed) {
    if (typeof item === 'string') {
      names.push(item)
      continue
    }
    if (Predicate.hasProperty(item, 'name') && Predicate.isString(item.name)) {
      names.push(item.name)
    }
  }
  return names
}

interface ZulipMessageEvent {
  readonly id: number
  readonly type: 'message'
  readonly message: StoredMessage
  readonly flags: ReadonlyArray<string>
}

interface ZulipReactionEvent {
  readonly id: number
  readonly type: 'reaction'
  readonly op: 'add' | 'remove'
  readonly user_id: number
  readonly message_id: number
  readonly emoji_name: string
  readonly emoji_code: string
  readonly reaction_type: string
}

type ZulipEvent = ZulipMessageEvent | ZulipReactionEvent

interface Waiter {
  readonly resolve: (events: ReadonlyArray<ZulipEvent>) => void
  readonly requestedAfter: number
  readonly timeoutHandle: ReturnType<typeof setTimeout>
}

interface QueueState {
  readonly queueId: string
  readonly mode: QueueMode
  readonly events: ZulipEvent[]
  waiters: Waiter[]
}

const EVENTS_LONG_POLL_MS = 500

export const startStatefulZulipRealm = (): StatefulRealm => {
  const realm: TestRealm = startTestRealm()
  const members = new Map<number, ZulipMember>()
  const streams = new Map<string, StreamRecord>()
  const messages: StoredMessage[] = []
  const reactions = new Map<number, Map<string, Set<number>>>()
  const presenceByUser = new Map<number, 'active' | 'idle' | 'offline'>()
  const subscribedStreams = new Set<string>()
  const queues = new Map<string, QueueState>()
  let selfId: number | undefined
  let nextStreamId = 1
  let nextMessageId = 1
  let nextQueueId = 1
  let nextEventId = 0

  const wake = (queue: QueueState, events: ReadonlyArray<ZulipEvent>): void => {
    if (events.length === 0) return
    const matched: Waiter[] = []
    for (const w of queue.waiters) {
      const slice = events.filter((e) => e.id > w.requestedAfter)
      if (slice.length === 0) continue
      matched.push(w)
      clearTimeout(w.timeoutHandle)
      w.resolve(slice)
    }
    if (matched.length === 0) return
    queue.waiters = queue.waiters.filter((w) => !matched.includes(w))
  }

  const messagesById = new Map<number, StoredMessage>()

  const queueMatches = (queue: QueueState, ev: ZulipEvent): boolean => {
    if (ev.type === 'message') {
      return queue.mode === 'mentions'
        ? ev.flags.includes('mentioned')
        : subscribedStreams.has(ev.message.display_recipient)
    }
    // Reaction events derive their visibility from the underlying
    // message — real Zulip only delivers a reaction event to a queue
    // whose narrow would have admitted the original message-posted
    // event for that message. Mirror that here so the fixture's
    // contract conforms.
    const target = messagesById.get(ev.message_id)
    if (target === undefined) return false
    if (queue.mode === 'mentions') {
      const me = selfId === undefined ? undefined : findMember(selfId)
      if (me === undefined) return false
      return containsMentionOf(target.content, me.full_name)
    }
    return subscribedStreams.has(target.display_recipient)
  }

  const enqueueEvent = (ev: ZulipEvent): void => {
    for (const queue of queues.values()) {
      if (!queueMatches(queue, ev)) continue
      queue.events.push(ev)
      wake(queue, [ev])
    }
  }

  const findMember = (userId: number): ZulipMember | undefined => members.get(userId)
  const findStreamByName = (name: string): StreamRecord | undefined => streams.get(name)

  const requireSelf = (): ZulipMember => {
    if (selfId === undefined) throw new Error('test realm: setSelf has not been called')
    const found = findMember(selfId)
    if (found === undefined) throw new Error(`test realm: self user_id=${selfId} is not a member`)
    return found
  }

  const allocTimestamp = (): number => {
    const last = messages[messages.length - 1]
    const baseline = last === undefined ? Math.floor(Date.now() / 1000) : last.timestamp + 1
    return baseline
  }

  realm.handle('GET', '/api/v1/users/me', () => {
    const me = requireSelf()
    return { body: { result: 'success', ...me, role: me.is_bot ? 400 : 100 } }
  })

  realm.handle('GET', '/api/v1/users', () => ({
    body: { result: 'success', members: [...members.values()] },
  }))

  let nextUserId = 10_000

  // POST /api/v1/bots — mint a new bot. Switches realm.self to the new
  // bot so subsequent /messages POSTs are attributed correctly. Real
  // Zulip identifies the poster from auth credentials; the fixture
  // approximates that by treating "the most recently minted/regenerated
  // bot is now active". Real Zulip's response shape is just
  // {api_key, user_id, ...} — no email field. The bot's delivery
  // email is reconstructed client-side from short_name + realm host.
  realm.handle('POST', '/api/v1/bots', (req) => {
    const params = new URLSearchParams(req.body)
    const full = params.get('full_name') ?? ''
    const short = params.get('short_name') ?? full
    const userId = nextUserId++
    const member: ZulipMember = {
      user_id: userId,
      email: `${short}-bot@localhost`,
      full_name: full,
      is_bot: true,
      is_active: true,
    }
    members.set(userId, member)
    selfId = userId
    return {
      body: {
        result: 'success',
        api_key: `key-${userId}`,
        user_id: userId,
      },
    }
  })

  // POST /api/v1/bots/{id}/api_key/regenerate — issue a new key for an
  // existing bot. Switches realm.self to that bot so subsequent posts
  // are attributed to it.
  realm.handlePattern('POST', /^\/api\/v1\/bots\/\d+\/api_key\/regenerate$/, (req) => {
    const match = /^\/api\/v1\/bots\/(\d+)\/api_key\/regenerate$/.exec(req.url.pathname)
    const id = match === null ? Number.NaN : Number(match[1])
    const existing = members.get(id)
    if (existing === undefined || !existing.is_bot || !existing.is_active) {
      return {
        body: { result: 'error', msg: 'Insufficient permission', code: 'BAD_REQUEST' },
        init: { status: 400 },
      }
    }
    selfId = id
    return {
      body: { result: 'success', api_key: `key-${id}-${Date.now()}` },
    }
  })

  // DELETE /api/v1/bots/{id} — deactivate the bot. Preserves the user
  // record (matching real Zulip's history-preserving behaviour) but
  // flips is_active to false.
  realm.handlePattern('DELETE', /^\/api\/v1\/bots\/\d+$/, (req) => {
    const match = /^\/api\/v1\/bots\/(\d+)$/.exec(req.url.pathname)
    const id = match === null ? Number.NaN : Number(match[1])
    const existing = members.get(id)
    if (existing === undefined) {
      return {
        body: { result: 'error', msg: 'No such bot', code: 'BAD_REQUEST' },
        init: { status: 400 },
      }
    }
    members.set(id, { ...existing, is_active: false })
    return { body: { result: 'success' } }
  })

  // POST /api/v1/users/{id}/reactivate — flip is_active back to true.
  // Used by the adapter's acquire flow when an existing bot has been
  // previously deactivated (reusing its short_name's reserved email
  // without re-minting).
  realm.handlePattern('POST', /^\/api\/v1\/users\/\d+\/reactivate$/, (req) => {
    const match = /^\/api\/v1\/users\/(\d+)\/reactivate$/.exec(req.url.pathname)
    const id = match === null ? Number.NaN : Number(match[1])
    const existing = members.get(id)
    if (existing === undefined) {
      return {
        body: { result: 'error', msg: 'No such user', code: 'BAD_REQUEST' },
        init: { status: 400 },
      }
    }
    members.set(id, { ...existing, is_active: true })
    selfId = id
    return { body: { result: 'success' } }
  })

  realm.handle('POST', '/api/v1/messages', (req) => {
    const params = new URLSearchParams(req.body)
    const to = params.get('to')
    const topic = params.get('topic')
    const content = params.get('content') ?? ''
    if (to === null) {
      return {
        body: { result: 'error', msg: 'Missing to', code: 'BAD_REQUEST' },
        init: { status: 400 },
      }
    }
    // Match real Zulip's `mandatory_topics: true` behaviour — every stream
    // message must carry a topic. Catches adapter regressions that would
    // otherwise only surface against the live realm.
    if (topic === null || topic.length === 0) {
      return {
        body: { result: 'error', msg: 'Missing topic', code: 'BAD_REQUEST' },
        init: { status: 400 },
      }
    }
    const stream = findStreamByName(to)
    if (stream === undefined) {
      return {
        body: { result: 'error', msg: `Unknown channel: ${to}`, code: 'BAD_REQUEST' },
        init: { status: 400 },
      }
    }
    const me = requireSelf()
    const stored: StoredMessage = {
      id: nextMessageId++,
      sender_id: me.user_id,
      sender_full_name: me.full_name,
      stream_id: stream.stream_id,
      display_recipient: stream.name,
      subject: topic,
      content,
      timestamp: allocTimestamp(),
    }
    messages.push(stored)
    messagesById.set(stored.id, stored)
    const flags: string[] = []
    if (containsMentionOf(stored.content, me.full_name)) flags.push('mentioned')
    nextEventId += 1
    enqueueEvent({ id: nextEventId, type: 'message', message: stored, flags })
    return { body: { result: 'success', id: stored.id } }
  })

  const reactionsFor = (
    messageId: number,
  ): ReadonlyArray<{
    user_id: number
    emoji_name: string
    emoji_code: string
    reaction_type: string
  }> => {
    const byEmoji = reactions.get(messageId)
    if (byEmoji === undefined) return []
    const out: Array<{
      user_id: number
      emoji_name: string
      emoji_code: string
      reaction_type: string
    }> = []
    for (const [emoji, reactors] of byEmoji) {
      for (const userId of reactors) {
        out.push({
          user_id: userId,
          emoji_name: emoji,
          emoji_code: emoji,
          reaction_type: 'unicode_emoji',
        })
      }
    }
    return out
  }

  const enrich = (
    m: StoredMessage,
  ): StoredMessage & {
    readonly reactions: ReadonlyArray<{
      user_id: number
      emoji_name: string
      emoji_code: string
      reaction_type: string
    }>
  } => ({ ...m, reactions: reactionsFor(m.id) })

  realm.handle('GET', '/api/v1/messages', (req) => {
    const filters = parseNarrow(req.url.searchParams.get('narrow'))
    const numBefore = Number(req.url.searchParams.get('num_before') ?? '100')
    const numAfter = Number(req.url.searchParams.get('num_after') ?? '0')
    const anchor = req.url.searchParams.get('anchor')
    // anchor=<numeric id> + num_before=0 + num_after=0 is the single-message
    // lookup used by the reaction cache-miss fallback (ass-ps6h). Return the
    // anchor message itself if it matches the narrow, else empty.
    const anchorId = anchor === null ? null : Number(anchor)
    if (anchorId !== null && Number.isFinite(anchorId) && numBefore === 0 && numAfter === 0) {
      const hit = messagesById.get(anchorId)
      const message = hit !== undefined && matchesNarrow(hit, filters) ? [enrich(hit)] : []
      return {
        body: {
          result: 'success',
          messages: message,
          anchor: anchorId,
          found_anchor: message.length > 0,
          found_newest: false,
          found_oldest: false,
          history_limited: false,
        },
      }
    }
    const matched = messages.filter((m) => matchesNarrow(m, filters))
    const limited = matched.slice(-Math.max(0, numBefore)).map(enrich)
    return {
      body: {
        result: 'success',
        messages: limited,
        anchor: 0,
        found_anchor: false,
        found_newest: true,
        found_oldest: false,
        history_limited: false,
      },
    }
  })

  const reactionsHandler = (req: { method: string; url: URL; body: string }) => {
    const path = req.url.pathname
    const match = /^\/api\/v1\/messages\/(\d+)\/reactions$/.exec(path)
    if (match === null) {
      return {
        body: { result: 'error', msg: 'Bad reactions path', code: 'BAD_REQUEST' },
        init: { status: 400 },
      }
    }
    const messageId = Number(match[1])
    const params = new URLSearchParams(req.body)
    const emoji = params.get('emoji_name') ?? ''
    const me = requireSelf()
    const byEmoji = reactions.get(messageId) ?? new Map<string, Set<number>>()
    const reactors = byEmoji.get(emoji) ?? new Set<number>()
    if (req.method === 'POST') reactors.add(me.user_id)
    if (req.method === 'DELETE') {
      reactors.delete(me.user_id)
      if (reactors.size === 0) byEmoji.delete(emoji)
    }
    if (reactors.size > 0) byEmoji.set(emoji, reactors)
    if (byEmoji.size > 0) reactions.set(messageId, byEmoji)
    else reactions.delete(messageId)
    nextEventId += 1
    enqueueEvent({
      id: nextEventId,
      type: 'reaction',
      op: req.method === 'POST' ? 'add' : 'remove',
      user_id: me.user_id,
      message_id: messageId,
      emoji_name: emoji,
      emoji_code: emoji,
      reaction_type: 'unicode_emoji',
    })
    return { body: { result: 'success' } }
  }

  realm.handlePattern('POST', /^\/api\/v1\/messages\/\d+\/reactions$/, reactionsHandler)
  realm.handlePattern('DELETE', /^\/api\/v1\/messages\/\d+\/reactions$/, reactionsHandler)

  realm.handlePattern('PATCH', /^\/api\/v1\/messages\/\d+$/, (req) => {
    const match = /^\/api\/v1\/messages\/(\d+)$/.exec(req.url.pathname)
    const messageId = match === null ? Number.NaN : Number(match[1])
    const existing = messagesById.get(messageId)
    if (existing === undefined) {
      return {
        body: { result: 'error', msg: 'Invalid message(s)', code: 'BAD_REQUEST' },
        init: { status: 400 },
      }
    }
    const params = new URLSearchParams(req.body)
    const content = params.get('content')
    if (content === null) return { body: { result: 'success' } }
    const updated: StoredMessage = { ...existing, content }
    messagesById.set(messageId, updated)
    const idx = messages.indexOf(existing)
    if (idx !== -1) messages[idx] = updated
    return { body: { result: 'success' } }
  })

  realm.handlePattern('GET', /^\/api\/v1\/users\/\d+\/presence$/, (req) => {
    const match = /^\/api\/v1\/users\/(\d+)\/presence$/.exec(req.url.pathname)
    const userId = match === null ? undefined : Number(match[1])
    if (userId === undefined || !findMember(userId)) {
      return {
        body: { result: 'error', msg: 'No such user', code: 'BAD_REQUEST' },
        init: { status: 400 },
      }
    }
    const status = presenceByUser.get(userId) ?? 'offline'
    return {
      body: {
        result: 'success',
        presence: { aggregated: { status, timestamp: Math.floor(Date.now() / 1000) } },
      },
    }
  })

  realm.handle('GET', '/api/v1/streams', () => ({
    body: {
      result: 'success',
      streams: Array.from(streams.values()).map((s) => ({
        stream_id: s.stream_id,
        name: s.name,
      })),
    },
  }))

  realm.handle('POST', '/api/v1/users/me/subscriptions', (req) => {
    for (const name of parseSubscriptionsBody(req.body)) {
      subscribedStreams.add(name)
    }
    return { body: { result: 'success' } }
  })

  realm.handle('DELETE', '/api/v1/users/me/subscriptions', (req) => {
    for (const name of parseSubscriptionsBody(req.body)) {
      subscribedStreams.delete(name)
    }
    return { body: { result: 'success' } }
  })

  realm.handle('POST', '/api/v1/register', (req) => {
    const params = new URLSearchParams(req.body)
    const mode = parseRegisterMode(params.get('narrow'))
    const queueId = `q${nextQueueId++}`
    queues.set(queueId, { queueId, mode, events: [], waiters: [] })
    return { body: { result: 'success', queue_id: queueId, last_event_id: nextEventId } }
  })

  realm.handle('GET', '/api/v1/events', (req) => {
    const queueId = req.url.searchParams.get('queue_id') ?? ''
    const lastEventId = Number(req.url.searchParams.get('last_event_id') ?? '0')
    const queue = queues.get(queueId)
    if (queue === undefined) {
      return {
        body: { result: 'error', msg: 'Bad event queue id', code: 'BAD_EVENT_QUEUE_ID' },
        init: { status: 400 },
      }
    }
    const ready = queue.events.filter((e) => e.id > lastEventId)
    if (ready.length > 0) {
      return { body: { result: 'success', events: ready } }
    }
    return new Promise((resolve) => {
      const timeoutHandle = setTimeout(() => {
        queue.waiters = queue.waiters.filter((w) => w !== waiter)
        resolve({ body: { result: 'success', events: [] } })
      }, EVENTS_LONG_POLL_MS)
      const waiter: Waiter = {
        requestedAfter: lastEventId,
        timeoutHandle,
        resolve: (events) => {
          resolve({ body: { result: 'success', events } })
        },
      }
      queue.waiters.push(waiter)
    })
  })

  return {
    url: realm.url,
    captured: realm.captured,
    addMember: (m) => {
      members.set(m.user_id, m)
    },
    addStream: (name) => {
      const existing = streams.get(name)
      if (existing !== undefined) return existing
      const record: StreamRecord = { stream_id: nextStreamId++, name }
      streams.set(name, record)
      return record
    },
    setSelf: (userId) => {
      selfId = userId
    },
    setPresence: (userId, status) => {
      presenceByUser.set(userId, status)
    },
    stop: realm.stop,
  }
}
