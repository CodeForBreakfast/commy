/** The shared contract's tests for MessageInbox. */

import { expect, test } from 'bun:test'
import {
  type AgentComms,
  decodeEmojiSync,
  decodeMessageBodySync,
  decodeThreadNameSync,
  decodeTimestampSync,
  type InboundEvent,
  mentionedIdentities,
} from '@commy/core/ports'
import { Duration, Effect, Option, Queue, type Scope, Stream } from 'effect'
import type { ContractEnv } from '../contract.ts'
import { postSpaced } from './helpers.ts'

/**
 * Generous deadline for "this event must eventually arrive" assertions. On an
 * in-process substrate the event is already enqueued so the wait resolves
 * immediately; on the live Zulip realm the events queue can take several
 * seconds to surface a post/mention. Sits well under the live suite's 45s
 * per-test budget so a genuine no-show still fails with the test's own clear
 * message rather than a bun:test timeout.
 */
const EVENT_DELIVERY_DEADLINE = Duration.seconds(30)

/**
 * Pull events off the queue in order until one satisfies `predicate`,
 * returning it. Recurses on each non-match — the caller wraps this in
 * `Effect.timeoutOption` so an event that never arrives bounds the wait
 * instead of blocking forever. Lets the mention-floor tests assert on the
 * `mention-received` event without coupling to whether a `message-posted`
 * precedes it (it does on Memory, may not live when the only thing
 * matching is the unconditional mention rule).
 */
const takeUntil = (
  queue: Queue.Queue<InboundEvent>,
  predicate: (event: InboundEvent) => boolean,
): Effect.Effect<InboundEvent> =>
  Queue.take(queue).pipe(Effect.filterOrElse(predicate, () => takeUntil(queue, predicate)))

/**
 * Await the first event matching `predicate`, failing the test (with
 * `description`) if none arrives within `EVENT_DELIVERY_DEADLINE`. Intervening
 * events are drained and discarded — so a positive event-delivery assertion
 * does not depend on the *position* of its event in the stream. On the live
 * realm an events queue under 429 backoff can redeliver or gap-replay a
 * `message-posted`, which a positional `Queue.take` + `.kind`
 * assertion would mistake for the wrong event; draining-until-match is immune.
 * Memory delivers exactly once so the match is immediate there.
 */
const awaitEvent = (
  queue: Queue.Queue<InboundEvent>,
  description: string,
  predicate: (event: InboundEvent) => boolean,
): Effect.Effect<InboundEvent> =>
  takeUntil(queue, predicate).pipe(
    Effect.timeoutOption(EVENT_DELIVERY_DEADLINE),
    Effect.flatMap(
      Option.match({
        onNone: () =>
          Effect.die(new Error(`events() did not observe ${description} within the deadline`)),
        onSome: Effect.succeed,
      }),
    ),
  )

/**
 * Start a subscriber on `events()` that mirrors every inbound event into an
 * unbounded queue. Tests pull from the queue in order, mimicking
 * AsyncIterator.next() semantics, and use `Effect.timeoutOption` on
 * `Queue.take` for "no event within Nms" assertions. Requires `Scope` — the
 * caller's `Effect.scoped` block owns the consumer fiber's lifetime, so the
 * fork is interrupted when the test body exits.
 */
const eventQueue = (
  comms: AgentComms,
): Effect.Effect<Queue.Queue<InboundEvent>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const queue = yield* Queue.unbounded<InboundEvent>()
    yield* Effect.forkScoped(
      comms.inbox.events().pipe(Stream.runForEach((event) => Queue.offer(queue, event))),
    )
    return queue
  })

export const inboxContract = (env: () => ContractEnv): void => {
  test('inbox.subscribe(channel) + post yields message-posted with sender = currentIdentity', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          const me = yield* env().comms.identity.currentIdentity()
          yield* env().comms.inbox.subscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          const ref = yield* env().comms.publisher.post(
            channel.name,
            decodeMessageBodySync('event-target'),
          )
          const event = yield* awaitEvent(
            queue,
            'message-posted for the post',
            (e) => e.kind === 'message-posted' && e.message.body.includes('event-target'),
          )
          if (event.kind !== 'message-posted') throw new Error('unexpected event kind')
          expect(event.message.sender.id).toEqual(me.id)
          expect(event.message.ref.id).toEqual(ref.id)
          expect(event.message.ref.channel.id).toEqual(channel.id)
        }),
      ),
    ))
  test('a message-posted event carries the permalinks that post returned', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          yield* env().comms.inbox.subscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          const ref = yield* env().comms.publisher.post(
            channel.name,
            decodeMessageBodySync('linked'),
            { thread: decodeThreadNameSync('design') },
          )
          const event = yield* awaitEvent(
            queue,
            'message-posted for the threaded post',
            (e) => e.kind === 'message-posted' && e.message.body.includes('linked'),
          )
          if (event.kind !== 'message-posted') throw new Error('unexpected event kind')
          expect(event.message.ref.permalink).toEqual(ref.permalink)
          expect(event.message.ref.channel.permalink).toEqual(ref.channel.permalink)
          expect(Option.map(event.message.ref.thread, (t) => t.permalink)).toEqual(
            Option.map(ref.thread, (t) => t.permalink),
          )
        }),
      ),
    ))
  test('a peer post names its sender by the identity the directory gives it', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const peerPost = env().peerPost
          if (peerPost === undefined) return
          const channel = yield* env().seedChannel('lobby')
          const peer = yield* env().seedAgent('alice')
          yield* env().comms.inbox.subscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          yield* peerPost(peer, channel.name, decodeMessageBodySync('from a peer'))
          const event = yield* awaitEvent(
            queue,
            'message-posted for the peer post',
            (e) => e.kind === 'message-posted' && e.message.body.includes('from a peer'),
          )
          if (event.kind !== 'message-posted') throw new Error('unexpected event kind')
          expect(event.message.sender).toEqual(peer)
        }),
      ),
    ))
  test('events() delivers posts in the order they were posted', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          const bodies = ['e0', 'e1', 'e2', 'e3', 'e4'].map((body) => decodeMessageBodySync(body))
          yield* env().comms.inbox.subscribe(channel.name)
          yield* Effect.forEach(bodies, (body) => env().comms.publisher.post(channel.name, body))
          const queue = yield* eventQueue(env().comms)
          const delivered = yield* Effect.forEach(bodies, (body) =>
            awaitEvent(
              queue,
              `message-posted for ${body}`,
              (e) => e.kind === 'message-posted' && bodies.includes(e.message.body),
            ).pipe(Effect.map((e) => (e.kind === 'message-posted' ? e.message.body : undefined))),
          )
          expect(delivered).toEqual(bodies)
        }),
      ),
    ))
  test('events() started before subscribe observes a post made after subscribe', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.inbox.subscribe(channel.name)
          yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('mid-stream'))
          yield* awaitEvent(
            queue,
            'the post made after a mid-stream subscribe',
            (e) => e.kind === 'message-posted' && e.message.body.includes('mid-stream'),
          )
        }),
      ),
    ))
  test('inbox.subscribe → post → events() observes the post (subscribe implies readiness)', () =>
    // The natural consumer shape: subscribe, post, then iterate. The
    // queue must see events that landed between subscribe's resolution
    // and the first events() call — subscribe()'s post-condition is that
    // the adapter has primed whatever it needs to capture them.
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          yield* env().comms.inbox.subscribe(channel.name)
          yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('pre-iterate'))
          const queue = yield* eventQueue(env().comms)
          const event = yield* awaitEvent(
            queue,
            'the post made after subscribe resolved',
            (e) => e.kind === 'message-posted' && e.message.body.includes('pre-iterate'),
          )
          if (event.kind !== 'message-posted')
            throw new Error(`expected message-posted, got ${event.kind}`)
          expect(event.message.body).toContain('pre-iterate')
        }),
      ),
    ))
  // The mention floor (an @-mention of you reaches you). A peer
  // posts the mention (sender ≠ self) — the cross-identity shape
  // realm.live.test.ts proves, and one a single-identity self-mention cannot,
  // since live Zulip's `is:mentioned` narrow is keyed to the queue owner.
  //
  // Self subscribes the channel here only to keep this case adjacent to the
  // no-subscription floor below, which is the stricter of the two.
  test('a peer mention surfaces as mention-received when self is subscribed to the channel', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const peerPost = env().peerPost
          if (peerPost === undefined) return
          const channel = yield* env().seedChannel('lobby')
          const me = yield* env().comms.identity.currentIdentity()
          const peer = yield* env().seedAgent('alice')
          yield* env().comms.inbox.subscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          yield* peerPost(peer, channel.name, decodeMessageBodySync(`@**${me.name}** wake up`), {
            mentions: [me],
          })
          const event = yield* awaitEvent(
            queue,
            'a peer mention of self',
            (e) => e.kind === 'mention-received',
          )
          if (event.kind !== 'mention-received')
            throw new Error(`expected mention-received, got ${event.kind}`)
          expect(mentionedIdentities(event.mentions).map((m) => m.id)).toContain(me.id)
        }),
      ),
    ))
  // The mention floor with NOTHING subscribed — the cell comms-n1my exists to
  // fix. A session outside a git repo resolves no project slug, so neither
  // default narrow set gives it anything to subscribe to; mentions are its
  // only inbox. It must still hear its own name, with no subscribe of any
  // kind, or the seat is silently deaf to the one thing addressed to it.
  test('a peer mention surfaces as mention-received with no subscription at all', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const peerPost = env().peerPost
          if (peerPost === undefined) return
          const channel = yield* env().seedChannel('lobby')
          const me = yield* env().comms.identity.currentIdentity()
          const peer = yield* env().seedAgent('alice')
          const queue = yield* eventQueue(env().comms)
          yield* peerPost(peer, channel.name, decodeMessageBodySync(`@**${me.name}** wake up`), {
            mentions: [me],
          })
          const event = yield* awaitEvent(
            queue,
            'a peer mention of self with nothing subscribed',
            (e) => e.kind === 'mention-received',
          )
          if (event.kind !== 'mention-received')
            throw new Error(`expected mention-received, got ${event.kind}`)
          expect(mentionedIdentities(event.mentions).map((m) => m.id)).toContain(me.id)
        }),
      ),
    ))
  // The other half of the floor: implicit mentions widen delivery for the
  // bound identity ONLY. Someone else's mention in an unsubscribed channel
  // stays silent.
  test('an unsubscribed channel stays silent for a post mentioning someone else', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          const alice = yield* env().seedAgent('alice')
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('hello alice'), {
            mentions: [alice],
          })
          const result = yield* Queue.take(queue).pipe(Effect.timeoutOption(Duration.millis(100)))
          expect(Option.isNone(result)).toBe(true)
        }),
      ),
    ))
  test('inbox.subscribe(channel) + react yields reaction-added with target = posted ref', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          const me = yield* env().comms.identity.currentIdentity()
          yield* env().comms.inbox.subscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          const ref = yield* env().comms.publisher.post(
            channel.name,
            decodeMessageBodySync('react target'),
          )
          // Drain message-posted first so the cache that backs reaction
          // target resolution is populated before the reaction lands.
          yield* awaitEvent(
            queue,
            'message-posted for the react target',
            (e) => e.kind === 'message-posted' && e.message.body.includes('react target'),
          )

          yield* env().comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
          const reacted = yield* awaitEvent(
            queue,
            'reaction-added',
            (e) => e.kind === 'reaction-added',
          )
          if (reacted.kind !== 'reaction-added')
            throw new Error(`expected reaction-added, got ${reacted.kind}`)
          expect(reacted.target.id).toEqual(ref.id)
          expect(reacted.target.channel.id).toEqual(channel.id)
          expect(reacted.emoji).toBe(decodeEmojiSync('thumbs_up'))
          expect(reacted.by.id).toEqual(me.id)
        }),
      ),
    ))
  test('reaction on a message the iterator never observed still yields reaction-added with the correct target', () =>
    // The reaction event arrives without stream/topic context — substrates
    // that lean on a MessageRef cache populated from message-posted events
    // (e.g. Zulip) must still resolve the target when the message pre-dates
    // the iterator. Posting before subscribe() means no message-posted ever
    // flows through events(), so the cache is empty when the reaction lands.
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          const me = yield* env().comms.identity.currentIdentity()
          const ref = yield* env().comms.publisher.post(
            channel.name,
            decodeMessageBodySync('pre-subscribe target'),
          )
          yield* env().comms.inbox.subscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
          const reacted = yield* awaitEvent(
            queue,
            'reaction-added for the pre-subscribe target',
            (e) => e.kind === 'reaction-added',
          )
          if (reacted.kind !== 'reaction-added')
            throw new Error(`expected reaction-added, got ${reacted.kind}`)
          expect(reacted.target.id).toEqual(ref.id)
          expect(reacted.target.channel.id).toEqual(channel.id)
          expect(reacted.emoji).toBe(decodeEmojiSync('thumbs_up'))
          expect(reacted.by.id).toEqual(me.id)
        }),
      ),
    ))
  test('inbox.subscribe(channel) + unreact yields reaction-removed', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          const me = yield* env().comms.identity.currentIdentity()
          yield* env().comms.inbox.subscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          const ref = yield* env().comms.publisher.post(
            channel.name,
            decodeMessageBodySync('react then unreact'),
          )
          yield* awaitEvent(
            queue,
            'message-posted for the react/unreact target',
            (e) => e.kind === 'message-posted' && e.message.body.includes('react then unreact'),
          )

          yield* env().comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
          yield* awaitEvent(queue, 'reaction-added', (e) => e.kind === 'reaction-added')

          yield* env().comms.publisher.unreact(ref, decodeEmojiSync('thumbs_up'))
          const removed = yield* awaitEvent(
            queue,
            'reaction-removed',
            (e) => e.kind === 'reaction-removed',
          )
          if (removed.kind !== 'reaction-removed')
            throw new Error(`expected reaction-removed, got ${removed.kind}`)
          expect(removed.target.id).toEqual(ref.id)
          expect(removed.emoji).toBe(decodeEmojiSync('thumbs_up'))
          expect(removed.by.id).toEqual(me.id)
        }),
      ),
    ))
  test('inbox.subscribe then unsubscribe suppresses subsequent posts to that channel', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          yield* env().comms.inbox.subscribe(channel.name)
          yield* env().comms.inbox.unsubscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.publisher.post(
            channel.name,
            decodeMessageBodySync('should-not-arrive'),
          )
          const result = yield* Queue.take(queue).pipe(Effect.timeoutOption(Duration.millis(100)))
          expect(Option.isNone(result)).toBe(true)
        }),
      ),
    ))
  test('inbox.unsubscribe of a channel also silences a topic held in it', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          const thread = decodeThreadNameSync('held')
          yield* env().comms.inbox.subscribe(channel.name)
          yield* env().comms.inbox.subscribe({ channel: channel.name, thread })
          yield* env().comms.inbox.unsubscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.publisher.post(
            channel.name,
            decodeMessageBodySync('should-not-arrive'),
            { thread },
          )
          const result = yield* Queue.take(queue).pipe(Effect.timeoutOption(Duration.millis(100)))
          expect(Option.isNone(result)).toBe(true)
        }),
      ),
    ))
  test('inbox.unsubscribe of a channel also silences a new-topics narrow held in it', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          yield* env().comms.inbox.subscribe(channel.name)
          yield* env().comms.inbox.subscribe({
            kind: 'new-topics-in-channel',
            channel: channel.name,
          })
          yield* env().comms.inbox.unsubscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.publisher.post(
            channel.name,
            decodeMessageBodySync('should-not-arrive'),
            { thread: decodeThreadNameSync('fresh') },
          )
          const result = yield* Queue.take(queue).pipe(Effect.timeoutOption(Duration.millis(100)))
          expect(Option.isNone(result)).toBe(true)
        }),
      ),
    ))
  test('a peer mention still arrives from a channel the seat unsubscribed from', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const peerPost = env().peerPost
          if (peerPost === undefined) return
          const channel = yield* env().seedChannel('lobby')
          const me = yield* env().comms.identity.currentIdentity()
          const peer = yield* env().seedAgent('alice')
          yield* env().comms.inbox.subscribe(channel.name)
          yield* env().comms.inbox.unsubscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          yield* peerPost(peer, channel.name, decodeMessageBodySync(`@**${me.name}** still here`), {
            mentions: [me],
          })
          const event = yield* awaitEvent(
            queue,
            'a peer mention of self in an unsubscribed channel',
            (e) => e.kind === 'mention-received',
          )
          if (event.kind !== 'mention-received')
            throw new Error(`expected mention-received, got ${event.kind}`)
          expect(mentionedIdentities(event.mentions).map((m) => m.id)).toContain(me.id)
        }),
      ),
    ))
  test('inbox.unsubscribe of one topic leaves another topic in that channel delivering', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          const held = { channel: channel.name, thread: decodeThreadNameSync('held') }
          const dropped = { channel: channel.name, thread: decodeThreadNameSync('dropped') }
          yield* env().comms.inbox.subscribe(held)
          yield* env().comms.inbox.subscribe(dropped)
          yield* env().comms.inbox.unsubscribe(dropped)
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('still arrives'), {
            thread: held.thread,
          })
          yield* awaitEvent(
            queue,
            'the post in the topic still held',
            (e) => e.kind === 'message-posted' && e.message.body.includes('still arrives'),
          )
        }),
      ),
    ))
  test('inbox.unsubscribe of one topic leaves another topic delivering beside a new-topics narrow', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          const held = { channel: channel.name, thread: decodeThreadNameSync('held') }
          const dropped = { channel: channel.name, thread: decodeThreadNameSync('dropped') }
          yield* env().comms.inbox.subscribe({
            kind: 'new-topics-in-channel',
            channel: channel.name,
          })
          yield* env().comms.inbox.subscribe(held)
          yield* env().comms.inbox.subscribe(dropped)
          yield* env().comms.inbox.unsubscribe(dropped)
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('opens held'), {
            thread: held.thread,
          })
          yield* env().comms.publisher.post(
            channel.name,
            decodeMessageBodySync('replies in held'),
            {
              thread: held.thread,
            },
          )
          yield* awaitEvent(
            queue,
            'the opener of the topic still held',
            (e) => e.kind === 'message-posted' && e.message.body.includes('opens held'),
          )
          yield* awaitEvent(
            queue,
            'the reply in the topic still held',
            (e) => e.kind === 'message-posted' && e.message.body.includes('replies in held'),
          )
        }),
      ),
    ))
  // What a seat reads back on the way up, and the ceiling on what it can
  // read. A subscription names a CHANNEL on every substrate worth targeting,
  // so a narrow below that — one topic, or first-messages-only — reads back as
  // its plain channel. A substrate that answered anything finer would make the
  // caller's own narrowing look redundant when it is not.
  test('inbox.subscriptions() reports the channels this seat subscribed, and only channels', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const lobby = yield* env().seedChannel('lobby')
          const annex = yield* env().seedChannel('annex')
          yield* env().comms.inbox.subscribe(lobby.name)
          yield* env().comms.inbox.subscribe({
            channel: annex.name,
            thread: decodeThreadNameSync('planning'),
          })
          const subscribed = yield* env().comms.inbox.subscriptions()
          expect([...subscribed].sort()).toEqual([annex.name, lobby.name].sort())
        }),
      ),
    ))
  test('inbox.subscriptions() drops a channel the seat unsubscribed from', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const lobby = yield* env().seedChannel('lobby')
          yield* env().comms.inbox.subscribe(lobby.name)
          yield* env().comms.inbox.unsubscribe(lobby.name)
          expect(yield* env().comms.inbox.subscriptions()).toEqual([])
        }),
      ),
    ))
  test('inbox.subscriptions() reports a channel subscribed twice once', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const lobby = yield* env().seedChannel('lobby')
        yield* env().comms.inbox.subscribe(lobby.name)
        yield* env().comms.inbox.subscribe(lobby.name)
        expect(yield* env().comms.inbox.subscriptions()).toEqual([lobby.name])
      }),
    ))
  test('inbox.subscriptions() keeps a channel while the seat still holds a topic in it', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const lobby = yield* env().seedChannel('lobby')
        const held = { channel: lobby.name, thread: decodeThreadNameSync('held') }
        const dropped = { channel: lobby.name, thread: decodeThreadNameSync('dropped') }
        yield* env().comms.inbox.subscribe(held)
        yield* env().comms.inbox.subscribe(dropped)
        yield* env().comms.inbox.unsubscribe(dropped)
        expect(yield* env().comms.inbox.subscriptions()).toEqual([lobby.name])
        yield* env().comms.inbox.unsubscribe(held)
        expect(yield* env().comms.inbox.subscriptions()).toEqual([])
      }),
    ))
  test('inbox.subscriptions() drops an unsubscribed channel even while narrows in it are held', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const lobby = yield* env().seedChannel('lobby')
        yield* env().comms.inbox.subscribe(lobby.name)
        yield* env().comms.inbox.subscribe({
          channel: lobby.name,
          thread: decodeThreadNameSync('held'),
        })
        yield* env().comms.inbox.subscribe({ kind: 'new-topics-in-channel', channel: lobby.name })
        yield* env().comms.inbox.unsubscribe(lobby.name)
        expect(yield* env().comms.inbox.subscriptions()).toEqual([])
      }),
    ))
  test('inbox.replay(since) returns past message-posted events with ts >= since', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* postSpaced(env().comms, channel.name, [
          decodeMessageBodySync('before-pivot'),
          decodeMessageBodySync('pivot'),
          decodeMessageBodySync('after-pivot'),
        ])
        const everything = yield* env().comms.history.readChannel(channel.name, {})
        const pivot = everything.find((m) => m.body === 'pivot')
        if (pivot === undefined) throw new Error('expected pivot message in channel history')
        const events = yield* env().comms.inbox.replay(pivot.ts)
        const postedBodies = events
          .filter(
            (e): e is Extract<InboundEvent, { kind: 'message-posted' }> =>
              e.kind === 'message-posted',
          )
          .map((e) => e.message.body)
        expect(postedBodies).toContain(decodeMessageBodySync('pivot'))
        expect(postedBodies).toContain(decodeMessageBodySync('after-pivot'))
        expect(postedBodies).not.toContain(decodeMessageBodySync('before-pivot'))
        expect(
          events.every(
            (e) => e.kind !== 'message-posted' || e.message.ref.channel.name === channel.name,
          ),
        ).toBe(true)
      }),
    ))
  test('inbox.replay surfaces a peer mention of self as mention-received beside its message-posted', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const peerPost = env().peerPost
        if (peerPost === undefined) return
        const channel = yield* env().seedChannel('lobby')
        const me = yield* env().comms.identity.currentIdentity()
        const peer = yield* env().seedAgent('alice')
        yield* peerPost(peer, channel.name, decodeMessageBodySync(`@**${me.name}** catch up`), {
          mentions: [me],
        })
        const [posted] = yield* env().comms.history.readChannel(channel.name, {})
        if (posted === undefined) throw new Error('expected the peer post in channel history')
        const events = yield* env().comms.inbox.replay(posted.ts)
        expect(events.map((e) => e.kind)).toEqual(['message-posted', 'mention-received'])
        const mention = events[1]
        if (mention?.kind !== 'mention-received') throw new Error('expected mention-received')
        expect(mentionedIdentities(mention.mentions).map((m) => m.id)).toContain(me.id)
      }),
    ))
  test('inbox.subscribe(new-topics-in-channel) yields the first message of a fresh topic', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          yield* env().comms.inbox.subscribe({
            kind: 'new-topics-in-channel',
            channel: channel.name,
          })
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('first in alpha'), {
            thread: decodeThreadNameSync('alpha'),
          })
          const event = yield* awaitEvent(
            queue,
            'the first message of topic alpha',
            (e) => e.kind === 'message-posted' && e.message.body.includes('first in alpha'),
          )
          if (event.kind !== 'message-posted')
            throw new Error(`expected message-posted, got ${event.kind}`)
          expect(Option.map(event.message.ref.thread, (t) => t.name)).toEqual(
            Option.some(decodeThreadNameSync('alpha')),
          )
        }),
      ),
    ))
  test('inbox.subscribe(new-topics-in-channel) suppresses subsequent messages in the same topic', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          yield* env().comms.inbox.subscribe({
            kind: 'new-topics-in-channel',
            channel: channel.name,
          })
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('first in alpha'), {
            thread: decodeThreadNameSync('alpha'),
          })
          yield* awaitEvent(
            queue,
            'the first message of topic alpha',
            (e) => e.kind === 'message-posted' && e.message.body.includes('first in alpha'),
          )

          yield* env().comms.publisher.post(
            channel.name,
            decodeMessageBodySync('second in alpha'),
            {
              thread: decodeThreadNameSync('alpha'),
            },
          )
          const result = yield* Queue.take(queue).pipe(Effect.timeoutOption(Duration.millis(200)))
          expect(Option.isNone(result)).toBe(true)
        }),
      ),
    ))
  test('inbox.subscribe(new-topics-in-channel) yields the first message of a second fresh topic', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          yield* env().comms.inbox.subscribe({
            kind: 'new-topics-in-channel',
            channel: channel.name,
          })
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('first in alpha'), {
            thread: decodeThreadNameSync('alpha'),
          })
          yield* awaitEvent(
            queue,
            'the first message of topic alpha',
            (e) => e.kind === 'message-posted' && e.message.body.includes('first in alpha'),
          )

          yield* env().comms.publisher.post(
            channel.name,
            decodeMessageBodySync('second in alpha'),
            {
              thread: decodeThreadNameSync('alpha'),
            },
          )
          yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('first in bravo'), {
            thread: decodeThreadNameSync('bravo'),
          })
          const next = yield* awaitEvent(
            queue,
            'the first message of topic bravo',
            (e) => e.kind === 'message-posted' && e.message.body.includes('first in bravo'),
          )
          if (next.kind !== 'message-posted') throw new Error('expected second message-posted')
          expect(Option.map(next.message.ref.thread, (t) => t.name)).toEqual(
            Option.some(decodeThreadNameSync('bravo')),
          )
        }),
      ),
    ))
  test('inbox.unsubscribe(new-topics-in-channel) suppresses future first-message events', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          const target = { kind: 'new-topics-in-channel' as const, channel: channel.name }
          yield* env().comms.inbox.subscribe(target)
          yield* env().comms.inbox.unsubscribe(target)
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.publisher.post(
            channel.name,
            decodeMessageBodySync('should-not-arrive'),
            {
              thread: decodeThreadNameSync('alpha'),
            },
          )
          const result = yield* Queue.take(queue).pipe(Effect.timeoutOption(Duration.millis(200)))
          expect(Option.isNone(result)).toBe(true)
        }),
      ),
    ))
  test('inbox.replay(since) past every message yields an empty result', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('only'))
        const everything = yield* env().comms.history.readChannel(channel.name, {})
        const last = everything[everything.length - 1]
        if (last === undefined) throw new Error('expected at least one message in channel history')
        const future = decodeTimestampSync(last.ts + 3600)
        const events = yield* env().comms.inbox.replay(future)
        expect(events).toEqual([])
      }),
    ))
  test('inbox.settingsChanges() emits no initial value', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const first = yield* env()
          .comms.inbox.settingsChanges()
          .pipe(Stream.runHead, Effect.timeoutOption(Duration.millis(100)))
        expect(Option.flatten(first)).toEqual(Option.none())
      }),
    ))
}
