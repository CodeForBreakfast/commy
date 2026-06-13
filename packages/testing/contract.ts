/**
 * Substrate-agnostic contract tests for the AgentComms ports.
 *
 * Parameterised over a factory that produces a fresh environment per test —
 * `comms` plus seed helpers for setting up channels and other identities.
 * Each adapter that claims to implement the AgentComms aggregate must pass
 * this suite.
 *
 * The factory shape is the only substrate-specific concession. Tests
 * exercise behaviour through the ports, never through adapter internals.
 *
 * Test bodies run as Effects: each `test(name, ...)` invokes
 * `Effect.runPromise(Effect.gen(...))` at the test boundary, so port calls
 * compose via `yield*` and the typed E channel survives instead of
 * collapsing across per-call Promise interop.
 */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test'
import type { AgentComms, ChannelRef, Identity, InboundEvent, MessageRef } from '@commy/core/ports'
import {
  decodeBotNameSync,
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeEmojiSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  decodeTimestampSync,
  PublisherError,
  UnknownChannel,
  UnknownIdentity,
} from '@commy/core/ports'
import { Duration, Effect, Exit, Option, Queue, type Scope, Stream } from 'effect'
import { REALM_HOOK_TIMEOUT_MS } from './realm-hooks.ts'

export interface ContractEnv {
  /**
   * The adapter under test, pre-acquired by the factory to its `self`
   * identity. Tests exercise it only through this surface.
   */
  readonly comms: AgentComms
  /**
   * Make a channel with the given display name available to the adapter.
   * Returns the ref tests use to address it.
   */
  readonly seedChannel: (name: string) => Effect.Effect<ChannelRef>
  /**
   * Make a peer identity available — populated in `directory.listAgents`
   * / `listHumans`, resolvable by `identity.resolve`. The factory chooses
   * a kind appropriate to the substrate (memory: caller-controlled; Zulip:
   * driven by the seeded user record).
   */
  readonly seedAgent: (name: string) => Effect.Effect<Identity>
  /**
   * Optional. Construct a fresh, unauthenticated adapter on the same
   * substrate for lifecycle tests that exercise acquire/release state
   * transitions from a clean start. Substrates whose `acquire` can
   * dynamically bind any name (Memory, post-rework Zulip with the
   * minter wired up) should provide this; substrates whose
   * acquire-acceptable names are pinned by external configuration
   * (transitional Zulip, future Discord) can omit it and the
   * suite skips the rebind/no-op tests.
   *
   * Test owns disposal of any extra adapters it spins up; the factory's
   * `dispose` still runs once per test and tears down what the factory
   * itself stood up.
   */
  readonly newUnacquiredAdapter?: () => Effect.Effect<AgentComms>
  /**
   * Optional. When set together with `newUnacquiredAdapter`, calling
   * `acquire(unacquirableName)` on a freshly-built adapter throws
   * `UnknownIdentity`. Substrates that can always mint omit this;
   * substrates with a pre-provisioned pool (Discord, Memory in
   * allowlist mode) opt in by setting it.
   */
  readonly unacquirableName?: string
  /**
   * Optional. Set when the substrate's message timestamps are too coarse to
   * distinguish messages posted back-to-back — Zulip stamps integer
   * **seconds**, so three posts inside one second share a `ts` and the
   * range-filter / single-timestamp assertions (which depend on distinct
   * per-message timestamps) can't hold. Substrates with fine-grained
   * timestamps (Memory) omit it and the suite runs those tests; the filtering
   * logic stays covered there. (The `…past every message` range/replay tests
   * offset by an hour, so they run regardless.)
   */
  readonly coarseTimestamps?: boolean
  /**
   * Optional. Set when the substrate does not deliver events about the
   * current identity's *own* posts/mentions back to its own inbox within the
   * contract's window — on the shared live Zulip realm the inline readiness
   * window is too tight and the minter-side `is:mentioned` narrow is keyed to
   * the queue owner, so a bot's self-authored post/mention doesn't surface on
   * its own `events()`. Substrates that loop self-events back (Memory) omit
   * it and the suite runs the readiness + self-mention tests; cross-identity
   * event delivery and mention *suppression* are exercised regardless.
   * `realm.live.test.ts` owns the live event-delivery coverage proper.
   */
  readonly noSelfEventDelivery?: boolean
  /** Tear down whatever the factory stood up. Called once per test. */
  readonly dispose: () => Effect.Effect<void>
}

export type ContractFactory = () => Promise<ContractEnv>

const findByBody = (
  messages: ReadonlyArray<{ readonly body: string }>,
  body: string,
): { readonly body: string } | undefined => messages.find((m) => m.body === body)

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

export const runAgentCommsContract = (label: string, factory: ContractFactory): void => {
  describe(`AgentComms contract — ${label}`, () => {
    let env: ContractEnv

    beforeEach(async () => {
      env = await factory()
    }, REALM_HOOK_TIMEOUT_MS)

    afterEach(() => Effect.runPromise(env.dispose()), REALM_HOOK_TIMEOUT_MS)

    test('publisher.post returns a MessageRef whose channel matches the destination', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const ref = yield* env.comms.publisher.post(channel, decodeMessageBodySync('hello world'))
          expect(ref.channel).toEqual(channel)
        }),
      ))

    // Substrates must surface unknown-channel failures explicitly. Zulip in
    // particular routes the failure to Notification Bot DMs and returns a
    // success-shaped response — the adapter's job is to pre-flight the channel
    // and throw before the substrate has a chance to swallow the error.
    test('publisher.post on an unknown channel fails with UnknownChannel', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const phantom: ChannelRef = {
            id: decodeChannelIdSync('999999999'),
            name: decodeChannelNameSync('phantom-channel-never-seeded'),
          }
          const error = yield* Effect.flip(
            env.comms.publisher.post(phantom, decodeMessageBodySync('should fail')),
          )
          expect(error).toBeInstanceOf(UnknownChannel)
        }),
      ))

    // Compare against the refs `seedChannel` returns rather than the literal
    // names: a substrate may namespace the underlying channel (the live Zulip
    // factory prefixes per-test, comms-e5vm.5). The neutral invariant is that
    // a seeded channel is enumerated, keyed by the name the substrate assigned.
    test('directory.listChannels enumerates seeded channels by name', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const alpha = yield* env.seedChannel('alpha')
          const bravo = yield* env.seedChannel('bravo')
          const channels = yield* env.comms.directory.listChannels()
          const names = channels.map((c) => String(c.name))
          expect(names).toContain(String(alpha.name))
          expect(names).toContain(String(bravo.name))
        }),
      ))

    test('history.readChannel surfaces a posted message with sender = currentIdentity', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const me = yield* env.comms.identity.currentIdentity()
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('round-trip'))
          const messages = yield* env.comms.history.readChannel(channel, {})
          const found = messages.find((m) => m.body === 'round-trip')
          expect(found).toBeDefined()
          expect(found?.sender.id).toEqual(me.id)
        }),
      ))

    test('publisher.post with thread is visible via history.readThread', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const thread = decodeThreadNameSync('design')
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('in thread'), {
            thread: { name: thread },
          })
          const messages = yield* env.comms.history.readThread(channel, thread, {})
          const found = findByBody(messages, 'in thread')
          expect(found).toBeDefined()
        }),
      ))

    test('publisher.post with thread is also visible via history.readChannel (channel reads include all topics)', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('topic-a'), {
            thread: { name: decodeThreadNameSync('alpha') },
          })
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('topic-b'), {
            thread: { name: decodeThreadNameSync('bravo') },
          })
          const messages = yield* env.comms.history.readChannel(channel, {})
          const bodies = messages.map((m) => m.body)
          expect(bodies).toContain(decodeMessageBodySync('topic-a'))
          expect(bodies).toContain(decodeMessageBodySync('topic-b'))
        }),
      ))

    test('publisher.post with mentions surfaces them via history.readChannel as Identity[]', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const alice = yield* env.seedAgent('alice')
          yield* env.comms.publisher.post(
            channel,
            decodeMessageBodySync(`@**${alice.name}** wake up`),
            {
              mentions: [alice],
            },
          )
          const messages = yield* env.comms.history.readChannel(channel, {})
          const found = messages.find((m) => m.body.includes('wake up'))
          expect(found).toBeDefined()
          expect(found?.mentions.map((m) => m.id)).toEqual([alice.id])
        }),
      ))

    // PostOpts.mentions is metadata-only: adapters must not mutate `body`
    // based on the array. Cross-adapter spec — both Zulip and memory must
    // round-trip the literal body string through history.readChannel even
    // when mentions[] is set.
    test('publisher.post does not mutate body based on opts.mentions', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const alice = yield* env.seedAgent('alice')
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('oi look here'), {
            mentions: [alice],
          })
          const messages = yield* env.comms.history.readChannel(channel, {})
          const found = messages.find((m) => m.body === 'oi look here')
          expect(found).toBeDefined()
        }),
      ))

    // Regression for comms-izp. Caller writes `@**Name**` inline AND passes
    // mentions[] for the same identity. Body must round-trip with exactly
    // one occurrence of the mention markup, not two.
    test('publisher.post with inline @-mention markup AND opts.mentions does not double the markup in body', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const alice = yield* env.seedAgent('alice')
          const body = decodeMessageBodySync(`@**${alice.name}** look at this`)
          yield* env.comms.publisher.post(channel, body, { mentions: [alice] })
          const messages = yield* env.comms.history.readChannel(channel, {})
          const found = messages.find((m) => m.body === body)
          if (found === undefined) throw new Error('expected posted message in channel history')
          const occurrences = found.body.split(`@**${alice.name}**`).length - 1
          expect(occurrences).toBe(1)
        }),
      ))

    test('publisher.post with replyTo does not throw (substrate may quote-block or drop)', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const parentRef = yield* env.comms.publisher.post(
            channel,
            decodeMessageBodySync('parent'),
          )
          const followUp: MessageRef = yield* env.comms.publisher.post(
            channel,
            decodeMessageBodySync('child'),
            { replyTo: parentRef },
          )
          expect(followUp.channel).toEqual(channel)
        }),
      ))

    test('publisher.edit replaces the body, surfaced via history.readChannel under the same MessageRef', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const ref = yield* env.comms.publisher.post(
            channel,
            decodeMessageBodySync('original body'),
          )
          yield* env.comms.publisher.edit(ref, decodeMessageBodySync('replacement body'))
          const messages = yield* env.comms.history.readChannel(channel, {})
          const found = messages.find((m) => String(m.ref.id) === String(ref.id))
          if (found === undefined) throw new Error('expected edited message in channel history')
          expect(found.body).toBe(decodeMessageBodySync('replacement body'))
          // Original body must be gone — edit is replace, not append.
          const original = messages.find((m) => m.body === 'original body')
          expect(original).toBeUndefined()
        }),
      ))

    test('publisher.edit on a thread message keeps the thread association', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const thread = decodeThreadNameSync('design')
          const ref = yield* env.comms.publisher.post(channel, decodeMessageBodySync('first cut'), {
            thread: { name: thread },
          })
          yield* env.comms.publisher.edit(ref, decodeMessageBodySync('second cut'))
          const messages = yield* env.comms.history.readThread(channel, thread, {})
          const found = messages.find((m) => String(m.ref.id) === String(ref.id))
          if (found === undefined) throw new Error('expected edited message in thread history')
          expect(found.body).toBe(decodeMessageBodySync('second cut'))
        }),
      ))

    // Editing a message the substrate has no record of is a non-fatal
    // domain failure, not a programmer error: it surfaces on the typed E
    // channel as a PublisherError, never as a defect. Zulip's PATCH on an
    // unknown id 400s into PublisherError; the memory adapter mints the same
    // shape when its message store has no such id.
    test('publisher.edit on an unknown message fails with PublisherError', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const phantom: MessageRef = {
            id: decodeMessageIdSync('999999999'),
            channel,
          }
          const error = yield* Effect.flip(
            env.comms.publisher.edit(phantom, decodeMessageBodySync('nope')),
          )
          expect(error).toBeInstanceOf(PublisherError)
        }),
      ))

    test('publisher.react then unreact leaves no observable error', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const ref = yield* env.comms.publisher.post(
            channel,
            decodeMessageBodySync('react target'),
          )
          yield* env.comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
          yield* env.comms.publisher.unreact(ref, decodeEmojiSync('thumbs_up'))
        }),
      ))

    test('publisher.react surfaces on history.readChannel as Reaction with by=[self]', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const me = yield* env.comms.identity.currentIdentity()
          const ref = yield* env.comms.publisher.post(
            channel,
            decodeMessageBodySync('reaction round-trip'),
          )
          yield* env.comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
          const messages = yield* env.comms.history.readChannel(channel, {})
          const found = messages.find((m) => m.body === 'reaction round-trip')
          if (found === undefined) throw new Error('expected reaction target in channel history')
          const thumbs = found.reactions.find((r) => r.emoji === 'thumbs_up')
          if (thumbs === undefined) throw new Error('expected thumbs_up reaction on message')
          expect(thumbs.by.map((i) => i.id)).toEqual([me.id])
        }),
      ))

    test('publisher.unreact removes the reaction from history.readChannel', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const ref = yield* env.comms.publisher.post(
            channel,
            decodeMessageBodySync('reaction removal'),
          )
          yield* env.comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
          yield* env.comms.publisher.unreact(ref, decodeEmojiSync('thumbs_up'))
          const messages = yield* env.comms.history.readChannel(channel, {})
          const found = messages.find((m) => m.body === 'reaction removal')
          if (found === undefined)
            throw new Error('expected reaction-removal target in channel history')
          expect(found.reactions.find((r) => r.emoji === 'thumbs_up')).toBeUndefined()
        }),
      ))

    test('history.readChannel reports reactions=[] for a message with no reactions', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('plain message'))
          const messages = yield* env.comms.history.readChannel(channel, {})
          const found = messages.find((m) => m.body === 'plain message')
          if (found === undefined) throw new Error('expected plain message in channel history')
          expect(found.reactions).toEqual([])
        }),
      ))

    test('history.readChannel range.since is inclusive', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          if (env.coarseTimestamps) return
          const channel = yield* env.seedChannel('lobby')
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('before'))
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('pivot'))
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('after'))
          const everything = yield* env.comms.history.readChannel(channel, {})
          const pivot = everything.find((m) => m.body === 'pivot')
          if (pivot === undefined) throw new Error('expected pivot message in channel history')
          const filtered = yield* env.comms.history.readChannel(channel, { since: pivot.ts })
          const bodies = filtered.map((m) => m.body)
          expect(bodies).toContain(decodeMessageBodySync('pivot'))
          expect(bodies).not.toContain(decodeMessageBodySync('before'))
        }),
      ))

    test('history.readChannel range.until is inclusive', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          if (env.coarseTimestamps) return
          const channel = yield* env.seedChannel('lobby')
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('before'))
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('pivot'))
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('after'))
          const everything = yield* env.comms.history.readChannel(channel, {})
          const pivot = everything.find((m) => m.body === 'pivot')
          if (pivot === undefined) throw new Error('expected pivot message in channel history')
          const filtered = yield* env.comms.history.readChannel(channel, { until: pivot.ts })
          const bodies = filtered.map((m) => m.body)
          expect(bodies).toContain(decodeMessageBodySync('pivot'))
          expect(bodies).not.toContain(decodeMessageBodySync('after'))
        }),
      ))

    test('history.readChannel range.since == range.until selects exactly one timestamp', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          if (env.coarseTimestamps) return
          const channel = yield* env.seedChannel('lobby')
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('before'))
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('pivot'))
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('after'))
          const everything = yield* env.comms.history.readChannel(channel, {})
          const pivot = everything.find((m) => m.body === 'pivot')
          if (pivot === undefined) throw new Error('expected pivot message in channel history')
          const filtered = yield* env.comms.history.readChannel(channel, {
            since: pivot.ts,
            until: pivot.ts,
          })
          const bodies = filtered.map((m) => m.body)
          expect(bodies).toEqual([decodeMessageBodySync('pivot')])
        }),
      ))

    test('directory.listAgents includes currentIdentity when the bot is kind=agent', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const me = yield* env.comms.identity.currentIdentity()
          if (me.kind !== 'agent') return
          const agents = yield* env.comms.directory.listAgents()
          expect(agents.map((a) => a.id)).toContain(me.id)
        }),
      ))

    test('identity.resolve returns the identity for a known seeded peer', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const alice = yield* env.seedAgent('alice')
          const found = yield* env.comms.identity.resolve(alice.name)
          expect(Option.getOrUndefined(found)?.id).toEqual(alice.id)
        }),
      ))

    test('identity.resolve returns undefined for an unknown name', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const found = yield* env.comms.identity.resolve(decodeDisplayNameSync('does-not-exist'))
          expect(Option.isNone(found)).toBe(true)
        }),
      ))

    test('identity.acquire on the bound name is idempotent — returns the same identity', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const me = yield* env.comms.identity.currentIdentity()
          const result = yield* env.comms.identity.acquire(decodeBotNameSync(me.name))
          expect(result.identity.id).toEqual(me.id)
          expect(result.identity.name).toEqual(me.name)
        }),
      ))

    test('identity.acquire on a different name rejects when already bound', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const me = yield* env.comms.identity.currentIdentity()
          const otherName = decodeBotNameSync(`${me.name}-conflict`)
          const exit = yield* Effect.exit(env.comms.identity.acquire(otherName))
          expect(Exit.isFailure(exit)).toBe(true)
        }),
      ))

    test('identity.release on an unauthenticated adapter is a no-op (does not throw)', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const build = env.newUnacquiredAdapter
          if (build === undefined) return
          const fresh = yield* build()
          yield* fresh.identity.release()
        }),
      ))

    test('identity.release then identity.acquire(name) rebinds the adapter to that name', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const build = env.newUnacquiredAdapter
          if (build === undefined) return
          const fresh = yield* build()
          const current = yield* env.comms.identity.currentIdentity()
          const rawName = `${current.name}-cycle`
          const acquireName = decodeBotNameSync(rawName)
          yield* fresh.identity.acquire(acquireName)
          const before = yield* fresh.identity.currentIdentity()
          expect(before.name).toEqual(decodeDisplayNameSync(rawName))
          yield* fresh.identity.release()
          yield* fresh.identity.acquire(acquireName)
          const after = yield* fresh.identity.currentIdentity()
          expect(after.name).toEqual(decodeDisplayNameSync(rawName))
        }),
      ))

    test('identity.acquire fails with UnknownIdentity for a name the substrate cannot bind', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const target = env.unacquirableName
          const build = env.newUnacquiredAdapter
          if (target === undefined || build === undefined) return
          const fresh = yield* build()
          const error = yield* Effect.flip(fresh.identity.acquire(decodeBotNameSync(target)))
          expect(error).toBeInstanceOf(UnknownIdentity)
        }),
      ))

    test('range.since past every message yields an empty result', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('only'))
          const everything = yield* env.comms.history.readChannel(channel, {})
          const last = everything[everything.length - 1]
          if (last === undefined)
            throw new Error('expected at least one message in channel history')
          const future = decodeTimestampSync(last.ts + 3600)
          const filtered = yield* env.comms.history.readChannel(channel, { since: future })
          expect(filtered).toEqual([])
        }),
      ))

    // The returned name must reflect (derive from) the seed input; a substrate
    // may namespace it (live Zulip prefixes per-test, comms-e5vm.5), so the
    // neutral invariant is containment of the seed input, not exact equality.
    test('seedChannel returns a ChannelRef whose name matches the seed input', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('design-talk')
          expect(String(channel.name)).toContain('design-talk')
        }),
      ))

    test('inbox.subscribe(channel) + post yields message-posted with sender = currentIdentity', () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const channel = yield* env.seedChannel('lobby')
            const me = yield* env.comms.identity.currentIdentity()
            yield* env.comms.inbox.subscribe(channel)
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(channel, decodeMessageBodySync('event-target'))
            const event = yield* Queue.take(queue)
            expect(event.kind).toBe('message-posted')
            if (event.kind !== 'message-posted') throw new Error('unexpected event kind')
            expect(event.message.body).toContain('event-target')
            expect(event.message.sender.id).toEqual(me.id)
            expect(event.message.ref.channel.id).toEqual(channel.id)
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
            if (env.noSelfEventDelivery) return
            const channel = yield* env.seedChannel('lobby')
            yield* env.comms.inbox.subscribe(channel)
            yield* env.comms.publisher.post(channel, decodeMessageBodySync('pre-iterate'))
            const queue = yield* eventQueue(env.comms)
            const result = yield* Queue.take(queue).pipe(Effect.timeoutOption(Duration.seconds(2)))
            if (Option.isNone(result))
              throw new Error('events() did not observe post made after subscribe resolved')
            const event = result.value
            expect(event.kind).toBe('message-posted')
            if (event.kind !== 'message-posted')
              throw new Error(`expected message-posted, got ${event.kind}`)
            expect(event.message.body).toContain('pre-iterate')
          }),
        ),
      ))

    test('post-acquire, a self-mention flows via inbox.events without an explicit subscribe', () =>
      // Universal rule (comms-5kx): every adapter, on identity.acquire,
      // implicitly registers the mentions narrow. Callers do not need to
      // subscribe('mentions') — @-mention delivery is the floor of
      // substrate participation, not a per-bot preference.
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            if (env.noSelfEventDelivery) return
            const channel = yield* env.seedChannel('lobby')
            const me = yield* env.comms.identity.currentIdentity()
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(
              channel,
              decodeMessageBodySync(`@**${me.name}** wake up`),
              {
                mentions: [me],
              },
            )
            const first = yield* Queue.take(queue)
            const second = yield* Queue.take(queue)
            expect(first.kind).toBe('message-posted')
            expect(second.kind).toBe('mention-received')
            if (second.kind === 'mention-received') {
              expect(second.mentions.map((m: Identity) => m.id)).toContain(me.id)
            }
          }),
        ),
      ))

    test('inbox.subscribe("mentions") + post with self mention yields message-posted + mention-received', () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            if (env.noSelfEventDelivery) return
            const channel = yield* env.seedChannel('lobby')
            const me = yield* env.comms.identity.currentIdentity()
            yield* env.comms.inbox.subscribe('mentions')
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(
              channel,
              decodeMessageBodySync(`@**${me.name}** wake up`),
              {
                mentions: [me],
              },
            )
            const first = yield* Queue.take(queue)
            const second = yield* Queue.take(queue)
            expect(first.kind).toBe('message-posted')
            expect(second.kind).toBe('mention-received')
            if (second.kind === 'mention-received') {
              expect(second.mentions.map((m: Identity) => m.id)).toContain(me.id)
            }
          }),
        ),
      ))

    test('inbox.subscribe("mentions") suppresses posts that do not mention current identity', () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const channel = yield* env.seedChannel('lobby')
            const alice = yield* env.seedAgent('alice')
            yield* env.comms.inbox.subscribe('mentions')
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(channel, decodeMessageBodySync('hello alice'), {
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
            const channel = yield* env.seedChannel('lobby')
            const me = yield* env.comms.identity.currentIdentity()
            yield* env.comms.inbox.subscribe(channel)
            const queue = yield* eventQueue(env.comms)
            const ref = yield* env.comms.publisher.post(
              channel,
              decodeMessageBodySync('react target'),
            )
            // Drain message-posted first so the cache that backs reaction
            // target resolution is populated before the reaction lands.
            const posted = yield* Queue.take(queue)
            expect(posted.kind).toBe('message-posted')

            yield* env.comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
            const reacted = yield* Queue.take(queue)
            expect(reacted.kind).toBe('reaction-added')
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
      // the iterator. Posting BEFORE subscribe() means no message-posted ever
      // flows through events(), so the cache is empty when the reaction lands.
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const channel = yield* env.seedChannel('lobby')
            const me = yield* env.comms.identity.currentIdentity()
            const ref = yield* env.comms.publisher.post(
              channel,
              decodeMessageBodySync('pre-subscribe target'),
            )
            yield* env.comms.inbox.subscribe(channel)
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
            const reacted = yield* Queue.take(queue)
            expect(reacted.kind).toBe('reaction-added')
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
            const channel = yield* env.seedChannel('lobby')
            const me = yield* env.comms.identity.currentIdentity()
            yield* env.comms.inbox.subscribe(channel)
            const queue = yield* eventQueue(env.comms)
            const ref = yield* env.comms.publisher.post(
              channel,
              decodeMessageBodySync('react then unreact'),
            )
            const posted = yield* Queue.take(queue)
            expect(posted.kind).toBe('message-posted')

            yield* env.comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
            const added = yield* Queue.take(queue)
            expect(added.kind).toBe('reaction-added')

            yield* env.comms.publisher.unreact(ref, decodeEmojiSync('thumbs_up'))
            const removed = yield* Queue.take(queue)
            expect(removed.kind).toBe('reaction-removed')
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
            const channel = yield* env.seedChannel('lobby')
            yield* env.comms.inbox.subscribe(channel)
            yield* env.comms.inbox.unsubscribe(channel)
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(channel, decodeMessageBodySync('should-not-arrive'))
            const result = yield* Queue.take(queue).pipe(Effect.timeoutOption(Duration.millis(100)))
            expect(Option.isNone(result)).toBe(true)
          }),
        ),
      ))

    test('inbox.replay(since) returns past message-posted events with ts >= since', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          if (env.coarseTimestamps) return
          const channel = yield* env.seedChannel('lobby')
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('before-pivot'))
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('pivot'))
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('after-pivot'))
          const everything = yield* env.comms.history.readChannel(channel, {})
          const pivot = everything.find((m) => m.body === 'pivot')
          if (pivot === undefined) throw new Error('expected pivot message in channel history')
          const events = yield* env.comms.inbox.replay(pivot.ts)
          const postedBodies = events
            .filter(
              (e): e is Extract<InboundEvent, { kind: 'message-posted' }> =>
                e.kind === 'message-posted',
            )
            .map((e) => e.message.body)
          expect(postedBodies).toContain(decodeMessageBodySync('pivot'))
          expect(postedBodies).toContain(decodeMessageBodySync('after-pivot'))
          expect(postedBodies).not.toContain(decodeMessageBodySync('before-pivot'))
        }),
      ))

    test('inbox.subscribe(new-topics-in-channel) yields the first message of a fresh topic', () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const channel = yield* env.seedChannel('lobby')
            yield* env.comms.inbox.subscribe({ kind: 'new-topics-in-channel', channel })
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(channel, decodeMessageBodySync('first in alpha'), {
              thread: { name: decodeThreadNameSync('alpha') },
            })
            const event = yield* Queue.take(queue)
            expect(event.kind).toBe('message-posted')
            if (event.kind !== 'message-posted')
              throw new Error(`expected message-posted, got ${event.kind}`)
            expect(event.message.body).toContain('first in alpha')
            expect(event.message.ref.thread?.name).toEqual(decodeThreadNameSync('alpha'))
          }),
        ),
      ))

    test('inbox.subscribe(new-topics-in-channel) suppresses subsequent messages in the same topic', () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const channel = yield* env.seedChannel('lobby')
            yield* env.comms.inbox.subscribe({ kind: 'new-topics-in-channel', channel })
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(channel, decodeMessageBodySync('first in alpha'), {
              thread: { name: decodeThreadNameSync('alpha') },
            })
            const first = yield* Queue.take(queue)
            expect(first.kind).toBe('message-posted')
            if (first.kind !== 'message-posted') throw new Error('expected first message-posted')
            expect(first.message.body).toContain('first in alpha')

            yield* env.comms.publisher.post(channel, decodeMessageBodySync('second in alpha'), {
              thread: { name: decodeThreadNameSync('alpha') },
            })
            const result = yield* Queue.take(queue).pipe(Effect.timeoutOption(Duration.millis(200)))
            expect(Option.isNone(result)).toBe(true)
          }),
        ),
      ))

    test('inbox.subscribe(new-topics-in-channel) yields the first message of a second fresh topic', () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const channel = yield* env.seedChannel('lobby')
            yield* env.comms.inbox.subscribe({ kind: 'new-topics-in-channel', channel })
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(channel, decodeMessageBodySync('first in alpha'), {
              thread: { name: decodeThreadNameSync('alpha') },
            })
            const first = yield* Queue.take(queue)
            expect(first.kind).toBe('message-posted')

            yield* env.comms.publisher.post(channel, decodeMessageBodySync('second in alpha'), {
              thread: { name: decodeThreadNameSync('alpha') },
            })
            yield* env.comms.publisher.post(channel, decodeMessageBodySync('first in bravo'), {
              thread: { name: decodeThreadNameSync('bravo') },
            })
            const next = yield* Queue.take(queue)
            expect(next.kind).toBe('message-posted')
            if (next.kind !== 'message-posted') throw new Error('expected second message-posted')
            expect(next.message.body).toContain('first in bravo')
            expect(next.message.ref.thread?.name).toEqual(decodeThreadNameSync('bravo'))
          }),
        ),
      ))

    test('inbox.unsubscribe(new-topics-in-channel) suppresses future first-message events', () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const channel = yield* env.seedChannel('lobby')
            const target = { kind: 'new-topics-in-channel' as const, channel }
            yield* env.comms.inbox.subscribe(target)
            yield* env.comms.inbox.unsubscribe(target)
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(channel, decodeMessageBodySync('should-not-arrive'), {
              thread: { name: decodeThreadNameSync('alpha') },
            })
            const result = yield* Queue.take(queue).pipe(Effect.timeoutOption(Duration.millis(200)))
            expect(Option.isNone(result)).toBe(true)
          }),
        ),
      ))

    test('inbox.replay(since) past every message yields an empty result', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          yield* env.comms.publisher.post(channel, decodeMessageBodySync('only'))
          const everything = yield* env.comms.history.readChannel(channel, {})
          const last = everything[everything.length - 1]
          if (last === undefined)
            throw new Error('expected at least one message in channel history')
          const future = decodeTimestampSync(last.ts + 3600)
          const events = yield* env.comms.inbox.replay(future)
          expect(events).toEqual([])
        }),
      ))
  })
}
