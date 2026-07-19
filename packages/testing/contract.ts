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
import type {
  AgentComms,
  ChannelName,
  ChannelRef,
  Identity,
  InboundEvent,
  MessageBody,
  MessageRef,
  PostOpts,
} from '@commy/core/ports'
import {
  ChannelDescriptionRejected,
  decodeBotNameSync,
  decodeChannelDescriptionSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeEmojiSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  decodeTimestampSync,
  MessagePermalinkSchema,
  PublisherError,
  UnknownChannel,
  UnknownIdentity,
  type UnresolvedMention,
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
   * Optional. Post a message authored by a seeded peer (sender ≠ self) into a
   * channel the bound self can observe. Drives the mention-floor tests:
   * a peer @-mentions self and self's `events()` must yield
   * `mention-received`. This is the cross-identity shape `realm.live.test.ts`
   * proves with observer ≠ sender — the contract can't express it through
   * `comms.publisher.post` alone because that always authors as self, and a
   * single-identity self-mention never surfaces on a substrate whose mention
   * narrow is keyed to the queue owner (live Zulip).
   *
   * Memory injects a peer-authored message straight into the same in-process
   * substrate; Zulip posts through a peer-bound adapter. Substrates that
   * cannot stand up a posting peer omit it and the suite skips the
   * peer-mention tests. The `peer` is one obtained from `seedAgent`.
   */
  readonly peerPost?: (
    peer: Identity,
    channel: ChannelName,
    body: MessageBody,
    opts?: PostOpts,
  ) => Effect.Effect<void>
  /**
   * Longest channel description this substrate stores, in characters. The
   * description tests build their over-length input from it rather than
   * assuming any substrate's number — the contract asserts that a substrate
   * *refuses* what it cannot hold, not what the cap happens to be. Each
   * substrate reports its own (Zulip 1024, memory its configured limit).
   */
  readonly channelDescriptionLimit: number
  /** Tear down whatever the factory stood up. Called once per test. */
  readonly dispose: () => Effect.Effect<void>
}

export type ContractFactory = () => Promise<ContractEnv>

const findByBody = (
  messages: ReadonlyArray<{ readonly body: string }>,
  body: string,
): { readonly body: string } | undefined => messages.find((m) => m.body === body)

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
 * precedes it (it does on Memory, may not on a mentions-only live narrow).
 */
const takeUntil = (
  queue: Queue.Queue<InboundEvent>,
  predicate: (event: InboundEvent) => boolean,
): Effect.Effect<InboundEvent> =>
  Queue.take(queue).pipe(
    Effect.flatMap((event) =>
      predicate(event) ? Effect.succeed(event) : takeUntil(queue, predicate),
    ),
  )

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
 * Post `bodies` in order, spacing consecutive posts by the substrate's
 * `timestampGranularity` so each lands on a distinct `Timestamp`. Memory
 * reports `Duration.zero` (its `ts` counter already separates posts) so the
 * sleep is a no-op there and the fixture stays fast; live Zulip reports 1s so
 * the three posts straddle distinct integer seconds and the range/replay
 * filters assert at full strength on the real adapter. The first post is never
 * delayed.
 */
const postSpaced = (
  comms: AgentComms,
  channel: ChannelName,
  bodies: ReadonlyArray<MessageBody>,
): Effect.Effect<void, UnknownChannel | UnresolvedMention | PublisherError> =>
  Effect.forEach(
    bodies,
    (body, index) =>
      (index === 0 ? Effect.void : Effect.sleep(comms.capabilities.timestampGranularity)).pipe(
        Effect.zipRight(comms.publisher.post(channel, body)),
      ),
    { discard: true },
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
          const ref = yield* env.comms.publisher.post(
            channel.name,
            decodeMessageBodySync('hello world'),
          )
          // Channel identity, not the whole ref: a substrate may decorate the
          // returned ChannelRef (e.g. Zulip hangs a permalink off it)
          // beyond the bare {id,name} the seed factory hands back.
          expect(ref.channel.id).toEqual(channel.id)
          expect(ref.channel.name).toEqual(channel.name)
        }),
      ))

    // Substrates must surface unknown-channel failures explicitly. Zulip in
    // particular routes the failure to Notification Bot DMs and returns a
    // success-shaped response — the adapter's job is to pre-flight the channel
    // and throw before the substrate has a chance to swallow the error.
    test('publisher.post on an unknown channel fails with UnknownChannel', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const phantom = decodeChannelNameSync('phantom-channel-never-seeded')
          const error = yield* Effect.flip(
            env.comms.publisher.post(phantom, decodeMessageBodySync('should fail')),
          )
          expect(error).toBeInstanceOf(UnknownChannel)
        }),
      ))

    // Compare against the refs `seedChannel` returns rather than the literal
    // names: a substrate may namespace the underlying channel (the live Zulip
    // factory prefixes per-test). The neutral invariant is that
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
          yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('round-trip'))
          const messages = yield* env.comms.history.readChannel(channel.name, {})
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
          yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('in thread'), {
            thread,
          })
          const messages = yield* env.comms.history.readThread(channel.name, thread, {})
          const found = findByBody(messages, 'in thread')
          expect(found).toBeDefined()
        }),
      ))

    // Resolution is a status distinct from the thread name: the name a consumer
    // addresses stays stable, and the flag surfaces on read as
    // ObservedThread.resolved. These hold on both substrates — memory tracks the
    // flag directly, Zulip renames the topic behind the adapter seam and strips
    // it back off on read.
    test('publisher.resolveThread marks a thread resolved, observable via history.readThread', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const thread = decodeThreadNameSync('design')
          yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('to resolve'), {
            thread,
          })
          yield* env.comms.publisher.resolveThread(channel.name, thread)
          const messages = yield* env.comms.history.readThread(channel.name, thread, {})
          const found = messages.find((m) => m.body === 'to resolve')
          if (found === undefined)
            throw new Error('expected the resolved thread still readable by its plain name')
          // The name is unchanged; the resolved status rides alongside it.
          expect(Option.map(found.ref.thread, (t) => t.name)).toEqual(Option.some(thread))
          expect(Option.map(found.ref.thread, (t) => t.resolved)).toEqual(Option.some(true))
        }),
      ))

    test("publisher.unresolveThread clears a thread's resolved status", () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const thread = decodeThreadNameSync('design')
          yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('round trip'), {
            thread,
          })
          yield* env.comms.publisher.resolveThread(channel.name, thread)
          yield* env.comms.publisher.unresolveThread(channel.name, thread)
          const messages = yield* env.comms.history.readThread(channel.name, thread, {})
          const found = messages.find((m) => m.body === 'round trip')
          if (found === undefined) throw new Error('expected the thread readable after unresolve')
          expect(Option.map(found.ref.thread, (t) => t.resolved)).toEqual(Option.some(false))
        }),
      ))

    test('publisher.resolveThread is idempotent when the thread is already resolved', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const thread = decodeThreadNameSync('design')
          yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('twice resolved'), {
            thread,
          })
          yield* env.comms.publisher.resolveThread(channel.name, thread)
          // Second resolve is a no-op, not an error.
          yield* env.comms.publisher.resolveThread(channel.name, thread)
          const messages = yield* env.comms.history.readThread(channel.name, thread, {})
          const found = messages.find((m) => m.body === 'twice resolved')
          if (found === undefined) throw new Error('expected the thread readable after re-resolve')
          expect(Option.map(found.ref.thread, (t) => t.resolved)).toEqual(Option.some(true))
        }),
      ))

    test('publisher.unresolveThread is idempotent when the thread is not resolved', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const thread = decodeThreadNameSync('design')
          yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('never resolved'), {
            thread,
          })
          // Unresolving an unresolved thread is a no-op, not an error.
          yield* env.comms.publisher.unresolveThread(channel.name, thread)
          const messages = yield* env.comms.history.readThread(channel.name, thread, {})
          const found = messages.find((m) => m.body === 'never resolved')
          if (found === undefined)
            throw new Error('expected the thread readable after no-op unresolve')
          expect(Option.map(found.ref.thread, (t) => t.resolved)).toEqual(Option.some(false))
        }),
      ))

    test('publisher.resolveThread on a thread with no messages fails with PublisherError', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const error = yield* Effect.flip(
            env.comms.publisher.resolveThread(channel.name, decodeThreadNameSync('ghost')),
          )
          expect(error).toBeInstanceOf(PublisherError)
        }),
      ))

    // A channel's description is standing state, not a message: it is written
    // through the publisher and read back through the directory, and what goes
    // in comes out verbatim. Absence is Option.none on both substrates —
    // there is no blank-description state to tell apart from an unset one.
    test('directory.channelDescription is None for a channel nobody has described', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const description = yield* env.comms.directory.channelDescription(channel.name)
          expect(description).toEqual(Option.none())
        }),
      ))

    test('publisher.setChannelDescription round-trips verbatim through directory.channelDescription', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const charter = decodeChannelDescriptionSync('Where the lobby crowd coordinates.')
          yield* env.comms.publisher.setChannelDescription(channel.name, Option.some(charter))
          const description = yield* env.comms.directory.channelDescription(channel.name)
          expect(description).toEqual(Option.some(charter))
        }),
      ))

    test('publisher.setChannelDescription replaces an existing description', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          yield* env.comms.publisher.setChannelDescription(
            channel.name,
            Option.some(decodeChannelDescriptionSync('first charter')),
          )
          const replacement = decodeChannelDescriptionSync('second charter')
          yield* env.comms.publisher.setChannelDescription(channel.name, Option.some(replacement))
          const description = yield* env.comms.directory.channelDescription(channel.name)
          expect(description).toEqual(Option.some(replacement))
        }),
      ))

    test('publisher.setChannelDescription with None clears the description back to absent', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          yield* env.comms.publisher.setChannelDescription(
            channel.name,
            Option.some(decodeChannelDescriptionSync('to be cleared')),
          )
          yield* env.comms.publisher.setChannelDescription(channel.name, Option.none())
          const description = yield* env.comms.directory.channelDescription(channel.name)
          expect(description).toEqual(Option.none())
        }),
      ))

    test('publisher.setChannelDescription is idempotent when the description is unchanged', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const charter = decodeChannelDescriptionSync('stable charter')
          yield* env.comms.publisher.setChannelDescription(channel.name, Option.some(charter))
          // Re-writing the same text is a no-op, not an error.
          yield* env.comms.publisher.setChannelDescription(channel.name, Option.some(charter))
          const description = yield* env.comms.directory.channelDescription(channel.name)
          expect(description).toEqual(Option.some(charter))
        }),
      ))

    test('publisher.setChannelDescription with None is idempotent on an undescribed channel', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          yield* env.comms.publisher.setChannelDescription(channel.name, Option.none())
          const description = yield* env.comms.directory.channelDescription(channel.name)
          expect(description).toEqual(Option.none())
        }),
      ))

    // The failure mode that matters: a substrate refuses what it cannot store
    // rather than storing a truncated copy. The limit is the substrate's own
    // (read off the env), so this asserts the refusal, never a number.
    test('publisher.setChannelDescription refuses a description longer than the substrate stores', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const tooLong = decodeChannelDescriptionSync('x'.repeat(env.channelDescriptionLimit + 1))
          const error = yield* Effect.flip(
            env.comms.publisher.setChannelDescription(channel.name, Option.some(tooLong)),
          )
          expect(error).toBeInstanceOf(ChannelDescriptionRejected)
          expect((error as ChannelDescriptionRejected).constraint).toBe('length')
          // Refused outright — nothing truncated was left behind.
          const description = yield* env.comms.directory.channelDescription(channel.name)
          expect(description).toEqual(Option.none())
        }),
      ))

    test('publisher.setChannelDescription accepts a description exactly at the substrate limit', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const atLimit = decodeChannelDescriptionSync('x'.repeat(env.channelDescriptionLimit))
          yield* env.comms.publisher.setChannelDescription(channel.name, Option.some(atLimit))
          const description = yield* env.comms.directory.channelDescription(channel.name)
          expect(description).toEqual(Option.some(atLimit))
        }),
      ))

    test('publisher.setChannelDescription refuses a multi-line description rather than reshaping it', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const multiLine = decodeChannelDescriptionSync('first line\nsecond line')
          const error = yield* Effect.flip(
            env.comms.publisher.setChannelDescription(channel.name, Option.some(multiLine)),
          )
          expect(error).toBeInstanceOf(ChannelDescriptionRejected)
          expect((error as ChannelDescriptionRejected).constraint).toBe('format')
          const description = yield* env.comms.directory.channelDescription(channel.name)
          expect(description).toEqual(Option.none())
        }),
      ))

    test('channel-description reads and writes on an unknown channel fail with UnknownChannel', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const absent = decodeChannelNameSync('no-such-channel-for-descriptions')
          const readError = yield* Effect.flip(env.comms.directory.channelDescription(absent))
          expect(readError).toBeInstanceOf(UnknownChannel)
          const writeError = yield* Effect.flip(
            env.comms.publisher.setChannelDescription(
              absent,
              Option.some(decodeChannelDescriptionSync('never lands')),
            ),
          )
          expect(writeError).toBeInstanceOf(UnknownChannel)
        }),
      ))

    test('publisher.post with thread is also visible via history.readChannel (channel reads include all topics)', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('topic-a'), {
            thread: decodeThreadNameSync('alpha'),
          })
          yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('topic-b'), {
            thread: decodeThreadNameSync('bravo'),
          })
          const messages = yield* env.comms.history.readChannel(channel.name, {})
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
            channel.name,
            decodeMessageBodySync(`@**${alice.name}** wake up`),
            {
              mentions: [alice],
            },
          )
          const messages = yield* env.comms.history.readChannel(channel.name, {})
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
          yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('oi look here'), {
            mentions: [alice],
          })
          const messages = yield* env.comms.history.readChannel(channel.name, {})
          const found = messages.find((m) => m.body === 'oi look here')
          expect(found).toBeDefined()
        }),
      ))

    // Caller writes `@**Name**` inline and passes
    // mentions[] for the same identity. Body must round-trip with exactly
    // one occurrence of the mention markup, not two.
    test('publisher.post with inline @-mention markup and opts.mentions does not double the markup in body', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const alice = yield* env.seedAgent('alice')
          const body = decodeMessageBodySync(`@**${alice.name}** look at this`)
          yield* env.comms.publisher.post(channel.name, body, { mentions: [alice] })
          const messages = yield* env.comms.history.readChannel(channel.name, {})
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
            channel.name,
            decodeMessageBodySync('parent'),
          )
          const followUp: MessageRef = yield* env.comms.publisher.post(
            channel.name,
            decodeMessageBodySync('child'),
            { replyTo: parentRef },
          )
          expect(followUp.channel.id).toEqual(channel.id)
          expect(followUp.channel.name).toEqual(channel.name)
        }),
      ))

    test('publisher.edit replaces the body, surfaced via history.readChannel under the same MessageRef', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          const ref = yield* env.comms.publisher.post(
            channel.name,
            decodeMessageBodySync('original body'),
          )
          yield* env.comms.publisher.edit(ref, decodeMessageBodySync('replacement body'))
          const messages = yield* env.comms.history.readChannel(channel.name, {})
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
          const ref = yield* env.comms.publisher.post(
            channel.name,
            decodeMessageBodySync('first cut'),
            {
              thread,
            },
          )
          yield* env.comms.publisher.edit(ref, decodeMessageBodySync('second cut'))
          const messages = yield* env.comms.history.readThread(channel.name, thread, {})
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
            thread: Option.none(),
            // An address target rebuilt from a bare id carries the id itself as
            // a placeholder permalink (never observed, never surfaced) — the
            // same transient the MCP edge's reconstructMessageRef mints.
            permalink: MessagePermalinkSchema.make('999999999'),
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
            channel.name,
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
            channel.name,
            decodeMessageBodySync('reaction round-trip'),
          )
          yield* env.comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
          const messages = yield* env.comms.history.readChannel(channel.name, {})
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
            channel.name,
            decodeMessageBodySync('reaction removal'),
          )
          yield* env.comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
          yield* env.comms.publisher.unreact(ref, decodeEmojiSync('thumbs_up'))
          const messages = yield* env.comms.history.readChannel(channel.name, {})
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
          yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('plain message'))
          const messages = yield* env.comms.history.readChannel(channel.name, {})
          const found = messages.find((m) => m.body === 'plain message')
          if (found === undefined) throw new Error('expected plain message in channel history')
          expect(found.reactions).toEqual([])
        }),
      ))

    test('history.readChannel range.since is inclusive', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          yield* postSpaced(env.comms, channel.name, [
            decodeMessageBodySync('before'),
            decodeMessageBodySync('pivot'),
            decodeMessageBodySync('after'),
          ])
          const everything = yield* env.comms.history.readChannel(channel.name, {})
          const pivot = everything.find((m) => m.body === 'pivot')
          if (pivot === undefined) throw new Error('expected pivot message in channel history')
          const filtered = yield* env.comms.history.readChannel(channel.name, { since: pivot.ts })
          const bodies = filtered.map((m) => m.body)
          expect(bodies).toContain(decodeMessageBodySync('pivot'))
          expect(bodies).not.toContain(decodeMessageBodySync('before'))
        }),
      ))

    test('history.readChannel range.until is inclusive', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          yield* postSpaced(env.comms, channel.name, [
            decodeMessageBodySync('before'),
            decodeMessageBodySync('pivot'),
            decodeMessageBodySync('after'),
          ])
          const everything = yield* env.comms.history.readChannel(channel.name, {})
          const pivot = everything.find((m) => m.body === 'pivot')
          if (pivot === undefined) throw new Error('expected pivot message in channel history')
          const filtered = yield* env.comms.history.readChannel(channel.name, { until: pivot.ts })
          const bodies = filtered.map((m) => m.body)
          expect(bodies).toContain(decodeMessageBodySync('pivot'))
          expect(bodies).not.toContain(decodeMessageBodySync('after'))
        }),
      ))

    test('history.readChannel range.since == range.until selects exactly one timestamp', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          yield* postSpaced(env.comms, channel.name, [
            decodeMessageBodySync('before'),
            decodeMessageBodySync('pivot'),
            decodeMessageBodySync('after'),
          ])
          const everything = yield* env.comms.history.readChannel(channel.name, {})
          const pivot = everything.find((m) => m.body === 'pivot')
          if (pivot === undefined) throw new Error('expected pivot message in channel history')
          const filtered = yield* env.comms.history.readChannel(channel.name, {
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
          yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('only'))
          const everything = yield* env.comms.history.readChannel(channel.name, {})
          const last = everything[everything.length - 1]
          if (last === undefined)
            throw new Error('expected at least one message in channel history')
          const future = decodeTimestampSync(last.ts + 3600)
          const filtered = yield* env.comms.history.readChannel(channel.name, { since: future })
          expect(filtered).toEqual([])
        }),
      ))

    // The returned name must reflect (derive from) the seed input; a substrate
    // may namespace it (live Zulip prefixes per-test), so the
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
            yield* env.comms.inbox.subscribe(channel.name)
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('event-target'))
            const event = yield* awaitEvent(
              queue,
              'message-posted for the post',
              (e) => e.kind === 'message-posted' && e.message.body.includes('event-target'),
            )
            if (event.kind !== 'message-posted') throw new Error('unexpected event kind')
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
            const channel = yield* env.seedChannel('lobby')
            yield* env.comms.inbox.subscribe(channel.name)
            yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('pre-iterate'))
            const queue = yield* eventQueue(env.comms)
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
    // Self subscribes the channel (queue mode 'all') so the mention surfaces on
    // live Zulip: a bound bot's events queue is minter-owned, so its own
    // mentions surface only in mode 'all' — which a channel subscription sets,
    // not the acquire-time / subscribe('mentions') narrow. The bare floor (a
    // mention without a channel subscribe) holds on Memory but not yet on
    // Zulip.
    test('a peer mention surfaces as mention-received when self is subscribed to the channel', () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const peerPost = env.peerPost
            if (peerPost === undefined) return
            const channel = yield* env.seedChannel('lobby')
            const me = yield* env.comms.identity.currentIdentity()
            const peer = yield* env.seedAgent('alice')
            yield* env.comms.inbox.subscribe(channel.name)
            const queue = yield* eventQueue(env.comms)
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
            expect(event.mentions.map((m: Identity) => m.id)).toContain(me.id)
          }),
        ),
      ))

    // Subscribing the 'mentions' narrow alongside the channel must not suppress
    // the delivery. The channel subscribe is again what surfaces the bound
    // bot's mention on live Zulip (mode 'all').
    test('inbox.subscribe("mentions") alongside a channel subscription yields a peer mention-received', () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const peerPost = env.peerPost
            if (peerPost === undefined) return
            const channel = yield* env.seedChannel('lobby')
            const me = yield* env.comms.identity.currentIdentity()
            const peer = yield* env.seedAgent('alice')
            yield* env.comms.inbox.subscribe(channel.name)
            yield* env.comms.inbox.subscribe('mentions')
            const queue = yield* eventQueue(env.comms)
            yield* peerPost(peer, channel.name, decodeMessageBodySync(`@**${me.name}** wake up`), {
              mentions: [me],
            })
            const event = yield* awaitEvent(
              queue,
              'a peer mention of self after subscribe("mentions")',
              (e) => e.kind === 'mention-received',
            )
            if (event.kind !== 'mention-received')
              throw new Error(`expected mention-received, got ${event.kind}`)
            expect(event.mentions.map((m: Identity) => m.id)).toContain(me.id)
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
            yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('hello alice'), {
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
            yield* env.comms.inbox.subscribe(channel.name)
            const queue = yield* eventQueue(env.comms)
            const ref = yield* env.comms.publisher.post(
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

            yield* env.comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
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
            const channel = yield* env.seedChannel('lobby')
            const me = yield* env.comms.identity.currentIdentity()
            const ref = yield* env.comms.publisher.post(
              channel.name,
              decodeMessageBodySync('pre-subscribe target'),
            )
            yield* env.comms.inbox.subscribe(channel.name)
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
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
            const channel = yield* env.seedChannel('lobby')
            const me = yield* env.comms.identity.currentIdentity()
            yield* env.comms.inbox.subscribe(channel.name)
            const queue = yield* eventQueue(env.comms)
            const ref = yield* env.comms.publisher.post(
              channel.name,
              decodeMessageBodySync('react then unreact'),
            )
            yield* awaitEvent(
              queue,
              'message-posted for the react/unreact target',
              (e) => e.kind === 'message-posted' && e.message.body.includes('react then unreact'),
            )

            yield* env.comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
            yield* awaitEvent(queue, 'reaction-added', (e) => e.kind === 'reaction-added')

            yield* env.comms.publisher.unreact(ref, decodeEmojiSync('thumbs_up'))
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
            const channel = yield* env.seedChannel('lobby')
            yield* env.comms.inbox.subscribe(channel.name)
            yield* env.comms.inbox.unsubscribe(channel.name)
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(
              channel.name,
              decodeMessageBodySync('should-not-arrive'),
            )
            const result = yield* Queue.take(queue).pipe(Effect.timeoutOption(Duration.millis(100)))
            expect(Option.isNone(result)).toBe(true)
          }),
        ),
      ))

    test('inbox.replay(since) returns past message-posted events with ts >= since', () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = yield* env.seedChannel('lobby')
          yield* postSpaced(env.comms, channel.name, [
            decodeMessageBodySync('before-pivot'),
            decodeMessageBodySync('pivot'),
            decodeMessageBodySync('after-pivot'),
          ])
          const everything = yield* env.comms.history.readChannel(channel.name, {})
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
            yield* env.comms.inbox.subscribe({
              kind: 'new-topics-in-channel',
              channel: channel.name,
            })
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('first in alpha'), {
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
            const channel = yield* env.seedChannel('lobby')
            yield* env.comms.inbox.subscribe({
              kind: 'new-topics-in-channel',
              channel: channel.name,
            })
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('first in alpha'), {
              thread: decodeThreadNameSync('alpha'),
            })
            yield* awaitEvent(
              queue,
              'the first message of topic alpha',
              (e) => e.kind === 'message-posted' && e.message.body.includes('first in alpha'),
            )

            yield* env.comms.publisher.post(
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
            const channel = yield* env.seedChannel('lobby')
            yield* env.comms.inbox.subscribe({
              kind: 'new-topics-in-channel',
              channel: channel.name,
            })
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('first in alpha'), {
              thread: decodeThreadNameSync('alpha'),
            })
            yield* awaitEvent(
              queue,
              'the first message of topic alpha',
              (e) => e.kind === 'message-posted' && e.message.body.includes('first in alpha'),
            )

            yield* env.comms.publisher.post(
              channel.name,
              decodeMessageBodySync('second in alpha'),
              {
                thread: decodeThreadNameSync('alpha'),
              },
            )
            yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('first in bravo'), {
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
            const channel = yield* env.seedChannel('lobby')
            const target = { kind: 'new-topics-in-channel' as const, channel: channel.name }
            yield* env.comms.inbox.subscribe(target)
            yield* env.comms.inbox.unsubscribe(target)
            const queue = yield* eventQueue(env.comms)
            yield* env.comms.publisher.post(
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
          const channel = yield* env.seedChannel('lobby')
          yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('only'))
          const everything = yield* env.comms.history.readChannel(channel.name, {})
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
