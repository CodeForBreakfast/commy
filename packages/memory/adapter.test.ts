import { expect, test } from 'bun:test'
import type { ChannelRef } from '@commy/core/ports'
import {
  ChannelPermalinkSchema,
  decodeBotNameSync,
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  decodeTimestampSync,
  HistoryError,
  PublisherError,
  ThreadPermalinkSchema,
} from '@commy/core/ports'
import {
  Array as Arr,
  Cause,
  Chunk,
  Duration,
  Effect,
  Exit,
  Option,
  Stream,
  TestClock,
  TestContext,
} from 'effect'
import { memoryAdapter } from './adapter.ts'

const acquired = async () => {
  const adapter = await Effect.runPromise(memoryAdapter())
  await Effect.runPromise(adapter.identity.acquire(decodeBotNameSync('hermes-agent')))
  return adapter
}

const phantomChannel = decodeChannelNameSync('phantom-channel-never-seeded')
// An observation ChannelRef for the never-seeded channel — only needed to fill
// the vestigial `channel` facet of an edit *address* target (edit resolves by
// message id and never reads it).
const phantomChannelRef: ChannelRef = {
  id: decodeChannelIdSync('999999999'),
  name: phantomChannel,
  permalink: ChannelPermalinkSchema.make('memory://commy/channel/999999999'),
}

// The port types these reads as
// Effect<…, HistoryError>; the memory adapter must honour that. Zulip's
// history reads return [] for an unknown channel (the narrow matches
// nothing), so this typed-failure shape is memory-specific and lives here
// rather than in the shared contract.
test('history.readChannel on an unknown channel fails with a typed HistoryError, not a defect', async () => {
  const adapter = await acquired()
  const exit = await Effect.runPromiseExit(adapter.history.readChannel(phantomChannel, {}))
  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) {
    expect(Cause.isDie(exit.cause)).toBe(false)
    const failure = Cause.failureOption(exit.cause)
    expect(failure._tag).toBe('Some')
    if (failure._tag === 'Some') {
      expect(failure.value).toBeInstanceOf(HistoryError)
      expect(failure.value.operation).toBe('readChannel')
    }
  }
})

test('history.readThread on an unknown channel fails with a typed HistoryError, not a defect', async () => {
  const adapter = await acquired()
  const exit = await Effect.runPromiseExit(
    adapter.history.readThread(phantomChannel, decodeThreadNameSync('design'), {}),
  )
  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) {
    expect(Cause.isDie(exit.cause)).toBe(false)
    const failure = Cause.failureOption(exit.cause)
    expect(failure._tag).toBe('Some')
    if (failure._tag === 'Some') {
      expect(failure.value).toBeInstanceOf(HistoryError)
      expect(failure.value.operation).toBe('readThread')
    }
  }
})

// editing a message the store has never seen is a non-fatal domain failure:
// it surfaces as a typed PublisherError on E, never as a thrown defect. This
// mirrors the Zulip adapter, where a PATCH on an unknown id 400s into a
// PublisherError; the assertion is duplicated in the shared contract, but
// kept here too to pin that memory surfaces it on E (not the Cause channel).
test('publisher.edit on an unknown message fails with a typed PublisherError, not a defect', async () => {
  const adapter = await acquired()
  const exit = await Effect.runPromiseExit(
    adapter.publisher.edit(
      { id: decodeMessageIdSync('999999999'), channel: phantomChannelRef, thread: Option.none() },
      decodeMessageBodySync('nope'),
    ),
  )
  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) {
    expect(Cause.isDie(exit.cause)).toBe(false)
    const failure = Cause.failureOption(exit.cause)
    expect(failure._tag).toBe('Some')
    if (failure._tag === 'Some') {
      expect(failure.value).toBeInstanceOf(PublisherError)
      expect(failure.value.operation).toBe('edit')
    }
  }
})

// The mutable-state Refs pin concurrency-safety: counter allocation and the
// acquire check-then-set both span `yield*` suspension points, so under
// `Effect.all` they must still allocate distinct ids and bind exactly once.
// Sequential semantics are already covered by the contract suite; these assert
// the interleaved case the Refs make correct.

// In-memory store, no rate-limited substrate — unbounded concurrency is the
// point here: it maximises interleaving against the message-id Ref.
test('concurrent posts allocate distinct, monotonic message ids under Effect.all', async () => {
  const adapter = await acquired()
  const channel = await Effect.runPromise(adapter.seedChannel('lobby').pipe(Effect.orDie))
  const refs = await Effect.runPromise(
    Effect.all(
      Arr.makeBy(25, (i) => adapter.publisher.post(channel.name, decodeMessageBodySync(`m${i}`))),
      { concurrency: 'unbounded' },
    ),
  )
  const ids = refs.map((r) => Number(r.id)).sort((a, b) => a - b)
  expect(new Set(ids).size).toBe(25)
  // Gapless run: ids 1..25 with no duplicates or holes.
  expect(ids).toEqual(Arr.makeBy(25, (i) => i + 1))
})

test('concurrent acquire on a fresh adapter binds exactly one name', async () => {
  const adapter = await Effect.runPromise(memoryAdapter())
  const names = ['agent-a', 'agent-b', 'agent-c', 'agent-d'].map((n) => decodeBotNameSync(n))
  const exits = await Effect.runPromise(
    Effect.all(
      names.map((name) => Effect.exit(adapter.identity.acquire(name))),
      { concurrency: 'unbounded' },
    ),
  )
  const successes = exits.filter(Exit.isSuccess)
  expect(successes).toHaveLength(1)
  const bound = await Effect.runPromise(adapter.identity.currentIdentity())
  if (successes[0]?._tag === 'Success') {
    expect(successes[0].value.identity.id).toEqual(bound.id)
  }
})

test('counter state persists across operations within one constructed adapter', async () => {
  const adapter = await acquired()
  const channel = await Effect.runPromise(adapter.seedChannel('lobby').pipe(Effect.orDie))
  const first = await Effect.runPromise(
    adapter.publisher.post(channel.name, decodeMessageBodySync('a')),
  )
  const second = await Effect.runPromise(
    adapter.publisher.post(channel.name, decodeMessageBodySync('b')),
  )
  expect(Number(second.id)).toBe(Number(first.id) + 1)
})

// The timestamp seed is read from Effect's Clock, not Date.now(): under
// TestClock the first post's ts is the deterministic floor(clockMs / 1000),
// proving the seed flows through the default Clock service.
test("the timestamp seed reads from Effect's Clock (first post ts = floor(clockMs/1000))", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* TestClock.adjust(Duration.seconds(1_700_000_000))
      const adapter = yield* memoryAdapter()
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      const channel = yield* adapter.seedChannel('lobby').pipe(Effect.orDie)
      const ref = yield* adapter.publisher.post(channel.name, decodeMessageBodySync('a'))
      const [message] = yield* adapter.history.readChannel(channel.name, {})
      expect(message?.ref.id).toBe(ref.id)
      expect(message?.ts).toBe(decodeTimestampSync(1_700_000_000))
    }).pipe(Effect.provide(TestContext.TestContext)),
  ))

// The memory adapter synthesises stable permalinks so the MCP tools rig
// (tools.test.ts) can assert the field is plumbed on every surface without a
// live Zulip realm. They are deliberately fake — a memory:// URI,
// not a Zulip narrow — since the memory substrate has no real web client.
test('publisher.post synthesises stable message and channel permalinks', async () => {
  const adapter = await acquired()
  const channel = await Effect.runPromise(adapter.seedChannel('lobby').pipe(Effect.orDie))
  const ref = await Effect.runPromise(
    adapter.publisher.post(channel.name, decodeMessageBodySync('hi')),
  )
  expect(ref.channel.permalink).toBe(
    ChannelPermalinkSchema.make(`memory://commy/channel/${channel.id}`),
  )
  expect(ref.permalink).toBe(`memory://commy/channel/${channel.id}/near/${ref.id}`)
})

test('publisher.post threads the synthesised permalink through the topic', async () => {
  const adapter = await acquired()
  const channel = await Effect.runPromise(adapter.seedChannel('lobby').pipe(Effect.orDie))
  const ref = await Effect.runPromise(
    adapter.publisher.post(channel.name, decodeMessageBodySync('hi'), {
      thread: decodeThreadNameSync('topic-a'),
    }),
  )
  expect(Option.map(ref.thread, (t) => t.permalink)).toEqual(
    Option.some(ThreadPermalinkSchema.make(`memory://commy/channel/${channel.id}/topic/topic-a`)),
  )
  expect(ref.permalink).toBe(`memory://commy/channel/${channel.id}/topic/topic-a/near/${ref.id}`)
})

test('directory.listChannels exposes a synthesised channel permalink', async () => {
  const adapter = await acquired()
  const channel = await Effect.runPromise(adapter.seedChannel('lobby').pipe(Effect.orDie))
  const channels = await Effect.runPromise(adapter.directory.listChannels())
  expect(channels[0]?.permalink).toBe(
    ChannelPermalinkSchema.make(`memory://commy/channel/${channel.id}`),
  )
})

test('history.messagePermalink resolves a stored message by id without a hint', async () => {
  const adapter = await acquired()
  const channel = await Effect.runPromise(adapter.seedChannel('lobby').pipe(Effect.orDie))
  const ref = await Effect.runPromise(
    adapter.publisher.post(channel.name, decodeMessageBodySync('hi')),
  )
  const link = await Effect.runPromise(adapter.history.messagePermalink(ref.id))
  expect(link).toEqual(Option.some(`memory://commy/channel/${channel.id}/near/${ref.id}`))
})

test('history.messagePermalink returns None for an unknown id with no hint', async () => {
  const adapter = await acquired()
  const link = await Effect.runPromise(adapter.history.messagePermalink(decodeMessageIdSync('404')))
  expect(link).toEqual(Option.none())
})

test('history.messagePermalink synthesises a channel-near link from a hint', async () => {
  const adapter = await acquired()
  const channel = await Effect.runPromise(adapter.seedChannel('lobby').pipe(Effect.orDie))
  const id = decodeMessageIdSync('77')
  const link = await Effect.runPromise(
    adapter.history.messagePermalink(id, { channel: decodeChannelNameSync('lobby') }),
  )
  expect(link).toEqual(Option.some(`memory://commy/channel/${channel.id}/near/${id}`))
})

test('history.messagePermalink synthesises a topic-near link from a hint with a thread', async () => {
  const adapter = await acquired()
  const channel = await Effect.runPromise(adapter.seedChannel('lobby').pipe(Effect.orDie))
  const id = decodeMessageIdSync('88')
  const link = await Effect.runPromise(
    adapter.history.messagePermalink(id, {
      channel: decodeChannelNameSync('lobby'),
      thread: decodeThreadNameSync('topic-a'),
    }),
  )
  expect(link).toEqual(Option.some(`memory://commy/channel/${channel.id}/topic/topic-a/near/${id}`))
})

test('history.messagePermalink returns None for a hint naming an unknown channel', async () => {
  const adapter = await acquired()
  const link = await Effect.runPromise(
    adapter.history.messagePermalink(decodeMessageIdSync('99'), {
      channel: decodeChannelNameSync('never-seeded-channel'),
    }),
  )
  expect(link).toEqual(Option.none())
})

test('separate constructed adapters hold independent counter state', async () => {
  const a = await Effect.runPromise(memoryAdapter())
  await Effect.runPromise(a.identity.acquire(decodeBotNameSync('hermes-agent')))
  const b = await Effect.runPromise(memoryAdapter())
  await Effect.runPromise(b.identity.acquire(decodeBotNameSync('hermes-agent')))
  const channelA = await Effect.runPromise(a.seedChannel('lobby').pipe(Effect.orDie))
  const channelB = await Effect.runPromise(b.seedChannel('lobby').pipe(Effect.orDie))
  const refA = await Effect.runPromise(a.publisher.post(channelA.name, decodeMessageBodySync('a')))
  const refB = await Effect.runPromise(b.publisher.post(channelB.name, decodeMessageBodySync('b')))
  // Each adapter starts its own message-id counter at 1 — state is
  // per-construction, not shared across the module.
  expect(Number(refA.id)).toBe(1)
  expect(Number(refB.id)).toBe(1)
})

// Events dispatched while no `events()` subscription is active accumulate in
// the adapter's pending-event buffer; the next `events()` subscription drains
// them before installing its emit hook. The contract suite pins single-event
// drain (subscribe → post → observe); this locks the *ordering* of a multi-
// event drain — the accumulated buffer must surface oldest-first (FIFO), the
// invariant the buffer's accumulate-then-drain implementation must preserve.
test('events() drains posts accumulated before subscription in FIFO order', async () => {
  const adapter = await acquired()
  const program = Effect.gen(function* () {
    const channel = yield* adapter.seedChannel('lobby').pipe(Effect.orDie)
    yield* adapter.inbox.subscribe(channel.name)
    // No `events()` subscriber is active yet, so these accumulate in the buffer.
    yield* Effect.forEach(
      Arr.makeBy(5, (i) => `e${i}`),
      (body) => adapter.publisher.post(channel.name, decodeMessageBodySync(body)),
    )
    const drained = yield* adapter.inbox
      .events()
      .pipe(Stream.take(5), Stream.runCollect, Effect.timeout(Duration.seconds(2)))
    return Chunk.toReadonlyArray(drained)
  })
  const events = await Effect.runPromise(Effect.scoped(program))
  const bodies = events.map((e) =>
    e.kind === 'message-posted' ? e.message.body : `unexpected:${e.kind}`,
  )
  expect(bodies).toEqual(['e0', 'e1', 'e2', 'e3', 'e4'])
})
