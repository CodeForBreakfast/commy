import { expect, test } from 'bun:test'
import {
  ChannelPermalinkSchema,
  decodeBotNameSync,
  decodeChannelNameSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  decodeTimestampSync,
  MessagePermalinkSchema,
  ThreadPermalinkSchema,
} from '@commy/core/ports'
import { Array as Arr, Duration, Effect, Option, TestClock, TestContext } from 'effect'
import { memoryAdapter } from './adapter.ts'

const acquired = async () => {
  const adapter = await Effect.runPromise(memoryAdapter())
  await Effect.runPromise(adapter.identity.acquire(decodeBotNameSync('hermes-agent')))
  return adapter
}

// The message-id Ref pins concurrency-safety: counter allocation spans `yield*`
// suspension points, so under `Effect.all` it must still allocate distinct ids.
// Sequential semantics are already covered by the contract suite; this asserts
// the interleaved case the Ref makes correct.

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
  expect(ref.permalink).toBe(
    MessagePermalinkSchema.make(`memory://commy/channel/${channel.id}/near/${ref.id}`),
  )
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
  expect(ref.permalink).toBe(
    MessagePermalinkSchema.make(
      `memory://commy/channel/${channel.id}/topic/topic-a/near/${ref.id}`,
    ),
  )
})

test('directory.listChannels exposes a synthesised channel permalink', async () => {
  const adapter = await acquired()
  const channel = await Effect.runPromise(adapter.seedChannel('lobby').pipe(Effect.orDie))
  const channels = await Effect.runPromise(adapter.directory.listChannels())
  expect(channels[0]?.permalink).toBe(
    ChannelPermalinkSchema.make(`memory://commy/channel/${channel.id}`),
  )
})

test('history.messagePermalink synthesises a channel-near link from a hint', async () => {
  const adapter = await acquired()
  const channel = await Effect.runPromise(adapter.seedChannel('lobby').pipe(Effect.orDie))
  const id = decodeMessageIdSync('77')
  const link = await Effect.runPromise(
    adapter.history.messagePermalink(id, { channel: decodeChannelNameSync('lobby') }),
  )
  expect(link).toEqual(
    Option.some(MessagePermalinkSchema.make(`memory://commy/channel/${channel.id}/near/${id}`)),
  )
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
