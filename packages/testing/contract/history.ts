/** The shared contract's tests for HistoryReader. */

import { expect, test } from 'bun:test'
import {
  type ChannelName,
  decodeMessageBodySync,
  decodeThreadNameSync,
  decodeTimestampSync,
} from '@commy/core/ports'
import { Effect } from 'effect'
import type { ContractEnv } from '../contract.ts'
import { postSpaced } from './helpers.ts'

export const historyContract = (env: () => ContractEnv): void => {
  test('history.readChannel surfaces a posted message with sender = currentIdentity', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const me = yield* env().comms.identity.currentIdentity()
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('round-trip'))
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === 'round-trip')
        expect(found).toBeDefined()
        expect(found?.sender.id).toEqual(me.id)
      }),
    ))
  test('history.readChannel reports reactions=[] for a message with no reactions', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('plain message'))
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === 'plain message')
        if (found === undefined) throw new Error('expected plain message in channel history')
        expect(found.reactions).toEqual([])
      }),
    ))
  test('history.readChannel range.since is inclusive', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* postSpaced(env().comms, channel.name, [
          decodeMessageBodySync('before'),
          decodeMessageBodySync('pivot'),
          decodeMessageBodySync('after'),
        ])
        const everything = yield* env().comms.history.readChannel(channel.name, {})
        const pivot = everything.find((m) => m.body === 'pivot')
        if (pivot === undefined) throw new Error('expected pivot message in channel history')
        const filtered = yield* env().comms.history.readChannel(channel.name, { since: pivot.ts })
        const bodies = filtered.map((m) => m.body)
        expect(bodies).toContain(decodeMessageBodySync('pivot'))
        expect(bodies).not.toContain(decodeMessageBodySync('before'))
      }),
    ))
  test('history.readChannel range.until is inclusive', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* postSpaced(env().comms, channel.name, [
          decodeMessageBodySync('before'),
          decodeMessageBodySync('pivot'),
          decodeMessageBodySync('after'),
        ])
        const everything = yield* env().comms.history.readChannel(channel.name, {})
        const pivot = everything.find((m) => m.body === 'pivot')
        if (pivot === undefined) throw new Error('expected pivot message in channel history')
        const filtered = yield* env().comms.history.readChannel(channel.name, { until: pivot.ts })
        const bodies = filtered.map((m) => m.body)
        expect(bodies).toContain(decodeMessageBodySync('pivot'))
        expect(bodies).not.toContain(decodeMessageBodySync('after'))
      }),
    ))
  test('history.readChannel range.since == range.until selects exactly one timestamp', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* postSpaced(env().comms, channel.name, [
          decodeMessageBodySync('before'),
          decodeMessageBodySync('pivot'),
          decodeMessageBodySync('after'),
        ])
        const everything = yield* env().comms.history.readChannel(channel.name, {})
        const pivot = everything.find((m) => m.body === 'pivot')
        if (pivot === undefined) throw new Error('expected pivot message in channel history')
        const filtered = yield* env().comms.history.readChannel(channel.name, {
          since: pivot.ts,
          until: pivot.ts,
        })
        const bodies = filtered.map((m) => m.body)
        expect(bodies).toEqual([decodeMessageBodySync('pivot')])
      }),
    ))
  test('range.since past every message yields an empty result', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('only'))
        const everything = yield* env().comms.history.readChannel(channel.name, {})
        const last = everything[everything.length - 1]
        if (last === undefined) throw new Error('expected at least one message in channel history')
        const future = decodeTimestampSync(last.ts + 3600)
        const filtered = yield* env().comms.history.readChannel(channel.name, { since: future })
        expect(filtered).toEqual([])
      }),
    ))
  const postInThreads = (channel: ChannelName, posts: ReadonlyArray<readonly [string, string]>) =>
    Effect.forEach(
      posts,
      ([thread, body], index) =>
        (index === 0
          ? Effect.void
          : Effect.sleep(env().comms.capabilities.timestampGranularity)
        ).pipe(
          Effect.zipRight(
            env().comms.publisher.post(channel, decodeMessageBodySync(body), {
              thread: decodeThreadNameSync(thread),
            }),
          ),
        ),
      { discard: true },
    )
  test('history.recentThreads reports one entry per thread, carrying its newest post', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const me = yield* env().comms.identity.currentIdentity()
        yield* postInThreads(channel.name, [
          ['deploy-issue', 'investigating'],
          ['feature-request', 'earlier reply'],
          ['feature-request', 'latest reply'],
        ])
        const threads = yield* env().comms.history.recentThreads(me.id)
        expect(threads.map((t) => [t.thread, t.lastPostBody])).toEqual([
          [decodeThreadNameSync('feature-request'), decodeMessageBodySync('latest reply')],
          [decodeThreadNameSync('deploy-issue'), decodeMessageBodySync('investigating')],
        ])
      }),
    ))
  test('history.recentThreads limit keeps the most recently posted-in threads', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const me = yield* env().comms.identity.currentIdentity()
        yield* postInThreads(channel.name, [
          ['oldest-thread', 'one'],
          ['middle-thread', 'two'],
          ['newest-thread', 'three'],
        ])
        const threads = yield* env().comms.history.recentThreads(me.id, { limit: 2 })
        expect(threads.map((t) => t.thread)).toEqual([
          decodeThreadNameSync('newest-thread'),
          decodeThreadNameSync('middle-thread'),
        ])
      }),
    ))
}
