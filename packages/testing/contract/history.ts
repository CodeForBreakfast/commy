/** The shared contract's tests for HistoryReader. */

import { expect, test } from 'bun:test'
import {
  type ChannelName,
  decodeChannelNameSync,
  decodeEmojiSync,
  decodeIdentityIdSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  decodeTimestampSync,
  type Message,
} from '@commy/core/ports'
import { Effect, Option } from 'effect'
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
  // History reads carry no BindError: they answer before the seat has bound
  // an identity.
  test('history.readChannel answers before identity.acquire', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const build = env().newUnacquiredAdapter
        if (build === undefined) return
        const channel = yield* env().seedChannel('lobby')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('before acquire'))
        const fresh = yield* build()
        const messages = yield* fresh.history.readChannel(channel.name, {})
        expect(messages.map((m) => m.body)).toContain(decodeMessageBodySync('before acquire'))
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
  test('history.readChannel returns messages oldest first', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        for (const body of ['first', 'second', 'third']) {
          yield* env().comms.publisher.post(channel.name, decodeMessageBodySync(body))
        }
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        expect(bodiesOf(messages, ['first', 'second', 'third'])).toEqual([
          'first',
          'second',
          'third',
        ])
      }),
    ))
  test('history.readChannel reads only the named channel', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const lobby = yield* env().seedChannel('lobby')
        const alpha = yield* env().seedChannel('alpha')
        yield* env().comms.publisher.post(lobby.name, decodeMessageBodySync('in lobby'))
        yield* env().comms.publisher.post(alpha.name, decodeMessageBodySync('in alpha'))
        const messages = yield* env().comms.history.readChannel(lobby.name, {})
        expect(bodiesOf(messages, ['in lobby', 'in alpha'])).toEqual(['in lobby'])
        expect(messages.every((m) => m.ref.channel.name === lobby.name)).toBe(true)
      }),
    ))
  test('history.readChannel names a peer sender by the identity the directory gives it', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const peerPost = env().peerPost
        if (peerPost === undefined) return
        const channel = yield* env().seedChannel('lobby')
        const peer = yield* env().seedAgent('alice')
        yield* peerPost(peer, channel.name, decodeMessageBodySync('from a peer'), {
          thread: decodeThreadNameSync('design'),
        })
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === 'from a peer')
        if (found === undefined) throw new Error('expected the peer post in channel history')
        expect(found.sender).toEqual(peer)
        expect(found.ref.channel.name).toEqual(channel.name)
        expect(Option.map(found.ref.thread, (t) => t.name)).toEqual(
          Option.some(decodeThreadNameSync('design')),
        )
      }),
    ))
  test('history.readChannel names the sender of a message as an agent after that agent is deactivated', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const peerPost = env().peerPost
        const deactivateAgent = env().deactivateAgent
        if (peerPost === undefined || deactivateAgent === undefined) return
        const channel = yield* env().seedChannel('lobby')
        const peer = yield* env().seedAgent('alice')
        yield* peerPost(peer, channel.name, decodeMessageBodySync('before leaving'))
        yield* deactivateAgent(peer)
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === 'before leaving')
        if (found === undefined) throw new Error('expected the peer post in channel history')
        expect(found.sender.kind).toBe('agent')
        expect(found.sender.id).toEqual(peer.id)
      }),
    ))
  test('history.readChannel reaches a window that lies below the newest messages', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* postSpaced(env().comms, channel.name, [
          decodeMessageBodySync('oldest'),
          decodeMessageBodySync('middle'),
          decodeMessageBodySync('newest'),
        ])
        const oldest = yield* tsOf(env().comms.history.readChannel(channel.name, {}), 'oldest')
        const window = yield* env().comms.history.readChannel(channel.name, {
          until: oldest,
          limit: 1,
        })
        expect(window.map((m) => m.body)).toEqual([decodeMessageBodySync('oldest')])
      }),
    ))
  test('history.readChannel limit keeps the newest messages in the window', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        for (const body of ['first', 'second', 'third']) {
          yield* env().comms.publisher.post(channel.name, decodeMessageBodySync(body))
        }
        const first = yield* tsOf(env().comms.history.readChannel(channel.name, {}), 'first')
        const window = yield* env().comms.history.readChannel(channel.name, {
          since: first,
          limit: 2,
        })
        expect(window.map((m) => m.body)).toEqual([
          decodeMessageBodySync('second'),
          decodeMessageBodySync('third'),
        ])
      }),
    ))
  test('history.readThread reads only the named thread', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* postInThreads(channel.name, [
          ['design', 'about design'],
          ['ops', 'about ops'],
        ])
        const messages = yield* env().comms.history.readThread(
          channel.name,
          decodeThreadNameSync('design'),
        )
        expect(bodiesOf(messages, ['about design', 'about ops'])).toEqual(['about design'])
      }),
    ))
  test('history.readThread reaches a window that lies below the newest messages', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* postInThreads(channel.name, [
          ['design', 'oldest'],
          ['design', 'middle'],
          ['design', 'newest'],
        ])
        const design = decodeThreadNameSync('design')
        const oldest = yield* tsOf(env().comms.history.readThread(channel.name, design), 'oldest')
        const window = yield* env().comms.history.readThread(channel.name, design, {
          until: oldest,
          limit: 1,
        })
        expect(window.map((m) => m.body)).toEqual([decodeMessageBodySync('oldest')])
      }),
    ))
  test('history.readChannel on an unknown channel fails with a typed HistoryError', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const error = yield* Effect.flip(env().comms.history.readChannel(unseededChannel, {}))
        expect(error._tag).toBe('HistoryError')
        expect(error.operation).toBe('readChannel')
      }),
    ))
  test('history.readThread on an unknown channel fails with a typed HistoryError', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const error = yield* Effect.flip(
          env().comms.history.readThread(unseededChannel, decodeThreadNameSync('design')),
        )
        expect(error._tag).toBe('HistoryError')
        expect(error.operation).toBe('readThread')
      }),
    ))
  test('history.readChannel groups reactions by emoji', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const me = yield* env().comms.identity.currentIdentity()
        const ref = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync('two reactions'),
        )
        yield* env().comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
        yield* env().comms.publisher.react(ref, decodeEmojiSync('tada'))
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === 'two reactions')
        if (found === undefined) throw new Error('expected the reaction target in channel history')
        expect(found.reactions.map((r) => [r.emoji, r.by.map((i) => i.id)])).toEqual([
          [decodeEmojiSync('thumbs_up'), [me.id]],
          [decodeEmojiSync('tada'), [me.id]],
        ])
      }),
    ))
  test('history.recentThreads names the channel and timestamp of each thread it reports', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const ops = yield* env().seedChannel('lobby')
        const project = yield* env().seedChannel('alpha')
        const me = yield* env().comms.identity.currentIdentity()
        yield* env().comms.publisher.post(ops.name, decodeMessageBodySync('investigating'), {
          thread: decodeThreadNameSync('deploy-issue'),
        })
        yield* Effect.sleep(env().comms.capabilities.timestampGranularity)
        yield* env().comms.publisher.post(project.name, decodeMessageBodySync('on it'), {
          thread: decodeThreadNameSync('feature-request'),
        })
        const investigating = yield* tsOf(
          env().comms.history.readChannel(ops.name, {}),
          'investigating',
        )
        const onIt = yield* tsOf(env().comms.history.readChannel(project.name, {}), 'on it')
        const threads = yield* env().comms.history.recentThreads(me.id)
        expect(threads).toEqual([
          {
            channel: project.name,
            thread: decodeThreadNameSync('feature-request'),
            lastPostTs: onIt,
            lastPostBody: decodeMessageBodySync('on it'),
          },
          {
            channel: ops.name,
            thread: decodeThreadNameSync('deploy-issue'),
            lastPostTs: investigating,
            lastPostBody: decodeMessageBodySync('investigating'),
          },
        ])
      }),
    ))
  test('history.recentThreads reports only the threads the given sender posted in', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const peerPost = env().peerPost
        if (peerPost === undefined) return
        const channel = yield* env().seedChannel('lobby')
        const me = yield* env().comms.identity.currentIdentity()
        const peer = yield* env().seedAgent('alice')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('mine'), {
          thread: decodeThreadNameSync('my-thread'),
        })
        yield* peerPost(peer, channel.name, decodeMessageBodySync('theirs'), {
          thread: decodeThreadNameSync('peer-thread'),
        })
        const mine = yield* env().comms.history.recentThreads(me.id)
        const theirs = yield* env().comms.history.recentThreads(peer.id)
        expect(mine.map((t) => t.thread)).toEqual([decodeThreadNameSync('my-thread')])
        expect(theirs.map((t) => t.thread)).toEqual([decodeThreadNameSync('peer-thread')])
      }),
    ))
  test('history.recentThreads reports nothing for an identity the substrate does not know', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('hello'), {
          thread: decodeThreadNameSync('greetings'),
        })
        const threads = yield* env().comms.history.recentThreads(unknownIdentity)
        expect(threads).toEqual([])
      }),
    ))
  test('history.messagePermalink without a hint locates the message and links to it', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const ref = yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('hi'), {
          thread: decodeThreadNameSync('topic-a'),
        })
        const link = yield* env().comms.history.messagePermalink(ref.id)
        expect(link).toEqual(Option.some(ref.permalink))
      }),
    ))
  test('history.messagePermalink with a channel and thread hint gives the same link', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const thread = decodeThreadNameSync('topic-a')
        const ref = yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('hi'), {
          thread,
        })
        const link = yield* env().comms.history.messagePermalink(ref.id, {
          channel: channel.name,
          thread,
        })
        expect(link).toEqual(Option.some(ref.permalink))
      }),
    ))
  test('history.messagePermalink with a channel hint links without locating the message', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const link = yield* env().comms.history.messagePermalink(unpostedId, {
          channel: channel.name,
        })
        expect(Option.isSome(link)).toBe(true)
      }),
    ))
  test('history.messagePermalink without a hint is None for a message that does not exist', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const link = yield* env().comms.history.messagePermalink(unpostedId)
        expect(link).toEqual(Option.none())
      }),
    ))
  test('history.messagePermalink is None for a hint naming an unknown channel', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const link = yield* env().comms.history.messagePermalink(unpostedId, {
          channel: unseededChannel,
        })
        expect(link).toEqual(Option.none())
      }),
    ))
}

const unseededChannel = decodeChannelNameSync('never-seeded-channel')
const unpostedId = decodeMessageIdSync('987654321')
const unknownIdentity = decodeIdentityIdSync('987654321')

/** The bodies among `wanted` that `messages` holds, in the order read. */
const bodiesOf = (
  messages: ReadonlyArray<Message>,
  wanted: ReadonlyArray<string>,
): ReadonlyArray<string> =>
  messages.map((m): string => m.body).filter((body) => wanted.includes(body))

/** The timestamp of the message with this body in what `read` returns. */
const tsOf = <E>(read: Effect.Effect<ReadonlyArray<Message>, E>, body: string) =>
  read.pipe(
    Effect.map((messages) => messages.find((m) => m.body === body)),
    Effect.flatMap((found) =>
      found === undefined
        ? Effect.dieMessage(`expected '${body}' in history`)
        : Effect.succeed(found.ts),
    ),
  )
