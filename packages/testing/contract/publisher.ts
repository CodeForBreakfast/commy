/** The shared contract's tests for MessagePublisher's verbs other than post. */

import { expect, test } from 'bun:test'
import {
  ChannelDescriptionRejected,
  decodeChannelDescriptionSync,
  decodeChannelNameSync,
  decodeEmojiSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  MessageEditRefused,
  MessagePermalinkSchema,
  type MessageRef,
  mentionedIdentities,
  PublisherError,
  UnknownChannel,
} from '@commy/core/ports'
import { Duration, Effect, Option, Queue } from 'effect'
import type { ContractEnv } from '../contract.ts'
import { awaitEvent, eventQueue } from './helpers.ts'

/** How long a write must stay silent on `events()` to count as emitting nothing. */
const SILENCE_WINDOW = Duration.millis(100)

export const publisherContract = (env: () => ContractEnv): void => {
  // Resolution is a status distinct from the thread name: the name a consumer
  // addresses stays stable, and the flag surfaces on read as
  // ObservedThread.resolved. These hold on both substrates — memory tracks the
  // flag directly, Zulip renames the topic behind the adapter seam and strips
  // it back off on read.
  test('publisher.resolveThread marks a thread resolved, observable via history.readThread', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const thread = decodeThreadNameSync('design')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('to resolve'), {
          thread,
        })
        yield* env().comms.publisher.resolveThread(channel.name, thread)
        const messages = yield* env().comms.history.readThread(channel.name, thread, {})
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
        const channel = yield* env().seedChannel('lobby')
        const thread = decodeThreadNameSync('design')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('round trip'), {
          thread,
        })
        yield* env().comms.publisher.resolveThread(channel.name, thread)
        yield* env().comms.publisher.unresolveThread(channel.name, thread)
        const messages = yield* env().comms.history.readThread(channel.name, thread, {})
        const found = messages.find((m) => m.body === 'round trip')
        if (found === undefined) throw new Error('expected the thread readable after unresolve')
        expect(Option.map(found.ref.thread, (t) => t.resolved)).toEqual(Option.some(false))
      }),
    ))
  // The substrate announces each flip with a notice in the thread, sent by the
  // substrate rather than the caller, and a subscriber receives it like any post.
  test('a subscriber receives a notice in the thread when it is resolved and unresolved', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          const thread = decodeThreadNameSync('design')
          const me = yield* env().comms.identity.currentIdentity()
          yield* env().comms.inbox.subscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('settle this'), {
            thread,
          })
          const awaitNotice = (resolved: boolean) =>
            awaitEvent(
              queue,
              `the notice that the thread was ${resolved ? 'resolved' : 'unresolved'}`,
              (e) =>
                e.kind === 'message-posted' &&
                e.message.sender.id !== me.id &&
                Option.exists(
                  e.message.ref.thread,
                  (t) => t.name === thread && t.resolved === resolved,
                ),
            )
          yield* env().comms.publisher.resolveThread(channel.name, thread)
          const resolvedNotice = yield* awaitNotice(true)
          yield* env().comms.publisher.unresolveThread(channel.name, thread)
          const unresolvedNotice = yield* awaitNotice(false)
          for (const notice of [resolvedNotice, unresolvedNotice]) {
            if (notice.kind !== 'message-posted') throw new Error('unexpected event kind')
            expect(notice.message.body).toContain(me.name)
            expect(notice.message.ref.channel.id).toEqual(channel.id)
          }
        }),
      ),
    ))
  test('publisher.resolveThread is idempotent when the thread is already resolved', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const thread = decodeThreadNameSync('design')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('twice resolved'), {
          thread,
        })
        yield* env().comms.publisher.resolveThread(channel.name, thread)
        // Second resolve is a no-op, not an error.
        yield* env().comms.publisher.resolveThread(channel.name, thread)
        const messages = yield* env().comms.history.readThread(channel.name, thread, {})
        const found = messages.find((m) => m.body === 'twice resolved')
        if (found === undefined) throw new Error('expected the thread readable after re-resolve')
        expect(Option.map(found.ref.thread, (t) => t.resolved)).toEqual(Option.some(true))
      }),
    ))
  test('publisher.unresolveThread is idempotent when the thread is not resolved', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const thread = decodeThreadNameSync('design')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('never resolved'), {
          thread,
        })
        // Unresolving an unresolved thread is a no-op, not an error.
        yield* env().comms.publisher.unresolveThread(channel.name, thread)
        const messages = yield* env().comms.history.readThread(channel.name, thread, {})
        const found = messages.find((m) => m.body === 'never resolved')
        if (found === undefined)
          throw new Error('expected the thread readable after no-op unresolve')
        expect(Option.map(found.ref.thread, (t) => t.resolved)).toEqual(Option.some(false))
      }),
    ))
  test('publisher.resolveThread on a thread with no messages fails with PublisherError', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const error = yield* Effect.flip(
          env().comms.publisher.resolveThread(channel.name, decodeThreadNameSync('ghost')),
        )
        expect(error).toBeInstanceOf(PublisherError)
      }),
    ))
  test('publisher.setChannelDescription round-trips verbatim through directory.channelDescription', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const charter = decodeChannelDescriptionSync('Where the lobby crowd coordinates.')
        yield* env().comms.publisher.setChannelDescription(channel.name, Option.some(charter))
        const description = yield* env().comms.directory.channelDescription(channel.name)
        expect(description).toEqual(Option.some(charter))
      }),
    ))
  test('publisher.setChannelDescription replaces an existing description', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* env().comms.publisher.setChannelDescription(
          channel.name,
          Option.some(decodeChannelDescriptionSync('first charter')),
        )
        const replacement = decodeChannelDescriptionSync('second charter')
        yield* env().comms.publisher.setChannelDescription(channel.name, Option.some(replacement))
        const description = yield* env().comms.directory.channelDescription(channel.name)
        expect(description).toEqual(Option.some(replacement))
      }),
    ))
  test('publisher.setChannelDescription with None clears the description back to absent', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* env().comms.publisher.setChannelDescription(
          channel.name,
          Option.some(decodeChannelDescriptionSync('to be cleared')),
        )
        yield* env().comms.publisher.setChannelDescription(channel.name, Option.none())
        const description = yield* env().comms.directory.channelDescription(channel.name)
        expect(description).toEqual(Option.none())
      }),
    ))
  test('publisher.setChannelDescription is idempotent when the description is unchanged', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const charter = decodeChannelDescriptionSync('stable charter')
        yield* env().comms.publisher.setChannelDescription(channel.name, Option.some(charter))
        // Re-writing the same text is a no-op, not an error.
        yield* env().comms.publisher.setChannelDescription(channel.name, Option.some(charter))
        const description = yield* env().comms.directory.channelDescription(channel.name)
        expect(description).toEqual(Option.some(charter))
      }),
    ))
  test('publisher.setChannelDescription with None is idempotent on an undescribed channel', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* env().comms.publisher.setChannelDescription(channel.name, Option.none())
        const description = yield* env().comms.directory.channelDescription(channel.name)
        expect(description).toEqual(Option.none())
      }),
    ))
  // The failure mode that matters: a substrate refuses what it cannot store
  // rather than storing a truncated copy. The limit is the substrate's own
  // (read off the env), so this asserts the refusal, never a number.
  test('publisher.setChannelDescription refuses a description longer than the substrate stores', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const tooLong = decodeChannelDescriptionSync('x'.repeat(env().channelDescriptionLimit + 1))
        const error = yield* Effect.flip(
          env().comms.publisher.setChannelDescription(channel.name, Option.some(tooLong)),
        )
        expect(error).toBeInstanceOf(ChannelDescriptionRejected)
        expect((error as ChannelDescriptionRejected).constraint).toBe('length')
        // Refused outright — nothing truncated was left behind.
        const description = yield* env().comms.directory.channelDescription(channel.name)
        expect(description).toEqual(Option.none())
      }),
    ))
  test('publisher.setChannelDescription accepts a description exactly at the substrate limit', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const atLimit = decodeChannelDescriptionSync('x'.repeat(env().channelDescriptionLimit))
        yield* env().comms.publisher.setChannelDescription(channel.name, Option.some(atLimit))
        const description = yield* env().comms.directory.channelDescription(channel.name)
        expect(description).toEqual(Option.some(atLimit))
      }),
    ))
  test('publisher.setChannelDescription refuses a multi-line description rather than reshaping it', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const multiLine = decodeChannelDescriptionSync('first line\nsecond line')
        const error = yield* Effect.flip(
          env().comms.publisher.setChannelDescription(channel.name, Option.some(multiLine)),
        )
        expect(error).toBeInstanceOf(ChannelDescriptionRejected)
        expect((error as ChannelDescriptionRejected).constraint).toBe('format')
        const description = yield* env().comms.directory.channelDescription(channel.name)
        expect(description).toEqual(Option.none())
      }),
    ))
  test('channel-description reads and writes on an unknown channel fail with UnknownChannel', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const absent = decodeChannelNameSync('no-such-channel-for-descriptions')
        const readError = yield* Effect.flip(env().comms.directory.channelDescription(absent))
        expect(readError).toBeInstanceOf(UnknownChannel)
        const writeError = yield* Effect.flip(
          env().comms.publisher.setChannelDescription(
            absent,
            Option.some(decodeChannelDescriptionSync('never lands')),
          ),
        )
        expect(writeError).toBeInstanceOf(UnknownChannel)
      }),
    ))
  test('publisher.edit replaces the body, surfaced via history.readChannel under the same MessageRef', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const ref = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync('original body'),
        )
        yield* env().comms.publisher.edit(ref, decodeMessageBodySync('replacement body'))
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => String(m.ref.id) === String(ref.id))
        if (found === undefined) throw new Error('expected edited message in channel history')
        expect(found.body).toBe(decodeMessageBodySync('replacement body'))
        // Original body must be gone — edit is replace, not append.
        const original = messages.find((m) => m.body === 'original body')
        expect(original).toBeUndefined()
      }),
    ))
  test('publisher.edit reports who the new body @-mentions, not the old', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const alice = yield* env().seedAgent('alice')
        const bob = yield* env().seedAgent('bob')
        const ref = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync(`@**${alice.name}** first cut`),
        )
        yield* env().comms.publisher.edit(ref, decodeMessageBodySync(`@**${bob.name}** second cut`))
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => String(m.ref.id) === String(ref.id))
        expect(mentionedIdentities(found?.mentions ?? []).map((m) => m.id)).toEqual([bob.id])
      }),
    ))
  test('publisher.edit on a thread message keeps the thread association', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const thread = decodeThreadNameSync('design')
        const ref = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync('first cut'),
          {
            thread,
          },
        )
        yield* env().comms.publisher.edit(ref, decodeMessageBodySync('second cut'))
        const messages = yield* env().comms.history.readThread(channel.name, thread, {})
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
        const channel = yield* env().seedChannel('lobby')
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
          env().comms.publisher.edit(phantom, decodeMessageBodySync('nope')),
        )
        expect(error).toBeInstanceOf(PublisherError)
      }),
    ))
  test('publisher.react then unreact leaves no observable error', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const ref = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync('react target'),
        )
        yield* env().comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
        yield* env().comms.publisher.unreact(ref, decodeEmojiSync('thumbs_up'))
      }),
    ))
  test('publisher.react surfaces on history.readChannel as Reaction with by=[self]', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const me = yield* env().comms.identity.currentIdentity()
        const ref = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync('reaction round-trip'),
        )
        yield* env().comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
        const messages = yield* env().comms.history.readChannel(channel.name, {})
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
        const channel = yield* env().seedChannel('lobby')
        const ref = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync('reaction removal'),
        )
        yield* env().comms.publisher.react(ref, decodeEmojiSync('thumbs_up'))
        yield* env().comms.publisher.unreact(ref, decodeEmojiSync('thumbs_up'))
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === 'reaction removal')
        if (found === undefined)
          throw new Error('expected reaction-removal target in channel history')
        expect(found.reactions.find((r) => r.emoji === 'thumbs_up')).toBeUndefined()
      }),
    ))
  test("publisher.edit on a peer's message fails with MessageEditRefused not-original-sender", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const peerPost = env().peerPost
        if (peerPost === undefined) return
        const channel = yield* env().seedChannel('lobby')
        const peer = yield* env().seedAgent('alice')
        yield* peerPost(peer, channel.name, decodeMessageBodySync('written by alice'))
        const before = yield* env().comms.history.readChannel(channel.name, {})
        const target = before.find((m) => m.body === 'written by alice')
        if (target === undefined) throw new Error("expected the peer's message in channel history")
        const error = yield* Effect.flip(
          env().comms.publisher.edit(target.ref, decodeMessageBodySync('rewritten by self')),
        )
        expect(error).toBeInstanceOf(MessageEditRefused)
        expect((error as MessageEditRefused).reason).toBe('not-original-sender')
        const after = yield* env().comms.history.readChannel(channel.name, {})
        const unchanged = after.find((m) => String(m.ref.id) === String(target.ref.id))
        expect(unchanged?.body).toBe(target.body)
      }),
    ))
  test('publisher.editingAvailable answers true where nothing has turned editing off', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        expect(yield* env().comms.publisher.editingAvailable()).toBe(true)
      }),
    ))
  // A driving adapter samples this at connect, before any identity is bound,
  // and a listen-only seat never binds one.
  test('publisher.editingAvailable answers on an adapter that has not acquired an identity', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const newUnacquiredAdapter = env().newUnacquiredAdapter
        if (newUnacquiredAdapter === undefined) return
        const fresh = yield* newUnacquiredAdapter()
        expect(yield* fresh.publisher.editingAvailable()).toBe(true)
      }),
    ))
  test('publisher.edit emits no inbound event', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          yield* env().comms.inbox.subscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          const ref = yield* env().comms.publisher.post(
            channel.name,
            decodeMessageBodySync('draft'),
          )
          yield* awaitEvent(
            queue,
            'message-posted for the edit target',
            (e) => e.kind === 'message-posted',
          )
          yield* env().comms.publisher.edit(ref, decodeMessageBodySync('final'))
          const result = yield* Queue.take(queue).pipe(Effect.timeoutOption(SILENCE_WINDOW))
          expect(result).toEqual(Option.none())
        }),
      ),
    ))
  test('publisher.setChannelDescription emits no inbound event', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const channel = yield* env().seedChannel('lobby')
          yield* env().comms.inbox.subscribe(channel.name)
          const queue = yield* eventQueue(env().comms)
          yield* env().comms.publisher.setChannelDescription(
            channel.name,
            Option.some(decodeChannelDescriptionSync('a quiet charter')),
          )
          const result = yield* Queue.take(queue).pipe(Effect.timeoutOption(SILENCE_WINDOW))
          expect(result).toEqual(Option.none())
        }),
      ),
    ))
}
