/** The shared contract's tests for MessagePublisher.post. */

import { expect, test } from 'bun:test'
import {
  decodeChannelNameSync,
  decodeMessageBodySync,
  decodeThreadNameSync,
  type MessageRef,
  mentionedIdentities,
  UnknownChannel,
} from '@commy/core/ports'
import { Effect, Option } from 'effect'
import type { ContractEnv } from '../contract.ts'

const findByBody = (
  messages: ReadonlyArray<{ readonly body: string }>,
  body: string,
): { readonly body: string } | undefined => messages.find((m) => m.body === body)

export const publisherPostingContract = (env: () => ContractEnv): void => {
  test('publisher.post returns a MessageRef whose channel matches the destination', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const ref = yield* env().comms.publisher.post(
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
          env().comms.publisher.post(phantom, decodeMessageBodySync('should fail')),
        )
        expect(error).toBeInstanceOf(UnknownChannel)
        if (error instanceof UnknownChannel) expect(error.channel).toEqual(phantom)
      }),
    ))
  test('publisher.post without a thread returns a MessageRef with no thread', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const ref = yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('bare'))
        expect(ref.thread).toEqual(Option.none())
      }),
    ))
  test('publisher.post with thread returns a MessageRef naming that thread, unresolved', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const thread = decodeThreadNameSync('planning')
        const ref = yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('hi'), {
          thread,
        })
        expect(Option.map(ref.thread, (t) => t.name)).toEqual(Option.some(thread))
        expect(Option.map(ref.thread, (t) => t.resolved)).toEqual(Option.some(false))
      }),
    ))
  // A substrate that resolves by renaming the thread must address the renamed
  // form, or the post mints a sibling thread under the clean name and the
  // read below finds the new message in an unresolved thread. The substrate
  // may add its own notice of the resolve to the thread, so the read looks for
  // the post rather than the whole thread.
  test('publisher.post into a resolved thread appends to it and leaves it resolved', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const thread = decodeThreadNameSync('planning')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('settled'), {
          thread,
        })
        yield* env().comms.publisher.resolveThread(channel.name, thread)
        const ref = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync('one more thing'),
          { thread },
        )
        expect(Option.map(ref.thread, (t) => t.name)).toEqual(Option.some(thread))
        expect(Option.map(ref.thread, (t) => t.resolved)).toEqual(Option.some(true))
        const messages = yield* env().comms.history.readThread(channel.name, thread, {})
        const appended = messages.find((m) => m.body === 'one more thing')
        if (appended === undefined) throw new Error('expected the post in the resolved thread')
        expect(Option.map(appended.ref.thread, (t) => t.resolved)).toEqual(Option.some(true))
      }),
    ))
  // A mention form inside a code span is literal text, not a failed delivery,
  // so it never triggers UnresolvedMention.
  test('publisher.post delivers a mention form naming nobody verbatim when it sits in a code span', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const body = decodeMessageBodySync('the dead form `@**Nobody Example**` names nobody')
        yield* env().comms.publisher.post(channel.name, body)
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        expect(findByBody(messages, body)).toBeDefined()
      }),
    ))
  test('publisher.post with thread is visible via history.readThread', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const thread = decodeThreadNameSync('design')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('in thread'), {
          thread,
        })
        const messages = yield* env().comms.history.readThread(channel.name, thread, {})
        const found = findByBody(messages, 'in thread')
        expect(found).toBeDefined()
      }),
    ))
  // A substrate may cut an over-long thread name short. The ref `post` returns
  // must carry the name that was kept, or the caller cannot address the thread.
  test('publisher.post returns the thread name the substrate kept, which addresses the thread', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const ref = yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('long'), {
          thread: decodeThreadNameSync('x'.repeat(env().threadNameLimit + 10)),
        })
        const kept = yield* Effect.fromNullable(Option.getOrUndefined(ref.thread)).pipe(
          Effect.orDie,
        )
        const messages = yield* env().comms.history.readThread(channel.name, kept.name, {})
        expect(findByBody(messages, 'long')).toBeDefined()
      }),
    ))
  test('publisher.post with thread is also visible via history.readChannel (channel reads include all topics)', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('topic-a'), {
          thread: decodeThreadNameSync('alpha'),
        })
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('topic-b'), {
          thread: decodeThreadNameSync('bravo'),
        })
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const bodies = messages.map((m) => m.body)
        expect(bodies).toContain(decodeMessageBodySync('topic-a'))
        expect(bodies).toContain(decodeMessageBodySync('topic-b'))
      }),
    ))
  test('publisher.post with mentions surfaces them via history.readChannel as Identity[]', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const alice = yield* env().seedAgent('alice')
        yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync(`@**${alice.name}** wake up`),
          {
            mentions: [alice],
          },
        )
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body.includes('wake up'))
        expect(found).toBeDefined()
        expect(mentionedIdentities(found?.mentions ?? []).map((m) => m.id)).toEqual([alice.id])
      }),
    ))
  // PostOpts.mentions is metadata-only: adapters must not mutate `body`
  // based on the array. Cross-adapter spec — both Zulip and memory must
  // round-trip the literal body string through history.readChannel even
  // when mentions[] is set.
  test('publisher.post does not mutate body based on opts.mentions', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const alice = yield* env().seedAgent('alice')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('oi look here'), {
          mentions: [alice],
        })
        const messages = yield* env().comms.history.readChannel(channel.name, {})
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
        const channel = yield* env().seedChannel('lobby')
        const alice = yield* env().seedAgent('alice')
        const body = decodeMessageBodySync(`@**${alice.name}** look at this`)
        yield* env().comms.publisher.post(channel.name, body, { mentions: [alice] })
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        const found = messages.find((m) => m.body === body)
        if (found === undefined) throw new Error('expected posted message in channel history')
        const occurrences = found.body.split(`@**${alice.name}**`).length - 1
        expect(occurrences).toBe(1)
      }),
    ))
  test('publisher.post with replyTo does not throw (substrate may quote-block or drop)', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const parentRef = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync('parent'),
        )
        const followUp: MessageRef = yield* env().comms.publisher.post(
          channel.name,
          decodeMessageBodySync('child'),
          { replyTo: parentRef },
        )
        expect(followUp.channel.id).toEqual(channel.id)
        expect(followUp.channel.name).toEqual(channel.name)
      }),
    ))
}
