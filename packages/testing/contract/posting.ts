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
import { Effect } from 'effect'
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
