/**
 * A smoke run of the Zulip adapter on the in-process realm, built by the same
 * factory the shared contract will use: a post reads back and arrives as an
 * event.
 */

import { expect } from 'bun:test'
import { decodeMessageBodySync } from '@commy/core/ports'
import { effectTest } from '@commy/testing/effect-test'
import { Duration, Effect, Fiber, Option, Stream } from 'effect'
import { inProcessContractFactory } from './in-process-contract.ts'

const contractEnv = Effect.acquireRelease(Effect.promise(inProcessContractFactory), (env) =>
  env.dispose(),
)

effectTest('a post on the in-process realm reads back with self as its sender', () =>
  Effect.gen(function* () {
    const env = yield* contractEnv
    const channel = yield* env.seedChannel('lobby')
    const me = yield* env.comms.identity.currentIdentity()
    const ref = yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('round-trip'))
    const messages = yield* env.comms.history.readChannel(channel.name, {})
    const found = messages.find((m) => m.ref.id === ref.id)
    expect(String(found?.body)).toBe('round-trip')
    expect(found?.sender.id).toBe(me.id)
  }),
)

effectTest('a post on the in-process realm arrives as an event for a subscriber', () =>
  Effect.gen(function* () {
    const env = yield* contractEnv
    const channel = yield* env.seedChannel('lobby')
    const me = yield* env.comms.identity.currentIdentity()
    yield* env.comms.inbox.subscribe(channel.name)
    const received = yield* env.comms.inbox.events().pipe(
      Stream.filter((e) => e.kind === 'message-posted'),
      Stream.runHead,
      Effect.timeout(Duration.seconds(10)),
      Effect.forkScoped,
    )
    const ref = yield* env.comms.publisher.post(channel.name, decodeMessageBodySync('event-target'))
    const event = yield* Fiber.join(received).pipe(Effect.map(Option.getOrThrow))
    if (event.kind !== 'message-posted')
      throw new Error(`expected message-posted, got ${event.kind}`)
    expect(event.message.ref.id).toBe(ref.id)
    expect(String(event.message.body)).toBe('event-target')
    expect(event.message.sender.id).toBe(me.id)
  }),
)
