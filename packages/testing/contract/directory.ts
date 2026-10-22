/** The shared contract's tests for Directory and Capabilities. */

import { expect, test } from 'bun:test'
import {
  decodeDisplayNameSync,
  decodeIdentityIdSync,
  decodeMessageBodySync,
  type Identity,
} from '@commy/core/ports'
import { Effect, Option } from 'effect'
import type { ContractEnv } from '../contract.ts'

export const directoryContract = (env: () => ContractEnv): void => {
  // Compare against the refs `seedChannel` returns rather than the literal
  // names: a substrate may namespace the underlying channel (the live Zulip
  // factory prefixes per-test). The neutral invariant is that
  // a seeded channel is enumerated, keyed by the name the substrate assigned.
  test('directory.listChannels enumerates seeded channels by name', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const alpha = yield* env().seedChannel('alpha')
        const bravo = yield* env().seedChannel('bravo')
        const channels = yield* env().comms.directory.listChannels()
        const names = channels.map((c) => String(c.name))
        expect(names).toContain(String(alpha.name))
        expect(names).toContain(String(bravo.name))
      }),
    ))
  test('directory.listChannels lists a channel under the ref seeding returned', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const channels = yield* env().comms.directory.listChannels()
        expect(channels.find((c) => c.id === channel.id)).toEqual(channel)
      }),
    ))
  // A channel's description is standing state, not a message: it is written
  // through the publisher and read back through the directory, and what goes
  // in comes out verbatim. Absence is Option.none on both substrates —
  // there is no blank-description state to tell apart from an unset one.
  test('directory.channelDescription is None for a channel nobody has described', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        const description = yield* env().comms.directory.channelDescription(channel.name)
        expect(description).toEqual(Option.none())
      }),
    ))
  test('directory.listAgents includes currentIdentity when the bot is kind=agent', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const me = yield* env().comms.identity.currentIdentity()
        if (me.kind !== 'agent') return
        const agents = yield* env().comms.directory.listAgents()
        expect(agents.map((a) => a.id)).toContain(me.id)
      }),
    ))
  test('directory.listAgents lists a seeded agent, and listHumans does not', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const peer = yield* env().seedAgent('alice')
        const agents = yield* env().comms.directory.listAgents()
        const humans = yield* env().comms.directory.listHumans()
        expect(agents).toContainEqual(peer)
        expect(agents.every((a) => a.kind === 'agent')).toBe(true)
        expect(humans.map((h) => h.id)).not.toContain(peer.id)
      }),
    ))
  test('directory.listHumans lists a seeded human, and listAgents does not', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const seedHuman = env().seedHuman
        if (seedHuman === undefined) return
        const human = yield* seedHuman('Alice Example')
        const humans = yield* env().comms.directory.listHumans()
        const agents = yield* env().comms.directory.listAgents()
        expect(humans).toContainEqual(human)
        expect(humans.every((h) => h.kind === 'human')).toBe(true)
        expect(agents.map((a) => a.id)).not.toContain(human.id)
      }),
    ))
  // Directory reads carry no BindError: they answer before the seat has
  // bound an identity.
  test('directory reads answer before identity.acquire', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const build = env().newUnacquiredAdapter
        if (build === undefined) return
        const fresh = yield* build()
        yield* fresh.directory.listAgents()
        yield* fresh.directory.listHumans()
        yield* fresh.directory.listChannels()
      }),
    ))
  test('directory.presence reports what the substrate reports for a human', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const seedHuman = env().seedHuman
        const setPresence = env().setPresence
        if (seedHuman === undefined || setPresence === undefined) return
        const human = yield* seedHuman('Alice Example')
        for (const presence of ['online', 'idle', 'offline'] as const) {
          yield* setPresence(human, presence)
          expect(yield* env().comms.directory.presence(human)).toBe(presence)
        }
      }),
    ))
  test('directory.presence is offline for a human with no presence record', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const seedHuman = env().seedHuman
        if (seedHuman === undefined) return
        const human = yield* seedHuman('Alice Example')
        expect(yield* env().comms.directory.presence(human)).toBe('offline')
      }),
    ))
  test('directory.presence is offline for an identity the directory does not know', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const stranger: Identity = {
          id: decodeIdentityIdSync('987654321'),
          name: decodeDisplayNameSync('stranger'),
          kind: 'human',
        }
        expect(yield* env().comms.directory.presence(stranger)).toBe('offline')
      }),
    ))
  // Unknowable, which is not the same as reported absent.
  test('directory.presence is unknown for an agent', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const peer = yield* env().seedAgent('alice')
        expect(yield* env().comms.directory.presence(peer)).toBe('unknown')
      }),
    ))
  test('posts spaced by capabilities.timestampGranularity carry increasing timestamps', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('lobby')
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('first'))
        yield* Effect.sleep(env().comms.capabilities.timestampGranularity)
        yield* env().comms.publisher.post(channel.name, decodeMessageBodySync('second'))
        const messages = yield* env().comms.history.readChannel(channel.name, {})
        expect(messages.map((m) => String(m.body))).toEqual(['first', 'second'])
        const [first, second] = messages
        expect(second?.ts).toBeGreaterThan(first?.ts ?? Number.POSITIVE_INFINITY)
      }),
    ))
  // The returned name must reflect (derive from) the seed input; a substrate
  // may namespace it (live Zulip prefixes per-test), so the
  // neutral invariant is containment of the seed input, not exact equality.
  test('seedChannel returns a ChannelRef whose name matches the seed input', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const channel = yield* env().seedChannel('design-talk')
        expect(String(channel.name)).toContain('design-talk')
      }),
    ))
}
