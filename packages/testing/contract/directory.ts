/** The shared contract's tests for Directory and Capabilities. */

import { expect, test } from 'bun:test'
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
