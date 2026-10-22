import { describe, expect, test } from 'bun:test'
import type { ChannelRef } from '@commy/core/ports'
import { Effect, Either } from 'effect'
import { LiveChannelMissing, requireLiveChannel } from './live-channel.ts'

const directoryListing = (...names: ReadonlyArray<string>) => ({
  listChannels: () =>
    Effect.succeed(names.map((name) => ({ id: name, name }) as unknown as ChannelRef)),
})

describe('requireLiveChannel', () => {
  test('passes when the realm lists the channel', async () => {
    const result = await Effect.runPromise(
      Effect.either(requireLiveChannel(directoryListing('test', 'sandbox'), 'test')),
    )
    expect(Either.isRight(result)).toBe(true)
  })

  test('fails naming the channel when the realm does not list it', async () => {
    const result = await Effect.runPromise(
      Effect.either(requireLiveChannel(directoryListing('test'), 'agent-comms-test')),
    )
    expect(Either.isLeft(result)).toBe(true)
    if (Either.isLeft(result)) {
      expect(result.left).toBeInstanceOf(LiveChannelMissing)
      expect(result.left.message).toContain('"agent-comms-test"')
    }
  })
})
