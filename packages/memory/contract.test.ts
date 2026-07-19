import { decodeBotNameSync } from '@commy/core/ports'
import { runAgentCommsContract } from '@commy/testing/contract'
import { Effect } from 'effect'
import { memoryAdapter } from './adapter.ts'

// The memory adapter's own cap, stated here rather than imported so the suite
// is given the substrate's number the way any other substrate gives it —
// nothing in the contract assumes a particular one.
const MEMORY_DESCRIPTION_LIMIT = 512

runAgentCommsContract('memory adapter', async () => {
  const adapter = await Effect.runPromise(memoryAdapter())
  await Effect.runPromise(adapter.identity.acquire(decodeBotNameSync('hermes-agent')))
  return {
    comms: adapter,
    seedChannel: (name) => adapter.seedChannel(name).pipe(Effect.orDie),
    seedAgent: (name) => adapter.seedAgent(name).pipe(Effect.orDie),
    newUnacquiredAdapter: () => memoryAdapter(),
    peerPost: (peer, channel, body, opts) =>
      adapter.peerPost(peer, channel, body, opts).pipe(Effect.asVoid, Effect.orDie),
    channelDescriptionLimit: MEMORY_DESCRIPTION_LIMIT,
    dispose: () => Effect.void,
  }
})

runAgentCommsContract('memory adapter (allowlist mode for UnknownIdentity coverage)', async () => {
  const adapter = await Effect.runPromise(
    memoryAdapter({
      acquirableNames: ['hermes-agent', 'hermes-agent-conflict', 'hermes-agent-cycle'],
      channelDescriptionLimit: MEMORY_DESCRIPTION_LIMIT,
    }),
  )
  await Effect.runPromise(adapter.identity.acquire(decodeBotNameSync('hermes-agent')))
  return {
    comms: adapter,
    seedChannel: (name) => adapter.seedChannel(name).pipe(Effect.orDie),
    seedAgent: (name) => adapter.seedAgent(name).pipe(Effect.orDie),
    newUnacquiredAdapter: () => memoryAdapter({ acquirableNames: ['hermes-agent-cycle'] }),
    unacquirableName: 'no-such-bot',
    peerPost: (peer, channel, body, opts) =>
      adapter.peerPost(peer, channel, body, opts).pipe(Effect.asVoid, Effect.orDie),
    channelDescriptionLimit: MEMORY_DESCRIPTION_LIMIT,
    dispose: () => Effect.void,
  }
})
