import { decodeBotNameSync } from '@commy/core/ports'
import { runAgentCommsContract } from '@commy/testing/contract'
import { Effect } from 'effect'
import { memoryAdapter } from './adapter.ts'

runAgentCommsContract('memory adapter', async () => {
  const adapter = await Effect.runPromise(memoryAdapter())
  await Effect.runPromise(adapter.identity.acquire(decodeBotNameSync('hermes-agent')))
  return {
    comms: adapter,
    seedChannel: (name) => adapter.seedChannel(name).pipe(Effect.orDie),
    seedAgent: (name) => adapter.seedAgent(name).pipe(Effect.orDie),
    newUnacquiredAdapter: () => memoryAdapter(),
    dispose: () => Effect.void,
  }
})

runAgentCommsContract('memory adapter (allowlist mode for UnknownIdentity coverage)', async () => {
  const adapter = await Effect.runPromise(
    memoryAdapter({
      acquirableNames: ['hermes-agent', 'hermes-agent-conflict', 'hermes-agent-cycle'],
    }),
  )
  await Effect.runPromise(adapter.identity.acquire(decodeBotNameSync('hermes-agent')))
  return {
    comms: adapter,
    seedChannel: (name) => adapter.seedChannel(name).pipe(Effect.orDie),
    seedAgent: (name) => adapter.seedAgent(name).pipe(Effect.orDie),
    newUnacquiredAdapter: () => memoryAdapter({ acquirableNames: ['hermes-agent-cycle'] }),
    unacquirableName: 'no-such-bot',
    dispose: () => Effect.void,
  }
})
