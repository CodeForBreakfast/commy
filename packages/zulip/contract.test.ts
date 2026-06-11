import type { ChannelRef } from '@commy/core/ports'
import { decodeBotNameSync, decodeChannelIdSync, decodeChannelNameSync } from '@commy/core/ports'
import { runAgentCommsContract } from '@commy/testing/contract'
import { FetchHttpClient, HttpClient } from '@effect/platform'
import { Effect, Option, Redacted } from 'effect'
import { zulipAdapter } from './adapter.ts'
import { ApiKey, BotEmail, RealmUrl } from './http.ts'
import { startStatefulZulipRealm } from './test-realm.ts'

const httpClient = Effect.runSync(HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer)))

let nextUserId = 1000

const allocUserId = (): number => nextUserId++

runAgentCommsContract('zulip adapter', async () => {
  const realm = startStatefulZulipRealm()
  const selfId = allocUserId()
  const selfName = decodeBotNameSync('hermes-agent')
  realm.addMember({
    user_id: selfId,
    email: `${selfName}-bot@example.com`,
    full_name: selfName,
    is_bot: true,
    is_active: true,
  })

  const buildAdapter = () =>
    Effect.runPromise(
      zulipAdapter({
        realmUrl: Effect.runSync(RealmUrl(realm.url)),
        minterEmail: Effect.runSync(BotEmail('minter@example.com')),
        minterApiKey: Redacted.make(Effect.runSync(ApiKey('minter-key'))),
      }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient)),
    )

  const adapter = await buildAdapter()
  await Effect.runPromise(adapter.identity.acquire(selfName))
  // After acquire's regenerate-key path, the test-realm set selfId to
  // selfId already (the post-regenerate hook flips realm.self to the
  // regenerated bot). Subsequent /messages POSTs are attributed to it.

  // Track adapters spun up via newUnacquiredAdapter so dispose can stop
  // them — the contract suite owns explicit lifecycle calls but cannot
  // close the underlying ZulipAdapter to release event-loop handles.
  const extras: Array<Awaited<ReturnType<typeof buildAdapter>>> = []

  return {
    comms: adapter,
    seedChannel: (name) =>
      Effect.sync(() => {
        const stream = realm.addStream(name)
        const ref: ChannelRef = {
          id: decodeChannelIdSync(String(stream.stream_id)),
          name: decodeChannelNameSync(stream.name),
        }
        return ref
      }),
    seedAgent: (name) =>
      Effect.gen(function* () {
        const userId = allocUserId()
        realm.addMember({
          user_id: userId,
          email: `${name}-bot@example.com`,
          full_name: name,
          is_bot: true,
          is_active: true,
        })
        const found = yield* adapter.identity.resolve(name).pipe(Effect.orDie)
        if (Option.isNone(found))
          throw new Error(`zulip realm: failed to resolve seeded agent ${name}`)
        return found.value
      }),
    newUnacquiredAdapter: () =>
      Effect.promise(async () => {
        const fresh = await buildAdapter()
        extras.push(fresh)
        return fresh
      }),
    dispose: () =>
      Effect.promise(async () => {
        for (const extra of extras) {
          await extra.close()
        }
        await adapter.close()
        await realm.stop()
      }),
  }
})
