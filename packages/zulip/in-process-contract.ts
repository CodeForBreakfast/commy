/**
 * The shared contract's environment for the Zulip adapter, built on a fresh
 * in-process realm per test: no socket, no live realm, no credentials.
 *
 * Test-only — never imported by production code.
 */

import { decodeBotNameSync, decodeChannelIdSync, decodeChannelNameSync } from '@commy/core/ports'
import type { ContractFactory } from '@commy/testing/contract'
import { HttpClient } from '@effect/platform'
import { Effect, Option, Redacted } from 'effect'
import { type ZulipAdapter, zulipAdapter } from './adapter.ts'
import { CHANNEL_DESCRIPTION_LIMIT, makeInProcessRealm, TOPIC_LIMIT } from './in-process-realm.ts'
import { permalinkBase, withChannelPermalink } from './permalink.ts'

const SELF_NAME = decodeBotNameSync('contract-self')

export const inProcessContractFactory: ContractFactory = async () => {
  const realm = await Effect.runPromise(makeInProcessRealm)

  const buildAdapter = (): Effect.Effect<ZulipAdapter> =>
    zulipAdapter({
      realmUrl: realm.url,
      minterEmail: realm.minter.email,
      minterApiKey: Redacted.make(realm.minter.apiKey),
    }).pipe(Effect.provideService(HttpClient.HttpClient, realm.client))

  const adapter = await Effect.runPromise(buildAdapter())
  await Effect.runPromise(adapter.identity.acquire(SELF_NAME))

  return {
    comms: adapter,
    seedChannel: (name) =>
      realm.addChannel(name).pipe(
        Effect.map((channel) =>
          withChannelPermalink(permalinkBase({ realmUrl: realm.url }), {
            id: decodeChannelIdSync(String(channel.streamId)),
            name: decodeChannelNameSync(channel.name),
          }),
        ),
      ),
    seedAgent: (name) =>
      realm.addUser(name, 'bot').pipe(
        Effect.zipRight(adapter.identity.resolve(decodeBotNameSync(name))),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.die(new Error(`seedAgent: ${name} is not resolvable`)),
            onSome: Effect.succeed,
          }),
        ),
        Effect.orDie,
      ),
    newUnacquiredAdapter: buildAdapter,
    // A peer posts through its own adapter, bound to the peer by the same
    // acquire any seat would use.
    peerPost: (peer, channel, body, opts) =>
      buildAdapter().pipe(
        Effect.tap((peerAdapter) => peerAdapter.identity.acquire(decodeBotNameSync(peer.name))),
        Effect.flatMap((peerAdapter) => peerAdapter.publisher.post(channel, body, opts)),
        Effect.asVoid,
        Effect.orDie,
      ),
    channelDescriptionLimit: CHANNEL_DESCRIPTION_LIMIT,
    threadNameLimit: TOPIC_LIMIT,
    // The realm and its adapters hold nothing outside the heap, so there is
    // nothing to tear down.
    dispose: () => Effect.void,
  }
}
