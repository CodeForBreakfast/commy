/**
 * The shared contract's environment for the Zulip adapter, built on a fresh
 * in-process realm per test: no socket, no live realm, no credentials.
 *
 * Test-only — never imported by production code.
 */

import {
  type AcquiredIdentity,
  type BindError,
  decodeBotNameSync,
  decodeChannelIdSync,
  decodeChannelNameSync,
  type Identity,
} from '@commy/core/ports'
import type { AttachmentContractFactory } from '@commy/testing/contract'
import { HttpClient } from '@effect/platform'
import { Array as Arr, Effect, Option, Redacted } from 'effect'
import { type ZulipAdapter, zulipAdapter } from './adapter.ts'
import {
  CHANNEL_DESCRIPTION_LIMIT,
  MESSAGE_LENGTH_LIMIT,
  makeInProcessRealm,
  TOPIC_LIMIT,
} from './in-process-realm.ts'
import { permalinkBase, withChannelPermalink } from './permalink.ts'

const SELF_NAME = decodeBotNameSync('contract-self')

export const inProcessContractFactory: AttachmentContractFactory = async () => {
  const realm = await Effect.runPromise(makeInProcessRealm)

  const buildAdapter = (
    bindOnDemand?: Effect.Effect<AcquiredIdentity, BindError>,
  ): Effect.Effect<ZulipAdapter> =>
    zulipAdapter({
      realmUrl: realm.url,
      minterEmail: realm.minter.email,
      minterApiKey: Redacted.make(realm.minter.apiKey),
      ...(bindOnDemand === undefined ? {} : { bindOnDemand }),
    }).pipe(Effect.provideService(HttpClient.HttpClient, realm.client))

  const adapter = await Effect.runPromise(buildAdapter())
  await Effect.runPromise(adapter.identity.acquire(SELF_NAME))

  const seedAgent = (name: string): Effect.Effect<Identity> =>
    realm.addUser(name, 'bot').pipe(
      Effect.zipRight(adapter.identity.resolve(decodeBotNameSync(name))),
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.die(new Error(`seedAgent: ${name} is not resolvable`)),
          onSome: Effect.succeed,
        }),
      ),
      Effect.orDie,
    )

  return {
    comms: adapter,
    attachments: adapter,
    seedChannel: (name) =>
      realm.addChannel(name).pipe(
        Effect.map((channel) =>
          withChannelPermalink(permalinkBase({ realmUrl: realm.url }), {
            id: decodeChannelIdSync(String(channel.streamId)),
            name: decodeChannelNameSync(channel.name),
          }),
        ),
      ),
    seedAgent,
    deactivateAgent: (agent) => realm.deactivateUser(Number(agent.id)),
    seedHuman: (name) =>
      realm.addUser(name, 'human').pipe(
        Effect.flatMap((userId) =>
          adapter.directory
            .listHumans()
            .pipe(Effect.map(Arr.findFirst((human) => human.id === String(userId)))),
        ),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.die(new Error(`seedHuman: ${name} is not listed`)),
            onSome: Effect.succeed,
          }),
        ),
        Effect.orDie,
      ),
    setPresence: (human, presence) =>
      realm.setPresence(Number(human.id), presence === 'online' ? 'active' : presence),
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
    revokeBoundCredential: () =>
      adapter.identity
        .currentIdentity()
        .pipe(Effect.flatMap((me) => realm.revokeApiKey(Number(me.id)))),
    channelDescriptionLimit: CHANNEL_DESCRIPTION_LIMIT,
    messageLengthLimit: MESSAGE_LENGTH_LIMIT,
    threadNameLimit: TOPIC_LIMIT,
    // The realm and its adapters hold nothing outside the heap, so there is
    // nothing to tear down.
    dispose: () => Effect.void,
  }
}
