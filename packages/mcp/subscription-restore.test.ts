import { describe, expect, test } from 'bun:test'
import { decodeChannelNameSync } from '@commy/core/ports'
import { Effect, Option } from 'effect'
import type { ProjectSlug, SessionId } from './bootstrap.ts'
import { parseSessionId } from './bootstrap.ts'
import { createNarrowSet } from './narrow-set.ts'
import type { SubscribeIntent } from './subscribe-parser.ts'
import { persistSubscriptions, seedDefaultsIfFresh } from './subscription-restore.ts'
import type { SubscriptionStore } from './subscription-store.ts'

const SID = '11111111-1111-4111-8111-111111111111'
const sid = (raw: string): SessionId => Option.getOrThrow(parseSessionId(raw))

const mentions: SubscribeIntent = { kind: 'mentions' }
const channel = (name: string): SubscribeIntent => ({
  kind: 'channel',
  channelName: decodeChannelNameSync(name),
})
const sortIntents = (intents: ReadonlyArray<SubscribeIntent>): ReadonlyArray<SubscribeIntent> =>
  [...intents].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))

const stubStore = (read: SubscriptionStore['read']): SubscriptionStore => ({
  read,
  write: () => Effect.void,
})

// The seed half of the old restore-or-seed, split out for the reactive core
// (comms-k7cv): restore now reacts to the session_id via the `Deferred` latch in
// `makeSessionRestore`, leaving seeding as its own store-gated, restore-free step.
// The `Pick` deps carry no `narrowSet`/`inbox`, so seeding structurally CANNOT
// restore — it only registers the acquire-gated Type-2 defaults, and only for a
// fresh session (store absent).
describe('seedDefaultsIfFresh', () => {
  test('store absent → registers defaults for a fresh session', async () => {
    let defaultsCall: { readonly project: ProjectSlug | undefined } | undefined
    await Effect.runPromise(
      seedDefaultsIfFresh(
        {
          subscriptionStore: stubStore(() => Effect.succeed(Option.none())),
          registerDefaults: (project) =>
            Effect.sync(() => {
              defaultsCall = { project }
            }),
        },
        sid(SID),
        undefined,
      ),
    )
    expect(defaultsCall).toEqual({ project: undefined })
  })

  test('store present → does NOT register defaults (a resume, not a fresh seed)', async () => {
    let defaultsCalled = false
    await Effect.runPromise(
      seedDefaultsIfFresh(
        {
          subscriptionStore: stubStore(() =>
            Effect.succeed(Option.some([mentions, channel('commy')])),
          ),
          registerDefaults: () =>
            Effect.sync(() => {
              defaultsCalled = true
            }),
        },
        sid(SID),
        undefined,
      ),
    )
    expect(defaultsCalled).toBe(false)
  })

  test('store present but empty → still a resume: defaults are NOT applied', async () => {
    let defaultsCalled = false
    await Effect.runPromise(
      seedDefaultsIfFresh(
        {
          subscriptionStore: stubStore(() => Effect.succeed(Option.some([]))),
          registerDefaults: () =>
            Effect.sync(() => {
              defaultsCalled = true
            }),
        },
        sid(SID),
        undefined,
      ),
    )
    expect(defaultsCalled).toBe(false)
  })
})

describe('persistSubscriptions', () => {
  test('writes the current narrow-set snapshot under the session id', async () => {
    const narrowSet = createNarrowSet()
    narrowSet.add(mentions)
    narrowSet.add(channel('commy'))
    const written: { sid: SessionId; intents: ReadonlyArray<SubscribeIntent> }[] = []
    const store: Pick<SubscriptionStore, 'write'> = {
      write: (s, intents) => Effect.sync(() => void written.push({ sid: s, intents })),
    }
    await Effect.runPromise(persistSubscriptions(store, narrowSet, sid(SID)))
    expect(written.length).toBe(1)
    expect(written[0]?.sid).toBe(sid(SID))
    expect(sortIntents(written[0]?.intents ?? [])).toEqual(
      sortIntents([mentions, channel('commy')]),
    )
  })
})
