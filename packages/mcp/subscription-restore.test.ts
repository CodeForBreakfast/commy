import { describe, expect, test } from 'bun:test'
import { decodeChannelNameSync } from '@commy/core/ports'
import { Effect, Option } from 'effect'
import type { ProjectSlug } from './bootstrap.ts'
import { createNarrowSet } from './narrow-set.ts'
import type { SubscribeIntent } from './subscribe-parser.ts'
import { persistSubscriptions, seedDefaultsIfFresh } from './subscription-restore.ts'
import type { SubscriptionStore } from './subscription-store.ts'

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
  advanceCursor: () => Effect.void,
})

// The seed half of the old restore-or-seed, split out for the reactive core:
// restore now reacts to the session_id via a boot-forked `restoreSubscriptions`
// off the session-bound store, leaving seeding as its own store-gated,
// restore-free step. The `Pick` deps carry no `narrowSet`/`inbox`, so seeding
// structurally CANNOT restore — it only registers the acquire-gated Type-2
// defaults, and only for a fresh session (store absent).
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
        undefined,
      ),
    )
    expect(defaultsCalled).toBe(false)
  })
})

describe('persistSubscriptions', () => {
  test('writes the current narrow-set snapshot to the session-bound store', async () => {
    const narrowSet = createNarrowSet()
    narrowSet.add(mentions)
    narrowSet.add(channel('commy'))
    const written: ReadonlyArray<SubscribeIntent>[] = []
    const store: Pick<SubscriptionStore, 'write'> = {
      write: (intents) => Effect.sync(() => void written.push(intents)),
    }
    await Effect.runPromise(persistSubscriptions(store, narrowSet))
    expect(written.length).toBe(1)
    expect(sortIntents(written[0] ?? [])).toEqual(sortIntents([mentions, channel('commy')]))
  })
})
