import { describe, expect, test } from 'bun:test'
import type { MessageInbox, SubscriptionTarget } from '@commy/core/ports'
import { decodeChannelNameSync, decodeThreadNameSync } from '@commy/core/ports'
import { Effect, Option } from 'effect'
import type { ProjectSlug, SessionId } from './bootstrap.ts'
import { parseSessionId } from './bootstrap.ts'
import { createNarrowSet } from './narrow-set.ts'
import type { SubscribeIntent } from './subscribe-parser.ts'
import { persistSubscriptions, restoreOrSeedSubscriptions } from './subscription-restore.ts'
import type { SubscriptionStore } from './subscription-store.ts'

const SID = '11111111-1111-4111-8111-111111111111'
const sid = (raw: string): SessionId => Option.getOrThrow(parseSessionId(raw))

const mentions: SubscribeIntent = { kind: 'mentions' }
const channel = (name: string): SubscribeIntent => ({
  kind: 'channel',
  channelName: decodeChannelNameSync(name),
})
const thread = (c: string, t: string): SubscribeIntent => ({
  kind: 'thread',
  channelName: decodeChannelNameSync(c),
  threadName: decodeThreadNameSync(t),
})

const sortIntents = (intents: ReadonlyArray<SubscribeIntent>): ReadonlyArray<SubscribeIntent> =>
  [...intents].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))

const stubStore = (read: SubscriptionStore['read']): SubscriptionStore => ({
  read,
  write: () => Effect.void,
})

const capturingInbox = (): {
  readonly inbox: Pick<MessageInbox, 'subscribe'>
  readonly targets: SubscriptionTarget[]
} => {
  const targets: SubscriptionTarget[] = []
  return {
    inbox: { subscribe: (t: SubscriptionTarget) => Effect.sync(() => void targets.push(t)) },
    targets,
  }
}

describe('restoreOrSeedSubscriptions', () => {
  test('store absent → registers defaults, leaves the narrow set otherwise untouched', async () => {
    const narrowSet = createNarrowSet()
    let defaultsCall: { readonly project: ProjectSlug | undefined } | undefined
    await Effect.runPromise(
      restoreOrSeedSubscriptions(
        {
          subscriptionStore: stubStore(() => Effect.succeed(Option.none())),
          narrowSet,
          inbox: capturingInbox().inbox,
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
    expect(narrowSet.intents()).toEqual([])
  })

  test('store present → replaces the narrow set with the persisted intents, skips defaults', async () => {
    const narrowSet = createNarrowSet()
    narrowSet.add(channel('stale')) // boot-seeded COMMY_SUBSCRIBE that must be wiped
    const persisted = [mentions, channel('commy'), thread('commy', 'work')]
    let defaultsCalled = false
    const cap = capturingInbox()
    await Effect.runPromise(
      restoreOrSeedSubscriptions(
        {
          subscriptionStore: stubStore(() => Effect.succeed(Option.some(persisted))),
          narrowSet,
          inbox: cap.inbox,
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
    expect(sortIntents(narrowSet.intents())).toEqual(sortIntents(persisted))
    // every restored intent is also wired on the substrate side
    expect(cap.targets.length).toBe(persisted.length)
  })

  test('store present but empty → narrow set becomes empty, defaults are NOT applied', async () => {
    const narrowSet = createNarrowSet()
    narrowSet.add(channel('stale'))
    let defaultsCalled = false
    await Effect.runPromise(
      restoreOrSeedSubscriptions(
        {
          subscriptionStore: stubStore(() => Effect.succeed(Option.some([]))),
          narrowSet,
          inbox: capturingInbox().inbox,
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
    expect(narrowSet.intents()).toEqual([])
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
