import { expect, test } from 'bun:test'
import type { InboundEvent, MessageRef, SubscriptionTarget } from '@commy/core/ports'
import {
  ChannelPermalinkSchema,
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeEmojiSync,
  decodeIdentityIdSync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  MessagePermalinkSchema,
  ThreadPermalinkSchema,
} from '@commy/core/ports'
import { type MemoryAdapter, memoryAdapter } from '@commy/memory/adapter'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import { Effect, HashSet, Option, type Scope, SynchronizedRef } from 'effect'
import type { SessionId } from './bootstrap.ts'
import { createEphemeralIdentityCache } from './identity-cache.ts'
import { buildMcpServer } from './mcp-server.ts'
import { createNarrowSet, type NarrowSet } from './narrow-set.ts'
import { intentToTarget, type SubscribeIntent } from './subscribe-parser.ts'
import { restoreSubscriptions } from './subscription-restore.ts'
import type { SubscriptionStore } from './subscription-store.ts'
import { registerTools } from './tools.ts'

// The incident (comms-k7cv): a resumed ephemeral seat whose first move is the
// passive `current_identity` reachability check must rehydrate its persisted
// narrow set — otherwise the event pump matches an empty set and silently drops
// inbound, including a human's decision reaction on a subscribed thread.

const DECISIONS_CHANNEL = 'commy'
const DECISIONS_THREAD = 'ref-types-split-decisions'
const SID_RESUME = '61b08d76-0000-4000-8000-000000000001'

const decisionsThreadIntent: SubscribeIntent = {
  kind: 'thread',
  channelName: decodeChannelNameSync(DECISIONS_CHANNEL),
  threadName: decodeThreadNameSync(DECISIONS_THREAD),
}

const inMemorySubscriptionStore = (
  seed: ReadonlyMap<string, ReadonlyArray<SubscribeIntent>> = new Map(),
): SubscriptionStore => {
  const store = new Map<string, ReadonlyArray<SubscribeIntent>>(seed)
  return {
    read: (id: SessionId) => Effect.sync(() => Option.fromNullable(store.get(id as string))),
    write: (id: SessionId, intents: ReadonlyArray<SubscribeIntent>) =>
      Effect.sync(() => void store.set(id as string, intents)),
  }
}

const reactionOnDecisionsThread = (): InboundEvent => {
  const channel = {
    id: decodeChannelIdSync(DECISIONS_CHANNEL),
    name: decodeChannelNameSync(DECISIONS_CHANNEL),
    permalink: ChannelPermalinkSchema.make(
      `https://zulip.example.com/#narrow/channel/${DECISIONS_CHANNEL}`,
    ),
  }
  const target: MessageRef = {
    id: decodeMessageIdSync('msg-decisions-ask'),
    channel,
    thread: Option.some({
      name: decodeThreadNameSync(DECISIONS_THREAD),
      permalink: ThreadPermalinkSchema.make(
        `https://zulip.example.com/#narrow/channel/${DECISIONS_CHANNEL}/topic/${DECISIONS_THREAD}`,
      ),
    }),
    permalink: MessagePermalinkSchema.make(
      `https://zulip.example.com/#narrow/channel/${DECISIONS_CHANNEL}/topic/${DECISIONS_THREAD}/near/1`,
    ),
  }
  return {
    kind: 'reaction-added',
    target,
    emoji: decodeEmojiSync('one'),
    by: {
      id: decodeIdentityIdSync('user:graeme'),
      name: decodeDisplayNameSync('graeme'),
      kind: 'human',
    },
  }
}

interface ResumeRig {
  readonly client: Client
  readonly adapter: MemoryAdapter
  readonly narrowSet: NarrowSet
  readonly substrateSubscribes: ReadonlyArray<SubscriptionTarget>
}

// Boot a fresh MCP server whose subscription store already holds the persisted
// narrow set of a prior session — the exact state after an MCP-child reboot on
// resume: in-memory narrow set empty, disk snapshot intact, same session_id.
// Restore is wired ONLY through `ensureSessionSubscriptions` (no onAcquire hook),
// isolating the current_identity path: the seat must rehydrate without acquiring.
const buildResumeRig = (
  persisted: ReadonlyArray<SubscribeIntent>,
): Effect.Effect<ResumeRig, never, Scope.Scope> =>
  Effect.gen(function* () {
    const adapter = yield* memoryAdapter()
    const substrateSubscribes: SubscriptionTarget[] = []
    const realSubscribe = adapter.inbox.subscribe.bind(adapter.inbox)
    const inbox = {
      ...adapter.inbox,
      subscribe: (target: SubscriptionTarget) =>
        Effect.sync(() => void substrateSubscribes.push(target)).pipe(
          Effect.zipRight(realSubscribe(target)),
        ),
    }
    const spiedAdapter = { ...adapter, inbox } as MemoryAdapter

    const identityCache = yield* createEphemeralIdentityCache({
      acquire: adapter.identity.acquire,
      release: adapter.identity.release,
      idleReleaseMs: 60 * 60 * 1000,
    })
    const narrowSet = createNarrowSet()
    const subscriptionStore = inMemorySubscriptionStore(new Map([[SID_RESUME, persisted]]))

    const restoredSessions = yield* SynchronizedRef.make(HashSet.empty<SessionId>())
    const restoreSessionSubscriptions = (sessionId: SessionId): Effect.Effect<void> =>
      SynchronizedRef.updateEffect(restoredSessions, (seen) =>
        HashSet.has(seen, sessionId)
          ? Effect.succeed(seen)
          : restoreSubscriptions({ subscriptionStore, narrowSet, inbox }, sessionId).pipe(
              Effect.catchAll(() => Effect.succeed(false)),
              Effect.map((restored) => (restored ? HashSet.add(seen, sessionId) : seen)),
            ),
      )

    const server = buildMcpServer()
    registerTools(server, {
      adapter: spiedAdapter,
      identityCache,
      narrowSet,
      restoreSessionSubscriptions,
    })

    const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair()
    const client = new Client({ name: 'commy-resume-test', version: '0.0.0' }, { capabilities: {} })
    yield* Effect.promise(() =>
      Promise.all([server.connect(serverTransport), client.connect(clientTransport)]),
    )
    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        await client.close()
        await server.close()
      }),
    )

    return { client, adapter: spiedAdapter, narrowSet, substrateSubscribes }
  })

test('passive current_identity on resume rehydrates the persisted narrow set so a decisions-topic reaction is deliverable', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildResumeRig([decisionsThreadIntent, { kind: 'mentions' }])

        // Rebooted: nothing restored yet, so the pump would drop the reaction.
        expect(rig.narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(false)

        // The resume's first move: the passive "am I still bound?" check.
        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'current_identity',
            arguments: { session_id: SID_RESUME },
          }),
        )

        // Passivity preserved: no identity acquired by the check.
        expect(result.structuredContent).toEqual({ state: 'unbound', identity: null })

        // But the narrow set is now live — the human's :one: reaction matches.
        expect(rig.narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(true)
        // and the thread was re-subscribed on the substrate.
        expect(rig.substrateSubscribes).toContainEqual(intentToTarget(decisionsThreadIntent))
      }),
    ),
  ))

// The fresh-session contract: restore-only must NOT seed. A passive
// current_identity for a session_id the store has never seen registers nothing
// — Type-2 defaults stay on the acquire hook (server.integration.test.ts).
const SID_FRESH = 'f9e5f9e5-0000-4000-8000-000000000002'

test('passive current_identity for a never-seen session restores nothing (no fresh-session seed)', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildResumeRig([decisionsThreadIntent])

        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'current_identity',
            arguments: { session_id: SID_FRESH },
          }),
        )

        expect(result.structuredContent).toEqual({ state: 'unbound', identity: null })
        expect(rig.narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(false)
        expect(rig.substrateSubscribes).toEqual([])
      }),
    ),
  ))
