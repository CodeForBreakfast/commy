/**
 * Zulip-only live addendum — the slice the cross-adapter contract can't express.
 *
 * The shared behaviour (acquire/release lifecycle, post/subscribe/events/replay,
 * react/unreact) is covered by `runAgentCommsContract` running against the real
 * realm (the live ContractFactory). This file keeps only the Zulip-specific
 * facts that contract has no vocabulary for:
 *
 * - **upload byte round-trip** — bytes survive a real /user_uploads POST + GET.
 * - **"Zulip can always mint"** — there is no unacquirable name; the minter can
 *   always mint (a substrate capability Discord will not share).
 * - **repeat-acquire regenerates the API key** — the regenerated key
 *   authenticates as the same bot.
 * - **resolve-then-post appends to the resolved topic** — resolution is a
 *   *rename* (✔ prefix) and Zulip creates topics implicitly on send, so only a
 *   real realm can prove a post by clean name lands in the resolved topic
 *   rather than minting a bare-name sibling. A fake substrate that models
 *   resolution as a flag masks this class of bug entirely.
 * - **unresolve onto an occupied bare name merges the two halves** — the
 *   rename is a server-side move, so only the realm can say what it does when
 *   the destination topic already exists. It merges, in message-id order.
 *
 * **Local-only** — env-gated, never runs in CI. With env vars unset the
 * suite skips silently so default `bun test` stays green.
 *
 * Required env vars (all must be present, else the suite is skipped):
 * - `ZULIP_SITE`              e.g. `https://zulip.example.com`
 * - `ZULIP_MINTER_EMAIL`      minter user email
 * - `ZULIP_MINTER_API_KEY`    minter user API key
 *
 * The resolve-then-post suite additionally needs a channel to post into, and
 * skips on its own when it is absent:
 * - `ZULIP_LIVE_CHANNEL_NAME` an existing channel the live tests post into
 */

import { describe, expect, test } from 'bun:test'
import type { BotName, Credentials, DisplayName, ReleaseOpts } from '@commy/core/ports'
import {
  decodeBotNameSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeMessageBodySync,
  decodeThreadNameSync,
} from '@commy/core/ports'
import { FetchHttpClient, HttpClient } from '@effect/platform'
import { Duration, Effect, Option, Redacted, Schema } from 'effect'
import type { ZulipAdapter } from './adapter.ts'
import { zulipAdapter } from './adapter.ts'
import { ApiKey, BotEmail, makeZulipHttp, RealmUrl, type ZulipHttp } from './http.ts'
import { applyResolvedPrefix } from './resolved-topic.ts'

interface LiveEnv {
  readonly site: string
  readonly minterEmail: string
  readonly minterApiKey: string
}

const readEnv = (): LiveEnv | undefined => {
  const required = {
    site: process.env['ZULIP_SITE'],
    minterEmail: process.env['ZULIP_MINTER_EMAIL'],
    minterApiKey: process.env['ZULIP_MINTER_API_KEY'],
  }
  for (const value of Object.values(required)) {
    if (value === undefined || value.length === 0) return undefined
  }
  return required as Record<keyof typeof required, string>
}

const env = readEnv()
const describeLive = env === undefined ? describe.skip : describe

/** Narrow `env` inside test callbacks where describe.skip has already guarded. */
const liveEnv = (): LiveEnv => {
  if (env === undefined) throw new Error('unreachable — describe.skip should have run')
  return env
}

/** Extract email + apiKey from an acquired identity's credentials record. */
const credentialsOf = (creds: Credentials): { email: string; apiKey: string } => {
  const email = creds['email']
  const apiKey = creds['apiKey']
  if (email === undefined || apiKey === undefined) {
    throw new Error('acquired credentials missing email or apiKey')
  }
  return { email, apiKey }
}

const httpClient = Effect.runSync(HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer)))

const NAME_PREFIX = 'cc-live-test'

const uniqueName = (role: string): DisplayName =>
  decodeDisplayNameSync(
    `${NAME_PREFIX}-${role}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
  )

const buildAdapter = (): Effect.Effect<ZulipAdapter> =>
  Effect.gen(function* () {
    const e = liveEnv()
    return yield* zulipAdapter({
      realmUrl: yield* RealmUrl(e.site),
      minterEmail: yield* BotEmail(e.minterEmail),
      minterApiKey: Redacted.make(yield* ApiKey(e.minterApiKey)),
    })
  }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient), Effect.orDie)

// Spacing between minter-authenticated calls. The live suite mints,
// regenerates and deactivates bots through the shared minter; bursting them
// trips Zulip's per-user rate limit on the shared realm (zulip.example.com),
// which knocks other live clients off MCP for the duration of the cool-off. Sleep
// before every acquire/release keeps the suite under the limit and leaves
// headroom for interactive sessions sharing the realm.
const MINTER_PACE = Duration.millis(900)

const pacedAcquire = (
  adapter: ZulipAdapter,
  name: BotName,
): ReturnType<ZulipAdapter['identity']['acquire']> =>
  Effect.sleep(MINTER_PACE).pipe(Effect.zipRight(adapter.identity.acquire(name)))

const pacedRelease = (
  adapter: ZulipAdapter,
  opts?: ReleaseOpts,
): ReturnType<ZulipAdapter['identity']['release']> =>
  Effect.sleep(MINTER_PACE).pipe(Effect.zipRight(adapter.identity.release(opts)))

const botHttp = (e: LiveEnv, creds: { email: string; apiKey: string }): Effect.Effect<ZulipHttp> =>
  Effect.gen(function* () {
    return yield* makeZulipHttp({
      realmUrl: yield* RealmUrl(e.site),
      email: yield* BotEmail(creds.email),
      apiKey: yield* ApiKey(creds.apiKey),
    })
  }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient), Effect.orDie)

const minterHttp = (e: LiveEnv): Effect.Effect<ZulipHttp> =>
  botHttp(e, { email: e.minterEmail, apiKey: e.minterApiKey })

const sendMessageSchema = Schema.Struct({
  result: Schema.Literal('success'),
  id: Schema.Int,
})

/** Just enough of GET /messages to prove a topic is empty. */
const messagesInTopicSchema = Schema.Struct({
  result: Schema.Literal('success'),
  messages: Schema.Array(Schema.Struct({ id: Schema.Int })),
})

const usersMeSchema = Schema.Struct({
  result: Schema.Literal('success'),
  user_id: Schema.Int,
  full_name: Schema.String,
  is_bot: Schema.Boolean,
  is_active: Schema.Boolean,
})

describeLive('zulip live identity — Zulip-only addendum — zulip.example.com', () => {
  test(
    'repeat acquire on the same name regenerates the API key (new key authenticates, old key may also still work)',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const name = uniqueName('regen')
          const first = yield* buildAdapter()
          yield* Effect.acquireUseRelease(
            pacedAcquire(first, decodeBotNameSync(name)),
            (firstAcq) =>
              Effect.gen(function* () {
                const secondAdapter = yield* buildAdapter()
                yield* Effect.acquireUseRelease(
                  pacedAcquire(secondAdapter, decodeBotNameSync(name)),
                  (secondAcq) =>
                    Effect.gen(function* () {
                      expect(secondAcq.identity.id).toEqual(firstAcq.identity.id)
                      // The two acquires return distinct credentials objects — the
                      // second is the regenerated key. Verify the regenerated key
                      // authenticates as the same bot.
                      const secondCreds = credentialsOf(secondAcq.credentials)
                      const newHttp = yield* botHttp(liveEnv(), secondCreds)
                      const me = yield* newHttp.get('/users/me', usersMeSchema)
                      expect(String(me.user_id)).toEqual(firstAcq.identity.id)
                    }),
                  () => pacedRelease(secondAdapter),
                )
              }),
            // First adapter's credentials were superseded by the regenerate;
            // release attempts a deactivate via the minter http (which still
            // works) and clears the in-memory binding.
            () => pacedRelease(first),
          )
        }),
      ),
    30_000,
  )

  test(
    'acquire of an UnknownIdentity-like name in our substrate still succeeds — Zulip can always mint',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          // Sanity: there's no `unacquirableName` for Zulip — the minter can
          // always mint. Captured to make the substrate's capability explicit
          // (Discord will not pass this).
          const adapter = yield* buildAdapter()
          const name = uniqueName('any')
          yield* Effect.acquireUseRelease(
            pacedAcquire(adapter, decodeBotNameSync(name)),
            (acquired) => Effect.sync(() => expect(acquired.identity.name).toEqual(name)),
            () => pacedRelease(adapter),
          )
        }),
      ),
    30_000,
  )
})

const liveChannelName = process.env['ZULIP_LIVE_CHANNEL_NAME']
const describeLiveChannel =
  env === undefined || liveChannelName === undefined || liveChannelName.length === 0
    ? describe.skip
    : describe

describeLiveChannel('zulip live resolve-then-post — zulip.example.com', () => {
  // The regression this file exists to catch (comms-00t0). Zulip has no
  // resolved flag — resolution *renames* the topic to `✔ <name>` — and Zulip
  // creates topics implicitly on send. So a post addressed to the clean name of
  // a resolved topic used to mint a bare-name *sibling*, silently splitting the
  // conversation at the resolve. The unit tests can only assert the topic
  // string we put on the wire; only a real realm proves Zulip agrees the two
  // messages are one conversation.
  //
  // The discriminating assertion is `readThread` returning BOTH messages.
  // readThread reads the bare name first and only falls back to the ✔ form when
  // it is empty — so had post forked a sibling, the bare name would hold just
  // the second message and this would come back with one. Two messages means no
  // sibling exists.
  test(
    'a post by clean name into a resolved topic appends to it, stays resolved, and mints no sibling',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = decodeChannelNameSync(liveChannelName ?? '')
          const thread = decodeThreadNameSync(
            `cc-live-resolve-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
          )
          const adapter = yield* buildAdapter()
          yield* Effect.acquireUseRelease(
            pacedAcquire(adapter, decodeBotNameSync(uniqueName('resolve'))),
            () =>
              Effect.gen(function* () {
                yield* adapter.publisher.post(
                  channel,
                  decodeMessageBodySync('opening the conversation'),
                  { thread },
                )
                yield* adapter.publisher.resolveThread(channel, thread)

                // The write under test: address the thread by its clean name
                // while the substrate knows it as `✔ <name>`.
                const appended = yield* adapter.publisher.post(
                  channel,
                  decodeMessageBodySync('appending after the resolve'),
                  { thread },
                )
                expect(Option.map(appended.thread, (t) => t.name)).toEqual(Option.some(thread))
                expect(Option.map(appended.thread, (t) => t.resolved)).toEqual(Option.some(true))

                // Both of our messages come back from ONE topic, in order. Had
                // post forked a sibling, the bare name would hold only the
                // second message — and readThread reads the bare name first, so
                // the opening message would be missing here.
                const messages = yield* adapter.history.readThread(channel, thread, {})
                const bodies = messages.map((m) => m.body)
                const opened = bodies.indexOf(decodeMessageBodySync('opening the conversation'))
                const appendedAt = bodies.indexOf(
                  decodeMessageBodySync('appending after the resolve'),
                )
                expect(opened).toBeGreaterThanOrEqual(0)
                expect(appendedAt).toBeGreaterThan(opened)
                // Between them sits Zulip's own resolve notification — the realm
                // posts one into the topic it renames. Asserted rather than
                // filtered out: its presence in this same topic is independent
                // proof the resolve and the append addressed one conversation.
                const notice = bodies.findIndex((b) => b.includes('marked this topic as resolved'))
                expect(notice).toBeGreaterThan(opened)
                expect(notice).toBeLessThan(appendedAt)
                // Appending did not re-open it — post never mutates thread state.
                for (const m of messages) {
                  expect(Option.map(m.ref.thread, (t) => t.resolved)).toEqual(Option.some(true))
                }
              }),
            () => pacedRelease(adapter),
          )
        }),
      ),
    30_000,
  )

  // What `unresolveThread` does when the bare name it renames onto is ALREADY
  // OCCUPIED (comms-g4oa). `setThreadResolved` has no occupied-name guard — it
  // PATCHes `✔ X` onto `X` with propagate_mode=change_all regardless — so commy
  // inherits whatever the server does, and only the realm can say what that is.
  //
  // Measured: Zulip MERGES. The source topic's messages fold into the occupied
  // destination and the ✔ sibling ceases to exist. The merge is ordered by
  // message id — a true chronological interleave, not a block-append — because
  // a Zulip topic is a narrow over the id-ordered message store. Nothing is
  // reordered and nothing is dropped. That makes `unresolveThread` the repair
  // for a thread already forked by the resolve-then-post bug, not a hazard.
  //
  // The interleave is the load-bearing assertion, and it needs a fork whose
  // halves are id-interleaved — which the bug alone cannot produce (once a bare
  // sibling exists, every later post lands in it). So the fork here is built by
  // hand: a `late` message is planted in the ✔ half with an id BETWEEN two
  // bare-half ids. A block-append would put it last; a merge puts it in the
  // middle.
  test(
    'unresolveThread onto an occupied bare name merges both halves into it, in message-id order',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const channel = decodeChannelNameSync(liveChannelName ?? '')
          const thread = decodeThreadNameSync(
            `cc-live-fork-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`,
          )
          const bareTopic = applyResolvedPrefix(thread, false)
          const resolvedTopic = applyResolvedPrefix(thread, true)
          const adapter = yield* buildAdapter()
          yield* Effect.acquireUseRelease(
            pacedAcquire(adapter, decodeBotNameSync(uniqueName('fork'))),
            (acquired) =>
              Effect.gen(function* () {
                // Raw sends, addressed at an explicit topic string — the point is
                // to bypass `addressThread` and mint the bare-name sibling the way
                // pre-comms-00t0 commy did. The publisher can no longer do this.
                const http = yield* botHttp(liveEnv(), credentialsOf(acquired.credentials))
                const sendTo = (topic: string, content: string) =>
                  http.post('/messages', sendMessageSchema, {
                    type: 'stream',
                    to: liveChannelName ?? '',
                    topic,
                    content,
                  })

                // The resolved half: two messages, then the rename to `✔ X`.
                yield* adapter.publisher.post(channel, decodeMessageBodySync('pre-resolve one'), {
                  thread,
                })
                yield* adapter.publisher.post(channel, decodeMessageBodySync('pre-resolve two'), {
                  thread,
                })
                yield* adapter.publisher.resolveThread(channel, thread)

                // The fork, and the id-interleave that discriminates merge from
                // block-append: `late` sits in the ✔ half but between the two
                // bare-half messages by id.
                yield* sendTo(bareTopic, 'forked sibling three')
                yield* sendTo(resolvedTopic, 'late, in the resolved half')
                yield* sendTo(bareTopic, 'forked sibling four')

                // The write under test — the destination topic already exists.
                yield* adapter.publisher.unresolveThread(channel, thread)

                // The ✔ sibling is gone. Probed at the substrate, by literal topic
                // string, rather than through `readThread` (whose bare-name-first
                // fallback would muddy what is being claimed): the topic held four
                // messages a moment ago and now holds none — so the source topic
                // moved wholesale, it did not partially merge.
                const leftBehind = yield* http.get('/messages', messagesInTopicSchema, {
                  anchor: 'newest',
                  num_before: 50,
                  num_after: 0,
                  narrow: JSON.stringify([
                    { operator: 'channel', operand: liveChannelName ?? '' },
                    { operator: 'topic', operand: resolvedTopic },
                  ]),
                  apply_markdown: false,
                })
                expect(leftBehind.messages).toEqual([])

                const messages = yield* adapter.history.readThread(channel, thread, {})
                const bodies = messages.map((m) => m.body)
                const at = (needle: string) => bodies.findIndex((b) => b.includes(needle))

                // Every message from both halves survives, exactly once, in send
                // order. `late` between the two siblings is the interleave: it
                // came from the OTHER half, so a block-append could not place it
                // there.
                expect(at('pre-resolve one')).toBeGreaterThanOrEqual(0)
                expect(at('pre-resolve two')).toBeGreaterThan(at('pre-resolve one'))
                expect(at('forked sibling three')).toBeGreaterThan(at('pre-resolve two'))
                expect(at('late, in the resolved half')).toBeGreaterThan(at('forked sibling three'))
                expect(at('forked sibling four')).toBeGreaterThan(at('late, in the resolved half'))

                // Both of Zulip's own status notices ride along into the merged
                // topic — the resolve notice as a mid-thread artefact of the fork,
                // the unresolve notice appended by the write under test.
                expect(at('marked this topic as resolved')).toBeGreaterThan(at('pre-resolve two'))
                expect(at('marked this topic as unresolved')).toBeGreaterThan(
                  at('forked sibling four'),
                )

                // One thread, under the bare name, unresolved.
                for (const m of messages) {
                  expect(Option.map(m.ref.thread, (t) => t.name)).toEqual(Option.some(thread))
                  expect(Option.map(m.ref.thread, (t) => t.resolved)).toEqual(Option.some(false))
                }
              }),
            () => pacedRelease(adapter),
          )
        }),
      ),
    45_000,
  )
})

describeLive('zulip live upload round-trip — zulip.example.com', () => {
  // The unit tests prove uploadRaw shapes a multipart request; only a real
  // round-trip proves Django actually accepts that body and the bytes survive.
  // Uses the minter directly — two HTTP calls, no bot minting, no rate-limit
  // pacing needed.
  test('uploadRaw → downloadRaw round-trips the bytes through /user_uploads', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* minterHttp(liveEnv())
        const bytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x01, 0x02])
        const filename = `cc-live-upload-${Date.now()}.bin`

        const uploaded = yield* http.uploadRaw(filename, bytes)
        expect(uploaded.url).toMatch(/^\/user_uploads\//)
        expect(uploaded.filename.length).toBeGreaterThan(0)

        const back = yield* http.downloadRaw(uploaded.url)
        expect(new Uint8Array(back.data)).toEqual(bytes)
      }),
    ))
})
