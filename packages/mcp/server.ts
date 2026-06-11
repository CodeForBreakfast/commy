import { basename, join } from 'node:path'
import { stderrLoggerLayer } from '@codeforbreakfast/core/logging'
import type {
  AcquiredIdentity,
  AgentComms,
  InboxError,
  MessageInbox,
} from '@codeforbreakfast/core/ports'
import { decodeChannelName, decodeThreadName } from '@codeforbreakfast/core/ports'
import { attachmentReference } from '@codeforbreakfast/zulip/adapter'
import { FetchHttpClient, FileSystem, type HttpClient } from '@effect/platform'
import { BunFileSystem, BunRuntime } from '@effect/platform-bun'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js'
import type { Scope } from 'effect'
import {
  Cause,
  Clock,
  ConfigProvider,
  Data,
  Duration,
  Effect,
  Layer,
  Option,
  Predicate,
  Schedule,
} from 'effect'
import type { BotName, GitContext, ProjectSlug } from './bootstrap.ts'
import {
  readGitContext as defaultReadGitContext,
  deriveProject,
  type EnvConfigError,
  parseEnv,
  SubstrateAdapter,
  subscribeFromEnv,
  ZulipAdapterLive,
} from './bootstrap.ts'
import { catchUpChannels } from './channels-catch-up.ts'
import { CursorStoreTag, FileCursorStoreLive } from './cursor-store.ts'
import { createEnsureBound } from './ensure-bound.ts'
import type { Notifier } from './event-pump.ts'
import { channelNotifier, startEventPump } from './event-pump.ts'
import type { IdentityCache } from './identity-cache.ts'
import { createEphemeralIdentityCache, createSingleIdentityCache } from './identity-cache.ts'
import { buildMcpServer } from './mcp-server.ts'
import { type CatchUpError, catchUpMentions } from './mentions-catch-up.ts'
import type { NarrowSet } from './narrow-set.ts'
import { createNarrowSet } from './narrow-set.ts'
import { raceReleaseAgainstTimeout } from './release-shutdown.ts'
import type { SubscribeIntent, SubscribeTokenError } from './subscribe-parser.ts'
import { intentToTarget } from './subscribe-parser.ts'
import { registerTools } from './tools.ts'

/**
 * Boot failed in a way the operator must fix (today: the persistent-mode
 * eager acquire was rejected by the substrate). `makeProgram` emits the
 * canonical stderr diagnostic and then fails with this; `runMain`'s
 * default teardown maps the failure Exit to process exit code 1.
 */
export class BootError extends Data.TaggedError('BootError')<{
  readonly message: string
}> {}

/**
 * Non-service deps for {@link makeProgram}. Services (the substrate
 * adapter, cursor store, ConfigProvider, logger) arrive through the app
 * Layer; these are the remaining per-run knobs production defaults and
 * tests override.
 */
export interface ProgramParams {
  /**
   * MCP transport to connect before spawning the event pump. Production
   * passes `new StdioServerTransport()`; the integration harness passes
   * an `InMemoryTransport`. Omitted (unit tests) → the MCP server is
   * built but never connected, safe only when no inbound events flow.
   */
  readonly transport?: Transport
  /**
   * Override the notifier the event pump dispatches into. Defaults to
   * `channelNotifier(mcp)`. Tests substitute a spy to assert on the
   * wire shape without driving a transport.
   */
  readonly notifier?: Notifier
  /**
   * Git-context probe for per-call project derivation (ass-v7b4).
   * Defaults to the real `git -C <cwd>` shell-out. Tests inject a fake
   * so derivation doesn't depend on the runner's cwd or git state.
   */
  readonly readGitContext?: (cwd: string) => GitContext
  /**
   * Logger Layer for diagnostics. Provided both at the program edge (so
   * the boot fiber + forked pump route `Effect.log*` to STDERR off the
   * MCP STDOUT channel) and onto the ephemeral `onAcquire` callback,
   * which runs on the cache's own runtime edge and so can't inherit the
   * program fiber's logger FiberRef. Defaults to {@link stderrLoggerLayer};
   * tests pass `captureLogger(lines)`.
   */
  readonly loggerLayer?: Layer.Layer<never>
  /**
   * Completion trigger raced against the daemon event pump (comms-8nkv /
   * comms-4c26). Production passes {@link clientDisconnect} over
   * `process.stdin` so the server exits when its MCP client closes the
   * stdio pipe — the only disconnect signal a plain parent exit delivers
   * (no SIGINT/SIGTERM). Omitted (tests, and the one-shot integration
   * harness) → shutdown is driven solely by the event stream ending or a
   * fiber interrupt, as before.
   */
  readonly shutdownSignal?: Effect.Effect<void>
}

/** Race budget for `identity.release()` in the shutdown finalizer. */
const RELEASE_TIMEOUT_MS = 5000

/**
 * Idle timeout for the ephemeral identity cache. Per ass-2dhb body:
 * "N is conservative — an hour, say." Sessions that go an hour without
 * an attribution-producing tool call get their bot deactivated and the
 * slot cleared. A returning session re-acquires via Zulip's reactivate
 * + regenerate-api-key path.
 */
const EPHEMERAL_IDLE_RELEASE_MS = 60 * 60 * 1000
const EPHEMERAL_IDLE_SWEEP_INTERVAL_MS = 5 * 60 * 1000

/**
 * Fork the ephemeral idle sweep onto a periodic schedule scoped to the
 * enclosing fiber Scope (comms-spj3.26). Replaces the old
 * `setInterval`/`unref`/`clearInterval` triplet:
 *
 *   - `Schedule.spaced` keeps `intervalMs` between sweeps — matching the
 *     old timer's period. (`Effect.repeat` runs the body once at fork too;
 *     the boot-time sweep is a no-op since no slot is idle yet.)
 *   - `Effect.forkScoped` ties the sweep fiber to the caller's Scope, so
 *     when that Scope closes (the pump's scoped region unwinds on natural
 *     end or signal-driven interrupt) the fiber is interrupted — the
 *     `clearInterval`-in-`finally` equivalent, without a manual handle.
 *   - The forked fiber never blocks process exit: it's a child of the
 *     Scope, not the main fiber's join set (the old `.unref()` role).
 *
 * The sweep reads the current wall-clock from Effect's `Clock` rather
 * than `Date.now()`, so tests can drive it deterministically with
 * `TestClock`. `sweepIdle` is the deliberately Promise-shaped cache seam
 * (see `identity-cache.ts`), lifted via `Effect.promise` exactly as the
 * shutdown body lifts `adapter.close()`.
 */
export const forkIdleSweep = (
  cache: Pick<IdentityCache, 'sweepIdle'>,
  intervalMs: number,
): Effect.Effect<void, never, Scope.Scope> => {
  const sweep = Clock.currentTimeMillis.pipe(
    Effect.flatMap((nowMs) => Effect.promise(() => cache.sweepIdle(nowMs))),
  )
  return Effect.asVoid(
    Effect.forkScoped(Effect.repeat(sweep, Schedule.spaced(Duration.millis(intervalMs)))),
  )
}

/**
 * Type-2 default sub set for interactive CC sessions (comms-iyf). Fires
 * once per ephemeral slot, right after the substrate-side acquire
 * resolves: registers the universal `mentions` narrow plus the project
 * broadcast topic `thread:#<project>/general` (skipped when no project
 * slug could be derived).
 *
 * Failures are swallowed with a log line — the bot is already minted at
 * this point and refusing the caller's tool call over a transient
 * substrate hiccup would be worse than the missing default. The
 * operator can compose the narrows by hand via the `subscribe` MCP
 * tool. Per-narrow registration is idempotent on both sides (Set-backed
 * narrowSet, Zulip's per-stream subscribe).
 */
const createType2DefaultsOnAcquire = (
  narrowSet: NarrowSet,
  inbox: MessageInbox,
): ((identity: AcquiredIdentity, project: ProjectSlug | undefined) => Effect.Effect<void>) => {
  const registerIntent = (intent: SubscribeIntent): Effect.Effect<void, InboxError> =>
    Effect.sync(() => narrowSet.add(intent)).pipe(
      Effect.zipRight(
        intentToTarget(intent).pipe(Effect.flatMap((target) => inbox.subscribe(target))),
      ),
    )
  return (_identity, project) =>
    registerIntent({ kind: 'mentions' }).pipe(
      Effect.zipRight(
        project !== undefined
          ? Effect.all([decodeChannelName(project), decodeThreadName('general')]).pipe(
              Effect.flatMap(([channelName, threadName]) =>
                registerIntent({ kind: 'thread', channelName, threadName }),
              ),
            )
          : Effect.void,
      ),
      Effect.catchAll((err) =>
        Effect.logError(
          `commy plugin: Type-2 default narrow registration failed: ${Cause.pretty(Cause.fail(err))}`,
        ),
      ),
    )
}

/**
 * Type-1 default sub set for persistent project concierges (comms-c2k).
 * Fires once at boot, immediately after the persistent-mode eager
 * acquire resolves. Registers:
 *
 *   1. `mentions` — universal: every persistent bot hears @-mentions.
 *   2. `new-topics:<project>` — first message of every new topic in the
 *      project channel. Concierge-specific delivery rule so a fresh
 *      enquiry surfaces while replies in unrelated topics stay quiet.
 *   3. `thread:<project>/general` — project broadcast topic.
 *
 * (2) and (3) are skipped when no project slug resolves from
 * `COMMY_PROJECT`; (1) always runs. Failures swallow with a log
 * line — the bot is already minted at this point and refusing service
 * over a transient substrate hiccup would be worse than the missing
 * default. The operator can compose narrows by hand via the `subscribe`
 * MCP tool.
 *
 * Returns the intents that were submitted to the substrate so the
 * boot-time channels catch-up (comms-3wl) can include them in its
 * recent-traffic skim.
 */
const logType1Failure = (err: unknown): Effect.Effect<void> =>
  Effect.logError(
    `commy plugin: Type-1 default narrow registration failed: ${Cause.pretty(Cause.fail(err))}`,
  )

const registerType1DefaultsOnBoot = (
  inbox: MessageInbox,
  narrowSet: NarrowSet,
  project: ProjectSlug | undefined,
): Effect.Effect<ReadonlyArray<SubscribeIntent>> =>
  (project !== undefined
    ? Effect.all([decodeChannelName(project), decodeThreadName('general')]).pipe(
        Effect.map(
          ([channelName, threadName]): ReadonlyArray<SubscribeIntent> => [
            { kind: 'mentions' },
            { kind: 'new-topics-in-channel', channelName },
            { kind: 'thread', channelName, threadName },
          ],
        ),
      )
    : Effect.succeed<ReadonlyArray<SubscribeIntent>>([{ kind: 'mentions' }])
  ).pipe(
    Effect.flatMap((intents) =>
      Effect.forEach(intents, (intent) =>
        Effect.sync(() => narrowSet.add(intent)).pipe(
          Effect.zipRight(
            intentToTarget(intent).pipe(Effect.flatMap((target) => inbox.subscribe(target))),
          ),
        ),
      ).pipe(
        Effect.as(intents),
        Effect.catchAll((err) => logType1Failure(err).pipe(Effect.as(intents))),
      ),
    ),
    Effect.catchAll((err) =>
      logType1Failure(err).pipe(
        Effect.as([{ kind: 'mentions' }] as ReadonlyArray<SubscribeIntent>),
      ),
    ),
  )

/**
 * Default boot-time channel/thread catch-up window for persistent bots
 * (comms-3wl). 4 hours covers overnight downtime without flooding the
 * model on a busy channel. Overridable per-deployment via the
 * `COMMY_CATCHUP_WINDOW_SECONDS` env var; set to 0 to disable.
 */
const DEFAULT_CATCHUP_WINDOW_SECONDS = 4 * 3600

const buildIdentityCache = (
  adapter: Pick<AgentComms, 'identity'>,
  botName: BotName | undefined,
  ephemeralOnAcquire?: (
    acquired: AcquiredIdentity,
    project: ProjectSlug | undefined,
  ) => Effect.Effect<void>,
): IdentityCache => {
  if (botName !== undefined) {
    const ensureBound = createEnsureBound({
      acquire: adapter.identity.acquire,
      name: botName,
    })
    return createSingleIdentityCache({ ensureBound })
  }
  return createEphemeralIdentityCache({
    acquire: adapter.identity.acquire,
    release: adapter.identity.release,
    idleReleaseMs: EPHEMERAL_IDLE_RELEASE_MS,
    ...(ephemeralOnAcquire !== undefined ? { onAcquire: ephemeralOnAcquire } : {}),
  })
}

/**
 * The plugin's boot program as ONE composed Effect (comms-spj3.39),
 * from parse → reconcile → identity → tools → pump, run at a single
 * `runMain` edge. Services (substrate adapter, cursor store,
 * ConfigProvider, logger) arrive through the app Layer;
 * {@link ProgramParams} carries the remaining per-run knobs.
 *
 * Boot sequence per the canonical V1 design (ass-x09b, refined by
 * ass-220u for lazy mode, ass-2dhb for per-session identity):
 *   1. parseEnv          — required minter creds, optional bot name +
 *                          subscribe list + project override, from the
 *                          ambient ConfigProvider set at the app edge.
 *   2. SubstrateAdapter  — the driven adapter, from context. Its
 *                          `close()` is a layer finalizer, so teardown
 *                          drops out of this program.
 *   3. projectForCwd     — per-call project resolver (ass-v7b4):
 *                          COMMY_PROJECT > caller-cwd git remote
 *                          basename > git root basename > undefined.
 *   4. narrowSet + mcp + notifier + resolveNow — built before the cache
 *                          so the ephemeral `onAcquire` callback closes
 *                          over them (Type-2 defaults need narrowSet,
 *                          comms-iyf; mentions catch-up needs notifier +
 *                          cursorStore + now, comms-ae4).
 *   5. buildIdentityCache — single (persistent) or ephemeral (1-slot,
 *                          release-then-acquire across session_id
 *                          transitions; ass-2dhb).
 *   6. ★ env-driven branch on `parsed.botName`:
 *      • Persistent (botName set): eager acquire now. Rejection writes
 *        the canonical diagnostic and FAILS with BootError — runMain's
 *        teardown maps that to exit 1 (ass-x09b.5.4).
 *      • Ephemeral (botName unset): skip the boot acquire; the first
 *        attribution-producing call mints `cc-[<project>-]<sid>` lazily.
 *   7. release finalizer — registered after acquire, gated on
 *                          `boundIdentityIds().size > 0` (ass-220u).
 *                          LIFO ordering: it runs AFTER pump-cancel
 *                          (registered later) and BEFORE the substrate
 *                          `close()` finalizer (the outer layer scope),
 *                          giving cancel-pump → release → close.
 *   8. subscribeFromEnv  — apply COMMY_SUBSCRIBE tokens.
 *   9. registerTools     — wire the cache + projectForCwd into the MCP
 *                          tool surface.
 *  10. mcp.connect       — bind to the supplied transport (stdio in prod).
 *  11. persistent boot-time mentions + channels catch-up (comms-rxo /
 *      comms-3wl). No-op in ephemeral mode (onAcquire owns that path).
 *  12. startEventPump    — filter inbound events through the narrowSet,
 *                          dispatch via the notifier. The pump is a
 *                          daemon, so a `pump.cancel` finalizer stops it
 *                          on scope unwind (signal interrupt under
 *                          runMain, or natural stream end).
 */
export const makeProgram = (
  params: ProgramParams = {},
): Effect.Effect<
  void,
  BootError | EnvConfigError | SubscribeTokenError | InboxError,
  SubstrateAdapter | CursorStoreTag | FileSystem.FileSystem
> =>
  Effect.scoped(
    Effect.gen(function* () {
      const parsed = yield* parseEnv
      const adapter = yield* SubstrateAdapter
      const cursorStore = yield* CursorStoreTag
      // The download/upload tool builders execute against this filesystem,
      // captured once from context (BunFileSystem.layer, provided in the
      // app layer) instead of self-providing a platform layer per call
      // (comms-5db).
      const fs = yield* FileSystem.FileSystem
      // Diagnostics route to STDERR via the logger layer provided at the
      // program edge — STDOUT is the MCP JSON-RPC channel and must stay
      // pristine. onAcquire (cache edge) self-provides this same layer.
      const loggerLayer = params.loggerLayer ?? stderrLoggerLayer
      const readGitContext = params.readGitContext ?? defaultReadGitContext
      // Catch-up is non-fatal: a transient substrate hiccup — or a defect
      // such as the memory adapter's id-strict history throwing
      // UnknownChannel — must not refuse boot. `catchAllCause` so a defect
      // is logged and swallowed too, matching the try/catch it replaces.
      const logCatchUpFailure =
        (label: string) =>
        (cause: Cause.Cause<CatchUpError>): Effect.Effect<void> =>
          Effect.logError(`commy plugin: ${label} catch-up failed: ${Cause.pretty(cause)}`)
      // Per-call project resolver. Operator override (COMMY_PROJECT)
      // is authoritative; otherwise derive from the calling session's cwd
      // at call time (ass-v7b4) — process cwd is irrelevant.
      const projectForCwd = (cwd: string | undefined): ProjectSlug | undefined => {
        if (parsed.project !== undefined) return parsed.project
        if (cwd === undefined) return undefined
        return Option.getOrUndefined(deriveProject({ cwd, readGitContext }))
      }

      // Minter subscription reconcile (ass-6a77): boot-time backstop that
      // keeps the minter subscribed to every public stream. Non-fatal —
      // log + continue. Silent in the steady-state no-op case.
      yield* adapter.reconcileMinterSubscriptions().pipe(
        Effect.flatMap((reconcile) => {
          if (reconcile.error !== undefined) {
            return Effect.logError(`commy plugin: minter reconcile failed: ${reconcile.error}`)
          }
          if (reconcile.added.length > 0) {
            return Effect.logInfo(
              `commy plugin: minter reconcile — subscribed minter to ${reconcile.added.length} new public stream(s): ${reconcile.added.join(', ')}`,
            )
          }
          return Effect.void
        }),
      )

      const narrowSet = createNarrowSet()
      const mcp = buildMcpServer()
      const notifier = params.notifier ?? channelNotifier(mcp)

      // Ephemeral-mode post-acquire hook (comms-iyf + comms-ae4): register
      // Type-2 default subs and replay missed @-mentions on every fresh
      // slot. The cache runs this via ensure-bound's own runtime edge, so
      // the logger layer is provided here on the composed Effect — keeps
      // onAcquire diagnostics off the MCP STDOUT channel. Undefined in
      // persistent mode (which uses its own post-acquire catch-up below).
      const registerType2Defaults =
        parsed.botName === undefined
          ? createType2DefaultsOnAcquire(narrowSet, adapter.inbox)
          : undefined
      const ephemeralOnAcquire =
        parsed.botName === undefined
          ? (acquired: AcquiredIdentity, project: ProjectSlug | undefined): Effect.Effect<void> =>
              (registerType2Defaults !== undefined
                ? registerType2Defaults(acquired, project)
                : Effect.void
              ).pipe(
                Effect.zipRight(
                  catchUpMentions({
                    cursorStore,
                    inbox: adapter.inbox,
                    identityId: acquired.identity.id,
                    notifier,
                  }).pipe(Effect.catchAllCause(logCatchUpFailure('ephemeral mentions'))),
                ),
                Effect.provide(loggerLayer),
              )
          : undefined

      const identityCache = buildIdentityCache(adapter, parsed.botName, ephemeralOnAcquire)

      // Persistent mode (COMMY_BOT_NAME set): eager acquire so a
      // misconfigured concierge dies at boot rather than on first message
      // (ass-x09b.5.4). The single-identity cache ignores the session_id.
      // Ephemeral mode: skip — the first tool call mints lazily (ass-2dhb).
      let type1Intents: ReadonlyArray<SubscribeIntent> = []
      if (parsed.botName !== undefined) {
        const botName = parsed.botName
        const ensureBound = yield* identityCache.ensureBoundFor(undefined)
        type1Intents = yield* Effect.tryPromise({
          // `ensureBound()` is a Promise edge owned by the acquire seam
          // (ensure-bound.ts); it squashes its own Cause, so the caught
          // value is the raw error, wrapped verbatim into the BootError.
          try: () => ensureBound(),
          catch: (err): BootError =>
            new BootError({ message: Predicate.isError(err) ? err.message : String(err) }),
        }).pipe(
          Effect.matchEffect({
            onFailure: (bootErr) =>
              Effect.logError(
                `commy plugin: acquire("${botName}") failed: ${bootErr.message}`,
              ).pipe(Effect.zipRight(Effect.fail(bootErr))),
            // Type-1 defaults (comms-c2k): post-acquire register the
            // universal `mentions` narrow plus project-specific subs.
            onSuccess: () => registerType1DefaultsOnBoot(adapter.inbox, narrowSet, parsed.project),
          }),
        )
      }

      // Release-on-shutdown finalizer (E2.8). Registered after acquire and
      // BEFORE the pump-cancel finalizer, so LIFO teardown runs
      // pump-cancel → release; the substrate `close()` (outer layer scope)
      // then runs last. Gated on the acquisition state (ass-220u): an
      // ephemeral session that never acquired has nothing to release.
      //
      // Persistent mode (COMMY_BOT_NAME set) releases with
      // `{ persistent: true }` so the substrate keeps the pinned bot active —
      // deactivating it would force the next session's acquire onto the
      // admin-only reactivate path and wedge a Member-rights minter
      // (comms-ch7). Ephemeral seats deactivate as before.
      const releasePersistent = parsed.botName !== undefined
      yield* Effect.addFinalizer(() =>
        identityCache.boundIdentityIds().size > 0
          ? raceReleaseAgainstTimeout(
              adapter.identity.release({ persistent: releasePersistent }),
              RELEASE_TIMEOUT_MS,
            )
          : Effect.void,
      )

      // Ephemeral mode runs a periodic idle sweep (forked into this scope).
      const runsIdleSweep = parsed.botName === undefined

      const subscribedIntents = yield* subscribeFromEnv(adapter.inbox, narrowSet, parsed)

      const toolsCache = registerTools(mcp, {
        adapter,
        identityCache,
        narrowSet,
        projectForCwd,
        downloadFile: (urlPath) =>
          Effect.gen(function* () {
            const result = yield* adapter.downloadFile(urlPath)
            const dir = yield* fs.makeTempDirectory({ prefix: 'commy-dl-' })
            const filePath = join(dir, basename(urlPath))
            yield* fs.writeFile(filePath, result.data)
            return { filePath, contentType: result.contentType, size: result.data.byteLength }
          }),
        upload: (path) =>
          Effect.gen(function* () {
            const data = yield* fs.readFile(path)
            const result = yield* adapter.uploadFile(basename(path), data)
            return {
              reference: attachmentReference(result),
              filename: result.filename,
              size: data.byteLength,
            }
          }),
      })

      if (params.transport !== undefined) {
        const transport = params.transport
        yield* Effect.promise(() => mcp.connect(transport))
      }

      // V1: pump uses a single bot-id getter (the 1-slot cache, ass-2dhb).
      const getBotIdentityId = () => {
        const ids = identityCache.boundIdentityIds()
        return ids.size === 0 ? undefined : ids.values().next().value
      }

      // Missed-mentions catch-up on persistent-mode resume (comms-rxo) +
      // boot-time channel/thread catch-up (comms-3wl). Both non-fatal —
      // log + continue. Ephemeral mode runs the equivalent via onAcquire.
      const persistentBotId = parsed.botName !== undefined ? getBotIdentityId() : undefined
      if (persistentBotId !== undefined) {
        yield* catchUpMentions({
          cursorStore,
          inbox: adapter.inbox,
          identityId: persistentBotId,
          notifier,
        }).pipe(Effect.catchAllCause(logCatchUpFailure('mentions')))

        const windowSeconds = parsed.catchupWindowSeconds ?? DEFAULT_CATCHUP_WINDOW_SECONDS
        const catchUpIntents = [...type1Intents, ...subscribedIntents]
        if (windowSeconds > 0 && catchUpIntents.length > 0) {
          yield* catchUpChannels({
            intents: catchUpIntents,
            history: adapter.history,
            notifier,
            botIdentityId: persistentBotId,
            windowSeconds,
          }).pipe(Effect.catchAllCause(logCatchUpFailure('channels')))
        }
      }

      const pump = yield* startEventPump({
        inbox: adapter.inbox,
        notifier,
        getBotIdentityId,
        match: (event) => narrowSet.matches(event, getBotIdentityId()),
        // Populate the tools-side identity cache from inbound events so
        // `presence` / `post` mentions / `react` can resolve ids only ever
        // seen via a notification (comms-lox).
        rememberIdentity: toolsCache.rememberIdentity,
        // Advance the per-identity cursor on every observed mention so the
        // next resume's catch-up has an accurate "have-seen-up-to" mark
        // (comms-rxo). Returns the write Effect for the pump to sequence;
        // its failures are swallowed because the advance is best-effort and
        // cursor writes are monotonic.
        onMention: (ts) => {
          const id = getBotIdentityId()
          if (id === undefined) return Effect.void
          return cursorStore.write(id, ts).pipe(Effect.catchAllCause(() => Effect.void))
        },
      })
      // The pump is a daemon (forkDaemon) — not scope-tied — so an explicit
      // finalizer interrupts it on scope unwind (signal interrupt under
      // runMain, or natural stream end). Registered after the release
      // finalizer so LIFO teardown runs pump-cancel first.
      yield* Effect.addFinalizer(() => pump.cancel)

      // Ephemeral idle sweep forked into this scope — interrupted when the
      // scope closes (pump end or signal-driven interrupt).
      if (runsIdleSweep) {
        yield* forkIdleSweep(identityCache, EPHEMERAL_IDLE_SWEEP_INTERVAL_MS)
      }

      // Block until either the event stream ends / the pump fatally parks
      // and is interrupted (the SIGINT/SIGTERM path), OR the MCP client
      // disconnects (comms-8nkv / comms-4c26). The pump is a daemon that
      // long-polls Zulip forever, so without the disconnect race a plain
      // client exit — which sends no signal — would block here and orphan
      // the server child. `raceFirst` interrupts the losing wait and the
      // scope then unwinds the finalizers (pump-cancel → release → close).
      yield* Effect.raceFirst(pump.done, params.shutdownSignal ?? Effect.never)
    }),
  )

/**
 * Production app Layer (comms-spj3.39): the substrate adapter (Zulip,
 * `close()` as a finalizer), the file-backed cursor store, and the
 * stderr logger. Reads `HttpClient` + `FileSystem` from context — the
 * platform layers are fed in once at the edge (comms-5db) so the Zulip
 * adapter and cursor store inject the held services at construction
 * rather than self-providing a layer per call. The ConfigProvider is fed
 * in the same way — built first, so `ZulipAdapterLive`'s own `parseEnv`
 * reads it during the layer build and the program fiber inherits the
 * same source.
 */
const AppLayer: Layer.Layer<
  SubstrateAdapter | CursorStoreTag,
  EnvConfigError,
  HttpClient.HttpClient | FileSystem.FileSystem
> = Layer.mergeAll(ZulipAdapterLive, FileCursorStoreLive, stderrLoggerLayer)

/**
 * Production dependency bundle: the config source (read from
 * `process.env`), the network client, and the file system. These are the
 * leaves `AppLayer` reads — the ConfigProvider during its build (so
 * `ZulipAdapterLive`'s own `parseEnv` reads it), and `HttpClient` +
 * `FileSystem` from context at construction (comms-5db). `fromEnv()` reads
 * `process.env` lazily; since the env never mutates at runtime the
 * lazy read is equivalent to a one-time snapshot. Tests substitute a
 * fixture platform (a `ConfigProvider.fromMap` source over the same
 * `AppLayer`) — provision at the dependency boundary, never a re-built
 * composition.
 */
export const PlatformLive: Layer.Layer<HttpClient.HttpClient | FileSystem.FileSystem> =
  Layer.mergeAll(
    Layer.setConfigProvider(ConfigProvider.fromEnv()),
    FetchHttpClient.layer,
    BunFileSystem.layer,
  )

/**
 * The fully-provided production layer: `AppLayer` fed its leaves by
 * `PlatformLive`. `provideMerge` keeps the platform services in the
 * output so `makeProgram`'s own captured `FileSystem` is satisfied by the
 * same provision that feeds the app layer's builds — one `Effect.provide`
 * at the edge, so the language-service `multipleEffectProvide` rule stays
 * green (comms-5db). `R = never`: the composition root has no unmet
 * requirements.
 */
export const MainLive: Layer.Layer<
  SubstrateAdapter | CursorStoreTag | HttpClient.HttpClient | FileSystem.FileSystem,
  EnvConfigError
> = Layer.provideMerge(AppLayer, PlatformLive)

/** Minimal stdin shape {@link clientDisconnect} listens on. */
interface CloseEmitter {
  once(event: string, listener: () => void): unknown
  removeListener(event: string, listener: () => void): unknown
}

/**
 * Resolves when the MCP client disconnects: its end of the stdio pipe
 * reaches EOF (`end`) or closes (`close`). A plain parent exit delivers no
 * SIGINT/SIGTERM, so this pipe-close is the server's only disconnect
 * signal. Raced against the daemon event pump in {@link main}, completing
 * here unwinds the program scope (pump-cancel → release → close) and the
 * ~166MB bun child exits instead of orphaning under systemd and
 * accumulating across sessions (comms-8nkv / comms-4c26).
 */
export const clientDisconnect = (stdin: CloseEmitter): Effect.Effect<void> =>
  Effect.async<void>((resume) => {
    let settled = false
    const onClose = () => {
      if (settled) return
      settled = true
      stdin.removeListener('end', onClose)
      stdin.removeListener('close', onClose)
      resume(Effect.void)
    }
    stdin.once('end', onClose)
    stdin.once('close', onClose)
    return Effect.sync(() => {
      stdin.removeListener('end', onClose)
      stdin.removeListener('close', onClose)
    })
  })

/**
 * Process entry point. A pure runtime edge: the boot program provided the
 * composition root, run at the single `runMain` call. Shutdown fires on
 * either of two triggers: a signal (SIGINT/SIGTERM) interrupting the
 * program fiber, or the MCP client disconnecting — {@link clientDisconnect}
 * over `process.stdin`, raced against the daemon pump inside
 * {@link makeProgram} so a plain parent exit (no signal) still tears down
 * instead of orphaning the child (comms-8nkv / comms-4c26). Either way the
 * scope finalizers fire (pump-cancel → release → close) and the Exit maps
 * to the process exit code via runMain's default teardown.
 *
 * `disablePrettyLogger` is mandatory: runMain otherwise swaps in the
 * pretty logger, whose `console.group`/`groupEnd` write to STDOUT and
 * corrupt the MCP JSON-RPC channel (the comms-spj3.25 stdio gotcha). The
 * stderr logger layer routes every `Effect.log*` to STDERR regardless.
 */
export const main = (): void =>
  BunRuntime.runMain(
    makeProgram({
      transport: new StdioServerTransport(),
      shutdownSignal: clientDisconnect(process.stdin),
    }).pipe(Effect.provide(MainLive)),
    { disablePrettyLogger: true },
  )

if (import.meta.main) {
  main()
}
