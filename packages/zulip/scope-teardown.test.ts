/**
 * Tier-3 residue: the **one** assertion in the suite that
 * genuinely cannot leave the socket — proving that interrupting an in-flight
 * `FetchHttpClient` long-poll on scope close actually tears down the underlying
 * TCP connection (`AbortSignal → fetch → socket`).
 *
 * Everything else the event pump does — gap-replay, 429 retry,
 * BAD_EVENT_QUEUE_ID reconnect, the Effect fiber-interrupt LOGIC of scope close
 * — moved onto the owned-fake stub HttpClient + TestClock in `adapter-events.test.ts`:
 * deterministic, no socket. The stub proves the fiber unwinds.
 * It cannot prove the *socket* unwinds, because there is no socket. That gap is
 * this test, and it is the **only surviving `Bun.serve` long-poll** in the suite.
 *
 * The honest assertion is **server-side**: a real `Bun.serve` whose long-poll
 * handler parks forever but listens on the request's `req.signal`. When the
 * client tears the connection down, Bun fires that abort. Observing it is proof
 * the TCP teardown reached the peer — not merely that the Effect fiber
 * unwound on our side. The Effect platform client wires this end to end:
 * `httpClient.make` aborts the request's `AbortController` `onInterrupt`, and
 * `FetchHttpClient` passes that `signal` into `fetch`.
 *
 * Lifecycle is `Effect.acquireRelease`/`Scope` throughout: the server is
 * acquired with `server.stop(true)` as its release, and the long-poll is forked
 * into an inner `Effect.scoped` whose close is the interruption under test — so
 * release is guaranteed even on the interruption path.
 */

import { effectTest } from '@commy/testing/effect-test'
import { FetchHttpClient, HttpClient } from '@effect/platform'
import { Data, Duration, Effect } from 'effect'

class ConnectionNotTornDown extends Data.TaggedError('ConnectionNotTornDown')<{
  readonly message: string
}> {}

interface LongPollServer {
  readonly url: string
  /** Resolves once the server has the long-poll request in flight. */
  readonly pollInFlight: Promise<void>
  /** Resolves when the client tears the in-flight connection down (`req.signal`). */
  readonly clientDisconnected: Promise<void>
  readonly stop: () => Promise<void>
}

// A real Bun.serve whose /events handler parks forever and reports, via
// req.signal, when the client disconnects mid-poll — the server-side proof of
// TCP teardown.
const startLongPollServer = (): LongPollServer => {
  const inFlight = Promise.withResolvers<void>()
  const disconnected = Promise.withResolvers<void>()
  const server = Bun.serve({
    port: 0,
    fetch: (req) => {
      inFlight.resolve()
      return new Promise<Response>((resolve) => {
        req.signal.addEventListener('abort', () => {
          disconnected.resolve()
          resolve(new Response(null, { status: 499 }))
        })
      })
    },
  })
  if (typeof server.port !== 'number') {
    throw new Error('long-poll server failed to bind a TCP port')
  }
  return {
    url: `http://localhost:${server.port}/events`,
    pollInFlight: inFlight.promise,
    clientDisconnected: disconnected.promise,
    stop: () => server.stop(true),
  }
}

effectTest(
  'closing the scope of an in-flight FetchHttpClient long-poll tears down the TCP connection',
  () =>
    Effect.gen(function* () {
      const server = yield* Effect.acquireRelease(Effect.sync(startLongPollServer), (s) =>
        Effect.promise(() => s.stop()),
      )
      const client = yield* HttpClient.HttpClient

      // Fork the real long-poll into an inner scope, hold until the server has
      // it in flight on the socket, then close the scope — the interruption is
      // what must propagate to the socket.
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* Effect.forkScoped(client.get(server.url))
          yield* Effect.promise(() => server.pollInFlight)
        }),
      )

      // The scope is closed; the forked fiber is interrupted. Proof of teardown
      // is the server observing the client's disconnect. A FetchHttpClient that
      // failed to abort the socket would leave this parked, and the timeout
      // fails the test loudly.
      yield* Effect.promise(() => server.clientDisconnected).pipe(
        Effect.timeoutFail({
          duration: Duration.seconds(3),
          onTimeout: () =>
            new ConnectionNotTornDown({
              message: 'scope close did not tear down the in-flight long-poll connection',
            }),
        }),
      )
    }),
  { layer: FetchHttpClient.layer, timeout: 10_000 },
)
