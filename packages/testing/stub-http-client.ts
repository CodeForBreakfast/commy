/**
 * Owned-fake `HttpClient` for the port the adapters declare in `R`
 * (`HttpClient.HttpClient`). Built on `@effect/platform`'s
 * `HttpClient.make(req => Effect<response>)`, it answers requests from an
 * in-memory registry keyed by method + path — no socket, no `Bun.serve`,
 * no real `FetchHttpClient`. This fakes the HTTP PORT we own, not the
 * remote Zulip wire a real-socket fixture stands in for.
 *
 * Responses are constructed as web `Response`s and wrapped with
 * `HttpClientResponse.fromWeb`, so they round-trip through `.status`,
 * `.text`, `.arrayBuffer`, and `.headers` exactly as a fetched response
 * would. Requests are serialized to a web `Request` the same way
 * `FetchHttpClient` serializes them, then captured for assertion — so a
 * form-urlencoded or multipart body is recorded byte-for-byte as the wire
 * would have seen it.
 *
 * Test-only — never imported by production code.
 */

import {
  type HttpBody,
  HttpClient,
  type HttpClientRequest,
  HttpClientResponse,
} from '@effect/platform'
import { Data, Effect, HashMap, Option, Predicate, Ref } from 'effect'

/**
 * A response that never resolves — the request is captured, then the effect
 * parks on `Effect.never`. Models the real Zulip long-poll *holding* the
 * connection open: an in-memory stub answers instantly, so without a hold an
 * eager consumer (the event-pump's `Stream.runDrain`) would burn through the
 * whole response sequence in a hot loop. A fiber blocked on a hang is
 * *interrupted* (not errored) when its scope closes — which is exactly the
 * scope-close-interrupt path the event-pump tests exercise.
 */
export type StubHang = {
  readonly hang: true
}

export type StubBody = {
  /** Object → JSON-encoded; string → verbatim; `Uint8Array` → raw bytes. */
  readonly body: unknown
  /** HTTP status; defaults to 200. */
  readonly status?: number
  /** Extra response headers, merged over the default `content-type`. */
  readonly headers?: Readonly<Record<string, string>>
}

export type StubResponse = StubBody | StubHang

export type CapturedHttpRequest = {
  readonly method: string
  readonly url: URL
  readonly headers: Headers
  readonly body: string
}

export type StubHttpClient = {
  /** Drop-in for the `HttpClient.HttpClient` service. */
  readonly client: HttpClient.HttpClient
  /** Register the sticky response for a (method, path) — used until overridden. */
  readonly respond: (method: string, path: string, response: StubResponse) => Effect.Effect<void>
  /**
   * Queue responses for a (method, path), consumed one per request in order.
   * Once drained, requests fall back to the sticky `respond` default for that
   * key (a 404 error envelope if none was registered). This is the seam the
   * event-pump needs: a `GET /events` chain where each poll returns the next
   * batch.
   */
  readonly respondSequence: (
    method: string,
    path: string,
    responses: ReadonlyArray<StubResponse>,
  ) => Effect.Effect<void>
  /** Every request the stub has answered, in order. */
  readonly captured: Effect.Effect<ReadonlyArray<CapturedHttpRequest>>
}

const routeKey = (method: string, path: string) => Data.struct({ method, path })

type RouteKey = ReturnType<typeof routeKey>

type RouteState = {
  readonly sticky: Option.Option<StubResponse>
  readonly queue: ReadonlyArray<StubResponse>
}

const emptyRoute: RouteState = { sticky: Option.none(), queue: [] }

const requestBodyInit = (body: HttpBody.HttpBody): string | Uint8Array | FormData | undefined => {
  switch (body._tag) {
    case 'Empty':
      return undefined
    case 'Raw':
    case 'Uint8Array':
      return body.body as string | Uint8Array
    case 'FormData':
      return body.formData
    case 'Stream':
      throw new Error('stub HttpClient does not support streaming request bodies')
  }
}

const responseBodyInit = (response: StubBody): string | Uint8Array => {
  if (Predicate.isUint8Array(response.body)) return response.body
  if (Predicate.isString(response.body)) return response.body
  return JSON.stringify(response.body)
}

const responseHeaders = (response: StubBody): Record<string, string> => {
  const base: Record<string, string> = Predicate.isUint8Array(response.body)
    ? {}
    : { 'content-type': 'application/json' }
  return { ...base, ...response.headers }
}

const notFound = (method: string, path: string): StubBody => ({
  body: { result: 'error', code: 'NO_STUB_HANDLER', msg: `no stub handler for ${method} ${path}` },
  status: 404,
})

export const makeStubHttpClient: Effect.Effect<StubHttpClient> = Effect.gen(function* () {
  const routes = yield* Ref.make(HashMap.empty<RouteKey, RouteState>())
  const captured = yield* Ref.make<ReadonlyArray<CapturedHttpRequest>>([])

  const updateRoute = (
    method: string,
    path: string,
    f: (state: RouteState) => RouteState,
  ): Effect.Effect<void> =>
    Ref.update(routes, (map) => {
      const key = routeKey(method, path)
      const current = HashMap.get(map, key).pipe(Option.getOrElse(() => emptyRoute))
      return HashMap.set(map, key, f(current))
    })

  const respond: StubHttpClient['respond'] = (method, path, response) =>
    updateRoute(method, path, (state) => ({ ...state, sticky: Option.some(response) }))

  const respondSequence: StubHttpClient['respondSequence'] = (method, path, responses) =>
    updateRoute(method, path, (state) => ({ ...state, queue: [...responses] }))

  const nextResponse = (method: string, path: string): Effect.Effect<StubResponse> =>
    Ref.modify(routes, (map) => {
      const key = routeKey(method, path)
      const state = HashMap.get(map, key).pipe(Option.getOrElse(() => emptyRoute))
      const [head, ...tail] = state.queue
      if (head !== undefined) {
        return [head, HashMap.set(map, key, { ...state, queue: tail })]
      }
      return [Option.getOrElse(state.sticky, () => notFound(method, path)), map]
    })

  const capture = (request: HttpClientRequest.HttpClientRequest, url: URL): Effect.Effect<void> =>
    Effect.promise(async () => {
      const webRequest = new Request(url.href, {
        method: request.method,
        headers: { ...request.headers },
        body: requestBodyInit(request.body),
      })
      const body = await webRequest.text()
      return {
        method: request.method,
        url,
        headers: webRequest.headers,
        body,
      } satisfies CapturedHttpRequest
    }).pipe(Effect.flatMap((entry) => Ref.update(captured, (all) => [...all, entry])))

  const client = HttpClient.make((request, url) =>
    capture(request, url).pipe(
      Effect.zipRight(nextResponse(request.method, url.pathname)),
      Effect.flatMap((response) =>
        Predicate.hasProperty(response, 'hang')
          ? Effect.never
          : Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                new Response(responseBodyInit(response), {
                  status: response.status ?? 200,
                  headers: responseHeaders(response),
                }),
              ),
            ),
      ),
    ),
  )

  return {
    client,
    respond,
    respondSequence,
    captured: Ref.get(captured),
  }
})
