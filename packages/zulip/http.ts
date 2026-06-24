import { messageOf } from '@commy/core/messageOf'
import { HttpBody, HttpClient, HttpClientRequest } from '@effect/platform'
import {
  Data,
  Duration,
  Effect,
  Either,
  Encoding,
  Option,
  type ParseResult,
  Schedule,
  ScheduleDecision,
  ScheduleInterval,
  Schema,
} from 'effect'

const decodeUrl = Schema.decodeEither(Schema.URL)

/**
 * Validate as an http(s) URL while keeping the value a verbatim string:
 * `requiredBrand` (bootstrap) and the `ZulipHttp` constructor both consume
 * a `string`, and round-tripping through `Schema.URL`'s `URL` output would
 * canonicalise the realm URL (e.g. append a trailing slash). `Schema.URL`
 * does the parse — no hand-rolled `new URL` try/catch — and the protocol
 * check rides on its decoded `URL`.
 */
export const realmUrlSchema = Schema.String.pipe(
  Schema.filter(
    (value) =>
      decodeUrl(value).pipe(
        Either.match({
          onLeft: () => false,
          onRight: (url) => url.protocol === 'http:' || url.protocol === 'https:',
        }),
      ),
    {
      message: () => 'realmUrl must be a non-empty http(s) URL',
    },
  ),
  Schema.brand('RealmUrl'),
)

export const botEmailSchema = Schema.String.pipe(
  Schema.minLength(3),
  Schema.pattern(/^[^\s@]+@[^\s@]+$/, {
    message: () => 'botEmail must look like an email address',
  }),
  Schema.brand('BotEmail'),
)

export const apiKeySchema = Schema.NonEmptyString.pipe(Schema.brand('ApiKey'))

export type RealmUrl = typeof realmUrlSchema.Type
export type BotEmail = typeof botEmailSchema.Type
export type ApiKey = typeof apiKeySchema.Type

export const RealmUrl = (raw: string): Effect.Effect<RealmUrl, ParseResult.ParseError> =>
  Schema.decodeUnknown(realmUrlSchema)(raw)

export const BotEmail = (raw: string): Effect.Effect<BotEmail, ParseResult.ParseError> =>
  Schema.decodeUnknown(botEmailSchema)(raw)

export const ApiKey = (raw: string): Effect.Effect<ApiKey, ParseResult.ParseError> =>
  Schema.decodeUnknown(apiKeySchema)(raw)

export type ZulipHttpConfig = {
  readonly realmUrl: RealmUrl
  readonly email: BotEmail
  readonly apiKey: ApiKey
  /**
   * Override the Host header on outgoing requests. Required when the
   * realmUrl is the cluster-internal service URL but the chart's nginx
   * is configured for the public vhost — otherwise nginx returns 400
   * Bad Request. Workstation callers reaching the realm by its public
   * URL get the right Host for free and should leave this unset.
   */
  readonly hostHeader?: string
}

/**
 * Tagged so Effect callers can discriminate it with `Effect.catchTag('ZulipApiError', ...)`.
 * `retryAfter` is the seconds the realm asked us to wait before retrying —
 * surfaced on rate-limit responses (status 429, code RATE_LIMIT_HIT) where
 * Zulip includes a top-level `retry-after` float in the error envelope;
 * undefined for non-rate-limit errors.
 * `cause` carries the underlying transport failure (`@effect/platform`'s
 * `RequestError` / `ResponseError`) when the error originates below the
 * Zulip envelope layer, so the DNS/socket context Effect's runtime
 * preserves isn't dropped at the seam; undefined for envelope-derived errors.
 */
export class ZulipApiError extends Data.TaggedError('ZulipApiError')<{
  readonly message: string
  readonly status: number
  readonly code: string | undefined
  readonly retryAfter: number | undefined
  readonly cause?: unknown
}> {}

const envelopeSchema = Schema.Struct({
  result: Schema.optional(Schema.Literal('success', 'error')),
  msg: Schema.optional(Schema.String),
  code: Schema.optional(Schema.String),
  'retry-after': Schema.optional(Schema.NonNegative),
})

type Envelope = Schema.Schema.Type<typeof envelopeSchema>

/**
 * Preserve extra properties because the response-body schema runs against
 * the decoded envelope downstream: a `/user_uploads` success body carries
 * `url` and `filename` alongside the envelope keys, and stripping them
 * here would leave the upload-response schema with nothing to parse.
 */
const decodeEnvelope = Schema.decodeUnknownEither(Schema.parseJson(envelopeSchema), {
  onExcessProperty: 'preserve',
})

export type ZulipParamValue = string | number | boolean
export type ZulipParams = Readonly<Record<string, ZulipParamValue>>

export const UserUploadPathSchema = Schema.NonEmptyString.pipe(
  Schema.pattern(/^\/user_uploads\//),
  Schema.brand('UserUploadPath'),
)

/**
 * Realm-relative `/user_uploads/...` path to an uploaded file. The brand
 * carries the invariant "this path was checked to be a user-upload path";
 * a bare string from any source cannot stand in for one. Minted at the two
 * boundaries that produce or accept such a path — `uploadRaw`'s response
 * parse and the `download_file` tool's input — so passing an arbitrary
 * string where an upload path is expected is unrepresentable. Lives in the
 * Zulip adapter (not core/ports) because `/user_uploads/` is a Zulip URL
 * convention, not a port-level concept.
 */
export type UserUploadPath = typeof UserUploadPathSchema.Type

export const decodeUserUploadPath = (
  raw: unknown,
): Effect.Effect<UserUploadPath, ParseResult.ParseError> =>
  Schema.decodeUnknown(UserUploadPathSchema)(raw)

/**
 * Synchronous decoder for TEST FIXTURES (comms-spj3.35) — sibling of
 * `decodeUserUploadPath`. A malformed literal in test setup is a
 * programmer error, so `decodeSync`'s throw is the legitimate fatal
 * case. PRODUCTION code must use `decodeUserUploadPath`, never this.
 */
export const decodeUserUploadPathSync = (raw: string): UserUploadPath =>
  Schema.decodeSync(UserUploadPathSchema)(raw)

export interface RawDownload {
  readonly data: Uint8Array
  readonly contentType: string
}

export interface UploadResult {
  /** Realm-relative `/user_uploads/...` path the file is reachable at. */
  readonly url: UserUploadPath
  /** Filename the realm stored the upload under (may be sanitised). */
  readonly filename: string
}

const uploadResponseSchema = Schema.Struct({
  url: Schema.String,
  filename: Schema.String,
})

const decodeUploadResponse = Schema.decodeUnknown(Schema.parseJson(uploadResponseSchema))

/**
 * Rate-limit (429) retry policy for one-shot calls. A 429 from Zulip is
 * backpressure, not a fault — it ships a `retry-after`. The send path
 * waits it out and retries so transient limits never surface to callers;
 * only a realm that stays rate-limited past the budget throws.
 *
 * The bound is a total wait budget, not an attempt count: under shared-
 * minter contention `retry-after` is sub-second, so a fixed count gives
 * up in a fraction of a second while the contention window lasts seconds.
 * Budgeting the cumulative wait instead means a small `retry-after` earns
 * many cheap attempts, a large one clamps to what's left, and the worst-
 * case caller-visible hang stays bounded either way (comms-nbz).
 *
 * `retry-after` is consumed here and never crosses the port. Mirrors the
 * long-poll iterator's 429 recovery in events.ts (comms-9wi).
 */
const RATE_LIMIT_RETRY_BUDGET_MS = 15_000
const DEFAULT_RATE_LIMIT_WAIT_MS = 1_000
const MIN_RATE_LIMIT_WAIT_MS = 100

interface RateLimitState {
  readonly waitedMs: number
}

/**
 * Custom Schedule consumed by `Effect.retry` to implement the cumulative-
 * wait-budget 429 recovery. The schedule reads each `ZulipApiError` that
 * comes back from the send attempt, decides whether to retry (429 with
 * budget remaining) and what delay to wait (clamped to remaining budget),
 * and emits the chosen delay as the next interval through
 * `ScheduleDecision.continueWith`.
 *
 * The wait is left to `Effect.retry`'s Schedule driver, which sleeps until
 * the emitted interval via the `Clock` default service — production waits
 * real time, while tests drive it with `TestClock.adjust`. Because the
 * delay rides on the interval, `Schedule.delays` can also replay the wait
 * sequence purely (no real timers) for policy assertions.
 */
export const rateLimitSchedule = (): Schedule.Schedule<Duration.Duration, ZulipApiError> =>
  Schedule.makeWithState<RateLimitState, ZulipApiError, Duration.Duration>(
    { waitedMs: 0 },
    (now, err, state) => {
      const remaining = RATE_LIMIT_RETRY_BUDGET_MS - state.waitedMs
      if (err.status !== 429 || remaining <= 0) {
        return Effect.succeed([state, Duration.zero, ScheduleDecision.done] as const)
      }
      const requested =
        err.retryAfter === undefined
          ? DEFAULT_RATE_LIMIT_WAIT_MS
          : Math.max(err.retryAfter * 1_000, MIN_RATE_LIMIT_WAIT_MS)
      const waitMs = Math.min(requested, remaining)
      const nextState: RateLimitState = { waitedMs: state.waitedMs + waitMs }
      return Effect.succeed([
        nextState,
        Duration.millis(waitMs),
        ScheduleDecision.continueWith(ScheduleInterval.after(now + waitMs)),
      ] as const)
    },
  )

const parseEnvelope = (text: string): Envelope | undefined => {
  if (text.length === 0) return undefined
  return Either.getOrUndefined(decodeEnvelope(text))
}

const classifyEnvelope = (
  text: string,
  status: number,
  url: string,
): Either.Either<string, ZulipApiError> => {
  const env = parseEnvelope(text)
  if (env !== undefined && env.result === 'error') {
    return Either.left(
      new ZulipApiError({
        message: env.msg ?? `HTTP ${status}`,
        status,
        code: env.code,
        retryAfter: env['retry-after'],
      }),
    )
  }
  if (status < 200 || status >= 300) {
    const message = env?.msg ?? (text.length > 0 ? text : `HTTP ${status}`)
    return Either.left(
      new ZulipApiError({
        message,
        status,
        code: env?.code,
        retryAfter: env?.['retry-after'],
      }),
    )
  }
  if (env === undefined) {
    return Either.left(
      new ZulipApiError({
        message: `non-JSON response from ${url}`,
        status,
        code: undefined,
        retryAfter: undefined,
      }),
    )
  }
  return Either.right(text)
}

/**
 * Wrap any non-ZulipApiError transport failure (Effect-platform's
 * `RequestError` / `ResponseError`) in a ZulipApiError so the rest of
 * the pipeline only sees a single typed failure mode.
 */
const transportError = (url: string, cause: unknown): ZulipApiError =>
  new ZulipApiError({
    message: messageOf(cause, `transport failure for ${url}`),
    status: 0,
    code: undefined,
    retryAfter: undefined,
    cause,
  })

/**
 * The Zulip HTTP request surface. A closure-based record of Effect-returning
 * verbs (comms-0m8) — not a class. Construction goes through
 * {@link makeZulipHttp}, which reads `HttpClient` from context, so the
 * dependency lands in the requirements channel rather than as a held field.
 */
export interface ZulipHttp {
  get<A, I>(
    path: string,
    schema: Schema.Schema<A, I>,
    params?: ZulipParams,
  ): Effect.Effect<A, ZulipApiError | ParseResult.ParseError>
  post<A, I>(
    path: string,
    schema: Schema.Schema<A, I>,
    body: ZulipParams,
  ): Effect.Effect<A, ZulipApiError | ParseResult.ParseError>
  patch<A, I>(
    path: string,
    schema: Schema.Schema<A, I>,
    body: ZulipParams,
  ): Effect.Effect<A, ZulipApiError | ParseResult.ParseError>
  delete<A, I>(
    path: string,
    schema: Schema.Schema<A, I>,
    body?: ZulipParams,
  ): Effect.Effect<A, ZulipApiError | ParseResult.ParseError>
  downloadRaw(realmPath: string): Effect.Effect<RawDownload, ZulipApiError>
  uploadRaw(
    filename: string,
    data: Uint8Array,
  ): Effect.Effect<UploadResult, ZulipApiError | ParseResult.ParseError>
}

/**
 * Construct a `ZulipHttp` for one credential set. `HttpClient` is read from
 * context (`yield* HttpClient.HttpClient`) so it lands in the requirements
 * channel and is provided once at the application edge — no per-call platform
 * layer, no injected client field (comms-0m8). The realm base URL and Basic-
 * auth header are derived once and captured in the returned closure.
 */
export const makeZulipHttp = (
  config: ZulipHttpConfig,
): Effect.Effect<ZulipHttp, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const realmRoot = config.realmUrl.replace(/\/+$/, '')
    const base = `${realmRoot}/api/v1`
    const authHeader = `Basic ${Encoding.encodeBase64(`${config.email}:${config.apiKey}`)}`
    const hostHeader = config.hostHeader

    const httpClient = (yield* HttpClient.HttpClient).pipe(
      HttpClient.mapRequest(
        HttpClientRequest.setHeaders({
          authorization: authHeader,
          ...(hostHeader === undefined ? {} : { host: hostHeader }),
        }),
      ),
    )

    // A path that doesn't start with '/' is a programmer error (every call
    // site passes an internal literal). Surface it as a defect in the Effect
    // channel rather than a synchronous throw from an Effect-returning verb.
    const resolvePath = (path: string): Effect.Effect<string> =>
      path.startsWith('/')
        ? Effect.succeed(base + path)
        : Effect.die(
            new TypeError(`ZulipHttp path must start with '/' — received: ${JSON.stringify(path)}`),
          )

    // Single request attempt — read the response body as text, then decide
    // whether it's a ZulipApiError.
    const attempt = (
      url: string,
      request: HttpClientRequest.HttpClientRequest,
    ): Effect.Effect<string, ZulipApiError> =>
      httpClient.execute(request).pipe(
        Effect.flatMap((response) =>
          response.text.pipe(
            Effect.mapError((cause) => transportError(url, cause)),
            Effect.flatMap((text) => classifyEnvelope(text, response.status, url)),
          ),
        ),
        Effect.catchTag('RequestError', (cause) => Effect.fail(transportError(url, cause))),
        Effect.catchTag('ResponseError', (cause) => Effect.fail(transportError(url, cause))),
      )

    const sendWithRetry = (
      url: string,
      request: HttpClientRequest.HttpClientRequest,
    ): Effect.Effect<string, ZulipApiError> =>
      attempt(url, request).pipe(Effect.retry({ schedule: rateLimitSchedule() }))

    const sendJson = <A, I>(
      url: string,
      request: HttpClientRequest.HttpClientRequest,
      schema: Schema.Schema<A, I>,
    ): Effect.Effect<A, ZulipApiError | ParseResult.ParseError> => {
      const decode = Schema.decodeUnknown(Schema.parseJson(schema))
      return sendWithRetry(url, request).pipe(Effect.flatMap(decode))
    }

    return {
      get: (path, schema, params) =>
        resolvePath(path).pipe(
          Effect.flatMap((url) => {
            const request =
              params === undefined
                ? HttpClientRequest.get(url)
                : HttpClientRequest.get(url).pipe(HttpClientRequest.setUrlParams(params))
            const diagnosticUrl = HttpClientRequest.toUrl(request).pipe(
              Option.map((u) => u.toString()),
              Option.getOrElse(() => url),
            )
            return sendJson(diagnosticUrl, request, schema)
          }),
        ),
      post: (path, schema, body) =>
        resolvePath(path).pipe(
          Effect.flatMap((url) =>
            sendJson(
              url,
              HttpClientRequest.post(url).pipe(HttpClientRequest.bodyUrlParams(body)),
              schema,
            ),
          ),
        ),
      patch: (path, schema, body) =>
        resolvePath(path).pipe(
          Effect.flatMap((url) =>
            sendJson(
              url,
              HttpClientRequest.patch(url).pipe(HttpClientRequest.bodyUrlParams(body)),
              schema,
            ),
          ),
        ),
      delete: (path, schema, body) =>
        resolvePath(path).pipe(
          Effect.flatMap((url) =>
            sendJson(
              url,
              body === undefined
                ? HttpClientRequest.del(url)
                : HttpClientRequest.del(url).pipe(HttpClientRequest.bodyUrlParams(body)),
              schema,
            ),
          ),
        ),
      downloadRaw: (realmPath) =>
        realmPath.startsWith('/')
          ? ((): Effect.Effect<RawDownload, ZulipApiError> => {
              const url = realmRoot + realmPath
              const request = HttpClientRequest.get(url)
              return httpClient.execute(request).pipe(
                Effect.flatMap((response) => {
                  if (response.status < 200 || response.status >= 300) {
                    return Effect.fail(
                      new ZulipApiError({
                        message: `download ${realmPath}: HTTP ${response.status}`,
                        status: response.status,
                        code: undefined,
                        retryAfter: undefined,
                      }),
                    )
                  }
                  return response.arrayBuffer.pipe(
                    Effect.map(
                      (buf): RawDownload => ({
                        data: new Uint8Array(buf),
                        contentType: response.headers['content-type'] ?? 'application/octet-stream',
                      }),
                    ),
                    Effect.mapError((cause) => transportError(url, cause)),
                  )
                }),
                Effect.catchTag('RequestError', (cause) => Effect.fail(transportError(url, cause))),
                Effect.catchTag('ResponseError', (cause) =>
                  Effect.fail(transportError(url, cause)),
                ),
              )
            })()
          : Effect.die(
              new TypeError(
                `ZulipHttp realmPath must start with '/' — received: ${JSON.stringify(realmPath)}`,
              ),
            ),
      uploadRaw: (filename, data) =>
        resolvePath('/user_uploads').pipe(
          Effect.flatMap((url) => {
            const form = new FormData()
            form.append('file', new Blob([data]), filename)
            const request = HttpClientRequest.post(url, { body: HttpBody.formData(form) })
            const decode = (
              envText: string,
            ): Effect.Effect<UploadResult, ZulipApiError | ParseResult.ParseError> =>
              decodeUploadResponse(envText).pipe(
                Effect.flatMap((parsed) =>
                  decodeUserUploadPath(parsed.url).pipe(
                    Effect.map(
                      (uploadUrl): UploadResult => ({ url: uploadUrl, filename: parsed.filename }),
                    ),
                  ),
                ),
              )
            return sendWithRetry(url, request).pipe(Effect.flatMap(decode))
          }),
        ),
    }
  })
