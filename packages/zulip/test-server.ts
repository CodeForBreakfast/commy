/**
 * Realm-shaped test fixture. Stands up a Bun.serve() on a free port,
 * dispatches by `${method} ${pathname}` to registered handlers, and
 * captures every request for after-the-fact assertions.
 *
 * Test-only — never imported by production code.
 */

export type CapturedRequest = {
  readonly method: string
  readonly url: URL
  readonly headers: Headers
  readonly body: string
}

export type RealmResponseInit = {
  readonly status?: number
  readonly headers?: Readonly<Record<string, string>>
}

export type RealmResponse = {
  readonly body: unknown
  readonly init?: RealmResponseInit
}

export type RealmHandler = (req: CapturedRequest) => RealmResponse | Promise<RealmResponse>

export type TestRealm = {
  readonly url: string
  readonly port: number
  readonly captured: ReadonlyArray<CapturedRequest>
  /**
   * Register a handler for the given (method, pathname). `pathname` is
   * matched literally; for variable path segments (e.g. message ids in
   * reaction URLs) use `handlePattern`.
   */
  readonly handle: (method: string, pathname: string, handler: RealmHandler) => void
  /**
   * Register a regex-matched handler. Falls back to here if no exact
   * `(method, pathname)` handler exists. Multiple patterns are tried in
   * registration order.
   */
  readonly handlePattern: (method: string, pattern: RegExp, handler: RealmHandler) => void
  readonly stop: () => Promise<void>
}

interface PatternHandler {
  readonly method: string
  readonly pattern: RegExp
  readonly handler: RealmHandler
}

export const startTestRealm = (): TestRealm => {
  const captured: CapturedRequest[] = []
  const handlers = new Map<string, RealmHandler>()
  const patternHandlers: PatternHandler[] = []

  const matchPatternHandler = (method: string, pathname: string): RealmHandler | undefined =>
    patternHandlers.find((p) => p.method === method && p.pattern.test(pathname))?.handler

  const server = Bun.serve({
    port: 0,
    fetch: async (req) => {
      const url = new URL(req.url)
      const recorded: CapturedRequest = {
        method: req.method,
        url,
        headers: new Headers(req.headers),
        body: await req.text(),
      }
      captured.push(recorded)
      const key = `${req.method} ${url.pathname}`
      const handler = handlers.get(key) ?? matchPatternHandler(req.method, url.pathname)
      if (handler === undefined) {
        const body = JSON.stringify({
          result: 'error',
          msg: `no test handler for ${key}`,
          code: 'NO_TEST_HANDLER',
        })
        return new Response(body, {
          status: 404,
          headers: { 'content-type': 'application/json' },
        })
      }
      const out = await handler(recorded)
      const headers: Record<string, string> = { 'content-type': 'application/json' }
      if (out.init?.headers !== undefined) {
        for (const [k, v] of Object.entries(out.init.headers)) {
          headers[k] = v
        }
      }
      return new Response(JSON.stringify(out.body), {
        status: out.init?.status ?? 200,
        headers,
      })
    },
  })

  if (typeof server.port !== 'number') {
    throw new Error('test realm failed to bind a TCP port')
  }

  return {
    url: `http://localhost:${server.port}`,
    port: server.port,
    captured,
    handle: (method, pathname, handler) => {
      handlers.set(`${method} ${pathname}`, handler)
    },
    handlePattern: (method, pattern, handler) => {
      patternHandlers.push({ method, pattern, handler })
    },
    stop: async () => {
      await server.stop(true)
    },
  }
}
