import { expect } from 'bun:test'
import { effectTest } from '@commy/testing/effect-test'
import { makeStubHttpClient, type StubHttpClient } from '@commy/testing/stub-http-client'
import { HttpClient } from '@effect/platform'
import { Effect, Option } from 'effect'
import { ApiKey, BotEmail, makeZulipHttp, RealmUrl, type ZulipHttp } from './http.ts'
import {
  mayMention,
  renderedContentForBatch,
  renderedContentPerMessage,
} from './rendered-content.ts'

const httpFor = (stub: StubHttpClient): Effect.Effect<ZulipHttp> =>
  Effect.gen(function* () {
    const config = {
      realmUrl: yield* RealmUrl('https://zulip.example.com').pipe(Effect.orDie),
      email: yield* BotEmail('minter@example.com').pipe(Effect.orDie),
      apiKey: yield* ApiKey('minter-key').pipe(Effect.orDie),
    }
    return yield* makeZulipHttp(config).pipe(
      Effect.provideService(HttpClient.HttpClient, stub.client),
    )
  })

const messagesBody = (messages: ReadonlyArray<{ id: number; content: string }>) => ({
  body: { result: 'success', messages },
})

// Zulip answers an anchor naming no live message with result:success and an
// empty list — zerver/views/message_fetch.py builds the response from whatever
// rows the range query returned and never raises for a missing anchor. That
// matters here because the rendered fetch runs inside the event pump's
// never-give-up retry: a hard error on a message deleted between the event
// arriving and the fetch being issued would wedge the pump on a message that
// no longer exists. It resolves to "nobody was mentioned" instead, and the
// message is still delivered.
effectTest('a message that no longer exists renders as no mention rather than an error', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/messages', messagesBody([]))
    const rendered = yield* renderedContentPerMessage(yield* httpFor(stub))(404)
    expect(rendered).toEqual(Option.none())
  }),
)

// Zulip's anchor is a range hint, not an exact match: a range query around a
// missing id happily returns a neighbour. Reading that neighbour's render
// would attribute someone else's mentions to this message.
effectTest('a response about some other message is not read as this one', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond(
      'GET',
      '/api/v1/messages',
      messagesBody([{ id: 999, content: '<p>hi</p>' }]),
    )
    const rendered = yield* renderedContentPerMessage(yield* httpFor(stub))(300)
    expect(rendered).toEqual(Option.none())
  }),
)

effectTest('a single-message fetch asks Zulip to render and returns what it rendered', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond(
      'GET',
      '/api/v1/messages',
      messagesBody([{ id: 300, content: '<p>rendered</p>' }]),
    )
    const rendered = yield* renderedContentPerMessage(yield* httpFor(stub))(300)
    expect(rendered).toEqual(Option.some('<p>rendered</p>'))
    const request = (yield* stub.captured).find((r) => r.url.pathname === '/api/v1/messages')
    expect(request?.url.searchParams.get('apply_markdown')).toBe('true')
  }),
)

// The bound, at the unit the rest of the design cites: one request for the
// whole batch, and none at all when nothing in it carries a sigil.
effectTest('a batch costs one rendered request however many mentions it holds', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond(
      'GET',
      '/api/v1/messages',
      messagesBody([
        { id: 1, content: '<p><span class="user-mention" data-user-id="7">@Graeme</span></p>' },
        { id: 2, content: '<p><span class="user-mention" data-user-id="9">@hermes</span></p>' },
      ]),
    )
    const lookup = yield* renderedContentForBatch(yield* httpFor(stub), { anchor: 'newest' }, [
      { content: '@**Graeme** one' },
      { content: '@**hermes-agent** two' },
    ])
    expect(yield* lookup(1)).toEqual(
      Option.some('<p><span class="user-mention" data-user-id="7">@Graeme</span></p>'),
    )
    expect(yield* lookup(2)).toEqual(
      Option.some('<p><span class="user-mention" data-user-id="9">@hermes</span></p>'),
    )
    expect(yield* stub.captured).toHaveLength(1)
  }),
)

effectTest('a batch nobody was mentioned in costs no request at all', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const lookup = yield* renderedContentForBatch(yield* httpFor(stub), { anchor: 'newest' }, [
      { content: 'nothing to see' },
      { content: 'still nothing' },
    ])
    expect(yield* lookup(1)).toEqual(Option.none())
    expect(yield* stub.captured).toEqual([])
  }),
)

effectTest('the candidate test declines the shapes that provably deliver nothing', () =>
  Effect.sync(() => {
    expect(mayMention('@**Graeme** hi')).toBe(true)
    expect(mayMention('@*backend* hi')).toBe(true)
    expect(mayMention('@**all** hi')).toBe(true)
    expect(mayMention('discuss `@**Graeme**` here')).toBe(false)
    expect(mayMention('as @_**Graeme** noted')).toBe(false)
    expect(mayMention('nobody at all')).toBe(false)
  }),
)
