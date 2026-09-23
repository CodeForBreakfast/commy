import { expect } from 'bun:test'
import { effectTest } from '@commy/testing/effect-test'
import { Effect, Ref } from 'effect'
import { readWindow } from './history-paging.ts'
import type { ZulipParams, ZulipParamValue } from './http.ts'

type Row = { readonly id: number; readonly timestamp: number }

const rowsUpTo = (count: number): ReadonlyArray<Row> =>
  Array.from({ length: count }, (_, i) => ({ id: i + 1, timestamp: 1000 + (i + 1) * 100 }))

type FakeRealm = {
  readonly fetchPage: (query: ZulipParams) => Effect.Effect<ReadonlyArray<Row>>
  readonly anchors: Effect.Effect<ReadonlyArray<ZulipParamValue>>
}

/**
 * A realm answering `anchor` + `num_before` the way Zulip does: `newest`
 * takes the newest `num_before` rows, an id takes that row plus `num_before`
 * older ones. Rows come back oldest first, and consecutive pages therefore
 * overlap on the anchor row.
 */
const fakeRealm = (rows: ReadonlyArray<Row>): Effect.Effect<FakeRealm> =>
  Effect.gen(function* () {
    const seen = yield* Ref.make<ReadonlyArray<ZulipParamValue>>([])
    const ascending = [...rows].sort((a, b) => a.id - b.id)
    return {
      anchors: Ref.get(seen),
      fetchPage: (query) =>
        Ref.update(seen, (all) => [...all, query['anchor'] ?? 'newest']).pipe(
          Effect.as(
            (() => {
              const size = Number(query['num_before'])
              if (query['anchor'] === 'newest') return ascending.slice(-size)
              const anchorIndex = ascending.findIndex((r) => r.id === Number(query['anchor']))
              if (anchorIndex === -1) return []
              return ascending.slice(Math.max(0, anchorIndex - size), anchorIndex + 1)
            })(),
          ),
        ),
    }
  })

const walk = (
  realm: FakeRealm,
  options: {
    readonly window?: { readonly since?: number; readonly until?: number }
    readonly pageSize?: number
    readonly cap?: number
    readonly maxPages?: number
    readonly onPage?: (
      query: ZulipParams,
      rows: ReadonlyArray<Row>,
    ) => Effect.Effect<ReadonlyArray<number>>
  } = {},
) =>
  readWindow({
    query: { narrow: '[]' },
    window: options.window ?? {},
    pageSize: options.pageSize ?? 3,
    cap: options.cap,
    maxPages: options.maxPages ?? 10,
    fetchPage: realm.fetchPage,
    onPage: options.onPage ?? ((_query, rows) => Effect.succeed(rows.map((r) => r.id))),
  })

// The defect this module exists to remove. Every message in the window sits
// below a page selected purely by recency, so a post-filter on that page
// returns nothing while looking authoritative.
effectTest('a window below the newest page is reached by paging back to it', () =>
  Effect.gen(function* () {
    const realm = yield* fakeRealm(rowsUpTo(9))
    expect(yield* walk(realm, { window: { until: 1300 }, cap: 3 })).toEqual([1, 2, 3])
  }),
)

// Each page is mapped against the query that fetched it, because the
// rendered-content read re-issues that same query.
effectTest('a page is mapped with the query that fetched it', () =>
  Effect.gen(function* () {
    const realm = yield* fakeRealm(rowsUpTo(9))
    const queries = yield* Ref.make<ReadonlyArray<ZulipParamValue>>([])
    yield* walk(realm, {
      window: { since: 1400 },
      onPage: (query, rows) =>
        Ref.update(queries, (all) => [...all, query['anchor'] ?? 'newest']).pipe(
          Effect.as(rows.map((r) => r.id)),
        ),
    })
    expect(yield* Ref.get(queries)).toEqual(['newest', 7])
  }),
)

// A page holding nothing in the window is never mapped, so it costs no
// rendered-content read.
effectTest('a page entirely outside the window is not mapped', () =>
  Effect.gen(function* () {
    const realm = yield* fakeRealm(rowsUpTo(9))
    const mappedPages = yield* Ref.make(0)
    yield* walk(realm, {
      window: { until: 1300 },
      cap: 3,
      onPage: (_query, rows) =>
        Ref.update(mappedPages, (n) => n + 1).pipe(Effect.as(rows.map((r) => r.id))),
    })
    expect(yield* Ref.get(mappedPages)).toBe(1)
    expect(yield* realm.anchors).toEqual(['newest', 7, 4])
  }),
)

// Zulip's anchor is inclusive, so consecutive pages share their boundary row.
effectTest('a row that appears on two pages is collected once', () =>
  Effect.gen(function* () {
    const realm = yield* fakeRealm(rowsUpTo(9))
    expect(yield* walk(realm, { window: { since: 1100 } })).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9])
  }),
)

// The anchor is a range hint, not an exact match — a range query around a
// deleted id returns its neighbours instead. The next anchor has to come from
// what actually returned, never from arithmetic on what was asked for.
effectTest('the next anchor comes from the rows that returned, not the one requested', () =>
  Effect.gen(function* () {
    const realm = yield* fakeRealm([
      { id: 2, timestamp: 1200 },
      { id: 3, timestamp: 1300 },
      { id: 7, timestamp: 1700 },
      { id: 8, timestamp: 1800 },
      { id: 9, timestamp: 1900 },
    ])
    const ids = yield* walk(realm, { window: { since: 1000 } })
    expect(yield* realm.anchors).toEqual(['newest', 7, 2])
    expect(ids).toEqual([2, 3, 7, 8, 9])
  }),
)

// An anchor naming no live message answers success with an empty list rather
// than an error, so an exhausted walk ends quietly.
effectTest('an empty page ends the walk', () =>
  Effect.gen(function* () {
    const realm = yield* fakeRealm([])
    expect(yield* walk(realm, { window: { since: 1000 } })).toEqual([])
    expect(yield* realm.anchors).toEqual(['newest'])
  }),
)

effectTest('a page that adds nothing new ends the walk', () =>
  Effect.gen(function* () {
    const realm = yield* fakeRealm(rowsUpTo(2))
    yield* walk(realm, { window: { since: 1000 } })
    // The second page re-offers the row the first already had, and there is
    // nothing older behind it.
    expect(yield* realm.anchors).toEqual(['newest', 1])
  }),
)

effectTest('crossing the lower bound ends the walk without reading further', () =>
  Effect.gen(function* () {
    const realm = yield* fakeRealm(rowsUpTo(9))
    const ids = yield* walk(realm, { window: { since: 1600 } })
    expect(yield* realm.anchors).toEqual(['newest', 7])
    expect(ids).toEqual([6, 7, 8, 9])
  }),
)

effectTest('the cap stops the walk and keeps the newest rows in the window', () =>
  Effect.gen(function* () {
    const realm = yield* fakeRealm(rowsUpTo(9))
    const ids = yield* walk(realm, { window: { since: 1000 }, cap: 4 })
    expect(ids).toEqual([6, 7, 8, 9])
    expect(yield* realm.anchors).toEqual(['newest', 7])
  }),
)

effectTest('an unbounded read of one page asks the realm exactly once', () =>
  Effect.gen(function* () {
    const realm = yield* fakeRealm(rowsUpTo(9))
    const ids = yield* walk(realm, { pageSize: 5, cap: 5, maxPages: 1 })
    expect(ids).toEqual([5, 6, 7, 8, 9])
    expect(yield* realm.anchors).toEqual(['newest'])
  }),
)

// A window far enough back to outrun the page budget returns a short read
// rather than walking the realm without limit.
effectTest('the page budget bounds how many requests one read can make', () =>
  Effect.gen(function* () {
    const realm = yield* fakeRealm(rowsUpTo(20))
    yield* walk(realm, { window: { since: 1000 }, maxPages: 2 })
    expect(yield* realm.anchors).toEqual(['newest', 18])
  }),
)
