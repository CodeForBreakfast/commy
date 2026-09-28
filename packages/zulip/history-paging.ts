import { Array as Arr, Effect, HashSet, Order } from 'effect'
import type { ZulipParams } from './http.ts'

/**
 * Reading a time window out of Zulip history.
 *
 * `GET /messages` selects a page by anchor and count and has no timestamp
 * predicate at all, so a caller holding a `since`/`until` window cannot ask
 * for it. Filtering the newest page instead is what produces a silent false
 * zero: when every message on a recency-selected page lies outside the
 * window, the filter empties it and the caller sees an authoritative-looking
 * nothing.
 */

/** The only two fields the walk reads off a message row. */
export type PagedRow = {
  readonly id: number
  readonly timestamp: number
}

/** Inclusive bounds, both optional. */
export type HistoryWindow = {
  readonly since?: number | undefined
  readonly until?: number | undefined
}

const inWindow =
  (window: HistoryWindow) =>
  (row: PagedRow): boolean => {
    if (window.since !== undefined && row.timestamp < window.since) return false
    if (window.until !== undefined && row.timestamp > window.until) return false
    return true
  }

type Walk<Out> = {
  readonly anchor: ZulipParams[string]
  readonly pagesLeft: number
  readonly seen: HashSet.HashSet<number>
  /** One entry per page, newest page first — reversed on the way out. */
  readonly byPage: ReadonlyArray<ReadonlyArray<Out>>
  readonly collected: number
  readonly more: boolean
}

/**
 * Read the messages in a time window, paging backwards to reach it.
 *
 * Results come back in ascending order. The walk is sequential by
 * construction — every anchor is derived from the page before it — so it
 * cannot fan out onto a rate-limited realm.
 *
 * `onPage` turns one page's in-window rows into results, and is handed the
 * query that fetched them: Zulip returns raw content or rendered content and
 * never both, so resolving a page's mentions means re-issuing that page's own
 * query. A walk that rendered once for the whole thing would index a single
 * page and quietly report no mentions on every other.
 *
 * The walk stops on the first of: a page carrying a row older than `since`,
 * so the window's start has been passed; `cap` results collected; a page that
 * adds no row the walk has not already seen, which covers both exhausted
 * history and an anchor naming no live message; or the `maxPages` budget.
 *
 * `cap` truncates from the old end, keeping the newest results.
 */
export const readWindow = <Row extends PagedRow, Out, E, R>(options: {
  /** Query fields shared by every page — the narrow, and nothing anchored. */
  readonly query: ZulipParams
  readonly window: HistoryWindow
  readonly pageSize: number
  /** Stop once this many results are held. Omit for no cap. */
  readonly cap: number | undefined
  /** Hard ceiling on requests, so a far-back window cannot walk the realm. */
  readonly maxPages: number
  readonly fetchPage: (query: ZulipParams) => Effect.Effect<ReadonlyArray<Row>, E, R>
  readonly onPage: (
    query: ZulipParams,
    rows: ReadonlyArray<Row>,
  ) => Effect.Effect<ReadonlyArray<Out>, E, R>
}): Effect.Effect<ReadonlyArray<Out>, E, R> => {
  const keep = inWindow(options.window)

  const step = (state: Walk<Out>): Effect.Effect<Walk<Out>, E, R> => {
    const query: ZulipParams = {
      ...options.query,
      anchor: state.anchor,
      num_before: options.pageSize,
      num_after: 0,
    }
    return options.fetchPage(query).pipe(
      Effect.flatMap((rows): Effect.Effect<Walk<Out>, E, R> => {
        const exhausted: Walk<Out> = { ...state, more: false }
        if (!Arr.isNonEmptyReadonlyArray(rows)) return Effect.succeed(exhausted)
        const fresh = rows.filter((r) => !HashSet.has(state.seen, r.id))
        if (!Arr.isNonEmptyReadonlyArray(fresh)) return Effect.succeed(exhausted)
        const kept = fresh.filter(keep)
        const oldestId = Arr.min(Order.number)(Arr.map(rows, (r) => r.id))
        const oldestTs = Arr.min(Order.number)(Arr.map(rows, (r) => r.timestamp))
        const crossedLowerBound =
          options.window.since !== undefined && oldestTs < options.window.since
        // A page holding nothing in the window is not mapped, and for the
        // rendered-content read that means no request.
        const mapped = Arr.isEmptyReadonlyArray(kept)
          ? Effect.succeed<ReadonlyArray<Out>>([])
          : options.onPage(query, kept)
        return mapped.pipe(
          Effect.map((out) => {
            const collected = state.collected + out.length
            const capMet = options.cap !== undefined && collected >= options.cap
            return {
              anchor: oldestId,
              pagesLeft: state.pagesLeft - 1,
              seen: rows.reduce((s, r) => HashSet.add(s, r.id), state.seen),
              byPage: [...state.byPage, out],
              collected,
              more: state.pagesLeft > 1 && !crossedLowerBound && !capMet,
            }
          }),
        )
      }),
    )
  }

  const start: Walk<Out> = {
    anchor: 'newest',
    pagesLeft: options.maxPages,
    seen: HashSet.empty<number>(),
    byPage: [],
    collected: 0,
    more: options.maxPages > 0,
  }

  return Effect.iterate(start, { while: (state) => state.more, body: step }).pipe(
    Effect.map((state) => {
      const all = Arr.flatten(Arr.reverse(state.byPage))
      return options.cap === undefined ? all : Arr.takeRight(all, options.cap)
    }),
  )
}
