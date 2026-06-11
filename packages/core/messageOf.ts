import { Predicate } from 'effect'

/**
 * Render an unknown error cause as a human-readable string. An `Error`
 * surfaces its `.message`; anything else falls back to `String(cause)`
 * unless an explicit `fallback` is supplied.
 */
export const messageOf = (cause: unknown, fallback?: string): string =>
  Predicate.isError(cause) ? cause.message : (fallback ?? String(cause))
