import { expect, test } from 'bun:test'
import { captureLogger } from '@commy/core/logging'
import { Effect } from 'effect'
import { raceReleaseAgainstTimeout } from './release-shutdown.ts'

// `raceReleaseAgainstTimeout` is the release-on-shutdown finalizer body
// (comms-spj3.39). The surrounding orchestration it used to carry — the
// acquisition gate, cancel-pump-then-release-then-close ordering, exit
// code, and the no-unsubscribe negative requirement — now lives in the
// program scope's finalizer wiring and is covered end-to-end by the
// Type-4 cron-shape integration tests. These tests pin the three release
// outcomes the helper itself owns.

test('release succeeds within budget → no stderr noise', async () => {
  const logs: string[] = []
  await Effect.runPromise(
    raceReleaseAgainstTimeout(Effect.void).pipe(Effect.provide(captureLogger(logs))),
  )
  expect(logs).toEqual([])
})

test('release defects → "release failed" logged, recovers to void', async () => {
  const logs: string[] = []
  await Effect.runPromise(
    raceReleaseAgainstTimeout(Effect.die(new Error('substrate fell over'))).pipe(
      Effect.provide(captureLogger(logs)),
    ),
  )
  expect(logs).toHaveLength(1)
  // Cause.pretty renders the defect with its error type prefix and stack.
  expect(logs[0]).toMatch(/release failed: .*substrate fell over/)
})

test('release hangs past budget → "release timed out" logged', async () => {
  const logs: string[] = []
  await Effect.runPromise(
    raceReleaseAgainstTimeout(Effect.never, 50).pipe(Effect.provide(captureLogger(logs))),
  )
  expect(logs).toHaveLength(1)
  expect(logs[0]).toMatch(/release timed out after 50ms/)
})
