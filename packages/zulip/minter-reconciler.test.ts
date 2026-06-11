import { expect, test } from 'bun:test'
import { type ChannelName, decodeChannelNameSync } from '@codeforbreakfast/core/ports'
import { Data, Effect } from 'effect'
import type { ReconcilerDeps } from './minter-reconciler.ts'
import { reconcileMinterSubscriptions } from './minter-reconciler.ts'

// Production deps fail with tagged errors (ZulipApiError / ParseError); this
// stand-in mirrors that shape so the failure-path tests exercise the
// reconciler's `instanceof Error ? .message` rendering branch with a
// representative error rather than a bare global Error.
class DepFailure extends Data.TaggedError('DepFailure')<{ readonly message: string }> {}

const buildDeps = <E = never>(
  overrides: Partial<ReconcilerDeps<E>> = {},
): ReconcilerDeps<E> & {
  readonly listCalls: { value: number }
  readonly subscribeCalls: { value: ReadonlyArray<ChannelName>[] }
} => {
  const listCalls = { value: 0 }
  const subscribeCalls: { value: ReadonlyArray<ChannelName>[] } = { value: [] }
  const baseDeps: ReconcilerDeps<E> = {
    listUnsubscribedPublicStreams: () =>
      Effect.sync(() => {
        listCalls.value += 1
        return []
      }),
    subscribeToStreams: (names) =>
      Effect.sync(() => {
        subscribeCalls.value = [...subscribeCalls.value, names]
        return names
      }),
  }
  return { ...baseDeps, ...overrides, listCalls, subscribeCalls }
}

test('returns empty report when minter is already up to date', async () => {
  const deps = buildDeps({
    listUnsubscribedPublicStreams: () => Effect.succeed([]),
  })

  const report = await Effect.runPromise(reconcileMinterSubscriptions(deps))

  expect(report).toEqual({ added: [], error: undefined })
  expect(deps.subscribeCalls.value).toEqual([])
})

test('batches a single subscribe call for every unsubscribed stream', async () => {
  const streams = [
    { name: decodeChannelNameSync('commy') },
    { name: decodeChannelNameSync('assistant') },
    { name: decodeChannelNameSync('homelab') },
  ]
  const deps = buildDeps({
    listUnsubscribedPublicStreams: () => Effect.succeed(streams),
    subscribeToStreams: (names) => Effect.succeed(names),
  })

  const report = await Effect.runPromise(reconcileMinterSubscriptions(deps))

  expect(report).toEqual({
    added: [
      decodeChannelNameSync('commy'),
      decodeChannelNameSync('assistant'),
      decodeChannelNameSync('homelab'),
    ],
    error: undefined,
  })
})

test('reports only the streams the substrate confirms as newly subscribed', async () => {
  // Race: another process already subscribed `homelab` in the window
  // between list and subscribe. The substrate response excludes it
  // from `subscribed`. The reconciler reports only the actual adds.
  const deps = buildDeps({
    listUnsubscribedPublicStreams: () =>
      Effect.succeed([
        { name: decodeChannelNameSync('commy') },
        { name: decodeChannelNameSync('homelab') },
      ]),
    subscribeToStreams: () => Effect.succeed([decodeChannelNameSync('commy')]),
  })

  const report = await Effect.runPromise(reconcileMinterSubscriptions(deps))

  expect(report.added).toEqual([decodeChannelNameSync('commy')])
  expect(report.error).toBeUndefined()
})

test('captures list failure and never invokes subscribe', async () => {
  const deps = buildDeps<DepFailure>({
    listUnsubscribedPublicStreams: () =>
      Effect.fail(new DepFailure({ message: 'realm unreachable' })),
  })

  const report = await Effect.runPromise(reconcileMinterSubscriptions(deps))

  expect(report).toEqual({ added: [], error: 'realm unreachable' })
  expect(deps.subscribeCalls.value).toEqual([])
})

test('captures subscribe failure after a successful list', async () => {
  const deps = buildDeps<DepFailure>({
    listUnsubscribedPublicStreams: () => Effect.succeed([{ name: decodeChannelNameSync('commy') }]),
    subscribeToStreams: () => Effect.fail(new DepFailure({ message: 'subscribe rejected' })),
  })

  const report = await Effect.runPromise(reconcileMinterSubscriptions(deps))

  expect(report).toEqual({ added: [], error: 'subscribe rejected' })
})

test('coerces non-Error throws to a string error message', async () => {
  const deps = buildDeps<string>({
    listUnsubscribedPublicStreams: () => Effect.fail('not-an-error-instance'),
  })

  const report = await Effect.runPromise(reconcileMinterSubscriptions(deps))

  expect(report.error).toBe('not-an-error-instance')
})
