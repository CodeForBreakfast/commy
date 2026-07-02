import { expect, test } from 'bun:test'
import { decodeIdentityIdSync } from '@commy/core/ports'
import { senderNarrow, userPresencePath, ZulipUserRef } from './user-ref.ts'

test('ZulipUserRef is a structural pass-through of the integer user id', () => {
  expect(Number(ZulipUserRef(473))).toBe(473)
})

test('senderNarrow builds a sender filter whose operand is the integer ref', () => {
  const narrow = senderNarrow(ZulipUserRef(473))
  expect(narrow.operator).toBe('sender')
  expect(Number(narrow.operand)).toBe(473)
  expect(typeof narrow.operand).toBe('number')
})

test('userPresencePath embeds the integer ref in the path', () => {
  expect(userPresencePath(ZulipUserRef(473))).toBe('/users/473/presence')
})

// ─── Type proofs ────────────────────────────────────────────────
// A user-scoped Zulip call must reject anything that isn't a ZulipUserRef
// minted from a real integer user_id. These @ts-expect-error directives fire
// at compile time (tsc, via `bun run check`); if a brand were weakened the
// directive would go unused and tsc would fail.

test('a bare string cannot reach a sender narrow', () => {
  // @ts-expect-error — Zulip needs the integer, not a numeric string
  senderNarrow('473')
  // @ts-expect-error — a bare IdentityId is the cross-substrate handle, not a Zulip user ref
  senderNarrow(decodeIdentityIdSync('473'))
  // @ts-expect-error — a bare number is not a ref; it must be minted at the parse boundary
  senderNarrow(473)
})

test('a bare string cannot reach the presence path', () => {
  // @ts-expect-error — a numeric string would coerce in the URL by accident, masking the defect
  userPresencePath('473')
  // @ts-expect-error — a bare IdentityId is not a Zulip user ref
  userPresencePath(decodeIdentityIdSync('473'))
  // @ts-expect-error — a bare number is not a ref
  userPresencePath(473)
})
