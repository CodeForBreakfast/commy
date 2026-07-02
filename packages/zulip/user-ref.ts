declare const zulipUserRefBrand: unique symbol

/**
 * A reference Zulip's user-scoped APIs can actually resolve: the integer
 * `user_id`. Zulip's `sender` narrow operand and `/users/{id}/…` paths accept
 * an integer user id (or an email) — never a numeric string.
 *
 * Minted ONLY from a real `user_id: number` at the adapter's parse boundary,
 * never by coercing a cross-substrate `IdentityId` string. Because the brand
 * is over `number`, neither a bare string nor a bare `number` (e.g.
 * `Number(identityId)`) satisfies it — the only way in is this constructor.
 */
export type ZulipUserRef = number & { readonly [zulipUserRefBrand]: never }

export const ZulipUserRef = (userId: number): ZulipUserRef => userId as ZulipUserRef

export interface SenderNarrow {
  readonly operator: 'sender'
  readonly operand: ZulipUserRef
}

export const senderNarrow = (ref: ZulipUserRef): SenderNarrow => ({
  operator: 'sender',
  operand: ref,
})

export const userPresencePath = (ref: ZulipUserRef): string => `/users/${ref}/presence`
