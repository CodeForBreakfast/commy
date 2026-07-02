import type { IdentityId, InboundEvent, Timestamp } from '@commy/core/ports'
import { Array as Arr, Option, Record as Rec, String as Str } from 'effect'

/**
 * Wire shape of a single inbound event after rendering. Drives the
 * `notifications/claude/channel` notification's `params` directly: the
 * Claude Code host wraps as `<channel source="commy" {meta}>{content}</channel>`,
 * mirroring the precedent established by the Discord plugin. The plugin
 * owns sanitisation of meta values; the host owns the wrapping tag.
 */
export interface ChannelEventPayload {
  readonly content: string
  readonly meta: Record<string, string>
}

/**
 * Numeric identity-id meta keys that ride on the machine-facing carrier
 * (`notifications/message`) but are omitted from the agent-display projection
 * (`notifications/claude/channel`). The bare numbers collide with message ids
 * and are noise in the agent's turn, while machine consumers (e.g. Hermes
 * `SessionSource` keying) read them off the data carrier.
 */
export const IDENTITY_ID_META_KEYS = ['sender_id', 'by_id'] as const

const ATTR_STRIP = /[[\]\r\n;]/g

const safeAttr = (value: string): string => value.replace(ATTR_STRIP, '_')

const buildMeta = (
  entries: ReadonlyArray<readonly [string, string | undefined]>,
): Record<string, string> =>
  Rec.fromEntries(
    Arr.filterMap(entries, ([k, v]) =>
      v === undefined ? Option.none() : Option.some([k, safeAttr(v)] as const),
    ),
  )

export const formatMessage = (
  event: Extract<InboundEvent, { readonly kind: 'message-posted' | 'mention-received' }>,
  botIdentityId: IdentityId | undefined,
): ChannelEventPayload => {
  const msg = event.message
  const ref = msg.ref
  const sender = msg.sender
  // Each name is individually safeAttr'd (which strips ';'), so a name can
  // never contain the join delimiter; the joined value is then merged in
  // WITHOUT a second buildMeta pass, which would strip the delimiters too.
  const mentionNames = Arr.map(msg.mentions, (m) => safeAttr(m.name))
  const mentioned =
    botIdentityId !== undefined && msg.mentions.some((m) => m.id === botIdentityId)
      ? 'true'
      : undefined

  const meta = buildMeta([
    ['channel_id', ref.channel.id],
    ['channel_name', ref.channel.name],
    ['thread', ref.thread?.name],
    ['message_id', ref.id],
    // Clickable narrow permalinks for the ref the substrate handed us — message,
    // channel, and topic. Omitted by buildMeta when the substrate
    // has none.
    ['permalink', ref.permalink],
    ['channel_permalink', ref.channel.permalink],
    ['thread_permalink', ref.thread?.permalink],
    ['sender_id', sender.id],
    ['sender_name', sender.name],
    ['sender_kind', sender.kind],
    ['ts', String(msg.ts)],
    ['mentioned', mentioned],
    // Gap-replay marker. When the substrate iterator
    // backfills messages on BAD_EVENT_QUEUE_ID recovery it stamps the
    // event with replayed=true; surface that as a meta attribute so the
    // CC host's `<channel ... replayed="true">` block tells the
    // consumer this is a backfilled message and not a fresh post. Live
    // events leave the flag undefined and the attribute is omitted.
    ['replayed', event.replayed === true ? 'true' : undefined],
  ])

  return {
    content: msg.body,
    meta: mentionNames.length > 0 ? { ...meta, mentions: mentionNames.join(';') } : meta,
  }
}

export const formatReaction = (
  event: Extract<InboundEvent, { readonly kind: 'reaction-added' | 'reaction-removed' }>,
  observedAt: Timestamp,
): ChannelEventPayload => {
  const action = event.kind === 'reaction-added' ? 'add' : 'remove'
  const meta = buildMeta([
    ['target_message_id', event.target.id],
    ['target_channel_name', event.target.channel.name],
    ['target_thread', event.target.thread?.name],
    ['target_permalink', event.target.permalink],
    ['reaction_emoji', event.emoji],
    ['reaction_action', action],
    ['by_id', event.by.id],
    ['by_name', event.by.name],
    ['by_kind', event.by.kind],
    ['ts', String(observedAt)],
  ])
  return { content: `[reaction ${action}] ${event.emoji}`, meta }
}

export const formatError = (kind: string, message: string): ChannelEventPayload => {
  const resolvedKind = Str.isEmpty(kind) ? 'unknown' : kind
  const meta = buildMeta([['error_kind', resolvedKind]])
  return { content: message, meta }
}
