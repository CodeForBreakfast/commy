/**
 * Zulip's constraints on a stream description, kept strictly inside the Zulip
 * adapter.
 *
 * Zulip stores a stream description as a single-line field capped at 1024
 * characters (`Stream.MAX_DESCRIPTION_LENGTH`, `zerver/models/streams.py`),
 * and its `update_stream` validator silently rewrites any newline to a space
 * (`ChannelDescription` in `zerver/views/streams.py`) rather than refusing the
 * write. Both are Zulip's numbers and Zulip's behaviour: core knows only that
 * a substrate may refuse a description it cannot store, expressed as
 * `ChannelDescriptionRejected`.
 *
 * The newline rewrite is the reason this module refuses multi-line input
 * instead of forwarding it. `setChannelDescription` promises that what a
 * caller writes is what `channelDescription` reads back; letting Zulip swap
 * the newlines out would break that quietly, which is exactly the silent
 * mangling the port exists to prevent.
 */

import type { ChannelDescription, ChannelName } from '@commy/core/ports'
import { ChannelDescriptionRejected } from '@commy/core/ports'
import { Option } from 'effect'

/** `Stream.MAX_DESCRIPTION_LENGTH` — zerver/models/streams.py. */
export const ZULIP_MAX_DESCRIPTION_LENGTH = 1024

/**
 * The refusal Zulip's own storage implies for this description, or `None` when
 * Zulip will store it verbatim. Checked before the write so an unstorable
 * value fails with a typed, actionable error instead of landing truncated or
 * newline-rewritten.
 */
export const rejectionFor = (
  channel: ChannelName,
  description: ChannelDescription,
): Option.Option<ChannelDescriptionRejected> =>
  description.length > ZULIP_MAX_DESCRIPTION_LENGTH
    ? Option.some(
        new ChannelDescriptionRejected({
          channel,
          substrate: 'zulip',
          constraint: 'length',
          detail: `description is too long (limit: ${ZULIP_MAX_DESCRIPTION_LENGTH} characters, got ${description.length})`,
        }),
      )
    : description.includes('\n')
      ? Option.some(
          new ChannelDescriptionRejected({
            channel,
            substrate: 'zulip',
            constraint: 'format',
            detail:
              'description must be a single line — zulip rewrites newlines to spaces rather than storing them',
          }),
        )
      : Option.none()

/**
 * The `description` field to send to `PATCH /streams/{id}` for a requested
 * description. Zulip has no separate "clear" operation and no null: an
 * undescribed stream is one whose description is the empty string, which is
 * why the port models absence as `Option.none()` rather than admitting a blank
 * description as a distinct value.
 */
export const toWireDescription = (description: Option.Option<ChannelDescription>): string =>
  Option.getOrElse(description, () => '')

/**
 * The port-facing description for what Zulip handed back. The empty string is
 * Zulip's undescribed state (`description` defaults to `""`), so it maps to
 * `None`, not to a description that happens to be blank.
 */
export const fromWireDescription = (raw: string): Option.Option<string> =>
  raw.length === 0 ? Option.none() : Option.some(raw)
