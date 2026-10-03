/**
 * Zulip narrow-URL construction. Substrate-specific URL formatting lives in
 * the adapter (callers don't hand-assemble a narrow URL), so every
 * ref the Zulip adapter hands back can carry a
 * ready-to-click permalink.
 *
 * The `#narrow/...` fragment is parsed client-side by Zulip's web app, which
 * decodes each operand via `decodeHashComponent` (`.`→`%` then
 * `decodeURIComponent`). `encodeHashComponent` is the exact inverse and the
 * clickability contract — it mirrors `web/src/internal_url.ts` and
 * `zerver/lib/url_encoding.py` in the Zulip source. `*`, `'`, `!`, `(`, `)`
 * are remapped because Zulip's frontend, unlike `encodeURIComponent`, escapes
 * them; the backend reaches the same output via `urllib.quote`.
 */

import type {
  ChannelId,
  ChannelName,
  ChannelPermalink,
  ChannelRef,
  MessageId,
  MessagePermalink,
  MessageRef,
  ThreadName,
  ThreadPermalink,
} from '@commy/core/ports'
import {
  ChannelPermalinkSchema,
  MessagePermalinkSchema,
  ThreadPermalinkSchema,
} from '@commy/core/ports'
import { Option } from 'effect'
import { applyResolvedPrefix } from './resolved-topic.ts'

const HASH_REPLACEMENTS: Readonly<Record<string, string>> = {
  '%': '.',
  '!': '.21',
  "'": '.27',
  '(': '.28',
  ')': '.29',
  '*': '.2A',
  '.': '.2E',
}

export const encodeHashComponent = (value: string): string =>
  encodeURIComponent(value).replace(
    /[%!'()*.]/g,
    (matched) => HASH_REPLACEMENTS[matched] ?? matched,
  )

/**
 * The human-facing realm origin for permalinks. When `hostHeader` is set the
 * `realmUrl` is the cluster-internal service URL and the public vhost lives in
 * the override (see `ZulipHttpConfig.hostHeader`), so a clickable link must use
 * the public host. Otherwise `realmUrl` is already the public origin.
 */
export const permalinkBase = (config: {
  readonly realmUrl: string
  readonly hostHeader?: string
}): string =>
  config.hostHeader === undefined
    ? config.realmUrl.replace(/\/+$/, '')
    : `https://${config.hostHeader}`

type ChannelLike = Pick<ChannelRef, 'id' | 'name'>

const channelSlug = (channel: ChannelLike): string =>
  `${channel.id}-${encodeHashComponent(channel.name.replaceAll(' ', '-'))}`

export const channelPermalink = (base: string, channel: ChannelLike): ChannelPermalink =>
  ChannelPermalinkSchema.make(`${base}/#narrow/channel/${channelSlug(channel)}`)

/**
 * A thread/topic permalink built with Zulip's `with` operator
 * (`.../topic/<name>/with/<anchor>`). The `with` operator locates the
 * conversation by the anchor message's id and treats the topic operand as a
 * stale-tolerant hint, so the link follows the conversation across a topic
 * rename, move, or resolve — where a `topic`-name narrow would 404. Mirrors
 * Zulip's own `by_stream_topic_url` (`web/src/internal_url.ts`); requires realm
 * feature level ≥271 (Zulip 9.0). The anchor is any member message of the
 * thread — every `ObservedThread` is the thread facet of an observed message,
 * so that message's id is always the anchor. The topic operand is the clean
 * port-facing name: a resolved thread's ✔ prefix is a substrate detail that
 * never reaches the URL, and the anchor keeps the link valid across the resolve
 * regardless — resolution is surfaced via `ObservedThread.resolved`, not here.
 */
export const topicPermalink = (
  base: string,
  channel: ChannelLike,
  topic: ThreadName,
  anchor: MessageId,
): ThreadPermalink =>
  ThreadPermalinkSchema.make(
    `${channelPermalink(base, channel)}/topic/${encodeHashComponent(topic)}/with/${anchor}`,
  )

type ThreadLike = { readonly name: ThreadName; readonly resolved: boolean }

/**
 * A single-message permalink in the form Zulip's own "Copy link to message"
 * builds (`by_conversation_and_time_url` in `web/src/hash_util.ts`):
 * `.../topic/<topic>/near/<id>`, or `.../near/<id>` when the topic is unknown.
 *
 * The channel operand is what makes the link open for a viewer who never
 * received the message. The server reads a channel's history, rather than only
 * the messages the viewer received, when the narrow names that channel
 * (`ok_to_include_history` in `zerver/lib/narrow.py`). A bare
 * `#narrow/id/<id>` therefore comes back empty for anyone who was not
 * subscribed when the message was sent.
 *
 * The web app keeps the link valid across a rename, move or resolve: it fetches
 * the `near` message and retargets the narrow when the message has moved from
 * the linked topic (`message_view.show`). The topic is written as the substrate
 * names it now, ✔ included, because the retarget only follows the message's
 * edit history, and a message sent into an already-resolved topic has none.
 */
export const messagePermalink = (
  base: string,
  channel: ChannelLike,
  id: MessageId,
  thread?: ThreadLike,
): MessagePermalink =>
  MessagePermalinkSchema.make(
    thread === undefined
      ? `${channelPermalink(base, channel)}/near/${id}`
      : `${channelPermalink(base, channel)}/topic/${encodeHashComponent(applyResolvedPrefix(thread.name, thread.resolved))}/near/${id}`,
  )

/** A channel ref carrying its narrow permalink. */
export const withChannelPermalink = (
  base: string,
  channel: { readonly id: ChannelId; readonly name: ChannelName },
): ChannelRef => ({ ...channel, permalink: channelPermalink(base, channel) })

/**
 * Assemble a fully-decorated MessageRef — message, channel, and (when present)
 * topic permalinks. The single ref-builder both the history/post paths and the
 * inbound-event path go through, so the two can never drift on URL shape.
 */
export const buildMessageRef = (
  base: string,
  id: MessageId,
  channel: { readonly id: ChannelId; readonly name: ChannelName },
  thread?: ThreadLike,
): MessageRef => {
  const decoratedChannel = withChannelPermalink(base, channel)
  const permalink = messagePermalink(base, decoratedChannel, id, thread)
  return thread === undefined
    ? {
        id,
        channel: decoratedChannel,
        thread: Option.none(),
        permalink,
      }
    : {
        id,
        channel: decoratedChannel,
        thread: Option.some({
          name: thread.name,
          resolved: thread.resolved,
          permalink: topicPermalink(base, decoratedChannel, thread.name, id),
        }),
        permalink,
      }
}
