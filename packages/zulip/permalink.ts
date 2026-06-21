/**
 * Zulip narrow-URL construction. Substrate-specific URL formatting lives in
 * the adapter (the bead's principle: callers should never hand-assemble a
 * narrow URL), so every ref the Zulip adapter hands back can carry a
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
  ChannelRef,
  MessageId,
  MessageRef,
  ThreadName,
} from '@commy/core/ports'

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

export const channelPermalink = (base: string, channel: ChannelLike): string =>
  `${base}/#narrow/channel/${channelSlug(channel)}`

export const topicPermalink = (base: string, channel: ChannelLike, topic: ThreadName): string =>
  `${channelPermalink(base, channel)}/topic/${encodeHashComponent(topic)}`

export const messagePermalink = (
  base: string,
  channel: ChannelLike,
  id: MessageId,
  topic?: ThreadName,
): string =>
  topic === undefined
    ? `${channelPermalink(base, channel)}/near/${id}`
    : `${topicPermalink(base, channel, topic)}/near/${id}`

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
  threadName?: ThreadName,
): MessageRef => {
  const decoratedChannel = withChannelPermalink(base, channel)
  return threadName === undefined
    ? { id, channel: decoratedChannel, permalink: messagePermalink(base, decoratedChannel, id) }
    : {
        id,
        channel: decoratedChannel,
        thread: {
          name: threadName,
          permalink: topicPermalink(base, decoratedChannel, threadName),
        },
        permalink: messagePermalink(base, decoratedChannel, id, threadName),
      }
}
