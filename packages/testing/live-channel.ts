import type { Directory, DirectoryError } from '@commy/core/ports'
import { Data, Effect } from 'effect'

export class LiveChannelMissing extends Data.TaggedError('LiveChannelMissing')<{
  readonly channel: string
}> {
  override get message(): string {
    return `ZULIP_LIVE_CHANNEL_NAME names "${this.channel}", but the realm does not list a channel by that name. Point it at a channel that exists.`
  }
}

/**
 * Fail a live suite up front, in plain words, when its configured channel is
 * not in the realm. Without it every channel-gated test fails separately with
 * a post error that never says the configuration is the cause.
 */
export const requireLiveChannel = (
  directory: Pick<Directory, 'listChannels'>,
  channel: string,
): Effect.Effect<void, DirectoryError | LiveChannelMissing> =>
  directory.listChannels().pipe(
    Effect.filterOrFail(
      (channels) => channels.some((c) => c.name === channel),
      () => new LiveChannelMissing({ channel }),
    ),
    Effect.asVoid,
  )
