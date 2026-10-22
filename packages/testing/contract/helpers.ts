import type {
  AgentComms,
  BindError,
  ChannelName,
  MessageBody,
  PublisherError,
  UnknownChannel,
  UnresolvedMention,
} from '@commy/core/ports'
import { Effect } from 'effect'

/**
 * Post `bodies` in order, spacing consecutive posts by the substrate's
 * `timestampGranularity` so each lands on a distinct `Timestamp`. Memory
 * reports `Duration.zero` (its `ts` counter already separates posts) so the
 * sleep is a no-op there and the fixture stays fast; live Zulip reports 1s so
 * the three posts straddle distinct integer seconds and the range/replay
 * filters assert at full strength on the real adapter. The first post is never
 * delayed.
 */
export const postSpaced = (
  comms: AgentComms,
  channel: ChannelName,
  bodies: ReadonlyArray<MessageBody>,
): Effect.Effect<void, BindError | UnknownChannel | UnresolvedMention | PublisherError> =>
  Effect.forEach(
    bodies,
    (body, index) =>
      (index === 0 ? Effect.void : Effect.sleep(comms.capabilities.timestampGranularity)).pipe(
        Effect.zipRight(comms.publisher.post(channel, body)),
      ),
    { discard: true },
  )
