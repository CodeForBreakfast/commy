/**
 * Zulip's cap on a message body, kept strictly inside the Zulip adapter.
 *
 * Zulip does not refuse a body past `max_message_length`. Its body normaliser
 * (`normalize_body`, `zerver/lib/message.py`) clips the text and appends a
 * truncation marker, then the send answers success. A sender cannot tell its
 * message was cut, so the adapter refuses an over-long body before sending.
 * The cap is a server setting, published only in the `/register` initial
 * state, and counted in Unicode code points.
 */

import { type MessageBody, MessageRejected } from '@commy/core/ports'
import { Option, Schema } from 'effect'

/** The slice of `/register` state that carries the cap. */
export const messageLimitStateSchema = Schema.Struct({
  queue_id: Schema.String,
  max_message_length: Schema.Int,
})

/**
 * The refusal Zulip's own clipping implies for this body, or `None` when Zulip
 * will store it whole.
 */
export const overlongBodyRejection = (
  operation: 'post' | 'edit',
  body: MessageBody,
  limit: number,
): Option.Option<MessageRejected> => {
  const length = [...body].length
  return length > limit
    ? Option.some(
        new MessageRejected({
          operation,
          substrate: 'zulip',
          detail: `message is too long (limit: ${limit} characters, got ${length})`,
        }),
      )
    : Option.none()
}
