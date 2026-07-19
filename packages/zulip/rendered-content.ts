import { Array as Arr, Effect, Option, type ParseResult, Schema } from 'effect'
import type { ZulipApiError, ZulipHttp, ZulipParams } from './http.ts'
import { mentionTokens } from './mentions.ts'

/**
 * Zulip's rendering of a message, fetched deliberately and used only to
 * adjudicate mentions.
 *
 * `GET /messages` returns raw content or rendered content, never both —
 * `finalize_payload` deletes `rendered_content` when `apply_markdown` is
 * false. So a caller that needs the raw body (every caller: `body` must stay
 * raw markdown for edit round-tripping) and also needs Zulip's mention
 * decision has to read the message twice. That second read is what this
 * module bounds.
 */
export type RenderedContentLookup = (
  messageId: number,
) => Effect.Effect<Option.Option<string>, ZulipApiError | ParseResult.ParseError>

const renderedMessagesSchema = Schema.Struct({
  result: Schema.Literal('success'),
  messages: Schema.Array(Schema.Struct({ id: Schema.Int, content: Schema.String })),
})

/**
 * The candidate test. Raw content cannot false-negative the mention sigil — a
 * body Zulip renders a mention span for always contains the markup that
 * produced it — so a body with no token mentions nobody and is worth no
 * request. It can and does false-*positive* (a token in a code span, a name
 * belonging to no one), which is exactly what the rendered read then settles.
 */
export const mayMention = (content: string): boolean =>
  !Arr.isEmptyReadonlyArray(mentionTokens(content))

const NEVER_RENDERED: RenderedContentLookup = () => Effect.succeedNone

/**
 * A lookup that reads one message's rendering on demand. For the events path,
 * where messages arrive one at a time and there is no batch to amortise
 * across — one rate-limited request per mention-bearing message. The
 * long-poll's exemption from `api_by_user` does not extend to this call: it is
 * a plain Django request on the same budget as any other.
 *
 * INVARIANT this call must not break: the pump's retry never gives up
 * (`defaultRetrySchedule`, wrapped around the whole poll step), so nothing
 * inside that step may fail *deterministically* — a failure that recurs on
 * every attempt stalls all inbound traffic, not only the message that caused
 * it. Substrate-level failures are fine; they resolve on their own. This
 * fetch currently satisfies that: Zulip answers an anchor naming no live
 * message with an empty result rather than an error. Anything that changes
 * that — a stricter decode here, a narrower permission on the reading
 * identity, an upstream change to `/messages` — needs the pump's retry
 * revisited, not just this function.
 */
export const renderedContentPerMessage =
  (http: ZulipHttp): RenderedContentLookup =>
  (messageId) =>
    http
      .get('/messages', renderedMessagesSchema, {
        anchor: messageId,
        num_before: 0,
        num_after: 0,
        narrow: '[]',
        apply_markdown: true,
      })
      .pipe(
        Effect.map((res) => {
          const message = res.messages[0]
          return message === undefined || message.id !== messageId
            ? Option.none()
            : Option.some(message.content)
        }),
      )

/**
 * A lookup over a whole batch, built from **at most one** request: the same
 * query the caller already issued for raw content, re-issued with
 * `apply_markdown: true`, indexed by message id.
 *
 * This is the property the design rests on — a history read or a replay costs
 * one extra request whether it contains one mention or a hundred, so a
 * mention-dense thread cannot scale the request count. Keep it exactly one;
 * a refactor into a per-message fetch would silently reintroduce the cost
 * this bounds.
 *
 * A batch whose raw bodies carry no mention sigil at all costs nothing.
 */
export const renderedContentForBatch = (
  http: ZulipHttp,
  query: ZulipParams,
  messages: ReadonlyArray<{ readonly content: string }>,
): Effect.Effect<RenderedContentLookup, ZulipApiError | ParseResult.ParseError> =>
  messages.some((m) => mayMention(m.content))
    ? http.get('/messages', renderedMessagesSchema, { ...query, apply_markdown: true }).pipe(
        Effect.map((res) => {
          const byId = new Map(res.messages.map((m) => [m.id, m.content]))
          return (messageId: number) => Effect.succeed(Option.fromNullable(byId.get(messageId)))
        }),
      )
    : Effect.succeed(NEVER_RENDERED)
