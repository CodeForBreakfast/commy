/**
 * commy is a channel-based substrate by design — every message
 * lives on a stream so the audit log is complete. Zulip's realm-level
 * direct-message policies (`direct_message_permission_group`,
 * `direct_message_initiator_group`) cannot express "block bot↔bot only"
 * because `zerver/actions/message_send.py:check_can_send_direct_message`
 * short-circuits both when the sender is a bot and when every recipient
 * is a bot (or self). So the bot-side of the substrate has to defend
 * itself: a wrapper sitting on every bot-authenticated `ZulipHttp` that
 * intercepts `POST /messages` requests with `type=private` and rejects
 * them when every non-self recipient is `kind: 'agent'`.
 *
 * Future-proofing: nothing in `publisher.post` today constructs
 * `type=private` payloads, but any code that adds a private-message
 * path will route through `boundHttp` and trip this guard.
 */

import { Data, Effect, Either, type ParseResult, Schema } from 'effect'

import type { ZulipApiError, ZulipHttp, ZulipParams } from './http.ts'
import type { ZulipUserRef } from './user-ref.ts'

export class BotToBotDirectMessageError extends Data.TaggedError('BotToBotDirectMessageError')<{
  readonly detail: string
}> {
  override get message(): string {
    return `commy: bot-to-bot direct messages are not permitted — ${this.detail}`
  }
}

export interface RecipientDirectory {
  readonly byId: ReadonlyMap<number, { readonly kind: 'agent' | 'human' }>
}

export type BotHttp = Pick<ZulipHttp, 'get' | 'post' | 'patch' | 'delete'>

const decodeUserIds = Schema.decodeUnknownEither(Schema.parseJson(Schema.Array(Schema.Int)))

const parseUserIds = (raw: unknown): ReadonlyArray<number> | undefined =>
  Either.getOrUndefined(decodeUserIds(raw))

const checkNotBotToBotDirect = (
  body: ZulipParams,
  resolveDirectory: () => Effect.Effect<RecipientDirectory, ZulipApiError | ParseResult.ParseError>,
  selfUserId: ZulipUserRef,
): Effect.Effect<void, BotToBotDirectMessageError | ZulipApiError | ParseResult.ParseError> => {
  if (body['type'] !== 'private') return Effect.void
  const recipientIds = parseUserIds(body['to'])
  if (recipientIds === undefined) {
    return Effect.fail(
      new BotToBotDirectMessageError({
        detail: `unrecognised recipient format (expected JSON array of user_ids, got ${JSON.stringify(body['to'])})`,
      }),
    )
  }
  const nonSelf = recipientIds.filter((id) => id !== selfUserId)
  if (nonSelf.length === 0) return Effect.void
  return resolveDirectory().pipe(
    Effect.flatMap((directory) => {
      const everyNonSelfIsBot = nonSelf.every((id) => {
        const ident = directory.byId.get(id)
        return ident !== undefined && ident.kind === 'agent'
      })
      return everyNonSelfIsBot
        ? Effect.fail(
            new BotToBotDirectMessageError({
              detail: `every non-self recipient ${JSON.stringify(nonSelf)} is a bot`,
            }),
          )
        : Effect.void
    }),
  )
}

export const wrapBotHttp = (
  inner: BotHttp,
  resolveDirectory: () => Effect.Effect<RecipientDirectory, ZulipApiError | ParseResult.ParseError>,
  selfUserId: ZulipUserRef,
): BotHttp => ({
  get: <A, I>(path: string, schema: Schema.Schema<A, I>, params?: ZulipParams) =>
    inner.get(path, schema, params),
  post: <A, I>(path: string, schema: Schema.Schema<A, I>, body: ZulipParams) => {
    const guard: Effect.Effect<
      void,
      BotToBotDirectMessageError | ZulipApiError | ParseResult.ParseError
    > =
      path === '/messages'
        ? checkNotBotToBotDirect(body, resolveDirectory, selfUserId)
        : Effect.void
    return guard.pipe(
      Effect.andThen(inner.post(path, schema, body)),
      // Bot-to-bot direct messages are a defensive guard against a code
      // path that should not exist — surface as a defect so it fails loud
      // for the caller and stays out of the typed publisher.post E channel.
      Effect.catchTag('BotToBotDirectMessageError', (cause) => Effect.die(cause)),
    )
  },
  patch: <A, I>(path: string, schema: Schema.Schema<A, I>, body: ZulipParams) =>
    inner.patch(path, schema, body),
  delete: <A, I>(path: string, schema: Schema.Schema<A, I>, body?: ZulipParams) =>
    inner.delete(path, schema, body),
})
