import { decodeGroupName, type Identity, Mention } from '@commy/core/ports'
import { Array as Arr, Data, Effect, Equal, Match, Option, type ParseResult } from 'effect'

/**
 * The directory surface mention resolution needs: a name-keyed map for the
 * common `@**Name**` form and an id resolver for Zulip's disambiguated
 * `@**Name|user_id**` form. `byUserId` is a function rather than a map so each
 * call site adapts its own id key type (raw `number` on the events path, the
 * `ZulipUserRef` brand on the adapter path) without a cast.
 */
export interface MentionDirectory {
  readonly byName: ReadonlyMap<string, Identity>
  readonly byUserId: (userId: number) => Identity | undefined
}

// The Zulip web client disambiguates a duplicated full_name by appending the
// user id: @**Name|user_id** (web/src/people.ts get_mention_syntax). Resolve
// such a token by id — byName holds only one of two same-named humans.
const DISAMBIGUATED = /^(?<name>.*)\|(?<id>\d+)$/

/**
 * Zulip's two mention markups, scanned as a single alternation so a body is
 * read left to right exactly once and the forms cannot disagree about what
 * counts as a mention (`zerver/lib/mention.py` `MENTIONS_RE` and
 * `USER_GROUP_MENTIONS_RE`):
 *
 *   - `@**Name**` — a user, or one of the wildcards; double asterisks
 *   - `@*group*` — a user group; SINGLE asterisks
 *
 * Zulip's silent forms (`@_**Name**`, `@_*group*`) render a link but notify
 * nobody, so neither alternative admits the `_` and they are correctly not
 * mentions here.
 */
const MENTION_PATTERN = /@\*\*([^*]+)\*\*|@\*([^*]+)\*/g

/**
 * Markdown constructs Zulip does not render a mention inside: fenced code
 * blocks and inline code spans. A token there notifies nobody, so it is not a
 * mention and must not count — the demonstrated failure (phantom mentions from
 * `@**Name**` written inside backticks as discussion examples).
 *
 * This is deliberately NOT a reproduction of Zulip's renderer (python-markdown
 * plus custom extensions, not CommonMark). It removes the two demonstrated
 * carve-outs; a token buried in a blockquote, spoiler, math span, or link text
 * can still drift from what Zulip actually delivers. That residual is the
 * price of reading raw markdown — `body` must stay raw for edit round-tripping,
 * so the rendered signal is not on this payload — and is tracked as follow-up
 * (read Zulip's own delivery signal rather than inferring it, comms-l1i8).
 * Best-effort and strictly better than a markdown-blind regex, not
 * authoritative.
 */
const CODE_REGIONS = /```[\s\S]*?```|~~~[\s\S]*?~~~|(`+)[\s\S]*?\1/g
const withoutCode = (content: string): string => content.replace(CODE_REGIONS, ' ')

/**
 * Zulip's channel- and topic-wide wildcards (`zerver/lib/mention.py`:
 * `stream_wildcards` = all/everyone/stream/channel and `topic_wildcards` =
 * topic, both matched case-sensitively). They share the `@**...**` sigil with a
 * personal mention but are not users, so the directory can never resolve them —
 * they name audiences, and the port models them as audiences.
 */
const CHANNEL_WILDCARDS: ReadonlySet<string> = new Set(['all', 'everyone', 'stream', 'channel'])
const TOPIC_WILDCARD = 'topic'

/**
 * A mention attempt as written, before the directory has a say. Both paths read
 * these — the write path validates them, the read path resolves them — so
 * neither can recognise a form the other is blind to.
 */
export type MentionToken = Data.TaggedEnum<{
  readonly UserToken: { readonly token: string }
  readonly GroupToken: { readonly name: string }
  readonly ChannelWildcardToken: Record<never, never>
  readonly TopicWildcardToken: Record<never, never>
}>
export const MentionToken = Data.taggedEnum<MentionToken>()

const classifyDoubleAsterisk = (token: string): MentionToken => {
  if (CHANNEL_WILDCARDS.has(token)) return MentionToken.ChannelWildcardToken()
  if (token === TOPIC_WILDCARD) return MentionToken.TopicWildcardToken()
  return MentionToken.UserToken({ token })
}

/**
 * The mention attempts in a body that Zulip would act on — code regions
 * removed, in the order they were written.
 */
export const mentionTokens = (content: string): ReadonlyArray<MentionToken> => {
  const tokens: MentionToken[] = []
  for (const [, doubleAsterisk, singleAsterisk] of withoutCode(content).matchAll(MENTION_PATTERN)) {
    tokens.push(
      doubleAsterisk === undefined
        ? MentionToken.GroupToken({ name: singleAsterisk ?? '' })
        : classifyDoubleAsterisk(doubleAsterisk),
    )
  }
  return tokens
}

const resolveUser = (token: string, directory: MentionDirectory): Option.Option<Identity> => {
  const disambiguated = DISAMBIGUATED.exec(token)
  if (disambiguated?.groups !== undefined) {
    return Option.fromNullable(directory.byUserId(Number(disambiguated.groups['id'])))
  }
  return Option.fromNullable(directory.byName.get(token))
}

// What makes two mentions the same mention: an identity by its id, a group by
// its name, and each wildcard audience by itself — a body that says @**all**
// twice mentions one audience, not two.
const identityOf = Mention.$match({
  UserMention: (m) => Data.struct({ kind: 'user' as const, id: m.identity.id }),
  GroupMention: (m) => Data.struct({ kind: 'group' as const, name: m.name }),
  ChannelWildcardMention: () => Data.struct({ kind: 'channel-wildcard' as const }),
  TopicWildcardMention: () => Data.struct({ kind: 'topic-wildcard' as const }),
})

const toMention = (
  directory: MentionDirectory,
): ((token: MentionToken) => Effect.Effect<Option.Option<Mention>, ParseResult.ParseError>) =>
  MentionToken.$match({
    ChannelWildcardToken: () => Effect.succeedSome(Mention.ChannelWildcardMention()),
    TopicWildcardToken: () => Effect.succeedSome(Mention.TopicWildcardMention()),
    GroupToken: ({ name }) =>
      decodeGroupName(name).pipe(Effect.map((n) => Option.some(Mention.GroupMention({ name: n })))),
    UserToken: ({ token }) =>
      Effect.succeed(
        Option.map(resolveUser(token, directory), (identity) => Mention.UserMention({ identity })),
      ),
  })

/**
 * Who a body actually mentions: markdown-aware tokens resolved against the
 * directory, deduped, order-preserving. Unresolvable user tokens (a dead
 * `@**Name**`, an unknown `|id`) are dropped — Zulip renders no mention for
 * them either. Wildcards and groups need no directory: Zulip delivers them
 * whether or not commy can name the recipients.
 */
export const extractMentions = (
  content: string,
  directory: MentionDirectory,
): Effect.Effect<ReadonlyArray<Mention>, ParseResult.ParseError> =>
  Effect.forEach(mentionTokens(content), toMention(directory)).pipe(
    Effect.map(Arr.getSomes),
    Effect.map(Arr.dedupeWith((a, b) => Equal.equals(identityOf(a), identityOf(b)))),
  )

/**
 * The mention tokens in an outbound body that resolve to no known identity —
 * deduped, order-preserving. A non-empty result is a dead form (e.g. a stale
 * `@**Graeme Foster**`) that Zulip would post verbatim and notify nobody for:
 * the write path rejects the post rather than delivering silence.
 *
 * Only a user token can be dead. A wildcard always delivers, and a group is
 * unverifiable from here — commy holds no group directory — so neither is
 * reported unresolved. Both are still recognised by the shared tokeniser, so no
 * form is acted on by one path and invisible to the other.
 */
export const unresolvedMentions = (
  content: string,
  directory: MentionDirectory,
): ReadonlyArray<string> =>
  Arr.dedupe(
    Arr.filterMap(mentionTokens(content), (token) =>
      Match.value(token).pipe(
        Match.tag('UserToken', ({ token: name }) =>
          Option.isNone(resolveUser(name, directory)) ? Option.some(name) : Option.none<string>(),
        ),
        Match.orElse(() => Option.none<string>()),
      ),
    ),
  )
