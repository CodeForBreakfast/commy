import type { Identity } from '@commy/core/ports'
import { Array as Arr, Data, Match, Option } from 'effect'

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
 * blocks and inline code spans. A token there notifies nobody.
 *
 * The two paths need this for different reasons. On the write path it is
 * correctness: a dead form quoted as an example must not fail the post, since
 * Zulip would never try to deliver it. On the read path it is only cost — the
 * rendered content decides who was mentioned, so a code-spanned token that
 * slipped through here would buy a wasted request and still resolve to no
 * mention. That is why this does not need to reproduce Zulip's renderer
 * (python-markdown plus custom extensions, not CommonMark), and why its known
 * drift at blockquotes, spoilers, math and link text no longer costs
 * fidelity.
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
