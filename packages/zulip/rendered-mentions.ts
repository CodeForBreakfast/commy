import { decodeGroupName, Mention } from '@commy/core/ports'
import { Array as Arr, Data, Effect, Equal, Option, type ParseResult } from 'effect'
import type { ZulipApiError } from './http.ts'
import type { MentionDirectory } from './mentions.ts'
import { mayMention, type RenderedContentLookup } from './rendered-content.ts'

/**
 * Zulip's rendered mention spans, read as the delivery record they are.
 *
 * `zerver/lib/markdown/__init__.py` emits a `<span>` for each mention it
 * decides to act on, and emits nothing at all for the forms it declines — a
 * token inside a code span, a `@**Name**` naming no one, a `@**Name|id**`
 * whose two halves disagree. So the presence of a span *is* the delivery
 * decision, for humans and for agents alike; there is nothing left to infer.
 *
 * The adjudicator is `class`, not `data-user-id`. A topic wildcard renders
 * with no id attribute whatsoever (the renderer's `elif topic_wildcard` arm
 * leaves `user_id` unset), so an id-keyed selector would silently drop a form
 * that notifies an entire topic.
 */

/**
 * Attributes of one rendered span. Zulip serialises through ElementTree, which
 * always double-quotes attribute values and escapes the delimiter inside them,
 * so a value never contains a bare `"`.
 */
const SPAN_TAG = /<span\b([^>]*)>([^<]*)/g
const ATTRIBUTE = /([\w-]+)\s*=\s*"([^"]*)"/g

const attributesOf = (rawAttributes: string): ReadonlyMap<string, string> =>
  new Map(
    Arr.map([...rawAttributes.matchAll(ATTRIBUTE)], ([, name, value]) => [name ?? '', value ?? '']),
  )

/**
 * The five entities ElementTree can put in element text, reversed. Zulip's
 * mention text is an `AtomicString` — no nested markup — so unescaping is the
 * whole of the decode. `&amp;` goes last or it would double-decode.
 */
const ENTITIES: ReadonlyArray<readonly [RegExp, string]> = [
  [/&lt;/g, '<'],
  [/&gt;/g, '>'],
  [/&quot;/g, '"'],
  [/&#39;/g, "'"],
  [/&amp;/g, '&'],
]

const unescapeText = (text: string): string =>
  ENTITIES.reduce((acc, [entity, char]) => acc.replace(entity, char), text)

/**
 * A notifying mention as Zulip rendered it. Silent spans have no
 * representation here at all: they render a link and notify nobody, so they
 * are not mentions. The renderer also *forces* silence on a deactivated user
 * or group regardless of how the author wrote it, which is why a dead account
 * stops being reported without commy having to know it died.
 */
export type MentionSpan = Data.TaggedEnum<{
  readonly UserSpan: { readonly userId: number }
  readonly GroupSpan: { readonly name: string }
  readonly ChannelWildcardSpan: Record<never, never>
  readonly TopicWildcardSpan: Record<never, never>
}>
export const MentionSpan = Data.taggedEnum<MentionSpan>()

const SILENT = 'silent'
const TOPIC_WILDCARD = 'topic-mention'
const CHANNEL_WILDCARD = 'channel-wildcard-mention'
const GROUP = 'user-group-mention'
const USER = 'user-mention'

// The rendered display name reads `@name` when it notifies; the sigil is
// presentation, not part of the name.
const withoutSigil = (text: string): string => unescapeText(text).replace(/^@/, '')

/**
 * Classify one span by its classes. The wildcard checks come before the plain
 * user check because Zulip stacks `channel-wildcard-mention` on top of
 * `user-mention` and gives it `data-user-id="*"` — reading that as a user
 * would resolve an audience to nobody.
 */
const classifySpan = (
  attributes: ReadonlyMap<string, string>,
  text: string,
): Option.Option<MentionSpan> => {
  const classes = new Set((attributes.get('class') ?? '').split(/\s+/))
  if (classes.has(SILENT)) return Option.none()
  if (classes.has(TOPIC_WILDCARD)) return Option.some(MentionSpan.TopicWildcardSpan())
  if (classes.has(CHANNEL_WILDCARD)) return Option.some(MentionSpan.ChannelWildcardSpan())
  if (classes.has(GROUP)) return Option.some(MentionSpan.GroupSpan({ name: withoutSigil(text) }))
  if (classes.has(USER)) {
    return Option.some(
      MentionSpan.UserSpan({ userId: Number(attributes.get('data-user-id') ?? Number.NaN) }),
    )
  }
  return Option.none()
}

/**
 * The notifying mention spans in a rendered body, in the order they appear.
 * Spans that are not mentions — Zulip renders plenty — yield nothing.
 */
export const mentionSpans = (rendered: string): ReadonlyArray<MentionSpan> =>
  Arr.filterMap([...rendered.matchAll(SPAN_TAG)], ([, rawAttributes, text]) =>
    classifySpan(attributesOf(rawAttributes ?? ''), text ?? ''),
  )

// What makes two mentions the same mention: an identity by its id, a group by
// its name, and each wildcard audience by itself — a body that says @**all**
// and @**everyone** names one audience, not two.
const identityOf = Mention.$match({
  UserMention: (m) => Data.struct({ kind: 'user' as const, id: m.identity.id }),
  GroupMention: (m) => Data.struct({ kind: 'group' as const, name: m.name }),
  ChannelWildcardMention: () => Data.struct({ kind: 'channel-wildcard' as const }),
  TopicWildcardMention: () => Data.struct({ kind: 'topic-wildcard' as const }),
})

const toMention = (
  directory: MentionDirectory,
): ((span: MentionSpan) => Effect.Effect<Option.Option<Mention>, ParseResult.ParseError>) =>
  MentionSpan.$match({
    ChannelWildcardSpan: () => Effect.succeedSome(Mention.ChannelWildcardMention()),
    TopicWildcardSpan: () => Effect.succeedSome(Mention.TopicWildcardMention()),
    GroupSpan: ({ name }) =>
      decodeGroupName(name).pipe(Effect.map((n) => Option.some(Mention.GroupMention({ name: n })))),
    UserSpan: ({ userId }) =>
      Effect.succeed(
        Option.map(Option.fromNullable(directory.byUserId(userId)), (identity) =>
          Mention.UserMention({ identity }),
        ),
      ),
  })

/**
 * Who a message mentions, according to Zulip. A span whose user id the
 * directory cannot name is dropped — commy can report a mention it cannot
 * name to nobody's benefit — and repeats collapse, order preserved.
 */
export const renderedMentions = (
  rendered: string,
  directory: MentionDirectory,
): Effect.Effect<ReadonlyArray<Mention>, ParseResult.ParseError> =>
  Effect.forEach(mentionSpans(rendered), toMention(directory)).pipe(
    Effect.map(Arr.getSomes),
    Effect.map(Arr.dedupeWith((a, b) => Equal.equals(identityOf(a), identityOf(b)))),
  )

/**
 * Who a message mentions, for a caller that holds its raw content and can
 * reach its rendering. The raw body is the candidate filter and nothing more:
 * it decides only whether asking Zulip is worth a request, never who was
 * mentioned. Because a render is a property of the message rather than of
 * whoever reads it, the answer is the same for the bound bot as for the
 * minter whose credentials fetched it.
 */
export const mentionsOfMessage = (
  renderedFor: RenderedContentLookup,
  message: { readonly id: number; readonly content: string },
  directory: MentionDirectory,
): Effect.Effect<ReadonlyArray<Mention>, ZulipApiError | ParseResult.ParseError> =>
  mayMention(message.content)
    ? renderedFor(message.id).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.succeed([] as ReadonlyArray<Mention>),
            onSome: (rendered) => renderedMentions(rendered, directory),
          }),
        ),
      )
    : Effect.succeed([])
