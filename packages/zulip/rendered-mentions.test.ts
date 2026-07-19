import { expect, test } from 'bun:test'
import type { Identity, Mention as MentionType } from '@commy/core/ports'
import {
  decodeDisplayNameSync,
  decodeGroupNameSync,
  decodeIdentityIdSync,
  Mention,
  mentionedIdentities,
  mentionsIdentity,
} from '@commy/core/ports'
import { Effect } from 'effect'
import type { MentionDirectory } from './mentions.ts'
import { renderedMentions } from './rendered-mentions.ts'

const mentionsIn = (rendered: string, dir: MentionDirectory): ReadonlyArray<MentionType> =>
  Effect.runSync(renderedMentions(rendered, dir))

const mentionedIds = (rendered: string, dir: MentionDirectory): ReadonlyArray<string> =>
  mentionedIdentities(mentionsIn(rendered, dir)).map((i) => i.id)

const identity = (id: string, name: string, kind: Identity['kind'] = 'human'): Identity => ({
  id: decodeIdentityIdSync(id),
  name: decodeDisplayNameSync(name),
  kind,
})

const GRAEME = identity('7', 'Graeme')
const HERMES = identity('9', 'hermes-agent', 'agent')

const directoryFor = (...identities: ReadonlyArray<Identity>): MentionDirectory => {
  const byId = new Map(identities.map((i) => [Number(i.id), i]))
  return {
    byName: new Map(identities.map((i) => [i.name as string, i])),
    byUserId: (userId) => byId.get(userId),
  }
}

const paragraph = (inner: string): string => `<p>${inner}</p>`

test('resolves a user-mention span against the directory', () => {
  const dir = directoryFor(GRAEME, HERMES)
  const rendered = paragraph(
    'oi <span class="user-mention" data-user-id="7">@Graeme</span> and ' +
      '<span class="user-mention" data-user-id="9">@hermes-agent</span> look',
  )
  expect(mentionedIds(rendered, dir)).toEqual([GRAEME.id, HERMES.id])
})

test('reads the span whichever order Zulip serialises its attributes in', () => {
  const dir = directoryFor(GRAEME)
  const rendered = paragraph('<span data-user-id="7" class="user-mention">@Graeme</span>')
  expect(mentionedIds(rendered, dir)).toEqual([GRAEME.id])
})

test('excludes a silent user mention — it links but notifies nobody', () => {
  const dir = directoryFor(GRAEME)
  const rendered = paragraph('<span class="user-mention silent" data-user-id="7">Graeme</span>')
  expect(mentionsIn(rendered, dir)).toEqual([])
})

test('reads a channel wildcard as an audience, not as the user id it carries', () => {
  const dir = directoryFor(GRAEME)
  const rendered = paragraph(
    '<span class="user-mention channel-wildcard-mention" data-user-id="*">@all</span>',
  )
  expect(mentionsIn(rendered, dir)).toEqual([Mention.ChannelWildcardMention()])
})

test('excludes a silent channel wildcard', () => {
  const dir = directoryFor(GRAEME)
  const rendered = paragraph(
    '<span class="user-mention channel-wildcard-mention silent" data-user-id="*">all</span>',
  )
  expect(mentionsIn(rendered, dir)).toEqual([])
})

// The renderer never assigns data-user-id on the topic-wildcard branch
// (zerver/lib/markdown/__init__.py, the `elif topic_wildcard` arm leaves
// user_id None), so a selector keyed on that attribute drops a delivered form.
test('reads a topic wildcard, which carries no data-user-id at all', () => {
  const dir = directoryFor(GRAEME)
  const rendered = paragraph('<span class="topic-mention">@topic</span>')
  expect(mentionsIn(rendered, dir)).toEqual([Mention.TopicWildcardMention()])
})

test('excludes a silent topic wildcard', () => {
  const dir = directoryFor(GRAEME)
  const rendered = paragraph('<span class="topic-mention silent">topic</span>')
  expect(mentionsIn(rendered, dir)).toEqual([])
})

test('names a group mention from the display name Zulip rendered', () => {
  const dir = directoryFor(GRAEME)
  const rendered = paragraph(
    '<span class="user-group-mention" data-user-group-id="4">@backend</span>',
  )
  expect(mentionsIn(rendered, dir)).toEqual([
    Mention.GroupMention({ name: decodeGroupNameSync('backend') }),
  ])
})

test('excludes a silent group mention', () => {
  const dir = directoryFor(GRAEME)
  const rendered = paragraph(
    '<span class="user-group-mention silent" data-user-group-id="4">backend</span>',
  )
  expect(mentionsIn(rendered, dir)).toEqual([])
})

test('unescapes entities in a rendered group name', () => {
  const dir = directoryFor(GRAEME)
  const rendered = paragraph(
    '<span class="user-group-mention" data-user-group-id="4">@r&amp;d</span>',
  )
  expect(mentionsIn(rendered, dir)).toEqual([
    Mention.GroupMention({ name: decodeGroupNameSync('r&d') }),
  ])
})

test('drops a user mention whose id the directory cannot name', () => {
  const dir = directoryFor(GRAEME)
  const rendered = paragraph('<span class="user-mention" data-user-id="404">@Ghost</span>')
  expect(mentionsIn(rendered, dir)).toEqual([])
})

// The asymmetry this bead exists to close: a code-spanned at-name is
// agent-visible under the raw-markdown inference and human-invisible in
// Zulip's delivery. The render has the final say and renders no span.
test('finds no mention in a code-spanned at-name, because Zulip rendered none', () => {
  const dir = directoryFor(GRAEME)
  const rendered = paragraph('use <code>@**Graeme**</code>, not the dead form')
  expect(mentionsIn(rendered, dir)).toEqual([])
})

test('dedupes a repeated mention and preserves the order they were written', () => {
  const dir = directoryFor(GRAEME, HERMES)
  const rendered = paragraph(
    '<span class="user-mention" data-user-id="9">@hermes-agent</span> ping ' +
      '<span class="user-mention" data-user-id="7">@Graeme</span> then ' +
      '<span class="user-mention" data-user-id="9">@hermes-agent</span> again',
  )
  expect(mentionedIds(rendered, dir)).toEqual([HERMES.id, GRAEME.id])
})

test('dedupes a repeated wildcard audience — one audience, not two', () => {
  const dir = directoryFor(GRAEME)
  const rendered = paragraph(
    '<span class="user-mention channel-wildcard-mention" data-user-id="*">@all</span> and ' +
      '<span class="user-mention channel-wildcard-mention" data-user-id="*">@everyone</span>',
  )
  expect(mentionsIn(rendered, dir)).toEqual([Mention.ChannelWildcardMention()])
})

test('ignores spans that are not mentions', () => {
  const dir = directoryFor(GRAEME)
  const rendered = paragraph(
    '<span class="katex"><span class="katex-mathml">x</span></span> and ' +
      '<span class="user-mention" data-user-id="7">@Graeme</span>',
  )
  expect(mentionedIds(rendered, dir)).toEqual([GRAEME.id])
})

test('finds nothing in rendered content with no spans at all', () => {
  const dir = directoryFor(GRAEME)
  expect(mentionsIn(paragraph('just a sentence'), dir)).toEqual([])
})

test('a wildcard audience reaches anyone who sees the message', () => {
  const dir = directoryFor(GRAEME)
  // HERMES is named nowhere in the render; both wildcards still reach them.
  const channelWide = mentionsIn(
    paragraph('<span class="user-mention channel-wildcard-mention" data-user-id="*">@all</span>'),
    dir,
  )
  const topicWide = mentionsIn(paragraph('<span class="topic-mention">@topic</span>'), dir)
  expect(mentionsIdentity(channelWide, HERMES.id)).toBe(true)
  expect(mentionsIdentity(topicWide, HERMES.id)).toBe(true)
})

test('a group mention is not claimed to reach a member commy cannot see', () => {
  const dir = directoryFor(GRAEME)
  // The render names the group, not its membership, and commy holds no group
  // directory — so a match here would be a guess rather than a delivery record.
  const group = mentionsIn(
    paragraph('<span class="user-group-mention" data-user-group-id="4">@backend</span>'),
    dir,
  )
  expect(mentionsIdentity(group, HERMES.id)).toBe(false)
})
