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
import { extractMentions, type MentionDirectory, unresolvedMentions } from './mentions.ts'

const mentionsIn = (content: string, dir: MentionDirectory): ReadonlyArray<MentionType> =>
  Effect.runSync(extractMentions(content, dir))

const mentionedIds = (content: string, dir: MentionDirectory): ReadonlyArray<string> =>
  mentionedIdentities(mentionsIn(content, dir)).map((i) => i.id)

const identity = (id: string, name: string, kind: Identity['kind'] = 'human'): Identity => ({
  id: decodeIdentityIdSync(id),
  name: decodeDisplayNameSync(name),
  kind,
})

const GRAEME = identity('7', 'Graeme')
const HERMES = identity('9', 'hermes-agent', 'agent')
// Two humans share a full_name — Zulip disambiguates them as @**Robin Reyes|11**.
const ROBIN_A = identity('11', 'Robin Reyes')
const ROBIN_B = identity('12', 'Robin Reyes')

const directoryFor = (...identities: ReadonlyArray<Identity>): MentionDirectory => {
  const byId = new Map(identities.map((i) => [Number(i.id), i]))
  return {
    // Name-keyed: two same-named humans collide, so byName holds only the last.
    byName: new Map(identities.map((i) => [i.name as string, i])),
    byUserId: (userId) => byId.get(userId),
  }
}

test('resolves a plain @**Name** mention against the directory', () => {
  const dir = directoryFor(GRAEME, HERMES)
  expect(mentionedIds('oi @**Graeme** and @**hermes-agent** look', dir)).toEqual([
    GRAEME.id,
    HERMES.id,
  ])
})

test('does not count a mention inside an inline code span', () => {
  const dir = directoryFor(GRAEME)
  // The exact shape that produced the phantom-mention dispute (msg 17982):
  // the token only appears inside backticks, as an example in a sentence.
  expect(mentionsIn('use `@**Graeme**`, not the dead form', dir)).toEqual([])
})

test('does not count a mention inside a multi-backtick code span', () => {
  const dir = directoryFor(GRAEME)
  expect(mentionsIn('here: ``@**Graeme**`` is literal', dir)).toEqual([])
})

test('does not count a mention inside a fenced code block', () => {
  const dir = directoryFor(GRAEME)
  const content = ['before', '```', 'ping @**Graeme** here', '```', 'after'].join('\n')
  expect(mentionsIn(content, dir)).toEqual([])
})

test('counts a real mention even when the message also discusses one in code', () => {
  const dir = directoryFor(GRAEME)
  // A real ping AND a code-span example in the same body — only the real one counts.
  expect(mentionedIds('@**Graeme** — never write `@**Graeme**` in a code span', dir)).toEqual([
    GRAEME.id,
  ])
})

test('deduplicates repeated mentions of the same identity', () => {
  const dir = directoryFor(GRAEME)
  expect(mentionedIds('@**Graeme** @**Graeme** @**Graeme**', dir)).toEqual([GRAEME.id])
})

test('resolves a disambiguated @**Name|id** mention to the right identity', () => {
  const dir = directoryFor(ROBIN_A, ROBIN_B)
  // byName resolves "Robin Reyes" to ROBIN_B (last writer); the |id form must
  // pin ROBIN_A specifically.
  expect(mentionedIds('hey @**Robin Reyes|11** over here', dir)).toEqual([ROBIN_A.id])
})

test('drops a mention whose token resolves to no known identity', () => {
  const dir = directoryFor(GRAEME)
  // The dead form: @**Graeme Foster** no longer exists in the directory.
  expect(mentionsIn('decision for @**Graeme Foster** to make', dir)).toEqual([])
})

test('unresolvedMentions reports a dead @**Name** token', () => {
  const dir = directoryFor(GRAEME)
  expect(unresolvedMentions('ping @**Graeme Foster** please', dir)).toEqual(['Graeme Foster'])
})

test('unresolvedMentions is empty when every mention resolves', () => {
  const dir = directoryFor(GRAEME, HERMES)
  expect(unresolvedMentions('@**Graeme** and @**hermes-agent**', dir)).toEqual([])
})

test('unresolvedMentions ignores a dead form that only appears inside code', () => {
  const dir = directoryFor(GRAEME)
  // Discussing the dead form as an example must not fail a post — Zulip would
  // never try to deliver it, so it is not silent non-delivery.
  const content = 'the dead form `@**Graeme Foster**` resolves to nobody'
  expect(unresolvedMentions(content, dir)).toEqual([])
})

test('unresolvedMentions reports an unknown |id token', () => {
  const dir = directoryFor(ROBIN_A)
  // |99 is not in the directory — a genuinely dead disambiguated form.
  expect(unresolvedMentions('@**Robin Reyes|99** hi', dir)).toEqual(['Robin Reyes|99'])
})

test('unresolvedMentions deduplicates a repeated dead token', () => {
  const dir = directoryFor(GRAEME)
  expect(unresolvedMentions('@**Ghost** @**Ghost**', dir)).toEqual(['Ghost'])
})

// Channel-/topic-wide wildcard mentions share the `@**...**` sigil but are not
// users, so byName never resolves them. They are legitimate, deliverable Zulip
// constructs, not dead forms — the write path must not reject a post that
// carries one. The full Zulip set (zerver/lib/mention.py): stream wildcards
// all/everyone/stream/channel plus the topic wildcard, case-sensitive.
test.each([
  'all',
  'everyone',
  'stream',
  'channel',
  'topic',
])('unresolvedMentions exempts the @**%s** wildcard', (wildcard) => {
  const dir = directoryFor(GRAEME)
  expect(unresolvedMentions(`heads up @**${wildcard}** — deploying`, dir)).toEqual([])
})

test('unresolvedMentions still reports a dead form alongside a wildcard', () => {
  const dir = directoryFor(GRAEME)
  // The wildcard is exempt; a genuinely dead @**Name** in the same body is not.
  expect(unresolvedMentions('@**all** and @**Graeme Foster**', dir)).toEqual(['Graeme Foster'])
})

test('unresolvedMentions does not exempt a capitalised wildcard look-alike', () => {
  const dir = directoryFor(GRAEME)
  // Zulip matches wildcards case-sensitively; @**All** is a failed user mention.
  expect(unresolvedMentions('ping @**All** now', dir)).toEqual(['All'])
})

// The read path's half of the wildcard story. Reporting no mention for a
// message Zulip delivers to every subscriber is the highest-consequence miss on
// the substrate — it is exactly the message that was supposed to wake everyone.
test.each([
  'all',
  'everyone',
  'stream',
  'channel',
])('reports @**%s** as a channel-wide mention', (wildcard) => {
  const dir = directoryFor(GRAEME)
  expect(mentionsIn(`heads up @**${wildcard}** — deploying`, dir)).toEqual([
    Mention.ChannelWildcardMention(),
  ])
})

test('reports @**topic** as a topic-wide mention, distinct from the channel wildcards', () => {
  const dir = directoryFor(GRAEME)
  expect(mentionsIn('@**topic** who owns this?', dir)).toEqual([Mention.TopicWildcardMention()])
})

test('a capitalised wildcard look-alike is not a wildcard on the read path either', () => {
  const dir = directoryFor(GRAEME)
  // Case-sensitive in Zulip, so @**All** is an unresolvable user mention and
  // drops — the read path must not be laxer than the write path.
  expect(mentionsIn('ping @**All** now', dir)).toEqual([])
})

test('the four channel wildcards collapse to one audience', () => {
  const dir = directoryFor(GRAEME)
  // Same set of recipients, so one mention — not four.
  expect(mentionsIn('@**all** @**everyone** @**stream** @**channel**', dir)).toEqual([
    Mention.ChannelWildcardMention(),
  ])
})

// Zulip's user-group markup is SINGLE asterisks (USER_GROUP_MENTIONS_RE). A
// pattern that requires `@**` never matches it, which is why group mentions
// were invisible in a way even the wildcards were not.
test('reports @*group* as a group mention', () => {
  const dir = directoryFor(GRAEME)
  expect(mentionsIn('@*backend* please review', dir)).toEqual([
    Mention.GroupMention({ name: decodeGroupNameSync('backend') }),
  ])
})

test('distinguishes a group mention from a user mention of the same name', () => {
  const dir = directoryFor(GRAEME)
  expect(mentionsIn('@*Graeme* and @**Graeme**', dir)).toEqual([
    Mention.GroupMention({ name: decodeGroupNameSync('Graeme') }),
    Mention.UserMention({ identity: GRAEME }),
  ])
})

test('deduplicates a repeated group mention', () => {
  const dir = directoryFor(GRAEME)
  expect(mentionsIn('@*backend* @*backend*', dir)).toEqual([
    Mention.GroupMention({ name: decodeGroupNameSync('backend') }),
  ])
})

test('does not count a group mention inside a code span', () => {
  const dir = directoryFor(GRAEME)
  expect(mentionsIn('the group form is `@*backend*`', dir)).toEqual([])
})

test('a silent mention notifies nobody, so it is not a mention', () => {
  const dir = directoryFor(GRAEME)
  // Zulip's @_ prefix renders a link without notifying (MENTIONS_RE `silent`).
  expect(mentionsIn('as @_**Graeme** noted, and @_*backend* too', dir)).toEqual([])
})

test('reports every form in one body, in the order written', () => {
  const dir = directoryFor(GRAEME)
  expect(mentionsIn('@**Graeme** @**all** @**topic** @*backend*', dir)).toEqual([
    Mention.UserMention({ identity: GRAEME }),
    Mention.ChannelWildcardMention(),
    Mention.TopicWildcardMention(),
    Mention.GroupMention({ name: decodeGroupNameSync('backend') }),
  ])
})

// The two paths read one tokeniser, so a form cannot be acted on by one and be
// invisible to the other. A group is recognised by both — reported inbound, and
// never mistaken for a dead user token on the way out.
test('unresolvedMentions does not report a group mention as a dead form', () => {
  const dir = directoryFor(GRAEME)
  expect(unresolvedMentions('@*backend* ship it', dir)).toEqual([])
})

test('unresolvedMentions still reports a dead form alongside a group mention', () => {
  const dir = directoryFor(GRAEME)
  expect(unresolvedMentions('@*backend* and @**Graeme Foster**', dir)).toEqual(['Graeme Foster'])
})

test('mentionsIdentity treats a wildcard as reaching anyone who sees the message', () => {
  const dir = directoryFor(GRAEME)
  // HERMES is nowhere in the body; the channel wildcard still reaches them.
  expect(mentionsIdentity(mentionsIn('@**all** standup', dir), HERMES.id)).toBe(true)
  expect(mentionsIdentity(mentionsIn('@**topic** standup', dir), HERMES.id)).toBe(true)
})

test('mentionsIdentity does not claim a group mention reaches an unknown member', () => {
  const dir = directoryFor(GRAEME)
  // Membership is not on the message and commy does not resolve it, so a match
  // here would be a guess. Tracked as follow-up, not answered by inventing one.
  expect(mentionsIdentity(mentionsIn('@*backend* ship it', dir), HERMES.id)).toBe(false)
})
