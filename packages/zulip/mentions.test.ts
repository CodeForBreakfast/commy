import { expect, test } from 'bun:test'
import type { Identity } from '@commy/core/ports'
import { decodeDisplayNameSync, decodeIdentityIdSync } from '@commy/core/ports'
import {
  type MentionDirectory,
  MentionToken,
  mentionTokens,
  unresolvedMentions,
} from './mentions.ts'

// The read path no longer asks this module WHO a body mentions — Zulip's own
// render answers that (rendered-mentions.ts). What survives here is the
// candidate test: does this body carry a mention sigil at all, and is it
// therefore worth one request to ask Zulip? The property that matters is that
// it never false-negatives — every form Zulip would deliver has to reach the
// render — and that it declines cheaply on the shapes that provably deliver
// nothing.
const candidates = (content: string): ReadonlyArray<MentionToken> => mentionTokens(content)

const isCandidate = (content: string): boolean => candidates(content).length > 0

const identity = (id: string, name: string, kind: Identity['kind'] = 'human'): Identity => ({
  id: decodeIdentityIdSync(id),
  name: decodeDisplayNameSync(name),
  kind,
})

const GRAEME = identity('7', 'Graeme')
const HERMES = identity('9', 'hermes-agent', 'agent')
// Two humans share a full_name — Zulip disambiguates them as @**Robin Reyes|11**.
const ROBIN_A = identity('11', 'Robin Reyes')

const directoryFor = (...identities: ReadonlyArray<Identity>): MentionDirectory => {
  const byId = new Map(identities.map((i) => [Number(i.id), i]))
  return {
    // Name-keyed: two same-named humans collide, so byName holds only the last.
    byName: new Map(identities.map((i) => [i.name as string, i])),
    byUserId: (userId) => byId.get(userId),
  }
}

test('treats every user mention in a body as a candidate', () => {
  expect(candidates('oi @**Graeme** and @**hermes-agent** look')).toEqual([
    MentionToken.UserToken({ token: 'Graeme' }),
    MentionToken.UserToken({ token: 'hermes-agent' }),
  ])
})

test('does not spend a request on a mention inside an inline code span', () => {
  // The exact shape that produced the phantom-mention dispute (msg 17982):
  // the token only appears inside backticks, as an example in a sentence.
  expect(isCandidate('use `@**Graeme**`, not the dead form')).toBe(false)
})

test('does not spend a request on a mention inside a multi-backtick code span', () => {
  expect(isCandidate('here: ``@**Graeme**`` is literal')).toBe(false)
})

test('does not spend a request on a mention inside a fenced code block', () => {
  const content = ['before', '```', 'ping @**Graeme** here', '```', 'after'].join('\n')
  expect(isCandidate(content)).toBe(false)
})

test('still asks Zulip when a body carries both a real mention and a coded example', () => {
  expect(candidates('@**Graeme** — never write `@**Graeme**` in a code span')).toEqual([
    MentionToken.UserToken({ token: 'Graeme' }),
  ])
})

test('does not spend a request on a silent mention, which notifies nobody', () => {
  // Zulip's @_ prefix renders a link without notifying (MENTIONS_RE `silent`).
  expect(isCandidate('as @_**Graeme** noted, and @_*backend* too')).toBe(false)
})

// Zero false negatives is the whole contract: a form that fails to become a
// candidate never reaches the render, and no later stage can recover it.
test.each([
  'all',
  'everyone',
  'stream',
  'channel',
  'topic',
])('treats the @**%s** wildcard as a candidate', (wildcard) => {
  expect(isCandidate(`heads up @**${wildcard}** — deploying`)).toBe(true)
})

test('treats a group mention as a candidate despite its single-asterisk markup', () => {
  expect(candidates('@*backend* please review')).toEqual([
    MentionToken.GroupToken({ name: 'backend' }),
  ])
})

test('treats a token naming nobody as a candidate — resolution is not its job', () => {
  // The candidate filter must not try to resolve. @**Graeme Foster** may be
  // dead, or may be someone this seat's directory has not seen; only the
  // render knows, and it only gets asked if this says yes.
  expect(isCandidate('decision for @**Graeme Foster** to make')).toBe(true)
})

test('treats a disambiguated @**Name|id** token as a candidate', () => {
  expect(candidates('hey @**Robin Reyes|11** over here')).toEqual([
    MentionToken.UserToken({ token: 'Robin Reyes|11' }),
  ])
})

test('recognises every form in one body, in the order written', () => {
  expect(candidates('@**Graeme** @**all** @**topic** @*backend*')).toEqual([
    MentionToken.UserToken({ token: 'Graeme' }),
    MentionToken.ChannelWildcardToken(),
    MentionToken.TopicWildcardToken(),
    MentionToken.GroupToken({ name: 'backend' }),
  ])
})

test('a body with no mention markup costs nothing', () => {
  expect(isCandidate('just a sentence about deployment')).toBe(false)
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
