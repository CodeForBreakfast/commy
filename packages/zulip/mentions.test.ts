import { expect, test } from 'bun:test'
import type { Identity } from '@commy/core/ports'
import { decodeDisplayNameSync, decodeIdentityIdSync } from '@commy/core/ports'
import { extractMentions, type MentionDirectory, unresolvedMentions } from './mentions.ts'

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
  const mentions = extractMentions('oi @**Graeme** and @**hermes-agent** look', dir)
  expect(mentions.map((m) => m.id)).toEqual([GRAEME.id, HERMES.id])
})

test('does not count a mention inside an inline code span', () => {
  const dir = directoryFor(GRAEME)
  // The exact shape that produced the phantom-mention dispute (msg 17982):
  // the token only appears inside backticks, as an example in a sentence.
  const mentions = extractMentions('use `@**Graeme**`, not the dead form', dir)
  expect(mentions).toEqual([])
})

test('does not count a mention inside a multi-backtick code span', () => {
  const dir = directoryFor(GRAEME)
  const mentions = extractMentions('here: ``@**Graeme**`` is literal', dir)
  expect(mentions).toEqual([])
})

test('does not count a mention inside a fenced code block', () => {
  const dir = directoryFor(GRAEME)
  const content = ['before', '```', 'ping @**Graeme** here', '```', 'after'].join('\n')
  expect(extractMentions(content, dir)).toEqual([])
})

test('counts a real mention even when the message also discusses one in code', () => {
  const dir = directoryFor(GRAEME)
  // A real ping AND a code-span example in the same body — only the real one counts.
  const mentions = extractMentions('@**Graeme** — never write `@**Graeme**` in a code span', dir)
  expect(mentions.map((m) => m.id)).toEqual([GRAEME.id])
})

test('deduplicates repeated mentions of the same identity', () => {
  const dir = directoryFor(GRAEME)
  const mentions = extractMentions('@**Graeme** @**Graeme** @**Graeme**', dir)
  expect(mentions.map((m) => m.id)).toEqual([GRAEME.id])
})

test('resolves a disambiguated @**Name|id** mention to the right identity', () => {
  const dir = directoryFor(ROBIN_A, ROBIN_B)
  // byName resolves "Robin Reyes" to ROBIN_B (last writer); the |id form must
  // pin ROBIN_A specifically.
  const mentions = extractMentions('hey @**Robin Reyes|11** over here', dir)
  expect(mentions.map((m) => m.id)).toEqual([ROBIN_A.id])
})

test('drops a mention whose token resolves to no known identity', () => {
  const dir = directoryFor(GRAEME)
  // The dead form: @**Graeme Foster** no longer exists in the directory.
  const mentions = extractMentions('decision for @**Graeme Foster** to make', dir)
  expect(mentions).toEqual([])
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
