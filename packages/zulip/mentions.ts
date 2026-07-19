import type { Identity } from '@commy/core/ports'

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
const MENTION_PATTERN = /@\*\*([^*]+)\*\*/g

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
 * (read Zulip's own delivery signal rather than inferring it). Best-effort and
 * strictly better than a markdown-blind regex, not authoritative.
 *
 * What this explicitly does NOT handle: wildcard mentions (@**all** /
 * **everyone** / **channel** / **topic**) resolve to no `byName` entry — they
 * are not users — so they are dropped here, a false negative Zulip delivers to
 * every subscriber. Fixing it needs port-level modelling (a wildcard is not an
 * `Identity`); tracked separately as comms-6fqc.
 */
const CODE_REGIONS = /```[\s\S]*?```|~~~[\s\S]*?~~~|(`+)[\s\S]*?\1/g
const withoutCode = (content: string): string => content.replace(CODE_REGIONS, ' ')

/**
 * The `@**Name**` / `@**Name|id**` tokens in a body that Zulip would treat as
 * mention attempts — code regions removed. The raw token text (inner capture),
 * not yet resolved: the write path validates these against the directory, the
 * read path resolves them.
 */
export const mentionTokens = (content: string): ReadonlyArray<string> => {
  const tokens: string[] = []
  for (const match of withoutCode(content).matchAll(MENTION_PATTERN)) {
    const token = match[1]
    if (token !== undefined) tokens.push(token)
  }
  return tokens
}

/**
 * Zulip's channel- and topic-wide wildcard mentions (zerver/lib/mention.py:
 * `stream_wildcards` all/everyone/stream/channel plus the `topic` wildcard,
 * matched case-sensitively). They share the `@**...**` sigil with a personal
 * mention but are not users, so `byName` never resolves them. They are
 * legitimate, deliverable constructs — not dead forms — so the write path must
 * not count them as unresolved. Reporting them in `mentions[]` needs a type
 * wider than `ReadonlyArray<Identity>`; that read-path modelling is comms-6fqc.
 */
const WILDCARD_MENTIONS: ReadonlySet<string> = new Set([
  'all',
  'everyone',
  'stream',
  'channel',
  'topic',
])

const resolveToken = (token: string, directory: MentionDirectory): Identity | undefined => {
  const disambiguated = DISAMBIGUATED.exec(token)
  if (disambiguated?.groups !== undefined) {
    return directory.byUserId(Number(disambiguated.groups['id']))
  }
  return directory.byName.get(token)
}

/**
 * The identities a body actually mentions: markdown-aware tokens resolved
 * against the directory, deduped, order-preserving. Unresolvable tokens (a
 * dead `@**Name**`, an unknown `|id`) are dropped — Zulip renders no mention
 * for them either.
 */
export const extractMentions = (
  content: string,
  directory: MentionDirectory,
): ReadonlyArray<Identity> => {
  const results: Identity[] = []
  const seen = new Set<string>()
  for (const token of mentionTokens(content)) {
    const ident = resolveToken(token, directory)
    if (ident === undefined) continue
    if (seen.has(ident.id)) continue
    seen.add(ident.id)
    results.push(ident)
  }
  return results
}

/**
 * The mention tokens in an outbound body that resolve to no known identity —
 * deduped, order-preserving. A non-empty result is a dead form (e.g. a stale
 * `@**Graeme Foster**`) that Zulip would post verbatim and notify nobody for:
 * the write path rejects the post rather than delivering silence.
 */
export const unresolvedMentions = (
  content: string,
  directory: MentionDirectory,
): ReadonlyArray<string> => {
  const results: string[] = []
  const seen = new Set<string>()
  for (const token of mentionTokens(content)) {
    if (WILDCARD_MENTIONS.has(token)) continue
    if (resolveToken(token, directory) !== undefined) continue
    if (seen.has(token)) continue
    seen.add(token)
    results.push(token)
  }
  return results
}
