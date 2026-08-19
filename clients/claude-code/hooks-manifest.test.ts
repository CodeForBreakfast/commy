import { expect, test } from 'bun:test'

import hooksManifest from './hooks/hooks.json'

/**
 * The PreToolUse hook in `hooks/hooks.json` injects `session_id` (and `cwd`)
 * into MCP tool args before the call reaches the server. Its matcher is a
 * hand-curated alternation over tool names.
 *
 * WHAT THIS ASSERTS, and why it changed (comms-g5zh.1). It used to derive
 * "tools that need an identity" by scanning `tools.ts` for `ensureBoundFor(`
 * call sites — a hand-maintained trigger table in the tool layer. That table is
 * gone: the mint decision now lives at the adapter port, where reaching for a
 * bound credential (`boundHttp`) IS the declaration that an identity is needed.
 * So the derivation is traced from there instead, which is also what
 * `comms-tww6` specifies:
 *
 *   A TOOL WHOSE ADAPTER PATH REACHES `boundHttp` MUST RECEIVE `session_id`
 *   AND BE IN THE `hooks.json` MATCHER.
 *
 * That is a stronger rule than the old one. The old test could only catch a
 * tool that called the wrapper and was missing from the matcher; it could not
 * see a tool that reached `boundHttp` while appearing in neither set — which is
 * exactly the live P1 that `comms-tww6` is open about.
 *
 * RECEIVES, NOT DECLARES (comms-tg70). The rule used to say DECLARE, and traced
 * a `session_id` property on the tool's advertised `inputSchema`. No tool
 * declares one now: `session_id` is host plumbing the model has no way to fill,
 * so it is supplied into `arguments` and accepted by the guard without being
 * advertised (`ToolDef.hostSuppliedArgs`). What a bound-path tool must have is
 * unchanged in substance — the id has to REACH it — so the trace moved to the
 * accept-side marker.
 *
 * THAT MOVE IS WHY {@link SESSION_ID_RECEIVING_TOOLS} EXISTS. A test that
 * derives a property from a source scan goes green when the scan stops matching
 * — every rule below reads "no tool violates it" and a scan finding nothing
 * satisfies all of them by looking at nothing. Pinning the receiving set makes
 * the scan itself the thing under test: rename the marker and the pin fails
 * loudly instead of the suite passing quietly.
 *
 * KNOWN VIOLATIONS ARE NAMED, NOT PAPERED OVER. Three tools violate the rule at
 * HEAD (see `TWW6_EXCEPTIONS`). Fixing them means giving them the host-supplied
 * `session_id`, which is out of scope here. Encoding the real rule with a
 * visible exception list beats asserting a weaker rule that passes: the day
 * `comms-tww6` lands, its author deletes entries from that list and this test
 * proves the fix.
 */

/**
 * Publisher verbs whose adapter implementation reaches `boundHttp`. Pinned
 * rather than parsed: two of them reach it through shared helpers
 * (`setThreadResolved`, `setChannelDescription`), which no line-wise scan
 * resolves honestly. `adapterVerbsReachingBoundHttp` below guards the pin, so a
 * verb that joins or leaves the seam fails this suite rather than silently
 * widening the set a tool has to be stamped for.
 */
const BOUND_VERBS = ['post', 'edit', 'react', 'unreact'] as const

/**
 * Declarations in `packages/zulip/adapter.ts` that call `boundHttp()`. The two
 * helpers here back four port verbs: `setThreadResolved` implements
 * `resolveThread` / `unresolveThread`, and `setChannelDescription` is its own
 * verb.
 */
const BOUND_HTTP_CALLERS = [
  'edit',
  'post',
  'react',
  'setChannelDescription',
  'setThreadResolved',
  'subscribe',
  'subscriptions',
  'unreact',
  'unsubscribe',
  'uploadFile',
] as const

/**
 * Inbox verbs whose adapter implementation reaches `boundHttp` (comms-g5zh.2 /
 * .3). Declaring interest writes realm state under the seat's own principal —
 * a subscription row and the event queue that delivers against it — so these
 * bind for the same reason the publisher verbs do.
 *
 * `subscriptions` (comms-g5zh.5) is the one READ in this set, and it belongs
 * here for a different reason than the writes: it asks what the realm holds
 * FOR THIS SEAT, so it can only be answered by the seat's own credential. The
 * minter's answer would be a different seat's subscriptions wearing this one's
 * name. It never causes a mint, though — its only caller checks that the seat
 * is already bound, or has grounds to believe its bot exists, before asking.
 *
 * Held apart from {@link BOUND_VERBS} because the tool-side trace resolves them
 * through a different receiver (`adapter.inbox.*`, not `adapter.publisher.*`).
 * That distinction is the whole reason the pre-existing guard could not see
 * them: it compared over publisher verbs alone, so a binding inbox verb sat
 * outside the compared set entirely and the suite stayed green.
 */
const BOUND_INBOX_VERBS = ['subscribe', 'subscriptions', 'unsubscribe'] as const

/**
 * Adapter ATTACHMENT verbs that reach `boundHttp` (comms-qpup). An upload
 * writes an `Attachment` row with an owner, and Zulip grants a reader access to
 * it only when that owner is also the sender of the referencing message — so
 * the upload goes out under the seat's own bot, like every other write.
 *
 * Held apart from the two sets above for the same reason they are held apart
 * from each other: the tool layer reaches these through a THIRD receiver. It
 * never names `adapter.uploadFile` — it receives a closure as a dep — which is
 * why the trace needs {@link ATTACHMENT_DEP_ADAPTER_MEMBER} to see this path at
 * all.
 */
const BOUND_ATTACHMENT_VERBS = ['uploadFile'] as const

/**
 * Which adapter member backs each attachment dep in `RegisterToolsDeps`, as
 * `server.ts` wires them.
 *
 * PINNED, NOT PARSED, and the limit of that is worth stating: nothing here
 * reads `server.ts`, so a rewire of `upload` onto a different member would
 * leave this map lying. Every other pin in this file carries the same limit
 * (see `BOUND_VERBS`), and resolving a closure across a second source file is a
 * different kind of test than this one. What the map buys is the thing that was
 * missing: a tool whose only route to `boundHttp` is a dep now sits INSIDE the
 * compared set instead of outside it.
 */
const ATTACHMENT_DEP_ADAPTER_MEMBER: Readonly<Record<string, string>> = {
  upload: 'uploadFile',
  downloadFile: 'downloadFile',
}

/**
 * Tools that reach `boundHttp` through an inbox verb while sitting outside the
 * matcher, so the hook never stamps them and the bind seam sees no session id.
 *
 * EMPTY, and it has to stay that way. An unstamped inbox verb is not a
 * `comms-tww6`-style attribution accident — it cannot inherit an earlier
 * call's seat, because `boundHttp` consults the binder on every call and
 * refuses outright when the context carries no session id. It simply FAILS.
 *
 * The matcher was widened to the seven tools that declare `session_id`, which
 * REVERSES commit `0f0e755` (PR #126) — that commit chose id-blind subscribe
 * and explicitly declined to add these two. The reversal is deliberate and
 * ratified: `#126`'s choice served the shared-minter architecture, where
 * subscribe wrote under the minter and needed no identity of its own.
 * `comms-g5zh.3` retires that architecture — subscribe now mints — so the
 * premise `#126` rested on is gone.
 */
const G5ZH3_MATCHER_PENDING = [] as const

/**
 * Tools that reach `boundHttp` while receiving no `session_id` and sitting
 * outside the matcher — the open P1 `comms-tww6`. They run under whatever seat
 * an EARLIER call happened to bind, so their attribution is inherited by
 * accident of ordering rather than established by the call itself.
 *
 * Delete an entry here when that tool gains `session_id`; the assertion below
 * then holds it to the rule.
 */
const TWW6_EXCEPTIONS = ['resolve_thread', 'set_channel_description', 'unresolve_thread'] as const

/**
 * Every tool that accepts a host-supplied `session_id`. EIGHT, the same eight
 * the PreToolUse matcher stamps today — but the two sets answer different
 * questions and are allowed to diverge again: `subscribe` and `unsubscribe`
 * are here for a non-CC ephemeral host that supplies the UUID itself, and a
 * listen-first seat reaches an identity through no other tool. Do not read a
 * statement about one of these sets as a statement about the other.
 *
 * Pinned, not derived, and that is the point — see the file header. This set is
 * what proves the scan below still sees anything at all.
 */
const SESSION_ID_RECEIVING_TOOLS = [
  'current_identity',
  'edit_message',
  'post',
  'react',
  'subscribe',
  'unreact',
  'unsubscribe',
  'upload_file',
] as const

/** Enclosing declaration names in the adapter source that call `boundHttp()`. */
function adapterVerbsReachingBoundHttp(source: string): ReadonlySet<string> {
  const reaching = new Set<string>()
  let current: string | undefined
  for (const line of source.split('\n')) {
    // `      post: (channel, body, opts?) => {` or `    const setThreadResolved = (`
    const named =
      line.match(/^ {6}([a-zA-Z]+): \(/)?.[1] ?? line.match(/^ {4}const ([a-zA-Z]+) = \(/)?.[1]
    if (named !== undefined) current = named
    // Skip prose: several comments name `boundHttp()` to explain why a path
    // deliberately does NOT use it.
    if (
      current !== undefined &&
      line.includes('boundHttp()') &&
      !line.trimStart().startsWith('//')
    ) {
      reaching.add(current)
    }
  }
  return reaching
}

interface ToolFacts {
  readonly verbs: ReadonlySet<string>
  readonly inboxVerbs: ReadonlySet<string>
  readonly attachmentVerbs: ReadonlySet<string>
  readonly receivesSessionId: boolean
  readonly advertisesSessionId: boolean
}

/**
 * `const <alias> = deps.<field>` bindings in the tools source — the hop that
 * hides an attachment tool's adapter path from a line-wise scan. `upload_file`'s
 * handler calls `upload(path)`; only this binding says `upload` is
 * `deps.upload`, and only {@link ATTACHMENT_DEP_ADAPTER_MEMBER} says
 * `deps.upload` is `adapter.uploadFile`.
 */
function depAliases(source: string): ReadonlyMap<string, string> {
  const aliases = new Map<string, string>()
  for (const line of source.split('\n')) {
    const m = line.match(/^ {4}const ([a-zA-Z]+) = deps\.([a-zA-Z]+)$/)
    const alias = m?.[1]
    const field = m?.[2]
    if (alias !== undefined && field !== undefined) aliases.set(alias, field)
  }
  return aliases
}

/**
 * Per-tool: which publisher verbs, inbox verbs and attachment verbs its handler
 * reaches, whether it accepts a host-supplied `session_id`, and whether it
 * advertises one on its `inputSchema`. The last is traced only so the
 * assertions can show it is nowhere — the two facts are separate and stay
 * separately measured.
 */
function toolFactsFromToolsSource(source: string): ReadonlyMap<string, ToolFacts> {
  const facts = new Map<
    string,
    {
      verbs: Set<string>
      inboxVerbs: Set<string>
      attachmentVerbs: Set<string>
      receivesSessionId: boolean
      advertisesSessionId: boolean
    }
  >()
  const aliases = depAliases(source)
  let current: string | undefined
  for (const line of source.split('\n')) {
    const named = line.match(/^ {6}name: '([a-z_]+)',$/)?.[1]
    if (named !== undefined) {
      current = named
      facts.set(named, {
        verbs: new Set(),
        inboxVerbs: new Set(),
        attachmentVerbs: new Set(),
        receivesSessionId: false,
        advertisesSessionId: false,
      })
    }
    const entry = current === undefined ? undefined : facts.get(current)
    if (entry === undefined) continue
    for (const verb of line.matchAll(/adapter\.publisher\.(\w+)/g)) {
      const captured = verb[1]
      if (captured !== undefined) entry.verbs.add(captured)
    }
    // Inbox verbs bind too (comms-g5zh.2/.3). Traced separately because the
    // receiver differs; tracing only `adapter.publisher.*` is precisely how a
    // binding verb stayed outside this suite's compared set.
    for (const verb of line.matchAll(/adapter\.inbox\.(\w+)/g)) {
      const captured = verb[1]
      if (captured !== undefined) entry.inboxVerbs.add(captured)
    }
    // Attachment verbs bind too (comms-qpup). Traced through the dep alias
    // because the tool layer never names the adapter member: `upload_file`
    // calls a closure `server.ts` handed it. Tracing only the two receivers
    // above is how a binding tool would sit outside this suite's compared set —
    // the comms-65nj failure this file's header names.
    for (const [alias, field] of aliases) {
      const member = ATTACHMENT_DEP_ADAPTER_MEMBER[field]
      if (member === undefined) continue
      // Skip the binding line itself; it sits between two tool defs and would
      // otherwise be attributed to whichever one the scan is currently inside.
      if (new RegExp(`^ {4}const ${alias} = deps\\.`).test(line)) continue
      if (new RegExp(`(?<![\\w.])${alias}(?![\\w])`).test(line)) entry.attachmentVerbs.add(member)
    }
    // The accept-side marker: an argument the host stamps in, admitted by the
    // guard in `registerTools` and absent from `inputSchema`.
    if (line.includes('hostSuppliedArgs: hostSuppliedSessionId')) entry.receivesSessionId = true
    // The advertise-side property, which no tool should have (comms-tg70).
    if (/^ +session_id: /.test(line)) entry.advertisesSessionId = true
  }
  return new Map(
    [...facts].map(([name, e]) => [
      name,
      { ...e, verbs: e.verbs, inboxVerbs: e.inboxVerbs, attachmentVerbs: e.attachmentVerbs },
    ]),
  )
}

function alternationToolsFromMatcher(matcher: string): ReadonlySet<string> {
  const captured = matcher.match(/\(([\w|]+)\)$/)?.[1]
  if (captured === undefined) {
    throw new Error(`matcher missing trailing alternation group: ${matcher}`)
  }
  return new Set(captured.split('|'))
}

interface PreToolUseEntry {
  readonly matcher: string
  readonly hooks: ReadonlyArray<{ readonly args?: ReadonlyArray<string> }>
}

function injectSessionIdMatcher(manifest: typeof hooksManifest): string {
  const entries: ReadonlyArray<PreToolUseEntry> = manifest.hooks.PreToolUse ?? []
  const found = entries.find((entry) =>
    entry.hooks.some((h) => (h.args ?? []).some((a) => a.includes('inject-session-id.ts'))),
  )
  if (found === undefined) {
    throw new Error('no PreToolUse hook found that runs inject-session-id.ts')
  }
  return found.matcher
}

const adapterSource = (): Promise<string> =>
  Bun.file(Bun.resolveSync('@commy/zulip/adapter', import.meta.dir)).text()

const toolsSource = (): Promise<string> =>
  Bun.file(Bun.resolveSync('@commy/mcp/tools', import.meta.dir)).text()

test('the set of adapter declarations reaching boundHttp is the pinned one', async () => {
  expect([...adapterVerbsReachingBoundHttp(await adapterSource())].sort()).toEqual([
    ...BOUND_HTTP_CALLERS,
  ])
})

// The pin that keeps the four rules below from holding by not looking. Each of
// them reads "no tool violates this", which a scan that matches nothing
// satisfies trivially — so assert first that the scan finds the set it is
// supposed to find.
test('the tools accepting a host-supplied session_id are exactly the pinned eight', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const receiving = [...facts]
    .filter(([, f]) => f.receivesSessionId)
    .map(([name]) => name)
    .sort()
  expect(receiving).toEqual([...SESSION_ID_RECEIVING_TOOLS])
})

// comms-tg70: `session_id` is supplied, never advertised. A human does not type
// their session id into the compose box (docs/agent-experience.md principle 1),
// and the model has no way to fill the field, so putting it on the schema only
// ever put plumbing on the agent's surface.
test('no tool advertises session_id on its inputSchema', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const advertising = [...facts]
    .filter(([, f]) => f.advertisesSessionId)
    .map(([name]) => name)
    .sort()
  expect(advertising).toEqual([])
})

test('every tool whose adapter path reaches boundHttp receives session_id', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const offenders = [...facts]
    .filter(
      ([, f]) =>
        [...f.verbs].some((v) => (BOUND_VERBS as ReadonlyArray<string>).includes(v)) &&
        !f.receivesSessionId,
    )
    .map(([name]) => name)
    .sort()
  expect(offenders).toEqual([])
})

test('every tool whose adapter path reaches boundHttp is in the PreToolUse matcher', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const matched = alternationToolsFromMatcher(injectSessionIdMatcher(hooksManifest))
  const missing = [...facts]
    .filter(([, f]) => [...f.verbs].some((v) => (BOUND_VERBS as ReadonlyArray<string>).includes(v)))
    .map(([name]) => name)
    .filter((name) => !matched.has(name))
    .sort()
  expect(missing).toEqual([])
})

// The same rule again, over the ATTACHMENT verbs that began binding with
// comms-qpup. Its own assertion with its own named set, for the reason the
// inbox rule has one: a rule that compares over a set which no longer covers
// every binding path goes green by not looking.
test('every tool whose adapter path reaches boundHttp via an attachment dep receives session_id', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const offenders = [...facts]
    .filter(
      ([, f]) =>
        [...f.attachmentVerbs].some((v) =>
          (BOUND_ATTACHMENT_VERBS as ReadonlyArray<string>).includes(v),
        ) && !f.receivesSessionId,
    )
    .map(([name]) => name)
    .sort()
  expect(offenders).toEqual([])
})

test('every tool whose adapter path reaches boundHttp via an attachment dep is in the matcher', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const matched = alternationToolsFromMatcher(injectSessionIdMatcher(hooksManifest))
  const missing = [...facts]
    .filter(([, f]) =>
      [...f.attachmentVerbs].some((v) =>
        (BOUND_ATTACHMENT_VERBS as ReadonlyArray<string>).includes(v),
      ),
    )
    .map(([name]) => name)
    .filter((name) => !matched.has(name))
    .sort()
  expect(missing).toEqual([])
})

// The pin that keeps the two rules above from holding by not looking. The
// attachment trace runs through an alias hop, so it has more ways to stop
// matching than the other two — assert it still finds the tool it is for.
test('the tools reaching a bound attachment verb are exactly upload_file', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const reaching = [...facts]
    .filter(([, f]) =>
      [...f.attachmentVerbs].some((v) =>
        (BOUND_ATTACHMENT_VERBS as ReadonlyArray<string>).includes(v),
      ),
    )
    .map(([name]) => name)
    .sort()
  expect(reaching).toEqual(['upload_file'])
})

// The same rule, stated over the INBOX verbs that began binding with
// comms-g5zh.2/.3. Kept as its own assertion with its own named list so the
// publisher-side rule above cannot go green on a set that no longer covers
// every binding path — the failure mode comms-65nj recorded, where a guard
// filed in May 2026 against exactly this drift stayed green for eight months
// because its compared set was scoped one level too low.
test('the tools that bind via an inbox verb but are unstamped are exactly the recorded set', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const matched = alternationToolsFromMatcher(injectSessionIdMatcher(hooksManifest))
  const unstamped = [...facts]
    .filter(([, f]) =>
      [...f.inboxVerbs].some((v) => (BOUND_INBOX_VERBS as ReadonlyArray<string>).includes(v)),
    )
    .map(([name]) => name)
    .filter((name) => !matched.has(name))
    .sort()
  expect(unstamped).toEqual([...G5ZH3_MATCHER_PENDING])
})

// The rule stated over ALL bound verbs, including the two helper-backed ones
// the tool layer never stamps. This is the assertion `comms-tww6` closes.
test('comms-tww6: the known unstamped bound-path tools are exactly the recorded exceptions', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const boundHttpVerbs = new Set(['resolveThread', 'unresolveThread', 'setChannelDescription'])
  const unstamped = [...facts]
    .filter(([, f]) => [...f.verbs].some((v) => boundHttpVerbs.has(v)) && !f.receivesSessionId)
    .map(([name]) => name)
    .sort()
  expect(unstamped).toEqual([...TWW6_EXCEPTIONS])
})

test('the matcher carries no tool that never reaches boundHttp and never binds', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const matched = alternationToolsFromMatcher(injectSessionIdMatcher(hooksManifest))
  // `current_identity` is in the matcher without reaching boundHttp: it reads
  // the binding passively and feeds the session-id deferred by asking. That is
  // a legitimate reason to be stamped, so it is named rather than derived.
  const orphans = [...matched]
    .filter((name) => name !== 'current_identity')
    .filter((name) => {
      const f = facts.get(name)
      if (f === undefined) return true
      // Any receiver counts. A tool binds through the publisher verbs, the
      // inbox verbs or an attachment dep; asking only about the first would
      // call a legitimately-stamped `subscribe` an orphan.
      return (
        ![...f.verbs].some((v) => (BOUND_VERBS as ReadonlyArray<string>).includes(v)) &&
        ![...f.inboxVerbs].some((v) => (BOUND_INBOX_VERBS as ReadonlyArray<string>).includes(v)) &&
        ![...f.attachmentVerbs].some((v) =>
          (BOUND_ATTACHMENT_VERBS as ReadonlyArray<string>).includes(v),
        )
      )
    })
    .sort()
  expect(orphans).toEqual([])
})

test('adapterVerbsReachingBoundHttp ignores commented-out mentions of boundHttp', () => {
  const synthetic = `
      editingAvailable: () =>
        // Read through the MINTER, never \`boundHttp()\`: sampled pre-acquire.
        minterHttp.post('/register'),
      react: (message, emoji) =>
        boundHttp().pipe(Effect.flatMap((http) => http.post('/x'))),
  `
  expect(adapterVerbsReachingBoundHttp(synthetic)).toEqual(new Set(['react']))
})

test('toolFactsFromToolsSource attributes verbs and session_id to the enclosing tool', () => {
  const synthetic = `
      name: 'alpha',
      hostSuppliedArgs: hostSuppliedSessionId,
      handler: async (args) => {
        await run(adapter.publisher.post(channel, body))
      },
      name: 'beta',
      handler: async () => {
        await run(adapter.history.readChannel(channel))
      },
      name: 'gamma',
      handler: async () => {
        await run(adapter.inbox.subscribe(target))
      },
      name: 'delta',
      inputSchema: {
        properties: {
          session_id: sessionIdField,
        },
      },
    const upload = deps.upload
      name: 'epsilon',
      hostSuppliedArgs: hostSuppliedSessionId,
      handler: async (args) => {
        const result = await run(upload(path))
      },
  `
  const facts = toolFactsFromToolsSource(synthetic)
  expect(facts.get('alpha')).toEqual({
    verbs: new Set(['post']),
    inboxVerbs: new Set(),
    attachmentVerbs: new Set(),
    receivesSessionId: true,
    advertisesSessionId: false,
  })
  expect(facts.get('beta')).toEqual({
    verbs: new Set(),
    inboxVerbs: new Set(),
    attachmentVerbs: new Set(),
    receivesSessionId: false,
    advertisesSessionId: false,
  })
  // A read through the inbox is still traced as an inbox verb here; whether it
  // BINDS is decided by `BOUND_INBOX_VERBS`, not by the receiver.
  expect(facts.get('gamma')).toEqual({
    verbs: new Set(),
    inboxVerbs: new Set(['subscribe']),
    attachmentVerbs: new Set(),
    receivesSessionId: false,
    advertisesSessionId: false,
  })
  // The advertise-side trace catches a schema property coming back, and does
  // not confuse it with the accept-side marker. `delta` also sits immediately
  // before the `const upload = deps.upload` binding, so its empty
  // `attachmentVerbs` is what proves the binding line is not attributed to
  // whichever tool the scan is currently inside.
  expect(facts.get('delta')).toEqual({
    verbs: new Set(),
    inboxVerbs: new Set(),
    attachmentVerbs: new Set(),
    receivesSessionId: false,
    advertisesSessionId: true,
  })
  // The alias hop, end to end: `upload` resolves through the binding to
  // `deps.upload`, and `ATTACHMENT_DEP_ADAPTER_MEMBER` resolves that to
  // `adapter.uploadFile` — the member the tool source never names.
  expect(facts.get('epsilon')).toEqual({
    verbs: new Set(),
    inboxVerbs: new Set(),
    attachmentVerbs: new Set(['uploadFile']),
    receivesSessionId: true,
    advertisesSessionId: false,
  })
})

test('alternationToolsFromMatcher splits the trailing parenthesised group', () => {
  expect(alternationToolsFromMatcher('mcp__plugin_commy_commy__(post|edit_message)')).toEqual(
    new Set(['post', 'edit_message']),
  )
})

test('alternationToolsFromMatcher throws when the matcher has no alternation group', () => {
  expect(() => alternationToolsFromMatcher('mcp__plugin_commy_commy__post')).toThrow(
    /missing trailing alternation group/,
  )
})
