---
name: effect-native-audit
description: Use when asked to audit this codebase for places we hand-rolled what Effect ships natively, or to run the Effect-native sweep. Triggers on "effect-native audit", "where are we reinventing Effect", "rolled our own instead of an Effect builtin", "sweep for native Effect helpers", "audit our FP/Effect modelling", "audit our concurrency/error handling". A workflow-driven structural, modelling & behaviour audit — not a substitute for the @effect/language-service idiom linter.
---

# Effect-native capability audit

Sweep the codebase on **four axes** for code that isn't Effect-native — three
**static** (non-native code that is *present*) and one **dynamic** (native code
*misused*):

- **Substitution** (verbs) — logic that re-implements what Effect already ships:
  a retry loop that is `Schedule`, manual env parsing that is `Config`, custom
  equality that is `Equal`/`Data`, a list helper already in `Array`. *Where did
  we hand-roll a helper?*
- **Structural** (wiring) — code whose construction / dependency-injection /
  error / resource **spine** is still imperative OOP even though the leaves
  return Effect: a `new`-able class doing DI (ZulipHttp), a `throw` inside an
  Effect-returning method, a dependency threaded as a config field instead of
  declared in `R`, a `try/finally` that wants `acquireRelease`. *Effect used
  inside functions ≠ an Effect-native program.* The substitution axis is blind
  here — no module's export inventory is named "don't write a class that does
  DI".
- **Modelling** (nouns) — data and state whose **type** throws away a guarantee
  an Effect data type would give for free: `T | null` that wants `Option`, a
  sentinel or pure `throw` that wants `Either`, booleans + optional fields that
  want a `Data.taggedEnum` (make illegal states unrepresentable), a bare `string`
  id that wants a branded type, in-place array mutation that wants an immutable
  build, hand-rolled equality that wants `Equal`/`Data`, a data-first helper that
  blocks clean `pipe` composition. The dishonesty is in the *representation*, not
  in a missing call or an imperative spine — so the substitution and structural
  axes walk past it.
- **Behaviour** (dynamics) — native-but-*misused* or policy code: how effects
  **run**, **fail**, and what they **trust**. Independent effects run sequentially
  that could be `Effect.all`-concurrent, an unbounded fan-out onto a shared service
  (the Zulip rate-limit hazard), a bare `Effect.fork` whose fiber leaks, untrusted
  data crossing the edge via `as`/`JSON.parse` with no `Schema` decode, an `E`
  channel typed `Error`/`unknown` or a `catchAll` that swallows a recoverable
  failure. The construct is already Effect-native — the smell is the *policy*. Each
  finder anchors on present code, not blanket absence ("no timeout anywhere").

This is the **structural, modelling & behaviour** complement to
`@effect/language-service`. The LSP (~60 rules, enforced at `tsc` time via
`bun run check`) owns per-line idiom: floating Effects, unnecessary `Effect.gen`,
`Effect.void`, global `fetch`/`Date`/`console`. It is blind to "this whole 40-line
block is a combinator that exists," "this whole module is shaped like an OOP
service," "this type lets the caller forget the absent case," and "this fan-out is
unbounded." Those blind spots are what this skill covers. **Never re-report what
the LSP already catches.**

## How it works

A `Workflow` script (`effect-native-audit.workflow.js`) fans out four kinds of
finder (see `domains.md`), all grounded in **local clones**: Effect source at
`~/Development/references/effect` and Effect docs at
`~/Development/references/effect-website` (GitHub-raw / WebFetch fallback when
absent).

**Substitution finders** (~23, one per Effect domain) work inventory-first:

1. List the module's *complete* export inventory from source — you cannot
   recognise a copy of `Array.partition` unless the whole `Array` surface is in
   your head.
2. Read the module's docs **section** for the *when & why* — the whole section,
   not just the intro page (Stream is six pages). API-reference-only modules
   (`Array`, `Record`, …) have no prose page and lean on source.
3. Scan our source files for logic that re-expresses a helper in that inventory.

**Structural finders** (~6, one per smell in the anti-pattern catalogue) work
smell-first: read the Effect *design* docs (`services`, `expected-errors`,
`scope`, `running-effects`, `the-effect-type`) for why the native shape wins and
when the imperative shape is justified, then grep an anchor pattern and read
each candidate to confirm the shape (a grep hit is not a finding).

**Modelling finders** (~7, one per representation smell) work smell-first too,
but grounded in the `code-style/*` + `data-types/*` docs (`option`, `either`,
`data`, `branded-types`, `chunk`, `trait/equal`, `dual`): they hunt the *type* of
data and state — null over `Option`, sentinels over `Either`, flags over a tagged
union, bare primitives over branded types, in-place mutation over immutable
structures, hand equality over `Equal`, data-first helpers over data-last. Three
of them deliberately border a structural or substitution smell (`sentinel-or-throw`
vs `throw-in-effect`, `in-place-mutation` vs `mutable-state`, `hand-equality`/
`nullable-return` vs the `Equal`/`Option` domains); each finder states its boundary
so the axes don't double-count.

**Behaviour finders** (~5, one per dynamic smell) work smell-first too, grounded in
the `concurrency/*`, `error-management/*`, and `schema/*` docs (`basic-concurrency`,
`fibers`, `two-error-types`, `getting-started`): they hunt how effects **run**
(sequential-not-concurrent, unbounded-fanout), **fail** (untyped-error-channel), and
what they **trust** (unvalidated-boundary, unsupervised-fork). Each anchors on a
*present* construct (an `Effect.all`/`fork`, an `as`/`JSON.parse`, a typed `E`) and
confirms a concrete misuse — for concurrency the data dependency between effects is
the whole question. Three border an existing axis (`unvalidated-boundary` vs
`internal-bridge`/`bare-primitive`, `untyped-error-channel` vs `throw-in-effect`,
`unsupervised-fork` vs `mutable-state`); each states its boundary.

Findings then flow — per-finding, no barrier — into an **adversarial verify**
pass: one refuter per finding. The axes split into **two opposite default
verdicts**, because their failure modes are opposite:

- **Substitution** — the refuter tries to *disprove the swap* (false twin) and
  **defaults to refuted** when unsure. A plausible-but-wrong helper swap is the
  risk, so the bar is "positively confirm the swap preserves behaviour."
- **Structural, modelling & behaviour** — the refuter asks only *is this a false
  positive — not actually a non-Effect-native shape, representation, or misuse?*
  and **defaults to surfaced** when unsure. It refutes only a misread or a
  genuinely-justified choice; "is the rewrite worth the blast radius" is a **gate**
  concern, not a refutation ground (else it kills true smells like ZulipHttp, which
  it agrees are real, on "not clearly worth it"). A surfaced false positive is
  cheaply cut at the gate; a refuted true smell is invisible. Two refuters carry an
  extra guard against over-flagging: `data-first-helper` kills a data-last rewrite
  that would be gratuitous point-free (composability is the goal, not currying); and
  `sequential-not-concurrent` refutes when the effects are genuinely data-dependent
  and so *must* be sequential.

A synthesis agent writes the report (deduping cross-axis sightings as it
reasons about the prose). The confirmed / low-confidence **bead partition** is
NOT trusted to that agent — the workflow derives it deterministically from each
survivor's own confidence (dedup by file:line, highest confidence wins, then
split). An LLM asked to losslessly re-split survivors drops and misfiles them.

## Running it

1. Get today's date (`date +%F`).
2. Launch the workflow, passing the date so the epic and report file are named:

   ```
   Workflow({
     scriptPath: '.claude/skills/effect-native-audit/effect-native-audit.workflow.js',
     args: { date: '<YYYY-MM-DD>' },
   })
   ```

   It runs in the background (~41 finders — 23 substitution + 6 structural + 7
   modelling + 5 behaviour — ~10 concurrent, then verify + synth); you are notified
   on completion. Watch live with `/workflows`. To run a single axis (cheaper re-run
   after editing its prompts, or to resume just one), pass
   `args: { date, axes: ['behaviour'] }`.

3. The workflow writes the report to `docs/effect-native-audit-<date>.md` and
   returns `{ confirmed[], lowConfidence[], refutedCount, coverage }`.

## After it returns — gate, then file (do NOT let the workflow file beads)

The workflow deliberately returns findings rather than writing to `bd`. A sweep
can surface ~25 items; auto-filing them straight into `bd ready` clogs the queue.

The report is **working scaffolding for this gate conversation, not a permanent
record** — beads are the durable artifact, so the report is binned once the
beads exist (step 4). Everything worth keeping must therefore land in a bead.

1. **Present** the `confirmed[]` list to Graeme — file:line, rolled → native,
   why. Also surface the `lowConfidence[]` items so he can promote any worth
   keeping; un-promoted ones are dropped when the report is binned.
2. **Wait for go-ahead.** Graeme may cut or downgrade confirmed items, or promote
   low-confidence ones, first.
3. On approval, **file under one tracking epic** so the items stay contained.
   Each bead's description must be self-contained (file:line, the rewrite, the
   why, source/line, docsRef) — it has to stand on its own once the report is
   gone. **Structural, modelling, and behaviour findings must carry their `blastRadius`** (the
   call sites a rewrite ripples to — a representation change moves every producer
   and consumer of the value) — that ripple is the real cost and the bead is
   unfileable without it. Do **not** add a `report: docs/...` pointer; that doc
   won't survive.

   ```bash
   bd create "Effect-native audit <YYYY-MM-DD>" --type epic
   # then per finding being filed (confirmed + any promoted low-confidence):
   bd create "<file>: <rolled> -> <native>" --type task \
     --parent <epic-id> \
     -d "<why, with docsRef and source/line; for structural/modelling/behaviour, the blastRadius>"
   ```

4. **Bin the report** — once the beads are generated, delete
   `docs/effect-native-audit-<date>.md`. The actionable record now lives in the
   epic tree; the coverage table and any dropped low-confidence items are
   process metadata, acceptable to lose.

Each filed item is its own native swap; implement them on their own beads with
TDD, not as a batch.

## Notes

- **Source AND docs are both mandatory.** For substitution, source gives the
  inventory (you can't grep `^export` from a prose page) and docs give the
  judgment; for structural, modelling, and behaviour, the design docs ARE the
  ground (the smell is a shape, a representation, or a dynamic policy, not an
  export). A finder with only one over-suggests or mis-verifies. See `domains.md`.
- **Two local clones.** Effect source at `~/Development/references/effect`; Effect
  docs at `~/Development/references/effect-website` (Astro/Starlight — the URL
  `…/docs/<slug>/` maps exactly to `…/docs/docs/<slug>.mdx`, and a multi-page
  section is a directory the finder reads in full). Both are workstation-local
  (recorded in `CLAUDE.local.md`); a cloud/CI agent falls back to GitHub-raw
  source + `WebFetch` docs, just slower. The coverage section flags any fallback.
- **Refresh on Effect bumps.** Source paths and docs slugs drift with releases —
  `domains.md` carries the checklist. The authoritative sets are the `DOMAINS`,
  `STRUCTURAL_SMELLS`, `MODELLING_SMELLS`, and `BEHAVIOUR_SMELLS` arrays in the
  workflow script; `domains.md` mirrors them for humans.
- **No auto-fix.** Findings become tracked work, not an automated diff.
