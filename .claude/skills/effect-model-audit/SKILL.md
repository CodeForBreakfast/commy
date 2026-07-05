---
name: effect-model-audit
description: Use when asked to audit this codebase's type modelling — whether the domain types are honest, whether they can represent illegal states, or for a periodic model-level type review. Triggers on "audit our type modelling", "are our types honest", "can our types represent illegal states", "model-level type review", "audit our domain types". The JUDGMENT sibling to effect-native-audit — a workflow-driven reasoning sweep over the port/domain type surface, not the anchor-driven regression net and not the @effect/language-service linter.
---

# Effect-model type audit

Sweep the **port/domain type surface** for **model-level dishonesty** — types
whose *shape* lets a caller construct, or forces a caller to fake, a value the
domain does not mean. This is not "did we hand-roll a helper" (that's the
sibling) and not "is this line idiomatic" (that's the LSP). It is: **does the
type's value space match the domain's, and do all its producers and consumers
mean the same thing by it?**

Four orthogonal **value-space lenses**, each run against each type-family:

- **L1 Illegal-representable** — can the type represent a value that is *invalid*
  in the domain? A combination of fields, an optional that should be required, a
  bare primitive admitting an out-of-domain string, a boolean pair whose illegal
  corner is representable. The general form of "flags-and-optionals for state"
  and "stringly-typed value", **unbound from any grep costume** — reason about
  the value space, don't match a token. *Invalid state must be unrepresentable.*
- **L2 Under-expressive** — the dual: is there a *legal* domain state the type
  **cannot** represent, forcing a sentinel, a "transparent placeholder", an
  impossible branch, or a comment apologising for a field that is "always absent
  here"? An under-expressive type pushes the missing case into convention or into
  a lie.
- **L3 Role coherence** — from the producer/consumer **census**: do all
  construction sites and all read sites mean the *same thing* by this type, or is
  it two roles in a trenchcoat — an **address** (what a caller supplies to target
  something) vs an **observation** (what the substrate hands back)? The tell is a
  producer that can legally build a value that is *illegal for some consumer's
  role*. **This is the reasoning that isolation skips** — and exactly what let the
  comms-ro14 `ChannelRef`/`ThreadRef`/`MessageRef` conflation through: every field
  was honest alone, the illegality was *relational* across the census.
- **L4 Boundary honesty** — at a substrate/trust boundary, does the type's *role*
  demand a parsed-once strong representation (a `Schema.Struct` with required
  fields, branded members, a decoded-once refinement) that it does not have — a
  loose interface the boundary must defend by convention? *Parse at boundaries,
  trust inside.*

## Why this is the complement the sibling can't reach

`effect-native-audit` is a **frequent, cheap, anchored regression net**: every one
of its ~41 finders is **anchor-driven** — it starts from a `grep:` pattern and
confirms a shape at the hit. That is exactly what makes it cheap to re-run and
deterministic to reason about, and it is also a **structural blind spot**: a
finder can only find a defect that has a *lexical anchor*. Role-conflation has
none. The comms-ro14 address/observation defect — a `ThreadRef` whose optional
narrow was honest for the *address* role but illegal for the *observation* role —
was **not** caught by any effect-native-audit sweep. It was noticed by hand while
building `resolve_thread`, because **you cannot grep for a concept**: every field
was honest in isolation; the illegality was relational.

This skill is the **periodic, expensive, judgment** sweep that reaches those
defects. It keeps the catalogue's discipline that matters — *coverage by
decomposition*, one bounded agent per enumerated cell, deterministic coverage, no
agent cherry-picking — but **changes the cell** from `pattern` to **`unit × lens`**.
The agent holds one **unit** (a type-family plus its full producer/consumer
census) and one **lens** (a value-space question) and **reasons**; it does not
scan for a token.

Three coverage layers, three jobs — **never re-report across them**:

| | anchor | cost | catches |
|---|---|---|---|
| `@effect/language-service` | per-line rule (~60), `tsc`-time | free, every `bun run check` | floating Effects, unnecessary `Effect.gen`, global `fetch`/`Date` |
| `effect-native-audit` | a `grep:` pattern per finder | cheap, frequent | hand-rolled helpers, imperative spines, present-but-anchorable representation smells |
| **`effect-model-audit`** (this) | a **type-family + census** per finder, reasoned | expensive, periodic | **relational / model-level dishonesty with no lexical anchor** |

L4 deliberately **borders** the sibling's `unvalidated-boundary` (a *missing*
decode at a trust edge) and `internal-bridge` (a `decodeUnknownSync` bridge that
*exists* at a seam). L4 is **model-level**: the type's *role* demands a strong
parsed representation, independent of whether any decode call exists. If a finding
is really "a decode call is missing here" or "there is a `*Sync` bridge here",
that is the sibling's territory — file it there, not as L4. The lens carries an
explicit boundary statement (see `domains.md`) so the axes don't double-count.

## How it works

A `Workflow` script (`effect-model-audit.workflow.js`) runs a four-phase pipeline.
The lenses are grounded in **modelling principles first** — the local
`strengthening-types` skill (`~/.claude/skills/strengthening-types`), DDD
make-illegal-states-unrepresentable, and Effect's `data-types/*` + `code-style/*`
design docs as the *vocabulary* for the honest representation (not an export
inventory to match against).

1. **Discover** — a cheap pre-pass enumerates the raw export surface
   (`grep '^export'` over `packages/core/ports.ts`) and groups every exported
   domain type into type-**families**, each bundled with **every** construction
   site (producers) and **every** read site (consumers) across `packages/`. The
   census is the heart of the pass — *reasoning about a type without its usage is
   the isolation that blinds the anchored audit*, so it is what makes cross-type
   role-conflation visible.
   - **HARD INVARIANT — a verified partition.** Every exported domain type lands
     in **exactly one** family. This is checked as a **coverage equation**
     (`assertPartition`) against the raw export list — missing (an export in no
     unit), duplicated (in >1), or unexpected (a member that isn't an export) all
     fail. A deterministic backstop sweeps any un-grouped export into an explicit
     `unassigned` unit so nothing escapes audit. Coverage is an equation, not a
     trusted list — this is how the no-cherry-picking bar is met at the unit level.
2. **Reason** — one bounded agent per **unit × lens** cell. The agent holds one
   family + its census and one lens, reads the actual type definitions and census
   sites, and **reasons** about the value space. Empty findings is a valid, honest
   result — a well-modelled family *should* return none.
3. **Verify** — per-finding, no barrier: each finding flows into one adversarial
   refuter the moment its finder returns. Verify is **default-surfaced and
   evidence-gated** — consistent with a modelling audit, a surfaced false positive
   is cut cheaply at the gate, but a refuted true smell is invisible. The refuter
   **refutes** any finding not grounded in **concrete evidence**: a named illegal
   value (L1), a specific missing legal state + the workaround it forces (L2), a
   specific disagreeing producer/consumer *pair* (L3), or the specific boundary
   (L4). "I'd model this differently" with no named illegal value is taste-noise,
   cut. **"Is the rewrite worth the blast radius" is never a refutation ground —
   it is a gate concern.** A verifier error keeps the finding (surfaced,
   unverified), never silently drops it.
4. **Synthesise** — a synthesis agent writes the report prose, deduping
   cross-lens sightings (L1 and L3 overlap **by design** — the same relational
   defect reached by the isolation path and the census path; they dedup by
   `file:line`). The confirmed / low-confidence **bead partition** is **not**
   trusted to that agent — the workflow derives it deterministically from each
   survivor's own confidence (`splitSurvivors`: dedup by `file:line`, highest
   confidence wins, then split).

Each surviving finding carries **concrete evidence** (the illegal value / the
disagreeing pair / the boundary), a **proposed remodelling**, and its
**blastRadius** — every producer + consumer a remodelling ripples to. The census
already computed the blast radius; here the ripple *is the whole point*.

## Running it

1. Get today's date (`date +%F`).
2. Launch the workflow, passing the date so the report file is named:

   ```
   Workflow({
     scriptPath: '.claude/skills/effect-model-audit/effect-model-audit.workflow.js',
     args: { date: '<YYYY-MM-DD>' },
   })
   ```

   It runs in the background (a discovery pass, then one agent per `unit × lens`
   cell — ~10 concurrent — then per-finding verify + synth); you are notified on
   completion. Watch live with `/workflows`. To run a single lens (cheaper re-run
   after editing its prompt, or to resume just one), pass
   `args: { date, lenses: ['L3'] }`.

3. The workflow writes the report to `docs/effect-model-audit-<date>.md` and
   returns `{ confirmed[], lowConfidence[], survivors[], refutedCount, coverage,
   partitionOk }`.

## After it returns — gate, then file (do NOT let the workflow file beads)

The workflow deliberately returns findings rather than writing to `bd`. Model
findings are **architectural** — a sweep can surface a handful of type
remodellings, and auto-filing them straight into `bd ready` clogs the queue with
work that hasn't been arbitrated.

The report is **working scaffolding for this gate conversation, not a permanent
record** — beads are the durable artifact, so the report is binned once the beads
exist (step 4). Everything worth keeping must land in a bead.

1. **Present** the `confirmed[]` list to Graeme — `file:line`, unit, lens, the
   **evidence** (the named illegal value / disagreeing producer-consumer pair /
   boundary), the proposed remodelling, and the **blastRadius**. Also surface the
   `lowConfidence[]` items so he can promote any worth keeping; un-promoted ones
   are dropped when the report is binned.
2. **Wait for go-ahead — the gate leans harder on Graeme here than the sibling's.**
   A model remodelling is a **taste-and-cost call**, not a mechanical helper swap:
   the finding says a *real* illegal state or role-conflation exists, but whether
   to split a type, brand a scalar, or absorb the blast radius is a judgment about
   the whole design. Graeme may cut, downgrade, or reshape a confirmed item, or
   promote a low-confidence one, before anything is filed. **"Is the remodelling
   worth the ripple" is decided *here*, deliberately — the verify pass was
   forbidden from touching it.**
3. On approval, **file under one tracking epic** so the items stay contained. Each
   bead's description must be **self-contained** — it has to stand on its own once
   the report is gone. Two fields are non-negotiable and make model beads differ
   from the sibling's:
   - the **evidence** — the concrete illegal value, missing state, or disagreeing
     producer/consumer pair (with both sites) that proves the smell is real, and
   - the **blastRadius** — every producer + consumer the remodelling ripples to. A
     type change is never local; that ripple is the real cost and the bead is
     unfileable without it.

   Do **not** add a `report: docs/...` pointer; that doc won't survive.

   ```bash
   bd create "Effect-model audit <YYYY-MM-DD>" --type epic
   # then per finding being filed (confirmed + any promoted low-confidence):
   bd create "<unit>: <lens> — <type> honesty" --type task \
     --parent <epic-id> \
     -d "<the evidence (named illegal value / disagreeing pair / boundary); the
         proposed remodelling; the blastRadius (every producer + consumer); the
         modelling-principle why (strengthening-types / DDD illegal-states / the
         cited data-types doc)>"
   ```

4. **Bin the report** — once the beads are generated, delete
   `docs/effect-model-audit-<date>.md`. The actionable record now lives in the
   epic tree; the coverage table and any dropped low-confidence items are process
   metadata, acceptable to lose.

Each filed item is its own remodelling; implement them on their own beads with
TDD, not as a batch — a type change ripples through its whole census.

## Notes

- **Modelling principles ARE the ground, not an export inventory.** Unlike the
  sibling's substitution axis (which needs the complete `Array`/`Option`/… export
  surface in front of the finder), these lenses are grounded in *why a
  representation is honest*: the local `strengthening-types` skill, DDD
  make-illegal-states-unrepresentable, and Effect's `data-types/*` + `code-style/*`
  design docs as the vocabulary for the fix. See `domains.md`.
- **Local clones, GitHub fallback.** `strengthening-types` at
  `~/.claude/skills/strengthening-types/SKILL.md`; Effect docs at
  `~/Development/references/effect-website` (the URL `…/docs/<slug>/` maps to
  `…/content/src/content/docs/docs/<slug>.mdx`). Both are workstation-local; a
  cloud/CI agent falls back to `WebFetch` on effect.website, just slower.
- **Scope v1 is the port/domain surface.** `packages/core/ports.ts` (the branded
  schemas, the address/observation refs, the port interfaces, the tagged errors),
  with the producer/consumer census spanning every construction + read site across
  `packages/` (adapters *construct* refs; application/mcp *read* them). Not every
  exported type in every package — that's where model-honesty defects bite and it
  keeps v1 bounded. Widen later if it earns its keep. `*.test.ts` and
  `node_modules` are out of scope.
- **The authoritative lens catalogue is the `LENSES` array** in
  `effect-model-audit.workflow.js` (the workflow can't read files, so it drives the
  finders from that array directly). `domains.md` mirrors it for humans — keep them
  in sync.
- **No auto-fix.** Findings become tracked, arbitrated work, not an automated diff.
