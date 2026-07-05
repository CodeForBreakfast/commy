# Lens catalogue & partition discipline

Human-readable mirror of the sweep's **cell**: a **unit × lens**. **The
authoritative copy is the `LENSES` array in `effect-model-audit.workflow.js`** —
the workflow script cannot read files, so it drives the finders from that array
directly. This page exists so a human can review and refresh the lenses without
reading JS. Keep them in sync.

The sibling `effect-native-audit` decomposes coverage by **pattern** (one finder
per `grep:` anchor). This skill decomposes by **unit × lens** instead: the cell is
one type-family (plus its full producer/consumer census) crossed with one
value-space question. The agent **reasons** about the value space; it does **not**
scan for a token. That is the whole divergence from the anchor-driven sibling, and
the reason it can reach relational defects (comms-ro14) that have no lexical
anchor.

## The unit partition — a verified equation, not a trusted list

A **unit** is a type-**family**: a set of exported domain types that share a role
or are mutually referential (the address/observation refs; the branded scalar ids;
the port-error tagged classes), bundled with its **census** — every construction
site (producers) and every read site (consumers) across `packages/`.

The census is the point. **Reasoning about a type without its usage is the
isolation that blinds an anchored audit** — a producer that can legally build a
value illegal for some consumer's role is invisible until you put the producer and
the consumer side by side. So each unit carries its whole producer/consumer census,
and that census is both the **L3 evidence source** and the **blastRadius** a
remodelling will ripple to.

**HARD INVARIANT — a partition.** Every exported domain type lands in **exactly
one** unit. The discovery pre-pass returns `exportedTypes` (the raw
`grep '^export'` ground truth) and `units` (the families); `assertPartition` then
checks the union of the families' members against that ground truth as a **coverage
equation**:

- **missing** — an exported type in *no* unit (a coverage hole),
- **duplicated** — an exported type in *more than one* unit,
- **unexpected** — a unit member that is *not* an exported domain type.

Any of the three fails the equation. A deterministic backstop then drops phantom
members, dedups a type to its first family, and sweeps any still-un-assigned
exports into an explicit `unassigned` unit — so **nothing escapes audit** even if
the pre-pass slips. Grouping adapts as the code grows; **coverage is a verified
equation, not a trusted list**. This is how the no-cherry-picking bar (the thing
that stops agents auditing only the interesting types) is met at the unit level.

## The four lenses

Four **orthogonal value-space questions**, each run against **each** unit. Grounded
in **modelling principles** — not an Effect export inventory:

- the local **`strengthening-types`** skill
  (`~/.claude/skills/strengthening-types/SKILL.md`): invalid state unrepresentable;
  strengthen never weaken; parse at boundaries, trust inside; finite set → union;
  domain-constrained value → brand; optional-always-present-in-some-states →
  discriminated union.
- **DDD** make-illegal-states-unrepresentable.
- Effect's **data types as the vocabulary** for the honest representation —
  `Option`, `Either`, `Data.taggedEnum`, branded/refined `Schema`, `Schema.Struct`
  with required fields. Supplementary design docs under
  `~/Development/references/effect-website/content/src/content/docs/docs/{data-types,code-style}`
  (`option`, `either`, `data`, `branded-types`, `pattern-matching`…) — read the
  relevant page for the *why* when you cite one; `WebFetch` effect.website if the
  clone is absent.

| Lens | Question | Honest representation | Grounding |
|---|---|---|---|
| **L1** Illegal-representable | Can this type represent a value that is **invalid** in the domain? | narrow the type so its admitted set = the domain's permitted set — required over optional, branded/refined over bare primitive, `Data.taggedEnum` over a boolean pair | strengthening-types §1 (invalid state unrepresentable); `data-types/data` |
| **L2** Under-expressive | Is there a **legal** domain state this type **cannot** represent? | widen honestly — the missing variant added to a union, an `Option` where a field is genuinely optional, a discriminated case instead of a sentinel-filled required field | strengthening-types (finite set → union); `data-types/option`, `data-types/either` |
| **L3** Role coherence | Do all producers and consumers mean the **same thing**, or is it two roles in a trenchcoat? | **split the roles** — an address type distinct from an observation type; a request type distinct from a response; a trusted-assembled form distinct from an untrusted inbound one | strengthening-types (parse at boundaries); the **comms-ro14** catch; DDD |
| **L4** Boundary honesty | At a substrate/trust boundary, does the type's **role** demand a parsed-once strong type it lacks? | a `Schema.Struct` with required fields / branded members / a decoded-once refinement that the boundary **produces**, then flows unchecked inside | strengthening-types §3 (parse at boundaries, trust inside); `schema` design docs |

### Evidence each lens demands (the verify gate)

A finding with no concrete evidence is **taste-noise and is refuted**. Each lens
demands a *specific* artefact:

- **L1** — a **named illegal value** the type admits (a concrete field assignment)
  and why the domain forbids it.
- **L2** — the **named missing legal state** *and* the workaround it forces at a
  concrete site (a sentinel, a dummy-filled required field, an impossible branch).
- **L3** — a **specific disagreeing producer/consumer pair** (both sites named) and
  the concrete value one makes that the other cannot accept.
- **L4** — the **specific boundary** and why its role demands the stronger type
  (which required fields / brands / refinement, and what defends the gap today).

"I would model this differently" with no such artefact is not a finding. And **"is
the remodelling worth the blast radius" is a *gate* concern, never a refutation
ground** — a real illegal state stays a finding no matter how large the ripple.

## The two bordering lenses

Two lenses deliberately overlap something else; each states its boundary so
synthesis doesn't double-count (dedup is still by `file:line`).

- **L1 ↔ L3 — overlap by design.** The *same* relational defect can be reached by
  the **isolation** path (L1, one type in a vacuum) or the **census** path (L3,
  the producer/consumer walk). Do **not** suppress an L3 finding because L1 might
  also see it, nor an L1 finding because "L3 is the census lens" — surface both;
  synthesis dedups by `file:line`. **L3 earns its own seat precisely because
  isolation-reasoning is what missed comms-ro14**: every field was honest alone,
  so L1's vacuum could not see the conflation that only the census makes visible.
  L3 is run **even when L1 found nothing**.

- **L4 ↔ the sibling's `unvalidated-boundary` / `internal-bridge`.** L4 is
  **model-level**: the *type's role* demands a strong parsed representation,
  independent of whether any decode call exists. The sibling's smells are
  **behaviour/structural**: `unvalidated-boundary` is a *missing*
  `Schema.decodeUnknown` at a trust edge; `internal-bridge` is a
  `decodeUnknownSync` bridge that *does exist* at a seam. If a finding is really "a
  decode call is missing here" or "there is a `*Sync` bridge here", it is the
  sibling's territory — **file it there, not as L4**. State the type-role reason
  (the shape the role demands) to stay on this side of the border.

## Out of scope

- Anything `@effect/language-service` already enforces at `tsc` time, and anything
  `effect-native-audit` catches with a lexical anchor (hand-rolled helpers,
  imperative spines, present-but-anchorable representation smells). This sweep is
  for the **relational / model-level dishonesty a per-line linter and an
  anchor-driven finder are both blind to**. **Never re-report what the sibling or
  the LSP catches.**
- **Scope v1** is the port/domain surface — `packages/core/ports.ts`, with the
  census spanning every construction + read site across `packages/`. Not every
  exported type in every package; that's where model-honesty defects bite and it
  keeps v1 bounded. Widen later if it earns its keep. `*.test.ts` and
  `node_modules` are out of scope.

## Refresh when the type surface or Effect version moves

1. Re-check `PORT_SURFACE` / `CENSUS_ROOTS` in the workflow still name the domain
   surface (packages get split/renamed; the port file may move).
2. Re-check the grounding paths — the `strengthening-types` skill path and the
   `data-types` / `code-style` docs slugs (effect.website restructures across
   majors; refresh with `git -C ~/Development/references/effect-website pull`).
3. Add or reshape a lens only if a *new orthogonal value-space question* emerges —
   the four are a considered set, not a cage, but a new lens must be genuinely
   orthogonal (a different reasoning path to a different defect), not a re-skin.
4. Edit the `LENSES` array in `effect-model-audit.workflow.js` and this table
   together.
