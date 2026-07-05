export const meta = {
  name: 'effect-model-audit',
  description:
    'Model-level type audit — the JUDGMENT sibling to effect-native-audit (the anchored regression net). Every effect-native-audit finder is anchor-driven (starts from a grep pattern), so it structurally CANNOT catch a defect with no lexical anchor — the address/observation role-conflation fixed by comms-ro14 (ChannelRef/ThreadRef/MessageRef conflating two directions via optionality) had none: every field was honest in isolation, the illegality was RELATIONAL. This skill keeps the catalogue\'s coverage-by-decomposition discipline (one bounded agent per enumerated cell, deterministic coverage) but CHANGES THE CELL from pattern to UNIT×LENS. A cheap DISCOVERY pre-pass enumerates every exported domain type and groups them into type-FAMILIES bundled with their full producer/consumer census (a verified PARTITION — every exported type in exactly one unit, checked as an equation, not trusted). Each unit is then run against four orthogonal value-space LENSES: L1 illegal-representable, L2 under-expressive, L3 role-coherence (census-driven — the ro14 catch), L4 boundary-honesty. The agent holds one unit + one lens and REASONS; it does not scan for a token. One adversarial refuter per finding, DEFAULT-SURFACED and EVIDENCE-GATED (refute anything lacking a named illegal value / missing state / disagreeing producer-consumer pair / specific boundary). Deterministic confidence partition, then synthesis writes a report for gated bead filing.',
  phases: [
    { title: 'Discover', detail: 'enumerate exported domain types -> type-families w/ full producer/consumer census; verified PARTITION against the raw export list (assertPartition)' },
    { title: 'Reason', detail: 'one bounded agent per unit×lens cell — L1 illegal-representable, L2 under-expressive, L3 role-coherence, L4 boundary-honesty; reasons, does not grep' },
    { title: 'Verify', detail: 'one adversarial refuter per finding — default-surfaced, evidence-gated (named illegal value / missing state / disagreeing pair / specific boundary or it is cut)' },
    { title: 'Synthesise', detail: 'dedup by file:line, deterministic confidence partition, write report' },
  ],
}

// ---------------------------------------------------------------------------
// Audit target — SCOPE v1: the port/domain types where model-honesty defects bite.
// packages/core is the port surface (branded schemas, the ChannelRef/ObservedThread/MessageRef
// structs, the port interfaces, the tagged errors). The adapters CONSTRUCT and READ those refs, so
// their construction/read sites are the producer/consumer census that makes cross-type conflation
// visible — reasoning about a type without its usage is the isolation that blinded the catalogue.
// Widen beyond this in a later version if it earns its keep. Tests + node_modules are out of scope.
// ---------------------------------------------------------------------------
const PORT_SURFACE = ['packages/core/ports.ts']
const CENSUS_ROOTS = ['packages']
const AUDIT_SCOPE = `The domain surface under audit (v1) is the port/domain types:
  ${PORT_SURFACE.join('\n  ')}
Their producer/consumer census spans every construction + read site across ${CENSUS_ROOTS.join(', ')}
(adapters CONSTRUCT refs, application/mcp READ them), excluding *.test.ts and any */node_modules/*.
Report every finding by its repo-relative path (e.g. packages/core/ports.ts, packages/zulip/refs.ts).`

// Read-only Effect source + docs clones — SUPPLEMENTARY grounding (data-types / code-style design
// docs). Unlike the sibling, the lenses are grounded in MODELLING PRINCIPLES first (the local
// strengthening-types skill + DDD make-illegal-states-unrepresentable), with Effect's data types as
// the vocabulary for the honest representation — not an export inventory to match against.
const STRENGTHENING_TYPES = '~/.claude/skills/strengthening-types/SKILL.md'
const DOCS_CLONE = '~/Development/references/effect-website/content/src/content/docs/docs'
const MODELLING_DOCS = `${DOCS_CLONE}/{data-types,code-style}` // Option, Either, Data, branded-types, pattern-matching...

// ---------------------------------------------------------------------------
// LENSES — AUTHORITATIVE copy. domains.md (child B) mirrors this for humans.
// Four ORTHOGONAL value-space QUESTIONS, each run against each unit. Each is a full reasoning prompt,
// NOT a grep — that is the whole divergence from the anchor-driven sibling. Grounded in modelling
// principles (strengthening-types, Effect data-types/* + code-style/* docs, DDD illegal-states).
// L1/L3 overlap BY DESIGN (same defect, isolation path vs census path — isolation is exactly what
// missed ro14, so the census path earns its own seat); L4 borders the sibling's behaviour/structural
// boundary smells. The bordering lenses state their boundary so synthesis does not double-count
// (dedup is still by file:line). prompt text is self-contained (no external interpolation) so the
// structure test can extract and evaluate this block in isolation.
// ---------------------------------------------------------------------------
// --- LENSES: authoritative value-space lens catalogue (test boundary) ---
const LENSES = [
  {
    id: 'L1',
    title: 'Illegal-representable',
    question: 'Can this type represent a value that is INVALID in the domain?',
    prompt: `Enumerate the set of values this type ADMITS, then compare it to the set the domain
PERMITS. If the type's set is strictly LARGER — a combination of fields, an optional that should be
required, a bare primitive that admits an out-of-domain string, a boolean pair whose illegal corner
is representable — the type can hold a value the domain forbids, and something downstream must defend
that corner by convention. This is the general form of "flags-and-optionals for state" and
"stringly-typed value", unbound from any grep costume: reason about the value space, do not match a
token. Invalid state must be unrepresentable (strengthening-types principle 1) — if it is
representable here, that is the finding. EVIDENCE REQUIRED: name a SPECIFIC illegal value the type
admits (concrete field assignment) and say why the domain forbids it. "I would model this
differently" with no named illegal value is taste, not a finding.`,
  },
  {
    id: 'L2',
    title: 'Under-expressive',
    question: 'Is there a LEGAL domain state this type CANNOT represent?',
    prompt: `The dual of L1. Is there a state that is VALID in the domain but that this type cannot
express, forcing a workaround — a sentinel value, a "transparent placeholder", an impossible branch
the code must handle anyway, a comment apologising for a field that is "always absent here"? An
under-expressive type pushes the missing case into convention or into a lie (a required field filled
with a dummy, an Option.none() standing in for "not that kind of thing"). EVIDENCE REQUIRED: name the
SPECIFIC legal domain state the type cannot represent AND the workaround it forces at a concrete site.
Absent a named missing state and its forced workaround, there is no finding.`,
  },
  {
    id: 'L3',
    title: 'Role coherence',
    question: 'Do all producers and consumers mean the SAME thing by this type, or is it two roles in a trenchcoat?',
    prompt: `Use the producer/consumer CENSUS for this unit. Walk every construction site and every
read site and ask: do they all mean the same thing by this type? Or is it serving two DIFFERENT roles
under one name — an ADDRESS (what a caller supplies to target something) vs an OBSERVATION (what the
substrate hands back); a REQUEST vs a RESPONSE; a TRUSTED assembled form vs an UNTRUSTED inbound one?
The tell is a PRODUCER that can legally construct a value which is ILLEGAL for some CONSUMER's role —
e.g. an optional field a consumer in one role must always have, or a producer that fills a field the
other role must leave empty. This is precisely the reasoning that isolation (L1, single type in a
vacuum) skips — and it is exactly what let the comms-ro14 conflation through: every field was honest
alone, the illegality was RELATIONAL across the census. Run this lens even when L1 found nothing.
EVIDENCE REQUIRED: a SPECIFIC disagreeing producer/consumer PAIR (name both sites) and the concrete
value one makes that the other cannot accept.`,
    boundary: `OVERLAPS L1 by design — the same defect can be reached by the isolation path (L1) or
the census path (L3). Do NOT suppress an L3 finding because L1 might also see it, and do NOT suppress
an L1 finding because "L3 is the census lens": surface both; synthesis dedups by file:line. The census
path exists as a separate seat precisely because isolation-reasoning is what missed ro14.`,
  },
  {
    id: 'L4',
    title: 'Boundary honesty',
    question: "At a substrate/trust boundary, does the type's ROLE demand a parsed-once strong type it does not have?",
    prompt: `At a substrate or trust boundary, judge whether this type's ROLE is a trusted-assembly
interface that DEMANDS a parsed-once strong representation — a Schema.Struct with required fields,
branded members, a decoded-once refinement — but is instead a loose shape the boundary must defend by
convention: a bare interface where a decoded Struct belongs, unbranded primitives carrying domain
constraints, optional-heavy fields standing in for "we'll check later". "Parse at boundaries, trust
inside" (strengthening-types principle 3): the boundary should PRODUCE a strong type that then flows
unchecked. Judge the TYPE'S ROLE, not whether a decode call happens to be present at some line.
EVIDENCE REQUIRED: name the SPECIFIC boundary and why the role there demands the stronger type (which
required fields / brands / refinement, and what defends the gap today).`,
    boundary: `BORDERS the sibling effect-native-audit's behaviour smell unvalidated-boundary (a
MISSING Schema.decodeUnknown call at a trust edge) and its structural smell internal-bridge (a
decodeUnknownSync bridge that DOES exist at a seam). L4 is MODEL-LEVEL: the TYPE'S ROLE demands a
strong parsed representation, independent of whether any decode call exists. If your finding is really
"a decode call is missing here" or "there is a *Sync bridge here", that is the sibling's territory —
file it there, not as L4. State the type-ROLE reason (the shape the role demands) to stay on this side
of the border.`,
  },
]
// --- end LENSES ---

// ---------------------------------------------------------------------------
// assertPartition — the HARD INVARIANT. Every exported domain type lands in exactly ONE unit.
// Coverage is a VERIFIED equation, not a trusted list: the discovery pre-pass returns families, and
// this checks the union of their members against the raw export set — missing (an exported type in no
// unit), duplicated (in >1 unit), unexpected (a unit member that is not an exported domain type).
// Kept marker-delimited + self-contained so the structure test can extract and exercise the real
// source (this workflow runs as a sandbox-wrapped body — it has no module import seam).
// ---------------------------------------------------------------------------
// --- assertPartition: coverage-equation invariant (test boundary) ---
function assertPartition(exportedTypes, units) {
  const exported = new Set(exportedTypes)
  const seenCount = new Map()
  const unexpected = []
  for (const unit of units) {
    for (const member of unit.members) {
      if (!exported.has(member)) {
        if (!unexpected.includes(member)) unexpected.push(member)
        continue
      }
      seenCount.set(member, (seenCount.get(member) ?? 0) + 1)
    }
  }
  const missing = exportedTypes.filter((t) => !seenCount.has(t))
  const duplicated = exportedTypes.filter((t) => (seenCount.get(t) ?? 0) > 1)
  return {
    ok: missing.length === 0 && duplicated.length === 0 && unexpected.length === 0,
    missing,
    duplicated,
    unexpected,
  }
}
// --- end assertPartition ---

// --- splitSurvivors: deterministic dedup + confidence split (test boundary) ---
// The confirmed/low-confidence partition is NOT agent-returned: an LLM asked to losslessly reproduce
// a split it can compute from each finding's own confidence drops and misfiles survivors. Dedup by
// file:line (an L1/L3 overlap-by-design hits the same site twice — keep the highest-confidence copy),
// then split by confidence. The gate derives its bead lists from this, deterministically.
function splitSurvivors(survivors) {
  const bySite = new Map()
  for (const finding of survivors) {
    const site = `${finding.file}:${finding.line}`
    const existing = bySite.get(site)
    if (!existing || (finding.confidence === 'high' && existing.confidence !== 'high')) {
      bySite.set(site, finding)
    }
  }
  const deduped = [...bySite.values()]
  return {
    confirmed: deduped.filter((f) => f.confidence === 'high'),
    lowConfidence: deduped.filter((f) => f.confidence !== 'high'),
  }
}
// --- end splitSurvivors ---

// ---------------------------------------------------------------------------
// Schemas. The discovery pre-pass returns a verified partition of families with their census; each
// lens finder returns model-honesty findings carrying CONCRETE EVIDENCE + a proposed remodelling +
// the blastRadius (the census already computed it — here the ripple is the whole point). The refuter
// returns a verdict. Synthesis returns only the report prose (the gate owns the confidence split).
// ---------------------------------------------------------------------------
const DISCOVERY_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    exportedTypes: {
      type: 'array',
      items: { type: 'string' },
      description: 'the RAW list of every exported domain TYPE name from the port surface (from grep ^export) — the ground truth the partition is checked against',
    },
    units: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          key: { type: 'string', description: 'stable family name, e.g. address-observation-refs, branded-scalars, port-errors' },
          members: { type: 'array', items: { type: 'string' }, description: 'the exported type names in this family — must partition exportedTypes exactly' },
          rationale: { type: 'string', description: 'why these types form one family (shared role, shared construction site, mutual reference)' },
          producers: { type: 'array', items: { type: 'string' }, description: 'every construction site: file:line where a member of this family is built/decoded' },
          consumers: { type: 'array', items: { type: 'string' }, description: 'every read site: file:line where a member of this family is consumed/destructured' },
        },
        required: ['key', 'members', 'rationale', 'producers', 'consumers'],
      },
    },
  },
  required: ['exportedTypes', 'units'],
}

const LENS_FINDING_FIELDS = {
  unit: { type: 'string', description: 'the family key this finding belongs to' },
  lens: { type: 'string', enum: ['L1', 'L2', 'L3', 'L4'], description: 'the lens that surfaced it' },
  file: { type: 'string', description: 'repo-relative path of the type definition, e.g. packages/core/ports.ts' },
  line: { type: 'number', description: 'first line of the type/shape at fault' },
  evidence: {
    type: 'string',
    description: 'the CONCRETE evidence — L1: a named illegal value the type admits; L2: a named missing legal state + the workaround it forces; L3: a specific disagreeing producer/consumer PAIR (both sites) + the value one makes the other cannot accept; L4: the specific boundary + why its role demands the stronger type',
  },
  remodelling: { type: 'string', description: 'the proposed honest representation for THIS type (branded/refined Schema, discriminated Data.taggedEnum, split address vs observation type, required-field Schema.Struct...)' },
  blastRadius: { type: 'string', description: 'every producer + consumer a remodelling ripples to — the census already enumerated them; a type change is never local, name them all' },
  confidence: { type: 'string', enum: ['high', 'low'], description: 'high only when the evidence is concrete and no documented exclusion applies' },
  why: { type: 'string', description: 'modelling-principle-grounded justification (strengthening-types / DDD illegal-states / the cited data-types doc)' },
}

const LENS_FINDINGS_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    unit: { type: 'string' },
    lens: { type: 'string' },
    findings: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: LENS_FINDING_FIELDS,
        required: ['unit', 'lens', 'file', 'line', 'evidence', 'remodelling', 'blastRadius', 'confidence', 'why'],
      },
    },
  },
  required: ['unit', 'lens', 'findings'],
}

const VERDICT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    refuted: { type: 'boolean', description: 'true if the finding lacks concrete evidence, misreads the code, or the current model is genuinely correct' },
    reason: { type: 'string', description: 'evidence/principle-grounded justification for the verdict' },
  },
  required: ['refuted', 'reason'],
}

const SYNTH_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    reportMarkdown: { type: 'string', description: 'the full report, also written to the report file' },
  },
  required: ['reportMarkdown'],
}

// ---------------------------------------------------------------------------
// Prompts.
// ---------------------------------------------------------------------------
function discoveryPrompt() {
  return `You are the DISCOVERY pre-pass of a model-level type audit. Your job is to enumerate the
domain type surface and group it into type-FAMILIES bundled with their full producer/consumer census.
Reasoning about a type WITHOUT its usage is the isolation that blinds an anchored audit — the census
is what makes cross-type role-conflation visible, so it is the heart of this pass.

1. ENUMERATE the raw export surface. Run \`grep -nE '^export (const|type|interface|class) ' \` over:
     ${PORT_SURFACE.join('\n     ')}
   From that, list exportedTypes = every exported domain TYPE name (a \`type\`/\`interface\`/\`class\`
   type, AND the \`.Type\` of a Schema.Struct/brand where that is the domain type callers use — e.g.
   ChannelRef, ObservedThread, MessageRef, Identity, Message, the branded scalars, the tagged errors).
   This is the GROUND TRUTH the partition is checked against — be complete and exact.

2. GROUP into type-FAMILIES. A family is a set of types that share a role or are mutually referential
   (e.g. the address/observation refs ChannelRef+ObservedThread+MessageRef; the branded scalar ids;
   the port-error tagged classes). HARD INVARIANT: every exported type lands in EXACTLY ONE family —
   a PARTITION. This is mechanically re-checked after you return; a type in no family or two families
   is a coverage break.

3. CENSUS each family. For every member type, find EVERY construction site (where it is built or
   decoded — producers) and EVERY read site (where it is destructured or consumed — consumers) across:
     ${CENSUS_ROOTS.join(', ')}   (exclude *.test.ts and */node_modules/*)
   Record them as file:line. This census IS the blast radius and the L3 evidence source.

${AUDIT_SCOPE}

Return exportedTypes (the raw list) and units (the families with rationale + producers + consumers).
Completeness matters more than tidiness — a missed export breaks the coverage equation.`
}

function lensFinderPrompt(unit, lens) {
  return `You are auditing ONE type-family — **${unit.key}** — through ONE value-space LENS —
**${lens.id} ${lens.title}**. You hold the family's full producer/consumer census. You REASON about
the value space; you do NOT scan for a token (that is the anchored sibling's job, and it structurally
misses relational defects like the comms-ro14 address/observation conflation — which is why this skill
exists).

THE FAMILY (unit "${unit.key}"):
  members:   ${(unit.members || []).join(', ')}
  rationale: ${unit.rationale || '(none given)'}
  producers (construction sites): ${(unit.producers || []).join(', ') || '(none found)'}
  consumers (read sites):         ${(unit.consumers || []).join(', ') || '(none found)'}

THE LENS — ${lens.id} ${lens.title}. Question: ${lens.question}
${lens.prompt}
${lens.boundary ? `\nBOUNDARY (do not double-count):\n${lens.boundary}` : ''}

GROUND your reasoning in MODELLING PRINCIPLES, not an Effect export inventory:
- The local strengthening-types skill: ${STRENGTHENING_TYPES} (invalid state unrepresentable;
  strengthen never weaken; parse at boundaries, trust inside; finite set -> union; domain-constrained
  value -> brand; optional-always-present-in-some-states -> discriminated union).
- DDD make-illegal-states-unrepresentable.
- Effect's data types as the VOCABULARY for the honest representation (Option, Either, Data.taggedEnum,
  branded/refined Schema, Schema.Struct with required fields). Supplementary design docs:
  ${MODELLING_DOCS} — read the relevant data-types/* + code-style/* page for the *why* when you cite
  one. If that clone is absent (cloud/CI), WebFetch the matching effect.website/docs page.

READ the actual type definitions and the census sites in:
${AUDIT_SCOPE}

For each finding: unit="${unit.key}", lens="${lens.id}", file:line of the type at fault, EVIDENCE (the
concrete artefact this lens demands — see EVIDENCE REQUIRED above; a finding with no concrete evidence
will be REFUTED), remodelling (the honest representation for THIS type), blastRadius (every producer +
consumer from the census — the ripple is the whole cost the gate weighs), confidence, and a
principle-grounded why.

Confidence "high" ONLY when the evidence is concrete AND no documented exclusion applies. If a plainer
shape may be justified, mark "low"; the verify pass scrutinises it. Empty findings is a valid, honest
result — a well-modelled family SHOULD return none. Do NOT invent findings to look productive.`
}

function refuteLensPrompt(f) {
  return `Adversarially verify ONE model-level finding. Default to SURFACED — a surfaced false positive
is cut cheaply at the human gate, but a refuted true smell is invisible. You refute ONLY on the
evidence gate or a genuine misread.

  unit:         ${f.unit}
  lens:         ${f.lens}
  file:         ${f.file}:${f.line}
  evidence:     ${f.evidence}
  remodelling:  ${f.remodelling}
  blastRadius:  ${f.blastRadius}
  why:          ${f.why}

Read the actual type at ${f.file}:${f.line} and the census/boundary the evidence cites.

REFUTE (refuted=true) only when one of these holds:
- EVIDENCE GATE (the primary ground): the finding is NOT grounded in the concrete artefact its lens
  demands — L1 with no NAMED illegal value the type admits; L2 with no NAMED missing legal state + the
  workaround it forces; L3 with no SPECIFIC disagreeing producer/consumer PAIR (both sites named) and
  the value one makes the other cannot accept; L4 with no SPECIFIC boundary + type-role reason. "I'd
  model this differently" / "this feels off" with no concrete illegal value, missing state, disagreeing
  pair, or boundary is taste-noise — refute it.
- MISREAD: the type does not actually have the claimed shape (the optional is genuinely optional in
  every role; the "two roles" are one role; the primitive has no domain constraint; the boundary is
  internal/already-parsed upstream), OR the finding is really the SIBLING's (L4 that is actually a
  missing decodeUnknown call or a *Sync bridge — note the reclassification and refute here).

Do NOT refute for any of these — they are HUMAN-GATE concerns, NEVER refutation grounds:
- "Is the remodelling worth the blast radius?" — WORTH is the gate's call. A real illegal state stays a
  finding no matter how large the ripple. If the blastRadius is wrong, correct it in the reason and
  CONFIRM (refuted=false).
- "This is a big multi-site type migration and I can't be sure the whole thing is correct" — the exact
  target type is the implementer's job on the bead; you only judge whether the SMELL is real.
- The proposed remodelling is imperfect but the evidence and direction are right — CONFIRM and note the
  correction.

Default to refuted=FALSE when unsure. Only refute when the evidence gate fails or you can affirmatively
show the current model is correct.`
}

// ===========================================================================
// args may arrive parsed or as a JSON string — normalise both so date/reportPath/lenses aren't
// silently dropped.
const A = (() => {
  try {
    return typeof args === 'string' ? JSON.parse(args) : args || {}
  } catch {
    return {}
  }
})()
const date = A.date || 'undated'
const reportPath = A.reportPath || `docs/effect-model-audit-${date}.md`
// All four lenses run by default; A.lenses (e.g. ['L3']) restricts the sweep — cheap to re-run a
// single lens after editing its prompt.
const activeLenses = A.lenses ? LENSES.filter((l) => A.lenses.includes(l.id)) : LENSES

// ---------------------------------------------------------------------------
// DISCOVER — the pre-pass, then the verified partition (the coverage equation).
// ---------------------------------------------------------------------------
phase('Discover')
log(`Effect-model audit ${date}: discovering the port/domain type surface over ${PORT_SURFACE.join(', ')}`)

const discovery = await agent(discoveryPrompt(), { label: 'discover:type-surface', phase: 'Discover', schema: DISCOVERY_SCHEMA })
const exportedTypes = discovery?.exportedTypes ?? []
let units = discovery?.units ?? []

// HARD INVARIANT: every exported domain type in exactly one unit. Coverage is a VERIFIED equation.
const partition = assertPartition(exportedTypes, units)
if (!partition.ok) {
  log(`Partition BREAK — missing=[${partition.missing.join(', ')}] duplicated=[${partition.duplicated.join(', ')}] unexpected=[${partition.unexpected.join(', ')}]. Repairing deterministically so coverage stays total.`)
  // Deterministic backstop so no exported type escapes audit: drop phantom members, dedup a type to
  // its first family, and sweep any un-assigned exports into an explicit `unassigned` unit. The
  // pre-pass is instructed to return a clean partition; this only guarantees totality when it slips.
  const claimed = new Set()
  units = units.map((u) => ({
    ...u,
    members: (u.members || []).filter((m) => exportedTypes.includes(m) && !claimed.has(m) && claimed.add(m)),
  }))
  const stillMissing = exportedTypes.filter((t) => !claimed.has(t))
  if (stillMissing.length) {
    units = [...units, { key: 'unassigned', members: stillMissing, rationale: 'swept in by the partition backstop — the pre-pass left these ungrouped', producers: [], consumers: [] }]
  }
  const repaired = assertPartition(exportedTypes, units)
  log(`Post-repair partition ok=${repaired.ok} across ${units.length} units covering ${exportedTypes.length} exported types`)
} else {
  log(`Partition verified: ${units.length} units cover ${exportedTypes.length} exported types exactly`)
}

// ---------------------------------------------------------------------------
// REASON -> VERIFY as a pipeline. One cell per unit×lens; a finding flows into its adversarial
// refuter the moment its finder returns (no barrier). Verify is DEFAULT-SURFACED: a verifier error or
// missing verdict keeps the finding (refuted=false, flagged unverified) rather than silently dropping
// a real smell — the opposite of a false-twin swap, where a miss defaults to dropped.
// ---------------------------------------------------------------------------
phase('Reason')
const cells = units.flatMap((u) => activeLenses.map((l) => ({ u, l })))
log(`Reasoning over ${cells.length} cells (${units.length} units × ${activeLenses.length} lenses)`)

const verified = await pipeline(
  cells,
  ({ u, l }) => agent(lensFinderPrompt(u, l), { label: `reason:${u.key}:${l.id}`, phase: 'Reason', schema: LENS_FINDINGS_SCHEMA }).catch(() => null),
  (result, { u, l }) => {
    if (!result) return { unit: u.key, lens: l.id, failed: true, verified: [] }
    if (!result.findings.length) return { unit: u.key, lens: l.id, failed: false, verified: [] }
    return parallel(
      result.findings.map((f) => () =>
        agent(refuteLensPrompt(f), { label: `verify:${f.unit}:${f.lens}:${f.file}:${f.line}`, phase: 'Verify', schema: VERDICT_SCHEMA })
          .then((v) => ({ ...f, refuted: v ? v.refuted : false, verifyFailed: !v, refuteReason: v ? v.reason : 'verifier returned no verdict — SURFACED unverified (default-surfaced)' }))
          .catch(() => ({ ...f, refuted: false, verifyFailed: true, refuteReason: 'verifier errored — SURFACED unverified (default-surfaced)' })),
      ),
    ).then((vf) => ({ unit: u.key, lens: l.id, failed: false, verified: vf }))
  },
)

const cellResults = verified.filter(Boolean)
const allVerified = cellResults.flatMap((c) => c.verified).filter(Boolean)
const survivors = allVerified.filter((f) => !f.refuted)
const refutedCount = allVerified.filter((f) => f.refuted).length
const failedCells = cellResults.filter((c) => c.failed).map((c) => `${c.unit}:${c.lens}`)
const coverage = cellResults.map((c) => ({ unit: c.unit, lens: c.lens, failed: !!c.failed, findings: c.verified.length }))

log(`Verified: ${survivors.length} survivors, ${refutedCount} refuted across ${cellResults.length} cells${failedCells.length ? `; ${failedCells.length} cells failed (${failedCells.join(', ')}) — re-run on resume` : ''}`)

// ---------------------------------------------------------------------------
// SYNTHESISE. The agent owns the report prose; the confirmed/low-confidence split is derived
// deterministically from the survivors (splitSurvivors), never trusted to an LLM.
// ---------------------------------------------------------------------------
phase('Synthesise')
let synthesis = null
let synthesisError = null
try {
  synthesis = await agent(
    `Synthesise the model-level type audit for ${date}. You are given the findings that SURVIVED
adversarial verification (refuted ones already dropped) and the unit×lens coverage.

Each finding carries: unit (type-family), lens (L1 illegal-representable / L2 under-expressive /
L3 role-coherence / L4 boundary-honesty), file:line, concrete EVIDENCE, a proposed remodelling, and
blastRadius (every producer + consumer a remodelling ripples to).

1. DEDUP: the same file:line may be surfaced by more than one lens — L1 and L3 overlap BY DESIGN (the
   isolation path and the census path reaching the same relational defect). Merge duplicates at a
   file:line, keeping the clearest evidence and citing every lens that found it. Do NOT collapse
   genuinely-distinct findings at different lines.
2. SPLIT for the report: confidence==="high" -> Confirmed; else -> Low-confidence. After dedup, a site
   high in any lens is high. (The gate ALSO derives the confirmed/low-confidence bead partition itself,
   deterministically — you own only the report prose.)
3. Write the report to ${reportPath} (use the Write tool). Structure:
     ## Effect-model audit — ${date}
     ### Confirmed findings   <- table: file:line | unit | lens | evidence | remodelling | blastRadius | why
     ### Low-confidence (report only)
     ### Coverage             <- units × lenses run, any failed cells, the verified partition size
4. Return reportMarkdown (identical to the file). The gate derives the bead lists from the survivors.

SURVIVORS (JSON):
${JSON.stringify(survivors, null, 2)}

COVERAGE (JSON):
${JSON.stringify({ exportedTypesCount: exportedTypes.length, units: units.map((u) => u.key), cells: coverage, failedCells }, null, 2)}`,
    { label: 'synthesise', phase: 'Synthesise', schema: SYNTH_SCHEMA },
  )
} catch (e) {
  synthesisError = String((e && e.message) || e)
  log(`Synthesis agent failed (${synthesisError}) — returning ${survivors.length} raw survivors for the orchestrator to write up`)
}

const { confirmed, lowConfidence } = splitSurvivors(survivors)

log(
  synthesis
    ? `Report written to ${reportPath}: ${confirmed.length} confirmed, ${lowConfidence.length} low-confidence (split deterministically from ${survivors.length} survivors)`
    : `No report file written (synthesis failed); ${confirmed.length} confirmed / ${lowConfidence.length} low-confidence split from ${survivors.length} survivors for write-up`,
)

return {
  date,
  reportPath,
  reportWritten: !!synthesis,
  synthesisError,
  exportedTypes,
  units: units.map((u) => ({ key: u.key, members: u.members })),
  partitionOk: assertPartition(exportedTypes, units).ok,
  confirmed,
  lowConfidence,
  survivors,
  refutedCount,
  coverage,
  failedCells,
}
