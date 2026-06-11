# Effect domain & smell checklist

Human-readable mirror of the sweep's **four axes**. **The authoritative copies are
the `DOMAINS`, `STRUCTURAL_SMELLS`, `MODELLING_SMELLS`, and `BEHAVIOUR_SMELLS`
arrays in `effect-native-audit.workflow.js`** — the workflow script cannot read
files, so it drives the finders from those arrays directly. This page exists so a
human can review and refresh the sets without reading JS. Keep them in sync.

The first three axes are **static** — they catch non-native code that is *present*.
The fourth is **dynamic** — it catches native-but-misused / policy code.

- **Substitution axis** (`DOMAINS`) — one finder per Effect module; each holds
  that module's *entire* export inventory, because you cannot spot that our
  helper duplicates `Array.partition` unless the whole `Array` surface is in
  front of you. *Where did we hand-roll a helper?* (verbs)
- **Structural axis** (`STRUCTURAL_SMELLS`) — one finder per shape anti-pattern;
  each reads the Effect *design* docs (not a module inventory) and greps for code
  whose construction / DI / error / resource spine is imperative OOP even though
  the leaves return Effect. *Effect used inside functions ≠ an Effect-native
  program.* (wiring)
- **Modelling axis** (`MODELLING_SMELLS`) — one finder per representation
  anti-pattern; each reads the `code-style/*` + `data-types/*` design docs and
  greps for data/state whose *type* throws away a guarantee an Effect data type
  would give for free (null over `Option`, sentinels over `Either`, flags +
  optional fields over a tagged union, bare primitives over branded types,
  in-place mutation over immutable structures, hand-rolled equality over `Equal`,
  data-first helpers that block clean `pipe` composition). *Is the representation
  itself honest?* (nouns)
- **Behaviour axis** (`BEHAVIOUR_SMELLS`) — one finder per dynamic anti-pattern;
  each reads the `concurrency/*` + `error-management/*` + `schema/*` design docs
  and greps for native-but-misused / policy code: how effects **run** (sequential
  when they could be concurrent, unbounded fan-out), **fail** (untyped or swallowed
  error channel), and what they **trust** (untrusted data crossing the edge with no
  `Schema` decode). Each finder anchors on *present* code (an `Effect.all`/`fork`,
  an `as`/`JSON.parse`, a typed `E`), not blanket absence. *Does it run, fail, and
  trust the way a great Effect program should?* (dynamics)

## Why two sources

- **Source** (`~/Development/references/effect`, fallback GitHub raw) — the
  *what*: the complete export inventory + signatures + JSDoc `@example`.
  Non-negotiable for substitution; grep-then-guess misses the copy-of-behaviour
  cases that matter most.
- **Docs** (`~/Development/references/effect-website`, fallback effect.website via
  `WebFetch`) — the *when & why*. Astro/Starlight: the URL `…/docs/<slug>/` maps
  exactly to `…/content/docs/docs/<slug>.mdx`, and a multi-page section (Stream,
  Schedule, requirements-management) is a **directory** the finder reads in full,
  not just the intro. For substitution this separates a real swap from a false
  twin; for structural, modelling, and behaviour the design docs ARE the ground,
  since the smell is a shape, a representation, or a dynamic policy, not an export.

## Substitution domains

`Docs` is a slug under the docs clone (`…/content/docs/docs/<slug>` — a page or a
section directory). `null` = API-reference only, no prose page (lean on source).

| Domain | Source module(s) | Docs slug |
|---|---|---|
| `Array` | `Array.ts` | — (API-ref only) |
| `Record` | `Record.ts`, `Struct.ts` | — (API-ref only) |
| `Chunk` | `Chunk.ts` | data-types/chunk |
| `Option` | `Option.ts` | data-types/option |
| `Either` | `Either.ts` | data-types/either |
| `Predicate` | `Predicate.ts`, `Function.ts` | getting-started/building-pipelines |
| `String` | `String.ts` | — (API-ref only) |
| `Number` | `Number.ts` | — (API-ref only) |
| `Effect` | `Effect.ts` | getting-started/control-flow |
| `Config` | `Config.ts` | configuration |
| `Schema` | `Schema.ts` | schema/ (section) |
| `Schedule` | `Schedule.ts` | scheduling/ (section) |
| `Stream` | `Stream.ts` | stream/ (section) |
| `HttpClient` | `platform/HttpClient.ts` + request/response | platform/ (section; HttpClient is API-ref) |
| `Layer` | `Layer.ts`, `Context.ts` | requirements-management/ (section) |
| `Match` | `Match.ts` | code-style/pattern-matching |
| `Equal` | `Equal.ts`, `Order.ts`, `Hash.ts`, `Data.ts` | trait/ (section) |
| `Ref` | `Ref.ts`, `SynchronizedRef.ts`, `STM.ts` | state-management/ (section) |
| `Duration` | `Duration.ts`, `Clock.ts`, `DateTime.ts` | data-types/duration |
| `Cause` | `Cause.ts`, `Exit.ts` | error-management/two-error-types |
| `Queue` | `Queue.ts`, `PubSub.ts`, `Mailbox.ts` | concurrency/ (section) |
| `Scope` | `Scope.ts` | resource-management/ (section) |
| `Cron` | `Cron.ts` | scheduling/cron |

## Structural smells

Each finder greps an anchor pattern, reads the design doc for the native shape
and its justified exceptions, then confirms each candidate by reading it. The
canonical misses that motivated this axis are `ZulipHttp` (comms-0m8) and the
adapter's manual-DI config (comms-ui2).

| Smell | What it is | Native shape | Docs slug |
|---|---|---|---|
| `oop-construction` | `new`-able class deriving state, methods return Effect | `make` Effect, or `Context.Tag`+`Layer`/`Effect.Service` | requirements-management/ |
| `manual-di` | dependency as constructor arg / config field / **threaded param** (incl. a `ConfigProvider`/env/clock/client source parameterised so prod & tests share one path) | declare in `R` via `yield* Tag`; **provide a real Layer in prod and a fixture Layer at the test boundary** — `ConfigProvider.fromEnv`-in-prod / `fromMap`-in-tests, never threaded | requirements-management/ |
| `throw-in-effect` | `throw` / throwing brand / `*Sync` inside an Effect-returning fn | `Effect.fail` / `Data.TaggedError` / `Schema.decodeUnknown` | error-management/expected-errors |
| `internal-bridge` | `run*`/`tryPromise`/`decodeUnknownSync` at an internal seam | run only at the app edge; `HttpClient` at the network edge | getting-started/running-effects |
| `mutable-state` | reassigned `let`/field as state across an Effect boundary | `Ref` / `SynchronizedRef` | state-management/ |
| `imperative-lifecycle` | `try/finally` or explicit `.close()` for cleanup | `acquireRelease` / `Scope` / Layer finalizer | resource-management/ |

The framing doc for the whole axis is `getting-started/the-effect-type` — effects
are lazy *descriptions*, not eagerly-executed state-deriving constructors.

## Modelling smells

Same shape as the structural finders (grep an anchor, read the design doc for the
honest representation and its justified exceptions, confirm by reading the site),
but hunting the *type* of data and state rather than the effect/DI/resource spine.
Verify defaults to **surfaced** (like structural): a false positive is cut cheaply
at the gate; a refuted true smell is invisible. Each finding carries `blastRadius`
— a representation change ripples to every producer and consumer of the value.

| Smell | What it is | Native representation | Docs slug |
|---|---|---|---|
| `nullable-return` | `T \| null` / `\| undefined` as "maybe absent" | `Option<T>` (`Option.fromNullable` + combinators) | data-types/option |
| `sentinel-or-throw` | sentinel (`-1`/`''`/`null`) or `throw` for expected absence in a **pure/sync** fn | `Option` / `Either` | data-types/either |
| `flag-stringly-state` | booleans + optional fields, or ad-hoc string tags, allowing illegal states | `Data.taggedEnum` + `Match.exhaustive` | data-types/data |
| `bare-primitive` | domain value as bare `string`/`number`, constraints enforced ad hoc | branded / refined type (`Schema.brand`, `Brand`, `Schema.NonEmptyString`…) | code-style/branded-types |
| `in-place-mutation` | `push`/`splice`/`sort`/property writes building **pure data** | immutable build / persistent `Chunk`/`HashMap` / `Data.struct` / `readonly` | data-types/chunk |
| `hand-equality` | field-by-field or `JSON.stringify` equality / stringified dedup | `Equal.equals` + `Data.struct`/`case` (value equality + `Hash`) | trait/equal |
| `data-first-helper` | helper wrapped in `(x) => f(cfg, x)` inside `pipe` because its signature is data-first | data-last (curried) signature / `dual` — clean `pipe` composition | code-style/dual |

Three smells deliberately border an existing axis; each finder states its boundary
so they don't double-count (synthesis still dedups same `file:line`):

- `sentinel-or-throw` vs structural `throw-in-effect` — pure/sync fn vs a `throw`
  *inside* an Effect-returning fn. Effect-returning ⇒ structural; pure ⇒ modelling.
- `in-place-mutation` vs structural `mutable-state` — mutation of pure data vs
  reassigned state *crossing* an Effect/async boundary. Crosses ⇒ structural.
- `hand-equality` / `nullable-return` vs the `Equal` / `Option` substitution
  domains — a representation that *discards a guarantee* (modelling) vs a
  *reimplemented helper* (substitution). They propose different work.

`data-first-helper` is in service of clean `pipe` composition, **not** currying for
its own sake — the finder and the refuter both reject a data-last rewrite that
would be gratuitous point-free (helper never piped, always fully applied at one
site, or reads worse).

## Behaviour smells

The **dynamic** axis: native-but-misused / policy code — how effects **run**,
**fail**, and what they **trust**. Smell-first like structural/modelling
(default-**surfaced** verify, `blastRadius` required), but each finder anchors on a
*present* construct (an `Effect.all`/`fork`, an `as`/`JSON.parse`, a typed `E`) and
confirms a concrete misuse — it does **not** scan for blanket absence ("no timeout
anywhere"), which is why timeout/retry/observability gaps are deliberately *not*
here. For concurrency smells the data dependency between effects is the whole
question and must be checked before flagging.

| Smell | What it is | Native behaviour | Docs slug |
|---|---|---|---|
| `sequential-not-concurrent` | independent effects run one-after-another (latency = sum, not max) | `Effect.all`/`forEach` with `{ concurrency }` — *only* when genuinely independent | concurrency/basic-concurrency |
| `unbounded-fanout` | `concurrency:"unbounded"` / fork-per-item over an externally-sized collection on a shared service | a **bounded** `{ concurrency: n }` / `Semaphore` (Zulip rate-limit hazard) | concurrency/basic-concurrency |
| `unsupervised-fork` | bare `Effect.fork` — fiber not scoped/joined, failures + interruption lost | `forkScoped` / `forkDaemon` with failures observed; often `Effect.all`/`race` | concurrency/fibers |
| `unvalidated-boundary` | untrusted external data via `as`/`JSON.parse` with **no** decode | `Schema.decodeUnknown` at the edge | schema/getting-started |
| `untyped-error-channel` | `E` is `Error`/`unknown`/string, or `catchAll`/`orDie`/`ignore` swallows a recoverable typed failure | tagged `Data.TaggedError` union + `catchTag`/`Match`; handle, don't downgrade | error-management/two-error-types |

Three smells border an existing axis; each finder states its boundary so they don't
double-count (synthesis still dedups same `file:line`):

- `unvalidated-boundary` vs structural `internal-bridge` vs modelling `bare-primitive`
  — *no decode at all* at the trust boundary (behaviour) vs *a `*Sync` decode bridge
  that exists* at a seam (structural) vs *a domain value lacking a brand* (modelling).
- `untyped-error-channel` vs structural `throw-in-effect` — the typed `E` *shape &
  handling* (behaviour) vs a raw `throw` escaping into a defect (structural).
- `unsupervised-fork` vs structural `mutable-state` — fiber *lifetime* (behaviour) vs
  shared state crossing an Effect boundary (structural).

## Out of scope

- Anything `@effect/language-service` already enforces at `tsc` time (floating
  Effects, unnecessary `Effect.gen`, `Effect.void`, global `fetch`/`Date`/
  `console`, untyped catch). Those fail `bun run check` already — this sweep is
  for the duplication, shape, representation, and dynamic policy a per-line linter
  is blind to.
- **Pure absence.** "This I/O has no `timeout`/`retry`," "no `withSpan`/structured
  logging anywhere" — a swap/smell detector has no present construct to anchor on,
  and blanket-absence scanning is too noisy. The behaviour axis only flags absence
  when it sits on a present anchor (an untrusted value being cast, a typed `E` being
  swallowed). A dedicated resilience/observability audit would be a sibling skill.
- Test files (`*.test.ts`) and `node_modules`. The sweep targets the non-test
  source files under `packages/` (the `core`, `testing`, `zulip`, `memory`,
  `mcp`, `plugin` workspace packages).
  (Test-idiom quality — `@effect/vitest`, `TestClock`, layer-swapped doubles — is a
  real blind spot this audit does not cover.)

## Refresh when bumping Effect

When the `effect` / `@effect/platform` dependency version changes:

1. Re-check each source path still exists (modules get split/renamed across
   majors).
2. Re-check each docs slug still resolves under the docs clone (effect.website
   restructures; refresh the clone with `git -C ~/Development/references/effect-website pull`).
3. Add rows for newly-significant modules or smells; the lists are a seed, not a
   cage.
4. Edit the `DOMAINS` / `STRUCTURAL_SMELLS` / `MODELLING_SMELLS` / `BEHAVIOUR_SMELLS`
   arrays in `effect-native-audit.workflow.js` and these tables together.
