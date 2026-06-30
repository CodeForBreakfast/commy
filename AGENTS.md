# Agent Instructions

commy is a hexagonal MCP substrate for inter-agent communication — see
[docs/architecture.md](docs/architecture.md) for the architecture overview and
workspace package map, [docs/self-hosting.md](docs/self-hosting.md) for the
operator's environment contract, and [docs/](docs/) for the substrate rationale
([why Zulip](docs/why-zulip.md)), [bot naming conventions](docs/naming.md), and
the [inbound event contract](docs/claude-channel-inbound-contract.md). The
[README](README.md) is the consumer-facing front door.

## Build & Test

Requires [Bun](https://bun.sh) (version pinned in `package.json` under
`packageManager`).

```bash
bun install        # install deps
bun run check      # run all quality gates via turbo (typecheck → lint → test, cached)
bun run lint:fix   # biome auto-fix (not cached, interactive use only)
bun run test:live  # run live suite against a real Zulip realm
```

**Always use `bun run check`** for quality gates. The individual scripts
(`test`, `typecheck`, `lint`) refuse to run outside turbo — they exist only as
turbo task targets. This ensures turbo's dependency ordering and caching are
always in play.

**Run the gate inside the dev shell.** `bun run check` (and the pre-commit
hook) includes the `clients/hermes` Python gate (`//#test:hermes` →
`scripts/test.sh`: ruff + pytest), which needs `uv`. `uv` lives in the flake
dev shell — so work inside `nix develop` (or `nix develop .#ci --command bun
run check` to use the lean CI shell). A bare `bun run check` outside the shell
fails the hermes task with `uv: command not found` whenever its turbo cache is
cold. CI runs the gate this way too (`.github/workflows/ci.yml`).

**Live tests** (`*.live.test.ts`) hit a real Zulip realm and can rate-limit
other clients sharing it. They are excluded from default discovery via
`bunfig.toml` `[test] pathIgnorePatterns`. The `test:live` script clears that
override and targets the file explicitly. Live tests are env-gated inside the
file too — without `ZULIP_SITE` / `ZULIP_MINTER_*` / `ZULIP_LIVE_CHANNEL_*`
set, the suite skips silently.

**Lint layers, when to reach for which:**

- **biome** — JS/TS style, formatting, generic lint (unused imports, etc.). Run via `bun run check` (turbo cached) or `bun run lint:fix` for auto-fix.
- **@effect/language-service** — Effect idiom (floating Effects, untyped catches, unnecessary `Effect.gen`, reaching for global `fetch`/`Date`/`console` instead of Effect-native services, ~60 rules). TypeScript-plugin: registered in `tsconfig.json` under `compilerOptions.plugins`, persisted at build time via `effect-language-service patch` in the `prepare` script — so diagnostics surface under `tsc --noEmit` (i.e. `bun run check`), not just in editors. Project-scope rule tweaks live in the tsconfig plugin entry; document the reason inline when disabling.

## Contributing

Work on a branch and open a pull request against `main`. CI runs the same
`bun run check` gate inside the nix CI shell; a green gate is required to
merge. File bugs and feature requests as GitHub issues.

Cutting a release is its own flow — see [docs/releasing.md](docs/releasing.md):
bump the seven lockstep version sites, write `RELEASE-NOTES/<version>.md`, open
the bump PR. The maintainer's merge is the single trigger; CI tags, publishes to
npm, and cuts the GitHub Release.

Maintainers additionally track internal work in a
[bd (beads)](https://github.com/gastownhall/beads) tracker that is not part of
this repository — external contributors don't need it and should use GitHub
issues instead.

## Conventions & Patterns

### Effect style

This codebase is built on [Effect](https://effect.website). Two rules shape every change that touches Effect-aware code:

**1. Effects flow through to a single runtime call at the application's edge.** The destination is one Effect composed bottom-up — adapters return Effect, ports return Effect, application bootstrap composes them into one Effect, and a single `Effect.runPromise` / `Effect.runSync` lives at the MCP SDK boundary (the only place where the host environment demands a Promise). Don't sprinkle `Effect.runPromise`, `Effect.runSync`, `Effect.tryPromise`, `Schema.decodeUnknownSync`, or throwing brand constructors at internal seams to "keep this method Promise-returning for now." Each bridge fragments the typed E channel, resists composition, and creates churn when the surrounding code converts later.

If converting a function to Effect would force its callers to convert too, that's expected. Either grow the current change to absorb the propagation, or file a paired follow-up issue — never introduce a temporary bridge as a placeholder.

**2. Reach for Effect's built-in helpers before writing imperative code.** The Effect ecosystem has many helpers — `Schema.NonEmptyString`, `Schema.minLength`, `Schema.pattern`, `Schema.URL`, `Schema.transformOrFail`, `ParseResult.try`, `Effect.gen`, `Effect.retry`, `Schedule.*`, `Stream.*`, `@effect/platform`'s `HttpClient` — and they're usually shorter AND more correct (annotations, AST integration, proper error formatting) than the hand-rolled equivalent. Before writing a `Schema.filter` with manual `value.length === 0` checks, regex tests, or try/catches around standard parses (URL, JSON, Date), grep the relevant Effect source first.

If you have a local read-only Effect source clone available (path is machine-local), grep it directly: `grep -n '^export' packages/effect/src/Schema.ts` (or `Effect.ts`, `Stream.ts`, `Schedule.ts`, etc.) finds helpers fast. Same for `packages/platform/src/HttpClient.ts` and friends. Otherwise read the same files on GitHub (`Effect-TS/effect`).

**Don't use:**
- `Schema.decodeUnknownSync` — throws on failure; use `Schema.decodeUnknown` (returns Effect)
- `Effect.tryPromise` at the network edge — use `@effect/platform`'s `HttpClient` which is Effect-native
- Throwing brand constructors inside Effect code — brand validators that can fail return `Effect<Brand, ParseError>`

**3. Common imperative → Effect-native swaps.** These shapes recur; reach for the native one first, while writing — not after. This table is a digest: the full four-axis catalogue (substitution / structural / modelling / behaviour) lives in `.claude/skills/effect-native-audit/domains.md` and is swept periodically by `/effect-native-audit`.

| imperative shape | native shape |
|---|---|
| `x instanceof Error ? x.message : String(x)` | `Predicate.isError(x) ? …` |
| `typeof o === 'object' && o !== null && 'k' in o` + casts | `Predicate.hasProperty(o, 'k')` |
| `switch (x.kind)` (esp. with a hand-rolled `const _: never` default) | `Match.value(x).pipe(Match.discriminatorsExhaustive('kind')({…}))` |
| `arr.flat().sort((a, b) => a.n - b.n)` | `Arr.flatten` + `Arr.sort(Order.mapInput(Order.number, …))` |
| a validating constructor returning `T \| undefined` | return `Option<T>` (callers stop re-wrapping with `Option.fromNullable`) |
| a sentinel value or bare `throw` for a recoverable case | `Either` / `Option` |
| booleans + optional fields with a cross-field invariant | `Data.TaggedEnum` — make illegal states unrepresentable |
| a bare `string` / `number` domain id or constrained value | branded `Schema.brand`, decoded once at the boundary |
| `new Map<string, …>` keyed on `` `${a}\|${b}` `` | `Data.struct({ a, b })` key + `HashMap` / `HashSet` (value equality, no delimiter collisions) |
| a `new`-able class or config-field thread doing DI | a `Context.Tag` service declared in `R` |
| an injected `now()` / `sleep()` thunk | `Clock.currentTimeMillis` / `Effect.sleep` (default services; `TestClock` in tests) |
| reassigned `let` mutated across `yield*` points | `Ref` / `SynchronizedRef` |
| `try/finally` for cleanup | `Effect.acquireRelease` / `Scope` |
| independent effects run sequentially | `Effect.all` / `Effect.forEach` with bounded `{ concurrency }` — never `'unbounded'` onto a rate-limited Zulip realm |
| untrusted data via `as` / `JSON.parse` at the edge | `Schema.decodeUnknown` |

This complements the linters, it doesn't repeat them: `@effect/language-service` and biome catch per-line idiom; this table is for the whole-block / whole-type / whole-module shapes a linter can't see.
