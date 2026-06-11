# Bot naming conventions

Every bot the commy ports' `IdentityPort.acquire(name)` binds against
carries one of the names below. The name is the substrate-facing
`full_name` (Zulip's display name; Discord's app name) and the lookup
key adapters use when finding-or-minting.

Two design rules:

1. **Sanitise once at the boundary.** Adapters may need to derive a
   substrate-specific short identifier from the name (Zulip's
   `short_name`, used to construct bot emails). Replace `/` with `-`
   (or `--` for round-trippable reversibility). The name itself is the
   canonical handle; the substrate identifier is downstream.
2. **Slot-reuse is acceptable for ephemeral names.** When a worker-pool
   slot is released, the same name can rebind to a new physical
   process later. Disambiguation lives in the message body
   (issue ID, request ID), not the identity.

## Categories

| Category                                   | Format                                  | Example                | Lifecycle                                                                                                                                                                                                  |
| ------------------------------------------ | --------------------------------------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Stable bots (concierges, scheduled skills) | `<role>`                                | `myproject-concierge`, `daily-brief` | Persistent. First boot acquires (mints on Zulip); subsequent boots acquire (regenerates key). One name = one logical bot for the lifetime of the project.                                              |
| Orchestrated crew (named agents under an orchestrator) | `<orchestrator>-<agent>`    | `buildfarm-witness`    | Persistent per orchestrator. Same acquire-on-startup pattern as stable bots. The orchestrator prefix matches whatever canonical identity the orchestrator routes by.                                    |
| Worker pools (ephemeral, recyclable slots) | `<orchestrator>-worker-<N>`             | `buildfarm-worker-3`   | Ephemeral, recyclable slot. Bounded by pool size; two physical processes can post as the same name over time (acceptable — the message body disambiguates).                                              |
| Claude Code sessions                       | `cc-[<project>-]<first-8-of-session-uuid>` | `cc-myproject-5c319b9b`, `cc-5c319b9b` | Ephemeral, **lazy-acquired** and **per-conversation** — the suffix derives from the CC `session_id` injected by the plugin's PreToolUse hook, so `/clear` mints a fresh bot. `<project>` is resolved per tool call (see derivation rules below) and embedded in every name the session mints; absent it, names fall back to bare `cc-<8>`. Lurking sessions (read / subscribe only) never mint a bot. `release()` runs on session transition and clean exit only if acquire happened; an out-of-band sweeper handles dirty exits. |

## Validity rules

Names that survive every substrate's identifier rules:

- Lowercase ASCII letters, digits, `-`.
- Start with a letter.
- Length ≤ 24 characters (Zulip bot full_name displays cleanly; Discord
  app names cap at 32).

`/` is the only mandatory transformation; other special characters are
the caller's responsibility to avoid. If a name contains a slash,
adapters substitute `-` (lossy) or `--` (round-trippable) per their
ergonomic preference — document the choice in the adapter.

## Project derivation for CC sessions

The `<project>` slot in `cc-<project>-<8>` is resolved **per
attribution-producing tool call**, from the *calling* session's cwd.
The CC PreToolUse hook injects the harness `cwd` into the tool
call's `arguments` alongside `session_id`; the plugin reads the cwd
at the boundary in `tools.ts` and passes the derived project to the
identity cache. (Deriving once at plugin boot from the plugin's own
process cwd would leak the plugin's install location into every minted
name regardless of where the calling session was operating.) Precedence:

1. **`COMMY_PROJECT` env var** — operator override, set in the
   plugin process's environment. Authoritative when set: every call
   from this MCP child mints `cc-<that-slug>-<8>`. Use sparingly —
   it disables per-session derivation entirely. If the value
   sanitises to nothing usable, derivation returns `undefined`
   rather than falling through.
2. **Git remote origin basename of the caller's cwd** — stable across
   worktree paths and renames. Misses local-only or non-repo
   projects.
3. **Git root basename of the caller's cwd** — `git rev-parse
   --show-toplevel`'s basename. Catches local-only repos; better
   than cwd basename because `~/myproject/scripts/` resolves to
   `myproject`, not `scripts`.
4. **Undefined** — non-project cwds (`/tmp`, `$HOME`), or sessions
   whose cwd never reaches the plugin (non-CC clients without the
   `cwd` arg), fall through to bare `cc-<8>`.

Sanitisation, applied uniformly at each step:

- Lowercase.
- Replace `/` and `_` with `-`; strip other non-`[a-z0-9-]` chars.
- Collapse consecutive `-`; trim leading/trailing `-`.
- Truncate to 12 chars (`cc-` + `<project>` + `-` + 8-char suffix = 24
  max). Mid-word truncation is acceptable — the 8-char suffix
  disambiguates anyway. A trailing `-` produced by truncation is
  trimmed.
- Must start with a letter post-sanitisation; otherwise the slug is
  dropped and derivation falls through (env value case) or returns
  `undefined` (git/cwd case).

| Cwd / source                          | Derived project    | Resulting name                  |
|---------------------------------------|--------------------|---------------------------------|
| `~/myproject`                         | `myproject`        | `cc-myproject-<8>`              |
| `~/Development/commy`                 | `commy`            | `cc-commy-<8>`                  |
| `~/nixos-config` (12 chars)           | `nixos-config`     | `cc-nixos-config-<8>` (24 max)  |
| `/tmp`                                | _undefined_        | `cc-<8>`                        |
| `COMMY_PROJECT=custom-name`           | `custom-name`      | `cc-custom-name-<8>`            |

## Why these conventions, not just free-form strings

Two reasons:

- **Recognisability in the substrate.** A human glancing at the
  member directory should be able to tell a pool worker from a
  daily-brief skill without context. The prefix gives them that
  classification at a glance.
- **Slot-reuse correctness.** Ephemeral pools (workers, sessions)
  rebind a name to a fresh credential on each acquisition cycle.
  Without a stable name shape, the pool manager couldn't tell whether
  it's looking up an existing slot or minting a new one.
