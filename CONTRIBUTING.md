# Contributing to commy

Thanks for your interest in commy. This document covers the practicalities;
[AGENTS.md](AGENTS.md) holds the detailed build/test reference and the Effect
conventions every change must follow — read it (and point your coding agent at
it) before writing code.

## Getting set up

Requires [Bun](https://bun.sh) (version pinned in `package.json`) and
[Nix](https://nixos.org) for the dev shell (the quality gate includes a Python
sub-project that needs `uv`, which the flake provides).

```bash
bun install
nix develop .#ci --command bun run check
```

A green `bun run check` is the whole gate: typecheck, lint (biome +
@effect/language-service), and tests, orchestrated by turbo.

## Making changes

1. Fork and branch from `main`.
2. Follow test-driven development: a new feature or bugfix comes with tests
   that fail before the change and pass after it.
3. Keep changes small and focused; match the style of the surrounding code.
4. Run `bun run check` inside the dev shell before pushing.
5. Open a pull request against `main`. CI runs the same gate; it must be
   green to merge.

Bug reports and feature requests go through GitHub issues. For substantial
changes, open an issue to discuss the approach before investing in an
implementation.

## Live tests

`bun run test:live` exercises a real Zulip realm and needs `ZULIP_SITE` /
`ZULIP_MINTER_*` / `ZULIP_LIVE_CHANNEL_*` set (see
`packages/zulip/realm.live.test.ts` for the full list). You don't need this
for normal contributions — CI does not run it, and the default suite covers
the ports against the in-memory adapter. Run it only if your change touches
the Zulip adapter's wire behaviour and you have a realm to test against.

## Licence

By contributing, you agree that your contributions are licensed under the
[Apache-2.0 licence](LICENSE) that covers the project.
