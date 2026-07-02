# Releasing commy

A commy release is driveable end-to-end except one step: the PR merge, which is
the maintainer's. A worker prepares a release-bump PR; the maintainer merges it;
CI does everything outward-facing (tag, npm publish, GitHub Release) from there.
No agent ever pushes a tag or publishes anything.

This is path B2: the merge is the single human trigger. The post-merge work
is fully automated in [`.github/workflows/release.yml`](../.github/workflows/release.yml),
so there is no maintainer-local "release-plugin" skill to depend on.

## The two halves

```
WORKER (this runbook)                         MAINTAINER    CI (release.yml)
─────────────────────                         ──────        ────────────────
1. decide the bump (semver)
2. edit the 7 lockstep version sites
3. write RELEASE-NOTES/<version>.md
4. open PR, drive `bun run check` green  ──►  merge PR  ──► detect release
                                                            verify 7-site parity
                                                            push commy-v<version> tag
                                                            publish npm (OIDC)
                                                            cut GitHub Release
```

Your deliverable is the PR. The merge fires the rest.

## Worker steps

### 1. Decide the bump

Sweep merged PRs since the last release tag:

```bash
git fetch origin --tags
last=$(git describe --tags --match 'commy-v*' --abbrev=0)
git log --oneline "${last}..origin/main"
```

Pick `MAJOR.MINOR.PATCH` per semver against what landed (behaviour change →
minor; fix only → patch; breaking → major). The previous version is `${last#commy-v}`.

### 2. Bump the 7 lockstep version sites

All seven must read the new version. Six are hand-edited; the seventh
(`uv.lock`) is regenerated, never hand-edited.

1. `clients/claude-code/.claude-plugin/plugin.json` — **canonical**; the tag,
   the npm artifact, and the detection logic all derive from this one.
2. `clients/claude-code/package.json`
3. `packages/mcp/package.json`
4. `packages/mcp/mcp-server.ts` — the `PLUGIN_VERSION` export
5. `clients/hermes/pyproject.toml`
6. `clients/hermes/commy/plugin.yaml`
7. `clients/hermes/uv.lock` — **regenerate**: after editing `pyproject.toml`,
   run

   ```bash
   cd clients/hermes && uv sync
   ```

   which updates the `commy-hermes` self-entry in `uv.lock`. Never hand-edit the
   lock — the hermes gate runs `uv sync` and a stale lock fails CI. `uv` lives
   in the flake dev shell (`nix develop`).

The first six are asserted in lockstep by
[`clients/claude-code/manifests.test.ts`](../clients/claude-code/manifests.test.ts);
that test now also asserts the `uv.lock` self-entry, so a forgotten `uv sync`
fails at the unit-test bar. A partial bump cannot land green.

### 3. Write the release notes

Create `RELEASE-NOTES/<version>.md` — see
[`RELEASE-NOTES/README.md`](../RELEASE-NOTES/README.md) for the convention and
template. This file is required: its presence is how CI decides the merge is
a release, and it is the body of the GitHub Release. Write it user-facing and
impact-classified, matching PRs [#90](https://github.com/CodeForBreakfast/commy/pull/90)
and [#93](https://github.com/CodeForBreakfast/commy/pull/93).

### 4. Open the PR and go green

```bash
git checkout -b release-<version>
git add -A
git commit -m "commy plugin: release <version>"
```

Drive the gate green **inside the dev shell** (the hermes gate needs `uv`):

```bash
nix develop --command bun run check
```

Open the PR against `main`. A green `bun run check` is the merge gate.

That's the whole worker deliverable. Stop here — do not push a tag or publish
anything. The merge is the maintainer's, and it triggers everything else.

## What CI does on merge (no action needed)

When the bump commit lands on `main`, `release.yml`:

1. **detect** — reads the canonical version, confirms `RELEASE-NOTES/<version>.md`
   exists and `commy-v<version>` is not already tagged. Logic is in
   [`scripts/release-detection.ts`](../scripts/release-detection.ts), unit-tested
   in `scripts/release-detection.test.ts`. An ordinary main push (no notes file,
   or already tagged) is a no-op.
2. **verify parity** — re-runs the seven-site lockstep test on the merged commit.
3. **tag** — creates and pushes `commy-v<version>` as the record.
4. **publish** — builds and publishes `@codeforbreakfast/commy-mcp` to npm via
   OIDC trusted publishing (no token).
5. **release** — cuts the GitHub Release from `RELEASE-NOTES/<version>.md`.

### Manual fallback

`release.yml` also has a `workflow_dispatch` trigger (Actions → Release → Run
workflow). It runs the same detection. Use it if the auto-trigger on merge does
not fire, or — with the `force` input — to recover a release that tagged but
failed before publishing. The notes file must still be present.

## Two constraints that must not be tripped

- **The npm publish step stays in `release.yml`.** npm's Trusted Publisher (OIDC)
  is pinned to the workflow **filename**. Renaming or moving the workflow makes
  npm reject the publish — that needs a maintainer-side npm-config change. If a
  rename ever seems unavoidable, stop and surface it.
- **CI triggers on the merge-to-`main`, not on the tag it pushes.** A tag pushed
  by the default `GITHUB_TOKEN` does not trigger another `on: push: tags`
  workflow, so the release must be driven directly off the release commit. Do not
  refactor this into a two-workflow "auto-tag fires the release" design — it
  silently will not fire.
