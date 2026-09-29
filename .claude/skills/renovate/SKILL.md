---
name: renovate
description: Use when explicitly asked to triage, review, evaluate, drain, or process Renovate dependency PRs — the held minor/major bumps Renovate opens but does not auto-merge. Triggers on phrases like "review the renovate PRs", "triage renovate", "drain the dependency queue", "evaluate the held bumps". Not for cutting a release or for the patch updates Renovate already auto-merges.
---

# Evaluate held Renovate PRs

Renovate auto-merges the safe updates on its own — patch, digest, pin and
lock-file maintenance flow unattended once CI is green (`.github/renovate.json5`
sets `automerge: true` by default, and the 7-day age gate plus the CI
install-age gate stop a fresh release from ever going green). What it
deliberately does **not** auto-merge are **minor and major** bumps: those can
carry behaviour changes, so the config holds them as ordinary open PRs labelled
`renovate/needs-review`. This skill is the human-grade evaluation layer for that
held queue.

There is no automatic hand-off — Renovate opens the PR and leaves it. This skill
is the pull side: invoke it to evaluate one held PR, and it merges the bump or
raises the decision it can't make.

## The contract

**One PR per invocation, lowest-risk first, then exit.** Run the skill again to
take the next one. Processing a single PR end-to-end keeps each evaluation
focused and each merge independently reviewable; draining the whole queue in one
pass blurs which bump caused which result.

## 1. Discover the queue

```bash
gh pr list --repo CodeForBreakfast/commy --state open \
  --author "app/renovate" --label "renovate/needs-review" \
  --json number,title,labels,mergeable,statusCheckRollup
```

Renovate runs as the `app/renovate` GitHub App. If the queue is empty, say so
and stop — there is nothing to evaluate.

## 2. Pick one, by risk

Order the queue and take the **first**:

1. **minor** before **major** — minor is lower semver risk.
2. Within a tier, prefer one whose CI (`check`) is already **green** — a red
   check usually means real work, and a green minor is often a clean merge.

## 3. Evaluate the bump

Read the PR body first — Renovate embeds the upstream changelog and release
notes directly. Then size the scope:

- **Minor, CI green:** a quick read. Confirm the changelog shows no behaviour
  toggle or deprecation that touches how we use the package. If clean, go to
  step 6.
- **Major, or a minor whose CI is red:** real research.
  - Read the changelog and the **migration guide** (PR body first; fetch the
    upstream release / `CHANGELOG` if the body is thin). Focus: breaking
    changes, removed/renamed APIs, deprecations, and required config changes.
  - `grep` the codebase for the package's imports and call sites to size the
    blast radius.
  - **If the fixes are small** — a couple of renamed symbols, a changed
    option — make them on the PR branch and carry on. A major that lands with
    small fixes needs no decision.
  - **If the breaking changes are extensive** — a real migration — **stop. Do
    not merge.** Raise it as a decision (see *Decisions*). A held major is a
    fine outcome; a half-done forced major is not.

## 4. Incompatible-dependency protocol

The most common reason a bump's gate fails is **not our code** — a peer
dependency in the tree doesn't support the new version yet. That is not ours to
force.

- Confirm the failure is a peer/ecosystem constraint, not our usage.
- Comment on the PR naming the blocking dependency, **leave the PR open**
  (Renovate keeps rebasing it as the ecosystem catches up), and move on. This
  is not a decision, so don't raise it as one.
- **Never** force-merge past it, and never downgrade or pin the peer to make it
  pass. Wait for upstream.

## 5. Keep the upgrade PR minimal

When research surfaces that the new version lets us delete a workaround, adopt a
simpler API, drop a deprecated call, or use a new capability worth adopting —
**don't do it in the upgrade PR.** File a separate follow-up bead for each
opportunity and keep the bump PR to the minimum that makes it mergeable. This
keeps the upgrade reviewable and decouples "upgrade the dependency" from "adopt
its new features."

Dedup follow-up beads by **package name, not PR number** — Renovate closes and
reopens PRs for the same update under new numbers, so a PR-number key
double-files. Put the package name in the bead title, because `bd search`
matches titles, and search for it before filing.

## 6. Gate, then merge or report

Run the repo gate the same way CI does:

```bash
nix develop .#ci --command bun run check
```

- **Green and the evaluation is clean:** read the PR's author and owner, then
  merge. Run the read as its own command before any merge command, so the
  transcript shows both —

  ```bash
  gh pr view <number> --repo CodeForBreakfast/commy --json author,url
  ```

  The author must read `app/renovate` and the url must carry the
  `CodeForBreakfast/commy` owner and repo. A `renovate/...` branch name proves
  neither. If the author is anyone else, stop and raise a decision.

  Then enable auto-merge, so the PR lands when CI on the head commit is green —

  ```bash
  gh pr merge <number> --repo CodeForBreakfast/commy --auto --squash
  ```

  GitHub refuses `--auto` on a PR that is already clean. In that case merge it
  directly, still without `--admin` —

  ```bash
  gh pr merge <number> --repo CodeForBreakfast/commy --squash
  ```

  `--auto` merges only once the branch ruleset's required `check` passes; it
  cannot bypass branch protection, so this is the gate enforcing itself, not a
  shortcut around it. A direct merge is held to the same ruleset.
- **Not confident, or breaking changes are extensive:** leave the PR open and
  raise it as a decision.

## Decisions

Merge every bump you can land without a maintainer's judgement. Two outcomes
need one:

- **A major whose migration is extensive** — someone has to choose when to
  schedule the work.
- **A bump you are not confident in** — the gate is green or nearly so, but
  something in the changelog or the code leaves the call open.

For each, write what the upgrade needs and what the choice is, in one message a
maintainer can answer in one read. Raise it wherever you take decisions from
the person you work for. If you have nowhere else, post it as a comment on the
PR.

## Guardrails

- **Never force-merge.** `--auto` respects the ruleset; a manual merge that
  skips a red gate does not — don't.
- **Ecosystem-blocked majors stay open.** A peer-dep constraint is upstream's to
  resolve; leave the PR for Renovate to keep rebasing (step 4).
- **Patch / digest / pin / lock-file maintenance are not this skill's job** —
  Renovate auto-merges those. This skill only touches `renovate/needs-review`.
