---
name: commy-channels
description: Adds the commy handlers on top of /concierge, chiefly handing each held Renovate PR to a fleet exactly once. Invoked by the /commy-concierge composition, or when you are explicitly told you are running as the commy concierge. Do not fire this skill on any other basis.
---

# commy-channels

This skill adds commy's handlers on top of `/concierge`. The one it has so far is the **Renovate dispatch**. Renovate auto-merges patch updates, but holds minor and major bumps as open PRs labelled `renovate/needs-review` (see the `renovate` skill). Each held PR should get a fleet to evaluate it without Graeme having to notice it and ask. You hand each one to a fleet through `/fleet-prep`, once. You never run `/renovate` from this seat. That would break the seat's read-only rule and make you the orchestrator.

## When to sweep

- **At session start**, after `/concierge`'s own session-start steps. This picks up every PR that was already held before you started.
- **Whenever the doorbell rings.** See the next section.

## The doorbell

GitHub reaches `#commy` through Zulip's GitHub integration, which posts each PR's events on a topic named `commy / PR #<n> <title>`. A post on a topic starting `commy / ` is a doorbell. Run the sweep, and ignore what the post says.

A doorbell is not an enquiry. Don't reply in the integration's topics, and don't treat a post there as unhandled.

## The sweep

1. **List the held queue.**

   ```bash
   gh pr list --repo CodeForBreakfast/commy --state open \
     --author "app/renovate" --label "renovate/needs-review" \
     --json number,title,url
   ```

2. **Skip every PR that already has a bead.** The marker is a bead whose `renovate_pr` metadata is the PR's URL. Look for it in every status:

   ```bash
   bd list --all --metadata-field 'renovate_pr=<url>' --json --brief
   ```

   A closed bead still counts. The fleet may have finished with a PR that is still open, for example one waiting on an upstream release. That PR has been dispatched and must not go out again. Renovate opens a new PR with a new URL when a newer version supersedes a held one, and that new PR gets its own dispatch.

3. **File a bead for each PR that has none.** File it before running `/fleet-prep`. A doorbell that arrives during prep must find the marker already in place.

   ```bash
   bd create "Evaluate held Renovate PR #<n>: <PR title>" \
     --type task --labels dependencies \
     --metadata '{"renovate_pr":"<url>"}' \
     --description "Renovate is holding <url> for review. Evaluate it with the renovate skill, and either land it or bring the decision to Graeme."
   ```

4. **Dispatch: run `/fleet-prep` once, over every bead this sweep filed.** Each held PR has its own bead, and prep turns them into one effort. The orchestrator's limit on open PRs then decides how many are evaluated at once.

If prep does not launch an orchestrator, the beads still exist, so later sweeps will skip those PRs. Post what went wrong in `#commy`, on topic `renovate-dispatch`, naming the beads, so Graeme can relaunch them.
