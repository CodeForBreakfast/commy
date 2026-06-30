# Release notes

One file per release: `RELEASE-NOTES/<version>.md`, where `<version>` is the
`MAJOR.MINOR.PATCH` the release bumps to (e.g. `RELEASE-NOTES/0.16.0.md`).

This file is **load-bearing**, not decoration. The release-bump PR checks it in,
and on merge to `main` the [`Release` workflow](../.github/workflows/release.yml)
uses it two ways:

1. Its presence (alongside an absent `commy-v<version>` tag) is how
   `scripts/release-detection.ts` decides the push is a real release — no notes
   file, no automated release.
2. It is the body of the cut GitHub Release (`gh release create --notes-file`).
   The Release ships exactly what you write here, so write it for the reader.

See [`docs/releasing.md`](../docs/releasing.md) for the full worker flow.

## Style

User-facing and impact-classified — what changed for someone running commy, not
a commit log. Match the curated tone of
[#90](https://github.com/CodeForBreakfast/commy/pull/90) and
[#93](https://github.com/CodeForBreakfast/commy/pull/93):

- Lead with a one-line framing of the release (`Minor release … 0.15.0 → 0.16.0`).
- **Highlights** — the changes a user would notice, each with a short paragraph
  explaining the behaviour change and why it matters. Group by impact, not by PR.
- **Maintenance** — dependency bumps, lockfile maintenance, internal-only changes
  worth recording but not headlining.
- Note anything a consumer must do (config change, migration). If nothing, say so.

## Template

```markdown
commy plugin: release <version>

<Minor|Patch|Major> release of the commy plugin, **<previous> → <version>**.

## Highlights

**<Headline change>.** What changed for the user, and why it matters. Keep it to
the behaviour, not the implementation.

## Maintenance

- <dependency / lockfile / internal change>

---

Seven-way version parity bumped in lockstep (enforced by `manifests.test.ts`).
```
