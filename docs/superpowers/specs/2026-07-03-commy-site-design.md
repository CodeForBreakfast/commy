# commy public site — design

**Date:** 2026-07-03
**Bead:** comms-t6xq
**Status:** approved mockups; this doc records the decisions and the build contract.

## Goal

Give commy a public face that communicates its value for collaborative
agentic work: a static site at **commy.social** plus a "trailer" README.
The differentiator is the operating model, not a feature list — plain
Claude Code sessions plus the substrate make a collective; no agent
framework, no gateway, no orchestrator in the middle.

**Positioning (ruling, 2026-07-03):** your agents are colleagues on a team
chat you own — you talk to them the way you talk to each other, and they
talk to each other the same way. Thread durability is a supporting
consequence (a fresh session catches up like a new hire, by reading the
thread), never the headline.

Brand register and visual system are specified in `PRODUCT.md` and
`DESIGN.md` at the repo root (constructivist poster identity: vermilion
`#C42B10`, ink, aged stock, Anton + Archivo Narrow, hard geometry,
single-theme by choice).

## Decisions

| Decision | Choice | Rejected |
|---|---|---|
| Site tech | Hand-rolled static HTML — the approved mockups *are* the site | Astro, Eleventy (YAGNI until the field manual grows past one pattern; migrate then) |
| Hosting | GitHub Pages via Actions deploy | Deploy-from-branch `/docs` folder (clashes with real docs) |
| Domain | **commy.social** (registered at Hostinger, 2026-07-03) | commy.chat/.red/.party — .social wins on the double meaning (socialist + social software), category honesty, and the fediverse self-hosted connotation; .party carries spam-list baggage; commy.dev/.app taken |
| README | Trailer-with-install | Bare logo+tagline+link (GitHub is still where evaluators land; making them click through to learn anything costs installs) |

## Pages (v1)

Source of truth: the two approved mockups (session artifacts). Both are
fully self-contained — fonts embedded as data-URI woff2, no external
requests.

1. **Landing** (`/`) — hero ("Your agents are colleagues. Talk to them
   like it."), animated thread replay with reduced-motion fallback, three
   vignette cards (handover / unblock / delegation), machinery diagram,
   campaign posters, self-hosting section, install.
2. **Field manual** (`/field-manual/`) — foreword, the two apparatus
   schemes (wiring, colleagues), pattern index (№ 1–7), pattern № 2
   written in full. Patterns № 1 and 3–7 ship marked **"to be issued"** —
   a numbered pamphlet series with forthcoming issues is on-register and
   honest about state.

## Repository layout

```
site/
  index.html            # landing
  field-manual/
    index.html
  assets/
    poster-march.webp   # extracted from data URIs — cacheable, keeps HTML reviewable
    poster-key.webp
  CNAME                 # "commy.social"
.github/workflows/pages.yml
```

- Fonts stay as data-URI `@font-face` (small, guarantees identical
  render); the two ~200 KB posters move to files.
- Adaptation from artifact to site: add the full document skeleton
  (`<!doctype html>`, `<head>`), meta description, Open Graph / Twitter
  card tags (poster crop as OG image), favicon (red wedge on ink),
  canonical URLs.
- No analytics — no central committee watches you read. No cookies, no
  external requests at all; the CSP-clean artifact discipline carries
  over as a feature.

## Deploy & DNS

- Workflow: on push to `main` touching `site/**`, upload
  `actions/upload-pages-artifact` from `site/` and `actions/deploy-pages`.
  No build step — the artifact is the directory.
- GitHub Pages settings: source "GitHub Actions", custom domain
  `commy.social`, enforce HTTPS.
- DNS at Hostinger (operator action, one-time): four apex `A` records to
  GitHub Pages IPs (`185.199.108.153`–`111.153`), `www` CNAME to
  `codeforbreakfast.github.io`. Verify the domain under the
  CodeForBreakfast org settings (TXT challenge) before going live so the
  apex can't be claimed by another Pages site.

## README slim-down

Current README is 139 lines; this is a refocus, not a gut.

**Keeps:** hero poster art, install block ("Installing — enlist your
agents", including the non-Claude-Code path), licence/versioning footer.

**Changes:**
- Lede becomes the approved landing paragraph ("commy puts every coding
  agent — and the humans toiling alongside them — on one team chat…").
- "What the collective gets you" collapses to one short rendered thread
  (fenced, text form) as the proof, replacing feature prose.
- "Bring your own realm" and "How it's built" reduce to two sentences
  each plus links into `docs/self-hosting.md` / `docs/architecture.md`.
- Add a prominent link to **commy.social** near the top.

## Accessibility

WCAG 2.2 AA per `PRODUCT.md`: ≥4.5:1 body contrast (≥3:1 large display
type), visible focus, semantic landmarks, `prefers-reduced-motion`
parity (replay renders static; entrance choreography disables).
Single-theme pages must stay legible regardless of OS theme (the pages
are self-grounded — every element sits on an explicit ground).

## Verification

- Local: open both pages from `file://` (self-containment check), click
  every link, tab through focus order, toggle reduced-motion.
- Post-deploy: `curl -I https://commy.social` for HTTPS + headers; OG
  tags via a card validator.
- No test infrastructure added for static pages; CI remains the existing
  `bun run check` gate, untouched by `site/**`.

## Non-goals (v1)

- No static-site generator, JS framework, or build step.
- No dark theme — single-theme by deliberate choice (DESIGN.md).
- No analytics or third-party embeds.
- No docs migration: `docs/*.md` stay in the repo; the site links to
  them on GitHub.
- Field-manual patterns № 1, 3–7 are not written in v1.
