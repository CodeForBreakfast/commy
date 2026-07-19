# Design

Visual system for commy's brand surfaces (landing site, README art direction).
Captured from the approved landing-page comp (2026-07-03). The world is a
1920s constructivist street poster framing modern machinery: Rodchenko/El
Lissitzky composition, true vermilion and ink on aged stock, transcripts
rendered as the "imagery".

## Color

Single-theme by deliberate choice — a poster is its own world; it does not
re-skin for dark mode.

| Token | Value | Role |
|---|---|---|
| `--red` | `#C42B10` | Vermilion — the brand color. Carries whole sections (drenched), the wedge, kickers, display accents. |
| `--red-deep` | `#9E2109` | Vermilion for small text on stock (contrast-safe) and hover states. |
| `--ink` | `#16120D` | Warm carbon black — text, borders, hard shadows, inverted sections. |
| `--stock` | `#E8DCC3` | Aged poster stock — the default ground. |
| `--stock-deep` | `#DACBA8` | Shadowed stock — alternate section ground, muted text on ink. |
| `--chalk` | `#F0E7D2` | Highlight paper — cards, type on ink/red. |
| `--ochre` | `#A87B23` | Sparing fourth — avatar variation only. |

Strategy: **committed-to-drenched**. At least one full section carries a red
ground; at least one carries ink. Stock is the connective tissue, never the
whole page. Semantic transcript colors (mention amber `#E8A03C`) live inside
the chat machinery only.

Contrast floors: 4.5:1 body and small labels, 3:1 display type ≥18px.
Muted small text on stock uses solid mixed values (e.g. `#5C5445`), never
opacity on ink.

## Typography

| Role | Face | Usage |
|---|---|---|
| Display | **Anton** (embedded woff2, data URI) | Uppercase only. Hero to h3, poster slogans, avatar initials. Line-height ≤1.05, letter-spacing ≥ 0. |
| Label | **Archivo Narrow 600** (embedded) | The kicker, nav, machine tags, captions. Uppercase, tracked 0.1–0.34em. |
| Body | system grotesque (`Helvetica Neue, Helvetica, Arial`) | Prose. 17px base, line-height 1.55 (1.65 light-on-dark). Max measure ~65ch. |
| Data | `ui-monospace` stack | Transcripts, commands, code chips. |

Fonts ship as data-URI `@font-face` — no CDN requests, identical render
everywhere.

## Layout & geometry

- **Hard geometry:** 3px ink borders, offset hard shadows
  (`8–10px 8–10px 0`), zero border-radius anywhere.
- **Diagonal composition:** the red wedge (El Lissitzky) drives into the
  hero machinery; pasted elements tilt ±0.4–1.1°.
- **Section rhythm:** grounds alternate stock → red → stock-deep → ink →
  stock; 4px ink rules separate stock-on-stock transitions.
- **One kicker.** "Seize the means of communication" above the hero h1 is
  the only tracked-caps eyebrow. Other sections open with the h2 itself,
  a fold-in slogan (`<span class="red">`), or a vertical marginal label —
  never a repeated eyebrow grammar.
- Vertical `writing-mode: vertical-rl` marginal labels are a signature
  device; use sparingly (one per page).

## Components

- **Transcript machinery** — near-black (`#1C1712`) panel, red topic header
  bar (`#channel ▸ topic`), square avatar blocks (Anton initial; red =
  agent, chalk = human, ochre = variation), Archivo name + mono
  machine-tag, mono reaction chips, tracked-caps syslines with rules.
- **Dispatch card** — chalk ground, ink header band with red em-word,
  mini transcript, 2px-rule caption. Pasted tilt; hover lifts against the
  shadow.
- **Mini-poster** — ink ground, chalk border, diagonal red hatch
  (`repeating-linear-gradient`), centered Anton stack.
- **Install chip / codeblock** — ink ground, chalk mono text, red `$`
  prompts, red offset shadow.

## Motion

- Ease: `cubic-bezier(0.22, 1, 0.36, 1)` (out-quint family). No bounce.
- **Hero choreography** (once, on load): copy children rise staggered
  ~80ms; wedge slides in from the left.
- **Transcript replay**: typing indicator → message rise, ~2.4s cadence,
  loops after a 7s hold; starts on intersection.
- Hovers: cards lift 3px against a grown shadow; nav underlines in red.
- `prefers-reduced-motion`: replay renders complete and static, entrance
  animations and transitions disable. Non-negotiable parity.

## Pages

The system above is defined by the landing page (`index.html`) and inherited
whole by every other surface. A new page adopts the palette, the type scale,
and the hard geometry without variation; what it may add is a component.

### Field manual (`field-manual/index.html`)

Seven patterns for a working collective. Inherits the full palette and type
system and holds the zero-radius rule throughout. It reuses the **transcript
machinery** unchanged — the pattern evidence is rendered threads, per the
show-don't-tell principle.

It carries **no kicker and no vertical marginal label**. Both are one-per-page
landing signatures; a page that is all body has nowhere to spend them, and
repeating them across pages would turn a signature into a grammar.

Its own components:

- **Pattern entry** — numbered display heading over a stated *shape* and the
  *itch* it scratches, then the evidence thread.
- **State chips** — mono chips tagging a pattern's coordination state
  (`pending`, `solo`).

## Voice in copy

Display type speaks propaganda ("No central committee", "Enlist"); body
copy speaks engineering (attribution, trust boundary, `node ≥ 23.6`).
Transcripts use real substrate semantics — channels, topics, handoff state
dumps, bead-style references — never invented UI affordances.
