# n1k1 — technical deck

A spatial, [impress.js](https://github.com/impress/impress.js)-based
presentation of n1k1's architecture and techniques, aimed at an audience
that knows database internals.

## Presenting

Open `index.html` in a browser — no server or network needed
(`impress.js` is vendored alongside).

- `space` / `→` — next step; `←` — previous (document order = narrative
  order)
- clicking a slide jumps to it — handy from the map steps
- the zoomed-out **map** appears right after the title and again at the
  end (`#map` / `#overview`)
- `H` — impress.js help popup

The fallback for browsers without CSS 3D support is a plain vertical
document, so the content is never trapped.

## Layout model (for editors)

The deck is one big top-down **poster**, not a linear stack:

- The **title** is bare text at the top center, huge.
- Eight **section panels** (`class="step hub"`, `data-scale="5"`) sit in
  two rows of four below it, read left-to-right in narrative order.
  A panel is a giant borderless card: a heading at the top, a faint
  watermark numeral, and deliberate empty space below.
- Each panel's **child slides** (`data-scale="1"`) sit *inside the
  panel's footprint*, positioned relative to the panel with
  `data-rel-to="<hub-id>"` + `data-rel-x`/`data-rel-y`
  (3-across rows at x = −1400/0/+1400, y = +350; 2×2 grids at x = ±800,
  y = −250/+900). Because children overlap their parent, the panel's
  tint and numeral stay visible around whichever child is active —
  that's what keeps you oriented.
- **Leaf** slides (fine print, `data-scale="0.4"`) park near the
  panel's bottom edge — smaller in presentation space, exactly because
  they are detail.
- **Camera steps** (`class="step camera"`, zero content): `#map` /
  `#overview` show the whole poster; the `#up-<section>` waypoints pull
  the camera back over a section's panel after its last slide, so
  moving to the next section reads as "up the hierarchy, then over".
- `data-max-scale="1"` on `#impress` keeps slides at 1:1 on large
  screens, which is what leaves a margin of parent-panel context
  visible around the active card.

To **add a slide**: copy a sibling `<div class="step" ...>`, keep its
`data-rel-to` pointing at the panel, and nudge `data-rel-x`/`data-rel-y`
(units are canvas pixels at scale 1; a slide is ~1100 wide). Document
order = presentation order, so place the div where it should occur in
the narrative — and keep it before the section's `#up-*` waypoint.

To **restyle**: everything visual is in `deck.css` (design tokens at the
top). Slide markup uses a small set of components — `.kicker`, `.stats`
/ `.stat`, `.cols`, `.pills`, `pre` code blocks with `.k`/`.s`/`.n`/`.c`
token spans, and `pre.diagram` for the phosphor-green ASCII diagrams.
Each panel picks its tint hue inline via `style="--hue:<deg>"`.

## Files

| file | what |
|---|---|
| `index.html` | the deck — hand-editable, one commented block per section |
| `deck.css` | all styling; design tokens (colors/fonts) at the top |
| `impress.js` | vendored impress.js (MIT), unmodified |

## Content sources

The slides distill `README.md`, `docs/design/DESIGN*.md`, and
`docs/blog.md`. Numbers on slides are quoted from
`docs/design/DESIGN-benchmark.md` and the per-topic design docs — if a
number changes there, it is stale here.
