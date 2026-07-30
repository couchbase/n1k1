# n1k1 — technical deck

A spatial, [impress.js](https://github.com/impress/impress.js)-based
presentation of n1k1's architecture and techniques, aimed at an audience
that knows database internals.

Published at **<https://couchbase.github.io/n1k1/deck/>** by
[`.github/workflows/pages.yml`](../../.github/workflows/pages.yml) on any
push to `master` under `docs/deck/**` — the three files are copied
verbatim, since there is nothing to generate.

## Presenting

Open `index.html` in a browser — no server or network needed
(`impress.js` is vendored alongside).

- `space` / `→` — next step; `←` — previous (document order = narrative
  order)
- clicking a slide jumps to it — handy from the map steps
- the zoomed-out **map** appears right after the title and again at the
  end (`#map` / `#overview`)
- bottom-left **⌂ home** — jump to the whole-poster view at any time
- bottom-left **jump-to-slide picker** — the full outline, indented by
  depth; it also tracks where you are as you present
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

### The bottom-left controls

Both are wired up by the small script at the end of `index.html`; the
styling is section 7 of `deck.css`.

- The **picker is built from the steps themselves** at load time, so a
  slide you add shows up automatically — there is no list to maintain.
  Its label comes from the slide's `h2`/`h1` (a section panel gets its
  watermark numeral prefixed); set `data-nav-label="…"` on a step to
  override. Indent depth is derived: section panel = 0, a slide with
  `data-rel-to` = 1, a `.leaf` = 2. `.camera` steps are left out.
- **⌂ home** targets `#map` rather than the identical `#overview`,
  because `#map` sits early in the step order — so pressing `→` after
  going home resumes at section 1 instead of wrapping to the title.
  Change the `HOME` constant to retarget it.
- Both controls blur themselves after use, so the arrow keys keep
  driving the deck. Their own key events are stopped from reaching
  `document`, since impress.js binds arrows/space/tab and `H` there
  without checking whether a form control has focus. (One consequence
  of using a real `<select>`: while it has focus, native type-ahead can
  change the selection — and therefore navigate.)

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
