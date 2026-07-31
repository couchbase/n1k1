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
- bottom-left row, in order — **⌂ home** (the whole-poster view),
  the **jump-to-slide picker** (full outline, indented by depth, and it
  tracks where you are), then **◀ prev / next ▶** for stepping without
  the keyboard. The row is styled to stay out of the slide's way while
  still signalling, to someone landing on the published page, that this
  is a deck to walk through.
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
- **Source slides** (`class="step src"`, `data-scale="0.2"`) are a third
  level: they hang off a *slide* rather than a panel
  (`data-rel-to="ext-udf"`), numbering as `7.1.1` … `7.4.1` — one per JS
  extension kind (UDF + aggregate, extract recipe, macro, the
  `vectorize_field` macro), holding code copied verbatim from
  `extensions/`. Two of them also show the **inline goldens** every
  extension ships (`.extensions test` runs them). Keep these cards under
  ~815px CSS tall or they overflow a 1080p viewport, and keep code lines
  inside their column — `pre` scrolls rather than wraps, which reads as
  truncation on a slide. `0.2` repeats the panel:slide ratio one
  level down — a source card is 1:5 against its parent just as a slide
  is 1:5 against its panel. `data-rel-x="360" data-rel-y="140"` puts it
  **inside the parent's lower-right quadrant** — covering only 4–6% of the
  parent at the panel view, yet deep enough in that the parent **fills the
  whole viewport** behind the code when the source slide is active. That
  overlap is the point: with less of it the parent slid off screen and
  7.1 → 7.1.1 read as a teleport. The budget, measured at 1080p: the
  active view spans only ±145 × ±82 world px while parent half-extents are
  550 × 236–292, so keep `|x| ≤ ~405` and `|y| ≤ ~154`.
- The parent also gets **`class="step src-parent"`**, a lighter surface
  tone, so what surrounds the code reads as a *surface* rather than as
  darkness. Three tones now encode depth: panel (section hue) → parent
  (lighter slate) → source card (darker). Add `src-parent` to any slide
  you give a `.src` child.
  Note `data-scale` never changes a slide's own on-screen size (the
  camera cancels it out — these render full size, code at ~20px); it
  only sets how magnified the *surroundings* are.
- **Camera steps** (`class="step camera"`, zero content): `#map` /
  `#overview` show the whole poster; the `#up-<section>` waypoints pull
  the camera back over a section's panel after its last slide, so
  moving to the next section reads as "up the hierarchy, then over".

### Camera framing &amp; the Z lift (readability vs grounding)

These two knobs trade off against each other, and both are tuned for a
back row reading projected text:

- **`data-width` / `data-height` on `#impress` is the camera framing**,
  not a pixel size — impress.js scales the world by
  `min(winW/width, winH/height)`. At the current `1450x816` an active
  card fills ~76% of screen width with ~24px body text on a 1080p
  projector (and ~32px on a 2560px display). Raise the numbers to pull
  the camera back, lower them to move in. Note impress.js's own default
  is `1920x1080`, which renders the deck at 1:1 — 18px body text, too
  small for a room.
- **`data-rel-z` lifts child slides toward the viewer** (1200; leaves
  2800) and is what makes that close framing affordable. Z does *not*
  change the active card's size — impress.js brings whatever step is
  active to the same plane — but it pushes the parent panel one
  perspective step further away, so the panel is drawn smaller and
  still frames the card instead of ballooning off-screen. Measured on
  a 1080p screen: without the lift ~15% of the panel's watermark
  numeral stays in frame; with it, ~80%, plus the panel's own heading
  sits above the card like a banner.
- **Leaves need a bigger lift** because `data-scale="0.4"` magnifies the
  whole world 2.5x when a leaf is active, which magnifies the parent
  too; 2800 compensates.

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

All four are wired up by the small script at the end of `index.html`;
the styling is section 7 of `deck.css`. They are meant to recede, but
their appearance is **constant** — no fade-in on hover. What keeps them
quiet is the palette, not opacity: nothing in the row uses the accent
color or pure white, and the brightest it ever gets is `--ink-dim`.

- The **picker is built from the steps themselves** at load time, so a
  slide you add shows up automatically — there is no list to maintain.
  Its label comes from the slide's `h2`/`h1` — read from a clone with
  `<br>` swapped for a space, so a two-line title doesn't run together —
  set `data-nav-label="…"` on a step to override. `.camera` steps are
  left out. Its width is **fixed**, not shrink-to-fit: an auto-sized
  picker would shift `prev`/`next` sideways on every step.
- **Outline numbers** are derived, not written down: a panel's number is
  its watermark numeral (`4`), and its slides continue it in document
  order (`4.1`, `4.2`, …). The counter keys off each slide's
  `data-rel-to`, not "the last panel seen", so moving a slide in the
  markup renumbers it correctly rather than silently misfiling it. The
  numbers carry the hierarchy on their own, so options are **not**
  indented — a closed `<select>` shows only the selected row, and leading
  spaces would show up in it as stray whitespace.
- **⌂ home** targets `#map` rather than the identical `#overview`,
  because `#map` sits early in the step order — so pressing `→` after
  going home resumes at section 1 instead of wrapping to the title.
  Change the `HOME` constant to retarget it.
- **◀ prev / next ▶** call `impress().prev()` / `.next()`, which wrap at
  both ends, so neither ever dead-ends.
- All four controls blur themselves after use, so the arrow keys keep
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
