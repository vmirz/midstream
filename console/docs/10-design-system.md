# 10 — Design System

Sources: `src/styles.css`, `src/components/`.

## 10.1 Design principles

The visual language targets the register of Apple and OpenAI product surfaces: quiet, dense where
it matters, and never decorative for its own sake.

1. **Data is the interface.** Chrome recedes; numbers, phases, and field paths are the only things
   with visual weight.
2. **One accent colour.** Blue is used for links, focus, and the current selection — nothing else.
   Status colour is reserved for status.
3. **Borders, not shadows.** Elevation appears only on genuinely floating surfaces (dialogs,
   toasts, dropdown affordances).
4. **Tabular numbers everywhere.** Live values change constantly; proportional digits would make
   the layout jitter.
5. **Small type, generous space.** Body text is 12–13 px with comfortable line height and padding,
   which is what makes a dense operational tool feel calm.
6. **Motion only to signal liveness.** One 2-second pulse on live indicators, one 180 ms fade for
   entering surfaces. Both disabled under `prefers-reduced-motion`.

## 10.2 Tokens

All tokens are CSS custom properties declared in an `@theme` block, with dark values overridden
under `.dark`. Tailwind 4 consumes them directly.

### Colour

| Token | Light | Dark | Use |
|-------|-------|------|-----|
| `--color-canvas` | `#ffffff` | `#08080a` | Page background |
| `--color-surface` | `#ffffff` | `#0f0f12` | Panels, sidebar, header |
| `--color-raised` | `#fafafa` | `#151519` | Table headers, hover, chips |
| `--color-line` | `rgba(9,9,11,.08)` | `rgba(255,255,255,.09)` | Default border |
| `--color-line-strong` | `rgba(9,9,11,.14)` | `rgba(255,255,255,.16)` | Inputs, scrollbar thumb |
| `--color-ink` | `#09090b` | `#f4f4f5` | Primary text |
| `--color-ink-muted` | `#6b7280` | `#9ca3af` | Secondary text |
| `--color-ink-faint` | `#9ca3af` | `#6b7280` | Labels, placeholders, null values |
| `--color-accent` | `#0a6cff` | `#3b8cff` | Links, focus, selection |
| `--color-accent-soft` | 10% accent | 14% accent | Selection background |

### Phase colour

| Token | Light | Dark | Phases |
|-------|-------|------|--------|
| `--color-phase-running` | `#059669` | `#34d399` | `running`, healthy |
| `--color-phase-pending` | `#2563eb` | `#60a5fa` | `requested`, `discovering`, `discovered`, `deploying` |
| `--color-phase-paused` | `#b45309` | `#fbbf24` | `stopped`, `paused`, `deleting`, degraded health |
| `--color-phase-failed` | `#dc2626` | `#f87171` | `failed`, `degraded`, `unreachable` |
| `--color-phase-idle` | `#6b7280` | `#9ca3af` | `deleted`, unknown |

Backgrounds are derived with `color-mix(in srgb, <token> 12%, transparent)`, so a tone stays
consistent across themes without a second token.

### Typography, radius, motion

```css
--font-sans: ui-sans-serif, -apple-system, BlinkMacSystemFont, 'SF Pro Text', 'Segoe UI', Inter, system-ui, sans-serif;
--font-mono: ui-monospace, 'SF Mono', 'JetBrains Mono', Menlo, Consolas, monospace;
--radius-control: 0.5rem;    /* buttons, inputs, chips */
--radius-panel:  0.875rem;   /* panels, cards */
```

System fonts mean zero webfont payload and native rendering on every platform. `font-feature-settings: 'cv02','cv03','cv04','ss01'` refines digit and letter shapes where the platform font supports it.

Type scale in practice: `20px/600` page titles, `15px/600` dialog titles, `13px/600` panel titles,
`13px` body, `12px` secondary, `11px/uppercase/tracked` labels, `22px/600` stat values.

Three animations, all respecting `prefers-reduced-motion`:

| Class | Effect |
|-------|--------|
| `.live-dot` | `console-pulse` — 2 s opacity 1 → 0.35 → 1 |
| `.fade-in` | `console-fade-in` — 180 ms opacity + 4 px rise |
| `.run-ring` | `run-travel` — a 5 s beam around the perimeter of whatever is being filled |

### `.run-ring` — the running indicator

An overlay on the panel being filled, not on the control that started it: the grid is where the eye
already is. It is drawn **above** the content so nothing opaque inside the panel — a header row, the
column rail — can sit on top of the edge it runs along.

The beam rides a **motion path**, not a rotating conic gradient:

```css
offset-path: inset(1px round calc(var(--radius-panel) - 1px));
offset-rotate: auto;
animation: run-travel 5s linear infinite;
```

A conic gradient turns at a constant *angular* rate, which on anything but a square traces the
perimeter at a wildly varying *linear* rate — the head crawls along the long edges and snaps through
the corners. On a grid twice as wide as it is tall that reads as a stutter rather than as motion.
`offset-path` is uniform: measured at **39 px per 1% of the lap on all four edges**.

| | Before | After |
|---|---|---|
| Lap | 1.6 s (~2,500 px/s on a full-width grid) | 5 s (~800 px/s) |
| Speed around the perimeter | varies by edge | constant |
| Shape | hard-edged wedge, seam at 0° | soft gradient tail, brightest at the leading edge |
| Between passes | nothing | a faint constant accent ring |

Two details that are easy to get wrong:

- **Inset by half the beam's thickness.** `inset(0 …)` centres the beam *on* the border line, which
  the panel's own `overflow: hidden` then cuts in half. One pixel in, the whole beam is visible.
- **The fallback still needs its mask.** Browsers without basic shapes on `offset-path` get the
  original conic sweep, slowed to 4 s and softened, reduced to a ring by the two-layer XOR mask.
  Drop that mask and the fallback renders a rotating gradient over the *whole panel*. The mask lives
  on the base rule and is removed inside `@supports (offset-path: inset(0 round 1px))`.

Under `prefers-reduced-motion` there is **no travel at all** — slowing a lap down still sends a
bright object across the screen. The ring breathes in place instead, which carries the same "still
working" meaning without moving.

### Utility classes

| Class | Definition |
|-------|-----------|
| `.panel` | `--color-surface` background, `--color-line` border, `--radius-panel` |
| `.numeric` | Monospace + `font-variant-numeric: tabular-nums` + `-0.01em` tracking |

## 10.3 Theming

The theme is applied **before first paint** by an inline script in `index.html`:

```js
const stored = localStorage.getItem('console.theme');
const dark = stored === 'dark' || (stored === null && matchMedia('(prefers-color-scheme: dark)').matches);
document.documentElement.classList.toggle('dark', dark);
```

Unset follows the OS; an explicit choice wins and persists. `<meta name="color-scheme" content="light dark">` makes native form controls and scrollbars match.

Dark mode is wired with `@custom-variant dark (&:where(.dark, .dark *))`, so `dark:` variants work
against the class rather than a media query.

## 10.4 Primitives (`components/ui/primitives.tsx`)

### `Button`

| Variant | Appearance |
|---------|------------|
| `primary` | Inverted — ink background, canvas text. The single strongest action on a screen |
| `secondary` | Surface with a strong border. Default |
| `ghost` | Text only until hover |
| `danger` | Filled with `--color-phase-failed` |

Sizes: `sm` (h-8, 13 px) and `md` (h-9, 14 px). `loading` renders a `Spinner` before the children
and disables the button; `disabled` drops opacity to 40%.

### Other primitives

| Component | Notes |
|-----------|-------|
| `Spinner` | Inline SVG, `animate-spin`, `currentColor` |
| `Panel` / `PanelHeader` | The standard content container; header takes `title`, `description`, `actions` |
| `Badge` | Tones `neutral`, `accent`, `running`, `pending`, `paused`, `failed`; optional `title` for a native tooltip |
| `Input` / `Textarea` / `Select` | Shared `FIELD_BASE`; `Select` embeds an inline SVG chevron as a data-URI background so no wrapper element is needed |
| `Field` | Label + control + hint/error. Error replaces hint when present |
| `SegmentedControl` | Generic over the value union; `role="tablist"` with `aria-selected` |
| `EmptyState` | Icon, title, description, action |
| `Skeleton` | Pulsing block for loading layouts |
| `KeyValue` | Uppercase micro-label over a value; `mono` switches to `.numeric` |

`Dialog` (`components/ui/Dialog.tsx`) portals to `document.body`, closes on `Escape`, locks body
scroll, and offers `md` (max-w-lg) and `lg` (max-w-3xl) widths.

`ToastProvider` (`components/ui/Toast.tsx`) exposes `useToast()` with `success` and `error`. It
keeps at most four toasts; success dismisses after 4.5 s, error after 8 s.

## 10.5 Display components

| Component | Purpose |
|-----------|---------|
| `PhaseBadge` | Phase with a tone dot; pulses for `running` and pending phases; `title` carries the reason |
| `PhaseDot` | Just the dot, for dense rows |
| `HealthBadge` | Runtime health with its own tone map |
| `DriftIndicator` | Renders only when `desired !== observed` |
| `RelativeTime` | Live-updating relative timestamp with an absolute `title` |
| `Identifier` | Monospace id with a copy button that appears on hover |
| `CopyButton` | Clipboard write with a 1.4 s check-mark confirmation |
| `StatCard` | Label, large value, optional unit, hint, and an optional sparkline flush to the bottom edge |
| `Sparkline` | Dependency-free SVG area glyph for stat cards |
| `TimeSeriesChart` | A series to read: value and time axes, crosshair readout, keyboard steps |
| `DistributionBar` | Proportional segments for phase distribution |
| `DataGrid` | Virtualized result grid |

### `TimeSeriesChart`

For a series someone will read rather than glance at: the Overview's throughput and consumer lag,
and the fleet throughput. Rendered at the container's real pixel size (a `ResizeObserver`), so text
and markers are never stretched; given no `height` it fills its container.

| Part | Spec |
|------|------|
| Line | 2 px, round joins, in the series colour; nulls break it |
| Area | the series colour at 14% fading to 2% |
| Value axis | 3–5 round ticks (`ticks.ts::valueAxis`), hairline solid gridlines in `--color-line`, labels in `--color-ink-faint`. `baseline="zero"` keeps zero on the axis (rates); `"auto"` fits the axis to values that stay well clear of zero (a large lag) |
| Time axis | clock times at round local steps (`ticks.ts::timeTicks`), seconds only for spans under a few minutes |
| Readout | a crosshair snapped to the nearest point, and a tooltip with the value first and the series, clock time and age after it; `←` `→` `Home` `End` step through points when the chart has focus |
| End marker | 3.5 px dot ringed in `--color-surface` |
| Empty | below two values in the window, a "Collecting…" message at the same height, so nothing jumps |

Status colours never paint a series: consumer lag is a line in the accent colour, and its state
(**Catching up**, **Falling behind**, **Caught up**) is a badge with an icon and a label.

### `Sparkline`

```
viewBox = "0 0 240 <height>"   preserveAspectRatio="none"
```

A fixed 240-unit viewBox stretched to the container means the path never needs recomputing on
resize. With fewer than two non-null points it renders the text "Collecting…" instead of a
misleading flat line. The area fill is a `linearGradient` from 18% to 0% opacity; the stroke uses
`vectorEffect="non-scaling-stroke"` so it stays 1.5 px regardless of the horizontal stretch.

## 10.6 Layout

```
┌──────────┬──────────────────────────────────────────┐
│ Sidebar  │ PageHeader   (title, subtitle, meta,     │
│ 216px    │               actions, optional slot)    │
│ (≥768px) ├──────────────────────────────────────────┤
│          │ PageBody     (max-w-[1400px], px-6 py-6) │
└──────────┴──────────────────────────────────────────┘
```

- Below 768 px the sidebar is replaced by a horizontal nav strip; the account menu and theme
  move out of view (a known gap), and a guest gets **Sign in** in the strip.
- `Avatar` (`components/Avatar.tsx`) shows initials on a hue derived from the person's email
  (`--avatar-hue`): a pale tint with dark initials in the light theme, a deep one with light
  initials in the dark theme; greyed for a deactivated person.
- `PageHeader` has a `children` slot used by the list page (search + filters) and the detail page
  (tab bar), so those controls sit inside the header's surface rather than floating in the body.
- Content is capped at 1400 px and centred — wider viewports get margin, not stretched tables.

Grid templates are declared once per table and shared between header and rows, which is what keeps
columns aligned without a table element:

```ts
const GRID = 'grid grid-cols-[minmax(0,2.4fr)_150px_110px_100px_100px_80px_120px] items-center gap-4';
```

## 10.7 Conventions to follow

| Rule | Reason |
|------|--------|
| Numbers use `.numeric` | Prevents layout jitter as live values change |
| Ids and column names are `<code>` in `.numeric` | Distinguishes machine strings from prose |
| Status is never colour-only | Accessibility; every tone is paired with a label |
| One `primary` button per screen | Keeps the primary action obvious |
| Panels own their padding | Callers pass content, not spacing |
| Empty states explain *why* | "No data to query yet" plus the reason beats a bare "No results" |
| Destructive actions confirm | Delete opens a dialog that states what is and is not removed |
| Timestamps are relative with absolute `title` | Scannable at a glance, precise on hover |

## 10.8 Adding a component

1. Is it generic? Add it to `components/ui/primitives.tsx`. Feature-specific? Keep it in the
   feature folder.
2. Use tokens, never literal colours. `text-[var(--color-ink-muted)]`, not `text-gray-500`.
3. Compose `cn()` (clsx + tailwind-merge) so callers can override classes predictably.
4. Return `ReactNode`, accept `className`, and forward refs for anything focusable.
5. Verify both themes and the ≥768 px / <768 px breakpoints before considering it done.
