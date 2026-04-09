# Water Digital Twin UI/UX Redesign Spec (Operator-Validated)

This spec was produced by a collaborative review between a senior frontend designer/full-stack engineer and a control room manager with 15 years of operational experience. Every proposal was validated against real operator workflows.

## Design Principles

1. **5-second situational awareness** — severity, scope, trajectory visible instantly
2. **Information density without clutter** — typographic hierarchy, color weight, spatial grouping
3. **Progressive disclosure** — summary first, detail on demand

---

## Implementation Priority Order

1. Incident banner overhaul — biggest single impact
2. Dark nav bar + live clock — instant visual upgrade, clock is control-room standard
3. Chart reference lines + threshold shading — the core Databricks ML story
4. Shift handover hero + KPI row — first view operators see at shift start
5. Penalty display upgrade — the "CFO number" for executive audiences
6. Alarm log severity banding — essential during alarm floods
7. Regulatory deadline progress bars — visceral urgency visualization
8. Root cause chain visualization — key demo narrative for upstream tracing
9. Comms log chat-style — polish
10. Everything else — progressive refinement

---

## 1. GLOBAL

### 1a. Typography Scale

5-level type scale:
- **Display** (`text-3xl font-bold tracking-tight tabular-nums`): KPIs, countdown timer, penalty amount
- **Heading** (`text-xl font-semibold`): View titles
- **Subheading** (`text-base font-semibold`): Card/panel titles
- **Body** (`text-sm`): Content, table cells
- **Caption** (`text-xs text-gray-500`): Timestamps, labels

Add `.kpi-display` utility class. Use `tabular-nums` on all numeric displays to prevent digit jumping.

### 1b. Incident Banner

**Current:** Pale `bg-red-50` strip with small text.

**New:** Bold `bg-red-600 text-white` situation bar.
- **Top row:** Incident ID + type, severity badge, live elapsed time (mono font, right-aligned)
- **Bottom row:** Live KPI strip — `3 DMAs | 441 properties | 2 sensitive sites | Next deadline: 6h`
- Extract `useCountdown` from RegulatoryView into a shared hook (`hooks/useCountdown.ts`)
- Single small pulsing dot only. No gradient pulse animation — causes alarm fatigue.
- KPI strip must update live (React Query invalidation) — stale data in the banner is worse than no banner.

**File:** `App.tsx` lines 82-93 (replace the current `bg-red-50` div)

### 1c. Navigation

**Current:** White `bg-white` nav with blue active state.

**New:** Dark nav: `bg-water-900 text-white`.
- Active: `bg-white/15 text-white`
- Inactive: `text-white/70 hover:text-white hover:bg-white/10`
- Live clock `HH:mm:ss` on far right — standard in every control room
- Logo: water droplet SVG or better wordmark
- No workflow step numbers or breadcrumb connecting lines. Operators jump between views constantly.

**File:** `App.tsx` lines 24-63 (NavBar component)

---

## 2. SHIFT HANDOVER

### 2a. Briefing Hero + KPI Row

**Current:** Flat card with 2-column `label: value` grid (ShiftHandover.tsx lines 52-63).

**New:**
- **Hero section:** Large incident title, display-size elapsed counter (`font-mono text-4xl`), severity badge, 1-line situation summary
- **KPI row:** `grid grid-cols-4 gap-4`, each card with `border-l-4` colored by severity:
  - Duration | Affected DMAs | Properties at Risk | Escalation Risk
  - Consider a 5th KPI for reservoir level when applicable

**File:** `ShiftHandover.tsx` lines 52-92

### 2b. Actions Timeline

**Current:** Simple `<ul>` with green dots (lines 97-117).

**New:** Vertical timeline with continuous left border, colored node circles:
- Green = done, Amber = outstanding, Blue = in-progress
- Timestamps prominent, action text beside each node
- Create reusable `<Timeline>` common component (reuse pattern from RegulatoryView lines 205-228)

**File:** `ShiftHandover.tsx` lines 94-137, new `components/common/Timeline.tsx`

### 2c. Sticky Acknowledge Footer

**Current:** Card at bottom of scroll (lines 161-174).

**New:** `sticky bottom-0` bar, always visible:
- Pre-acknowledge: `bg-amber-50 border-t-2 border-amber-400` + amber button
- Post-acknowledge: `bg-green-50 border-t-2 border-green-400` + checkmark + timestamp
- No shake animation on navigation away. The persistent amber bar is sufficient reminder.

**File:** `ShiftHandover.tsx` lines 160-174

---

## 3. ALARM LOG

### 3a. Table Redesign

**Current:** Plain white table with hover states (AlarmLog.tsx lines 93-119).

**New:**
- Row severity banding: RED rows get `bg-red-50/50 border-l-3 border-l-red-500`, AMBER rows get `bg-amber-50/30 border-l-3 border-l-amber-500`, GREEN stays neutral
- Time column: relative time primary ("2m ago") with absolute time as tooltip/secondary
- Sticky `<thead>` with `sticky top-0 z-10`
- Row expansion on click for full details

**File:** `AlarmLog.tsx` lines 93-119

### 3b. Filter Bar

**Current:** Two `<select>` dropdowns (lines 60-85).

**New:** Segmented button group: `All (47) | RED (7) | AMBER (12) | GREEN (28)`
- Count badges give instant scope awareness
- "Latest" auto-scroll toggle — clearly marked ON/OFF state
- Total event count shown next to page title

**File:** `AlarmLog.tsx` lines 57-86

---

## 4. NETWORK MAP + DMA DETAIL

### 4a. Map Controls

**Current:** Filter buttons above the map.

**New:** Bottom-center translucent filter bar:
- `bg-white/90 backdrop-blur-sm rounded-full shadow-lg`
- DMA counts in each filter button
- Legend in bottom-right for RED/AMBER/GREEN meanings
- Dark tooltips on hover — add `border` or `shadow-xl` for contrast against dark map areas
- Test tooltip contrast against CARTO Positron basemap

**File:** `MapView.tsx`

### 4b. DMA Side Panel — Root Cause Chain

**Current:** Root cause shown as text string (DMADetail.tsx ~line 79).

**New:** Visual flow diagram:
```
[Pump Station] --> [Trunk Main] --> [DMA] --> [downstream]
```
- RAG-colored nodes for each asset
- Sensor pressure mini-bars: horizontal bars (0-30m scale), red and visually short for low pressure
- Colored stat cards for pressure/flow (red if below threshold)
- No mini-map in panel header — redundant, wastes 420px panel space
- Sparkline trend arrows only if backed by actual trend data — don't show from single-point values

**File:** `DMADetail.tsx`

### 4c. Segmented Control Tabs

**Current:** Basic tab buttons.

**New:** `bg-gray-100 rounded-lg p-0.5` container, `bg-white shadow-sm` active state

**File:** `DMADetail.tsx` tab section

---

## 5. REGULATORY VIEW

### 5a. Deadline Progress Bars

**Current:** Static text with RAGBadge (RegulatoryView.tsx lines 136-149).

**New:** Progress bar per deadline:
- `width = min(100, (hoursElapsed / threshold) * 100)%`
- Color: green -> amber (>70%) -> red (>90%)
- "Remaining time" in large text beside each bar
- Status icons: checkmark (DONE), warning triangle (approaching), X (BREACHED)
- Breached deadlines: `bg-red-50 border-2 border-red-300` with crossed-out time

**File:** `RegulatoryView.tsx` lines 136-149

### 5b. Penalty Display

**Current:** Red box with small formula text (lines 153-177).

**New:**
- Hero penalty amount: `text-4xl font-bold text-red-600`
- Visual formula: each factor in a `bg-white rounded-lg px-3 py-2 border shadow-sm` box connected by operator symbols
- Card wrapper: `border-l-4 border-l-red-500`
- **Must label:** "PROJECTED — assumes no remedial action" to prevent panic-driven bad decisions

**File:** `RegulatoryView.tsx` lines 151-177

### 5c. C-MeX Ring Gauge

**Current:** Simple progress bar (lines 180-197).

**New:** CSS `conic-gradient` or SVG donut:
- Percentage in center
- Color: green >70%, amber 40-70%, red <40%
- Target label: "Target: >85% for upper quartile C-MeX"

**File:** `RegulatoryView.tsx` lines 179-197

### 5d. Auditable Timeline

**Current:** Vertical timeline with basic dots (lines 199-228).

**New:**
- Decision-maker attribution per node ("Detected by SCADA", "Escalated by Night Operator")
- Elapsed time connectors between nodes ("[+15 min]")
- Dashed outlines for future/pending deadline nodes
- Keep straight vertical left-aligned — faster to scan (no alternating left/right)

**File:** `RegulatoryView.tsx` lines 199-228

---

## 6. ASSET DETAIL

### 6a. Chart Annotations (HIGH PRIORITY)

**Current:** Plain Recharts ComposedChart with no annotations (AssetDetail.tsx).

**New:** Add Recharts reference elements:
```tsx
<ReferenceArea yAxisId="left" y1={0} y2={10} fill="rgba(220,38,38,0.08)" />
<ReferenceLine x={anomalyTime} stroke="#DC2626" strokeDasharray="5 5" label="Anomaly detected" />
<ReferenceLine x={complaintTime} stroke="#F59E0B" strokeDasharray="5 5" label="First complaint" />
```
- The gap between anomaly and complaint lines IS the demo story: "ML caught it N minutes before customers"
- Increase chart height from 220px to 280px

**File:** `AssetDetail.tsx` (chart section)

### 6b. Playbook Checklist

**Current:** Buttons with Accept/Defer/N/A options.

**New:**
- Numbered step circles on the left
- Status icons: empty circle (undecided), green checkmark (accepted), amber clock (deferred), gray dash (N/A)
- Progress indicator: "3 of 7 steps decided" with progress bar
- Keep buttons always visible (no hover-to-reveal) — control rooms may use touchscreens

**File:** `AssetDetail.tsx` (playbook section)

### 6c. Anomaly Callout

**Current:** Text-based anomaly alert.

**New:** Hero metric card:
- "44 min" in `text-3xl font-bold` inside `bg-gradient-to-r from-red-600 to-red-700 text-white rounded-xl p-4`
- Subtitle: "AI detected this anomaly 44 minutes before the first customer complaint"
- Confidence bar for anomaly score (data already available in `firstAnomaly.score`)

**File:** `AssetDetail.tsx` (anomaly section)

---

## 7. COMMUNICATIONS LOG

### 7a. Chat-Style Layout

**Current:** Flat list of communication entries (CommunicationsLog.tsx).

**New:**
- Outbound right-aligned (`ml-auto max-w-[80%] bg-water-50`), inbound left-aligned (`bg-gray-50`)
- Channel icons (envelope, phone, chat bubble, radio, people, gear) instead of text badges
- System messages: centered, `text-gray-400 italic text-xs`
- Time dividers: subtle thin line with centered timestamp
- Test carefully in `max-h-64` container — left/right bouncing in small space can feel chaotic

**File:** `CommunicationsLog.tsx`

### 7b. Persistent Chat Input

**Current:** Toggle button to show/hide form.

**New:**
- Always-visible compact input at bottom of card
- Expands to full form (recipient, channel selector) on focus
- Replaces current toggle button pattern

**File:** `CommunicationsLog.tsx`

---

## 8. CUSTOMER IMPACT

### 8a. What-If Slider

**Current:** Basic range input (CustomerImpact.tsx).

**New:**
- Styled range input with tick marks at 0/25/50/75/100%
- Preset buttons: "Current (~50%)" | "Full outage (0%)" | "Restored (100%)"
- Card transitions must be very subtle (100-150ms, tiny scale). Nothing bouncy.

**File:** `CustomerImpact.tsx`

### 8b. Impact Cards

**Current:** Basic colored dot + count.

**New:**
- High-impact card gets `ring-2 ring-red-400` glow when count > threshold (~10 properties)
- No `scale-105` — breaks grid alignment. Ring/glow only.
- No donut/pie chart — the 4 colored number boxes are clear enough

**File:** `CustomerImpact.tsx`

---

## 9. COMMON COMPONENTS

### 9a. RAGBadge

- Dot size increase to `w-2.5 h-2.5`
- `ring-1 ring-inset` for depth
- `animate-pulse` on RED dots only during active incidents

**File:** `components/common/RAGBadge.tsx`

### 9b. Skeleton Screens

- Replace spinner for full-page loads with layout-matching skeleton screens
- Keep spinner for small inline loads (side panel content)
- Create `components/common/Skeleton.tsx`

### 9c. TimelineStrip Interactive

- Time labels at start ("00:00") and end ("now")
- Custom hover tooltip replacing browser `title`
- Current-time needle indicator
- Slightly taller: `h-8`

**File:** `components/common/TimelineStrip.tsx`

---

## 10. CSS / STYLE SYSTEM

### 10a. Card Elevation Classes

Add to `styles/index.css`:
```css
.card-flat { /* no shadow, for nested content */ }
.card { @apply bg-white rounded-xl shadow-sm border border-gray-200 p-5; }
.card-elevated { @apply bg-white rounded-xl shadow-md border border-gray-200 p-5; }
.card-critical { @apply bg-red-50 rounded-xl border-2 border-red-200 p-5; }
```

### 10b. Transitions

Apply transitions surgically to buttons, links, cards, badges — NOT all DOM elements. Control rooms may have older hardware. Do not use `* { transition }`.

---

## 3 Missing Features (Flagged by Operator)

### 1. Keyboard Shortcuts
- Number keys 1-5 to switch views
- Escape to close side panel
- Arrow keys to navigate alarm rows
- Standard in SCADA/control room software

### 2. Regulatory View Export
- "Copy to clipboard" or "Export PDF" button for the regulatory timeline + deadlines
- Operators need to email DWI status updates at 3am

### 3. Connection Status Indicator
- "LIVE" green dot or "DISCONNECTED" red indicator in the nav bar
- Currently uses React Query with `staleTime: 60_000` and no staleness indicator
- Operators must trust the data is current

---

## Key Operator Feedback Themes

- **Never add animations, hidden controls, or visual complexity that doesn't carry information.**
- Everything visible should serve a purpose.
- No gratuitous animation — causes alarm fatigue.
- No hidden controls — control rooms may use touchscreens with no hover.
- No forced workflow — operators jump between views constantly.
- Penalty projections must be labeled "PROJECTED" to prevent panic decisions.
