# SPIKE 0002 — Deep-linking convention for URL-as-state

Status: Shelved (design captured, not scheduled)
Requirement: none yet — no REQ filed; open this spike again before proposing one.

Spikes live in this directory and follow the ADR shell (`../adr/`): numbered file,
Status header, Context, then Question / Findings / Outcome instead of a Decision. A
spike closes by producing or amending a requirement, or by recording why not.

## Context

Design principle under discussion: screen state should be replicated in the URL
query string wherever reasonable ("deep linking" / "URL as state"), so a copied
link reproduces what's on screen — filters, search, selection, sort, open
panel/tab. Standard practice for data/admin UIs (GitHub, Linear, Grafana, Datadog
all do this for filter/tab/selected-entity state). Caveats: never put secrets/PII
in the URL; don't reflect high-frequency or transient UI (hover, in-progress form
drafts, loading flags) into it; debounce/`replace` writes so history isn't spammed
per keystroke.

No shared URL-state hook exists in `provisa-ui/src` today (checked `src/hooks`,
`src/lib`). Where URL sync exists at all, it's a hand-written `useSearchParams`
closure duplicated verbatim across pages.

## Question

What's the current state of URL-reflected state across the UI, and is a shared
convention/hook worth adopting?

## Findings

Survey of `provisa-ui/src/pages/*.tsx` (file:line citations from the live agent
survey, not re-verified here — re-check before acting):

| Page | URL-synced now | Missing but shareable | Pattern |
|---|---|---|---|
| SourcesPage | `search`, `expanded` row | `page`, modal-open flags | mix — 2 fields wired, rest local `useState` |
| RelationshipsPage | `search` | `page`, `sortCol/Dir`, `groupBy`, `expanded`, modal flags | same mix, duplicated code from SourcesPage |
| TablesPage | `source` (read-only seed, never written back) | `page`, `sortCol/Dir`, `groupBy`, `expanded`, `showErd` | URL read-once only |
| LineagePage | `sql`, `focus` (read-once on entry) | live `sql` edits, active graph | URL = one-shot deep-link; ongoing state in `sessionStorage` |
| JsonApiPage | none | table, fields, filter, sort, group, funcs, pageSize, tab | already assembles an internal query string (`displayUrl`) that's just never pushed to `window.location` — cheapest page to wire |
| OpenApiPage / GrpcPage | none | opened endpoint/method | state passed via ephemeral router `location.state`, lost on refresh/share |
| CommandsPage, SecurityPage, RequestsPage, AdminPage sub-state, GraphPage | none | search, page, form-open, graph frames/history | local `useState` only |
| AdminPage top tab | yes, via path segment | sub-tab pagination/modals | fine as-is |

Cross-page observation: every page that filters/sorts/paginates a table
reimplements the same shape (`search`, `page`, `sortCol`/`sortDir`,
`groupBy`/`collapsedGroups`, `expanded`) independently.

## Proposed convention (not built)

- One shared hook, `src/hooks/useUrlState.ts`, wrapping `useSearchParams`: typed
  get/set per key, `replace: true` by default, debounced writer for text-search
  fields.
- Standard key names across pages: `q` (search), `page`, `sort`, `dir`, `group`,
  `id`/`expanded`, `panel` (open modal/form).
- Bookmarkable-state rule baked into the hook's doc comment: URL if a user would
  want to share/bookmark it; local state if it's a draft, transient loading flag,
  or hover state.
- Reference migration path: replace the duplicated `updateSearch`/`updateExpanded`
  closures in SourcesPage/RelationshipsPage first (mechanical, proves the hook
  out), then extend to TablesPage/JsonApiPage/others.

## Outcome

Shelved. No REQ filed, no code written. Revisit by building the hook and
migrating one reference page (Sources or Relationships), then decide whether to
extend it further.
