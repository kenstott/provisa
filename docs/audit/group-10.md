# Audit — Group 10: UI & Admin Surfaces

Date: 2026-06-18
Scope: **Group 10 — UI & Admin Surfaces** (REQ-058–063, 164–167, 242–245, 248–249,
395–396, 401, 404, 410). Code lives under `provisa-ui/src/`, `provisa/api/admin/`,
and `provisa/core/models.py`.
Method: read each requirement against implementation with file:line evidence from
Grep/Read. Companion to the Group-2 audit ([group-2.md](group-2.md)).

## Classification key

- **To spec** — implemented and matches the requirement
- **Incomplete** — partially implemented
- **Not to spec** — implemented differently than the requirement states
- **Not added** — required but missing

## Summary

| REQ | Sub-area | Status | Finding |
| --- | --- | --- | --- |
| 058 | UI & Frontend | To spec | Every route wrapped in `CapabilityGate`; NavBar renders by capability — `provisa-ui/src/components/CapabilityGate.tsx:22`, `provisa-ui/src/App.tsx:112` |
| 059 | UI & Frontend | To spec | Roles hold independent `capabilities` + `domain_access` lists, full CRUD in API + UI — `provisa/api/admin/roles_router.py:28`, `provisa-ui/src/components/admin/RolesTab.tsx:39` |
| 060 | UI & Frontend | Incomplete | Six core capabilities defined (plus extras); the "holders also execute queued creation requests" half depends on REQ-063, not built — `provisa/security/rights.py:21` |
| 061 | UI & Frontend | To spec | Shared `ConfirmDialog` with consequence text used on destructive deletes — `provisa-ui/src/components/ConfirmDialog.tsx:43` |
| 062 | UI & Frontend | Incomplete | Enforcement metadata (RLS, columns, scope) built for `compile_query`, but the action/command test path does not surface it — `provisa/api/admin/dev_queries.py:53`, `provisa/api/admin/actions_router.py:402` |
| 063 | UI & Frontend | Not added | No creation-request queue; `approval_hook.py` is runtime query approval, a different system — `provisa/auth/approval_hook.py:35` |
| 164 | Admin & Config | To spec | `GET/PUT /admin/config` writes `.bak` then reloads via `_load_and_build()` — `provisa/api/admin/settings_router.py:19` |
| 165 | Admin & Config | To spec | `GET/PUT /admin/settings` covers redirect, sampling, cache at runtime — `provisa/api/admin/settings_router.py:50` |
| 166 | Admin & Config | To spec | Relationships page: add form + inline edit + materialize toggle + delete — `provisa-ui/src/components/relationships/RelationshipRow.tsx:114`, `AddRelationshipForm.tsx:182` |
| 167 | Admin & Config | To spec | `POST /admin/discover/relationships` calls LLM; candidates accept/reject in UI — `provisa/api/admin/discovery.py:57`, `provisa-ui/src/pages/RelationshipsPage.tsx:198` |
| 242 | Commands UI | To spec | Commands page lists functions + webhooks grouped, with all named columns — `provisa-ui/src/pages/CommandsPage.tsx:737` |
| 243 | Commands UI | To spec | Add form type selector renders DB Function vs Webhook field sets — `provisa-ui/src/pages/CommandsPage.tsx:351` |
| 244 | Commands UI | To spec | Webhook inline type builder: dynamic name + GraphQL-type rows — `provisa-ui/src/pages/CommandsPage.tsx:548` |
| 245 | Commands UI | Incomplete | Test button executes function/webhook but applies no governance pipeline (masking/RLS/role) — `provisa/api/admin/actions_router.py:434` |
| 248 | UI & Design | To spec | Voyager runs in an iframe loading React 18 + standalone bundle from CDN — `provisa-ui/src/pages/SchemaExplorer.tsx:49` |
| 249 | UI & Design | To spec | Masking fields inline on `Column`; loaded from `table_columns` at startup — `provisa/core/models.py:286`, `provisa/api/app.py:1822` |
| 395 | UI & Frontend | To spec | PK checkbox in add-form, edit-form, and read-only view — `provisa-ui/src/pages/TablesPage.tsx:826` |
| 396 | UI & Frontend | To spec | "Exclude from query" disabled when no PK (`disabled={!hasPk}`) — `provisa-ui/src/components/graph/NodeContextMenu.tsx:104` |
| 401 | UI & Frontend | Incomplete | `isForeignKey`/`isAlternateKey` typed and persisted, but no FK/AK badges rendered — `provisa-ui/src/pages/TablesPage.tsx:1209` |
| 404 | UI & Frontend | To spec | "Apply To" toggle sets `table_id` xor `domain_id`, other NULL — `provisa-ui/src/pages/SecurityPage.tsx:531` |
| 410 | UI & Frontend | To spec | Non-numeric PK values single-quoted in Cypher WHERE — `provisa-ui/src/components/graph/GraphFrame.tsx:194` |

15 To spec, 4 Incomplete, 1 Not added (REQ-063), 0 Not to spec. The path hint for
REQ-410 (`pages/GraphFrame.tsx`) is wrong — the file is at
`provisa-ui/src/components/graph/GraphFrame.tsx`.

## Detail

### UI & Frontend (role-driven surfaces)

- **REQ-058 — To spec.** `CapabilityGate` conditionally renders children by
  capability (`provisa-ui/src/components/CapabilityGate.tsx:22`); every page route is
  wrapped in it (`provisa-ui/src/App.tsx:112`) and `NavBar` builds nav groups from the
  user's unioned capabilities (`provisa-ui/src/components/NavBar.tsx:37`).
- **REQ-059 — To spec.** Role model carries `capabilities` and `domain_access` as
  independent lists; full CRUD via `provisa/api/admin/roles_router.py:28` and the
  `RolesTab` editor (`provisa-ui/src/components/admin/RolesTab.tsx:39`).
- **REQ-060 — Incomplete.** The six named capabilities plus extras are enumerated in
  `provisa/security/rights.py:21` and mirrored in `provisa-ui/src/types/auth.ts:12`.
  The requirement's second clause — create-capability holders also execute queued
  creation requests — has no implementation because it rides on REQ-063.
- **REQ-061 — To spec.** `ConfirmDialog` takes a `consequence` prop shown to the user
  (`provisa-ui/src/components/ConfirmDialog.tsx:43`) and wraps destructive deletes in
  `SourcesPage`/`CommandsPage`.
- **REQ-062 — Incomplete.** `_build_enforcement_metadata` assembles RLS filters,
  excluded columns, and schema scope (`provisa/api/admin/dev_queries.py:53`) returned
  by `compile_query`, but the action/command test endpoint
  (`provisa/api/admin/actions_router.py:402`) returns raw rows without that metadata.
- **REQ-063 — Not added.** No creation-request queue model, route, or UI exists.
  `provisa/auth/approval_hook.py:35` provides `ApprovalRequest`/`ApprovalResponse` for
  runtime *query* approval, which is a separate concern. (Tracked as being implemented
  separately.)
- **REQ-395 — To spec.** PK checkbox in add-form (`provisa-ui/src/pages/TablesPage.tsx:826`),
  edit-form (`:1912`), and read-only checkmark (`:1231`).
- **REQ-396 — To spec.** `disabled={!hasPk}` with greyed styling and explanatory
  tooltip (`provisa-ui/src/components/graph/NodeContextMenu.tsx:104`).
- **REQ-401 — Incomplete.** `isForeignKey`/`isAlternateKey` exist in types
  (`provisa-ui/src/types/admin.ts:64`) and persist to backend
  (`provisa-ui/src/pages/TablesPage.tsx:607`), but no FK/AK badge renders — only
  `nativeFilterType` badges show (`:1209`).
- **REQ-404 — To spec.** "Apply To" dropdown (`provisa-ui/src/pages/SecurityPage.tsx:631`)
  drives `tableId`/`domainId` xor population (`:531`).
- **REQ-410 — To spec.** `pkLit` single-quotes non-numeric values and escapes inner
  quotes (`provisa-ui/src/components/graph/GraphFrame.tsx:194`), used in WHERE at
  `:226` and `:295`.

### Admin & Configuration

- **REQ-164 — To spec.** `GET/PUT /admin/config` downloads/uploads YAML, writes a
  `.bak` backup before overwrite, then reloads via `_load_and_build()`
  (`provisa/api/admin/settings_router.py:19`).
- **REQ-165 — To spec.** `GET/PUT /admin/settings` returns and updates redirect,
  sampling, and cache settings at runtime (`provisa/api/admin/settings_router.py:50`).
- **REQ-166 — To spec.** `AddRelationshipForm` includes a materialize checkbox
  (`provisa-ui/src/components/relationships/AddRelationshipForm.tsx:182`); `RelationshipRow`
  has inline edit, materialize toggle, and delete
  (`provisa-ui/src/components/relationships/RelationshipRow.tsx:114`).
- **REQ-167 — To spec.** `POST /admin/discover/relationships` gathers FK constraints
  and calls the LLM analyzer (`provisa/api/admin/discovery.py:57`,
  `provisa/discovery/analyzer.py:81`); the Sparkles button and `CandidatesTable` drive
  discovery and accept/reject (`provisa-ui/src/pages/RelationshipsPage.tsx:198`).

### Commands UI

- **REQ-242 — To spec.** Functions and webhooks render in separate grouped tables with
  source, domain, kind, visible_to, returns, and arg count
  (`provisa-ui/src/pages/CommandsPage.tsx:737`).
- **REQ-243 — To spec.** Type selector toggles DB Function vs Webhook field sets in
  `renderFormFields` (`provisa-ui/src/pages/CommandsPage.tsx:351`).
- **REQ-244 — To spec.** Inline return-type builder adds dynamic field-name + GraphQL-type
  rows (`provisa-ui/src/pages/CommandsPage.tsx:548`).
- **REQ-245 — Incomplete.** Test button calls the test endpoint
  (`provisa-ui/src/pages/CommandsPage.tsx:331`), which executes the function/webhook
  (`provisa/api/admin/actions_router.py:402`) but runs a plain query without applying
  the governance pipeline (`:434`); spec asks for masked columns, RLS filters, and role.

### UI & Design Patterns

- **REQ-248 — To spec.** `SchemaExplorer` mounts Voyager in an iframe whose `srcDoc`
  loads React 18 production builds and `voyager.standalone.js` from CDN — no component
  fork (`provisa-ui/src/pages/SchemaExplorer.tsx:49`).
- **REQ-249 — To spec.** `mask_type`/`mask_pattern`/`mask_replace`/`mask_value`/
  `mask_precision` are inline on `Column` (`provisa/core/models.py:286`); loaded from
  `table_columns` rows at startup by `_load_masking_rules` (`provisa/api/app.py:1822`).
  No separate `MaskingRule` model or table.

## Named tests

The requirements name four Playwright specs and two Python test files. The Python
files exist; none of the four named specs exist — the e2e suite uses different
filenames.

| Named test | Status | Note |
| --- | --- | --- |
| `provisa-ui/e2e/pages.spec.ts` | Missing | No such file; closest coverage is `no-domain-mode.spec.ts`, `relationships-header.spec.ts` |
| `provisa-ui/e2e/tables.spec.ts` | Missing | Tables coverage lives in `tables-register.spec.ts` |
| `provisa-ui/e2e/graph.spec.ts` | Missing | Graph coverage in `graph-query-panel-height.spec.ts`, `graph-show-children.spec.ts` |
| `provisa-ui/e2e/security.spec.ts` | Missing | No security e2e spec found |
| `tests/integration/test_admin_api.py` | Exists | — |
| `tests/e2e/test_admin_flow.py` | Exists | — |

## Remaining tasks

| # | REQ | Type | Effort | Task |
| --- | --- | --- | --- | --- |
| 1 | 063 | Feature | L | Build creation-request queue: model, queue API, submit/review UI, specific rejection reasons |
| 2 | 060 | Feature | M | Wire create-capability holders to execute queued creation requests (depends on task 1) |
| 3 | 062 | Feature | M | Surface enforcement metadata (RLS filters, excluded columns, schema scope) in the action/command test response, not just `compile_query` |
| 4 | 245 | Feature | M | Apply the governance pipeline (masking, RLS, role) when the command test button executes |
| 5 | 401 | UI | S | Render read-only FK/AK badges in the column editor from existing `isForeignKey`/`isAlternateKey` fields |
| 6 | 058–062, 395–404 | Test | M | Add the named Playwright specs (`pages`, `tables`, `graph`, `security`) or update the requirement doc to the actual filenames |
