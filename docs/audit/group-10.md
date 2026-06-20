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

## Implementation plan

### Phase A — Quick wins (unblocked, S effort)

#### A1 — REQ-401: FK/AK badges

- `provisa-ui/src/pages/TablesPage.tsx:1209` — render read-only FK/AK badges from
  `isForeignKey`/`isAlternateKey` fields that already exist in types and persist to the
  backend. No API change required.

### Phase B — Governance test path (unblocked, M effort)

#### B1 — REQ-062: Surface enforcement metadata in test response

- `provisa/api/admin/actions_router.py:402` — call `_build_enforcement_metadata` and
  include `rls_filters`, `excluded_columns`, `schema_scope` in the test endpoint
  response alongside the existing rows.
- `provisa-ui/src/pages/CommandsPage.tsx:331` — display the returned metadata in the
  test result panel.

#### B2 — REQ-245: Apply governance pipeline during test execution

- `provisa/api/admin/actions_router.py:434` — route test execution through
  `apply_governance` (masking + RLS + role) instead of the current raw query path.
- Add a role selector to the test UI (`provisa-ui/src/pages/CommandsPage.tsx:331`);
  the selected role is passed to the endpoint and used to build the governance context.
- B1 and B2 touch the same endpoint and should be done in one pass.

### Phase C — E2E test coverage (unblocked, S effort)

#### C1 — REQ-058–062, 395–404: Remap named specs to actual files

Update `docs/arch/requirements.md` to replace the four non-existent named spec paths
with the actual files that provide equivalent coverage:

| Named (non-existent) | Actual coverage file |
| --- | --- |
| `provisa-ui/e2e/pages.spec.ts` | `no-domain-mode.spec.ts`, `relationships-header.spec.ts` |
| `provisa-ui/e2e/tables.spec.ts` | `tables-register.spec.ts` |
| `provisa-ui/e2e/graph.spec.ts` | `graph-query-panel-height.spec.ts`, `graph-show-children.spec.ts` |
| `provisa-ui/e2e/security.spec.ts` | *(no existing coverage — note as gap)* |

### Phase D — Creation-request queue (unblocked, L effort)

#### D1 — REQ-063 + REQ-434 + REQ-480: Data model + API

Request types: `relationship`, `view`, `webhook_registration` (REQ-480).

New table `creation_requests`:

| Column | Type | Notes |
| --- | --- | --- |
| `id` | uuid PK | |
| `type` | text | `relationship` \| `view` \| `webhook_registration` |
| `requester` | text | identity of submitter |
| `payload` | jsonb | full create payload |
| `status` | text | `pending` \| `executed` \| `rejected` |
| `rejection_reason` | text | required when `status = rejected`; constrained to typed enum per request type (see below) |
| `created_at` | timestamptz | |
| `resolved_at` | timestamptz | nullable |
| `resolved_by` | text | nullable; identity of executor/rejecter |

Relationships require up to 2 approvers before execution. Add:

| Column | Type | Notes |
| --- | --- | --- |
| `approvals` | jsonb | list of `{approver, approved_at}`; execution gated on required count |
| `required_approvals` | int | defaults to 1; 2 for `relationship` type |

Routes:

- `POST /admin/creation-requests` — submit; any authenticated user.
- `GET /admin/creation-requests` — list; filterable by `status`, `type`.
- `POST /admin/creation-requests/{id}/approve` — approver only (capability check).
- `POST /admin/creation-requests/{id}/reject` — approver only; body requires `reason`.
- `POST /admin/creation-requests/{id}/execute` — triggered automatically when
  `approvals` reaches `required_approvals`; also callable manually by a single approver
  for non-relationship types.

**Rejection reasons** — the spec requires "specific and actionable" reasons but does not
enumerate them. The following set is proposed; confirm before implementation:

| Type | Reasons |
| --- | --- |
| `relationship` | `duplicate`, `incorrect_join_columns`, `wrong_cardinality`, `source_not_registered`, `insufficient_detail` |
| `view` | `duplicate`, `query_invalid`, `governance_violation`, `out_of_scope`, `insufficient_detail` |
| `webhook_registration` | `duplicate`, `endpoint_unreachable`, `schema_mismatch`, `governance_violation`, `insufficient_detail` |

#### D2 — REQ-063: Submit/review UI

- New admin page at **Admin › Requests** (`provisa-ui/src/pages/RequestsPage.tsx`).
- Wired into `App.tsx` under the `ADMIN` capability gate.
- Pending queue: table showing type, requester, submitted date, payload summary, action buttons.
- Approve button: adds current user to `approvals`; shows approval count vs required.
- Reject button: opens a dropdown of typed rejection reasons, then confirms.
- Resolved tab: shows executed/rejected items with reason and resolver.

### Phase E — Capability wiring (blocked on D, M effort)

#### E1 — REQ-060: create-capability → queue execution

- Blocked on D1 + D2.
- Update the `CREATE` capability check so holders can also *execute* items from the
  creation-request queue (not just submit).
- Update `CapabilityGate` usage and role enforcement at the relevant API paths.

### Sequencing

```text
A1              (unblocked — start here)
B1 → B2         (unblocked, do together)
C1              (unblocked, parallel to B)
D1 → D2         (unblocked, largest item)
E1              (blocked on D1 + D2)
```

### Open decision

**Rejection reasons (D1):** the spec says reasons must be "specific and actionable"
but does not enumerate them. The proposed set above is a draft — confirm or replace
before implementing D1.
