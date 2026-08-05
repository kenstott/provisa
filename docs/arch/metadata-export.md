# Metadata Export — Phased Implementation Plan

Covers REQ-1068 … REQ-1074 and REQ-1368. Outbound only: Provisa publishes its governance
metadata to external catalogs and never reads one back as source of truth.

**Status: all phases shipped.** What was built diverged from the plan in four places, each
noted inline below: the vendor adapters are mapping-tested rather than fixture-tested, Atlas
needed an HTTP-basic auth mode the plan did not anticipate, the sync jobs live in
`provisa/api/metadata_export/sync.py` rather than in `provisa/scheduler/jobs.py`, and the
event path publishes the full snapshot rather than a delta. The user-facing page is
`docs/metadata-export.md` (REQ-1368).

## Grounding (verified in this tree)

| Thing the plan leans on | Where it is | State |
| --- | --- | --- |
| Provider-ABC precedent | `provisa/auth/models.py:48` (`AuthProvider`), `provisa/core/mail.py:56` (`EmailSender` Protocol + `email_sender()` factory at `:130`) | exists |
| Config root | `provisa/core/models.py:1185` `ProvisaConfig`; `MailConfig` at `:915` is the shape to copy | exists |
| Domains / stewards | `Domain` `provisa/core/models.py:284`; steward rule in `provisa/core/domain_policy.py` (REQ-609) | exists |
| Relationships + owner/version | `Relationship` `provisa/core/models.py:660` (`owner`, `version`, `needs_review`) | exists |
| Masking facts | `Column.unmasked_to` / `mask_type` / `mask_pattern` / `mask_replace` / `mask_value` / `mask_precision`, `provisa/core/models.py:355-360` | exists |
| RLS facts | `RLSRule` `provisa/core/models.py:765`; `ProvisaConfig.rls_rules` `:1229` | exists |
| Visibility facts | `provisa/security/` (REQ-039/040) | exists |
| Column lineage | `provisa/lineage/columns.py:40` `resolve_column_lineage`, `provisa/lineage/graph.py:220` `build_column_graph` | exists |
| MV DAG | `provisa/events/lineage.py:177` `dependents`, `:188` `find_cycle` | exists |
| Event substrate | `provisa/events/queue.py:37` `post_event`, `:69` `claim`, `:160` `complete`; handler factories in `provisa/events/handlers.py` | exists (REQ-942 complete) |
| Scheduler | `provisa/scheduler/jobs.py` (APScheduler, cron jobs from config) | exists |
| Org record | `provisa/control_plane/models.py:30` `Org` — `id`, `name`, `data_plane_id`, `created_at` | exists |
| Tier / entitlement | nothing. No `tier`, no `entitlement` anywhere in `provisa/` | **missing** |
| `provisa/api/metadata_export/provider.py` (cited by REQ-1068) | — | **missing** |

Two consequences: REQ-1073 needs an entitlement primitive built first (Phase 0), because
`Org` carries no tier today; and every requirement here is still `proposed`, so each phase
starts by moving its REQ to `accepted` (`/req-accept`) and generating `.feature` files
under `tests/features/`.

## Phase 0 — Tier primitive (unblocks REQ-1073)

Smallest thing that makes a MUST-gate enforceable, not a general billing system.

- Add `tier: str` to `provisa/control_plane/models.py` `Org` and to the control-plane store.
- Add `provisa/control_plane/entitlements.py`: `require_tier(org_id, feature)` raising a
  typed `EntitlementError`; feature keys are constants, not strings at call sites.
- No default-allow. An org with an unrecognised tier is denied — REQ-1073 is a MUST
  constraint, so the failure mode is refusal, never silent pass-through.

Tests — `tests/unit/test_entitlements.py`: allowed tier passes; disallowed tier raises;
unknown tier raises; the error names the feature and the org.

## Phase 1 — REQ-1068: provider interface + per-org config

- `provisa/api/metadata_export/provider.py`
  - `MetadataExport` ABC: `provider_name: str`, `async publish(snapshot: MetadataSnapshot) -> PublishResult`,
    `async health() -> None`.
  - `PublishResult`: counts per asset kind + per-asset errors. Errors are returned and
    surfaced, not swallowed.
- `provisa/api/metadata_export/config.py`: `MetadataExportConfig` pydantic model
  (`enabled`, `provider`, `endpoint`, credential fields, `reconcile_cron`), mounted on
  `ProvisaConfig` as `metadata_export` — same shape as `MailConfig`.
- `provisa/api/metadata_export/registry.py`: `metadata_export(config)` factory mirroring
  `provisa/core/mail.py:130 email_sender()`. Unknown provider name → raise.
- Directionality is a structural invariant: the module exposes no read/ingest entry point.

Tests
- `tests/unit/test_metadata_export_provider.py` — factory resolves each registered name;
  unknown name raises; disabled config yields no provider; credentials never appear in
  `repr()` or logs.
- `tests/unit/test_metadata_export_config.py` — config parses from YAML, validation
  rejects `enabled: true` with no provider/endpoint.
- Import-boundary test: `provisa/api/metadata_export/` defines no function whose name
  starts with `ingest`/`import_`/`pull` (guards the outbound-only constraint).

## Phase 2 — REQ-1070: the internal metadata model + snapshot builder

Vendor-neutral model first; every adapter later maps *from* this, never from `ProvisaConfig`
directly.

- `provisa/api/metadata_export/model.py`: `MetadataSnapshot` with `datasets`, `tables`,
  `columns`, `domains`, `owners`, `relationships`, `lineage_edges`, `governance_tags`.
- `provisa/api/metadata_export/builder.py`: `build_snapshot(config, lineage_source) -> MetadataSnapshot`.
  - Assets/columns/descriptions/aliases from `ProvisaConfig`.
  - Domains + stewards via `provisa/core/domain_policy.py` (REQ-609). A domain with no
    steward cannot serve governed data, so it is published as `pending`, not omitted.
  - Approved relationships from `Relationship`, carrying `owner` and `version`;
    `needs_review: true` is published as an attribute, not filtered out.
  - Column-level lineage from `provisa/lineage/columns.py:40` over compiled SQL, plus the
    MV DAG from `provisa/events/lineage.py:177`.

Tests
- `tests/unit/test_metadata_snapshot_builder.py` — golden snapshot from a fixture config
  (`/snapshot-testing`); asserts column count, steward attribution, relationship version,
  and that a masked column is present with its masking attribute (Phase 3 fills the value).
- `tests/unit/test_metadata_snapshot_lineage.py` — a 3-hop MV chain produces the expected
  column-level edges, not table-level approximations.
- `tests/features/REQ-1070.feature` + steps.

## Phase 3 — REQ-1071: governance-signal projection

- `provisa/api/metadata_export/governance.py`: `governance_tags(config, security_view) -> list[GovernanceTag]`.
  - Masked columns from the `Column.mask_*` fields and `unmasked_to`.
  - RLS-restricted tables from `ProvisaConfig.rls_rules` / `RLSRule`.
  - Visibility-restricted assets from `provisa/security/` (REQ-039/040).
- Tags carry the *fact* (this column is masked, by this rule, for these roles) — not the
  mask pattern itself, which is a policy secret.

Tests
- `tests/unit/test_metadata_export_governance.py` — each of the three signal kinds
  produces a tag; a role in `unmasked_to` does not suppress the tag; mask patterns and
  RLS predicate bodies are absent from the emitted payload (this is the leak test).
- `tests/features/REQ-1071.feature`.

## Phase 4 — REQ-1069a: OpenLineage + OpenMetadata adapters

The two standards-first targets. Both are subclasses of `MetadataExport` over the Phase 2
model.

- `provisa/api/metadata_export/openlineage.py` — emits OpenLineage `RunEvent`s with the
  `columnLineage`, `schema`, `ownership`, and `dataQuality` facets. Lineage edges come from
  compiled queries and the MV DAG, so runs are real executions, not synthetic scans.
- `provisa/api/metadata_export/openmetadata.py` — maps assets to the OpenMetadata ingestion
  API (`createOrUpdate` for database/schema/table/column entities); governance tags become
  OpenMetadata tags/glossary terms.

Tests
- `tests/unit/test_openlineage_emit.py` — emitted JSON validates against the pinned
  OpenLineage JSON Schema (vendored under `tests/fixtures/`); column-lineage facet matches
  the Phase 2 graph.
- `tests/unit/test_openmetadata_map.py` — entity FQNs, hierarchy, and tag mapping.
- `tests/integration/test_metadata_export_openlineage_e2e.py` — publish against a Marquez
  container (OpenLineage reference server) on a per-worktree compose project; assert the
  dataset + column lineage is readable back out of Marquez.
- `tests/integration/test_metadata_export_openmetadata_e2e.py` — same shape against an
  OpenMetadata container.
- `tests/features/REQ-1069.feature`.

## Phase 5 — REQ-1069b: vendor adapters

Each is a thin subclass; no new metadata model.

- `atlas.py` — Apache Atlas REST v2 (`/api/atlas/v2/entity/bulk`) with a Provisa typedef
  bootstrap. **Microsoft Purview rides this adapter** — its ingestion surface is
  Atlas-API-compatible; the difference is auth (Entra ID token) and base URL, so Purview is
  a config variant plus an auth strategy, not a fourth code path.
- `datahub.py` — MCP/MCE emit over the DataHub REST sink.
- `atlan.py`, `collibra.py` — REST asset upsert.

Tests
- `tests/unit/test_metadata_export_vendors.py` — one file rather than one per vendor: all
  four map the SAME governed fixture the e2e publishes, so the four mappings are compared
  against each other rather than each against its own recorded payload. Recorded HTTP
  fixtures were dropped — a fixture recorded from our own client proves only that the client
  did not change.
- `tests/integration/test_metadata_export_atlas_e2e.py` — real Apache Atlas container
  (`requires_atlas`), publish then read every assertion back out of Atlas's own API. This is
  also the Purview contract test, since the wire format is the same.
- Atlan, Collibra, DataHub and Purview are *not executed against a live service*. Atlas
  proved one thing the plan had wrong: stock Apache Atlas answers a bearer token with 401,
  so `auth_mode: basic` was added end to end (config field `username`, the adapter's header,
  the admin router, the React form, the docs).

## Phase 6 — REQ-1072: sync

Shipped as `provisa/api/metadata_export/sync.py`.

- Event-driven: metadata changes post through `provisa/events/queue.py:37 post_event` and
  fan one work item to the org's export target; `drain` claims that target, publishes, and
  completes. Claim (not fanout) — duplicate publishes across the fleet are wrong, which is
  exactly the dispatch rule REQ-942 states. The injector sits at `_rebuild_schemas`, the one
  chokepoint every model mutation already passes through, so a new mutation cannot forget to
  publish.
- Reconcile: a per-org cron job on the embedded APScheduler. NOT in
  `provisa/scheduler/jobs.py` as planned — that module builds jobs from deployment config,
  while `reconcile_cron` is an org setting that has to be re-armed when an admin saves it.
  The jobs are armed at startup and again on every save, and removed when an org loses the
  entitlement rather than left firing into a skip.
- Open decision 1 resolved AGAINST the plan's recommendation: the event path publishes the
  full snapshot, not a delta. A delta needs its own builder and its own correctness
  argument; the full snapshot already has one (every adapter upserts by fully-qualified
  name), which is what makes the two paths converge instead of racing.
- Per-org scoping: the events queue lives in the org's own schema, and the org id is bound
  around every publish — a publish for org A can never read org B's config.

Tests
- `tests/unit/test_metadata_export_sync.py` — claim/complete lifecycle against a real
  SQLite control plane; a failed or raising publish leaves the work item reclaimable, never
  silently completed; two drains publish once; one org's queue is invisible to another
  (which absorbed the planned separate tenant-isolation file, since isolation IS the
  queue's own boundary).
- `tests/unit/test_metadata_export_gate.py` — the REQ-1073 gate on the path with no request
  behind it: jobs armed for an entitled org, disarmed when the tier lapses.
- `tests/features/REQ-1072.feature` + `tests/steps/steps_metadata_export_sync.py`.

## Phase 7 — REQ-1073: premium gate + admin surface

- `require_tier(org_id, Feature.METADATA_EXPORT)` at the publish entry point and at the
  config-write endpoint — both, so a non-premium org cannot stage a config that a later
  reconcile would honour.
- Admin UI tab: provider selection, credential entry, `health()` check, last-publish
  status and per-asset error list.

Tests
- `tests/unit/test_metadata_export_admin_surface.py` — the four endpoints driven directly,
  which is what the planned admin e2e would have covered; premium publishes, non-premium is
  refused at read, write, health and publish.
- `provisa-ui/src/__tests__/MetadataExportTab.test.tsx` and `provisa-ui/e2e/metadata-export-admin.spec.ts`.
- No `REQ-1073.feature`: REQ-1073 is a constraint, and the generator writes features only
  for behavioral requirements.

## Sequencing

```
Phase 0 ─┐
Phase 1 ─┼─► Phase 2 ─► Phase 3 ─┬─► Phase 4 ─► Phase 5
         │                       └─► Phase 6
         └────────────────────────────────────► Phase 7
```

Phase 4 is the first point with a shippable end-to-end path (OpenLineage → Marquez).
Phases 5 and 6 are independent of each other.

## Decisions, as settled

1. **Snapshot cadence vs. event granularity** — settled as *full snapshot on both paths*,
   against the original recommendation of a delta on the event path. A delta would need a
   second builder and a second correctness argument; the full snapshot already has one, and
   one snapshot shape is what makes the event path and the reconcile converge on the same
   catalog state rather than race into different ones.
2. **Purview auth** — settled as a widened `MetadataExportConfig` (`auth_mode` plus the
   Entra fields), not a pluggable strategy object. Atlas then forced a third mode anyway:
   stock Apache Atlas rejects a bearer token, so `basic` joined `api_key`, `bearer` and
   `entra`. Four flat modes on one config beat four strategy classes for a branch this
   small, and the admin form renders the modes directly.
3. **Tier names** — settled as `free` / `standard` / `premium`, with metadata export
   requiring `premium`. An unrecognised tier is refused, never defaulted.

## Not executed against a live service

Atlas is exercised against a real container, and Purview rides the same code path — same
routes, same envelope, same type model — so the mapping is proven for both. What is NOT
proven live: Purview's Entra token exchange, and the Atlan, Collibra and DataHub transports.
Purview was costed and skipped deliberately: an Azure Purview account bills its Data Map at
a 1-capacity-unit minimum around the clock from creation, and the only thing it would
exercise beyond the Atlas run is the client-credentials POST.
