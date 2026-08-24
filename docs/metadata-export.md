# Metadata Export

Provisa publishes the metadata it governs — tables, columns, domains, stewards, approved
relationships and column-level lineage — to an external data catalog.

Publication is **outbound only**. There is no path that reads an external catalog back into
Provisa, and none is planned: Provisa is the upstream of this relationship. A description edited
in the target catalog is overwritten by the next publish.

## Supported targets

Two open standards are first-class, and vendor catalogs are reached through the adapter that
matches each one's ingestion API.

| `provider` | Target | Protocol |
| --- | --- | --- |
| `openlineage` | Marquez, or any OpenLineage consumer | OpenLineage events posted to `/api/v1/lineage` [tool-verified: provisa/api/metadata_export/openlineage.py:343-345] |
| `openmetadata` | OpenMetadata | Entity upsert by fully-qualified name, then lineage edges by the server-assigned UUID [tool-verified: provisa/api/metadata_export/openmetadata.py:460-540] |
| `atlas` | Apache Atlas, and Microsoft Purview | Atlas RDBMS entities posted to `/api/atlas/v2/entity/bulk` [tool-verified: provisa/api/metadata_export/atlas.py:409] |
| `atlan` | Atlan | The same Atlas-shaped transport, mounted at `/api/meta` and typed by Atlan's own asset types [tool-verified: provisa/api/metadata_export/atlan.py:62-65] |
| `datahub` | DataHub | One aspect proposal per asset facet, posted to `/aspects?action=ingestProposal` [tool-verified: provisa/api/metadata_export/datahub.py:340] |
| `collibra` | Collibra | Assets, relations and attributes upserted through the synchronous import job at `/rest/2.0/import/json-job` [tool-verified: provisa/api/metadata_export/collibra.py:229] |

Purview needs no adapter of its own: its ingestion API *is* the Atlas API — the same routes, the
same entity envelope, the same RDBMS type model — so it is the `atlas` provider pointed at a
Purview endpoint with `auth_mode: entra`. [tool-verified: provisa/api/metadata_export/atlas.py:11-16]

Atlas publishes a source as an `rdbms_instance` with one `rdbms_db` beneath it, tables and columns
as `rdbms_table` and `rdbms_column`, and one `Process` per derived table carrying its inputs,
outputs and compiled transforms. Domains and approved relationships have no Atlas type, so they
ride in `userDescription` as one JSON document; Atlas drops an attribute its type has not declared
rather than refusing it. Governance signals become Atlas classifications, whose typedefs are
registered first — an existing typedef is left alone rather than updated, because an update would
overwrite what a catalog admin has since added to it.
[tool-verified: provisa/api/metadata_export/atlas.py:231-336, 455-481]

DataHub is aspect-oriented rather than entity-oriented: each asset is a URN, and Provisa proposes
only the aspects it owns — `tagProperties` for each governance tag, then `datasetProperties`,
`schemaMetadata`, `globalTags`, `ownership` and `upstreamLineage`, the last carrying the
column-level `fineGrainedLineages`. Aspects Provisa does
not own are never touched. [tool-verified: provisa/api/metadata_export/datahub.py:13-16, 181-330]

Collibra identifies an asset by its name inside a domain inside a community, and its import job
takes the payload as a multipart file part rather than as a JSON body.
[tool-verified: provisa/api/metadata_export/collibra.py:224-263]

The OpenLineage adapter emits one `DatasetEvent` per governed table plus one `RunEvent` per
derived view carrying its column-level lineage. The OpenMetadata adapter upserts database
services, databases, schemas, tables, a user per steward, and domains, creates a classification
and tags for the governance signals, and then adds the lineage edges.
[tool-verified: provisa/api/metadata_export/openmetadata.py:161-221, 259-350]

Two things there are OpenMetadata's own addressing rules rather than Provisa's choices. A domain's
owner is an entity reference the server resolves by the UUID it assigned, so each steward is
upserted as a user first and the id that comes back is substituted at publish time; a steward id
that is not already an address is qualified with the reserved domain `provisa.invalid`, so no
synthesized address can reach a real mailbox. Approved relationships ride in a custom property,
`provisaRelationships`, which the adapter registers on the `table` type before the first table that
carries one — OpenMetadata rejects an extension field its entity type has not declared. The
property is a `string` holding a JSON array because OpenMetadata's own tabular property type caps a
table at three columns and an approved relationship carries eight fields.
[tool-verified: provisa/api/metadata_export/openmetadata.py:71-80, 223-258, 431-459]

## Configuration

Metadata export is a **per-organization** setting: the catalog an org publishes to, and the
credentials it publishes with, belong to that org rather than to the deployment. Configure it in
**Admin → Metadata Export**, or under `metadata_export` in the config YAML.

```yaml
metadata_export:
  enabled: true
  provider: openlineage        # openlineage | openmetadata | atlas | atlan | datahub | collibra
  endpoint: http://marquez:5000
  auth_mode: api_key           # api_key | bearer | basic | entra
  api_key: ${MARQUEZ_API_KEY}
  reconcile_cron: "0 * * * *"
  timeout_seconds: 30
```

[tool-verified: `MetadataExportConfig`, provisa/core/models.py:953-983]

| Setting | Meaning |
| --- | --- |
| `enabled` | Whether this org publishes at all. An enabled target with no `provider` or no `endpoint` is refused when it is saved, not at the next publish. |
| `provider` | Which adapter backs the target. An unrecognized name is refused when the adapter is constructed. |
| `endpoint` | Base URL of the target catalog. |
| `auth_mode` | How the adapter authenticates. `api_key` sends the `api_key` field, `bearer` sends the `token` field, `basic` pairs `username` with the `token` field as HTTP basic — which is stock Apache Atlas's own authentication, and it answers a bearer token with 401 — and `entra` is the Microsoft Entra client-credentials flow that Purview needs, reading `entra_tenant_id`, `entra_client_id` and `entra_client_secret`. |
| `username` | The account name for `basic`. Unused by the other modes. |
| `reconcile_cron` | Cron schedule for this org's full-snapshot reconcile. Re-armed when you save, so a change takes effect without a restart — see [How the target stays current](#how-the-target-stays-current). |
| `timeout_seconds` | Per-request timeout against the target. |

Credentials are **write-only through the UI**. The admin surface reports each one as set or not
set and never returns a stored value, so leaving a credential field blank keeps the stored one and
clearing it removes it. [tool-verified: metadata_export_router.py:131-187]

### Operating it from the Admin tab

- **Test connection** calls the target and reports the text it refused with, which is what
  separates a wrong URL from a rejected credential.
- **Publish now** pushes a full snapshot and shows the outcome, including the per-asset reasons
  behind a partial publish. A catalog that rejects one table does not cost the publish the rest:
  each rejection is reported against the asset it belongs to. [tool-verified: provider.py:46-63]

The tab is available to holders of the `org_settings` right, and only for organizations whose
plan includes metadata export. Both the tab and every endpoint behind it enforce that.

### From the command line

`provisa metadata export` posts to the same `/admin/metadata-export/publish` endpoint that
**Publish now** uses — the single publish path (REQ-1072). Run it from cron or CI when you need
a timed export outside the `reconcile_cron` schedule. [tool-verified: `_cmd_metadata_export` in
provisa/cli.py:272-310; `publish_metadata_export` in
provisa/api/admin/metadata_export_router.py:210-234]

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN" \
  --timeout 300
```

| Flag | Default | Notes |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`, then `http://127.0.0.1:8000` | Under multitenancy, the host names the org (`acme.provisa.org`). [tool-verified: cli.py:284, 413-416] |
| `--token` | `$PROVISA_API_TOKEN` | Bearer token for an identity holding `org_settings`. Omit entirely on unauthenticated deployments — no `Authorization` header is sent when the token is empty. [tool-verified: cli.py:285, 289-290] |
| `--timeout` | `300` | Seconds before the HTTP call is abandoned. [tool-verified: cli.py:425] |

Exit code 0 means every asset published. Exit code 1 means partial publish or a connection
failure. Per-asset errors print to stderr alongside the summary line, so a cron job records them
in the mail log without obscuring the exit code. [tool-verified: cli.py:303-310]

A daily export at 06:00:

```cron
0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

## What is published

[tool-verified: `MetadataSnapshot`, provisa/api/metadata_export/model.py:160-176]

- **Sources, tables and columns** — names, data types, descriptions and aliases. Only tables
  marked **Data Product** in their registration are published; a relationship, lineage edge or
  governance tag that touches an unmarked table is withheld with it, so the catalog never
  receives a reference to a table that was not sent.
  [tool-verified: `build_snapshot`, provisa/api/metadata_export/builder.py]
- **Domains** — each domain's description and its steward. A domain with no steward publishes
  without one rather than with an invented owner.
- **Approved relationships** — the modeled joins, with cardinality, alias, owner and version.
- **Business glossary** — live terms with definitions, typed relationships, and their physical column refs. A term must be in service, defined, and grounded in a published column to export; relationship edges publish only when both endpoint terms do. See [Business Glossary](glossary.md) for the full admission rule and the exclude-from-export control.
- **Lineage** — column-level edges with the transforms applied along each one.

Lineage is derived from the compiled definitions of governed views and the materialized-view DAG,
not inferred by scanning a warehouse. A column that a view computes from two upstream columns
publishes both edges, along with the functions applied.

## Governance signals in the target catalog

Three enforcement facts are projected onto the assets they govern, as facets in OpenLineage and as
classification tags in OpenMetadata: `masked`, `rls_restricted` and `visibility_restricted`.
[tool-verified: `GovernanceSignal`, provisa/api/metadata_export/model.py:131-137]

Each signal names the governed asset, the rule that governs it, the roles it restricts, and the
roles exempt from it. A consumer reading the external catalog can see that a column is masked, and
for whom, without opening Provisa.

**Rule bodies are never published.** A mask pattern or an RLS predicate is the policy itself, and
storing it in an external catalog beside the asset it restricts would hand a reader the shape of
the data the policy exists to withhold. Only the fact and the rule's identity leave Provisa.
[tool-verified: governance.py:11-22]

## How the target stays current

Three paths publish, and all three send the same full snapshot (REQ-1072).

- **Change-driven.** A governed model change queues a publish for the org and a drain sends it
  within about fifteen seconds. The work item is *claimed*, so two Provisa processes never
  publish the same change to the same catalog.
- **Scheduled reconcile.** The org's `reconcile_cron` republishes the whole snapshot, correcting
  drift from an event that never arrived or a catalog restored from a backup.
- **On demand.** **Publish now** in the Admin tab, or `provisa metadata export` from the
  command line — both call the same endpoint and return the same result.
[tool-verified: provisa/api/metadata_export/sync.py:76-200]

They send the same snapshot because a delta on the change path would need its own builder and
its own correctness argument, while the full snapshot already has one: every adapter upserts by
fully-qualified name, so republishing an unchanged asset overwrites it rather than duplicating
it, and the paths converge on the same catalog state instead of racing into different ones.

A publish the catalog rejected is not marked done. The work item stays claimed, the lease
lapses, and the next drain retries it — so a target that was briefly unreachable catches up on
its own rather than waiting for the reconcile.
[tool-verified: provisa/api/metadata_export/sync.py:169-200]

Both scheduled paths are armed per org and re-armed when an admin saves the settings, so a
changed `reconcile_cron` takes effect without a restart. An org that is disabled or below the
REQ-1073 tier has both jobs removed rather than left firing.
[tool-verified: provisa/api/metadata_export/sync.py:225-300]

## Related

- [Security Model](security.md) — the masking, RLS and column-visibility rules these signals report on.
- [Column-Level Lineage](lineage.md) — the lineage graph the published edges come from.
