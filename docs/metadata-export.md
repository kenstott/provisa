# Metadata Export

Provisa publishes the metadata it governs — tables, columns, domains, stewards, approved
relationships and column-level lineage — to an external data catalog.

Publication is **outbound only**. There is no path that reads an external catalog back into
Provisa, and none is planned: Provisa is the upstream of this relationship. A description edited
in the target catalog is overwritten by the next publish.

## Supported targets

Two open standards have their own adapters, and vendor catalogs are reached through the adapter that
matches each one's ingestion API.

| `provider` | Target | Protocol |
| --- | --- | --- |
| `openlineage` | Marquez, or any OpenLineage consumer | OpenLineage events posted to `/api/v1/lineage` |
| `openmetadata` | OpenMetadata | Entity upsert by fully-qualified name, then lineage edges by the server-assigned UUID |
| `atlas` | Apache Atlas, and Microsoft Purview | Atlas RDBMS entities posted to `/api/atlas/v2/entity/bulk` |
| `atlan` | Atlan (beta) | The same Atlas-shaped transport, mounted at `/api/meta` and typed by Atlan's own asset types |
| `datahub` | DataHub | One aspect proposal per asset aspect, posted to `/aspects?action=ingestProposal` |
| `collibra` | Collibra | Assets, relations and attributes upserted through the synchronous import job at `/rest/2.0/import/json-job` |
| `snowflake_horizon` | Snowflake Horizon Catalog | Engine-native — no HTTP call. Opens its own connection off the engine's configured DSN and runs `CREATE SHARE` / `CREATE ORGANIZATION LISTING` |
| `purview` | Microsoft Purview: the Data Map, then Unified Catalog data products | The Atlas API at `{endpoint}`, then `https://{entra_tenant_id}-api.purview-service.microsoft.com/datagovernance/catalog` |
| `unity_catalog` | Databricks Unity Catalog: a schema of views per data product, optionally a Delta Share | Engine-native: SQL on the Databricks engine's own warehouse connection |
| `datazone` | Amazon DataZone (SageMaker Catalog) data products | The DataZone API of the target's `aws_region`, signed with its AWS keys |
| `dataplex` | Google Dataplex data products | REST calls to `{endpoint}/v1/projects/{gcp_project}/locations/{gcp_location}/dataProducts...` |

### Atlan (beta)

The Atlan adapter has not yet run against a live Atlan tenant. It is built on Atlan's Atlas-shaped
API as Atlan documents it, and its updates follow Atlas's rules: an update starts from the live
asset's attributes, only Provisa's `provisa_*` tags are added or removed, and an asset whose live
read fails is not written. Unit tests cover all of this; no end-to-end run against Atlan does yet.
Until one does, the Admin tab and every page that names Atlan as a target show it as
**Atlan (beta)**.

Snowflake Horizon Catalog and BigQuery Analytics Hub only publish **Data Products** — see [Data Products](data-products.md#metadata-export) — not the general
table/column/lineage snapshot the six adapters above publish. A snapshot with no Data Products
publishes nothing to either.

`snowflake_horizon` is engine-native, unlike every other adapter: Horizon Catalog is not a remote
catalog reached over `endpoint`/`api_key`; it lives inside the same Snowflake account the engine
already runs governed SQL against, so the adapter opens its own connection off the engine's
configured DSN instead. `endpoint` and `api_key` are not settings this provider has, and the config
validator does not require them for it. Each Data Product
becomes a Snowflake `SHARE` over its member tables' physical addresses, wrapped in an internal
`CREATE ORGANIZATION LISTING` (`distribution: ORGANIZATION`, not `EXTERNAL`, because Marketplace-shaped listings never surface in the account's own Horizon Catalog / Data sharing UI). `publish=false` on
the Data Product keeps the listing DRAFT; `publish=true` takes it live immediately.

The listing's data dictionary is Snowflake's own, generated from the share: every member the share
grants appears with its columns and the COMMENTs the landing reconcile wrote. Provisa
keeps the grants equal to the membership on each publish, revoking a former member's `SELECT`. The
manifest adds what the dictionary cannot infer: the first five members in the share's
primary database as `data_dictionary.featured` (Snowflake caps featured objects at five, under one
database), each with its real object kind, and `data_preview.has_pii` plus `pii_columns` from the
masking rules on member columns. Provisa owns the manifest. After the `CREATE`, the live manifest is
read back and replaced with `ALTER LISTING` when it differs, so a change made in Snowsight's listing
wizard does not survive the next publish.

The listing's documentation link is the product's own page in Provisa, at the org's public origin
(`mail.base_url`, by the invitation's host rule) plus `/data-products?product=<id>`; the Analytics
Hub listing carries the same link as its documentation. The Data Products page opens
the product that parameter names. A loopback origin, the install default, links to the hosted
Provisa front door at `cloud.provisa.dev` instead.

Keys are not part of the catalog publish. A landed table's `PRIMARY KEY` and the `FOREIGN KEY`s its
relationships imply belong to the landed model and converge with the tables in the landing
reconcile, for every landed table whether or not a Data Product names it. On Snowflake
the constraints live on the `_landing` replicas and are mirrored onto the per-source views as
`PROVISA_GOVERNANCE.PRIMARY_KEY` / `FOREIGN_KEY` column tags, which is what Horizon Catalog renders
as keys and join paths. The same reconcile writes each landed table's description and column descriptions as COMMENTs and
the stewards' tag assignments as `PROVISA_GOVERNANCE` tags onto the replica, its backing view and
an MV's store table when it is converged. Databricks and BigQuery hold the
same facts on the landed table itself, which is the compiler's physical name there (no view layer):
Unity Catalog informational constraints, COMMENTs and `provisa_governance:<tag>` tags on Databricks
; `NOT ENFORCED` constraints, descriptions and `provisa_governance_<tag>` labels on
BigQuery, where a column tag has no counterpart and is reported withheld. `snowflake_horizon` therefore
publishes no constraints and no model tags of its own; it still appends governance facts to the
descriptions of the Data Product tables it publishes.

`dataplex` publishes each data product as a Google Dataplex data product in the target's
`gcp_project` and `gcp_location`. The product's name, purpose, limitations, usage, service level,
support and documentation link go into its display name and description; its version and status
become labels. Its owners are the email addresses of the members holding its owner role (a
grant in force, with an address in the member directory), and the same members approve access
requests. A product whose owner role has no holder with an address is reported and not sent.
Access groups are not published yet.

Each member is a data asset, by the BigQuery object it is. A member on a BigQuery source is its
own table, in the source's project. A member the BigQuery engine serves from a replica is the
replica's export view in the org's export dataset, which the replica reconcile creates beside the
replica. Any other member, a view among them, has no BigQuery object and is reported by name. Each
asset carries its member's freshness as the `provisa-freshness-seconds` label, and an asset whose
table is no longer a member is removed. Dataplex answers each change with a long-running
operation; the publish follows each to its end, up to `timeout_seconds`.

`purview` publishes in two steps. The Data Map goes first, through the Atlas adapter it
extends: the same entities the `atlas` provider sends, at the target's `endpoint`, signed in with
the Entra client credentials (`auth_mode: entra`). Then each data product becomes a Unified
Catalog data product, at the tenant's Unified Catalog host, in the governance domain named
after the product's domain. A product whose domain has no governance domain of that name is
reported and not sent; Provisa does not create governance domains. Its owners are the members
holding its owner role, their addresses resolved to Entra users through Microsoft Graph, which
needs the app's `User.Read.All` permission; an address Graph does not know is reported. Its
members' Data Map entities become its data assets. Its status follows the product's (active is
published; deprecated and retired are expired; any other is draft; a product with no status is
published when the product is published). Its update frequency is the shortest freshness among
its members. Approvers and access policies are not published yet.

`datazone` publishes each data product as an Amazon DataZone data product in the target's
DataZone domain, owned by its project, and publishes it so that consumers find it and request it
by subscription. Each member is an asset of a Provisa table type, which the first publish creates
in the domain: a `ProvisaTableForm` form (the member's Provisa URI, its address, its columns and
its freshness) on a `ProvisaTable` asset type. The product's fields go in a
`ProvisaDataProductForm` form. A member is read through Provisa, where its governance is enforced,
so DataZone grants nothing itself: the owning project approves each subscription. An asset is
found again by its external identifier, the member's Provisa URI, and a product by its name in the
project; each publish adds a revision and publishes it. Members that are Glue tables are not
published as Glue assets yet.

`unity_catalog` publishes when Databricks is the configured engine, like `snowflake_horizon`
when Snowflake is. Unity Catalog has no data-product entity, so each data product becomes one
schema, `<catalog>.<product id>`, in the catalog the engine works in, holding a view over each
member. Nothing is copied: a view reads its member where the engine keeps it, the member's Delta
replica, or the external table of a lake table read in place. Each view is named for the member's
business name and carries its descriptions; each governance fact is a
`provisa_governance:<signal>` tag naming the rule that governs it, never the rule. The schema's
comment holds the product's purpose, limitations, usage, service level and support; its owner,
team, version and status are `provisa_product:<field>` tags. A view that is no longer a member is
dropped. With `delta_share` set, the product's views are also published as the Delta Share
`provisa_<product id>`, which a Databricks admin grants to the recipients of other data teams;
sharing needs the member in Delta, so a member stored in another format is reported and left out.
Publishing as a listing in a Databricks private exchange is a later phase.

Purview needs no adapter of its own: its ingestion API *is* the Atlas API — the same routes, the
same entity envelope, the same RDBMS type model — so it is the `atlas` provider pointed at a
Purview endpoint with `auth_mode: entra`.

Atlas publishes a source as an `rdbms_instance` with one `rdbms_db` beneath it, tables and columns
as `rdbms_table` and `rdbms_column`, and one `Process` per derived table carrying its inputs,
outputs and compiled transforms. Domains and approved relationships have no Atlas type, so they
ride in `userDescription` as one JSON document; Atlas drops an attribute its type has not declared
rather than refusing it. Governance signals become Atlas classifications, whose typedefs are registered first. An existing typedef is left alone rather than updated, because an update would
overwrite what a catalog admin has since added to it.

DataHub is aspect-oriented rather than entity-oriented: each asset is a URN, and Provisa proposes
only the aspects it owns — `tagProperties` for each governance tag, then `datasetProperties`,
`schemaMetadata`, `globalTags`, `ownership` and `upstreamLineage`, the last carrying the
column-level `fineGrainedLineages`. Aspects Provisa does
not own are never touched.

Collibra identifies an asset by its name inside a domain inside a community, and its import job
takes the payload as a multipart file part rather than as a JSON body.

The OpenLineage adapter emits one `DatasetEvent` per governed table plus one `RunEvent` per
derived view carrying its column-level lineage. The OpenMetadata adapter upserts database
services, databases, schemas, tables, a user per steward, and domains, creates a classification
and tags for the governance signals, and then adds the lineage edges.

Two things there are OpenMetadata's own addressing rules rather than Provisa's choices. A domain's
owner is an entity reference the server resolves by the UUID it assigned, so each steward is
upserted as a user first and the id that comes back is substituted at publish time; a steward id
that is not already an address is qualified with the reserved domain `provisa.invalid`, so no
synthesized address can reach a real mailbox. Approved relationships ride in a custom property,
`provisaRelationships`, which the adapter registers on the `table` type before the first table that carries one, because OpenMetadata rejects an extension field its entity type has not declared. The
property is a `string` holding a JSON array because OpenMetadata's own tabular property type caps a
table at three columns and an approved relationship carries far more than that.

## Configuration

Metadata export is a **per-organization** setting: the catalogs an org publishes to, and the
credentials it publishes with, belong to that org rather than to the deployment. An org may publish
to several catalogs at once, two of one provider among them; one publish reaches every target.
Configure it in **Admin → Metadata Export**, or under `metadata_export` in the config YAML.

```yaml
metadata_export:
  enabled: true
  reconcile_cron: "0 * * * *"
  targets:
    - id: marquez
      provider: openlineage    # openlineage | openmetadata | atlas | purview | atlan | datahub | collibra | snowflake_horizon | unity_catalog | dataplex | datazone
      endpoint: http://marquez:5000
      auth_mode: api_key       # api_key | bearer | basic | entra | google_service_account
      api_key: ${MARQUEZ_API_KEY}
      timeout_seconds: 30
    - id: openmetadata-prod
      provider: openmetadata
      endpoint: https://openmetadata.example.com
      auth_mode: bearer
      token: ${OPENMETADATA_TOKEN}
    - id: dataplex
      provider: dataplex
      endpoint: https://dataplex.googleapis.com
      auth_mode: google_service_account
      google_service_account_json: ${DATAPLEX_SERVICE_ACCOUNT_JSON}
      gcp_project: acme-data
      gcp_location: us
```

| Setting | Meaning |
| --- | --- |
| `enabled` | Whether this org publishes at all. An enabled export with no target is refused when it is saved, not at the next publish. |
| `targets` | The catalogs the org publishes to. A target left out of the list when the settings are saved is removed. |
| `id` | The target's name within the org, unique there. What Provisa records of each published asset's id in that catalog is kept under it, so two targets of one provider never share it. |
| `provider` | Which adapter backs the target. An unrecognized name is refused when the adapter is constructed. |
| `endpoint` | Base URL of the target catalog. A target with no endpoint is refused when it is saved. Not required for `snowflake_horizon`, which connects off the engine's own configured DSN instead. |
| `auth_mode` | How the adapter authenticates. `api_key` sends the `api_key` field, `bearer` sends the `token` field, `basic` pairs `username` with the `token` field as HTTP basic — which is stock Apache Atlas's own authentication, and it answers a bearer token with 401 — `entra` is the Microsoft Entra client-credentials flow that Purview needs, reading `entra_tenant_id`, `entra_client_id` and `entra_client_secret`, and `google_service_account` signs in to Dataplex with the service account key in `google_service_account_json`. |
| `username` | The account name for `basic`. Unused by the other modes. |
| `gcp_project`, `gcp_location` | Where a `dataplex` target keeps the org's data products. A `dataplex` target without both is refused when it is saved. |
| `aws_region`, `datazone_domain_id`, `datazone_project_id` | The DataZone domain a `datazone` target publishes to and the project that owns its data products. A `datazone` target without all three is refused when it is saved; it has no `endpoint`. |
| `aws_access_key_id`, `aws_secret_access_key` | The AWS keys a `datazone` target signs its requests with. |
| `delta_share` | Whether a `unity_catalog` target also publishes each product as a Delta Share. A `unity_catalog` target has no `endpoint`: it publishes through the Databricks engine. |
| `reconcile_cron` | Cron schedule for this org's full-snapshot reconcile. Re-armed when you save, so a change takes effect without a restart — see [How the target stays current](#how-the-target-stays-current). |
| `timeout_seconds` | Per-request timeout against the target. |

Credentials are **write-only through the UI**. The admin surface reports each target's credentials as
set or not set and never returns a stored value, so leaving a credential field blank keeps the one
stored for that target and clearing it removes it.

### Operating it from the Admin tab

- **Test connection** calls each target and reports the text it refused with, which is what
  separates a wrong URL from a rejected credential.
- **Publish now** pushes a full snapshot to every target and shows each one's outcome, including
  the per-asset reasons behind a partial publish. A catalog that rejects one table does not cost the publish the rest:
  each rejection is reported against the asset it belongs to.

The tab is available to holders of the `org_settings` right. On a self-hosted install nothing else
gates it. On the hosted service the organization's plan must include metadata export: the plan sets
the org's tier, which a platform administrator can change in **Admin → Orgs**. Both the tab and every
endpoint behind it enforce that.

### From the command line

`provisa metadata export` posts to the same `/admin/metadata-export/publish` endpoint that **Publish now** uses, so there is one publish path. Run it from cron or CI when you need
a timed export outside the `reconcile_cron` schedule.

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN" \
  --timeout 300
```

| Flag | Default | Notes |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`, then `http://127.0.0.1:8000` | Under multitenancy, the host names the org (`acme.provisa.org`). |
| `--token` | `$PROVISA_API_TOKEN` | Bearer token for an identity holding `org_settings`. Omit entirely on unauthenticated deployments — no `Authorization` header is sent when the token is empty. |
| `--timeout` | `300` | Seconds before the HTTP call is abandoned. |

Exit code 0 means every asset published to every target. Exit code 1 means a partial publish to any
target, or a connection failure. Each target gets its own summary line; per-asset errors print to
stderr, naming the target, so a cron job records them
in the mail log without obscuring the exit code.

A daily export at 06:00:

```cron
0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

## What is published

- **Sources, tables and columns** — names, data types, descriptions and aliases. Only tables
  marked **Data Product** in their registration are published; a relationship, lineage edge or
  governance tag that touches an unmarked table is withheld with it, so the catalog never
  receives a reference to a table that was not sent.

- **Domains** — each domain's description and its steward. A domain with no steward publishes
  without one rather than with an invented owner.
- **Approved relationships** — the modeled joins, with cardinality, alias, owner and version. Each
  edge also carries its `kind`. A `junction` edge reaches its target through an associative table
  rather than a column pair, so its `sourceColumn`/`targetColumn` are the two endpoint keys and a
  catalog that read them as a join condition would invent a foreign key that does not exist; the
  junction table, its two key columns, and the discriminator that splits it into several edge types
  publish alongside as `via`.

- **Ontology** — live terms with their definitions and typed relationships, each term's role (class, property or concept), and Provisa's assignment of each term to the data it means: a class term to the tables it grounds, a property term to its columns. A term must be in service, defined, and grounded to export; a relationship publishes only when both its terms do. See [Ontology](ontology.md) for the admission rule and the exclude-from-export control. Each target publishes the terms into one glossary Provisa owns, as the table below shows. Provisa writes and removes only its own assignments, recognized by the catalog's own record of who made them, so a steward's assignment is never removed or duplicated. `openlineage`, `snowflake_horizon` and `unity_catalog` publish no terms.

| Target | Terms | Relationships | Role | Assignment to data |
| --- | --- | --- | --- | --- |
| `openmetadata` | The `provisa_<org>` glossary, beneath a Classes, Properties or Concepts group term | `KIND_OF` as the term hierarchy, `SYNONYM_OF` as synonyms, the rest as related terms | The `provisaRole` custom property | `Automated` glossary labels on tables and columns |
| `datahub` | The org's Provisa node, in its Classes, Properties or Concepts child node | Inherits, contains and related terms | `provisaRole` in the term's custom properties | Term associations attributed to Provisa's actor |
| `atlas`, Purview's Data Map | The Provisa glossary | Atlas's own term relations | `provisaRole` in the term's additional attributes | Semantic assignments whose source is Provisa |
| `atlan` | The Provisa glossary | Atlas's own term relations | `provisaRole` in the term's additional attributes | Semantic assignments whose source is Provisa |
| `collibra` | The Provisa Glossary domain | Collibra's term relation types | The `Provisa Role` attribute, which the target must have | Relations created by Provisa's own user |
| `purview`, Unified Catalog | The governance domain named after the term's domain; for a term in several, the first of them alphabetically that Purview has | `KIND_OF` as the parent, synonym and related relationships | Not exported: the Unified Catalog term has no free property | A class term's relationship to its tables' data assets; a property term's columns reach Purview through the Data Map only |
| `dataplex` | The `provisa-<org>` glossary, in a Classes, Properties or Concepts category | Synonym and related entry links | The `provisa-role` label | Definition entry links from BigQuery tables and columns, whose ids Provisa gives the `provisa-` prefix; a table whose entry Dataplex has not catalogued within a minute is reported by name |
| `datazone` | The `Provisa glossary (<org>)` glossary | `KIND_OF` as `isA`; DataZone has no synonym or related relation | Not exported: the glossary term has no free property | None: DataZone records no author for an assignment, so Provisa's could not be told from a steward's |

- **Lineage** — column-level edges with the transforms applied along each one.

Lineage is derived from the compiled definitions of governed views and the materialized-view DAG,
not inferred by scanning a warehouse. A column that a view computes from two upstream columns
publishes both edges, along with the functions applied.

## Data products as ODPS and ODCS

Each data product is also served in the two open standards a catalog can import without an adapter:
an [Open Data Product Standard](https://github.com/bitol-io/open-data-product-standard) 1.0
document for the product, and an
[Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard) 3.1
contract for each of its member tables.

```http
GET /data-products/{product id}/odps.yaml
GET /data-products/{product id}/contracts/{source}.{schema}.{table}.yaml
```

An OpenMetadata target (2.0.3 or later) is also sent each member's contract through its ODCS
import, attached to the member table and replacing the contract it had, so the contract shows on
the table in OpenMetadata.

Both are built from the same snapshot the export publishes. Reading them requires the
`data_product_read` right, and on the hosted service a plan that includes metadata export.

The ODPS document carries the product's purpose, limitations and usage as its description, plus
its status, version, domain, team roles and support contact. It lists one output port per member,
naming that member's contract. The SLA stays prose and rides as a custom property, because a product
spans several members and a structured SLA could not say which one it describes.

Each contract carries the member's columns with their types, descriptions and primary key, plus:

- the governance tags on each column (`masked`, `visibility_restricted`, `rls_restricted`), never
  the rule behind them;
- the member's freshness: its `cache_ttl`, else its source's, as a latency promise; a member that
  declares neither makes no freshness promise;
- the quality checks registered against the member, as the checker that runs them wrote them.

A member has no version of its own, so its contract takes the product's. A product with no version
publishes `unversioned`, since both standards require a version and no release was cut. A product
with no status is `active` when published and `draft` when not.

## Governance signals in the target catalog

Three enforcement facts are projected onto the assets they govern, as OpenLineage run/dataset metadata and as
classification tags in OpenMetadata: `masked`, `rls_restricted` and `visibility_restricted`.

Each signal names the governed asset, the rule that governs it, the roles it restricts, and the
roles exempt from it. A consumer reading the external catalog can see that a column is masked, and
for whom, without opening Provisa.

**Rule bodies are never published.** A mask pattern or an RLS predicate is the policy itself, and
storing it in an external catalog beside the asset it restricts would hand a reader the shape of
the data the policy exists to withhold. Only the fact and the rule's identity leave Provisa.

## How the target stays current

Three paths publish, and all three send the same full snapshot.

- **Change-driven.** A governed model change queues a publish for the org and a drain sends it
  within about fifteen seconds. The work item is *claimed*, so two Provisa processes never
  publish the same change to the same catalog.
- **Scheduled reconcile.** The org's `reconcile_cron` republishes the whole snapshot, correcting
  drift from an event that never arrived or a catalog restored from a backup.
- **On demand.** **Publish now** in the Admin tab, or `provisa metadata export` from the command line. Both call the same endpoint and return the same result.

They send the same snapshot because a delta on the change path would need its own builder and
its own correctness argument, while the full snapshot already has one: every adapter upserts by
fully-qualified name, so republishing an unchanged asset overwrites it rather than duplicating
it, and the paths converge on the same catalog state instead of racing into different ones.

A publish the catalog rejected is not marked done. The work item stays claimed, the lease
lapses, and the next drain retries it, so a target that was briefly unreachable catches up on
its own rather than waiting for the reconcile.

Both scheduled paths are armed per org and re-armed when an admin saves the settings, so a
changed `reconcile_cron` takes effect without a restart. An org that is disabled or below the
Premium tier has both jobs removed rather than left firing.

## Related

- [Security Model](security.md) — the masking, RLS and column-visibility rules these signals report on.
- [Column-Level Lineage](lineage.md) — the lineage graph the published edges come from.
