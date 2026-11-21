# Data Products

A data product is a named, owned bundle of tables published together for consumption. It is the unit the catalog exposes to consumers, a curated set of tables that a domain has declared ready. Fields follow the ODPS (Open Data Product Standard) vocabulary where Provisa already has the source of truth.

## Domain ownership rule

Every data product is owned by exactly one domain (`domain_id` is a required field). A table may join a data product only when both share the same `domain_id`. The UI scopes the table picker to the product's domain; the backend refuses a member whose domain does not match the product's when the product's members are saved.

A product that needs data from another domain must bring that data in as a domain view first, then include the view as a member.

## Output ports

The tables and commands that are members of a data product are its **output ports**, the queryable surface consumers see. Membership is many-to-many: a table, view or command may belong to any number of products in its domain, and a product to any number of members. Members are added and removed on the product only: the `setDataProductMembers` mutation (or the product's `tables` and `commands` lists in configuration). A table's own page lists the products it belongs to, read-only.

## Offers

Access to a data product is by offers. A product starts with one offer, `default`: the `viewer` role, the full data, with data quality results, without profiles, not open. Any other offer is the owner's to add.

Each offer has:

| Field | Notes |
| --- | --- |
| `name` | Unique within the product |
| `role_id` | The role accepting the offer grants. It must be a role the owner may grant: a role their `roles` entitlement list reaches, through a role they act as that holds `assign_entitlements` or `user_management`. Saving any other role is refused by name |
| `data_level` | `full` (the product's datasets), `sample` (one or more existing views over the datasets, named in `sample_views`, which together read every dataset and nothing else) or `synthetic` (every dataset must have a profiler) |
| `dq_results` | Whether the product's data quality results are included |
| `profiles` | Whether the datasets' profiles are included. A synthetic offer has none |
| `open` | Allowed only when the data is already public: the role is one every role of the org inherits, it reads every dataset, and no grant of it leaves a dataset out. Marking an offer open otherwise is refused, naming what is not public. An open offer is no request: the API reports `query_now` for it |
| `duration_days` | How long what accepting grants lasts; none means no end |

A product's datasets are its member tables and views. A checker table the product owns holds its data quality results and is not a dataset.

### Bids

A signed-in requester bids for an offer and states a reason. Provisa does not decide the bid: the bid fires the outbound request hook, and whatever receives it decides. If the offer changes after the bid, accepting is refused and the requester bids again. Declining states a reason. A requester may withdraw a pending bid or cancel an accepted one; either is a cancel, sent through the same hook.

### Request hooks

The org's request hook is a URL and a secret, set at `PUT /admin/data-products/request-hook`. The secret is stored as you enter it, either a value or a reference such as `${secret:NAME}`, and is resolved when it is used, like any other credential. A response says only whether the secret is set; it never returns the secret. Each request is posted to it as JSON, signed in `X-Provisa-Signature` (`sha256=` and the hex HMAC-SHA256 of the body under the secret), with its id as the `Idempotency-Key`:

```json
{"id": "…", "kind": "bid", "requester": "u1", "product": "c360",
 "offer": {"name": "default", "role": "viewer", "data_level": "full", "sample_views": [],
           "dq_results": true, "profiles": false, "duration_days": null},
 "contracts": {"sales.orders": "1.0.0"}, "reason": "churn study"}
```

A cancel carries the bid's terms, `"kind": "cancel"` and the id of the request it cancels in `cancels`.

With no hook configured and no identity provider holding access, a request lands in the product's own queue: the owner role accepts or declines it there, and a cancel is applied at once. Where the identity provider holds access (`auth.assignments_source: claims`), a request with no hook configured is refused.

The decision returns through `POST /admin/data-products/decisions`: `request_id`, `outcome` (`accepted` or `declined`), `decider`, `decided_at` and, for a decline, `reason`. The caller is signed in and decides as themselves, as the product's owner role. A decision is refused when it matches no pending request or when the decider may not grant the offer's role. The same decision sent again is answered with the request as it stands.

Where Provisa holds access, an accepted bid is an ordinary admin grant of exactly what was bid, attributed to the decider. It assigns the role, or re-grants a role the requester already holds, and adds entries to the requester's `tables` entitlement list for that role: the datasets (or, for a sample, the views alone, never the tables beneath them), plus the data quality results and the profiles where the offer includes them. A requester who did not hold the role is granted it narrowed to those entries.

With a duration, the entries end after it, and so does a role grant the acceptance made. A role granted again ends at the latest of the expiries that granted it, and no end is later than any date. An offer's summary states any extension accepting it would make, for example "extends your viewer role from 21 Nov 2026 to no end date".

An accepted cancel removes what the bid's grant alone added: its entries, with their expiry, and the role if the grant made it. It does not bring an extended date back. Where the identity provider holds access, a decision only updates the request's status.

An open offer whose data stops being public is listed for review (`review`). Accepting a synthetic offer is refused for now, because no read yet serves a role the product's synthetic data.

## Contracts

The contracts are derived from the offers. For every offer, each dataset has one ODCS contract, the offer's terms applied to that dataset:

- its schema is the dataset's columns as the offer's role reads them: a column the role does not read is left out, and a masked column is stated as masked;
- its row rules are stated in words (each rule's description);
- its data level: the full dataset, the offer's sample views over it, or synthetic;
- the product's data quality promises are always stated, and the offer says whether their results can be read;
- the dataset's profile tables are listed where the offer includes profiles.

What all of a product's contracts share is written once on the product: its owner, description (`purpose`, `usage`, `limitations`), `freshness` (seconds a read may trail its source; without it, each dataset's own cache freshness applies) and support.

An offer's contracts are derived again whenever the offer is saved. A contract whose terms changed takes its next version: a breaking change (a column removed or masked, a row rule added, less data, data quality results or profiles withdrawn, a shorter duration) takes the next major version, and any other change the next minor one. Each version is kept with its differences from the one before.

The offer contracts supersede the per-table contracts written by hand.

The ODPS export writes one output port per offer and dataset, versioned by and naming that offer's contract, and lists the offers under `customProperties` (`offers`). Each contract is served at `/data-products/{id}/offers/{offer}/contracts/{source.schema.table}.yaml`, and its versions at `.../versions.json`. Each field of a contract names its grounded term, and the terms the product reaches are served as an Apache Ossie ontology at `/data-products/{id}/ontology.ossie.yaml` (see [Grounded terms](metadata-export.md#grounded-terms)).

## Offers API

| Method and path | Who | What |
| --- | --- | --- |
| `GET /admin/data-products/{id}/offers` | `data_product_read` | The offers, each with its contracts and the caller's extension summary |
| `GET /admin/data-products/{id}/grantable-roles` | `data_product_rw` in the domain | The roles the caller may put in an offer |
| `POST /admin/data-products/{id}/offers`, `PUT .../offers/{offer_id}`, `DELETE .../offers/{offer_id}` | `data_product_rw` in the domain, and the product's row rules | Add, change or remove an offer. Removing one declines its pending bids |
| `POST /admin/data-products/{id}/offers/{offer_id}/bids` | a signed-in requester | Bid, stating `reason`; fires the request hook. Refused for an open offer (`query_now`) |
| `GET /admin/data-products/{id}/bids` | the owner role sees every bid, anyone else their own | The bids, newest first |
| `POST /admin/data-products/{id}/bids/{bid_id}/accept`, `.../decline` (with `reason`) | `data_product_read` and the product's owner role | Decide a bid in the local queue, through the decision hook |
| `POST /admin/data-products/{id}/bids/{bid_id}/withdraw`, `.../cancel` (with `reason`) | `data_product_read`, as the requester | Cancel a pending or an accepted bid |
| `POST /admin/data-products/decisions` | `data_product_read` and the product's owner role | The decision hook |
| `GET`, `PUT /admin/data-products/request-hook` | org settings | The org's request hook |
| `GET /admin/data-products/{id}/profiles` | `data_product_read` | Each dataset's latest profile, sanitized for the caller's role: a masked or PII-tagged column shows its type, null rate and distinct count, and no value |

## Detail panel sections

Opening a data product in the admin UI shows these panels:

| Panel | What it shows |
| --- | --- |
| Output Ports | Member tables and their columns; member commands; sample queries (GraphQL, SQL, Cypher, gRPC, JSON:API, REST) |
| Contracts | The product's offers, each opening to the contract it gives on each dataset |
| Related Terms | Glossary terms linked to the product's member tables |
| Related Tables | Tables reachable from member tables through approved relationships but not yet part of the product |
| Relationships | Approved relationships between this product's member tables |
| Lineage | Column lineage graph showing member tables as the published endpoint plus every upstream table. Requires the `view_governance` capability |
| Input Ports | One-hop inputs → transform → outputs derived from lineage. Requires `view_governance` |
| Data Quality | Checker tables whose contracts scan this product's output ports; one row per check per run. Includes a rules modal and PII tag display |

## Metadata export {: #metadata-export }

Only tables assigned to a product publish to external catalogs by default. `build_snapshot` applies a `data_products_only` filter, which withholds unassigned tables along with their relationship edges, lineage edges and governance tags. Sources and domains always publish regardless.

A product with no exported members does not publish. An empty listing would claim a product exists with nothing behind it.

Only catalogs with a native data-product concept publish it as an entity of its own; the rest publish the (already-filtered) member tables without a product grouping:

| Catalog | Published as |
| --- | --- |
| Snowflake Horizon | SHARE + organization listing (native Data Product); `publish=false` keeps it DRAFT, `publish=true` takes it live. The listing's data dictionary covers every member and column; the first five members are featured and masked columns are hidden in the preview |
| BigQuery Analytics Hub | Analytics Hub listing (native) |
| OpenMetadata | `DataProduct` entity (native) |
| DataHub | Native `dataProduct` URN entity with its own properties/ownership aspects |
| Collibra | Asset of a `Data Product` community type, related to member tables |
| Apache Atlas | Best-effort custom `provisa_data_product` typedef, because Atlas has no native data-product type |
| Atlan | Best-effort custom `DataProduct` typedef guess, because Atlan has no documented stable type for this |
| OpenLineage | Not a listing. Member tables carry a `provisa_data_product` custom facet naming the product |

## Fields

| Field | Required | Notes |
| --- | --- | --- |
| `id` | Yes | Machine-readable stable identifier, e.g. `customer_360` |
| `domain_id` | Yes | Owning domain; membership rule enforced against this |
| `name` | Yes | Display name |
| `owner_role` | No | Role accountable for this product; distinct from the domain steward. Naming a role here lets it edit the products that name it (see [row rules](security.md#a-rules-rights)) |
| `team_role` | No | Role whose holders form the day-to-day working team; resolves to individuals |
| `purpose` | No | What this product publishes and why |
| `limitations` | No | Known constraints, caveats, or exclusions |
| `usage` | No | How to consume this product |
| `version` | No | e.g. `1.2.0` |
| `status` | No | `draft`, `live`, `deprecated` or `retired`; defaults to `draft` |
| `support` | No | Free-text support guidance |
| `support_contact` | No | Email or URL; required by Snowflake Horizon Catalog organization listing manifests |
| `publish` | No | `true` to publish Horizon Catalog listings immediately; new listings default to DRAFT |
| `custom_properties` | No | Arbitrary key-value metadata not covered by the standard fields |
| `freshness` | No | Seconds a read may trail its source, promised by every contract of the product |
