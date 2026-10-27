# Data Products

A data product is a named, owned bundle of tables published together for consumption. It is the unit the catalog exposes to consumers: a curated set of tables that a domain has declared ready. Fields follow the ODPS (Open Data Product Standard) vocabulary where Provisa already has the source of truth.

## Domain ownership rule

Every data product is owned by exactly one domain (`domain_id` is a required field). A table may join a data product only when both share the same `domain_id`. The UI scopes the table picker to the product's domain; the backend rejects a `product_id` assignment whose domain does not match the product's at save time.

A product that needs data from another domain must bring that data in as a domain view first, then include the view as a member.

## Output ports

The tables and commands assigned to a data product are its **output ports**: the queryable surface consumers see. Assigning a table sets `Table.product_id`; clearing it removes the membership. A table belongs to at most one product. Commands in the same domain may also be assigned as members.

## Contracts

Each member table is an output port with its own data contract, in the ODCS format. A table gets a draft contract at version `0.1.0` when it joins a product, promising every column it has. The contract has its own version, separate from the product's. A checker table that a product owns is not an output port and has no contract.

A contract holds:

| Field | Notes |
| --- | --- |
| `version` | Required. Changed when the contract stops promising a column or promises it as another type |
| `status` | `proposed`, `draft`, `active`, `deprecated` or `retired` |
| `description` | What the port is for |
| `terms` | `usage`, `limitations` and `notice_period` (for example `P3M`) |
| `sla` | Service levels such as availability or retention. Freshness is not declared here: it comes from the table's cache TTL |
| `columns` | The columns the port promises: name, type, description and whether the column is never null |

Two rules keep a contract and its table in step:

- A contract saved at the same version may not drop a promised column or change its type. Save it at a new version.
- A table save may not drop or retype a column its contract promises. Release a new contract version without the column first. In a configuration file, the table and its new contract can change in the same save.

Edit contracts on the product's form, in its Contracts panel, or declare them in configuration on the table:

```yaml
tables:
  - source_id: warehouse
    schema_name: public
    table_name: daily_revenue
    product_id: revenue
    contract:
      version: 1.0.0
      status: active
      terms: {usage: Join on day}
      sla:
        - {property: availability, value: 99.9, unit: "%"}
      columns:
        - {name: day, type: date, required: true}
        - {name: amount, type: "numeric(12, 2)"}
```

The ODPS export writes each output port with its contract's version and the contract itself, at `/data-products/{id}/contracts/{source.schema.table}.yaml`.

## Detail panel sections

Opening a data product in the admin UI shows these panels:

| Panel | What it shows |
| --- | --- |
| Output Ports | Member tables and their columns; member commands; sample queries (GraphQL, SQL, Cypher, gRPC, JSON:API, REST) |
| Contracts | Each output port's contract: its version and status, terms, service levels and promised columns |
| Related Terms | Glossary terms linked to the product's member tables |
| Related Tables | Tables reachable from member tables through approved relationships but not yet part of the product |
| Relationships | Approved relationships between this product's member tables |
| Lineage | Column lineage graph showing member tables as the published endpoint plus every upstream table. Requires the `view_governance` capability |
| Input Ports | One-hop inputs → transform → outputs derived from lineage. Requires `view_governance` |
| Data Quality | Checker tables whose contracts scan this product's output ports; one row per check per run. Includes a rules modal and PII tag display |

## Metadata export {: #metadata-export }

Only tables assigned to a product publish to external catalogs by default. `build_snapshot` applies a `data_products_only` filter: unassigned tables are withheld, along with their relationship edges, lineage edges, and governance tags. Sources and domains always publish regardless.

A product with no exported members does not publish. An empty listing would claim a product exists with nothing behind it.

Only catalogs with a native data-product concept publish it as an entity of its own; the rest publish the (already-filtered) member tables without a product grouping:

| Catalog | Published as |
| --- | --- |
| Snowflake Horizon | SHARE + organization listing (native Data Product); `publish=false` keeps it DRAFT, `publish=true` takes it live. The listing's data dictionary covers every member and column; the first five members are featured and masked columns are hidden in the preview |
| BigQuery Analytics Hub | Analytics Hub listing (native) |
| OpenMetadata | `DataProduct` entity (native) |
| DataHub | Native `dataProduct` URN entity with its own properties/ownership aspects |
| Collibra | Asset of a `Data Product` community type, related to member tables |
| Apache Atlas | Best-effort custom `provisa_data_product` typedef — Atlas has no native data-product type |
| Atlan | Best-effort custom `DataProduct` typedef guess — Atlan has no documented stable type for this |
| OpenLineage | Not a listing — member tables carry a `provisa_data_product` custom facet naming the product |

## Fields

| Field | Required | Notes |
| --- | --- | --- |
| `id` | Yes | Machine-readable stable identifier, e.g. `customer_360` |
| `domain_id` | Yes | Owning domain; membership rule enforced against this |
| `name` | Yes | Display name |
| `owner_role` | No | Role accountable for this product; distinct from the domain steward |
| `team_role` | No | Role whose holders form the day-to-day working team; resolves to individuals |
| `purpose` | No | What this product publishes and why |
| `limitations` | No | Known constraints, caveats, or exclusions |
| `usage` | No | How to consume this product |
| `version` | No | e.g. `1.2.0` |
| `status` | No | e.g. `proposed`, `active`, `deprecated`, `retired` |
| `support` | No | Free-text support guidance |
| `support_contact` | No | Email or URL; required by Snowflake Horizon Catalog organization listing manifests |
| `publish` | No | `true` to publish Horizon Catalog listings immediately; new listings default to DRAFT |
| `custom_properties` | No | Arbitrary key-value metadata not covered by the standard fields |
