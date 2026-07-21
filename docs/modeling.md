# Data Modeling (Entities & Facts)

Provisa gives you two declarative primitives — `entity` and `fact` — that cover the building
blocks every star schema and Data Vault is assembled from. Declare the spec; Provisa lowers it to
exactly the materialized-view, bitemporal, and relationship definitions you would otherwise
hand-write (REQ-1164). [tool-verified: modeling.py module docstring lines 11-28]

## What entities and facts are

An **entity** is a keyed, deduplicated, optionally-historized projection of a source relation. You
name it, point it at a source, declare the business key and the attributes you want to carry,
and choose a history mode. Provisa writes the view SQL and registers the MV. When history is
enabled, the MV is bitemporal. [tool-verified: `Entity` dataclass, modeling.py lines 53-69;
`entity_registration` function, modeling.py lines 105-120]

A **fact** is a join to entity keys, reduced to a declared grain, with aggregated measures. Provisa
writes an aggregate-MV query (`GROUP BY` grain + FK columns) and registers a relationship for each
declared dimension link. A fact with no measures is a pure key-set — the Data Vault link pattern.
[tool-verified: `Fact` dataclass, modeling.py lines 91-102; `fact_registration` function, modeling.py
lines 123-141; comment at line 130 "a measureless fact is a pure key-set (DV link)"]

Both constructs are IR. The generated definitions retarget across engines — materialized in Oracle,
Databricks, or left virtual over an MPP engine — without remodeling. [tool-verified: modeling.py
docstring lines 25-28]

## History modes

Three modes are available on an entity [tool-verified: `_HISTORY` constant at modeling.py line 38,
`_HISTORY_MODE` dict at modeling.py line 40]:

| Mode | Meaning | Bitemporal mode |
|---|---|---|
| `none` | Current-only. No history. | — |
| `scd2` | Track every change. Append only changed rows (delta) keyed on the business key. | `delta` |
| `snapshot` | Track every refresh. Append the full result set each refresh, stamped with system time. | `snapshot` |

`scd2` needs a business key to compute the delta. `snapshot` works on any engine but storage grows
by a full copy per refresh. Pick `scd2` for large, slowly-changing sources; pick `snapshot` when
you need full history and the source can't supply a key.

Facts have no history mode — temporal coverage comes from the underlying entity history.

## Measures and aggregations

Measures are declared as `column:agg` pairs. Supported aggregations [tool-verified: `_AGGS`
at modeling.py line 41]:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

The default aggregation is `sum` [tool-verified: `Measure.agg` default at modeling.py line 75].

## Worked example: Customer entity + Sales fact

### The source tables

- `raw.customers` — id, name, region, tier
- `raw.orders` — order_id, customer_id, amount, quantity

### Register the Customer entity

=== "Admin UI"

    1. Open **Tables** and click **+ Model**.
    2. Choose **Entity (dimension)**.
    3. Fill the form:
       - **Name:** `Customer`
       - **Source relation:** `raw.customers`
       - **Domain:** *(your domain)*
       - **Business key:** `id`
       - **Attributes:** `name, region, tier`
       - **History:** `SCD2 (track changes — delta bitemporal)`
    4. Click **Create**.

=== "GraphQL API"

    ```graphql
    mutation {
      registerEntity(input: {
        name: "Customer"
        source: "raw.customers"
        domainId: "sales"
        key: ["id"]
        attributes: ["name", "region", "tier"]
        history: "scd2"
      }) {
        success
        message
      }
    }
    ```

Provisa generates and registers this bitemporal MV [tool-verified: `entity_registration` in
modeling.py lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, business key: ["id"]
```

### Register the Sales fact

=== "Admin UI"

    1. Click **+ Model** again.
    2. Choose **Fact**.
    3. Fill the form:
       - **Name:** `Sales`
       - **Source relation:** `raw.orders`
       - **Domain:** *(your domain)*
       - **Grain:** `order_id`
       - **Measures:** `amount:sum, quantity:sum`
       - **Dimensions:** `Customer:customer_id`
    4. Click **Create**.

=== "GraphQL API"

    ```graphql
    mutation {
      registerFact(input: {
        name: "Sales"
        source: "raw.orders"
        domainId: "sales"
        grain: ["order_id"]
        measures: [
          { column: "amount", agg: "sum" }
          { column: "quantity", agg: "sum" }
        ]
        dimensions: [
          { entity: "Customer", via: "customer_id" }
        ]
      }) {
        success
        message
      }
    }
    ```

Provisa generates and registers [tool-verified: `fact_registration` in modeling.py lines 123-141]:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Plus one registered relationship: `Sales.customer_id → Customer` (cardinality: many-to-one).
[tool-verified: `fact_table_input` in modeling_register.py lines 89-98, cardinality at line 95]

## The Model form (admin UI)

The **+ Model** button appears on the **Tables** page (tooltip: "Model an entity or fact (star
schema / Data Vault)"). [tool-verified: tablesPage.json line 13; TablesPage.tsx lines 441-450]

A segmented control at the top of the modal switches between **Entity (dimension)** and **Fact**.
[tool-verified: ModelingForm.tsx lines 102-110]

### Entity fields

[tool-verified: ModelingForm.tsx lines 141-171; modelingForm.json]

| Field | Required | Notes |
|---|---|---|
| Name | yes | The MV name in the catalog |
| Source relation | yes | Dotted relation, e.g. `raw.customers` |
| Domain | yes | Domain the MV belongs to |
| Business key | yes | Comma-separated key column(s), e.g. `id` |
| Attributes | no | Comma-separated attribute columns, e.g. `name, region, tier` |
| History | no | `none` / `scd2` / `snapshot`; default is `none` |

### Fact fields

[tool-verified: ModelingForm.tsx lines 172-196; modelingForm.json]

| Field | Required | Notes |
|---|---|---|
| Name | yes | The MV name in the catalog |
| Source relation | yes | Dotted relation, e.g. `raw.orders` |
| Domain | yes | Domain the MV belongs to |
| Grain | yes | Comma-separated grain column(s), e.g. `order_id` |
| Measures | no | `col:agg` pairs, comma-separated, e.g. `amount:sum, quantity:sum` |
| Dimensions | no | `Entity:fk_column` pairs, comma-separated, e.g. `Customer:customer_id` |

When `agg` is omitted in a measure (`amount` instead of `amount:sum`), it defaults to `sum`.
[tool-verified: ModelingForm.tsx line 73 `agg: agg || "sum"`]

## The GraphQL API

Both mutations live in the admin schema. [tool-verified: schema_mutation.py lines 449-472]

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

`EntityInput` fields [tool-verified: types.py lines 449-456]:

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | String | — | Catalog name for the entity MV |
| `source` | String | — | Source relation (`schema.table` or quoted) |
| `domainId` | String | — | Domain id |
| `key` | [String] | — | Business key column(s) |
| `attributes` | [String] | `[]` | Attribute columns to project |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
| `visibleTo` | [String] | `["public"]` | Role visibility list |

### `registerFact`

```graphql
mutation RegisterFact($input: FactInput!) {
  registerFact(input: $input) {
    success
    message
  }
}
```

`FactInput` fields [tool-verified: types.py lines 472-479]:

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | String | — | Catalog name for the fact MV |
| `source` | String | — | Source relation |
| `domainId` | String | — | Domain id |
| `grain` | [String] | — | Grain column(s) for the GROUP BY |
| `measures` | [MeasureInput] | `[]` | `{ column, agg }` pairs |
| `dimensions` | [DimRefInput] | `[]` | `{ entity, via }` pairs |
| `visibleTo` | [String] | `["public"]` | Role visibility list |

`MeasureInput`: `{ column: String, agg: String }` — agg defaults to `"sum"`.
[tool-verified: types.py lines 460-462]

`DimRefInput`: `{ entity: String, via: String }` — `entity` is the referenced entity name;
`via` is the FK column on the fact source.
[tool-verified: types.py lines 465-468]

On success, `registerFact` returns a message of the form:
`Fact 'Sales' registered with 1 dimension link(s)`.
[tool-verified: schema_mutation.py line 471]

## Kimball star schema and Data Vault

Neither pattern requires separate tooling. The same two primitives compose into both.

**Kimball star schema**

- A dimension is an entity with the attributes you want to conformed-select. Choose `none` for
  SCD Type 1 (current only, rebuilt on refresh). Choose `scd2` for SCD Type 2 (row-versioned,
  full history in the bitemporal MV). [tool-verified: modeling.py docstring line 18-20]
- A fact table is a fact. Declare the grain, the additive measures, and one dimension reference
  per FK. Provisa writes the GROUP BY and registers the join paths. [tool-verified: modeling.py
  docstring lines 21-23]

**Data Vault**

- A hub is an entity with `history: "none"` — the deduplicated business-key set.
- A satellite is an entity with `history: "scd2"` or `history: "snapshot"` — the time-stamped
  attribute record beside the hub key. [tool-verified: modeling.py docstring line 19]
- A link is a fact with no measures — a pure key-set joining two or more entities. Add
  `measures` to turn it into a Point-in-Time or bridge aggregate. [tool-verified: modeling.py
  line 130 comment]

The model does not decide the methodology. Grain, conformance, SCD choice, and the hub/satellite
split remain the modeler's decisions. Provisa executes them. [tool-verified: modeling.py
docstring lines 25-26]

## The IR payoff

Every registration call goes through the same path as a hand-written MV. The entity/fact spec
is an intermediate representation — not a template, not a macro. The warehouse it targets is a
property of the deployment, not of the model. Change the target engine and the same `entity` /
`fact` declarations materialize there, because the generated SQL and bitemporal modes are
engine-neutral by construction. [tool-verified: modeling.py docstring lines 25-28;
modeling_register.py lines 56-66, 80-88]
