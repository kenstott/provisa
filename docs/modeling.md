<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Data Modeling (Entities & Facts)

Provisa gives you two declarative primitives — `entity` and `fact` — that cover the building
blocks every star schema and Data Vault is assembled from. Declare the spec; Provisa lowers it to
exactly the materialized-view, bitemporal, and relationship definitions you would otherwise
hand-write (REQ-1164). [tool-verified: modeling.py module docstring lines 11-28]

## What entities and facts are

An **entity** is a keyed, deduplicated, optionally-historized projection of a source relation. You
name it, point it at a source, declare the entity key and the attributes you want to carry,
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
| --- | --- | --- |
| `none` | Current-only. No history. | — |
| `scd2` | Track every change. Append only changed rows (delta) keyed on the entity key. | `delta` |
| `snapshot` | Track every refresh. Append the full result set each refresh, stamped with system time. | `snapshot` |

`scd2` needs an entity key to compute the delta. `snapshot` works on any engine but storage grows
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
       - **Entity key:** `id`
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
-- registered as a bitemporal delta MV, entity key: ["id"]
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
| --- | --- | --- |
| Name | yes | The MV name in the catalog |
| Source relation | yes | Dotted relation, e.g. `raw.customers` |
| Domain | yes | Domain the MV belongs to |
| Entity key | yes | Comma-separated key column(s), e.g. `id` |
| Attributes | no | Comma-separated attribute columns, e.g. `name, region, tier` |
| History | no | `none` / `scd2` / `snapshot`; default is `none` |

### Fact fields

[tool-verified: ModelingForm.tsx lines 172-196; modelingForm.json]

| Field | Required | Notes |
| --- | --- | --- |
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
| --- | --- | --- | --- |
| `name` | String | — | Catalog name for the entity MV |
| `source` | String | — | Source relation (`schema.table` or quoted) |
| `domainId` | String | — | Domain id |
| `key` | [String] | — | Entity key column(s) |
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
| --- | --- | --- | --- |
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

### Kimball star schema

This walkthrough builds a three-dimension star. Two source tables are new:

- `raw.products` — `product_id`, `name`, `category`, `list_price` [inferred: introduced for this example]
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month` [inferred: introduced for this example]

`raw.orders` also gains `product_id` and `order_date` columns here. [inferred]

#### Choosing SCD type

History mode is the only dial between SCD Type 1 and Type 2:

| SCD type | History mode | Effect |
| --- | --- | --- |
| Type 1 (current only) | `none` | MV rebuilt on refresh; no row history |
| Type 2 (versioned) | `scd2` | Bitemporal delta MV; each change appends a new row keyed on the entity key |

[tool-verified: `_HISTORY_MODE` at modeling.py line 40; `entity_registration` history branch at
lines 115-119]

Use `scd2` when downstream queries need to join a dimension as it existed at transaction time — a
customer's tier at the moment of purchase, not their current tier. Use `none` for stable lookups.
A date spine never changes. A product catalog where you only need the current price can rebuild on
every refresh.

#### Grain decision

The grain is the lowest level of detail the fact answers. `order_id` gives one row per order,
preserving the ability to count distinct orders and join to any dimension at order granularity.
A coarser grain — say `["customer_id", "order_date"]` — pre-aggregates across orders and discards
that detail permanently. Declare the narrowest grain the business needs; coarser rollups are cheap
to derive afterward.

#### Register the dimensions

**Customer** (SCD Type 2 — tier changes must be preserved):

```graphql
mutation {
  registerEntity(input: {
    name: "Customer"
    source: "raw.customers"
    domainId: "sales"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

Generates a bitemporal delta MV keyed on `id` [tool-verified: entity_registration modeling.py
lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

**Product** (SCD Type 1 — current catalog, no version history needed):

```graphql
mutation {
  registerEntity(input: {
    name: "Product"
    source: "raw.products"
    domainId: "sales"
    key: ["product_id"]
    attributes: ["name", "category", "list_price"]
    history: "none"
  }) { success message }
}
```

Generates an ordinary MV rebuilt on refresh [tool-verified: entity_registration modeling.py
lines 105-114; `mv_bitemporal_mode` is only added when `history != "none"`, line 115]:

```sql
SELECT "product_id", "name", "category", "list_price" FROM "raw"."products"
```

**DateDim** (no history — a date is immutable):

```graphql
mutation {
  registerEntity(input: {
    name: "DateDim"
    source: "raw.date_spine"
    domainId: "sales"
    key: ["date_key"]
    attributes: ["year", "quarter", "month"]
    history: "none"
  }) { success message }
}
```

Generates:

```sql
SELECT "date_key", "year", "quarter", "month" FROM "raw"."date_spine"
```

#### Register the Sales fact across three dimensions

Grain: `order_id`. Three dimension references — one FK column each. Both measures are additive sums.

```graphql
mutation {
  registerFact(input: {
    name: "Sales"
    source: "raw.orders"
    domainId: "sales"
    grain: ["order_id"]
    measures: [
      { column: "amount",   agg: "sum" }
      { column: "quantity", agg: "sum" }
    ]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
      { entity: "DateDim",  via: "order_date"  }
    ]
  }) { success message }
}
```

Provisa computes `group_cols = dedup([grain] + [dim FKs])`
= `["order_id", "customer_id", "product_id", "order_date"]` and generates
[tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Three relationships are registered automatically [tool-verified: modeling_register.py lines 89-98,
cardinality `"many_to_one"` at line 95]:

| Relationship | Cardinality |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### Conformed dimensions

A conformed dimension is registered once and referenced by name from any number of facts. Suppose
`raw.returns` holds `return_id`, `customer_id`, `product_id`, and `amount`. The Returns fact reuses
Customer and Product without re-registering them:

```graphql
mutation {
  registerFact(input: {
    name: "Returns"
    source: "raw.returns"
    domainId: "sales"
    grain: ["return_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
    ]
  }) { success message }
}
```

Both `Sales` and `Returns` point to the same `Customer` and `Product` entities. Provisa's join
paths enforce that queries through either fact traverse the same dimension definition
[tool-verified: fact_registration uses entity name as `target_table` at modeling.py lines 138-140;
fact_table_input wires `target_table_id` from that name at modeling_register.py lines 91-93].

---

### Data Vault

The same primitives map directly onto Data Vault vocabulary:

| DV artifact | Primitive | History |
| --- | --- | --- |
| Hub | `entity` | `none` — entity keys only |
| Satellite | `entity` | `scd2` or `snapshot` — attribute history beside the hub key |
| Link | `fact` with no measures | — |
| Bridge / aggregate link | `fact` with measures | — |

The example builds a minimal vault over `raw.customers` and `raw.orders`.

#### Hubs

A hub holds the entity key and nothing else. `attributes: []` with `history: "none"` produces a
deduplicated current key set; attribute history lives entirely in the satellite.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerHub"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

Generates [tool-verified: entity_registration modeling.py lines 107-108;
`cols = dedup([*key, *attributes])` = `["id"]` when `attributes=[]`]:

```sql
SELECT "id" FROM "raw"."customers"
```

```graphql
mutation {
  registerEntity(input: {
    name: "OrderHub"
    source: "raw.orders"
    domainId: "vault"
    key: ["order_id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

Generates:

```sql
SELECT "order_id" FROM "raw"."orders"
```

#### Satellite

The satellite sits beside the hub key and carries full attribute history. Use `scd2` to append
only changed rows; use `snapshot` to stamp every full refresh.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerSat"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

Generates [tool-verified: entity_registration modeling.py lines 115-119;
`_HISTORY_MODE["scd2"]` = `"delta"` at modeling.py line 40]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` and `CustomerHub` both key on `id`. The hub is the stable join target; the satellite
provides point-in-time attribute access through the bitemporal layer.

#### Link (measureless fact)

A link records which hub keys co-occurred — keys only, no measures. Provisa omits the `GROUP BY`
when `measures` is empty [tool-verified: modeling.py lines 130-131:
`if f.measures: view_sql += " GROUP BY ..."`].

```graphql
mutation {
  registerFact(input: {
    name: "OrderCustomerLink"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: []
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`.
No measures, so no `GROUP BY`. Generates [tool-verified: fact_registration modeling.py lines
125-131]:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Two relationships registered: `OrderCustomerLink.customer_id → CustomerHub` and
`OrderCustomerLink.order_id → OrderHub`, both many-to-one
[tool-verified: modeling_register.py lines 89-98].

#### Bridge / aggregate link

Add measures to the link and Provisa emits the `GROUP BY`, producing a pre-aggregated bridge. At
`order_id` grain with one customer per order, the result is one aggregated row per order:

```graphql
mutation {
  registerFact(input: {
    name: "OrderSummary"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`
(the duplicate `order_id` from the dimension list is dropped by `_dedup`). Generates
[tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

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
