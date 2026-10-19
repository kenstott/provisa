<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Data Modeling (Entities & Facts)

Provisa gives you two declarative primitives, `entity` and `fact`, which cover the building blocks every star schema and Data Vault is put together from. Declare the spec, and Provisa lowers it to exactly the materialized-view, bitemporal and relationship definitions you would otherwise write by hand.

## What entities and facts are

An **entity** is a keyed, deduplicated, optionally-historized projection of a source relation. You
name it, point it at a source, declare the entity key and the attributes you want to carry,
and choose a history mode. Provisa writes the view SQL and registers the MV. When history is
enabled, the MV is bitemporal.

A **fact** is a join to entity keys, reduced to a declared grain, with aggregated measures. Provisa writes an aggregate-MV query (`GROUP BY` grain + FK columns) and registers a relationship for each declared dimension link. A fact with no measures is a pure key set, which is the Data Vault link pattern.

Both constructs are IR. The generated definitions move across engines without remodeling: materialized in Oracle or Databricks, or left virtual over an MPP engine.

## History modes

Three modes are available on an entity:

| Mode | Meaning | Bitemporal mode |
| --- | --- | --- |
| `none` | Current-only. No history. | — |
| `scd2` | Track every change. Append only changed rows (delta) keyed on the entity key. | `delta` |
| `snapshot` | Track every refresh. Append the full result set each refresh, stamped with system time. | `snapshot` |

`scd2` needs an entity key to compute the delta. `snapshot` works on any engine but storage grows
by a full copy per refresh. Pick `scd2` for large, slowly-changing sources; pick `snapshot` when
you need full history and the source can't supply a key.

Facts have no history mode of their own; their coverage over time comes from the history of the entities underneath.

## Measures and aggregations

Measures are declared as `column:agg` pairs. Supported aggregations:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

The default aggregation is `sum`.

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

Provisa generates and registers this bitemporal MV:

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

Provisa generates and registers:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Plus one registered relationship: `Sales.customer_id → Customer` (cardinality: many-to-one).

## The Model form (admin UI)

The **+ Model** button appears on the **Tables** page (tooltip: "Model an entity or fact (star
schema / Data Vault)").

A segmented control at the top of the modal switches between **Entity (dimension)** and **Fact**.

### Entity fields

| Field | Required | Notes |
| --- | --- | --- |
| Name | yes | The MV name in the catalog |
| Source relation | yes | Dotted relation, e.g. `raw.customers` |
| Domain | yes | Domain the MV belongs to |
| Entity key | yes | Comma-separated key column(s), e.g. `id` |
| Attributes | no | Comma-separated attribute columns, e.g. `name, region, tier` |
| History | no | `none` / `scd2` / `snapshot`; default is `none` |

### Fact fields

| Field | Required | Notes |
| --- | --- | --- |
| Name | yes | The MV name in the catalog |
| Source relation | yes | Dotted relation, e.g. `raw.orders` |
| Domain | yes | Domain the MV belongs to |
| Grain | yes | Comma-separated grain column(s), e.g. `order_id` |
| Measures | no | `col:agg` pairs, comma-separated, e.g. `amount:sum, quantity:sum` |
| Dimensions | no | `Entity:fk_column` pairs, comma-separated, e.g. `Customer:customer_id` |

When `agg` is omitted in a measure (`amount` instead of `amount:sum`), it defaults to `sum`.

## The GraphQL API

Both mutations live in the admin schema.

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

`EntityInput` fields:

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

`FactInput` fields:

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

`DimRefInput`: `{ entity: String, via: String }` — `entity` is the referenced entity name;
`via` is the FK column on the fact source.

On success, `registerFact` returns a message of the form:
`Fact 'Sales' registered with 1 dimension link(s)`.

## Kimball star schema and Data Vault

Neither pattern requires separate tooling. The same two primitives compose into both.

### Kimball star schema

This walkthrough builds a three-dimension star. Two source tables are new:

- `raw.products` — `product_id`, `name`, `category`, `list_price`
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month`

`raw.orders` also gains `product_id` and `order_date` columns here.

#### Choosing SCD type

History mode is the only dial between SCD Type 1 and Type 2:

| SCD type | History mode | Effect |
| --- | --- | --- |
| Type 1 (current only) | `none` | MV rebuilt on refresh; no row history |
| Type 2 (versioned) | `scd2` | Bitemporal delta MV; each change appends a new row keyed on the entity key |

Use `scd2` when downstream queries need to join a dimension as it was at transaction time, such as a customer's tier at the moment of purchase instead of their current tier. Use `none` for stable lookups. A date spine never changes, and a product catalog where you only need the current price can rebuild on every refresh.

#### Grain decision

The grain is the lowest level of detail the fact answers. `order_id` gives one row per order and keeps the ability to count distinct orders and join to any dimension at the order level. A coarser grain, say `["customer_id", "order_date"]`, pre-aggregates across orders and throws that detail away for good. Declare the narrowest grain the business needs; coarser rollups are cheap to derive later.

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

Generates a bitemporal delta MV keyed on `id`:

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

Generates an ordinary MV rebuilt on refresh:

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

Grain: `order_id`. Three dimension references, with one FK column each. Both measures are additive sums.

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
:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Three relationships are registered automatically:

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
.

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

Generates:

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

Generates:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` and `CustomerHub` both key on `id`. The hub is the stable join target; the satellite
provides point-in-time attribute access through the bitemporal layer.

#### Link (measureless fact)

A link records which hub keys occurred together. It holds keys only and no measures, and Provisa leaves out the `GROUP BY` when `measures` is empty.

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
No measures, so no `GROUP BY`. Generates:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Two relationships registered: `OrderCustomerLink.customer_id → CustomerHub` and
`OrderCustomerLink.order_id → OrderHub`, both many-to-one
.

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
:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

The model does not decide the methodology. Grain, conformance, SCD choice, and the hub/satellite
split remain the modeler's decisions. Provisa executes them.

## Metrics

A **metric** is a named, governed aggregate definition with no grain of its own. The grain, meaning the dimensions the aggregate is broken out by, is bound by the caller at query time instead of at definition time. That's what sets a metric apart from a view: a view fixes the grain when it's created, and a metric stays open until it's queried.

### The Metric object

| Field | Required | Notes |
| --- | --- | --- |
| `name` | yes | snake_case, e.g. `net_revenue`. Validated: `[a-z][a-z0-9_]*` |
| `expression` | yes | Aggregate ANSI-SQL; must include at least one aggregate function |
| `datatype` | no | Result type hint, e.g. `number`, `integer` |
| `description` | no | Human-readable business definition |
| `ai_context` | no | Text for AI consumers — projects to MCP tools, pg_description, GraphQL docs, and Ossie export |
| `visible_to` | no | Role list; defaults to `["*"]` (all roles) |
| `from_fact` | — | Set automatically when the metric was generated from a fact measure |

Column references inside the expression must be qualified with their table (`orders.amount`, not plain `amount`). An unqualified column is a hard error when the metric is expanded, and nothing lets it pass with a warning.

The metric repository validates the expression on every write. An expression that does not parse or
contains no aggregate function is rejected; it is never stored.

Example config entry:

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### Querying a metric

The compiler reserves the `metrics` schema, and every metric can be addressed as a virtual relation inside it. Query it like a table, and the columns you select become the dimension set and the GROUP BY:

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

The compiler rewrites this into a real grouped aggregate over the underlying semantic tables before
governance runs, so RLS and masking apply to the actual columns.

`SELECT *` against a metric relation is refused. Name the dimension columns and `value` explicitly.

When a metric's expression spans multiple tables, the compiler joins them through registered
relationships. A dimension that is a column of a directly-referenced table resolves to that table.
A dimension one relationship hop away is joined in automatically. Two hops or an ambiguous
dimension is a hard error naming the offender.

### Metrics from fact specs

When you register a fact, each declared measure auto-registers a corresponding Metric object.
The metric's `from_fact` field records the source fact table name, and the valid grouping
dimensions are the entity attributes reachable over the fact's FK relationships.

Auto-registered metrics appear on the Metrics page with a **fact** badge. You can edit them like
any other metric.

### Metric-composed views (view_metrics)

A `view_metrics` view closes a metric's grain at definition time. Declare the metric names,
dimension columns, and optional filters; the compiler generates the SELECT.

```yaml
tables:
  - source_id: pg1
    domain_id: sales
    schema: public
    table: monthly_revenue
    view_metrics:
      metrics: [net_revenue]
      dimensions: [region, month]
      filters: ["orders.status = 'completed'"]
```

The compiler generates (for this example):

```sql
SELECT orders.region AS region, orders.month AS month,
       SUM(orders.amount) - SUM(orders.refunds) AS net_revenue
FROM orders
WHERE orders.status = 'completed'
GROUP BY orders.region, orders.month
```

`view_metrics` and `view_sql` are mutually exclusive on the same table.

**Auto-regeneration on metric change.** When a metric's expression is updated, every
`view_metrics` view that references it recompiles and the new SQL is persisted immediately.
The view cannot drift from the metric definition by construction.

**Inline `metric()` calls in free-hand view SQL.** Hand-written `view_sql` can also reference
metrics via `metric('name')`. The compiler replaces each call with the metric's expression and
records a lineage edge. This gives free-hand views the same recompile-on-change property when
they reference a metric rather than copy its formula.

Note: views from the config file that use inline `metric()` calls regenerate when the config reloads, and a metric upsert doesn't regenerate them.

### The Metrics admin page

Open the **Metrics** nav item to manage governed metrics. Click a row to expand a read-only detail
panel; click **Edit** inside it to switch to inline editing (no modal). **New Metric** opens an
inline creation card above the table. The delete confirmation is the only modal on the page.

The create/edit form offers a three-picker builder for fact-sourced metrics: choose the source
fact table (filtered to `modelingRole=fact`), a measure column, and an aggregate function
(`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`). The datatype is derived automatically:
`COUNT → bigint`, `AVG → numeric`, `SUM/MIN/MAX → the measure column's type`.
The expression textarea remains the escape hatch for arbitrary expressions.

## The IR payoff

Every registration call goes through the same path as a hand-written MV. The entity and fact spec is an intermediate representation; it isn't a template or a macro. The warehouse it targets is a property of the deployment, and the model doesn't name it. Change the target engine, and the same `entity` / `fact` declarations materialize there, because the generated SQL and the bitemporal modes are engine-neutral by design.
