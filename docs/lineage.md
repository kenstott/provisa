# Column-Level Lineage

Provisa tracks column-level data lineage statically. It computes lineage from SQL definitions and command contracts, so nothing has to run. You get two views: a DAG for each statement, and a federation-wide provenance graph that spans every registered view and materialized view (MV).

## The lineage explorer

Open **Lineage** in the UI (`/lineage`). Paste a SQL statement and click **Build statement
graph** to see its column-level DAG. Click **Federation graph** to load the provenance graph over
every MV in the registry.

## Statement-level DAG

Each named output column in your SQL becomes a node. The builder traces it back through every
CTE, subquery, join, and inline command call to its source columns, building a directed graph
from source inputs to final outputs.

### Worked example

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

This statement produces three output columns. The graph for `geo_u` looks like:

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region`, and `orders.geo` are **source** nodes (the narrow input contract
  of `enrich_grpc_set` declares `id` and `region`; the full taint-closure connects all declared
  inputs to all outputs).
- `e.embedding` and `e.geo` are **command** nodes at the `enrich_grpc_set` boundary.
- `geo_u` is a **derived** node produced by the `UPPER` SQL function.

The command boundary is **not opaque**. Because `enrich_grpc_set` declares its input columns
(`id`, `region`) and output columns (`id`, `embedding`, `geo`), the lineage engine splices the
taint closure continuously from the source relation's declared columns to each output.

### Node kinds and visual cues

| Node kind | Color | Meaning |
| --- | --- | --- |
| `source` | Green | A base table column |
| `derived` | Blue | Produced by a SQL expression (function, operator, CTE) |
| `command` | Purple | An output column from a registered command |

Additional rings on a node:

- **Orange ring** — a final output column of the statement.
- **Double border** — the column's relation is a materialized view (MV/CTAS snapshot).
- **Red ring** — member of a cycle classified as an error.
- **Yellow ring** — member of a cycle classified as a feedback loop.

### Named transforms on edges

Every edge carries the raw SQL expression that produces the target column, plus a list of named
operations: SQL functions (`sql_function`), arithmetic/logical operators (`operator`), registered
commands (`command`), bare column references (`identity`), and literals (`constant`).

An edge from a command call is rendered as a dashed purple line in the UI.

## Federation-wide graph

The federation graph merges every registered MV's per-statement lineage into one provenance graph. A node's identity is `relation.column`, so a view's output column and another view's input reference to that column collapse to one node. The result is a single DAG from base source columns to every derived dataset in the platform.

Use `focus`, `direction`, and `depth` to scope the view at federation scale without recomputing
the graph.

## Cycles

The lineage engine detects every directed cycle and **classifies** it. A cycle is described, and it's never rejected.

| Classification | Border color | Meaning |
| --- | --- | --- |
| `feedback` | Yellow | The cycle crosses a materialized node — a legal, time-lagged feedback loop. The MV snapshot is the version boundary that makes it well-defined. |
| `error` | Red | No materialization boundary on the loop — a circular definition with no stable evaluation order. Likely a design error. |

A `feedback` cycle is a valid pattern. An enrichment MV that feeds a derived column back into its own source relation works as long as one node on the loop is materialized, because the snapshot separates the two halves in time. An `error` cycle needs an operator's judgment. It usually means two views reference each other with no snapshot in between.

## API

Both endpoints are **static**. They read definitions and contracts, and never data.

### POST /admin/lineage/graph

Returns the column-level DAG for a single SQL statement.

```http
POST /admin/lineage/graph
Content-Type: application/json

{
  "sql": "SELECT o.id, e.embedding FROM orders o JOIN enrich_grpc_set('main.public.orders') e ON o.id = e.id",
  "dialect": "postgres"
}
```

Response shape:

```json
{
  "nodes": [
    {"id": "orders.id", "column": "id", "relation": "orders", "kind": "source", "materialized": false}
  ],
  "edges": [
    {
      "source": "orders.id",
      "target": "e.id",
      "transform": "enrich_grpc_set(...)",
      "ops": [{"name": "enrich_grpc_set", "kind": "command"}]
    }
  ],
  "outputs": ["id", "embedding"]
}
```

Returns HTTP 422 when the SQL cannot be parsed.

### GET /admin/lineage/federation

Returns the merged provenance graph over all MVs in the registry.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

Query parameters:

| Parameter | Values | Default | Effect |
| --- | --- | --- | --- |
| `focus` | A node id | — | Scope the response to the sub-graph around this node |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Which direction to traverse from `focus` |
| `depth` | integer | unbounded | Maximum hop distance from `focus` |

Response is the same shape as the statement graph, with a `cycles` field added
:

```json
{
  "nodes": [...],
  "edges": [...],
  "outputs": [...],
  "cycles": [
    {
      "nodes": ["orders.region", "enriched_orders.region"],
      "has_materialization_boundary": true,
      "classification": "feedback"
    }
  ]
}
```

## What each refresh was built from

Each refresh of a materialized view records what it was built from: the version of the view's definition, the version of its inputs and how that version was established, and each output column's source columns and expression. The ops view `"ops"."mv_refresh_lineage"` returns one row per refresh:

| Column | Holds |
|---|---|
| `mv_id` | The materialized view refreshed |
| `target_table` | The table the refresh wrote |
| `definition_version` | The version of the view's definition |
| `input_version`, `input_version_kind` | The version of its inputs: an Iceberg snapshot, a watermark, a freshness token or the refresh time |
| `trace_id` | The refresh's trace in `"ops"."traces"` |
| `refreshed_at` | When it ran |
| `column_derivations` | Each output column with its source columns and expression, as JSON |

```sql
SELECT refreshed_at, definition_version, input_version, input_version_kind
FROM "ops"."mv_refresh_lineage"
WHERE mv_id = 'view-view_exposure'
ORDER BY refreshed_at
```

The view reads the refresh records in `"ops"."traces"` and answers on every engine that keeps them. On Trino those are the `otel` catalog's Iceberg tables. On DuckDB the server writes its own trace records as Parquet files under its telemetry folder (`PROVISA_TELEMETRY_DIR`, `~/.provisa/telemetry` by default), one file per export batch in a folder per day. The scheduled telemetry compaction (`otel.compact_cron`) merges each day's small files into files of up to 64 MB. Each organization reads only its own refreshes, and the records are kept indefinitely. Spans are recorded only while the OpenTelemetry SDK is enabled (`OTEL_SDK_DISABLED` unset or false).

## A view as it stood, and as it was recorded

A view declared bitemporal keeps every version it has held: each refresh appends, and no version is rewritten, on any store. A plain read answers the current state. Two request headers read its past:

- `X-Provisa-As-Of` reads the view as of a time, or over a range, on its clock: the source's own change time where the view declares `bitemporal_source_time`, Provisa's load time where it does not.
- `X-Provisa-As-Recorded` reads it as Provisa had recorded it at a time. Versions recorded after that time are left out, so a correction recorded later does not change a replay of the past.

As of 30 September and as recorded on 1 October gives the original figure; as recorded on 5 October gives the correction. The audit record of each statement keeps both times (`as_of`, `as_recorded`), so a report can be reproduced as it was first produced. [Views with history](configuration.md#views-with-history-time-travel) covers declaring a bitemporal view and every surface that takes the two values.

## What a column rename or drop would break

A column carries two names, and each is stored by a different set of artifacts.

The **exposed name** is what the SQL and GraphQL surfaces show: `table_columns.alias`, falling back
to the snake_case default when no alias is set. Views, materialized views, metric expressions, RLS predicates, DQ
contracts, metric-view grains and MV row keys are all authored against that name, so **renaming an
alias breaks them just as surely as deleting the column does**.

The **physical name** is `table_columns.column_name`, the identity that survives the table upsert's
wholesale column replace. Relationships, [glossary](glossary.md) bindings, tag assignments, the watermark column
and column presets store this one, so they only break when the column is **removed**.

`columnDependents` reports both. Downstream views and MVs come from slicing the federation graph at
the column's exposed name; the artifacts that graph does not cover come from a direct scan of the
registry.

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

`breaksOn` is `rename` for an exposed-name reference and `remove` for a physical-name one, so a
caller can tell which half of the edit each artifact is reacting to.

Ask this **before** the save. A renamed column is located by the exposed name it still carries in
the registry; once the alias has landed, the old name is gone and the query finds nothing.

The Tables page runs the query automatically when a pending edit changes an alias or shrinks the column set, and lists what it finds. The warning is advisory. It names the affected artifacts, and the administrator decides. It never blocks the save, because the estate's consumers can't all be reached: an outside dashboard or a client application that queries the column by name is beyond the registry's knowledge. For the same reason, scans over free SQL text match the column as an identifier token without resolving scope, so they can name an artifact that turns out not to use the column. For a warning, over-reporting is the safe direction.

## Using lineage to govern command contracts

Because the taint closure connects every declared input column to every declared output column,
the breadth of that closure depends entirely on what you declare.

Consider a command that takes a full orders table (`id`, `region`, `amount`, `customer_id`, `discount`, `notes`, ...) and returns an `embedding`. If the input contract lists all those columns, every downstream column that uses the embedding shows lineage from all of them. That's accurate, but it doesn't help, because you can't tell what actually mattered.

Declare only `id` and `text` (the columns the embedding model actually reads), and the lineage
cone tightens to those two source columns. The derivation is both sound and precise.

See [Commands](commands.md) for the mechanics of declaring a narrow input contract.
