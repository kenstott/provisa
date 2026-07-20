# Column-Level Lineage

Provisa tracks column-level data lineage statically — computed from SQL definitions and command
contracts, no execution required. Two views are available: a per-statement DAG and a
federation-wide provenance graph spanning all registered views and materialized views (MVs).

## The lineage explorer

Navigate to **Lineage** in the UI (`/lineage`). Paste a SQL statement and click **Build statement
graph** to see its column-level DAG. Click **Federation graph** to load the provenance graph over
every MV in the registry. [tool-verified: LineagePage.tsx:28-119]

## Statement-level DAG (REQ-1160)

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

```
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region`, and `orders.geo` are **source** nodes (the narrow input contract
  of `enrich_grpc_set` declares `id` and `region`; the full taint-closure connects all declared
  inputs to all outputs). [tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` and `e.geo` are **command** nodes — the `enrich_grpc_set` boundary.
- `geo_u` is a **derived** node produced by the `UPPER` SQL function.

The command boundary is **not opaque**. Because `enrich_grpc_set` declares its input columns
(`id`, `region`) and output columns (`id`, `embedding`, `geo`), the lineage engine splices the
taint closure continuously from the source relation's declared columns to each output.
[tool-verified: `_splice_commands` and `_input_relation` in graph.py:245-271]

### Node kinds and visual cues

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| Node kind | Color | Meaning |
|---|---|---|
| `source` | Green | A base table column |
| `derived` | Blue | Produced by a SQL expression (function, operator, CTE) |
| `command` | Purple | An output column from a registered command |

Additional rings on a node:

- **Orange ring** — a final output column of the statement.
- **Double border** — the column's relation is a materialized view (MV/CTAS snapshot).
- **Red ring** — member of a cycle classified as an error.
- **Yellow ring** — member of a cycle classified as a feedback loop.

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### Named transforms on edges

Every edge carries the raw SQL expression that produces the target column, plus a list of named
operations: SQL functions (`sql_function`), arithmetic/logical operators (`operator`), registered
commands (`command`), bare column references (`identity`), and literals (`constant`).
[tool-verified: TransformOp and name_transform in graph.py:36-145]

An edge from a command call is rendered as a dashed purple line in the UI.
[tool-verified: LineageDag.tsx:122-124]

## Federation-wide graph (REQ-1161)

The federation graph merges every registered MV's per-statement lineage into one provenance graph.
Node identity is `relation.column` — a view's output column and another view's input reference
to the same column collapse to one node. The result is a single DAG from base source columns to
every derived dataset in the platform. [tool-verified: `build_federation_graph` in merge.py:205-229
and `qualify_outputs` in graph.py:275-299]

Use `focus`, `direction`, and `depth` to scope the view at federation scale without recomputing
the graph. [tool-verified: `slice_graph` in merge.py:160-189]

## Cycles (REQ-1161)

Cycles are described, not rejected. The lineage engine detects every directed cycle and
**classifies** it. [tool-verified: `Cycle.classification` property in merge.py:43-46]

| Classification | Border color | Meaning |
|---|---|---|
| `feedback` | Yellow | The cycle crosses a materialized node — a legal, time-lagged feedback loop. The MV snapshot is the version boundary that makes it well-defined. |
| `error` | Red | No materialization boundary on the loop — a circular definition with no stable evaluation order. Likely a design error. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

A `feedback` cycle is not a failure. An enrichment MV that feeds back a derived column into its
own source relation is a valid pattern as long as one node on the loop is materialized — the
snapshot isolates the two halves temporally. An `error` cycle needs operator judgment: it usually
means two views reference each other with no snapshot in between.

## API

Both endpoints are **static** — they read definitions and contracts, not data.

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

[tool-verified: `lineage_graph` endpoint at lineage_router.py:45-54, LineageGraphRequest model at
lineage_router.py:29-31]

Response shape [tool-verified: `LineageGraph.to_dict` in graph.py:82-105]:

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
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

Returns the merged provenance graph over all MVs in the registry.

```
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

Query parameters [tool-verified: function signature at lineage_router.py:73-76]:

| Parameter | Values | Default | Effect |
|---|---|---|---|
| `focus` | A node id | — | Scope the response to the sub-graph around this node |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Which direction to traverse from `focus` |
| `depth` | integer | unbounded | Maximum hop distance from `focus` |

Response is the same shape as the statement graph, with a `cycles` field added
[tool-verified: `MergedGraph.to_dict` in merge.py:60-64]:

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

## Using lineage to govern command contracts

Because the taint closure connects every declared input column to every declared output column,
the breadth of that closure depends entirely on what you declare.

Consider a command that takes a full orders table (`id`, `region`, `amount`, `customer_id`,
`discount`, `notes`, ...) and returns an `embedding`. If the input contract lists all those
columns, every downstream column that uses the embedding will show lineage from all of them.
That is accurate but not useful — it is hard to tell what actually mattered.

Declare only `id` and `text` (the columns the embedding model actually reads), and the lineage
cone tightens to those two source columns. The derivation is both sound and precise.

See [Commands](commands.md) for the mechanics of declaring a narrow input contract.
