# Graph Analytics — Architecture

## Overview

Graph analytics runs as a server-side Python pipeline. The browser submits a Cypher query and an algorithm name; the backend executes the query, builds an in-memory graph, runs the algorithm via NetworkX or igraph, and returns augmented node/edge data with algorithm output merged into properties.

---

## Endpoint

```
POST /data/graph-analytics
Content-Type: application/json

{
  "query": "MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 500",
  "algorithm": "pagerank",
  "params": { "alpha": 0.85 }
}
```

**Response:**
```json
{
  "nodes": [
    { "id": "42", "label": "Person", "properties": { "name": "Alice" }, "_analytics": { "score": 0.031, "cluster": 2 } }
  ],
  "edges": [...],
  "algorithm": "pagerank",
  "elapsed_ms": 142
}
```

The `_analytics` object is merged into each node/edge. Keys vary by algorithm (see table below).

---

## Algorithms

| Name | `algorithm` key | Output keys | Library |
|------|----------------|-------------|---------|
| PageRank | `pagerank` | `score` | NetworkX |
| Betweenness centrality | `betweenness` | `score` | NetworkX |
| Closeness centrality | `closeness` | `score` | NetworkX |
| Degree centrality | `degree` | `score`, `in_degree`, `out_degree` | NetworkX |
| Eigenvector centrality | `eigenvector` | `score` | NetworkX |
| Louvain community detection | `louvain` | `cluster` | `python-louvain` |
| Leiden community detection | `leiden` | `cluster` | `leidenalg` |
| Girvan-Newman communities | `girvan_newman` | `cluster` | NetworkX |
| K-core decomposition | `kcore` | `core_number` | NetworkX |
| Local clustering coefficient | `clustering` | `score` | NetworkX |

---

## Backend Implementation

### Location
`provisa/api/rest/graph_analytics_router.py`

### Pipeline

```
POST /data/graph-analytics
  │
  ├─ 1. Execute Cypher query → rows via existing cypher_router pipeline
  │
  ├─ 2. Extract nodes + edges from rows (reuse extractElements logic)
  │
  ├─ 3. Build networkx.DiGraph
  │      nodes: id → {label, properties}
  │      edges: (src_id, tgt_id, {type, properties})
  │
  ├─ 4. Run algorithm (dispatch table keyed on algorithm name)
  │
  ├─ 5. Merge _analytics dict into each node/edge
  │
  └─ 6. Return augmented nodes + edges as JSON
```

### Dependencies
Add to `pyproject.toml` optional group `[graph]`:
- `networkx>=3.0`
- `python-louvain>=0.16` (for Louvain)
- `leidenalg>=0.10` (for Leiden, requires `igraph`)

### Algorithm dispatch (pseudocode)

```python
ALGORITHMS = {
    "pagerank":    lambda G, p: nx.pagerank(G, **p),
    "betweenness": lambda G, p: nx.betweenness_centrality(G, **p),
    "closeness":   lambda G, p: nx.closeness_centrality(G, **p),
    "degree":      lambda G, p: {n: {"score": d, "in_degree": G.in_degree(n), "out_degree": G.out_degree(n)} for n, d in nx.degree_centrality(G).items()},
    "eigenvector": lambda G, p: nx.eigenvector_centrality_numpy(G, **p),
    "louvain":     lambda G, p: community.best_partition(G.to_undirected(), **p),
    "kcore":       lambda G, p: nx.core_number(G),
    "clustering":  lambda G, p: nx.clustering(G.to_undirected(), **p),
}
```

---

## UI Integration

### GraphFrame changes
- "Analyze" button (⬡▸) in frame header, visible when graph data is present
- Opens an `AnalyticsPanel` overlay (dropdown of algorithms + param inputs + Run button)
- On submit: POST to `/data/graph-analytics` with current frame's query + chosen algorithm
- On response: merge `_analytics` into frame nodes/edges; re-render graph

### Visual encoding
Node size and color driven by analytics output:

| Output key | Visual mapping |
|------------|---------------|
| `score` (centrality) | Node radius scales linearly with score (min 30px, max 80px) |
| `cluster` (community) | Node color overridden by cluster ID → PALETTE index |
| `core_number` | Node opacity by k-core tier |

Cytoscape style functions read `ele.data("_analytics")` at render time.

### State
`colorOverrides` in `GraphFrame` is extended to support cluster-based coloring:
```ts
analyticsOverrides: Record<string, { color?: string; size?: number }>
// keyed by node id, merged into cytoscape style functions
```

---

## Node Grouping

### Core Principle

Grouping is a **view transform**, not a data transform. The underlying nodes and edges are unchanged; the rendering layer applies a grouping function `node → groupKey` derived from any attribute. This keeps the model clean and composable across arbitrary attributes — data properties, injected metadata (`domain`), or analytics output (`cluster`).

---

### Attribute Discovery

After any query result or analytics pass, scan all node properties to build a per-label map of groupable attributes (categorical fields: strings, low-cardinality integers). `domain` is always present (injected by the semantic layer). `cluster` appears after community detection analytics. All other attributes come from the data itself.

```ts
// Derived from frame nodes after each result
type GroupableAttributes = Record<string, string[]>
// { "Person": ["domain", "industry", "cluster", "country"],
//   "Company": ["domain", "sector", "cluster", "region"] }
```

The UI builds the grouping controls dynamically from this map — no hardcoding of attribute names.

---

### Grouping State

```ts
type GroupingEncoding = "color" | "hull" | "ring";

interface GroupingLayer {
  attribute: string;       // e.g. "domain", "cluster", "country"
  encoding: GroupingEncoding;
  colorMap: Record<string, string>; // groupValue → hex color
}

interface GroupingState {
  layers: GroupingLayer[];
  collapsed: Set<string>;  // groupKeys currently collapsed to supernodes
}
```

Multiple layers are supported simultaneously, each using a different visual channel (color, hull, ring) to avoid overloading any single encoding.

---

### Rendering Phases

#### Phase 1 — Color encoding + convex hull overlays

One active grouping attribute at a time. The attribute drives two simultaneous encodings:

- **Color**: node `background-color` overridden by `colorMap[node.properties[attribute]]`
- **Hull**: translucent filled SVG polygon drawn around each group's node positions, labeled with the group value

Hulls render in an SVG layer overlaid on the Cytoscape canvas. Node positions are read from `cy.nodes().positions()` after layout stabilizes. Hulls recompute on pan/zoom via a Cytoscape `viewport` event listener.

Convex hull algorithm: gift wrapping (Graham scan) on the 2D positions, expanded outward by a fixed padding (e.g. 30px).

#### Phase 2 — Multiple simultaneous groupings

Each layer in `GroupingState.layers` uses a distinct visual channel:

| Encoding | Implementation |
|----------|---------------|
| `color` | Overrides node `background-color` in Cytoscape style function |
| `hull` | SVG convex hull polygon per group value, drawn on overlay layer |
| `ring` | Outer `border-color` + thicker `border-width` on node circle |

Only one layer may use `color`; only one may use `ring`. Multiple `hull` layers are permitted (nested/overlapping hulls with different stroke colors).

#### Phase 3 — Collapse groups

A "collapse" toggle per group in the legend. When collapsed:

1. All nodes in the group are hidden (`display: none` in Cytoscape)
2. A synthetic supernode is added representing the group (count badge, group label)
3. Edges to/from hidden members are rewritten to point to/from the supernode
4. Clicking the supernode expands the group and restores original nodes/edges

Supernode ids use a reserved prefix (`__group__<attribute>__<value>`) to avoid collision with real node ids.

---

### UI Control Placement

Grouping controls belong in a **controls bar** inside `gf-graph-area`, above the Cytoscape canvas and below the frame header. The Inspector (right panel) is for selected-item details only — grouping is a frame-level concern.

```
┌─────────────────────────────────────────────────────────┐
│  Group by [domain ▾]  [+ add layer]                     │  ← controls bar
│  ● domain=Finance  ● domain=Tech  ● domain=Health        │  ← legend
├─────────────────────────────────────────────────────────┤
│                                                         │
│              Cytoscape canvas                           │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Controls bar components:**
- `GroupByDropdown`: populates from discovered groupable attributes for the labels present in the current frame
- `AddLayerButton`: adds a second grouping layer (Phase 2+)
- `GroupLegend`: shows each group value as a colored swatch; each swatch has a collapse toggle (Phase 3)

**Inspector read-only display:** The Inspector panel shows the selected node's group membership as additional read-only fields alongside its other properties (e.g. `domain: Finance`, `cluster: 3`). No controls here.

---

### Component Structure

```
GraphFrame
  ├── gf-header (query, view buttons, analyze button)
  ├── GroupingControlsBar           ← new, Phase 1
  │     ├── GroupByDropdown
  │     └── GroupLegend
  └── gf-graph-area
        ├── GraphCanvas (Cytoscape)
        ├── HullOverlay (SVG)       ← new, Phase 1
        └── Inspector
```

`GroupingState` is held in `GraphFrame` state and passed down to both `GraphCanvas` (for color/ring encoding) and `HullOverlay` (for convex hull drawing). `GroupByDropdown` receives the `groupableAttributes` map derived from the current frame's nodes.

---

### Interaction with Analytics

Analytics output (`_analytics.cluster`, `_analytics.score`) is just more node data. The grouping system treats it identically to any other attribute:

- After a community detection run, `cluster` appears in `groupableAttributes`
- The user can immediately select "Group by cluster" in the controls bar
- Nodes are colored and hulled by cluster ID automatically
- Centrality `score` is not groupable (continuous, not categorical) — it drives size encoding instead via `analyticsOverrides`

---

## Constraints

- Max graph size for analytics: 10,000 nodes / 50,000 edges (configurable). Return 413 if exceeded.
- Algorithms run synchronously in the request thread for now. Move to background task if p99 > 5s.
- Leiden requires `igraph` C extension — document build dependency in `Dockerfile`.
- Girvan-Newman is O(n³); restrict to graphs < 500 nodes or require explicit `force=true` param.
