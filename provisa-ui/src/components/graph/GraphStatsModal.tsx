// Copyright (c) 2026 Kenneth Stott
// Canary: a3f2c7d1-88b4-4e29-b5a1-9c3e6f0d2741
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import type { GNode, GEdge } from "./graph-model";

interface GraphWideStats {
  // Instant
  nodeCount: number;
  edgeCount: number;
  nodesByLabel: [string, number][];
  edgesByType: [string, number][];
  density: number;
  avgDegree: number;
  maxDegree: number;
  isolatedCount: number;
  topHubs: { label: string; name: string; degree: number }[];
  // Async
  componentCount: number | null;
  largestComponentSize: number | null;
  diameter: number | null;
  avgPathLength: number | null;
}

function buildAdjacency(
  nodes: Map<string, GNode>,
  edges: Map<string, GEdge>,
): { adj: Map<number, Set<number>>; idToKey: Map<number, string> } {
  const adj = new Map<number, Set<number>>();
  const idToKey = new Map<number, string>();
  nodes.forEach((n, k) => {
    adj.set(n.id, new Set());
    idToKey.set(n.id, k);
  });
  edges.forEach((e) => {
    adj.get(e.start)?.add(e.end);
    adj.get(e.end)?.add(e.start);
  });
  return { adj, idToKey };
}

function computeInstant(
  nodes: Map<string, GNode>,
  edges: Map<string, GEdge>,
): Omit<GraphWideStats, "componentCount" | "largestComponentSize" | "diameter" | "avgPathLength"> {
  const nodeCount = nodes.size;
  const edgeCount = edges.size;

  const labelCounts = new Map<string, number>();
  nodes.forEach((n) => labelCounts.set(n.label, (labelCounts.get(n.label) ?? 0) + 1));
  const nodesByLabel = [...labelCounts.entries()].sort((a, b) => b[1] - a[1]);

  const typeCounts = new Map<string, number>();
  edges.forEach((e) => typeCounts.set(e.type, (typeCounts.get(e.type) ?? 0) + 1));
  const edgesByType = [...typeCounts.entries()].sort((a, b) => b[1] - a[1]);

  const n = nodeCount;
  const density = n > 1 ? edgeCount / (n * (n - 1)) : 0;

  const degree = new Map<number, number>();
  nodes.forEach((nd) => degree.set(nd.id, 0));
  edges.forEach((e) => {
    degree.set(e.start, (degree.get(e.start) ?? 0) + 1);
    degree.set(e.end, (degree.get(e.end) ?? 0) + 1);
  });

  let degreeSum = 0;
  let maxDegree = 0;
  degree.forEach((d) => {
    degreeSum += d;
    if (d > maxDegree) maxDegree = d;
  });
  const avgDegree = nodeCount > 0 ? degreeSum / nodeCount : 0;
  const isolatedCount = [...degree.values()].filter((d) => d === 0).length;

  const hubEntries: { label: string; name: string; degree: number }[] = [];
  nodes.forEach((n) => {
    const d = degree.get(n.id) ?? 0;
    const name =
      (n.properties["name"] as string) ??
      (n.properties["id"] as string) ??
      String(n.id);
    hubEntries.push({ label: n.label, name: String(name).slice(0, 30), degree: d });
  });
  const topHubs = hubEntries.sort((a, b) => b.degree - a.degree).slice(0, 7);

  return { nodeCount, edgeCount, nodesByLabel, edgesByType, density, avgDegree, maxDegree, isolatedCount, topHubs };
}

async function computeComponents(
  nodes: Map<string, GNode>,
  edges: Map<string, GEdge>,
): Promise<{ componentCount: number; largestComponentSize: number }> {
  const { adj } = buildAdjacency(nodes, edges);
  const visited = new Set<number>();
  let componentCount = 0;
  let largestComponentSize = 0;
  const ids = [...adj.keys()];

  for (let i = 0; i < ids.length; i++) {
    const start = ids[i];
    if (visited.has(start)) continue;
    componentCount++;
    let size = 0;
    const queue = [start];
    visited.add(start);
    while (queue.length > 0) {
      const cur = queue.shift()!;
      size++;
      for (const nb of adj.get(cur) ?? []) {
        if (!visited.has(nb)) {
          visited.add(nb);
          queue.push(nb);
        }
      }
    }
    if (size > largestComponentSize) largestComponentSize = size;
    // yield to event loop every 500 nodes to stay responsive
    if (i % 500 === 499) await new Promise((r) => setTimeout(r, 0));
  }

  return { componentCount, largestComponentSize };
}

async function computeDiameter(
  nodes: Map<string, GNode>,
  edges: Map<string, GEdge>,
  signal: AbortSignal,
): Promise<{ diameter: number; avgPathLength: number }> {
  const { adj } = buildAdjacency(nodes, edges);
  const ids = [...adj.keys()];
  const n = ids.length;
  let diameter = 0;
  let totalPathLength = 0;
  let reachablePairs = 0;

  for (let i = 0; i < n; i++) {
    if (signal.aborted) break;
    const src = ids[i];
    const dist = new Map<number, number>();
    dist.set(src, 0);
    const queue = [src];
    let qi = 0;
    while (qi < queue.length) {
      const cur = queue[qi++];
      const d = dist.get(cur)!;
      for (const nb of adj.get(cur) ?? []) {
        if (!dist.has(nb)) {
          dist.set(nb, d + 1);
          queue.push(nb);
        }
      }
    }
    dist.forEach((d, id) => {
      if (id === src) return;
      if (d > diameter) diameter = d;
      totalPathLength += d;
      reachablePairs++;
    });
    if (i % 100 === 99) await new Promise((r) => setTimeout(r, 0));
  }

  const avgPathLength = reachablePairs > 0 ? totalPathLength / reachablePairs : 0;
  return { diameter, avgPathLength };
}

interface QueryStatsSource {
  field: string;
  source: string;
  strategy: string;
  elapsed_ms: number;
  rows: number;
  cache_hit?: boolean;
}

interface QueryStats {
  total_elapsed_ms?: number;
  sources?: QueryStatsSource[];
}

interface Props {
  nodes: Map<string, GNode>;
  edges: Map<string, GEdge>;
  queryStats?: unknown;
}

function Spinner() {
  return (
    <span
      style={{
        display: "inline-block",
        width: 12,
        height: 12,
        border: "2px solid var(--border)",
        borderTopColor: "var(--primary)",
        borderRadius: "50%",
        animation: "gs-spin 0.7s linear infinite",
      }}
    />
  );
}

const TOOLTIPS: Record<string, string> = {
  "Nodes": "Total number of nodes (entities) in the current graph.",
  "Edges": "Total number of relationships between nodes.",
  "Density": "Ratio of actual edges to the maximum possible edges. Near 0 = sparse; near 1 = fully connected. Most real-world graphs are sparse (<0.01).",
  "Average": "Mean number of edges per node (in + out). Higher values indicate a more interconnected graph.",
  "Maximum": "Degree of the most connected node — the top hub. A very high max relative to average suggests a hub-and-spoke structure.",
  "Isolated nodes": "Nodes with no edges at all. In a data model these are orphaned entities with no modeled relationships — usually a data quality gap.",
  "Components": "Number of disconnected subgraphs. More than one means some nodes cannot reach others by any path. Ideally this is 1 for a unified data model.",
  "Largest component": "Percentage of nodes in the biggest connected subgraph. Low values mean your data is fragmented — entities that should be related aren't linked yet.",
  "Diameter": "Longest shortest path between any two reachable nodes. Measures how 'wide' the graph is. A high diameter means some entities are many hops apart.",
  "Avg path length": "Average number of hops between all reachable pairs of nodes. Lower values indicate a tighter, more navigable graph ('small world' property).",
  "Top hubs by degree": "Nodes with the most connections. These are the most central entities in the graph — joins or traversals through them reach the widest set of neighbors.",
  "Nodes by label": "Breakdown of node count by entity type.",
  "Edges by type": "Breakdown of relationship count by relationship type.",
};

function StatRow({ label, value }: { label: string; value: React.ReactNode }) {
  const tip = TOOLTIPS[label];
  return (
    <div className="gs-stat-row" title={tip}>
      <span className="gs-stat-label" style={tip ? { cursor: "help", borderBottom: "1px dotted var(--text-muted)" } : undefined}>
        {label}
      </span>
      <span className="gs-stat-value">{value}</span>
    </div>
  );
}

export function GraphStatsPanel({ nodes, edges, queryStats }: Props) {
  const qs = queryStats as QueryStats | undefined;
  const [instant, setInstant] = useState<ReturnType<typeof computeInstant> | null>(null);
  const [components, setComponents] = useState<{ componentCount: number; largestComponentSize: number } | null>(null);
  const [pathStats, setPathStats] = useState<{ diameter: number; avgPathLength: number } | null>(null);
  const [pathRunning, setPathRunning] = useState(true);

  useEffect(() => {
    const ctrl = new AbortController();

    setTimeout(() => {
      if (ctrl.signal.aborted) return;
      setInstant(computeInstant(nodes, edges));
    }, 0);

    computeComponents(nodes, edges).then((r) => {
      if (!ctrl.signal.aborted) setComponents(r);
    });

    computeDiameter(nodes, edges, ctrl.signal).then((r) => {
      if (!ctrl.signal.aborted) {
        setPathStats(r);
        setPathRunning(false);
      }
    });

    return () => ctrl.abort();
  }, [nodes, edges]);

  const pct = (v: number, total: number) =>
    total > 0 ? ` (${Math.round((v / total) * 100)}%)` : "";

  return (
    <div className="gs-panel">
      <style>{`@keyframes gs-spin { to { transform: rotate(360deg); } }`}</style>
      {!instant ? (
        <div className="gs-loading"><Spinner /> Computing…</div>
      ) : (
        <div className="gs-grid">
          <div className="gs-card">
            <div className="gs-card-title">Size</div>
            <StatRow label="Nodes" value={instant.nodeCount.toLocaleString()} />
            <StatRow label="Edges" value={instant.edgeCount.toLocaleString()} />
            <StatRow label="Density" value={instant.density.toExponential(2)} />
          </div>

          <div className="gs-card">
            <div className="gs-card-title">Degree</div>
            <StatRow label="Average" value={instant.avgDegree.toFixed(2)} />
            <StatRow label="Maximum" value={instant.maxDegree} />
            <StatRow label="Isolated nodes" value={`${instant.isolatedCount}${pct(instant.isolatedCount, instant.nodeCount)}`} />
          </div>

          <div className="gs-card">
            <div className="gs-card-title">Connectivity</div>
            <StatRow label="Components" value={components ? components.componentCount : <Spinner />} />
            <StatRow
              label="Largest component"
              value={components ? `${components.largestComponentSize.toLocaleString()}${pct(components.largestComponentSize, instant.nodeCount)}` : <Spinner />}
            />
            <StatRow label="Diameter" value={pathRunning ? <Spinner /> : (pathStats?.diameter ?? "—")} />
            <StatRow label="Avg path length" value={pathRunning ? <Spinner /> : (pathStats ? pathStats.avgPathLength.toFixed(2) : "—")} />
          </div>

          <div className="gs-card">
            <div className="gs-card-title" title={TOOLTIPS["Top hubs by degree"]} style={{ cursor: "help" }}>Top hubs by degree</div>
            <div className="gs-hub-list">
              {instant.topHubs.map((h, i) => (
                <div key={i} className="gs-hub-row">
                  <span className="gs-hub-label">{h.label}</span>
                  <span className="gs-hub-name">{h.name}</span>
                  <span className="gs-hub-degree">{h.degree}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="gs-card">
            <div className="gs-card-title" title={TOOLTIPS["Nodes by label"]} style={{ cursor: "help" }}>Nodes by label</div>
            {instant.nodesByLabel.map(([lbl, cnt]) => (
              <div key={lbl} className="gs-kv">
                <span className="gs-kv-key">{lbl}</span>
                <span className="gs-kv-val">{cnt.toLocaleString()}</span>
              </div>
            ))}
          </div>

          <div className="gs-card">
            <div className="gs-card-title" title={TOOLTIPS["Edges by type"]} style={{ cursor: "help" }}>Edges by type</div>
            {instant.edgesByType.map(([type, cnt]) => (
              <div key={type} className="gs-kv">
                <span className="gs-kv-key">{type}</span>
                <span className="gs-kv-val">{cnt.toLocaleString()}</span>
              </div>
            ))}
          </div>

          {qs && (
            <div className="gs-card">
              <div className="gs-card-title">Query execution</div>
              {qs.total_elapsed_ms !== undefined && (
                <div className="gs-stat-row">
                  <span className="gs-stat-label">Total</span>
                  <span className="gs-stat-value">{qs.total_elapsed_ms.toFixed(1)} ms</span>
                </div>
              )}
              {(qs.sources ?? []).map((s, i) => (
                <div key={i} className="gs-qs-source">
                  <div className="gs-qs-source-name">{s.field}</div>
                  {s.strategy !== "federated" && <div className="gs-kv"><span className="gs-kv-key">source</span><span className="gs-kv-val gs-qs-mono">{s.source}</span></div>}
                  <div className="gs-kv"><span className="gs-kv-key">strategy</span><span className="gs-kv-val">{s.strategy}</span></div>
                  <div className="gs-kv"><span className="gs-kv-key">elapsed</span><span className="gs-kv-val">{s.elapsed_ms.toFixed(1)} ms</span></div>
                  <div className="gs-kv"><span className="gs-kv-key">rows</span><span className="gs-kv-val">{s.rows}</span></div>
                  {s.cache_hit !== undefined && <div className="gs-kv"><span className="gs-kv-key">cache</span><span className="gs-kv-val">{s.cache_hit ? "hit" : "—"}</span></div>}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
