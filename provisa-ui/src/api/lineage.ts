// Copyright (c) 2026 Kenneth Stott
// Canary: 6b3f0d9c-2a41-4e78-9c05-1d7e8a4f2b63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1160/REQ-1161: client for the column-level lineage graph endpoints.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface LineageTransformOp {
  name: string;
  kind: "sql_function" | "operator" | "command" | "identity" | "constant";
  args?: string[]; // REQ-1160: literal arguments, so an edge reads as a formula — substring(1, 3)
}

export interface LineageNode {
  id: string;
  column: string;
  relation: string | null;
  kind: "source" | "derived" | "command";
  materialized: boolean;
}

export interface LineageEdge {
  source: string;
  target: string;
  transform: string;
  ops: LineageTransformOp[];
}

export interface LineageCycle {
  nodes: string[];
  has_materialization_boundary: boolean;
  classification: "feedback" | "error";
}

export interface LineageGraphData {
  nodes: LineageNode[];
  edges: LineageEdge[];
  outputs: string[];
  cycles?: LineageCycle[];
}

/**
 * Analyses started ahead of the page that will consume them, keyed by `dialect\nsql`.
 *
 * /admin/lineage/graph reads the view registry and parses the statement on every call — there is
 * no server-side cache — so a cold analysis is seconds of work that LineagePage only starts once
 * it has mounted. The guided tour knows one step early which statement is coming and calls
 * {@link prefetchLineageGraph}; by the time the page mounts the request is in flight or done, and
 * its own fetchLineageGraph call adopts that promise instead of starting a second analysis.
 *
 * An entry is consumed exactly once and only by an identical (sql, dialect) pair, so a later
 * Analyze click on the same statement re-queries the server and cannot be served something stale.
 */
const prefetched = new Map<string, Promise<LineageGraphData>>();

const lineageKey = (sql: string, dialect: string) => `${dialect}\n${sql}`;

/**
 * Start the analysis for `sql` now so a later {@link fetchLineageGraph} for the same statement
 * resolves without waiting. A rejection is held in the map and surfaces when the real caller
 * adopts the promise — the error is delivered to the page that asked for the graph, never dropped.
 */
export function prefetchLineageGraph(sql: string, dialect = "postgres"): void {
  const key = lineageKey(sql, dialect);
  if (prefetched.has(key)) return;
  const inflight = fetchLineageGraph(sql, dialect);
  prefetched.set(key, inflight);
  // An unadopted rejection would surface as an unhandled promise rejection; attaching a no-op
  // handler marks it handled without discarding it — the stored promise still rejects for whoever
  // adopts it below.
  inflight.catch(() => undefined);
}

// REQ-1160: full column-level DAG for a single SQL statement.
export async function fetchLineageGraph(
  sql: string,
  dialect = "postgres",
): Promise<LineageGraphData> {
  const key = lineageKey(sql, dialect);
  const warm = prefetched.get(key);
  if (warm) {
    prefetched.delete(key);
    return warm;
  }
  const resp = await fetch(`${API_BASE}/admin/lineage/graph`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql, dialect }),
  });
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed("Lineage graph", resp.status)));
  }
  return resp.json();
}

// REQ-1161: federation-wide merged provenance graph, optionally scoped to a focus node.
export async function fetchFederationGraph(opts?: {
  focus?: string;
  direction?: "upstream" | "downstream" | "both";
  depth?: number;
  domains?: string[]; // REQ-1161: restrict to these domain ids (empty/undefined = all)
  // REQ-1625: read the federation from these roles' vantage point — the columns they can query seed
  // the graph and everything those columns derive from is returned, ancestors in full. Empty = the
  // "All roles" selection, which seeds from every registered column.
  roles?: string[];
}): Promise<LineageGraphData> {
  const params = new URLSearchParams();
  if (opts?.focus) params.set("focus", opts.focus);
  if (opts?.direction) params.set("direction", opts.direction);
  if (opts?.depth != null) params.set("depth", String(opts.depth));
  if (opts?.domains?.length) params.set("domains", opts.domains.join(","));
  if (opts?.roles?.length) params.set("roles", opts.roles.join(","));
  const qs = params.toString();
  const resp = await fetch(`${API_BASE}/admin/lineage/federation${qs ? `?${qs}` : ""}`);
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed("Federation graph", resp.status)));
  }
  return resp.json();
}
