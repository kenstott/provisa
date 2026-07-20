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

// REQ-1160: full column-level DAG for a single SQL statement.
export async function fetchLineageGraph(sql: string, dialect = "postgres"): Promise<LineageGraphData> {
  const resp = await fetch(`${API_BASE}/admin/lineage/graph`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql, dialect }),
  });
  if (!resp.ok) {
    const detail = await resp.text();
    throw new Error(`Lineage graph failed (${resp.status}): ${detail}`);
  }
  return resp.json();
}

// REQ-1161: federation-wide merged provenance graph, optionally scoped to a focus node.
export async function fetchFederationGraph(opts?: {
  focus?: string;
  direction?: "upstream" | "downstream" | "both";
  depth?: number;
  domains?: string[]; // REQ-1161: restrict to these domain ids (empty/undefined = all)
}): Promise<LineageGraphData> {
  const params = new URLSearchParams();
  if (opts?.focus) params.set("focus", opts.focus);
  if (opts?.direction) params.set("direction", opts.direction);
  if (opts?.depth != null) params.set("depth", String(opts.depth));
  if (opts?.domains?.length) params.set("domains", opts.domains.join(","));
  const qs = params.toString();
  const resp = await fetch(`${API_BASE}/admin/lineage/federation${qs ? `?${qs}` : ""}`);
  if (!resp.ok) {
    const detail = await resp.text();
    throw new Error(`Federation graph failed (${resp.status}): ${detail}`);
  }
  return resp.json();
}
