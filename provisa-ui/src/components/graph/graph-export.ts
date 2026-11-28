// Copyright (c) 2026 Kenneth Stott
// Canary: 80ba4c56-0c4e-4949-a66a-731b71319aab
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { isRdfCell } from "./sparql";
import type { GNode, GEdge } from "./graph-model";

const _INTERNAL_NODE_PROPS = new Set([
  "degIn",
  "degOut",
  "degTotal",
  "scl1",
  "scl2",
  "scl3",
  "l1Cluster",
  "l2Cluster",
  "l3Cluster",
]);

function _toCypherLiteral(v: unknown): string {
  if (v === null || v === undefined) return "null";
  if (typeof v === "boolean") return String(v);
  if (typeof v === "number") return String(v);
  if (typeof v === "string") return JSON.stringify(v);
  return JSON.stringify(JSON.stringify(v));
}

function _exportableProps(props: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(props).filter(([k]) => !_INTERNAL_NODE_PROPS.has(k)));
}

export function buildCypherScript(nodes: GNode[], edges: GEdge[]): string {
  const lines: string[] = ["// Provisa Neo4j export", "// Nodes"];
  for (const n of nodes) {
    const props = _exportableProps(n.properties);
    const setParts = Object.entries(props)
      .map(([k, v]) => `${k}: ${_toCypherLiteral(v)}`)
      .join(", ");
    const setStr = setParts ? ` SET n += {${setParts}}` : "";
    // Domain-union nodes omit tableLabel; reconstruct from compound label "Domain:Table"
    const effectiveTable =
      n.tableLabel || (n.label.includes(":") ? n.label.split(":")[1] : n.label);
    const effectiveDomain = n.label.includes(":") ? n.label.split(":")[0] : "";
    const labelStr =
      effectiveDomain && effectiveDomain !== effectiveTable
        ? `\`${effectiveTable}\`:\`${effectiveDomain}\``
        : `\`${effectiveTable}\``;
    lines.push(`MERGE (n:${labelStr} {_provisa_id: ${n.id}})${setStr};`);
  }
  lines.push("", "// Relationships");
  for (const e of edges) {
    lines.push(
      `MATCH (a:\`${e.startNode.tableLabel}\` {_provisa_id: ${e.start}}), ` +
        `(b:\`${e.endNode.tableLabel}\` {_provisa_id: ${e.end}}) ` +
        `MERGE (a)-[:\`${e.type}\`]->(b);`,
    );
  }
  return lines.join("\n");
}

export interface Neo4jConnection {
  url: string;
  username: string;
  password: string;
  database: string;
}

export async function exportToNeo4j(
  nodes: GNode[],
  edges: GEdge[],
  conn: Neo4jConnection,
): Promise<{ imported: number; errors: string[] }> {
  const res = await fetch("/data/neo4j-export", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      url: conn.url,
      username: conn.username,
      password: conn.password,
      database: conn.database,
      nodes: nodes.map((n) => ({
        id: n.id,
        label: n.label,
        tableLabel: n.tableLabel,
        properties: _exportableProps(n.properties),
      })),
      edges: edges.map((e) => ({
        start: e.start,
        end: e.end,
        type: e.type,
        startNodeLabel: e.startNode.tableLabel,
        endNodeLabel: e.endNode.tableLabel,
      })),
    }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error((body as { error?: string }).error ?? `HTTP ${res.status}`);
  }
  return res.json() as Promise<{ imported: number; errors: string[] }>;
}

export function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export function toCSV(columns: string[], rows: Record<string, unknown>[]): string {
  const esc = (v: unknown) => {
    // An RDF term (a SPARQL answer's) is written whole: the IRI, never its compacted form.
    const s =
      v === null || v === undefined
        ? ""
        : isRdfCell(v)
          ? v.value
          : typeof v === "object"
            ? JSON.stringify(v)
            : String(v);
    return s.includes(",") || s.includes('"') || s.includes("\n")
      ? `"${s.replace(/"/g, '""')}"`
      : s;
  };
  return [
    columns.map(esc).join(","),
    ...rows.map((r) => columns.map((c) => esc(r[c])).join(",")),
  ].join("\n");
}
