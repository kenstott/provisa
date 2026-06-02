// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Pure graph-model helpers and wire types extracted from GraphFrame.tsx.
// No React/Apollo dependencies — independently testable. Imported by
// GraphFrame (the component) and GraphPage. See react-graphql SKILL.md
// (Module Size — Subdivide Past 1000 Lines).

import type { Relationship } from "../../types/admin";

// ── Palette ───────────────────────────────────────────────────────────────────
export const PALETTE = [
  "#6366f1",
  "#22c55e",
  "#f59e0b",
  "#ec4899",
  "#14b8a6",
  "#f97316",
  "#8b5cf6",
  "#06b6d4",
  "#d946ef",
  "#10b981",
  "#e11d48",
  "#0ea5e9",
  "#84cc16",
  "#a855f7",
  "#f43f5e",
  "#3b82f6",
  "#eab308",
  "#d97706",
  "#7c3aed",
  "#059669",
  "#dc2626",
  "#2563eb",
  "#ca8a04",
  "#9333ea",
  "#16a34a",
  "#db2777",
  "#0891b2",
  "#65a30d",
  "#7c2d12",
  "#1d4ed8",
  "#be185d",
  "#0369a1",
  "#4d7c0f",
  "#6d28d9",
  "#047857",
  "#c2410c",
  "#1e40af",
  "#a16207",
  "#86198f",
  "#064e3b",
  "#b91c1c",
  "#1e3a8a",
  "#92400e",
  "#581c87",
  "#14532d",
  "#831843",
  "#164e63",
  "#365314",
  "#3b0764",
  "#052e16",
  "#ff6b6b",
  "#ffd93d",
  "#6bcb77",
  "#4d96ff",
  "#ff922b",
  "#cc5de8",
  "#74c0fc",
  "#a9e34b",
  "#ff8787",
  "#63e6be",
];

// ── Stable node ID ────────────────────────────────────────────────────────────
const LS_IDS = "provisa_stable_node_ids";
const LS_CTR = "provisa_stable_node_id_counter";

function _stableKey(
  node: { label: string; properties: Record<string, unknown> },
  pkMap: Record<string, string[]>,
): string {
  const pkCols = pkMap[node.label] ?? [];
  if (pkCols.length > 0) {
    const pkVal = pkCols.map((c) => String(node.properties[c] ?? "")).join(":");
    return `${node.label}::${pkVal}`;
  }
  // Fallback: hash all property values deterministically
  const sorted = Object.keys(node.properties)
    .sort()
    .map((k) => `${k}=${String(node.properties[k])}`)
    .join("|");
  return `${node.label}::${sorted}`;
}

export function getStableNodeId(
  node: { label: string; properties: Record<string, unknown> },
  pkMap: Record<string, string[]>,
): number {
  const key = _stableKey(node, pkMap);
  let map: Record<string, number> = {};
  try {
    map = JSON.parse(localStorage.getItem(LS_IDS) ?? "{}");
  } catch {
    /* */
  }
  if (key in map) return map[key];
  let ctr = 0;
  try {
    ctr = parseInt(localStorage.getItem(LS_CTR) ?? "0", 10) || 0;
  } catch {
    /* */
  }
  ctr += 1;
  map[key] = ctr;
  try {
    localStorage.setItem(LS_IDS, JSON.stringify(map));
    localStorage.setItem(LS_CTR, String(ctr));
  } catch {
    /* */
  }
  return ctr;
}

export function labelColor(label: string): string {
  let h = 0;
  for (let i = 0; i < label.length; i++) h = (h * 31 + label.charCodeAt(i)) & 0xffff;
  return PALETTE[h % PALETTE.length];
}

export function darkenColor(hex: string, factor = 0.5): string {
  const c = hex.replace("#", "");
  const r = parseInt(c.slice(0, 2), 16);
  const g = parseInt(c.slice(2, 4), 16);
  const b = parseInt(c.slice(4, 6), 16);
  const d = (v: number) =>
    Math.round(v * factor)
      .toString(16)
      .padStart(2, "0");
  return `#${d(r)}${d(g)}${d(b)}`;
}

const CLUSTER_COLORS = [
  "#6366f1",
  "#8b5cf6",
  "#ec4899",
  "#f97316",
  "#eab308",
  "#22c55e",
  "#06b6d4",
  "#3b82f6",
  "#ef4444",
  "#14b8a6",
  "#a855f7",
  "#f43f5e",
  "#84cc16",
  "#0ea5e9",
  "#d946ef",
];
export function clusterColor(id: string): string {
  // Hash the string id to a stable index
  let h = 0;
  for (let i = 0; i < id.length; i++) h = (Math.imul(31, h) + id.charCodeAt(i)) | 0;
  return CLUSTER_COLORS[
    ((h % CLUSTER_COLORS.length) + CLUSTER_COLORS.length) % CLUSTER_COLORS.length
  ];
}

// ── Relationship line override type ──────────────────────────────────────────
export interface RelLineOverride {
  width: number;
  style: "solid" | "dashed" | "dotted";
}

// ── Wire types ────────────────────────────────────────────────────────────────
export interface GNode {
  id: string;
  label: string;
  properties: Record<string, unknown>;
}
export interface GEdge {
  identity: string;
  start: string;
  end: string;
  type: string;
  properties: Record<string, unknown>;
  startNode: GNode;
  endNode: GNode;
}

export function isNode(v: unknown): v is GNode {
  if (typeof v !== "object" || v === null) return false;
  const o = v as Record<string, unknown>;
  return "label" in o && "properties" in o && !("startNode" in o);
}
export function isEdge(v: unknown): v is GEdge {
  if (typeof v !== "object" || v === null) return false;
  const o = v as Record<string, unknown>;
  return "type" in o && "startNode" in o && "endNode" in o && ("identity" in o || "id" in o);
}

export function extractElements(rows: unknown[]): {
  nodes: Map<string, GNode>;
  edges: Map<string, GEdge>;
} {
  const nodes = new Map<string, GNode>();
  const edges = new Map<string, GEdge>();
  function walk(v: unknown) {
    if (v === null || v === undefined) return;
    if (isEdge(v)) {
      walk(v.startNode);
      walk(v.endNode);
      edges.set(v.identity, v);
    } else if (isNode(v)) {
      nodes.set(`${v.label}:${v.id}`, v);
    } else if (Array.isArray(v)) {
      v.forEach(walk);
    } else if (typeof v === "object") {
      Object.values(v as Record<string, unknown>).forEach(walk);
    }
  }
  rows.forEach(walk);
  return { nodes, edges };
}

// ── Remaining relationships query builder ────────────────────────────────────
function _formatId(id: unknown): string {
  if (typeof id === "number") return String(id);
  const s = String(id);
  return isNaN(Number(s)) || s === "" ? `"${s.replace(/"/g, '\\"')}"` : s;
}

export function buildRemainingRelsQueries(
  nodes: Map<string, GNode>,
  pkMap: Record<string, string[]>,
  schemaRels?: Array<{ type: string; source: string; target: string }>,
): Array<string> {
  const byLabel = new Map<string, GNode[]>();
  nodes.forEach((n) => {
    const arr = byLabel.get(n.label) ?? [];
    arr.push(n);
    byLabel.set(n.label, arr);
  });
  const labels = [...byLabel.keys()];
  const visibleTableLabels = new Set(
    labels.map((l) => (l.includes(":") ? l.split(":").pop()! : l)),
  );
  const queries: string[] = [];
  for (const srcLabel of labels) {
    const srcNodes = byLabel.get(srcLabel) ?? [];
    const tableLabel = srcLabel.includes(":") ? srcLabel.split(":").pop()! : srcLabel;
    const pkCol = (pkMap[srcLabel] ?? pkMap[tableLabel] ?? [])[0] ?? null;
    if (!pkCol) continue;
    const srcIds = srcNodes.map((n) => _formatId(String(n.id))).join(", ");
    for (const tgtLabel of labels) {
      const tgtNodes = byLabel.get(tgtLabel) ?? [];
      const tgtTableLabel = tgtLabel.includes(":") ? tgtLabel.split(":").pop()! : tgtLabel;
      const tgtPkCol = (pkMap[tgtLabel] ?? pkMap[tgtTableLabel] ?? [])[0] ?? null;
      if (!tgtPkCol) continue;
      const tgtIds = tgtNodes.map((n) => _formatId(String(n.id))).join(", ");
      queries.push(
        `MATCH (a:${tableLabel})-[r]->(b:${tgtTableLabel}) WHERE a.${pkCol} IN [${srcIds}] AND b.${tgtPkCol} IN [${tgtIds}] RETURN a, r, b`,
      );
    }
    // For non-visible targets: generate discovery queries using schema relationships
    if (schemaRels) {
      for (const rel of schemaRels) {
        const rSrcLabel = rel.source.includes(":") ? rel.source.split(":").pop()! : rel.source;
        if (rSrcLabel !== tableLabel) continue;
        const tgtTableLabel = rel.target.includes(":") ? rel.target.split(":").pop()! : rel.target;
        if (visibleTableLabels.has(tgtTableLabel)) continue;
        queries.push(
          `MATCH (a:${tableLabel})-[:${rel.type}]->(b:${tgtTableLabel}) WHERE a.${pkCol} IN [${srcIds}] RETURN a, b`,
        );
      }
    }
  }
  return queries;
}

// ── Query exclusion injection ─────────────────────────────────────────────────
// Injects a WHERE NOT clause before the RETURN clause for the variable matching
// `label` in the query. Uses `n.<pkCol> IN [<pkValue>]` when a PK is available,
// falling back to `id(n) IN [<nodeId>]` otherwise.
// Returns null if the label/variable can't be found.
export function injectExclusion(
  query: string,
  label: string,
  nodeId: string,
  pkCol: string | null,
  pkValue: unknown,
  relationships?: Relationship[],
): string | null {
  const tableLabel = label.includes(":") ? label.split(":").pop()! : label;
  const escapedLabel = tableLabel.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

  // Try named variable first: (varName:...Label...)
  const namedRe = new RegExp(`\\(([a-zA-Z_][a-zA-Z0-9_]*):[^)]*${escapedLabel}[^)]*\\)`, "i");
  let varName: string;
  let workingQuery = query;

  const namedMatch = workingQuery.match(namedRe);
  if (namedMatch) {
    varName = namedMatch[1];
  } else {
    // Anonymous node: (:...Label...) — inject a variable
    const anonRe = new RegExp(`\\(:[^)]*${escapedLabel}[^)]*\\)`, "i");
    const anonMatch = workingQuery.match(anonRe);
    if (anonMatch) {
      varName = "_excl";
      workingQuery = workingQuery.replace(anonMatch[0], anonMatch[0].replace("(:", `(${varName}:`));
    } else {
      // Untyped variable (e.g. `(a)` with no label). Use relationship type mapping to infer.
      if (!relationships?.length) return null;
      // Build relType → {sourceTable, targetTable} map from registered relationships
      const relMap = new Map<string, { src: string; tgt: string }>();
      for (const rel of relationships) {
        const key = rel.computedCypherAlias ?? rel.alias;
        if (key)
          relMap.set(key.toUpperCase(), { src: rel.sourceTableName, tgt: rel.targetTableName });
      }
      // Parse all (src)-[r:TYPE]->(tgt) and (src)<-[r:TYPE]-(tgt) patterns
      const relPatternRe =
        /\((\w*)\)\s*(?:<-\[(?:\w*):(\w+)\]-|-\[(?:\w*):(\w+)\]-?>)\s*\((\w*)\)/gi;
      let found: { varN: string; newQuery: string } | null = null;
      let m: RegExpExecArray | null;
      while ((m = relPatternRe.exec(workingQuery)) !== null) {
        const [, srcVar, revType, fwdType, tgtVar] = m;
        const relType = (revType ?? fwdType).toUpperCase();
        const mapping = relMap.get(relType);
        if (!mapping) continue;
        // Check if tableLabel matches source or target table name
        const srcMatches = mapping.src.toLowerCase() === tableLabel.toLowerCase();
        const tgtMatches = mapping.tgt.toLowerCase() === tableLabel.toLowerCase();
        if (!srcMatches && !tgtMatches) continue;
        const chosenVar = srcMatches ? srcVar : tgtVar;
        // chosenVar may be "" (anonymous `()`) — generate a fresh variable name in that case
        const assignedVar = chosenVar || "_excl";
        // Build the replacement token: `(assignedVar:Label)` replacing `(chosenVar)` in the full match
        const fullText = m[0];
        const oldToken = `(${chosenVar})`; // e.g. "(a)" or "()"
        const newToken = `(${assignedVar}:${tableLabel})`;
        // Replace only the correct occurrence: src = first, tgt = last
        let patchedFull: string;
        if (srcMatches) {
          patchedFull = fullText.replace(oldToken, newToken);
        } else {
          const lastIdx = fullText.lastIndexOf(oldToken);
          if (lastIdx === -1) continue;
          patchedFull =
            fullText.slice(0, lastIdx) + newToken + fullText.slice(lastIdx + oldToken.length);
        }
        if (patchedFull === fullText) continue;
        const patched =
          workingQuery.slice(0, m.index) +
          patchedFull +
          workingQuery.slice(m.index! + fullText.length);
        found = { varN: assignedVar, newQuery: patched };
        break;
      }
      if (!found) return null;
      varName = found.varN;
      workingQuery = found.newQuery;
    }
  }

  const usePk = pkCol !== null && pkValue !== undefined && pkValue !== null;
  const valLit = usePk
    ? isNaN(Number(pkValue))
      ? `'${String(pkValue).replace(/'/g, "\\'")}'`
      : String(pkValue)
    : isNaN(Number(nodeId))
      ? `'${nodeId.replace(/'/g, "\\'")}'`
      : nodeId;
  const clause = usePk ? `${varName}.${pkCol} IN [${valLit}]` : `id(${varName}) IN [${valLit}]`;

  // If we already have an exclusion for this variable+column, extend the IN list
  const existingRePk = usePk
    ? new RegExp(`(WHERE|AND)\\s+NOT\\s+${varName}\\.${pkCol}\\s+IN\\s+\\[([^\\]]*)\\]`, "i")
    : new RegExp(`(WHERE|AND)\\s+NOT\\s+id\\(${varName}\\)\\s+IN\\s+\\[([^\\]]*)\\]`, "i");
  const existing = workingQuery.match(existingRePk);
  if (existing) {
    const prefix = existing[1].toUpperCase();
    const currentList = existing[2];
    const replacement = usePk
      ? `${prefix} NOT ${varName}.${pkCol} IN [${currentList}, ${valLit}]`
      : `${prefix} NOT id(${varName}) IN [${currentList}, ${valLit}]`;
    return workingQuery.replace(existingRePk, replacement);
  }

  // Inject before RETURN. The backend translator guards optional variables with
  // IS NULL so this WHERE does not filter out rows where the variable is NULL.
  const returnIdx = workingQuery.search(/\bRETURN\b/i);
  if (returnIdx === -1) return null;
  const before = workingQuery.slice(0, returnIdx).trimEnd();
  const after = workingQuery.slice(returnIdx);
  const connector = /\bWHERE\b/i.test(before) ? "\nAND NOT " : "\nWHERE NOT ";
  return `${before}${connector}${clause}\n${after}`;
}

// ── Frame data ────────────────────────────────────────────────────────────────
export interface FrameData {
  id: string;
  query: string;
  status: "loading" | "done" | "error";
  nodes: Map<string, GNode>;
  edges: Map<string, GEdge>;
  rows: Record<string, unknown>[];
  columns: string[];
  error?: string;
  elapsed?: number;
}
