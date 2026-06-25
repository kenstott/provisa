// Copyright (c) 2026 Kenneth Stott
// Canary: a6523b19-3f2a-4392-87b3-86416b1ff2db
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
  "#4C8EDA",
  "#6DCE9E",
  "#FFC454",
  "#DA7194",
  "#57C7E3",
  "#F16667",
  "#9575CD",
  "#569480",
  "#BDB76B",
  "#77A6D8",
  "#FF8C69",
  "#82C9B0",
  "#C9A85C",
  "#B07CC6",
  "#5BAED6",
  "#D4876A",
  "#6BAD8C",
  "#E8B84B",
  "#A87FC0",
  "#5CA5C9",
  "#D98B7A",
  "#72B89E",
  "#DFB95A",
  "#9B85BE",
  "#67A8CF",
  "#CF8C74",
  "#63A88C",
  "#D4AA53",
  "#9480B7",
  "#5BA3C5",
  "#CA8E7E",
  "#6BA88D",
  "#D4A94E",
  "#8E7AAD",
  "#609DC1",
  "#C58E80",
  "#68A47F",
  "#CBA349",
  "#8A73A4",
  "#5A99BC",
  "#C28B83",
  "#66A078",
  "#C49E44",
  "#856C9A",
  "#5895B7",
  "#BC8886",
  "#64A073",
  "#BF9840",
  "#806591",
  "#5691B3",
  "#B98589",
  "#629C6F",
  "#BA933C",
  "#7B5E88",
  "#548DAF",
  "#B6828C",
  "#60986B",
  "#B58E38",
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
    Math.min(255, Math.round(v * factor))
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
  id: number;
  label: string;
  tableLabel: string;
  properties: Record<string, unknown>;
}
export interface GEdge {
  identity: string;
  start: number;
  end: number;
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

// ── Query exclusion injection ─────────────────────────────────────────────────
// Injects a WHERE NOT clause before the RETURN clause for the variable matching
// `label` in the query. Uses `n.<pkCol> IN [<pkValue>]` when a PK is available,
// falling back to `id(n) IN [<nodeId>]` otherwise.
// Returns null if the label/variable can't be found.
export function injectExclusion(
  query: string,
  tableLabel: string,
  nodeId: string,
  pkCol: string | null,
  pkValue: unknown,
  relationships?: Relationship[],
): string | null {
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
      if (!found) {
        // Last resort: bare variable in MATCH with no label — use id() predicate.
        // Matches patterns like `(n)` or `(a)` that have no colon/type annotation.
        const bareRe = /\(([a-zA-Z_][a-zA-Z0-9_]*)\s*\)/g;
        const matchSection = workingQuery.slice(0, workingQuery.search(/\bRETURN\b/i));
        let bareVar: string | null = null;
        let bm: RegExpExecArray | null;
        while ((bm = bareRe.exec(matchSection)) !== null) {
          const candidate = bm[1];
          if (!["WHERE", "WITH", "MATCH", "OPTIONAL", "RETURN", "ORDER", "LIMIT", "SKIP", "NOT", "AND", "OR"].includes(candidate.toUpperCase())) {
            bareVar = candidate;
            break;
          }
        }
        if (!bareVar) return null;
        varName = bareVar;
        // Force id() usage — bare variable has no label so pk-based filter could match wrong table
        pkCol = null;
      } else {
        varName = found.varN;
        workingQuery = found.newQuery;
      }
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

// ── Graph stats ───────────────────────────────────────────────────────────────
export interface GraphStats {
  in_degree: number;
  out_degree: number;
  degree: number;
  degree_centrality: number;
  schema_L1?: string;
  schema_L2?: string;
  schema_L3?: string;
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
  queryStats?: unknown;
  pinned?: boolean;
}
