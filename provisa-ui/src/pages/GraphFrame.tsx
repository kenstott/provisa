// Copyright (c) 2026 Kenneth Stott
// Canary: a3f7e2d1-8c4b-4a9f-b5e6-2d1c7f8a3e4b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useRef, useEffect, useState, useCallback, useMemo } from "react";
import type { Relationship } from "../types/admin";
import CodeMirror from "@uiw/react-codemirror";
import { cypherLanguage } from "@neo4j-cypher/codemirror";
import { json as jsonLang } from "@codemirror/lang-json";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView, keymap } from "@codemirror/view";
import { Prec } from "@codemirror/state";
import { createPortal } from "react-dom";
import cytoscape from "cytoscape";
import type { Core, NodeSingular } from "cytoscape";
// eslint-disable-next-line @typescript-eslint/no-explicit-any
import fcoseRaw from "cytoscape-fcose";
// eslint-disable-next-line @typescript-eslint/no-explicit-any
import layoutUtilitiesRaw from "cytoscape-layout-utilities";
// eslint-disable-next-line @typescript-eslint/no-explicit-any
import cytoscapeSvgRaw from "cytoscape-svg";
// CJS bundles — .default may or may not be present depending on bundler
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const fcose = (fcoseRaw as any).default ?? fcoseRaw;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const layoutUtilities = (layoutUtilitiesRaw as any).default ?? layoutUtilitiesRaw;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const cytoscapeSvg = (cytoscapeSvgRaw as any).default ?? cytoscapeSvgRaw;
try { cytoscape.use(fcose); } catch { /* already registered */ }
try { cytoscape.use(layoutUtilities); } catch { /* already registered */ }
try { cytoscape.use(cytoscapeSvg); } catch { /* already registered */ }

function _downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function _toCSV(columns: string[], rows: Record<string, unknown>[]): string {
  const esc = (v: unknown) => {
    const s = v === null || v === undefined ? "" : typeof v === "object" ? JSON.stringify(v) : String(v);
    return s.includes(",") || s.includes('"') || s.includes("\n") ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [columns.map(esc).join(","), ...rows.map((r) => columns.map((c) => esc(r[c])).join(","))].join("\n");
}

// ── Palette ───────────────────────────────────────────────────────────────────
export const PALETTE = [
  "#6366f1","#22c55e","#f59e0b","#ec4899","#14b8a6",
  "#f97316","#8b5cf6","#06b6d4","#d946ef","#10b981",
  "#e11d48","#0ea5e9","#84cc16","#a855f7","#f43f5e",
  "#3b82f6","#eab308","#d97706","#7c3aed","#059669",
  "#dc2626","#2563eb","#ca8a04","#9333ea","#16a34a",
  "#db2777","#0891b2","#65a30d","#7c2d12","#1d4ed8",
  "#be185d","#0369a1","#4d7c0f","#6d28d9","#047857",
  "#c2410c","#1e40af","#a16207","#86198f","#064e3b",
  "#b91c1c","#1e3a8a","#92400e","#581c87","#14532d",
  "#831843","#164e63","#365314","#3b0764","#052e16",
  "#ff6b6b","#ffd93d","#6bcb77","#4d96ff","#ff922b",
  "#cc5de8","#74c0fc","#a9e34b","#ff8787","#63e6be",
];

// ── Stable node ID ────────────────────────────────────────────────────────────
const LS_IDS = "provisa_stable_node_ids";
const LS_CTR = "provisa_stable_node_id_counter";

function _stableKey(node: { label: string; properties: Record<string, unknown> }, pkMap: Record<string, string[]>): string {
  const pkCols = pkMap[node.label] ?? [];
  if (pkCols.length > 0) {
    const pkVal = pkCols.map((c) => String(node.properties[c] ?? "")).join(":");
    return `${node.label}::${pkVal}`;
  }
  // Fallback: hash all property values deterministically
  const sorted = Object.keys(node.properties).sort().map((k) => `${k}=${String(node.properties[k])}`).join("|");
  return `${node.label}::${sorted}`;
}

export function getStableNodeId(node: { label: string; properties: Record<string, unknown> }, pkMap: Record<string, string[]>): number {
  const key = _stableKey(node, pkMap);
  let map: Record<string, number> = {};
  try { map = JSON.parse(localStorage.getItem(LS_IDS) ?? "{}"); } catch { /* */ }
  if (key in map) return map[key];
  let ctr = 0;
  try { ctr = parseInt(localStorage.getItem(LS_CTR) ?? "0", 10) || 0; } catch { /* */ }
  ctr += 1;
  map[key] = ctr;
  try { localStorage.setItem(LS_IDS, JSON.stringify(map)); localStorage.setItem(LS_CTR, String(ctr)); } catch { /* */ }
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
  const d = (v: number) => Math.round(v * factor).toString(16).padStart(2, "0");
  return `#${d(r)}${d(g)}${d(b)}`;
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

export function extractElements(rows: unknown[]): { nodes: Map<string, GNode>; edges: Map<string, GEdge> } {
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
): Array<string> {
  const byLabel = new Map<string, GNode[]>();
  nodes.forEach((n) => {
    const arr = byLabel.get(n.label) ?? [];
    arr.push(n);
    byLabel.set(n.label, arr);
  });
  const labels = [...byLabel.keys()];
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
        `MATCH (a:${tableLabel})-[r]->(b:${tgtTableLabel}) WHERE a.${pkCol} IN [${srcIds}] AND b.${tgtPkCol} IN [${tgtIds}] RETURN a, r, b`
      );
    }
  }
  return queries;
}

// ── Query exclusion injection ─────────────────────────────────────────────────
// Injects a WHERE NOT clause before the RETURN clause for the variable matching
// `label` in the query. Uses `n.<pkCol> IN [<pkValue>]` when a PK is available,
// falling back to `id(n) IN [<nodeId>]` otherwise.
// Returns null if the label/variable can't be found.
function injectExclusion(
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
        if (key) relMap.set(key.toUpperCase(), { src: rel.sourceTableName, tgt: rel.targetTableName });
      }
      // Parse all (src)-[r:TYPE]->(tgt) and (src)<-[r:TYPE]-(tgt) patterns
      const relPatternRe = /\((\w*)\)\s*(?:<-\[(?:\w*):(\w+)\]-|\-\[(?:\w*):(\w+)\]-?>)\s*\((\w*)\)/gi;
      let found: { varN: string; newQuery: string } | null = null;
      let m: RegExpExecArray | null;
      while ((m = relPatternRe.exec(workingQuery)) !== null) {
        const [fullMatch, srcVar, revType, fwdType, tgtVar] = m;
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
          patchedFull = fullText.slice(0, lastIdx) + newToken + fullText.slice(lastIdx + oldToken.length);
        }
        if (patchedFull === fullText) continue;
        const patched = workingQuery.slice(0, m.index) + patchedFull + workingQuery.slice(m.index! + fullText.length);
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
    ? (isNaN(Number(pkValue)) ? `"${String(pkValue).replace(/"/g, '\\"')}"` : String(pkValue))
    : (isNaN(Number(nodeId)) ? `"${nodeId.replace(/"/g, '\\"')}"` : nodeId);
  const clause = usePk
    ? `${varName}.${pkCol} IN [${valLit}]`
    : `id(${varName}) IN [${valLit}]`;

  // If we already have an exclusion for this variable+column, extend the IN list
  const existingRePk = usePk
    ? new RegExp(`(WHERE|AND)\\s+NOT\\s+${varName}\\.${pkCol}\\s+IN\\s+\\[([^\\]]*)\\]`, "i")
    : new RegExp(`(WHERE|AND)\\s+NOT\\s+id\\(${varName}\\)\\s+IN\\s+\\[([^\\]]*)\\]`, "i");
  const existing = workingQuery.match(existingRePk);
  if (existing) {
    const prefix = existing[1].toUpperCase(); // WHERE or AND
    const currentList = existing[2];
    const replacement = usePk
      ? `${prefix} NOT ${varName}.${pkCol} IN [${currentList}, ${valLit}]`
      : `${prefix} NOT id(${varName}) IN [${currentList}, ${valLit}]`;
    return workingQuery.replace(existingRePk, replacement);
  }

  // Inject before RETURN
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

// ── Inspector panel ───────────────────────────────────────────────────────────
interface InspectorProps {
  selected: { kind: "node"; data: GNode } | { kind: "edge"; data: GEdge } | null;
  colorOverrides: Record<string, string>;
  onColorChange: (label: string, color: string) => void;
  onClose: () => void;
  width: number;
  onResizeStart: (e: React.MouseEvent) => void;
  relationships?: Relationship[];
  onSaveEdgeAlias?: (relId: number, cqlAlias: string, gqlAlias: string) => Promise<void>;
  pkMap: Record<string, string[]>;
}

function Inspector({ selected, colorOverrides, onColorChange, onClose, width, onResizeStart, relationships, onSaveEdgeAlias, pkMap }: InspectorProps) {
  const [inspView, setInspView] = useState<"details" | "json">("details");
  const [showPalette, setShowPalette] = useState(false);
  const [hovered, setHovered] = useState(false);
  const [edgeCql, setEdgeCql] = useState("");
  const [edgeGql, setEdgeGql] = useState("");
  const [savingAlias, setSavingAlias] = useState(false);

  // Find matching relationship when an edge is selected
  const matchedRel = useMemo(() => {
    if (!selected || selected.kind !== "edge" || !relationships) return null;
    const edgeType = (selected.data as GEdge).type;
    return relationships.find((r) => (r.alias ?? r.computedCypherAlias) === edgeType) ?? null;
  }, [selected, relationships]);

  // Sync alias inputs when selection changes
  const prevRelId = useRef<number | null>(null);
  if (matchedRel && matchedRel.id !== prevRelId.current) {
    prevRelId.current = matchedRel.id;
    setEdgeCql(matchedRel.alias ?? "");
    setEdgeGql(matchedRel.graphqlAlias ?? "");
  }
  if (!matchedRel && prevRelId.current !== null) {
    prevRelId.current = null;
    setEdgeCql("");
    setEdgeGql("");
  }

  if (!selected) return null;

  const viewSel = hovered && (
    <div className="gf-insp-viewsel">
      {(["details", "json"] as const).map((v) => (
        <button key={v} className={`gf-insp-viewbtn ${inspView === v ? "active" : ""}`}
                onClick={() => setInspView(v)} title={v}>
          {v === "details" ? "⊡" : "{}"}
        </button>
      ))}
    </div>
  );

  const isN = selected.kind === "node";
  const label = isN ? selected.data.label : (selected.data as GEdge).type;
  const color = colorOverrides[label] ?? labelColor(label);
  const props = selected.data.properties;

  const nodeLabel = isN ? (selected.data as GNode).label : "";
  const colonIdx = nodeLabel.indexOf(":");
  const domain = colonIdx > 0 ? nodeLabel.slice(0, colonIdx) : null;
  const typeName = colonIdx > 0 ? nodeLabel.slice(colonIdx + 1) : nodeLabel;

  const stableId = isN ? getStableNodeId(selected.data as GNode, pkMap) : null;

  const pkCols = isN ? (pkMap[(selected.data as GNode).label] ?? []) : [];
  const idColName = pkCols[0] ?? null;
  const pkEntry: Record<string, unknown> = isN && idColName && !((idColName) in props)
    ? { [idColName]: (selected.data as GNode).id }
    : {};

  const allFields: Record<string, unknown> = isN
    ? {
        ...(domain ? { domain } : {}),
        label: typeName || nodeLabel,
        ...pkEntry,
        ...props,
      }
    : (() => {
        const e = selected.data as GEdge;
        const srcLabel = e.startNode.label;
        const srcColon = srcLabel.indexOf(":");
        const edgeDomain = srcColon > 0 ? srcLabel.slice(0, srcColon) : (srcLabel || null);
        return {
          ...(edgeDomain ? { domain: edgeDomain } : {}),
          identity: e.identity, start: e.start, end: e.end, type: e.type, ...props,
        };
      })();

  return (
    <div className="gf-inspector" style={{ width }}
         onMouseEnter={() => setHovered(true)}
         onMouseLeave={() => { setHovered(false); setShowPalette(false); }}>
      <div className="gf-inspector-resize-handle" onMouseDown={onResizeStart} />
      <button className="gf-insp-close" onClick={onClose} title="Close">✕</button>
      {viewSel}
      <div style={{ position: "relative", alignSelf: "flex-start" }}>
        <div className="gf-inspector-badge" style={{ background: color, cursor: "pointer" }}
             title="Click to change color" onClick={() => setShowPalette((p) => !p)}>
          {isN ? (typeName || label) : label}
        </div>
        {showPalette && (
          <div className="gf-color-palette">
            {PALETTE.map((c) => (
              <button key={c} className="gf-color-swatch"
                      style={{ background: c, outline: color === c ? "2px solid #fff" : "none" }}
                      onClick={() => { onColorChange(label, c); setShowPalette(false); }} />
            ))}
          </div>
        )}
      </div>
      <div className="gf-inspector-kind">{isN ? "Node" : "Relationship"}</div>
      <div className="gf-inspector-id">&lt;id&gt;: {isN ? stableId : (selected.data as GEdge).identity}</div>
      {!isN && (
        <>
          <div className="gf-inspector-endpoints">
            <span style={{ color: colorOverrides[(selected.data as GEdge).startNode.label] ?? labelColor((selected.data as GEdge).startNode.label) }}>
              {(selected.data as GEdge).startNode.label || (selected.data as GEdge).start}
            </span>
            {" → "}
            <span style={{ color: colorOverrides[(selected.data as GEdge).endNode.label] ?? labelColor((selected.data as GEdge).endNode.label) }}>
              {(selected.data as GEdge).endNode.label || (selected.data as GEdge).end}
            </span>
          </div>
          {matchedRel && onSaveEdgeAlias && (
            <div style={{ padding: "0.5rem 0", display: "flex", flexDirection: "column", gap: "0.4rem", borderTop: "1px solid var(--border)", marginTop: "0.25rem" }}>
              <label style={{ fontSize: "0.75rem", color: "var(--text-muted)", display: "flex", flexDirection: "column", gap: "0.2rem" }}>
                CQL Alias (UPPER_SNAKE)
                <input
                  value={edgeCql}
                  onChange={(e) => setEdgeCql(e.target.value)}
                  placeholder={matchedRel.computedCypherAlias ?? (selected.data as GEdge).type}
                  style={{ fontSize: "0.8rem", padding: "0.2rem 0.4rem" }}
                />
              </label>
              <label style={{ fontSize: "0.75rem", color: "var(--text-muted)", display: "flex", flexDirection: "column", gap: "0.2rem" }}>
                GQL Alias (camelCase)
                <input
                  value={edgeGql}
                  onChange={(e) => setEdgeGql(e.target.value)}
                  placeholder={matchedRel.graphqlAlias ?? ""}
                  style={{ fontSize: "0.8rem", padding: "0.2rem 0.4rem" }}
                />
              </label>
              <button
                style={{ fontSize: "0.75rem", padding: "0.2rem 0.5rem", alignSelf: "flex-end" }}
                disabled={savingAlias}
                onClick={async () => {
                  setSavingAlias(true);
                  await onSaveEdgeAlias(matchedRel.id, edgeCql, edgeGql);
                  setSavingAlias(false);
                }}
              >
                {savingAlias ? "Saving…" : "Save"}
              </button>
            </div>
          )}
        </>
      )}
      {inspView === "details" && (
        <table className="gf-inspector-table">
          <tbody>
            {Object.entries(allFields).map(([k, v]) => (
              <tr key={k}>
                <td className="gf-prop-key">{k}</td>
                <td className="gf-prop-val">{v === null || v === undefined ? "" : typeof v === "object" ? JSON.stringify(v) : String(v)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {inspView === "json" && (
        <CodeMirror
          value={JSON.stringify(selected.data, null, 2)}
          extensions={[jsonLang()]}
          theme={oneDark}
          basicSetup={{ foldGutter: true, lineNumbers: false, highlightActiveLine: false }}
          readOnly
          style={{ fontSize: 12, flex: 1, overflow: "auto" }}
        />
      )}
    </div>
  );
}

// ── Graph canvas ──────────────────────────────────────────────────────────────
interface CanvasProps {
  nodes: Map<string, GNode>;
  edges: Map<string, GEdge>;
  overlayNodes: Map<string, GNode>;
  overlayEdges: Map<string, GEdge>;
  onSelect: (item: { kind: "node"; data: GNode } | { kind: "edge"; data: GEdge } | null) => void;
  colorOverrides: Record<string, string>;
  sizeOverrides: Record<string, number>;
  labelProperty: Record<string, string>;
  relLineOverrides: Record<string, RelLineOverride>;
  onExcludeNode: (nodeKeys: string[]) => void;
  pkMap: Record<string, string[]>;
  relationships: Relationship[];
  showingChildren: Set<string>;
  onToggleChildren: (nodeKey: string) => void;
  showingParents: Set<string>;
  onToggleParents: (nodeKey: string) => void;
  onCyReady?: (cy: Core | null) => void;
}

type LayoutMode = "force" | "hierarchy";

const LAYOUT_OPTIONS: Record<LayoutMode, cytoscape.LayoutOptions> = {
  force: {
    name: "fcose",
    animate: false,
    packComponents: true,
    nodeRepulsion: () => 10000,
    idealEdgeLength: () => 80,
    gravity: 0.25,
    numIter: 2500,
    nodeSeparation: 80,
    tilingPaddingVertical: 20,
    tilingPaddingHorizontal: 20,
  } as cytoscape.LayoutOptions,
  hierarchy: {
    name: "breadthfirst",
    animate: false,
    directed: true,
    padding: 20,
    spacingFactor: 1.4,
  } as cytoscape.LayoutOptions,
};

function GraphCanvas({ nodes, edges, overlayNodes, overlayEdges, onSelect, colorOverrides, sizeOverrides, labelProperty, relLineOverrides, onExcludeNode, pkMap, relationships, showingChildren, onToggleChildren, showingParents, onToggleParents, onCyReady }: CanvasProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<Core | null>(null);
  const colorOverridesRef = useRef(colorOverrides);
  colorOverridesRef.current = colorOverrides;
  const sizeOverridesRef = useRef(sizeOverrides);
  sizeOverridesRef.current = sizeOverrides;
  const labelPropertyRef = useRef(labelProperty);
  labelPropertyRef.current = labelProperty;
  const relLineOverridesRef = useRef(relLineOverrides);
  relLineOverridesRef.current = relLineOverrides;
  const [layoutMode, setLayoutMode] = useState<LayoutMode>("force");
  const layoutModeRef = useRef<LayoutMode>("force");
  // Track nodes that the user has manually dragged; these stay anchored during re-layout
  const anchoredRef = useRef<Set<string>>(new Set());
  // Prevents concurrent layout runs from clobbering each other's unlock step
  const layoutRunningRef = useRef(false);
  // Stable ref to nudgeLayout so event handlers can call it without stale closure
  const nudgeLayoutRef = useRef<() => void>(() => {});
  // Node right-click context menu
  const [nodeCtxMenu, setNodeCtxMenu] = useState<{ x: number; y: number; nodeId: string; selectedNodeIds: string[] } | null>(null);

  const fitView = useCallback(() => cyRef.current?.fit(undefined, 40), []);

  const runLayout = useCallback((mode?: LayoutMode) => {
    const cy = cyRef.current;
    if (!cy) return;
    if (layoutRunningRef.current) return;
    layoutRunningRef.current = true;
    const m = mode ?? layoutModeRef.current;
    const anchored = anchoredRef.current;
    cy.nodes().forEach((n) => { if (anchored.has(n.id())) n.lock(); });
    // For force mode with no edges, use grid — it fills the canvas rectangle optimally.
    // For force mode with edges, use fcose — clusters connected components.
    // For hierarchy mode, always use breadthfirst.
    let opts: cytoscape.LayoutOptions;
    if (cy.nodes().length === 0) {
      opts = { name: "null" } as cytoscape.LayoutOptions;
    } else if (m === "hierarchy") {
      opts = LAYOUT_OPTIONS.hierarchy;
    } else if (cy.edges().length === 0) {
      // No relationships — grid fills the container rectangle by auto-sizing rows/cols
      opts = { name: "grid", animate: false, fit: true, padding: 30, avoidOverlap: true, avoidOverlapPadding: 12 } as cytoscape.LayoutOptions;
    } else {
      opts = LAYOUT_OPTIONS.force;
    }
    const layout = cy.layout(opts);
    layout.one("layoutstop", () => {
      cy.batch(() => {
        cy.nodes().forEach((node) => {
          const lbl = node.data("label") as string;
          const n = node.data("_node") as GNode | undefined;
          const base = colorOverridesRef.current[lbl] ?? labelColor(lbl);
          node.style("background-color", base);

          const sz = sizeOverridesRef.current[lbl] ?? 44;
          node.style({ width: sz, height: sz, "text-max-width": `${sz - 8}px` });
          if (n) {
            const prop = labelPropertyRef.current[n.label];
            node.style("label", prop
              ? String(n.properties[prop] ?? n.id)
              : String(n.properties["name"] ?? n.properties["title"] ?? n.id));
          }
        });
      });
      cy.nodes().forEach((n) => { if (anchored.has(n.id())) n.unlock(); });
      layoutRunningRef.current = false;
      cy.fit(undefined, 40);
    });
    layout.run();
  }, []);

  const nudgeLayout = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    if (layoutRunningRef.current) return;
    if (cy.edges().length === 0 || layoutModeRef.current === "hierarchy") {
      runLayout();
      return;
    }
    layoutRunningRef.current = true;
    const anchored = anchoredRef.current;
    cy.nodes().forEach((n) => { if (anchored.has(n.id())) n.lock(); });
    const opts = {
      ...LAYOUT_OPTIONS.force,
      randomize: false,
      animate: true,
      animationDuration: 1000,
      animationEasing: "ease-out" as const,
      numIter: 500,
      fit: false,
    } as cytoscape.LayoutOptions;
    const layout = cy.layout(opts);
    layout.one("layoutstop", () => {
      cy.batch(() => {
        cy.nodes().forEach((node) => {
          const lbl = node.data("label") as string;
          const n = node.data("_node") as GNode | undefined;
          const base = colorOverridesRef.current[lbl] ?? labelColor(lbl);
          node.style("background-color", base);

          const sz = sizeOverridesRef.current[lbl] ?? 44;
          node.style({ width: sz, height: sz, "text-max-width": `${sz - 8}px` });
          if (n) {
            const prop = labelPropertyRef.current[n.label];
            node.style("label", prop
              ? String(n.properties[prop] ?? n.id)
              : String(n.properties["name"] ?? n.properties["title"] ?? n.id));
          }
        });
      });
      cy.nodes().forEach((n) => { if (anchored.has(n.id())) n.unlock(); });
      layoutRunningRef.current = false;
      // No fit — preserve current viewport after nudge
    });
    layout.run();
  }, [runLayout]);

  // Keep ref in sync so the cytoscape "free" event always calls the latest nudgeLayout
  useEffect(() => { nudgeLayoutRef.current = nudgeLayout; }, [nudgeLayout]);

  const toggleLayout = useCallback(() => {
    setLayoutMode((prev) => {
      const next: LayoutMode = prev === "force" ? "hierarchy" : "force";
      layoutModeRef.current = next;
      runLayout(next);
      return next;
    });
  }, [runLayout]);

  // Full rebuild — fires only when the base graph (query result) changes
  useEffect(() => {
    if (!containerRef.current) return;
    const els: cytoscape.ElementDefinition[] = [];
    nodes.forEach((n) => {
      els.push({ group: "nodes", data: { id: `${n.label}:${n.id}`, label: n.label, _node: n } });
    });
    edges.forEach((e) => {
      const srcKey = `${e.startNode.label}:${e.startNode.id}`;
      const tgtKey = `${e.endNode.label}:${e.endNode.id}`;
      if (nodes.has(srcKey) && nodes.has(tgtKey)) {
        els.push({
          group: "edges",
          data: { id: e.identity, source: srcKey, target: tgtKey, label: e.type, _edge: e },
        });
      }
    });

    const cy = cytoscape({
      container: containerRef.current,
      elements: els,
      style: [
        {
          selector: "node",
          style: {
            "background-color": (ele: NodeSingular) => {
              const lbl = ele.data("label") as string;
              return colorOverridesRef.current[lbl] ?? labelColor(lbl);
            },
            "label": (ele: NodeSingular) => {
              const n = ele.data("_node") as GNode | undefined;
              if (!n) return String(ele.data("label") ?? "");
              const prop = labelPropertyRef.current[n.label];
              if (prop) return String(n.properties[prop] ?? n.id);
              return String(n.properties["name"] ?? n.properties["title"] ?? n.id);
            },
            "color": "#fff",
            "font-size": 10,
            "text-valign": "center",
            "text-halign": "center",
            "width": (ele: NodeSingular) => {
              const lbl = ele.data("label") as string;
              return sizeOverridesRef.current[lbl] ?? 44;
            },
            "height": (ele: NodeSingular) => {
              const lbl = ele.data("label") as string;
              return sizeOverridesRef.current[lbl] ?? 44;
            },
            "text-wrap": "ellipsis",
            "text-max-width": (ele: NodeSingular) => {
              const lbl = ele.data("label") as string;
              const sz = sizeOverridesRef.current[lbl] ?? 44;
              return `${sz - 8}px`;
            },
            "border-width": 2,
            "border-color": (ele: NodeSingular) => {
              const lbl = ele.data("label") as string;
              const base = colorOverridesRef.current[lbl] ?? labelColor(lbl);
              return darkenColor(base, 0.75);
            },
          },
        },
        {
          selector: "node:selected",
          style: {
            "border-width": 4,
            "border-color": "#fff",
          },
        },
        {
          selector: "edge",
          style: {
            "line-color": "#3a3d4e",
            "target-arrow-color": "#3a3d4e",
            "target-arrow-shape": "triangle",
            "curve-style": "bezier",
            "label": "data(label)",
            "font-size": 8,
            "color": "#6b6f82",
            "text-background-opacity": 0,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            "width": ((ele: any) => relLineOverridesRef.current[ele.data("label") as string]?.width ?? 1.5) as any,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            "line-style": ((ele: any) => relLineOverridesRef.current[ele.data("label") as string]?.style ?? "solid") as any,
          },
        },
        {
          selector: "edge:selected",
          style: {
            "line-color": "#6366f1",
            "target-arrow-color": "#6366f1",
            "width": 2.5,
          },
        },
      ],
      layout: { name: "null" } as cytoscape.LayoutOptions,
      minZoom: 0.05,
      maxZoom: 8,
    });

    cy.on("tap", "node", (evt) => {
      setNodeCtxMenu(null);
      onSelect({ kind: "node", data: evt.target.data("_node") as GNode });
    });
    cy.on("tap", "edge", (evt) => {
      setNodeCtxMenu(null);
      onSelect({ kind: "edge", data: evt.target.data("_edge") as GEdge });
    });
    cy.on("tap", (evt) => {
      if (evt.target === cy) { onSelect(null); setNodeCtxMenu(null); }
    });
    cy.on("cxttap", "node", (evt) => {
      const pos = evt.renderedPosition ?? evt.position;
      const clickedId = evt.target.id() as string;
      const selectedIds = cy.$("node:selected").map((n) => n.id() as string);
      const selectedNodeIds = selectedIds.includes(clickedId) && selectedIds.length > 1 ? selectedIds : [clickedId];
      setNodeCtxMenu({ x: pos.x, y: pos.y, nodeId: clickedId, selectedNodeIds });
    });
    cy.on("cxttap", (evt) => {
      if (evt.target === cy) setNodeCtxMenu(null);
    });
    // Track manually dragged nodes as anchored, then auto-nudge
    // "free" fires on every click too — only nudge if position actually changed
    const grabPositions = new Map<string, { x: number; y: number }>();
    cy.on("grab", "node", (evt) => {
      const pos = evt.target.position();
      grabPositions.set(evt.target.id() as string, { x: pos.x, y: pos.y });
    });
    cy.on("free", "node", (evt) => {
      const id = evt.target.id() as string;
      const before = grabPositions.get(id);
      const after = evt.target.position();
      grabPositions.delete(id);
      if (!before || (Math.abs(after.x - before.x) < 1 && Math.abs(after.y - before.y) < 1)) return;
      anchoredRef.current.add(id);
      nudgeLayoutRef.current();
    });

    cyRef.current = cy;
    onCyReady?.(cy);
    anchoredRef.current = new Set();
    layoutRunningRef.current = false;
    if (els.length > 0) runLayout(layoutModeRef.current);
    return () => {
      cyRef.current = null;
      onCyReady?.(null);
      cy.destroy();
    };
  }, [nodes, edges]); // eslint-disable-line react-hooks/exhaustive-deps

  // Incremental overlay update — adds/removes overlay nodes+edges without full re-layout
  const prevOverlayNodesRef = useRef<Map<string, GNode>>(new Map());
  const prevOverlayEdgesRef = useRef<Map<string, GEdge>>(new Map());
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    const prevNodes = prevOverlayNodesRef.current;
    const prevEdges = prevOverlayEdgesRef.current;
    // Batch 1: removals + node additions — edges need nodes committed first
    cy.batch(() => {
      // Remove nodes no longer in overlay
      prevNodes.forEach((_, k) => {
        if (!overlayNodes.has(k)) cy.$id(`${k.split(":")[0]}:${k.split(":").slice(1).join(":")}`).remove();
      });
      // Remove edges no longer in overlay
      prevEdges.forEach((_, k) => {
        if (!overlayEdges.has(k)) cy.$id(k).remove();
      });
      // Add new overlay nodes
      overlayNodes.forEach((n, k) => {
        if (!prevNodes.has(k) && cy.$id(`${n.label}:${n.id}`).length === 0) {
          cy.add({ group: "nodes", data: { id: `${n.label}:${n.id}`, label: n.label, _node: n } });
        }
      });
    });
    // Batch 2: edge additions — nodes are fully committed before edges reference them
    cy.batch(() => {
      overlayEdges.forEach((e, k) => {
        if (!prevEdges.has(k) && cy.$id(e.identity).length === 0) {
          const srcKey = `${e.startNode.label}:${e.startNode.id}`;
          const tgtKey = `${e.endNode.label}:${e.endNode.id}`;
          if (cy.$id(srcKey).length > 0 && cy.$id(tgtKey).length > 0) {
            cy.add({ group: "edges", data: { id: e.identity, source: srcKey, target: tgtKey, label: e.type, _edge: e } });
          }
        }
      });
    });

    const hadNewNodes = [...overlayNodes.keys()].some((k) => !prevNodes.has(k));
    prevOverlayNodesRef.current = new Map(overlayNodes);
    prevOverlayEdgesRef.current = new Map(overlayEdges);
    if (hadNewNodes) nudgeLayout();
  }, [overlayNodes, overlayEdges]); // eslint-disable-line react-hooks/exhaustive-deps

  // Update node colors when colorOverrides changes without rebuilding the graph
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.batch(() => {
      cy.nodes().forEach((node) => {
        const lbl = node.data("label") as string;
        const base = colorOverridesRef.current[lbl] ?? labelColor(lbl);
        node.style("background-color", base);
        node.style("border-color", darkenColor(base, 0.75));
      });
    });
  }, [colorOverrides]);

  // Update node sizes without rebuilding
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.batch(() => {
      cy.nodes().forEach((node) => {
        const lbl = node.data("label") as string;
        const sz = sizeOverridesRef.current[lbl] ?? 44;
        node.style({ width: sz, height: sz, "text-max-width": `${sz - 8}px` });
      });
    });
  }, [sizeOverrides]);

  // Update node display labels without rebuilding
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.batch(() => {
      cy.nodes().forEach((node) => {
        const n = node.data("_node") as GNode | undefined;
        if (!n) return;
        const prop = labelPropertyRef.current[n.label];
        const txt = prop
          ? String(n.properties[prop] ?? n.id)
          : String(n.properties["name"] ?? n.properties["title"] ?? n.id);
        node.style("label", txt);
      });
    });
  }, [labelProperty]);

  // Update edge width/style when relLineOverrides changes
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.batch(() => {
      cy.edges().forEach((edge) => {
        const type = edge.data("label") as string;
        const ov = relLineOverridesRef.current[type];
        edge.style({ width: ov?.width ?? 1.5, "line-style": ov?.style ?? "solid" });
      });
    });
  }, [relLineOverrides]);

  return (
    <div className="gf-canvas-wrap">
      <div ref={containerRef} className="gf-canvas" />
      <div className="gf-canvas-controls">
        <button className="gf-ctrl-btn" onClick={() => cyRef.current?.zoom(cyRef.current.zoom() * 1.3)} title="Zoom in">+</button>
        <button className="gf-ctrl-btn" onClick={() => cyRef.current?.zoom(cyRef.current.zoom() * 0.77)} title="Zoom out">−</button>
        <button className="gf-ctrl-btn" onClick={fitView} title="Fit to screen">⤢</button>
        <div className="gf-ctrl-divider" />
        <button
          className={`gf-ctrl-btn${layoutMode === "hierarchy" ? " active" : ""}`}
          onClick={toggleLayout}
          title={layoutMode === "force" ? "Switch to hierarchical layout" : "Switch to force layout"}
        >
          {layoutMode === "force" ? "⋮" : "⊟"}
        </button>
        <button
          className="gf-ctrl-btn"
          onClick={nudgeLayout}
          title="Nudge layout (refine from current positions)"
        >
          ⟳
        </button>
      </div>
      {nodeCtxMenu && (() => {
        const ctxNode = nodes.get(nodeCtxMenu.nodeId) ?? overlayNodes.get(nodeCtxMenu.nodeId);
        const ctxLabel = ctxNode?.label ?? "";
        const tableLabel = ctxLabel.includes(":") ? ctxLabel.split(":").pop()! : ctxLabel;
        const ctxPkCols = pkMap[ctxLabel] ?? [];
        const hasPk = ctxPkCols.length > 0;
        const tl = tableLabel.toLowerCase();
        const cl = ctxLabel.toLowerCase();
        const hasChildRels = relationships.some(
          (r) => r.sourceTableName.toLowerCase() === tl || r.sourceTableName.toLowerCase() === cl,
        );
        const hasParentRels = relationships.some(
          (r) => r.targetTableName.toLowerCase() === tl || r.targetTableName.toLowerCase() === cl,
        );
        return (
        <div
          className="gf-node-ctx-menu"
          style={{ left: nodeCtxMenu.x, top: nodeCtxMenu.y }}
          onMouseDown={(e) => e.stopPropagation()}
        >
          <button
            className="gf-node-ctx-item"
            disabled={!hasPk}
            title={hasPk ? "Exclude this node from the query" : "No primary key configured — cannot exclude"}
            style={!hasPk ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
            onClick={() => {
              if (!hasPk) return;
              const cy = cyRef.current;
              nodeCtxMenu.selectedNodeIds.forEach((id) => {
                anchoredRef.current.delete(id);
                if (cy) cy.remove(cy.$id(id));
              });
              onExcludeNode(nodeCtxMenu.selectedNodeIds);
              onSelect(null);
              setNodeCtxMenu(null);
            }}
          >
            Exclude {nodeCtxMenu.selectedNodeIds.length > 1 ? `${nodeCtxMenu.selectedNodeIds.length} nodes` : "from query"}
          </button>
          <button
            className="gf-node-ctx-item"
            disabled={!hasChildRels}
            style={!hasChildRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
            title={hasChildRels ? undefined : "No outgoing relationships for this node type"}
            onClick={() => {
              if (!hasChildRels) return;
              nodeCtxMenu.selectedNodeIds.forEach((id) => onToggleChildren(id));
              setNodeCtxMenu(null);
            }}
          >
            {showingChildren.has(nodeCtxMenu.nodeId) ? "Hide children" : "Show children"}
          </button>
          <button
            className="gf-node-ctx-item"
            disabled={!hasParentRels}
            style={!hasParentRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
            title={hasParentRels ? undefined : "No incoming relationships for this node type"}
            onClick={() => {
              if (!hasParentRels) return;
              nodeCtxMenu.selectedNodeIds.forEach((id) => onToggleParents(id));
              setNodeCtxMenu(null);
            }}
          >
            {showingParents.has(nodeCtxMenu.nodeId) ? "Hide parents" : "Show parents"}
          </button>
          <button
            className="gf-node-ctx-item"
            onClick={() => {
              const cy = cyRef.current;
              if (cy) {
                const selectedIds = new Set(nodeCtxMenu.selectedNodeIds);
                cy.nodes().forEach((n) => {
                  if (selectedIds.has(n.id())) n.unselect();
                  else n.select();
                });
              }
              onSelect(null);
              setNodeCtxMenu(null);
            }}
          >
            Invert selection
          </button>
          <button
            className="gf-node-ctx-item"
            onClick={() => {
              const cy = cyRef.current;
              if (cy) cy.nodes().select();
              onSelect(null);
              setNodeCtxMenu(null);
            }}
          >
            Select all
          </button>
          <div className="gf-node-ctx-divider" />
          <button
            className="gf-node-ctx-item gf-node-ctx-item--danger"
            onClick={() => {
              const cy = cyRef.current;
              if (cy) {
                nodeCtxMenu.selectedNodeIds.forEach((id) => {
                  anchoredRef.current.delete(id);
                  cy.remove(cy.$id(id));
                });
              }
              onSelect(null);
              setNodeCtxMenu(null);
            }}
          >
            Remove {nodeCtxMenu.selectedNodeIds.length > 1 ? `${nodeCtxMenu.selectedNodeIds.length} nodes` : "node"}
          </button>
        </div>
        );
      })()}
    </div>
  );
}

// ── Table view ────────────────────────────────────────────────────────────────
function cellText(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function TableView({ columns, rows, wrap, height, colWidths, setColWidths }: { columns: string[]; rows: Record<string, unknown>[]; wrap?: boolean; height?: number; colWidths: number[]; setColWidths: (w: number[]) => void }) {
  const dragRef = useRef<{ colIdx: number; startX: number; startW: number } | null>(null);

  const colWidthsRef = useRef(colWidths);
  colWidthsRef.current = colWidths;

  const onResizeStart = useCallback((e: React.MouseEvent, idx: number) => {
    e.preventDefault();
    dragRef.current = { colIdx: idx, startX: e.clientX, startW: colWidthsRef.current[idx] };
    const onMove = (me: MouseEvent) => {
      if (!dragRef.current) return;
      const delta = me.clientX - dragRef.current.startX;
      const newW = Math.max(40, dragRef.current.startW + delta);
      setColWidths(colWidthsRef.current.map((w, i) => i === dragRef.current!.colIdx ? newW : w));
    };
    const onUp = () => {
      dragRef.current = null;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, [setColWidths]);

  if (rows.length === 0) return <div className="gf-table-empty">No rows</div>;
  return (
    <div className="gf-table-outer" style={height !== undefined ? { height } : undefined}>
      <CsvCopyButton columns={columns} rows={rows} />
      <div className="gf-table-wrap">
        <table className="gf-table" style={{ tableLayout: "fixed", width: colWidths.reduce((a, b) => a + b, 0) + 40 }}>
          <colgroup>
            <col style={{ width: 40 }} />
            {colWidths.map((w, i) => <col key={i} style={{ width: w }} />)}
          </colgroup>
          <thead>
            <tr>
              <th className="gf-th-rownum" />
              {columns.map((c, i) => (
                <th key={c} style={{ position: "relative", width: colWidths[i] }}>
                  <span className="gf-th-label">{c}</span>
                  <span
                    className="gf-col-resize"
                    onMouseDown={(e) => onResizeStart(e, i)}
                  />
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i}>
                <td className="gf-td-rownum">{i + 1}</td>
                {columns.map((c) => (
                  <td key={c} className={wrap ? "gf-td-wrap" : ""}>{cellText(r[c])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function CsvCopyButton({ columns, rows }: { columns: string[]; rows: Record<string, unknown>[] }) {
  const [copied, setCopied] = useState(false);
  const copy = useCallback(() => {
    navigator.clipboard.writeText(_toCSV(columns, rows)).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [columns, rows]);
  return (
    <button className={`gf-tbl-copy-btn${copied ? " copied" : ""}`} onClick={copy} title="Copy as CSV">
      {copied ? "✓" : "⎘"}
    </button>
  );
}

function JsonCopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const copy = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [text]);
  return (
    <button className={`gf-json-copy-btn${copied ? " copied" : ""}`} onClick={copy} title="Copy JSON">
      {copied ? "✓" : "⎘"}
    </button>
  );
}

// ── Frame component ───────────────────────────────────────────────────────────
interface GraphFrameProps {
  frame: FrameData;
  onClose: (id: string) => void;
  onRerun: (id: string, query: string) => void;
  colorOverrides: Record<string, string>;
  sizeOverrides: Record<string, number>;
  labelProperty: Record<string, string>;
  relLineOverrides: Record<string, RelLineOverride>;
  onColorChange: (label: string, color: string) => void;
  pkMap: Record<string, string[]>;
  relationships?: Relationship[];
  onSaveEdgeAlias?: (relId: number, cqlAlias: string, gqlAlias: string) => Promise<void>;
}

export function GraphFrame({ frame, onClose, onRerun, colorOverrides, sizeOverrides, labelProperty, relLineOverrides, onColorChange, pkMap, relationships, onSaveEdgeAlias }: GraphFrameProps) {
  const [view, setView] = useState<"graph" | "table" | "json">("graph");
  const [selected, setSelected] = useState<{ kind: "node"; data: GNode } | { kind: "edge"; data: GEdge } | null>(null);
  const [collapsed, setCollapsed] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [inspectorWidth, setInspectorWidth] = useState(260);
  const [graphAreaHeight, setGraphAreaHeight] = useState(460);
  const [editQuery, setEditQuery] = useState(frame.query);
  const editQueryRef = useRef(editQuery);
  editQueryRef.current = editQuery;
  const [overlayData, setOverlayData] = useState<Map<string, { nodes: Map<string, GNode>; edges: Map<string, GEdge> }>>(new Map);
  const handleRerun = useCallback((id: string, query: string) => {
    setOverlayData(new Map());
    onRerun(id, query);
  }, [onRerun]);
  const [showDlMenu, setShowDlMenu] = useState(false);
  const [tableWrap, setTableWrap] = useState(false);
  const [tableColWidths, setTableColWidths] = useState<number[]>(() => frame.columns.map(() => 180));
  const prevColumnsKey = useRef(frame.columns.join(","));
  if (frame.columns.join(",") !== prevColumnsKey.current) {
    prevColumnsKey.current = frame.columns.join(",");
    setTableColWidths(frame.columns.map(() => 180));
  }
  const canvasCyRef = useRef<Core | null>(null);
  const inspWidthRef = useRef(inspectorWidth);
  inspWidthRef.current = inspectorWidth;
  const graphAreaHeightRef = useRef(graphAreaHeight);
  graphAreaHeightRef.current = graphAreaHeight;

  const _resolveNodeForKey = useCallback((nodeKey: string): GNode | undefined => {
    let gNode: GNode | undefined = frame.nodes.get(nodeKey);
    if (!gNode) {
      for (const d of overlayData.values()) {
        gNode = d.nodes.get(nodeKey);
        if (gNode) break;
      }
    }
    return gNode;
  }, [frame.nodes, overlayData]);

  const _fetchNeighbors = useCallback(async (cypherQuery: string): Promise<{ nodes: Map<string, GNode>; edges: Map<string, GEdge> } | null> => {
    const res = await fetch("/data/cypher", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query: cypherQuery, params: {} }),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      let err: unknown;
      try { err = JSON.parse(text); } catch { err = text; }
      console.error("show neighbors query failed (HTTP", res.status, "):", err);
      return null;
    }
    const data = await res.json();
    const rows: Record<string, unknown>[] = data.rows ?? [];
    return extractElements(rows);
  }, []);

  const handleToggleChildren = useCallback(async (nodeKey: string) => {
    const overlayKey = `${nodeKey}:children`;
    if (overlayData.has(overlayKey)) {
      setOverlayData((prev) => { const next = new Map(prev); next.delete(overlayKey); return next; });
      return;
    }
    const gNode = _resolveNodeForKey(nodeKey);
    if (!gNode) return;
    const tableLabel = gNode.label.includes(":") ? gNode.label.split(":").pop()! : gNode.label;
    const pkCols = pkMap[gNode.label] ?? pkMap[tableLabel] ?? [];
    const pkCol = pkCols[0] ?? null;
    const pkValue = gNode.properties[pkCol ?? ""] ?? gNode.id;
    const pkLit = pkValue === null || pkValue === undefined
      ? null
      : isNaN(Number(pkValue)) ? `"${String(pkValue).replace(/"/g, '\\"')}"` : String(pkValue);
    if (!pkLit || !pkCol) return;
    const tl = tableLabel.toLowerCase();
    const rels = (relationships ?? []).filter((r) => r.sourceTableName.toLowerCase() === tl);
    if (rels.length === 0) return;
    // Run each relationship as a separate query and merge — avoids Trino UNION schema mismatch
    const merged: { nodes: Map<string, GNode>; edges: Map<string, GEdge> } = { nodes: new Map(), edges: new Map() };
    await Promise.all(rels.map(async (r) => {
      const relType = (r.computedCypherAlias ?? r.alias ?? "").toUpperCase();
      const q = `MATCH (n:${tableLabel})-[r:${relType}]->(child) WHERE n.${pkCol} = ${pkLit} RETURN n, r, child`;
      const result = await _fetchNeighbors(q);
      if (result) {
        result.nodes.forEach((n, k) => merged.nodes.set(k, n));
        result.edges.forEach((e, k) => merged.edges.set(k, e));
      }
    }));
    if (merged.nodes.size > 0 || merged.edges.size > 0) {
      setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
    }
  }, [frame.nodes, overlayData, pkMap, relationships, _resolveNodeForKey, _fetchNeighbors]);

  const handleToggleParents = useCallback(async (nodeKey: string) => {
    const overlayKey = `${nodeKey}:parents`;
    if (overlayData.has(overlayKey)) {
      setOverlayData((prev) => { const next = new Map(prev); next.delete(overlayKey); return next; });
      return;
    }
    const gNode = _resolveNodeForKey(nodeKey);
    if (!gNode) return;
    const tableLabel = gNode.label.includes(":") ? gNode.label.split(":").pop()! : gNode.label;
    const pkCols = pkMap[gNode.label] ?? pkMap[tableLabel] ?? [];
    const pkCol = pkCols[0] ?? null;
    // gNode.id holds the PK value (graph rewriter places it in "id" and excludes it from properties)
    const pkValue = gNode.properties[pkCol ?? ""] ?? gNode.id;
    const pkLit = pkValue === null || pkValue === undefined
      ? null
      : isNaN(Number(pkValue)) ? `"${String(pkValue).replace(/"/g, '\\"')}"` : String(pkValue);
    if (!pkLit || !pkCol) return;
    const tl = tableLabel.toLowerCase();
    const rels = (relationships ?? []).filter((r) => r.targetTableName.toLowerCase() === tl);
    if (rels.length === 0) return;
    // Run each relationship as a separate query and merge — avoids Trino UNION schema mismatch
    const merged: { nodes: Map<string, GNode>; edges: Map<string, GEdge> } = { nodes: new Map(), edges: new Map() };
    await Promise.all(rels.map(async (r) => {
      const relType = (r.computedCypherAlias ?? r.alias ?? "").toUpperCase();
      const q = `MATCH (parent)-[r:${relType}]->(n:${tableLabel}) WHERE n.${pkCol} = ${pkLit} RETURN n, r, parent`;
      const result = await _fetchNeighbors(q);
      if (result) {
        result.nodes.forEach((n, k) => merged.nodes.set(k, n));
        result.edges.forEach((e, k) => merged.edges.set(k, e));
      }
    }));
    if (merged.nodes.size > 0 || merged.edges.size > 0) {
      setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
    }
  }, [frame.nodes, overlayData, pkMap, relationships, _resolveNodeForKey, _fetchNeighbors]);

  const handleExcludeNode = useCallback((nodeKeys: string[]) => {
    // Chain exclusions across all selected nodes, then only update query text.
    // Nodes are already removed from canvas by the caller — no relayout or overlay reset needed.
    let currentQuery = editQueryRef.current;
    for (const nodeKey of nodeKeys) {
      const gNode = frame.nodes.get(nodeKey);
      if (!gNode) continue;
      const label = gNode.label;
      const nodeId = String(gNode.id);
      const pkCols = pkMap[label] ?? [];
      const pkCol = pkCols[0] ?? null;
      const pkValue = pkCol ? gNode.properties[pkCol] : undefined;
      const newQuery = injectExclusion(currentQuery, label, nodeId, pkCol, pkValue, relationships);
      if (newQuery) currentQuery = newQuery;
    }
    if (currentQuery !== editQueryRef.current) {
      setEditQuery(currentQuery);
    }
  }, [frame.nodes, pkMap, relationships]);

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    const startX = e.clientX;
    const startW = inspWidthRef.current;
    const onMove = (me: MouseEvent) => {
      setInspectorWidth(Math.max(160, Math.min(480, startW + (startX - me.clientX))));
    };
    const onUp = () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, []);

  const handleFrameResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    const startY = e.clientY;
    const startH = graphAreaHeightRef.current;
    const onMove = (me: MouseEvent) => {
      setGraphAreaHeight(Math.max(150, Math.min(1200, startH + (me.clientY - startY))));
    };
    const onUp = () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, []);

  const overlayNodes = useMemo(() => {
    if (overlayData.size === 0) return new Map<string, GNode>();
    const m = new Map<string, GNode>();
    for (const d of overlayData.values()) d.nodes.forEach((n, k) => { if (!frame.nodes.has(k)) m.set(k, n); });
    return m;
  }, [frame.nodes, overlayData]);

  const overlayEdges = useMemo(() => {
    if (overlayData.size === 0) return new Map<string, GEdge>();
    const m = new Map<string, GEdge>();
    for (const d of overlayData.values()) d.edges.forEach((e, k) => { if (!frame.edges.has(k)) m.set(k, e); });
    return m;
  }, [frame.edges, overlayData]);

  const mergedNodes = useMemo(() => {
    if (overlayNodes.size === 0) return frame.nodes;
    const m = new Map(frame.nodes);
    overlayNodes.forEach((n, k) => m.set(k, n));
    return m;
  }, [frame.nodes, overlayNodes]);

  const mergedEdges = useMemo(() => {
    if (overlayEdges.size === 0) return frame.edges;
    const m = new Map(frame.edges);
    overlayEdges.forEach((e, k) => m.set(k, e));
    return m;
  }, [frame.edges, overlayEdges]);

  const showingChildren = useMemo(() => new Set(
    Array.from(overlayData.keys()).filter(k => k.endsWith(":children")).map(k => k.slice(0, -":children".length))
  ), [overlayData]);
  const showingParents = useMemo(() => new Set(
    Array.from(overlayData.keys()).filter(k => k.endsWith(":parents")).map(k => k.slice(0, -":parents".length))
  ), [overlayData]);

  const remainingRelsActive = overlayData.has("__remaining_rels");

  const handleToggleRemainingRels = useCallback(async () => {
    if (overlayData.has("__remaining_rels")) {
      setOverlayData((prev) => { const next = new Map(prev); next.delete("__remaining_rels"); return next; });
      return;
    }
    const queries = buildRemainingRelsQueries(mergedNodes, pkMap);
    if (queries.length === 0) return;
    const merged: { nodes: Map<string, GNode>; edges: Map<string, GEdge> } = { nodes: new Map(), edges: new Map() };
    await Promise.all(queries.map(async (q) => {
      const result = await _fetchNeighbors(q);
      if (result) {
        result.nodes.forEach((n, k) => merged.nodes.set(k, n));
        result.edges.forEach((e, k) => merged.edges.set(k, e));
      }
    }));
    if (merged.nodes.size > 0 || merged.edges.size > 0) {
      setOverlayData((prev) => new Map(prev).set("__remaining_rels", merged));
    }
  }, [overlayData, mergedNodes, pkMap, _fetchNeighbors]);

  const hasGraph = frame.nodes.size > 0 || frame.edges.size > 0;
  const activeView: "graph" | "table" | "json" = hasGraph ? view : (view === "json" ? "json" : "table");

  const renderHeader = (isModal: boolean) => (
    <div className="gf-header">
      <div className="gf-query-editor-wrap">
        <CodeMirror
          className="gf-header-query-input"
          value={editQuery}
          theme={oneDark}
          extensions={[
            cypherLanguage(),
            EditorView.lineWrapping,
            Prec.highest(keymap.of([{
              key: "Enter",
              run: () => { handleRerun(frame.id, editQuery.trim()); return true; },
            }])),
          ]}
          onChange={(val) => setEditQuery(val)}
          basicSetup={{ lineNumbers: false, foldGutter: false, highlightActiveLine: false }}
        />
        <button
          className="gf-copy-query-btn"
          title="Copy query"
          onClick={() => navigator.clipboard.writeText(editQuery)}
        >⎘</button>
      </div>
      <div className="gf-header-meta">
        {frame.status === "loading" && <span className="gf-loading">Running…</span>}
        {frame.status === "done" && (
          <span className="gf-meta-text">
            {frame.nodes.size} nodes · {frame.edges.size} rels
            {frame.elapsed !== undefined && ` · ${frame.elapsed}ms`}
          </span>
        )}
        {frame.status === "error" && <span className="gf-meta-error">Error</span>}
      </div>
      <div className="gf-header-actions">
        <button className="gf-run-inline-btn" onClick={() => handleRerun(frame.id, editQuery.trim())} title="Run">▶</button>
        {hasGraph && frame.status === "done" && (
          <button
            className={`gf-icon-btn${remainingRelsActive ? " active" : ""}`}
            onClick={handleToggleRemainingRels}
            title={remainingRelsActive ? "Hide remaining relationships" : "Show remaining relationships between visible nodes"}
          >⊕</button>
        )}
        {hasGraph && (
          <button className={`gf-view-btn ${activeView === "graph" ? "active" : ""}`}
                  onClick={() => setView("graph")} title="Graph">⬡</button>
        )}
        <button className={`gf-view-btn ${activeView === "table" ? "active" : ""}`}
                onClick={() => setView("table")} title="Table">⊞</button>
        <button className={`gf-view-btn ${activeView === "json" ? "active" : ""}`}
                onClick={() => setView("json")} title="JSON">{"{}"}</button>
        {activeView === "table" && frame.rows.length > 0 && (
          <button
            className={`gf-icon-btn${tableWrap ? " gf-icon-btn--on" : ""}`}
            title="Wrap cell text"
            onClick={() => setTableWrap((v) => !v)}
          >⇌</button>
        )}
        {frame.status === "done" && (frame.rows.length > 0 || hasGraph) && (
          <div className="gf-dl-wrap">
            <button
              className="gf-icon-btn"
              title="Download"
              onClick={() => setShowDlMenu((v) => !v)}
            >
              <svg width="14" height="14" viewBox="0 0 16 16" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
                <path d="M8 10.5L4.5 7h2V2h3v5h2L8 10.5z"/>
                <rect x="2" y="12" width="12" height="1.5" rx="0.75"/>
              </svg>
            </button>
            {showDlMenu && (
              <div className="gf-dl-menu" onMouseLeave={() => setShowDlMenu(false)}>
                {frame.rows.length > 0 && (
                  <button className="gf-dl-item" onClick={() => {
                    const json = JSON.stringify(frame.rows, null, 2);
                    _downloadBlob(new Blob([json], { type: "application/json" }), "result.json");
                    setShowDlMenu(false);
                  }}>JSON</button>
                )}
                {frame.rows.length > 0 && (
                  <button className="gf-dl-item" onClick={() => {
                    const csv = _toCSV(frame.columns, frame.rows);
                    _downloadBlob(new Blob([csv], { type: "text/csv" }), "result.csv");
                    setShowDlMenu(false);
                  }}>CSV</button>
                )}
                {hasGraph && activeView === "graph" && (
                  <button className="gf-dl-item" onClick={() => {
                    const cy = canvasCyRef.current;
                    if (!cy) return;
                    const blob = cy.png({ output: "blob", bg: "transparent", full: true }) as unknown as Blob;
                    _downloadBlob(blob, "graph.png");
                    setShowDlMenu(false);
                  }}>PNG</button>
                )}
                {hasGraph && activeView === "graph" && (
                  <button className="gf-dl-item" onClick={() => {
                    const cy = canvasCyRef.current;
                    if (!cy) return;
                    const blob = cy.jpg({ output: "blob", bg: "transparent", full: true }) as unknown as Blob;
                    _downloadBlob(blob, "graph.jpg");
                    setShowDlMenu(false);
                  }}>JPG</button>
                )}
                {hasGraph && activeView === "graph" && (
                  <button className="gf-dl-item" onClick={() => {
                    const cy = canvasCyRef.current;
                    if (!cy) return;
                    const svgStr = cy.svg({ full: true, bg: "transparent" });
                    _downloadBlob(new Blob([svgStr], { type: "image/svg+xml" }), "graph.svg");
                    setShowDlMenu(false);
                  }}>SVG</button>
                )}
              </div>
            )}
          </div>
        )}
        {!isModal && (
          <button className="gf-icon-btn" onClick={() => setExpanded(true)} title="Expand">⤢</button>
        )}
        {isModal && (
          <button className="gf-icon-btn" onClick={() => setExpanded(false)} title="Exit full screen">⤡</button>
        )}
        {!isModal && (
          <button className="gf-icon-btn" onClick={() => setCollapsed((c) => !c)} title={collapsed ? "Expand" : "Collapse"}>
            {collapsed ? "▼" : "▲"}
          </button>
        )}
        <button className="gf-icon-btn" onClick={() => onClose(frame.id)} title="Close">✕</button>
      </div>
    </div>
  );

  const frameBody = (
    <div className="gf-body">
      {frame.status === "error" && <div className="gf-error">{frame.error}</div>}
      {frame.status !== "error" && hasGraph && (
        <div className="gf-graph-area" style={{ height: graphAreaHeight, display: activeView === "graph" ? undefined : "none" }}>
          <GraphCanvas nodes={frame.nodes} edges={frame.edges} overlayNodes={overlayNodes} overlayEdges={overlayEdges} onSelect={setSelected} colorOverrides={colorOverrides} sizeOverrides={sizeOverrides} labelProperty={labelProperty} relLineOverrides={relLineOverrides} onExcludeNode={handleExcludeNode} pkMap={pkMap} relationships={relationships ?? []} showingChildren={showingChildren} onToggleChildren={handleToggleChildren} showingParents={showingParents} onToggleParents={handleToggleParents} onCyReady={(cy) => { canvasCyRef.current = cy; }} />
          <Inspector selected={selected} colorOverrides={colorOverrides} onColorChange={onColorChange}
                     onClose={() => setSelected(null)}
                     width={inspectorWidth} onResizeStart={handleResizeStart}
                     relationships={relationships} onSaveEdgeAlias={onSaveEdgeAlias}
                     pkMap={pkMap} />
        </div>
      )}
      {frame.status !== "error" && activeView === "table" && (
        <TableView columns={frame.columns} rows={frame.rows} wrap={tableWrap} height={graphAreaHeight} colWidths={tableColWidths} setColWidths={setTableColWidths} />
      )}
      {frame.status !== "error" && activeView === "json" && (() => {
        const jsonStr = JSON.stringify(frame.rows, null, 2);
        return (
          <div className="gf-json-wrap">
            <CodeMirror
              className="gf-json-view"
              value={jsonStr}
              theme={oneDark}
              height={`${graphAreaHeight}px`}
              readOnly
              basicSetup={{ foldGutter: true, lineNumbers: true }}
              extensions={[jsonLang(), EditorView.lineWrapping]}
            />
            <JsonCopyButton text={jsonStr} />
          </div>
        );
      })()}
    </div>
  );

  return (
    <>
      <div className={`gf-frame${expanded ? " gf-expanded" : ""}`}>
        {renderHeader(false)}
        {!collapsed && frameBody}
        {!collapsed && !expanded && (
          <div className="gf-frame-resize-handle" onMouseDown={handleFrameResizeStart} />
        )}
      </div>
      {expanded && createPortal(
        <div className="gf-modal-overlay" onClick={() => setExpanded(false)}>
          <div className="gf-modal-frame" onClick={(e) => e.stopPropagation()}>
            {renderHeader(true)}
          </div>
        </div>,
        document.body
      )}
    </>
  );
}
