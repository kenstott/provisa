// Copyright (c) 2026 Kenneth Stott
// Canary: a3f7e2d1-8c4b-4a9f-b5e6-2d1c7f8a3e4b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useRef, useEffect, useLayoutEffect, useState, useCallback, useMemo } from "react";
import type { Relationship } from "../types/admin";
import CodeMirror from "@uiw/react-codemirror";
import * as _neo4jCypherMod from "@neo4j-cypher/codemirror";
import "@neo4j-cypher/codemirror/css/cypher-codemirror.css";
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const { getCypherLanguageExtensions: _getGFCypherExts, cypherLinter: _gfCypherLinter } = _neo4jCypherMod as any;
const _gfCypherLangExts = _getGFCypherExts({ cypherLanguage: true } as any);
import { json as jsonLang } from "@codemirror/lang-json";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView, keymap } from "@codemirror/view";
import { Prec } from "@codemirror/state";
import { createPortal } from "react-dom";
import { CopySymbolButton } from "../components/CopyButton";
import cytoscape from "cytoscape";
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

// Local types for cytoscape — avoids import-resolution issues with the package's
// legacy export= declarations under moduleResolution "bundler".
type CyLayoutOptions = { name: string; [key: string]: unknown };
type CyElementDefinition = { group?: "nodes" | "edges"; data: Record<string, unknown>; [key: string]: unknown };
// Aliases kept for callers that reference the old names.
type CyElementDef = CyElementDefinition;
interface CyElement {
  id(): string;
  data(key: string): unknown;
  data(key: string, value: unknown): CyElement;
  lock(): CyElement;
  unlock(): CyElement;
  locked(): boolean;
  select(): CyElement;
  unselect(): CyElement;
  style(name: string, value: unknown): CyElement;
  style(props: Record<string, unknown>): CyElement;
  addClass(cls: string): CyElement;
  removeClass(cls: string): CyElement;
  position(): { x: number; y: number };
  position(pos: { x: number; y: number }): CyElement;
  source(): CyElement;
  target(): CyElement;
  neighborhood(): CyCollection;
}
interface CyCollection {
  length: number;
  [index: number]: CyElement;
  forEach(fn: (ele: CyElement, i: number) => void): this;
  map<T>(fn: (ele: CyElement, i: number) => T): T[];
  filter(fn: ((ele: CyElement) => boolean) | string): this;
  select(): this;
  unselect(): this;
  remove(): this;
  position(): { x: number; y: number };
  position(pos: { x: number; y: number }): this;
  lock(): this;
  unlock(): this;
  locked(): boolean;
  addClass(cls: string): this;
  removeClass(cls: string): this;
  neighborhood(selector?: string): CyCollection;
  data(key: string): unknown;
  id(): string;
}
interface CyNodeCollection extends CyCollection {
  forEach(fn: (ele: CyElement, i: number) => void): this;
}
interface CyEvent {
  target: CyElement & CyInstance;
  position: { x: number; y: number };
  renderedPosition?: { x: number; y: number };
}
interface CyInstance {
  $(selector: string): CyCollection;
  $id(id: string): CyCollection;
  nodes(selector?: string): CyNodeCollection;
  edges(selector?: string): CyCollection;
  elements(selector?: string): CyCollection;
  add(eles: CyElementDef | CyElementDef[] | CyCollection): CyCollection;
  remove(eles: CyCollection | string): CyCollection;
  batch(fn: () => void): void;
  layout(options: CyLayoutOptions): { run(): void; stop(): void; one(evt: string, fn: () => void): void };
  fit(eles?: CyCollection, padding?: number): void;
  zoom(): number;
  zoom(level: number): void;
  pan(): { x: number; y: number };
  pan(pos: { x: number; y: number }): void;
  on(events: string, fn: (e: CyEvent) => void): void;
  on(events: string, selector: string, fn: (e: CyEvent) => void): void;
  off(events: string, fn?: (e: CyEvent) => void): void;
  png(options?: Record<string, unknown>): string;
  jpg(options?: Record<string, unknown>): string;
  svg(options?: Record<string, unknown>): string;
  destroy(): void;
  style(sheet?: unknown): void;
}

function _downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function _compositeGraphDownload(
  cy: CyInstance,
  hullSvg: SVGSVGElement | null,
  filename: string,
  format: "png" | "jpg",
) {
  const bg = format === "jpg" ? "white" : "transparent";
  const mimeType = format === "jpg" ? "image/jpeg" : "image/png";
  const hasHulls = hullSvg && hullSvg.children.length > 0;

  if (!hasHulls) {
    const blob = (format === "jpg"
      ? cy.jpg({ output: "blob", bg, full: true })
      : cy.png({ output: "blob", bg, full: true })) as unknown as Blob;
    _downloadBlob(blob, filename);
    return;
  }

  // Hull coords are renderedPosition()-based (viewport pixels). Capture viewport only.
  const dataUrl = (format === "jpg"
    ? cy.jpg({ output: "dataURL", bg })
    : cy.png({ output: "dataURL", bg })) as string;

  const img = new Image();
  img.onload = () => {
    const canvas = document.createElement("canvas");
    canvas.width = img.width;
    canvas.height = img.height;
    const ctx = canvas.getContext("2d")!;
    ctx.drawImage(img, 0, 0);

    // viewBox maps hull CSS-pixel coords → PNG physical pixels (handles devicePixelRatio)
    const container = (cy as any).container() as HTMLElement;
    const cssW = container.offsetWidth;
    const cssH = container.offsetHeight;
    const svgClone = hullSvg!.cloneNode(true) as SVGSVGElement;
    svgClone.setAttribute("viewBox", `0 0 ${cssW} ${cssH}`);
    svgClone.setAttribute("width", String(img.width));
    svgClone.setAttribute("height", String(img.height));

    const svgData = new XMLSerializer().serializeToString(svgClone);
    const svgBlob = new Blob([svgData], { type: "image/svg+xml;charset=utf-8" });
    const svgUrl = URL.createObjectURL(svgBlob);
    const svgImg = new Image();
    const finish = () => {
      URL.revokeObjectURL(svgUrl);
      canvas.toBlob((b) => { if (b) _downloadBlob(b, filename); }, mimeType);
    };
    svgImg.onload = () => { ctx.drawImage(svgImg, 0, 0, img.width, img.height); finish(); };
    svgImg.onerror = finish;
    svgImg.src = svgUrl;
  };
  img.src = dataUrl;
}

function _downloadGraphSvg(cy: CyInstance, hullSvg: SVGSVGElement | null) {
  const hasHulls = hullSvg && hullSvg.children.length > 0;
  // Viewport SVG shares coord system with renderedPosition(); full=true would differ.
  const svgBase = cy.svg({ full: !hasHulls, bg: "transparent" });
  if (!hasHulls) {
    _downloadBlob(new Blob([svgBase], { type: "image/svg+xml" }), "graph.svg");
    return;
  }
  const hullContent = Array.from(hullSvg!.children)
    .map((c) => new XMLSerializer().serializeToString(c))
    .join("");
  const composite = svgBase.replace("</svg>", `<g class="hulls">${hullContent}</g></svg>`);
  _downloadBlob(new Blob([composite], { type: "image/svg+xml" }), "graph.svg");
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

const CLUSTER_COLORS = [
  "#6366f1","#8b5cf6","#ec4899","#f97316","#eab308",
  "#22c55e","#06b6d4","#3b82f6","#ef4444","#14b8a6",
  "#a855f7","#f43f5e","#84cc16","#0ea5e9","#d946ef",
];
function clusterColor(id: string): string {
  // Hash the string id to a stable index
  let h = 0;
  for (let i = 0; i < id.length; i++) h = (Math.imul(31, h) + id.charCodeAt(i)) | 0;
  return CLUSTER_COLORS[((h % CLUSTER_COLORS.length) + CLUSTER_COLORS.length) % CLUSTER_COLORS.length];
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
  schemaRels?: Array<{ type: string; source: string; target: string }>,
): Array<string> {
  const byLabel = new Map<string, GNode[]>();
  nodes.forEach((n) => {
    const arr = byLabel.get(n.label) ?? [];
    arr.push(n);
    byLabel.set(n.label, arr);
  });
  const labels = [...byLabel.keys()];
  const visibleTableLabels = new Set(labels.map(l => l.includes(":") ? l.split(":").pop()! : l));
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
    // For non-visible targets: generate discovery queries using schema relationships
    if (schemaRels) {
      for (const rel of schemaRels) {
        const rSrcLabel = rel.source.includes(":") ? rel.source.split(":").pop()! : rel.source;
        if (rSrcLabel !== tableLabel) continue;
        const tgtTableLabel = rel.target.includes(":") ? rel.target.split(":").pop()! : rel.target;
        if (visibleTableLabels.has(tgtTableLabel)) continue;
        queries.push(
          `MATCH (a:${tableLabel})-[:${rel.type}]->(b:${tgtTableLabel}) WHERE a.${pkCol} IN [${srcIds}] RETURN a, b`
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
        if (key) relMap.set(key.toUpperCase(), { src: rel.sourceTableName, tgt: rel.targetTableName });
      }
      // Parse all (src)-[r:TYPE]->(tgt) and (src)<-[r:TYPE]-(tgt) patterns
      const relPatternRe = /\((\w*)\)\s*(?:<-\[(?:\w*):(\w+)\]-|\-\[(?:\w*):(\w+)\]-?>)\s*\((\w*)\)/gi;
      let found: { varN: string; newQuery: string } | null = null;
      let m: RegExpExecArray | null;
      while ((m = relPatternRe.exec(workingQuery)) !== null) {
        const [_fullMatch, srcVar, revType, fwdType, tgtVar] = m;
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
    ? (isNaN(Number(pkValue)) ? `'${String(pkValue).replace(/'/g, "\\'")}'` : String(pkValue))
    : (isNaN(Number(nodeId)) ? `'${nodeId.replace(/'/g, "\\'")}'` : nodeId);
  const clause = usePk
    ? `${varName}.${pkCol} IN [${valLit}]`
    : `id(${varName}) IN [${valLit}]`;

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

// ── Cluster helpers ───────────────────────────────────────────────────────────
type ClusterLevel = "none" | "l1" | "l2" | "l3" | string;

// Sanitize a property value for use in a Cytoscape element ID
function _cidToId(val: string): string { return val.replace(/[^a-zA-Z0-9_-]/g, "_"); }

function buildClusterElements(
  nodes: Map<string, GNode>,
  edges: Map<string, GEdge>,
  level: Exclude<ClusterLevel, "none">,
  overlayEdges?: Map<string, GEdge>,
  collapsedClusters: Set<string> = new Set(),
): CyElementDefinition[] {
  const clusterKey = level === "l1" || level === "schema_L1" ? "scl1"
    : level === "l2" || level === "schema_L2" ? "scl2"
    : level === "l3" || level === "schema_L3" ? "scl3"
    : level;

  // 1. Cluster nodes — compound hull (expanded) or collapsed super-node
  const clusterLabels = new Map<string, Set<string>>();
  const clusterSizes = new Map<string, number>();
  nodes.forEach((n) => {
    const raw = n.properties[clusterKey];
    if (raw === null || raw === undefined) return;
    const cid = String(raw);
    if (!clusterLabels.has(cid)) { clusterLabels.set(cid, new Set()); clusterSizes.set(cid, 0); }
    clusterLabels.get(cid)!.add(n.label.includes(":") ? n.label.split(":").pop()! : n.label);
    clusterSizes.set(cid, (clusterSizes.get(cid) ?? 0) + 1);
  });

  const els: CyElementDefinition[] = [];
  clusterLabels.forEach((labels, cid) => {
    if (collapsedClusters.has(cid)) {
      // Collapsed: single representative node
      els.push({
        group: "nodes",
        data: {
          id: `__collapsed_${level}_${_cidToId(cid)}`,
          label: `${cid}\n(${clusterSizes.get(cid) ?? 0})`,
          _collapsed: true,
          _clusterId: cid,
          _clusterLevel: level,
          _clusterSize: clusterSizes.get(cid) ?? 0,
          _color: clusterColor(cid),
        },
      });
    } else {
      els.push({
        group: "nodes",
        data: {
          id: `__cluster_${level}_${_cidToId(cid)}`,
          label: cid,
          _cluster: true,
          _clusterId: cid,
          _clusterLevel: level,
          _clusterSize: clusterSizes.get(cid) ?? 0,
          _clusterLabels: [...labels].sort().join(", "),
        },
      });
    }
  });

  // 2. Data child nodes — only for expanded clusters
  const nodeToCid = new Map<string, string | null>();
  nodes.forEach((n, k) => {
    const raw = n.properties[clusterKey];
    nodeToCid.set(k, raw !== null && raw !== undefined ? String(raw) : null);
  });

  nodes.forEach((n, k) => {
    const cid = nodeToCid.get(k) ?? null;
    if (cid !== null && collapsedClusters.has(cid)) return; // hidden inside collapsed super-node
    const parentId = cid !== null ? `__cluster_${level}_${_cidToId(cid)}` : undefined;
    els.push({
      group: "nodes",
      data: { id: k, label: n.label, _node: n, ...(parentId ? { parent: parentId, _inCluster: true } : {}) },
    });
  });

  // 3. Edges — intra-cluster between data nodes; everything crossing a cluster boundary
  //    (including free↔cluster and collapsed↔anything) becomes a meta-edge.
  const allEdges = overlayEdges ? new Map([...edges, ...overlayEdges]) : edges;
  const metaEdges = new Map<string, { src: string; tgt: string; type: string; count: number }>();

  // Effective routing ID for an edge endpoint:
  //   collapsed cluster member → collapsed super-node
  //   expanded cluster member  → compound hull node (for meta-edge dedup)
  //   free node                → null (use data node key directly)
  const routingId = (_nodeKey: string, cid: string | null): string | null => {
    if (cid === null) return null;
    if (collapsedClusters.has(cid)) return `__collapsed_${level}_${_cidToId(cid)}`;
    return `__cluster_${level}_${_cidToId(cid)}`;
  };

  allEdges.forEach((e) => {
    const srcKey = `${e.startNode.label}:${e.startNode.id}`;
    const tgtKey = `${e.endNode.label}:${e.endNode.id}`;
    if (!nodes.has(srcKey) || !nodes.has(tgtKey)) return;
    const srcCid = nodeToCid.get(srcKey) ?? null;
    const tgtCid = nodeToCid.get(tgtKey) ?? null;

    // Same cluster: intra-cluster data edge (expanded) or drop (collapsed)
    if (srcCid !== null && srcCid === tgtCid) {
      if (!collapsedClusters.has(srcCid)) {
        els.push({ group: "edges", data: { id: e.identity, source: srcKey, target: tgtKey, label: e.type, _edge: e } });
      }
      return;
    }

    const srcRouting = routingId(srcKey, srcCid);
    const tgtRouting = routingId(tgtKey, tgtCid);

    // Both free: plain data edge
    if (srcRouting === null && tgtRouting === null) {
      els.push({ group: "edges", data: { id: e.identity, source: srcKey, target: tgtKey, label: e.type, _edge: e } });
      return;
    }

    // At least one side is clustered: consolidate into meta-edge
    const srcId = srcRouting ?? srcKey;
    const tgtId = tgtRouting ?? tgtKey;
    const metaKey = `${srcId}→${tgtId}:${e.type}`;
    const existing = metaEdges.get(metaKey);
    if (existing) { existing.count += 1; }
    else { metaEdges.set(metaKey, { src: srcId, tgt: tgtId, type: e.type, count: 1 }); }
  });

  metaEdges.forEach(({ src, tgt, type, count }, metaKey) => {
    els.push({
      group: "edges",
      data: {
        id: `__meta_${metaKey}`,
        source: src,
        target: tgt,
        label: count > 1 ? `${type} (×${count})` : type,
        _metaEdge: true,
        _metaCount: count,
        _metaType: type,
      },
    });
  });

  return els;
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
  labelSiblings?: Record<string, string[]>;
  showingChildrenNatural: Set<string>;
  onToggleChildren: (nodeKey: string) => void;
  onToggleChildrenBatch: (nodeKeys: string[], circular?: boolean) => void;
  showingChildrenCircular: Set<string>;
  onToggleChildrenCircular: (nodeKey: string) => void;
  showingParents: Set<string>;
  onToggleParents: (nodeKey: string) => void;
  onToggleParentsBatch: (nodeKeys: string[], circular?: boolean) => void;
  showingParentsCircular: Set<string>;
  onToggleParentsCircular: (nodeKey: string) => void;
  onCyReady?: (cy: CyInstance | null) => void;
  clusterLevel: ClusterLevel;
  hullSvgRef?: React.Ref<SVGSVGElement>;
}

type LayoutMode = "force" | "hierarchy";

const PIN_SVG = "data:image/svg+xml;utf8," + encodeURIComponent(
  `<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'>` +
  `<circle cx='8' cy='6' r='5' fill='%23fbbf24' stroke='%2392400e' stroke-width='1.5'/>` +
  `<circle cx='8' cy='6' r='2' fill='%2392400e'/>` +
  `<line x1='8' y1='11' x2='8' y2='16' stroke='%2392400e' stroke-width='1.5' stroke-linecap='round'/>` +
  `</svg>`
);

const LAYOUT_OPTIONS: Record<LayoutMode, CyLayoutOptions> = {
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
  } as CyLayoutOptions,
  hierarchy: {
    name: "breadthfirst",
    animate: false,
    directed: true,
    padding: 20,
    spacingFactor: 1.4,
  } as CyLayoutOptions,
};

function GraphCanvas({ nodes, edges, overlayNodes, overlayEdges, onSelect, colorOverrides, sizeOverrides, labelProperty, relLineOverrides, onExcludeNode, pkMap, relationships, labelSiblings: _labelSiblings, showingChildrenNatural, onToggleChildren: _onToggleChildren, onToggleChildrenBatch, showingChildrenCircular, onToggleChildrenCircular: _onToggleChildrenCircular, showingParents, onToggleParents: _onToggleParents, onToggleParentsBatch, showingParentsCircular, onToggleParentsCircular: _onToggleParentsCircular, onCyReady, clusterLevel, hullSvgRef }: CanvasProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<CyInstance | null>(null);
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
  const [edgeDistance, setEdgeDistance] = useState(() => {
    const saved = localStorage.getItem("provisa.graph.edgeDistance");
    return saved ? Number(saved) : 80;
  });
  const edgeDistanceRef = useRef(edgeDistance);
  const nudgeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const nudgeHeldRef = useRef(false);
  const circularChildParentsRef = useRef(showingChildrenCircular);
  circularChildParentsRef.current = showingChildrenCircular;
  const circularParentNodesRef = useRef(showingParentsCircular);
  circularParentNodesRef.current = showingParentsCircular;
  // Track nodes that the user has manually dragged; these stay anchored during re-layout
  const anchoredRef = useRef<Set<string>>(new Set());
  // Prevents concurrent layout runs from clobbering each other's unlock step
  const layoutRunningRef = useRef(false);
  // Tracks the active cytoscape layout object so it can be stopped before starting a new one
  const activeLayoutRef = useRef<{ stop: () => void } | null>(null);
  // Stable ref to nudgeLayout so event handlers can call it without stale closure
  const nudgeLayoutRef = useRef<(freeNodes?: Set<string>, aggressive?: boolean) => void>(() => {});
  // SVG hull circles drawn over the canvas for cluster visualization
  const [hullCircles, setHullCircles] = useState<Array<{ cid: string; x: number; y: number; r: number }>>([]);
  const [collapsedClusters, setCollapsedClusters] = useState<Set<string>>(new Set());
  const collapsedClustersRef = useRef<Set<string>>(new Set());
  const clusterLevelRef = useRef(clusterLevel);
  const hullDragRef = useRef<{ cid: string; lastX: number; lastY: number; startX: number; startY: number } | null>(null);
  clusterLevelRef.current = clusterLevel;
  // Reset collapsed state when cluster level changes
  useEffect(() => {
    setCollapsedClusters(new Set());
    collapsedClustersRef.current = new Set();
  }, [clusterLevel]);
  const toggleCollapse = useCallback((cid: string) => {
    setCollapsedClusters((prev) => {
      const next = new Set(prev);
      if (next.has(cid)) next.delete(cid); else next.add(cid);
      collapsedClustersRef.current = next;
      return next;
    });
  }, []);
  const computeHulls = useCallback(() => {
    const cy = cyRef.current;
    if (!cy || clusterLevelRef.current === "none") { setHullCircles([]); return; }
    const collapsed = collapsedClustersRef.current;
    const hulls: Array<{ cid: string; x: number; y: number; r: number }> = [];
    cy.nodes("[?_cluster]").forEach((cn: any) => {
      const cid = cn.data("_clusterId") as string;
      if (collapsed.has(cid)) return; // collapsed clusters have no hull
      const children = cn.children() as any;
      if (children.length === 0) return;
      let sumX = 0, sumY = 0;
      children.forEach((c: any) => { const p = c.renderedPosition(); sumX += p.x; sumY += p.y; });
      const cx = sumX / children.length;
      const cyPos = sumY / children.length;
      let maxR = 30;
      children.forEach((c: any) => {
        const p = c.renderedPosition();
        maxR = Math.max(maxR, Math.hypot(p.x - cx, p.y - cyPos) + c.renderedWidth() / 2 + 20);
      });
      hulls.push({ cid, x: cx, y: cyPos, r: maxR });
    });
    setHullCircles(hulls);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => {
    const onMove = (e: MouseEvent) => {
      const drag = hullDragRef.current;
      if (!drag) return;
      const cy = cyRef.current;
      if (!cy) return;
      const zoom = cy.zoom();
      const dx = (e.clientX - drag.lastX) / zoom;
      const dy = (e.clientY - drag.lastY) / zoom;
      const clusterId = `__cluster_${clusterLevelRef.current}_${_cidToId(drag.cid)}`;
      (cy as any).getElementById(clusterId).children().forEach((n: any) => {
        const pos = n.position();
        n.position({ x: pos.x + dx, y: pos.y + dy });
      });
      drag.lastX = e.clientX;
      drag.lastY = e.clientY;
      computeHulls();
    };
    const onUp = (e: MouseEvent) => {
      const drag = hullDragRef.current;
      if (!drag) return;
      hullDragRef.current = null;
      const dist = Math.hypot(e.clientX - drag.startX, e.clientY - drag.startY);
      if (dist < 5) toggleCollapse(drag.cid);
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
    return () => {
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
  }, [computeHulls, toggleCollapse]);
  // Node right-click context menu
  const [nodeCtxMenu, setNodeCtxMenu] = useState<{ x: number; y: number; nodeId: string; selectedNodeIds: string[] } | null>(null);
  const menuRef = useRef<HTMLDivElement>(null);

  // Clamp menu inside canvas-wrap after each render so it never gets clipped
  useLayoutEffect(() => {
    const menu = menuRef.current;
    if (!menu) return;
    const wrap = menu.parentElement;
    if (!wrap) return;
    const wRect = wrap.getBoundingClientRect();
    const mRect = menu.getBoundingClientRect();
    let left = parseFloat(menu.style.left) || 0;
    let top = parseFloat(menu.style.top) || 0;
    // Clamp horizontally
    if (mRect.right > wRect.right) left -= mRect.right - wRect.right;
    if (mRect.left < wRect.left) left += wRect.left - mRect.left;
    // Clamp vertically: prefer flipping above the node if it overflows below
    if (mRect.bottom > wRect.bottom) top -= mRect.height;
    if (top < 0) top = 0;
    menu.style.left = `${left}px`;
    menu.style.top = `${top}px`;
    menu.style.visibility = "visible";
  }, [nodeCtxMenu]);

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
    let opts: CyLayoutOptions;
    if (cy.nodes().length === 0) {
      opts = { name: "null" } as CyLayoutOptions;
    } else if (m === "hierarchy") {
      opts = LAYOUT_OPTIONS.hierarchy;
    } else if (cy.edges().length === 0) {
      // No relationships — grid fills the container rectangle by auto-sizing rows/cols
      opts = { name: "grid", animate: false, fit: true, padding: 30, avoidOverlap: true, avoidOverlapPadding: 12 } as CyLayoutOptions;
    } else {
      const inCluster = clusterLevelRef.current !== "none";
      opts = {
        ...LAYOUT_OPTIONS.force,
        idealEdgeLength: () => edgeDistanceRef.current,
        // In cluster mode: higher nestingFactor shortens ideal edge length within compounds,
        // pulling cluster members together; higher repulsion spreads clusters apart.
        ...(inCluster ? { nestingFactor: 0.1, nodeRepulsion: () => 25000 } : {}),
      } as CyLayoutOptions;
    }
    const layout = cy.layout(opts);
    activeLayoutRef.current = layout;
    const applyStyles = () => {
      try {
        cy.batch(() => {
          cy.nodes().forEach((node) => {
            if (node.data("_cluster")) return;
            if (node.data("_collapsed")) return;
            const lbl = node.data("label") as string;
            const n = node.data("_node") as GNode | undefined;
            const base = colorOverridesRef.current[lbl] ?? labelColor(lbl);
            node.style("background-color", base);
            const base_sz = sizeOverridesRef.current[lbl] ?? 44;
            const sz = node.data("_inCluster") ? base_sz / 2 : base_sz;
            node.style({ width: sz, height: sz, "text-max-width": `${sz - 4}px` });
            if (n) {
              const prop = labelPropertyRef.current[n.label];
              node.style("label", prop
                ? String(n.properties[prop] ?? n.id)
                : String(n.properties["name"] ?? n.properties["title"] ?? n.id));
            }
            if (anchoredRef.current.has(node.id() as string)) node.addClass("pinned");
            else node.removeClass("pinned");
          });
        });
      } catch { /* cy may have been destroyed */ }
    };
    const releaseRun = () => {
      try { cy.nodes().forEach((n) => { if (anchored.has(n.id())) n.unlock(); }); } catch { /* cy may have been destroyed */ }
      layoutRunningRef.current = false;
    };

    const safetyTimer = setTimeout(() => { applyStyles(); releaseRun(); try { cy.fit(undefined, 40); } catch { /* cy may have been destroyed */ } }, 1000);
    layout.one("layoutstop", () => {
      clearTimeout(safetyTimer);
      applyStyles();
      releaseRun();
      try { cy.fit(undefined, 40); } catch { /* cy may have been destroyed */ }
    });
    layout.run();
  }, []);

  const nudgeLayout = useCallback((freeNodes?: Set<string>, aggressive = false) => {
    const cy = cyRef.current;
    if (!cy) return;
    if (layoutRunningRef.current) return;
    if (cy.edges().length === 0 || layoutModeRef.current === "hierarchy") {
      runLayout();
      return;
    }
    layoutRunningRef.current = true;
    try {
      const anchored = anchoredRef.current;
      // When freeNodes is provided, lock everything except those nodes (and unlock anchored after)
      const tempLocked = new Set<string>();
      cy.nodes().forEach((n) => {
        const id = n.id() as string;
        if (freeNodes && !freeNodes.has(id)) {
          if (!n.locked()) { n.lock(); tempLocked.add(id); }
        } else if (anchored.has(id)) {
          n.lock();
        }
      });
      const opts = {
        ...LAYOUT_OPTIONS.force,
        idealEdgeLength: () => edgeDistanceRef.current,
        randomize: false,
        animate: true,
        animationDuration: aggressive ? 2000 : 600,
        animationEasing: "ease-out" as const,
        numIter: aggressive ? 2000 : 300,
        fit: false,
      } as CyLayoutOptions;
      const layout = cy.layout(opts);
      activeLayoutRef.current = layout;
      const applyStylesNudge = () => {
        try {
          cy.batch(() => {
            cy.nodes().forEach((node) => {
              if (node.data("_cluster")) return;
              const lbl = node.data("label") as string;
              const n = node.data("_node") as GNode | undefined;
              const base = colorOverridesRef.current[lbl] ?? labelColor(lbl);
              node.style("background-color", base);
              const base_sz2 = sizeOverridesRef.current[lbl] ?? 44;
              const sz = node.data("_inCluster") ? base_sz2 / 2 : base_sz2;
              node.style({ width: sz, height: sz, "text-max-width": `${sz - 4}px` });
              if (n) {
                const prop = labelPropertyRef.current[n.label];
                node.style("label", prop
                  ? String(n.properties[prop] ?? n.id)
                  : String(n.properties["name"] ?? n.properties["title"] ?? n.id));
              }
              if (anchoredRef.current.has(node.id() as string)) node.addClass("pinned");
              else node.removeClass("pinned");
            });
          });
        } catch { /* cy may have been destroyed */ }
      };
      const releaseNudge = () => {
        try {
          tempLocked.forEach((id) => { const n = cy.$id(id); if (n.length > 0) n.unlock(); });
          cy.nodes().forEach((n) => { if (anchored.has(n.id())) n.unlock(); });
        } catch { /* cy may have been destroyed */ }
        layoutRunningRef.current = false;
        if (nudgeHeldRef.current) nudgeLayoutRef.current(undefined, true);
      };
      const safetyTimerNudge = setTimeout(() => { applyStylesNudge(); releaseNudge(); }, aggressive ? 3000 : 1000);
      layout.one("layoutstop", () => {
        clearTimeout(safetyTimerNudge);
        applyStylesNudge();
        releaseNudge();
      });
      layout.run();
    } catch {
      layoutRunningRef.current = false;
    }
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

  // Full rebuild — fires only when the base graph (query result) or cluster level changes
  useEffect(() => {
    if (!containerRef.current) return;
    const els: CyElementDefinition[] = clusterLevel !== "none"
      ? buildClusterElements(nodes, edges, clusterLevel, overlayEdges, collapsedClusters)
      : (() => {
          const _els: CyElementDefinition[] = [];
          nodes.forEach((n) => {
            _els.push({ group: "nodes", data: { id: `${n.label}:${n.id}`, label: n.label, _node: n } });
          });
          edges.forEach((e) => {
            const srcKey = `${e.startNode.label}:${e.startNode.id}`;
            const tgtKey = `${e.endNode.label}:${e.endNode.id}`;
            if (nodes.has(srcKey) && nodes.has(tgtKey)) {
              _els.push({ group: "edges", data: { id: e.identity, source: srcKey, target: tgtKey, label: e.type, _edge: e } });
            }
          });
          return _els;
        })();

    const cy = cytoscape({
      container: containerRef.current,
      elements: els,
      style: [
        {
          selector: "node",
          style: {
            "background-color": (ele: CyElement) => {
              const lbl = ele.data("label") as string;
              return colorOverridesRef.current[lbl] ?? labelColor(lbl);
            },
            "label": (ele: CyElement) => {
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
            "width": (ele: CyElement) => {
              const lbl = ele.data("label") as string;
              return sizeOverridesRef.current[lbl] ?? 44;
            },
            "height": (ele: CyElement) => {
              const lbl = ele.data("label") as string;
              return sizeOverridesRef.current[lbl] ?? 44;
            },
            "text-wrap": "ellipsis",
            "text-max-width": (ele: CyElement) => {
              const lbl = ele.data("label") as string;
              const sz = sizeOverridesRef.current[lbl] ?? 44;
              return `${sz - 8}px`;
            },
            "border-width": 2,
            "border-color": (ele: CyElement) => {
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
          selector: "node.pinned",
          style: {
            "background-image": PIN_SVG,
            "background-width": "14px",
            "background-height": "14px",
            "background-position-x": "88%",
            "background-position-y": "8%",
            "background-fit": "none",
            "background-clip": "none",
            "background-image-opacity": 1,
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
        {
          selector: "edge[?_metaEdge]",
          style: {
            "line-color": "#6366f1",
            "target-arrow-color": "#6366f1",
            "target-arrow-shape": "triangle",
            "curve-style": "bezier",
            "label": "data(label)",
            "font-size": 9,
            "color": "#a5b4fc",
            "text-background-opacity": 0.7,
            "text-background-color": "#1a1d2e",
            "text-background-padding": "2px",
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            "width": ((ele: any) => Math.min(0.75 + Math.log1p(ele.data("_metaCount") as number) * 0.5, 3)) as any,
            "line-style": "dashed",
            "line-dash-pattern": [6, 3],
          },
        },
        {
          selector: "node[?_cluster]",
          style: {
            "background-opacity": 0,
            "border-width": 0,
            "label": "",
            "padding": "36px",
            "events": "no" as const,
          },
        },
        {
          selector: "node[?_inCluster]",
          style: {
            "width": (ele: CyElement) => {
              const lbl = ele.data("label") as string;
              return (sizeOverridesRef.current[lbl] ?? 44) / 2;
            },
            "height": (ele: CyElement) => {
              const lbl = ele.data("label") as string;
              return (sizeOverridesRef.current[lbl] ?? 44) / 2;
            },
            "font-size": 7,
            "text-max-width": (ele: CyElement) => {
              const lbl = ele.data("label") as string;
              return `${(sizeOverridesRef.current[lbl] ?? 44) / 2 - 4}px`;
            },
          },
        },
        {
          selector: "node[?_collapsed]",
          style: {
            shape: "ellipse" as const,
            "background-color": "data(_color)",
            "background-opacity": 0.85,
            "border-width": 2,
            "border-color": "data(_color)",
            "border-opacity": 1,
            width: 64,
            height: 64,
            "color": "#fff",
            "font-size": 9,
            "text-wrap": "wrap" as const,
            "text-max-width": "56px",
            "text-valign": "center" as const,
            "text-halign": "center" as const,
          },
        },
      ],
      layout: { name: "null" } as CyLayoutOptions,
      minZoom: 0.05,
      maxZoom: 8,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    }) as any as CyInstance;

    cy.on("tap", "node[?_collapsed]", (evt) => {
      const cid = evt.target.data("_clusterId") as string;
      if (cid) toggleCollapse(cid);
    });
    cy.on("tap", "node", (evt) => {
      setNodeCtxMenu(null);
      if (!evt.target.data("_collapsed")) onSelect({ kind: "node", data: evt.target.data("_node") as GNode });
    });
    cy.on("tap", "edge", (evt) => {
      setNodeCtxMenu(null);
      const edgeData = evt.target.data("_edge") as GEdge | undefined;
      if (edgeData) onSelect({ kind: "edge", data: edgeData });
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
    cy.on("layoutstop", computeHulls);
    cy.on("viewport", computeHulls);
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
      evt.target.addClass("pinned");
      nudgeLayoutRef.current();
    });

    cyRef.current = cy;
    onCyReady?.(cy);
    anchoredRef.current = new Set();
    activeLayoutRef.current = null;
    layoutRunningRef.current = false;
    if (els.length > 0) runLayout(layoutModeRef.current);
    return () => {
      cyRef.current = null;
      activeLayoutRef.current = null;
      onCyReady?.(null);
      cy.destroy();
      setHullCircles([]);
    };
  }, [nodes, edges, overlayEdges, clusterLevel, collapsedClusters]); // eslint-disable-line react-hooks/exhaustive-deps

  // Incremental overlay update — adds/removes overlay nodes+edges without full re-layout
  const prevOverlayNodesRef = useRef<Map<string, GNode>>(new Map());
  const prevOverlayEdgesRef = useRef<Map<string, GEdge>>(new Map());
  useEffect(() => {
    if (clusterLevel !== "none") return;
    const cy = cyRef.current;
    if (!cy) return;
    const prevNodes = prevOverlayNodesRef.current;
    const prevEdges = prevOverlayEdgesRef.current;
    // Batch 1: removals + node additions — edges need nodes committed first
    cy.batch(() => {
      // Remove nodes no longer in overlay
      prevNodes.forEach((_, k) => {
        if (!overlayNodes.has(k)) {
          const cyId = `${k.split(":")[0]}:${k.split(":").slice(1).join(":")}`;
          cy.$id(cyId).remove();
          anchoredRef.current.delete(cyId);
        }
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
          // Guard against duplicate edges with different identities but same endpoints+type
          const dupExists =
            cy.edges(`[source="${srcKey}"][target="${tgtKey}"][label="${e.type}"]`).length > 0 ||
            cy.edges(`[source="${tgtKey}"][target="${srcKey}"][label="${e.type}"]`).length > 0;
          if (!dupExists && cy.$id(srcKey).length > 0 && cy.$id(tgtKey).length > 0) {
            cy.add({ group: "edges", data: { id: e.identity, source: srcKey, target: tgtKey, label: e.type, _edge: e } });
          }
        }
      });
    });

    // Arrange circular children in a ring around their parents; lock so layout won't move them
    const newCyIds = new Set<string>();
    overlayNodes.forEach((n, k) => { if (!prevNodes.has(k)) newCyIds.add(`${n.label}:${n.id}`); });
    circularChildParentsRef.current.forEach((parentId) => {
      const parentNode = cy.$id(parentId);
      if (parentNode.length === 0) return;
      const pos = parentNode.position();
      const r = edgeDistanceRef.current;
      const children = parentNode.neighborhood("node").filter((n) => newCyIds.has(n.id() as string));
      if (children.length === 0) return;
      children.forEach((node, i) => {
        const angle = (2 * Math.PI * i) / children.length - Math.PI / 2;
        node.position({ x: pos.x + r * Math.cos(angle), y: pos.y + r * Math.sin(angle) });
        node.lock();
        anchoredRef.current.add(node.id() as string);
        node.addClass("pinned");
      });
    });
    // Arrange circular parents in a ring around their child node; lock so layout won't move them
    circularParentNodesRef.current.forEach((childId) => {
      const childNode = cy.$id(childId);
      if (childNode.length === 0) return;
      const pos = childNode.position();
      const r = edgeDistanceRef.current;
      const parents = childNode.neighborhood("node").filter((n) => newCyIds.has(n.id() as string));
      if (parents.length === 0) return;
      parents.forEach((node, i) => {
        const angle = (2 * Math.PI * i) / parents.length - Math.PI / 2;
        node.position({ x: pos.x + r * Math.cos(angle), y: pos.y + r * Math.sin(angle) });
        node.lock();
        anchoredRef.current.add(node.id() as string);
        node.addClass("pinned");
      });
    });

    const hadNewNodes = [...overlayNodes.keys()].some((k) => !prevNodes.has(k));
    const hadNewEdges = [...overlayEdges.keys()].some((k) => !prevOverlayEdgesRef.current.has(k));
    const allNewAreCircular = hadNewNodes && [...overlayNodes.keys()]
      .filter((k) => !prevNodes.has(k))
      .every((k) => { const n = overlayNodes.get(k)!; return cy.$id(`${n.label}:${n.id}`).locked(); });

    // Position new (non-circular) nodes near their connected parent before nudge
    const newCyIdsForNudge = new Set<string>();
    if (hadNewNodes && !allNewAreCircular) {
      overlayNodes.forEach((n, k) => {
        if (prevNodes.has(k)) return;
        const cyId = `${n.label}:${n.id}`;
        const cyNode = cy.$id(cyId);
        if (cyNode.length === 0 || cyNode.locked()) return;
        newCyIdsForNudge.add(cyId);
        // Find a connected node already on canvas to seed position
        const connected = cyNode.neighborhood("node").filter((nb: CyElement) => !newCyIdsForNudge.has(nb.id() as string));
        if (connected.length > 0) {
          const parentPos = connected[0].position();
          const angle = Math.random() * 2 * Math.PI;
          const dist = edgeDistanceRef.current * (0.8 + Math.random() * 0.4);
          cyNode.position({ x: parentPos.x + dist * Math.cos(angle), y: parentPos.y + dist * Math.sin(angle) });
        }
      });
    }

    prevOverlayNodesRef.current = new Map(overlayNodes);
    prevOverlayEdgesRef.current = new Map(overlayEdges);
    if ((hadNewNodes && !allNewAreCircular) || hadNewEdges) nudgeLayout(newCyIdsForNudge.size > 0 ? newCyIdsForNudge : undefined, true);
  }, [overlayNodes, overlayEdges, clusterLevel]); // eslint-disable-line react-hooks/exhaustive-deps

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
        const base_sz3 = sizeOverridesRef.current[lbl] ?? 44;
        const sz = node.data("_inCluster") ? base_sz3 / 2 : base_sz3;
        node.style({ width: sz, height: sz, "text-max-width": `${sz - 4}px` });
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
    <div
      className="gf-canvas-wrap"
      tabIndex={0}
      onKeyDown={(e) => {
        if ((e.metaKey || e.ctrlKey) && e.key === "a") {
          e.preventDefault();
          cyRef.current?.nodes().select();
        } else if ((e.metaKey || e.ctrlKey) && e.key === "r") {
          e.preventDefault();
          nudgeLayout(undefined, true);
        }
      }}
    >
      <div ref={containerRef} className="gf-canvas" />
      {hullCircles.length > 0 && (
        <svg ref={hullSvgRef} style={{ position: "absolute", inset: 0, width: "100%", height: "100%", pointerEvents: "none" }}>
          {hullCircles.map(({ cid, x, y, r }) => (
            <g key={cid}>
              <circle
                cx={x} cy={y} r={r}
                fill={clusterColor(cid)} fillOpacity={0.1}
                stroke={clusterColor(cid)} strokeWidth={8} strokeOpacity={0}
                style={{ pointerEvents: "stroke", cursor: "grab" }}
                onMouseDown={(e) => {
                  e.preventDefault();
                  hullDragRef.current = { cid, lastX: e.clientX, lastY: e.clientY, startX: e.clientX, startY: e.clientY };
                }}
              />
              <circle cx={x} cy={y} r={r} fill="none" stroke={clusterColor(cid)} strokeWidth={1.5} strokeOpacity={0.75} style={{ pointerEvents: "none" }} />
              <text
                x={x} y={y - r - 6}
                textAnchor="middle" fill={clusterColor(cid)} fontSize={11} fontWeight="bold" fontFamily="sans-serif"
                style={{ pointerEvents: "all", cursor: "pointer", userSelect: "none" }}
                onClick={() => toggleCollapse(cid)}
                {...{ title: "Click to collapse group" } as any}
              >{cid} ⊟</text>
            </g>
          ))}
        </svg>
      )}
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
          onMouseDown={() => {
            nudgeHeldRef.current = true;
            const cy = cyRef.current;
            const sel = cy ? (cy.nodes(":selected") as any).not("[?_cluster]") : null;
            const freeNodes = sel && sel.length > 0
              ? new Set(sel.map((n: any) => n.id() as string) as string[])
              : undefined;
            nudgeLayout(freeNodes, true);
          }}
          onMouseUp={() => { nudgeHeldRef.current = false; }}
          onMouseLeave={() => { nudgeHeldRef.current = false; }}
          title="Nudge layout — nudges selected nodes (or all if none selected); hold to keep iterating"
        >
          ⟳
        </button>
        <div className="gf-ctrl-divider" />
        <label className="gf-ctrl-label" title="Edge length">↔</label>
        <input
          type="range"
          className="gf-ctrl-slider"
          min={40}
          max={400}
          step={10}
          value={edgeDistance}
          onChange={(e) => {
            const v = Number(e.target.value);
            edgeDistanceRef.current = v;
            setEdgeDistance(v);
            localStorage.setItem("provisa.graph.edgeDistance", String(v));
            if (nudgeTimerRef.current) clearTimeout(nudgeTimerRef.current);
            nudgeTimerRef.current = setTimeout(() => nudgeLayout(), 150);
          }}
          title={`Edge length: ${edgeDistance}px`}
        />
      </div>
      {nodeCtxMenu && (() => {
        const ctxNode = nodes.get(nodeCtxMenu.nodeId) ?? overlayNodes.get(nodeCtxMenu.nodeId);
        const ctxLabel = ctxNode?.label ?? "";
        const tableLabel = ctxLabel.includes(":") ? ctxLabel.split(":").pop()! : ctxLabel;
        const ctxPkCols = pkMap[ctxLabel] ?? [];
        const hasPk = ctxPkCols.length > 0;
        const norm = (s: string) => s.toLowerCase().replace(/_/g, "");
        const tl = norm(tableLabel);
        const cl = norm(ctxLabel);
        const myPkKey = (pkMap[ctxLabel] ?? pkMap[tableLabel] ?? []).join(",");
        const siblingTls = myPkKey
          ? Object.entries(pkMap)
            .filter(([lbl, cols]) => cols.join(",") === myPkKey && lbl !== ctxLabel && lbl !== tableLabel)
            .map(([lbl]) => norm(lbl.includes(":") ? lbl.split(":").pop()! : lbl))
          : [];
        const isSource = (r: typeof relationships[0]) =>
          norm(r.sourceTableName) === tl || norm(r.sourceTableName) === cl || siblingTls.includes(norm(r.sourceTableName));
        const isTarget = (r: typeof relationships[0]) =>
          norm(r.targetTableName) === tl || norm(r.targetTableName) === cl || siblingTls.includes(norm(r.targetTableName));
        const hasChildRels = relationships.some(isSource);
        const hasParentRels = relationships.some(isTarget);
        return (
        <div
          ref={menuRef}
          className="gf-node-ctx-menu"
          style={{ left: nodeCtxMenu.x, top: nodeCtxMenu.y, visibility: "hidden" }}
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
          {nodeCtxMenu.selectedNodeIds.some((id) => anchoredRef.current.has(id)) && (
            <button
              className="gf-node-ctx-item"
              onClick={() => {
                const cy = cyRef.current;
                nodeCtxMenu.selectedNodeIds.forEach((id) => {
                  anchoredRef.current.delete(id);
                  if (cy) { cy.$id(id).unlock(); cy.$id(id).removeClass("pinned"); }
                });
                // Stop any in-progress layout and force-reset the gate so nudge can start fresh
                try { activeLayoutRef.current?.stop(); } catch { /* ignore */ }
                activeLayoutRef.current = null;
                layoutRunningRef.current = false;
                setNodeCtxMenu(null);
                nudgeLayoutRef.current();
              }}
            >
              Unfix position
            </button>
          )}
          <button
            className="gf-node-ctx-item"
            disabled={!hasChildRels}
            style={!hasChildRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
            title={hasChildRels ? undefined : "No outgoing relationships for this node type"}
            onClick={() => {
              if (!hasChildRels) return;
              onToggleChildrenBatch(nodeCtxMenu.selectedNodeIds, false);
              setNodeCtxMenu(null);
            }}
          >
            {showingChildrenNatural.has(nodeCtxMenu.nodeId) ? "Hide children" : "Show children"}
          </button>
          <button
            className="gf-node-ctx-item"
            disabled={!hasChildRels}
            style={!hasChildRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
            title={hasChildRels ? "Arrange children in a ring around this node" : "No outgoing relationships for this node type"}
            onClick={() => {
              if (!hasChildRels) return;
              onToggleChildrenBatch(nodeCtxMenu.selectedNodeIds, true);
              setNodeCtxMenu(null);
            }}
          >
            {showingChildrenCircular.has(nodeCtxMenu.nodeId) ? "Hide children (circular)" : "Show children (circular)"}
          </button>
          <button
            className="gf-node-ctx-item"
            disabled={!hasParentRels}
            style={!hasParentRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
            title={hasParentRels ? undefined : "No incoming relationships for this node type"}
            onClick={() => {
              if (!hasParentRels) return;
              onToggleParentsBatch(nodeCtxMenu.selectedNodeIds, false);
              setNodeCtxMenu(null);
            }}
          >
            {showingParents.has(nodeCtxMenu.nodeId) ? "Hide parents" : "Show parents"}
          </button>
          <button
            className="gf-node-ctx-item"
            disabled={!hasParentRels}
            style={!hasParentRels ? { opacity: 0.4, cursor: "not-allowed" } : undefined}
            title={hasParentRels ? "Arrange parents in a ring around this node" : "No incoming relationships for this node type"}
            onClick={() => {
              if (!hasParentRels) return;
              onToggleParentsBatch(nodeCtxMenu.selectedNodeIds, true);
              setNodeCtxMenu(null);
            }}
          >
            {showingParentsCircular.has(nodeCtxMenu.nodeId) ? "Hide parents (circular)" : "Show parents (circular)"}
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
          <div className="gf-node-ctx-submenu-wrap">
            <button className="gf-node-ctx-item gf-node-ctx-item--has-sub">Select</button>
            <div className="gf-node-ctx-submenu">
              <button
                className="gf-node-ctx-item"
                onClick={() => {
                  const cy = cyRef.current;
                  if (cy) cy.nodes().select();
                  onSelect(null);
                  setNodeCtxMenu(null);
                }}
              >
                All
              </button>
              <button
                className="gf-node-ctx-item"
                onClick={() => {
                  const cy = cyRef.current;
                  if (cy) {
                    const targetLabel = ctxNode?.label ?? "";
                    cy.nodes().forEach((n) => {
                      if ((n.data("label") as string) === targetLabel) n.select();
                      else n.unselect();
                    });
                  }
                  onSelect(null);
                  setNodeCtxMenu(null);
                }}
              >
                All of this type
              </button>
              <button
                className="gf-node-ctx-item"
                onClick={() => {
                  const cy = cyRef.current;
                  if (cy) {
                    cy.nodes().unselect();
                    nodeCtxMenu.selectedNodeIds.forEach((id) => {
                      cy.$id(id).neighborhood("node").select();
                    });
                  }
                  onSelect(null);
                  setNodeCtxMenu(null);
                }}
              >
                Connected
              </button>
              <button
                className="gf-node-ctx-item"
                onClick={() => {
                  const cy = cyRef.current;
                  if (cy) {
                    cy.nodes().unselect();
                    nodeCtxMenu.selectedNodeIds.forEach((id) => {
                      // outgoing edges → targets
                      cy.$id(id).neighborhood("edge").forEach((e) => {
                        if ((e.source().id() as string) === id) e.target().select();
                      });
                    });
                  }
                  onSelect(null);
                  setNodeCtxMenu(null);
                }}
              >
                Children
              </button>
              <button
                className="gf-node-ctx-item"
                onClick={() => {
                  const cy = cyRef.current;
                  if (cy) {
                    cy.nodes().unselect();
                    nodeCtxMenu.selectedNodeIds.forEach((id) => {
                      // incoming edges → sources
                      cy.$id(id).neighborhood("edge").forEach((e) => {
                        if ((e.target().id() as string) === id) e.source().select();
                      });
                    });
                  }
                  onSelect(null);
                  setNodeCtxMenu(null);
                }}
              >
                Parents
              </button>
            </div>
          </div>
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
  onTableDrop?: (frameId: string, compoundLabel: string) => void;
  colorOverrides: Record<string, string>;
  sizeOverrides: Record<string, number>;
  labelProperty: Record<string, string>;
  relLineOverrides: Record<string, RelLineOverride>;
  onColorChange: (label: string, color: string) => void;
  pkMap: Record<string, string[]>;
  relationships?: Relationship[];
  schemaRels?: Array<{ type: string; source: string; target: string }>;
  autoImpute?: boolean;
  onSaveEdgeAlias?: (relId: number, cqlAlias: string, gqlAlias: string) => Promise<void>;
}

export function GraphFrame({ frame, onClose, onRerun, onTableDrop, colorOverrides, sizeOverrides, labelProperty, relLineOverrides, onColorChange, pkMap, relationships, schemaRels, autoImpute: autoImputeProp = false, onSaveEdgeAlias }: GraphFrameProps) {
  const [view, setView] = useState<"graph" | "table" | "json">("graph");
  const [selected, setSelected] = useState<{ kind: "node"; data: GNode } | { kind: "edge"; data: GEdge } | null>(null);
  const [collapsed, setCollapsed] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [inspectorWidth, setInspectorWidth] = useState(260);
  const [graphAreaHeight, setGraphAreaHeight] = useState(460);
  const [editQuery, setEditQuery] = useState(frame.query);
  const editQueryRef = useRef(editQuery);
  editQueryRef.current = editQuery;
  useEffect(() => { setEditQuery(frame.query); }, [frame.query]);
  const [overlayData, setOverlayData] = useState<Map<string, { nodes: Map<string, GNode>; edges: Map<string, GEdge> }>>(new Map);
  const [autoImpute, setAutoImpute] = useState(autoImputeProp);
  const [dragOver, setDragOver] = useState(false);
  const handleRerun = useCallback((id: string, query: string) => {
    setOverlayData(new Map());
    onRerun(id, query);
  }, [onRerun]);
  const [showDlMenu, setShowDlMenu] = useState(false);
  const [clusterLevel, setClusterLevel] = useState<ClusterLevel>("none");
  const [tableWrap, setTableWrap] = useState(false);
  const [tableColWidths, setTableColWidths] = useState<number[]>(() => frame.columns.map(() => 180));
  const prevColumnsKey = useRef(frame.columns.join(","));
  if (frame.columns.join(",") !== prevColumnsKey.current) {
    prevColumnsKey.current = frame.columns.join(",");
    setTableColWidths(frame.columns.map(() => 180));
  }
  const canvasCyRef = useRef<CyInstance | null>(null);
  const canvasHullSvgRef = useRef<SVGSVGElement | null>(null);
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

  type MergedOverlay = { nodes: Map<string, GNode>; edges: Map<string, GEdge> };

  const _fetchChildrenForNode = useCallback(async (nodeKey: string): Promise<MergedOverlay | null> => {
    const gNode = _resolveNodeForKey(nodeKey);
    if (!gNode) return null;
    const tableLabel = gNode.label.includes(":") ? gNode.label.split(":").pop()! : gNode.label;
    const pkCols = pkMap[gNode.label] ?? pkMap[tableLabel] ?? [];
    const pkCol = pkCols[0] ?? null;
    const pkValue = gNode.properties[pkCol ?? ""] ?? gNode.id;
    const pkLit = pkValue === null || pkValue === undefined
      ? null
      : isNaN(Number(pkValue)) ? `'${String(pkValue).replace(/'/g, "\\'")}'` : String(pkValue);
    if (!pkLit || !pkCol) return null;
    const norm = (s: string) => s.toLowerCase().replace(/_/g, "");
    const tl = norm(tableLabel);
    const rels = (relationships ?? []).filter((r) => norm(r.sourceTableName) === tl);
    const myPkKey = pkCols.join(",");
    const siblingSourceRels = rels.length === 0 ? (() => {
      const siblingTls = Object.entries(pkMap)
        .filter(([lbl, cols]) => cols.join(",") === myPkKey && lbl !== gNode.label && lbl !== tableLabel)
        .map(([lbl]) => norm(lbl.includes(":") ? lbl.split(":").pop()! : lbl));
      return (relationships ?? []).filter(r => siblingTls.includes(norm(r.sourceTableName)));
    })() : null;
    const effectiveRels = rels.length > 0 ? rels : (siblingSourceRels ?? []);
    const effectiveLabel = rels.length > 0 ? tableLabel : (() => {
      if (!siblingSourceRels || siblingSourceRels.length === 0) return tableLabel;
      const sib = siblingSourceRels[0];
      return Object.keys(pkMap).find(lbl => norm(lbl.includes(":") ? lbl.split(":").pop()! : lbl) === norm(sib.sourceTableName))?.split(":").pop() ?? sib.sourceTableName;
    })();
    if (effectiveRels.length === 0) return null;
    const merged: MergedOverlay = { nodes: new Map(), edges: new Map() };
    await Promise.all(effectiveRels.map(async (r) => {
      const relType = (r.alias ?? r.graphqlAlias ?? "").toUpperCase();
      const q = `MATCH (n:${effectiveLabel})-[r:${relType}]->(child) WHERE n.${pkCol} = ${pkLit} RETURN n, r, child`;
      const result = await _fetchNeighbors(q);
      if (result) { result.nodes.forEach((n, k) => merged.nodes.set(k, n)); result.edges.forEach((e, k) => merged.edges.set(k, e)); }
    }));
    if (siblingSourceRels && siblingSourceRels.length > 0) {
      const sibNodeKey = [...merged.nodes.keys()].find((k) => { const n = merged.nodes.get(k)!; return String(n.id) === String(pkValue) && n.label !== gNode.label; });
      if (sibNodeKey) { merged.nodes.delete(sibNodeKey); merged.edges.forEach((edge) => { if (`${edge.startNode.label}:${edge.startNode.id}` === sibNodeKey) edge.startNode = gNode; }); }
    }
    return (merged.nodes.size > 0 || merged.edges.size > 0) ? merged : null;
  }, [pkMap, relationships, _resolveNodeForKey, _fetchNeighbors]);

  const _fetchParentsForNode = useCallback(async (nodeKey: string): Promise<MergedOverlay | null> => {
    const gNode = _resolveNodeForKey(nodeKey);
    if (!gNode) return null;
    const tableLabel = gNode.label.includes(":") ? gNode.label.split(":").pop()! : gNode.label;
    const pkCols = pkMap[gNode.label] ?? pkMap[tableLabel] ?? [];
    const pkCol = pkCols[0] ?? null;
    const pkValue = gNode.properties[pkCol ?? ""] ?? gNode.id;
    const pkLit = pkValue === null || pkValue === undefined
      ? null
      : isNaN(Number(pkValue)) ? `'${String(pkValue).replace(/'/g, "\\'")}'` : String(pkValue);
    if (!pkLit || !pkCol) return null;
    const norm = (s: string) => s.toLowerCase().replace(/_/g, "");
    const tl = norm(tableLabel);
    const rels = (relationships ?? []).filter((r) => norm(r.targetTableName) === tl);
    const myPkKey = pkCols.join(",");
    const siblingTargetRels = rels.length === 0 ? (() => {
      const siblingTls = Object.entries(pkMap)
        .filter(([lbl, cols]) => cols.join(",") === myPkKey && lbl !== gNode.label && lbl !== tableLabel)
        .map(([lbl]) => norm(lbl.includes(":") ? lbl.split(":").pop()! : lbl));
      return (relationships ?? []).filter(r => siblingTls.includes(norm(r.targetTableName)));
    })() : null;
    const effectiveRels = rels.length > 0 ? rels : (siblingTargetRels ?? []);
    const effectiveLabel = rels.length > 0 ? tableLabel : (() => {
      if (!siblingTargetRels || siblingTargetRels.length === 0) return tableLabel;
      const sib = siblingTargetRels[0];
      return Object.keys(pkMap).find(lbl => norm(lbl.includes(":") ? lbl.split(":").pop()! : lbl) === norm(sib.targetTableName))?.split(":").pop() ?? sib.targetTableName;
    })();
    if (effectiveRels.length === 0) return null;
    const merged: MergedOverlay = { nodes: new Map(), edges: new Map() };
    await Promise.all(effectiveRels.map(async (r) => {
      const relType = (r.alias ?? r.graphqlAlias ?? "").toUpperCase();
      const q = `MATCH (parent)-[r:${relType}]->(n:${effectiveLabel}) WHERE n.${pkCol} = ${pkLit} RETURN n, r, parent`;
      const result = await _fetchNeighbors(q);
      if (result) { result.nodes.forEach((n, k) => merged.nodes.set(k, n)); result.edges.forEach((e, k) => merged.edges.set(k, e)); }
    }));
    if (siblingTargetRels && siblingTargetRels.length > 0) {
      const sibNodeKey = [...merged.nodes.keys()].find((k) => { const n = merged.nodes.get(k)!; return String(n.id) === String(pkValue) && n.label !== gNode.label; });
      if (sibNodeKey) { merged.nodes.delete(sibNodeKey); merged.edges.forEach((edge) => { if (`${edge.endNode.label}:${edge.endNode.id}` === sibNodeKey) edge.endNode = gNode; }); }
    }
    return (merged.nodes.size > 0 || merged.edges.size > 0) ? merged : null;
  }, [pkMap, relationships, _resolveNodeForKey, _fetchNeighbors]);

  const handleToggleChildren = useCallback(async (nodeKey: string) => {
    const overlayKey = `${nodeKey}:children`;
    if (overlayData.has(overlayKey)) { setOverlayData((prev) => { const next = new Map(prev); next.delete(overlayKey); return next; }); return; }
    const merged = await _fetchChildrenForNode(nodeKey);
    if (merged) setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
  }, [overlayData, _fetchChildrenForNode]);

  const handleToggleChildrenCircular = useCallback(async (nodeKey: string) => {
    const overlayKey = `${nodeKey}:children:circular`;
    if (overlayData.has(overlayKey)) { setOverlayData((prev) => { const next = new Map(prev); next.delete(overlayKey); return next; }); return; }
    const merged = await _fetchChildrenForNode(nodeKey);
    if (merged) setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
  }, [overlayData, _fetchChildrenForNode]);

  const handleToggleChildrenBatch = useCallback(async (nodeKeys: string[], circular = false) => {
    const suffix = circular ? ":children:circular" : ":children";
    const toRemove = nodeKeys.filter(id => overlayData.has(`${id}${suffix}`));
    const toAdd = nodeKeys.filter(id => !overlayData.has(`${id}${suffix}`));
    if (toAdd.length === 0) {
      setOverlayData((prev) => { const next = new Map(prev); toRemove.forEach(id => next.delete(`${id}${suffix}`)); return next; });
      return;
    }
    // All nodes fetched in parallel off-screen; single setOverlayData call renders them all at once.
    const results = await Promise.all(toAdd.map(id => _fetchChildrenForNode(id)));
    setOverlayData((prev) => {
      const next = new Map(prev);
      toRemove.forEach(id => next.delete(`${id}${suffix}`));
      toAdd.forEach((id, i) => { if (results[i]) next.set(`${id}${suffix}`, results[i]!); });
      return next;
    });
  }, [overlayData, _fetchChildrenForNode]);

  const handleToggleParents = useCallback(async (nodeKey: string) => {
    const overlayKey = `${nodeKey}:parents`;
    if (overlayData.has(overlayKey)) { setOverlayData((prev) => { const next = new Map(prev); next.delete(overlayKey); return next; }); return; }
    const merged = await _fetchParentsForNode(nodeKey);
    if (merged) setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
  }, [overlayData, _fetchParentsForNode]);

  const handleToggleParentsCircular = useCallback(async (nodeKey: string) => {
    const overlayKey = `${nodeKey}:parents:circular`;
    if (overlayData.has(overlayKey)) { setOverlayData((prev) => { const next = new Map(prev); next.delete(overlayKey); return next; }); return; }
    const merged = await _fetchParentsForNode(nodeKey);
    if (merged) setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
  }, [overlayData, _fetchParentsForNode]);

  const handleToggleParentsBatch = useCallback(async (nodeKeys: string[], circular = false) => {
    const suffix = circular ? ":parents:circular" : ":parents";
    const toRemove = nodeKeys.filter(id => overlayData.has(`${id}${suffix}`));
    const toAdd = nodeKeys.filter(id => !overlayData.has(`${id}${suffix}`));
    if (toAdd.length === 0) {
      setOverlayData((prev) => { const next = new Map(prev); toRemove.forEach(id => next.delete(`${id}${suffix}`)); return next; });
      return;
    }
    const results = await Promise.all(toAdd.map(id => _fetchParentsForNode(id)));
    setOverlayData((prev) => {
      const next = new Map(prev);
      toRemove.forEach(id => next.delete(`${id}${suffix}`));
      toAdd.forEach((id, i) => { if (results[i]) next.set(`${id}${suffix}`, results[i]!); });
      return next;
    });
  }, [overlayData, _fetchParentsForNode]);

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
    // Dedup against frame edges by both identity key and endpoint+type fingerprint
    const frameFingerprints = new Set<string>();
    frame.edges.forEach((e) => {
      frameFingerprints.add(`${e.startNode.label}:${e.startNode.id}→${e.endNode.label}:${e.endNode.id}:${e.type}`);
      // Also store reversed fingerprint so backward-traversal frame edges match canonical imputed edges
      frameFingerprints.add(`${e.endNode.label}:${e.endNode.id}→${e.startNode.label}:${e.startNode.id}:${e.type}`);
    });
    const m = new Map<string, GEdge>();
    for (const d of overlayData.values()) {
      d.edges.forEach((e, k) => {
        if (frame.edges.has(k)) return;
        const fp = `${e.startNode.label}:${e.startNode.id}→${e.endNode.label}:${e.endNode.id}:${e.type}`;
        if (frameFingerprints.has(fp)) return;
        m.set(k, e);
      });
    }
    return m;
  }, [frame.edges, overlayData]);

  const showingChildrenNatural = useMemo(() => new Set(
    Array.from(overlayData.keys()).filter(k => k.endsWith(":children")).map(k => k.slice(0, -":children".length))
  ), [overlayData]);
  const showingChildrenCircular = useMemo(() => new Set(
    Array.from(overlayData.keys()).filter(k => k.endsWith(":children:circular")).map(k => k.slice(0, -":children:circular".length))
  ), [overlayData]);
  const showingParents = useMemo(() => new Set(
    Array.from(overlayData.keys()).filter(k => k.endsWith(":parents")).map(k => k.slice(0, -":parents".length))
  ), [overlayData]);
  const showingParentsCircular = useMemo(() => new Set(
    Array.from(overlayData.keys()).filter(k => k.endsWith(":parents:circular")).map(k => k.slice(0, -":parents:circular".length))
  ), [overlayData]);

  // When autoImpute is turned off, clear its overlay
  useEffect(() => {
    if (!autoImpute) {
      setOverlayData((prev) => { const next = new Map(prev); next.delete("__remaining_rels"); return next; });
    }
  }, [autoImpute]);

  // Run imputation whenever the frame result changes or autoImpute toggles on
  useEffect(() => {
    if (!autoImpute || frame.status !== "done" || frame.nodes.size === 0) return;
    let cancelled = false;
    const queries = buildRemainingRelsQueries(frame.nodes, pkMap, schemaRels);
    if (queries.length === 0) return;
    const merged: { nodes: Map<string, GNode>; edges: Map<string, GEdge> } = { nodes: new Map(), edges: new Map() };
    Promise.all(queries.map(async (q) => {
      const result = await _fetchNeighbors(q);
      if (!cancelled && result) {
        result.nodes.forEach((n, k) => merged.nodes.set(k, n));
        result.edges.forEach((e, k) => merged.edges.set(k, e));
      }
    })).then(() => {
      if (!cancelled && (merged.nodes.size > 0 || merged.edges.size > 0)) {
        setOverlayData((prev) => new Map(prev).set("__remaining_rels", merged));
      }
    });
    return () => { cancelled = true; };
  }, [autoImpute, frame.status, frame.nodes, pkMap, schemaRels, _fetchNeighbors]); // eslint-disable-line react-hooks/exhaustive-deps

  const hasGraph = frame.nodes.size > 0 || frame.edges.size > 0;

  // Properties available for grouping: virtual schema_L1/L2/L3 (mapped to scl1/scl2/scl3)
  // followed by any scalar property with more than one distinct value.
  const groupableAttrs = useMemo(() => {
    if (frame.nodes.size === 0) return [];
    const SKIP = new Set(["scl1", "scl2", "scl3"]);
    const schemaVirtuals: string[] = [];
    for (const [virtName, prop] of [["schema_L1", "scl1"], ["schema_L2", "scl2"], ["schema_L3", "scl3"]] as const) {
      const vals = new Set<string>();
      frame.nodes.forEach((n) => { const v = n.properties[prop]; if (v !== null && v !== undefined) vals.add(String(v)); });
      if (vals.size > 1) schemaVirtuals.push(virtName);
    }
    const counts = new Map<string, Set<string>>();
    frame.nodes.forEach((n) => {
      Object.entries(n.properties).forEach(([k, v]) => {
        if (SKIP.has(k) || v === null || v === undefined) return;
        if (typeof v === "object") return;
        if (!counts.has(k)) counts.set(k, new Set());
        counts.get(k)!.add(String(v));
      });
    });
    const regularAttrs = [...counts.entries()]
      .filter(([, vals]) => vals.size > 1)
      .sort((a, b) => b[1].size - a[1].size)
      .map(([k]) => k);
    return [...schemaVirtuals, ...regularAttrs];
  }, [frame.nodes]);
  const activeView: "graph" | "table" | "json" = hasGraph ? view : (view === "json" ? "json" : "table");

  const renderHeader = (isModal: boolean) => (
    <div className="gf-header">
      <div className="gf-query-editor-wrap">
        <CodeMirror
          className="gf-header-query-input"
          value={editQuery}
          theme={oneDark}
          extensions={[
            ..._gfCypherLangExts,
            _gfCypherLinter({ showErrors: false }),
            EditorView.lineWrapping,
            Prec.highest(keymap.of([{
              key: "Enter",
              run: () => { handleRerun(frame.id, editQuery.trim()); return true; },
            }])),
          ]}
          onChange={(val) => setEditQuery(val)}
          basicSetup={{ lineNumbers: false, foldGutter: false, highlightActiveLine: false }}
        />
        <CopySymbolButton text={editQuery} className="gf-copy-query-btn" title="Copy query" />
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
            className={`gf-icon-btn${autoImpute ? " gf-icon-btn--on" : ""}`}
            onClick={() => setAutoImpute(v => !v)}
            title={autoImpute ? "Auto-impute relationships ON — click to disable" : "Auto-impute relationships between visible nodes"}
          >⊕</button>
        )}
        {hasGraph && frame.status === "done" && groupableAttrs.length > 0 && (
          <select
            className={`gf-attr-select${groupableAttrs.includes(clusterLevel) ? " gf-icon-btn--on" : ""}`}
            value={groupableAttrs.includes(clusterLevel) ? clusterLevel : ""}
            onChange={(e) => setClusterLevel(e.target.value || "none")}
            title="Group nodes by attribute (double-click a hull to collapse; double-click collapsed node to expand)"
          >
            <option value="">⬡ group</option>
            {groupableAttrs.map((a) => <option key={a} value={a}>{a}</option>)}
          </select>
        )}
        {hasGraph && (
          <button className={`gf-view-btn ${activeView === "graph" ? "active" : ""}`}
                  onClick={() => setView("graph")} title="Graph">✦</button>
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
                    _compositeGraphDownload(cy, canvasHullSvgRef.current, "graph.png", "png");
                    setShowDlMenu(false);
                  }}>PNG</button>
                )}
                {hasGraph && activeView === "graph" && (
                  <button className="gf-dl-item" onClick={() => {
                    const cy = canvasCyRef.current;
                    if (!cy) return;
                    _compositeGraphDownload(cy, canvasHullSvgRef.current, "graph.jpg", "jpg");
                    setShowDlMenu(false);
                  }}>JPG</button>
                )}
                {hasGraph && activeView === "graph" && (
                  <button className="gf-dl-item" onClick={() => {
                    const cy = canvasCyRef.current;
                    if (!cy) return;
                    _downloadGraphSvg(cy, canvasHullSvgRef.current);
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
      {frame.status === "error" && (
        <div className="gf-error gf-error--copyable">
          <span className="gf-error-text">{frame.error}</span>
          <CopySymbolButton text={frame.error ?? ""} className="gf-error-copy-btn" title="Copy error" />
        </div>
      )}
      {frame.status !== "error" && hasGraph && (
        <div className="gf-graph-area" style={{ height: graphAreaHeight, display: activeView === "graph" ? undefined : "none" }}>
          <GraphCanvas nodes={frame.nodes} edges={frame.edges} overlayNodes={overlayNodes} overlayEdges={overlayEdges} onSelect={setSelected} colorOverrides={colorOverrides} sizeOverrides={sizeOverrides} labelProperty={labelProperty} relLineOverrides={relLineOverrides} onExcludeNode={handleExcludeNode} pkMap={pkMap} relationships={relationships ?? []} showingChildrenNatural={showingChildrenNatural} onToggleChildren={handleToggleChildren} onToggleChildrenBatch={handleToggleChildrenBatch} showingChildrenCircular={showingChildrenCircular} onToggleChildrenCircular={handleToggleChildrenCircular} showingParents={showingParents} onToggleParents={handleToggleParents} onToggleParentsBatch={handleToggleParentsBatch} showingParentsCircular={showingParentsCircular} onToggleParentsCircular={handleToggleParentsCircular} onCyReady={(cy) => { canvasCyRef.current = cy; }} clusterLevel={clusterLevel} hullSvgRef={canvasHullSvgRef} />
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
      <div
        className={`gf-frame${expanded ? " gf-expanded" : ""}${dragOver ? " gf-frame--drag-over" : ""}`}
        onDragOver={(e) => { if (e.dataTransfer.types.includes("text/x-provisa-label")) { e.preventDefault(); setDragOver(true); } }}
        onDragLeave={() => setDragOver(false)}
        onDrop={(e) => { setDragOver(false); const label = e.dataTransfer.getData("text/x-provisa-label"); if (label && onTableDrop) { e.preventDefault(); onTableDrop(frame.id, label); } }}
      >
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
