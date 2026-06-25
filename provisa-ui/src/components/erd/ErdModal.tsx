// Copyright (c) 2026 Kenneth Stott
// Canary: a3d9e2f1-7b4c-4a8e-9d5f-2c1b6e3a7f8d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useRef, useEffect, useState, useCallback } from "react";
import { createPortal } from "react-dom";
import { X, Download, ChevronDown, ChevronRight, Layers, Maximize2 } from "lucide-react";
import cytoscape from "cytoscape";
import elkRaw from "cytoscape-elk";
import cytoscapeSvgRaw from "cytoscape-svg";
import { buildErdElements } from "./erd-model";
import type { ColumnDetail, ErdNodeDomain, ErdNodeTable } from "./erd-model";
import type { RegisteredTable, Relationship, Domain } from "../../types/admin";
import { labelColor, darkenColor } from "../graph/graph-model";
import { downloadBlob } from "../graph/graph-export";
import type { CyInstance, CyEvent, CyLayoutOptions } from "../graph/cytoscape-types";

// ── cytoscape plugin registration ────────────────────────────────────────────
type CyExt = Parameters<typeof cytoscape.use>[0];
type CyExtModule = { default?: CyExt } | CyExt;
const _interop = (m: CyExtModule): CyExt => (m as { default?: CyExt }).default ?? (m as CyExt);
try { cytoscape.use(_interop(elkRaw as CyExtModule)); } catch { /* already registered */ }
try { cytoscape.use(_interop(cytoscapeSvgRaw as CyExtModule)); } catch { /* already registered */ }

type Pt = { x: number; y: number };

// Returns the set of node IDs that have at least one edge (any kind).
// True orphans (no edges) are excluded from fCoSE and placed in a grid instead.
function nodesWithEdges(cy: CyInstance): Set<string> {
  const ids = new Set<string>();
  cy.edges(".erd-rel").forEach((e) => {
    ids.add(e.data("source") as string);
    ids.add(e.data("target") as string);
  });
  return ids;
}

// Push overlapping compound (.erd-domain) nodes apart so they don't visually
// overlap. Translates children of the "right/bottom" compound in each pair.
// Must be called after all children have been positioned (including orphan grid).
function resolveCompoundOverlaps(cy: CyInstance): void {
  type BB = { x1: number; y1: number; x2: number; y2: number };
  const PAD = 60;
  const MAX_ITER = 20;

  const domainArr: ReturnType<typeof cy.nodes>[number][] = [];
  cy.nodes(".erd-domain").forEach((d) => {
    if (!(d.children() as unknown as { empty(): boolean }).empty()) domainArr.push(d);
  });
  if (domainArr.length < 2) return;

  for (let iter = 0; iter < MAX_ITER; iter++) {
    const bbs = domainArr.map((d) =>
      (d as unknown as { boundingBox(o: object): BB }).boundingBox({ includeLabels: true })
    );
    let moved = false;

    for (let i = 0; i < domainArr.length; i++) {
      for (let j = i + 1; j < domainArr.length; j++) {
        const a = bbs[i];
        const b = bbs[j];
        const overlapX = Math.min(a.x2, b.x2) - Math.max(a.x1, b.x1) + PAD;
        const overlapY = Math.min(a.y2, b.y2) - Math.max(a.y1, b.y1) + PAD;
        if (overlapX <= 0 || overlapY <= 0) continue;

        // Push j away from i along the axis with the smaller overlap.
        let dx = 0, dy = 0;
        if (overlapX <= overlapY) {
          dx = b.x1 < a.x1 ? -overlapX : overlapX;
        } else {
          dy = b.y1 < a.y1 ? -overlapY : overlapY;
        }
        domainArr[j].children().forEach((child) => {
          const p = (child as { position(): Pt }).position();
          (child as { position(p: Pt): void }).position({ x: p.x + dx, y: p.y + dy });
        });
        // Update j's bbox in place for subsequent pairs this iteration.
        bbs[j] = { x1: b.x1 + dx, y1: b.y1 + dy, x2: b.x2 + dx, y2: b.y2 + dy };
        moved = true;
      }
    }
    if (!moved) break;
  }
}

// Phase 2: place isolated nodes (no cross-domain edges) in a compact grid
// below each domain's post-layout bounding box. domainBboxes must be computed
// BEFORE isolated nodes are shown (while only connected nodes are visible),
// so the bbox reflects only the connected-node region.
function placeIsolatedGrid(
  cy: CyInstance,
  isolatedIds: Set<string>,
  domainBboxes: Map<string, { x1: number; x2: number; y2: number }>,
): void {
  const PAD = 20;
  cy.nodes(".erd-domain").forEach((domain) => {
    const domainId = (domain as { id(): string }).id();
    const isolated: Array<{ id: string }> = [];
    domain.children().forEach((n) => {
      if (isolatedIds.has((n as { id(): string }).id())) isolated.push({ id: (n as { id(): string }).id() });
    });
    if (isolated.length === 0) return;

    // Measure actual node sizes (nodes are visible at this point).
    type NS = { w: number; h: number };
    const sizes: NS[] = isolated.map(({ id }) => {
      const n = cy.getElementById(id) as unknown as { width(): number; height(): number };
      return { w: n.width(), h: n.height() };
    });
    const cols = Math.ceil(Math.sqrt(isolated.length));
    const rows = Math.ceil(isolated.length / cols);
    const colWidths = Array.from({ length: cols }, (_, c) =>
      Math.max(...isolated.map((_, i) => i % cols === c ? sizes[i].w : 0))
    );
    const rowHeights = Array.from({ length: rows }, (_, r) =>
      Math.max(...isolated.map((_, i) => Math.floor(i / cols) === r ? sizes[i].h : 0))
    );
    const colX = colWidths.reduce<number[]>((acc, _w, i) => {
      acc.push(i === 0 ? 0 : acc[i - 1] + colWidths[i - 1] + PAD); return acc;
    }, []);
    const rowY = rowHeights.reduce<number[]>((acc, _h, i) => {
      acc.push(i === 0 ? 0 : acc[i - 1] + rowHeights[i - 1] + PAD); return acc;
    }, []);
    const totalW = colX[cols - 1] + colWidths[cols - 1];

    // Use the pre-captured connected-only bbox. If the domain had no connected
    // nodes, fall back to the domain node's own position.
    const dbb = domainBboxes.get(domainId);
    let gridOriginX: number, gridOriginY: number;
    if (dbb) {
      gridOriginX = (dbb.x1 + dbb.x2) / 2 - totalW / 2;
      gridOriginY = dbb.y2 + PAD * 2;
    } else {
      const dpos = (domain as unknown as { position(): Pt }).position();
      gridOriginX = dpos.x - totalW / 2;
      gridOriginY = dpos.y;
    }

    cy.batch(() => {
      isolated.forEach(({ id }, i) => {
        const col = i % cols;
        const row = Math.floor(i / cols);
        const newPos = {
          x: gridOriginX + colX[col] + colWidths[col] / 2,
          y: gridOriginY + rowY[row] + rowHeights[row] / 2,
        };
        (cy.getElementById(id) as unknown as { position(p: Pt): void }).position(newPos);
      });
    });
  });
}

// ── stylesheet ────────────────────────────────────────────────────────────────
function buildErdStylesheet() {
  return [
    {
      selector: "node",
      style: { "text-wrap": "wrap", "font-family": "monospace" },
    },
    {
      selector: ".erd-domain",
      style: {
        shape: "roundrectangle",
        "background-color": (ele: { data(k: string): unknown }) =>
          labelColor(ele.data("domainId") as string),
        "background-opacity": 0.13,
        "border-color": (ele: { data(k: string): unknown }) =>
          labelColor(ele.data("domainId") as string),
        "border-width": 2,
        "border-style": "solid",
        label: (ele: { data(k: string): unknown }) => ele.data("label") as string,
        "text-valign": "top",
        "text-halign": "center",
        color: "#e2e8f0",
        "font-size": 13,
        "font-weight": "bold",
        padding: "32px",
        "compound-sizing-wrt-labels": "include",
        "min-width": 120,
        "min-height": 80,
      },
    },
    {
      selector: ".erd-table",
      style: {
        shape: "rectangle",
        "background-color": "#1e293b",
        "border-color": (ele: { data(k: string): unknown }) =>
          darkenColor(labelColor(ele.data("domainId") as string), 1.2),
        "border-width": 1,
        label: (ele: { data(k: string): unknown }) => ele.data("displayLabel") as string,
        "text-valign": "center",
        "text-halign": "center",
        "text-justification": "left",
        color: "#e2e8f0",
        "font-size": 10,
        "text-wrap": "wrap",
        width: 170,
        height: (ele: { data(k: string): unknown }) =>
          Math.max(24, ((ele.data("lineCount") as number) ?? 1) * 13 + 6),
      },
    },
    {
      selector: ".erd-table:selected",
      style: { "border-color": "#60a5fa", "border-width": 2 },
    },
    {
      selector: ".erd-rel",
      style: {
        "curve-style": "bezier",
        "line-color": "#475569",
        width: 1.5,
        "target-arrow-color": "#475569",
        "target-arrow-shape": "triangle",
        "source-arrow-color": "#475569",
        "source-arrow-shape": (ele: { data(k: string): unknown }) =>
          (ele.data("cardinality") as string) === "many_to_many" ||
          (ele.data("cardinality") as string) === "many_to_one"
            ? "triangle"
            : "none",
        label: (ele: { data(k: string): unknown }) => ele.data("label") as string,
        "font-size": 9,
        color: "#94a3b8",
        "text-rotation": "none",
        "text-margin-y": (ele: { data(k: string): unknown }) => {
          const label = (ele.data("label") as string) ?? "";
          const hash = label.split("").reduce((s: number, c: string) => s + c.charCodeAt(0), 0);
          return (hash % 3 - 1) * 14;
        },
        "text-background-color": "#1e293b",
        "text-background-opacity": 1,
        "text-background-padding": "3px",
      },
    },
    {
      // proxy edges (collapsed-domain → table/domain) rendered dashed
      selector: ".erd-rel--proxy",
      style: {
        "line-style": "dashed",
        "line-dash-pattern": [6, 3],
        "line-color": "#334155",
        "target-arrow-color": "#334155",
        "source-arrow-color": "#334155",
        color: "#475569",
      },
    },
  ];
}

// ── small toolbar-button helper ───────────────────────────────────────────────
function TBtn({
  onClick, title, active, children,
}: {
  onClick: () => void;
  title?: string;
  active?: boolean;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      title={title}
      style={{
        padding: "2px 8px",
        fontSize: 11,
        background: active ? "#334155" : "transparent",
        color: active ? "#e2e8f0" : "#64748b",
        border: "1px solid #334155",
        borderRadius: 4,
        cursor: "pointer",
        display: "flex",
        alignItems: "center",
        gap: 4,
      }}
    >
      {children}
    </button>
  );
}

// ── types ─────────────────────────────────────────────────────────────────────
interface TooltipState {
  visible: boolean;
  x: number;
  y: number;
  title: string;
  body: string;
}

interface ErdModalProps {
  tables: RegisteredTable[];
  relationships: Relationship[];
  domains: Domain[];
  activeDomain: string | null;
  onClose: () => void;
}

// ── component ─────────────────────────────────────────────────────────────────
export function ErdModal({ tables, relationships, domains, activeDomain, onClose }: ErdModalProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<CyInstance | null>(null);
  const domainPickerRef = useRef<HTMLDivElement>(null);
  // nodeId → pinned model-space position (no Cytoscape lock — nodes stay draggable)
  const pinnedNodesRef = useRef<Map<string, { x: number; y: number }>>(new Map());

  const [columnDetail, setColumnDetail] = useState<ColumnDetail>(
    () => (localStorage.getItem("erd.columnDetail") as ColumnDetail | null) ?? "key",
  );
  const [edgeRouting, setEdgeRouting] = useState<"bezier" | "taxi">(
    () => (localStorage.getItem("erd.edgeRouting") as "bezier" | "taxi" | null) ?? "bezier",
  );
  const edgeRoutingRef = useRef<"bezier" | "taxi">(
    (localStorage.getItem("erd.edgeRouting") as "bezier" | "taxi" | null) ?? "bezier",
  );
  const [gridSnap, setGridSnap] = useState<number>(
    () => parseInt(localStorage.getItem("erd.gridSnap") ?? "0", 10),
  );
  const gridSnapRef = useRef<number>(parseInt(localStorage.getItem("erd.gridSnap") ?? "0", 10));
  const [showOrphans, setShowOrphans] = useState(
    () => localStorage.getItem("erd.showOrphans") !== "false",
  );
  const showOrphansRef = useRef(showOrphans);
  useEffect(() => { showOrphansRef.current = showOrphans; }, [showOrphans]);
  const isolatedIdsRef = useRef<Set<string>>(new Set());
  const connectedDomainBboxesRef = useRef<Map<string, { x1: number; x2: number; y2: number }>>(new Map());
  const isDraggingRef = useRef(false);
  const [collapsedDomains, setCollapsedDomains] = useState<Set<string>>(new Set());
  const [hiddenDomains, setHiddenDomains] = useState<Set<string>>(() => {
    const stored = localStorage.getItem("erd.hiddenDomains");
    if (stored !== null) return new Set(JSON.parse(stored) as string[]);
    // Default: hide meta and ops domains
    const allIds = (activeDomain ? tables.filter((t) => t.domainId === activeDomain) : tables)
      .map((t) => t.domainId);
    return new Set(allIds.filter((id) => /meta|ops/i.test(id)));
  });
  const [showDomainPicker, setShowDomainPicker] = useState(false);
  const [tooltip, setTooltip] = useState<TooltipState>({
    visible: false, x: 0, y: 0, title: "", body: "",
  });

  const [hoveredDomainId, setHoveredDomainId] = useState<string | null>(null);
  const [resizeHandleBox, setResizeHandleBox] = useState<{ x: number; y: number; w: number; h: number } | null>(null);
  const handleHoverRef = useRef(false);
  const resizeDragRef = useRef<{
    corner: "se" | "sw" | "ne" | "nw";
    startMouseX: number;
    startMouseY: number;
    startBox: { x: number; y: number; w: number; h: number };
  } | null>(null);

  const updateHandleBox = useCallback((domainId: string | null) => {
    if (!domainId || !cyRef.current || !containerRef.current) {
      setResizeHandleBox(null);
      return;
    }
    const node = cyRef.current.getElementById(`d:${domainId}`);
    if (!node || (node as unknown as { empty(): boolean }).empty()) { setResizeHandleBox(null); return; }
    const bb = (node as unknown as { renderedBoundingBox(opts: object): { x1: number; y1: number; w: number; h: number } })
      .renderedBoundingBox({ includeLabels: false });
    const rect = containerRef.current.getBoundingClientRect();
    setResizeHandleBox({ x: rect.left + bb.x1, y: rect.top + bb.y1, w: bb.w, h: bb.h });
  }, []);

  // Keep refs in sync so Cytoscape event handlers always see current values.
  useEffect(() => { gridSnapRef.current = gridSnap; }, [gridSnap]);
  useEffect(() => { edgeRoutingRef.current = edgeRouting; }, [edgeRouting]);

  // Persist toolbar choices across sessions.
  useEffect(() => { localStorage.setItem("erd.columnDetail", columnDetail); }, [columnDetail]);
  useEffect(() => { localStorage.setItem("erd.edgeRouting", edgeRouting); }, [edgeRouting]);
  useEffect(() => { localStorage.setItem("erd.gridSnap", String(gridSnap)); }, [gridSnap]);
  useEffect(() => { localStorage.setItem("erd.showOrphans", String(showOrphans)); }, [showOrphans]);
  useEffect(() => {
    localStorage.setItem("erd.hiddenDomains", JSON.stringify([...hiddenDomains]));
  }, [hiddenDomains]);

  // All domain IDs present in the scoped table list (before hiding).
  const allDomainIds = [
    ...new Set(
      (activeDomain ? tables.filter((t) => t.domainId === activeDomain) : tables).map(
        (t) => t.domainId,
      ),
    ),
  ];

  // Close picker on outside click.
  useEffect(() => {
    if (!showDomainPicker) return;
    const handler = (e: MouseEvent) => {
      if (domainPickerRef.current && !domainPickerRef.current.contains(e.target as Node)) {
        setShowDomainPicker(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [showDomainPicker]);

  // ── initialise / rebuild on structural changes ────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return;
    const visibleTables = tables;
    const elements = buildErdElements(
      visibleTables, relationships, domains,
      collapsedDomains, hiddenDomains, columnDetail, activeDomain,
    );
    const allEls = [...elements.nodes, ...elements.edges] as unknown[];

    const cy = cytoscape({
      container: containerRef.current,
      elements: allEls as Parameters<typeof cytoscape>[0]["elements"],
      style: buildErdStylesheet() as unknown as Parameters<typeof cytoscape>[0]["style"],
      // Layout is run manually below, after all handlers are registered,
      // so that the layoutstop handler fires on the initial layout.
    }) as unknown as CyInstance;

    cyRef.current = cy;

    cy.on("grabon", "node", () => { isDraggingRef.current = true; });

    // Two-phase post-layout: place orphan grid below each domain's connected bbox.
    cy.on("layoutstop", () => {
      if (isDraggingRef.current) return;
      if (gridSnapRef.current > 0) {
        const g = gridSnapRef.current;
        cy.nodes(".erd-table").forEach((n) => {
          const p = (n as { position(): { x: number; y: number } }).position();
          (n as { position(p: { x: number; y: number }): void }).position({
            x: Math.round(p.x / g) * g,
            y: Math.round(p.y / g) * g,
          });
        });
        // Align connected pairs that are within 1 grid step in x or y — eliminates
        // unnecessary bends in right-angle edge routing after fCoSE snap jitter.
        const ALIGN_THRESHOLD = g;
        cy.edges(".erd-rel").forEach((edge) => {
          const s = cy.getElementById(edge.data("source") as string) as unknown as { position(): Pt; position(p: Pt): void };
          const t = cy.getElementById(edge.data("target") as string) as unknown as { position(): Pt; position(p: Pt): void };
          const sp = s.position();
          const tp = t.position();
          const dx = Math.abs(sp.x - tp.x);
          const dy = Math.abs(sp.y - tp.y);
          if (dx <= ALIGN_THRESHOLD && dx < dy) {
            const ax = Math.round(((sp.x + tp.x) / 2) / g) * g;
            s.position({ ...sp, x: ax });
            t.position({ ...tp, x: ax });
          } else if (dy <= ALIGN_THRESHOLD && dy < dx) {
            const ay = Math.round(((sp.y + tp.y) / 2) / g) * g;
            s.position({ ...sp, y: ay });
            t.position({ ...tp, y: ay });
          }
        });
      }

      // Collapsed domain leaf nodes placed inside an expanded compound by fCoSE render
      // behind its background and become invisible. Move them outside.
      cy.nodes(".erd-domain").forEach((leafDomain) => {
        if (!(leafDomain.children() as unknown as { empty(): boolean }).empty()) return;
        cy.nodes(".erd-domain").forEach((compound) => {
          if (compound.id() === leafDomain.id()) return;
          if ((compound.children() as unknown as { empty(): boolean }).empty()) return;
          const cbb = (compound as unknown as { boundingBox(o: object): { x1: number; x2: number; y1: number; y2: number } })
            .boundingBox({});
          const pos = (leafDomain as { position(): { x: number; y: number } }).position();
          if (pos.x > cbb.x1 && pos.x < cbb.x2 && pos.y > cbb.y1 && pos.y < cbb.y2) {
            (leafDomain as { position(p: { x: number; y: number }): void })
              .position({ x: cbb.x2 + 120, y: (cbb.y1 + cbb.y2) / 2 });
          }
        });
      });

      // Capture per-domain bboxes NOW — true orphans are still hidden (display:none),
      // so the compound bbox reflects only edge-connected nodes.
      const edgeConnectedNow = nodesWithEdges(cy);
      const domainBboxes = new Map<string, { x1: number; x2: number; y2: number }>();
      cy.nodes(".erd-domain").forEach((domain) => {
        let hasConnected = false;
        domain.children().forEach((n) => {
          if (edgeConnectedNow.has((n as { id(): string }).id())) hasConnected = true;
        });
        if (!hasConnected) return;
        const bb = (domain as unknown as { boundingBox(o: object): { x1: number; x2: number; y2: number } })
          .boundingBox({ includeLabels: false });
        domainBboxes.set((domain as { id(): string }).id(), bb);
      });

      // Phase 2: place orphan grid, then apply showOrphans visibility.
      const edgeConnected = nodesWithEdges(cy);
      const isolatedIds = new Set(
        cy.nodes(".erd-table")
          .filter((n) => !edgeConnected.has((n as { id(): string }).id()))
          .map((n) => (n as { id(): string }).id())
      );
      // Store for the showOrphans toggle effect (avoids re-running fCoSE on toggle).
      isolatedIdsRef.current = isolatedIds;
      connectedDomainBboxesRef.current = domainBboxes;

      placeIsolatedGrid(cy, isolatedIds, domainBboxes);

      // Re-snap all nodes to grid after post-processing moves.
      if (gridSnapRef.current > 0) {
        const g = gridSnapRef.current;
        cy.nodes(".erd-table").forEach((n) => {
          const p = (n as { position(): Pt }).position();
          (n as { position(p: Pt): void }).position({
            x: Math.round(p.x / g) * g,
            y: Math.round(p.y / g) * g,
          });
        });
      }

      // Push connected nodes apart on each axis independently.
      // Primary axis gets MIN_LABEL_GAP; secondary axis gets 0 (just clear overlap).
      // Runs after re-snap so the re-snap cannot undo the push.
      // Uses Math.ceil so tiny gaps always produce at least one grid step of movement.
      {
        type BB = { x1: number; x2: number; y1: number; y2: number };
        const MIN_LABEL_GAP = 60;
        const g = gridSnapRef.current > 0 ? gridSnapRef.current : 1;
        cy.edges(".erd-rel").forEach((edge) => {
          const s = cy.getElementById(edge.data("source") as string) as unknown as { position(): Pt; position(p: Pt): void; boundingBox(o: object): BB };
          const t = cy.getElementById(edge.data("target") as string) as unknown as { position(): Pt; position(p: Pt): void; boundingBox(o: object): BB };
          let sp = s.position();
          let tp = t.position();
          const sbb = s.boundingBox({});
          const tbb = t.boundingBox({});
          const xSep = tp.x > sp.x ? tbb.x1 - sbb.x2 : sbb.x1 - tbb.x2;
          const ySep = tp.y > sp.y ? tbb.y1 - sbb.y2 : sbb.y1 - tbb.y2;
          const primaryY = Math.abs(sp.x - tp.x) <= Math.abs(sp.y - tp.y);
          // Secondary axis is only pushed when there is actual 2D overlap (both seps negative).
          // This avoids splitting nodes that share a row/column but don't visually overlap.
          const has2dOverlap = xSep < 0 && ySep < 0;

          // Y axis: MIN_LABEL_GAP if primary; clear 2D overlap only on secondary.
          if (primaryY ? ySep < MIN_LABEL_GAP : (has2dOverlap && ySep < 0)) {
            const yNeeded = primaryY ? MIN_LABEL_GAP : 0;
            const push = Math.ceil((yNeeded - ySep) / 2 / g) * g;
            if (tp.y > sp.y) {
              s.position({ ...sp, y: sp.y - push });
              t.position({ ...tp, y: tp.y + push });
            } else {
              s.position({ ...sp, y: sp.y + push });
              t.position({ ...tp, y: tp.y - push });
            }
            sp = s.position();
            tp = t.position();
          }

          // X axis: MIN_LABEL_GAP if primary; clear 2D overlap only on secondary.
          if (!primaryY ? xSep < MIN_LABEL_GAP : (has2dOverlap && xSep < 0)) {
            const xNeeded = !primaryY ? MIN_LABEL_GAP : 0;
            const push = Math.ceil((xNeeded - xSep) / 2 / g) * g;
            if (tp.x > sp.x) {
              s.position({ ...sp, x: sp.x - push });
              t.position({ ...tp, x: tp.x + push });
            } else {
              s.position({ ...sp, x: sp.x + push });
              t.position({ ...tp, x: tp.x - push });
            }
          }
        });
      }

      // Refresh per-domain bboxes after gap enforcement moved connected nodes,
      // then re-place orphan grid so it lands below the final connected positions.
      {
        type BBxy = { x1: number; x2: number; y2: number };
        const freshBboxes = new Map<string, BBxy>();
        cy.nodes(".erd-domain").forEach((domain) => {
          let x1 = Infinity, x2 = -Infinity, y2 = -Infinity, hasConnected = false;
          domain.children().forEach((child) => {
            if (isolatedIds.has((child as { id(): string }).id())) return;
            const bb = (child as unknown as { boundingBox(o: object): { x1: number; x2: number; y1: number; y2: number } }).boundingBox({});
            x1 = Math.min(x1, bb.x1); x2 = Math.max(x2, bb.x2); y2 = Math.max(y2, bb.y2);
            hasConnected = true;
          });
          if (hasConnected) freshBboxes.set((domain as { id(): string }).id(), { x1, x2, y2 });
        });
        connectedDomainBboxesRef.current = freshBboxes;
        placeIsolatedGrid(cy, isolatedIds, freshBboxes);
      }

      // Apply final orphan visibility without triggering re-layout.
      if (showOrphansRef.current) {
        cy.nodes(".erd-table").style("display", "element");
      } else {
        isolatedIds.forEach((id) => {
          (cy.getElementById(id) as unknown as { style(k: string, v: string): void }).style("display", "none");
        });
      }

      // Resolve compound overlaps AFTER orphans are shown so compound bboxes
      // include the full orphan grid area (hidden nodes are excluded from bbox).
      resolveCompoundOverlaps(cy);

      cy.fit(undefined, 10);

      // Apply current edge routing (handles rebuilds triggered by showOrphans etc.)
      cy.$(".erd-rel").style("curve-style", edgeRoutingRef.current);
      if (edgeRoutingRef.current === "taxi") {
        const seenPairs = new Set<string>();
        cy.$(".erd-rel").forEach((edge) => {
          const s = edge.data("source") as string;
          const t = edge.data("target") as string;
          const key = [s, t].sort().join("↔");
          if (seenPairs.has(key)) {
            edge.style("display", "none");
          } else {
            seenPairs.add(key);
            edge.style("display", "element");
          }
        });
      }
    });

    cy.on("tap", ".erd-domain", (evt: CyEvent) => {
      const domainId = evt.target.data("domainId") as string;
      setCollapsedDomains((prev) => {
        const next = new Set(prev);
        if (next.has(domainId)) next.delete(domainId); else next.add(domainId);
        return next;
      });
    });

    cy.on("mouseover", "node", (evt: CyEvent) => {
      const type = evt.target.data("type") as string;
      let title = "";
      let body = "";
      if (type === "domain") {
        title = evt.target.data("label") as string;
        body = (evt.target.data("description") as string) || "";
        setTooltip({
          visible: !!title,
          x: (containerRef.current?.getBoundingClientRect().left ?? 0) + (evt.renderedPosition ?? evt.position).x + 12,
          y: (containerRef.current?.getBoundingClientRect().top ?? 0) + (evt.renderedPosition ?? evt.position).y + 12,
          title,
          body,
        });
        setHoveredDomainId(evt.target.data("domainId") as string);
        updateHandleBox(evt.target.data("domainId") as string);
        return;
      } else if (type === "table") {
        title = evt.target.data("tableName") as string;
        body = (evt.target.data("description") as string) || "";
      }
      if (title) {
        const pos = evt.renderedPosition ?? evt.position;
        const rect = containerRef.current?.getBoundingClientRect();
        setTooltip({
          visible: true,
          x: (rect?.left ?? 0) + pos.x + 12,
          y: (rect?.top ?? 0) + pos.y + 12,
          title,
          body,
        });
      }
    });

    cy.on("mouseout", "node", () => {
      setTooltip((t) => ({ ...t, visible: false }));
      if (!resizeDragRef.current && !handleHoverRef.current) {
        setHoveredDomainId(null);
        setResizeHandleBox(null);
      }
    });

    cy.on("dragfree", "node", (evt: CyEvent) => {
      isDraggingRef.current = false;
      const id = evt.target.id() as string;
      // Only pin table nodes — compound domain nodes have unreliable positions
      // on fresh cy instances and corrupt fixedNodeConstraint on next rebuild.
      if (!id.startsWith("t:")) return;
      const rawX = evt.target.position("x") as number;
      const rawY = evt.target.position("y") as number;
      let pos = { x: rawX, y: rawY };
      if (gridSnapRef.current > 0) {
        const g = gridSnapRef.current;
        pos = { x: Math.round(rawX / g) * g, y: Math.round(rawY / g) * g };
        (evt.target as { position(p: { x: number; y: number }): void }).position(pos);
      }
      pinnedNodesRef.current.set(id, pos);
    });

    // Update resize handles when cy viewport changes
    cy.on("pan zoom", () => {
      setHoveredDomainId((id) => { updateHandleBox(id); return id; });
    });

    // Hide true orphans before layout so ELK only places connected nodes.
    // layoutstop shows them and places them in a per-domain grid below.
    const connectedBeforeLayout = nodesWithEdges(cy);
    cy.nodes(".erd-table").forEach((n) => {
      if (!connectedBeforeLayout.has((n as { id(): string }).id())) {
        n.style("display", "none");
      }
    });

    cy.layout({
      name: "elk",
      animate: false,
      elk: {
        algorithm: "layered",
        "elk.direction": "UP",
        "elk.hierarchyHandling": "INCLUDE_CHILDREN",
        "elk.layered.spacing.nodeNodeBetweenLayers": "150",
        "elk.spacing.nodeNode": "60",
        "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
        "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
        "elk.padding": "[top=40,left=20,bottom=20,right=20]",
      },
    } as CyLayoutOptions).run();

    return () => { cy.destroy(); cyRef.current = null; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tables, relationships, domains, collapsedDomains, hiddenDomains, columnDetail, activeDomain]);

  // Toggle orphan visibility without re-running fCoSE.
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    const isolatedIds = isolatedIdsRef.current;
    if (showOrphans) {
      // Recompute connected bbox from current positions so resolveCompoundOverlaps
      // translations don't stale the ref.
      type BBxy = { x1: number; x2: number; y2: number };
      const liveBboxes = new Map<string, BBxy>();
      cy.nodes(".erd-domain").forEach((domain) => {
        let x1 = Infinity, x2 = -Infinity, y2 = -Infinity, hasConnected = false;
        domain.children().forEach((child) => {
          if (isolatedIds.has((child as { id(): string }).id())) return;
          const bb = (child as unknown as { boundingBox(o: object): { x1: number; x2: number; y1: number; y2: number; w: number } }).boundingBox({});
          if (!bb.w) return;
          x1 = Math.min(x1, bb.x1); x2 = Math.max(x2, bb.x2); y2 = Math.max(y2, bb.y2);
          hasConnected = true;
        });
        if (hasConnected) liveBboxes.set((domain as { id(): string }).id(), { x1, x2, y2 });
      });
      placeIsolatedGrid(cy, isolatedIds, liveBboxes);
      isolatedIds.forEach((id) => {
        (cy.getElementById(id) as unknown as { style(k: string, v: string): void }).style("display", "element");
      });
    } else {
      isolatedIds.forEach((id) => {
        (cy.getElementById(id) as unknown as { style(k: string, v: string): void }).style("display", "none");
      });
    }
    cy.fit(undefined, 10);
  }, [showOrphans]);

  // ── snap-to-grid change: snap current positions in-place (no re-layout) ─
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy || gridSnap === 0) return;
    const g = gridSnap;
    const nodes = cy.nodes(".erd-table");
    if ((nodes as { empty(): boolean }).empty()) return;
    cy.batch(() => {
      nodes.forEach((n) => {
        const p = (n as { position(): { x: number; y: number } }).position();
        (n as { position(p: { x: number; y: number }): void }).position({
          x: Math.round(p.x / g) * g,
          y: Math.round(p.y / g) * g,
        });
      });
    });
  }, [gridSnap]);

  // ── edge routing update (no rebuild) ────────────────────────────────────
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.batch(() => {
      cy.$(".erd-rel").style("curve-style", edgeRouting);

      if (edgeRouting === "taxi") {
        // Collapse parallel / complementary edges to a single edge per undirected pair.
        const seenPairs = new Set<string>();
        cy.$(".erd-rel").forEach((edge) => {
          const src = edge.data("source") as string;
          const tgt = edge.data("target") as string;
          const key = [src, tgt].sort().join("↔");
          if (seenPairs.has(key)) {
            edge.style("display", "none");
          } else {
            seenPairs.add(key);
            edge.style("display", "element");
          }
        });
      } else {
        cy.$(".erd-rel").style("display", "element");
      }
    });
  }, [edgeRouting]);

  // ── resize pointer handlers ──────────────────────────────────────────────
  const onResizePointerDown = (
    corner: "se" | "sw" | "ne" | "nw",
    e: React.PointerEvent<HTMLDivElement>,
  ) => {
    if (!resizeHandleBox) return;
    e.stopPropagation();
    e.currentTarget.setPointerCapture(e.pointerId);
    resizeDragRef.current = {
      corner,
      startMouseX: e.clientX,
      startMouseY: e.clientY,
      startBox: { ...resizeHandleBox },
    };
  };

  const onResizePointerMove = (e: React.PointerEvent<HTMLDivElement>) => {
    const drag = resizeDragRef.current;
    if (!drag || !resizeHandleBox) return;
    const dx = e.clientX - drag.startMouseX;
    const dy = e.clientY - drag.startMouseY;
    const b = drag.startBox;

    let { x, y, w, h } = b;
    if (drag.corner === "se") { w = Math.max(80, b.w + dx); h = Math.max(60, b.h + dy); }
    else if (drag.corner === "sw") { x = b.x + dx; w = Math.max(80, b.w - dx); h = Math.max(60, b.h + dy); }
    else if (drag.corner === "ne") { y = b.y + dy; w = Math.max(80, b.w + dx); h = Math.max(60, b.h - dy); }
    else if (drag.corner === "nw") { x = b.x + dx; y = b.y + dy; w = Math.max(80, b.w - dx); h = Math.max(60, b.h - dy); }

    setResizeHandleBox({ x, y, w, h });
  };

  const onResizePointerUp = (e: React.PointerEvent<HTMLDivElement>) => {
    const drag = resizeDragRef.current;
    resizeDragRef.current = null;
    if (!drag || !hoveredDomainId || !cyRef.current || !resizeHandleBox || !containerRef.current) return;

    e.stopPropagation();

    // Convert screen-space resize box back to model space
    const cy = cyRef.current;
    const zoom = (cy as { zoom(): number }).zoom();
    const pan = (cy as { pan(): { x: number; y: number } }).pan();
    const rect = containerRef.current.getBoundingClientRect();

    const screenX1 = resizeHandleBox.x - rect.left;
    const screenY1 = resizeHandleBox.y - rect.top;
    const modelX1 = (screenX1 - pan.x) / zoom;
    const modelY1 = (screenY1 - pan.y) / zoom;
    const modelW = resizeHandleBox.w / zoom;
    const modelH = resizeHandleBox.h / zoom;

    const domainNode = cy.getElementById(`d:${hoveredDomainId}`);
    const children = cy.nodes(`[parent = "d:${hoveredDomainId}"]`);
    if (!(children as unknown as { empty(): boolean }).empty()) {
      // Read current compound bounds (before removing fCoSE size bypass).
      const oldBb = (domainNode as unknown as {
        boundingBox(o: object): { x1: number; y1: number; x2: number; y2: number; w: number; h: number };
      }).boundingBox({ includeLabels: false });

      // Scale each child's position proportionally from old compound bounds to
      // new model bounds, preserving the two-phase layout structure.
      cy.batch(() => {
        (domainNode as unknown as { removeStyle(s: string): void }).removeStyle("width height");
        children.forEach((child) => {
          const childPos = (child as { position(): Pt }).position();
          const relX = oldBb.w > 0 ? (childPos.x - oldBb.x1) / oldBb.w : 0.5;
          const relY = oldBb.h > 0 ? (childPos.y - oldBb.y1) / oldBb.h : 0.5;
          const pos = {
            x: modelX1 + relX * modelW,
            y: modelY1 + relY * modelH,
          };
          const id = (child as { id(): string }).id();
          pinnedNodesRef.current.delete(id);
          (child as { position(p: Pt): void }).position(pos);
          pinnedNodesRef.current.set(id, pos);
        });
      });
    }

    // Read freshly computed compound bounds after Cytoscape finishes its render cycle.
    requestAnimationFrame(() => updateHandleBox(hoveredDomainId));
  };

  // ── exports ───────────────────────────────────────────────────────────────
  const EXPORT_BG = "#0f172a";
  const EXPORT_PAD = 10;

  const addRasterPadding = (blob: Blob, mimeType: string, quality: number, filename: string) => {
    const url = URL.createObjectURL(blob);
    const img = new Image();
    img.onload = () => {
      const canvas = document.createElement("canvas");
      canvas.width = img.width + EXPORT_PAD * 2;
      canvas.height = img.height + EXPORT_PAD * 2;
      const ctx = canvas.getContext("2d")!;
      ctx.fillStyle = EXPORT_BG;
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.drawImage(img, EXPORT_PAD, EXPORT_PAD);
      URL.revokeObjectURL(url);
      canvas.toBlob((b) => b && downloadBlob(b, filename), mimeType, quality);
    };
    img.src = url;
  };

  const exportSvg = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    const raw = cy.svg({ full: true, bg: EXPORT_BG }) as string;
    // Inject 10px padding by expanding width/height and wrapping content in a translate group
    const padded = raw.replace(
      /(<svg[^>]*\swidth="(\d+(?:\.\d+)?)"[^>]*\sheight="(\d+(?:\.\d+)?)"[^>]*>)/,
      (_match: string, _tag: string, w: string, h: string) => {
        const nw = parseFloat(w) + EXPORT_PAD * 2;
        const nh = parseFloat(h) + EXPORT_PAD * 2;
        return _tag
          .replace(/width="[^"]*"/, `width="${nw}"`)
          .replace(/height="[^"]*"/, `height="${nh}"`) +
          `<rect width="${nw}" height="${nh}" fill="${EXPORT_BG}"/><g transform="translate(${EXPORT_PAD},${EXPORT_PAD})">`;
      },
    ).replace("</svg>", "</g></svg>");
    downloadBlob(new Blob([padded], { type: "image/svg+xml" }), "erd.svg");
  }, []);

  const exportPng = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    addRasterPadding(cy.png({ output: "blob", full: true, bg: EXPORT_BG }) as unknown as Blob, "image/png", 1, "erd.png");
  }, []);

  const exportJpeg = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    addRasterPadding(cy.jpg({ output: "blob", full: true, bg: EXPORT_BG, quality: 0.92 }) as unknown as Blob, "image/jpeg", 0.92, "erd.jpg");
  }, []);

  // ── collapse all / expand all (visible domains only) ─────────────────────
  const visibleDomainIds = allDomainIds.filter((id) => !hiddenDomains.has(id));
  const allCollapsed = visibleDomainIds.length > 0 && visibleDomainIds.every((id) => collapsedDomains.has(id));
  const toggleAll = () =>
    setCollapsedDomains(allCollapsed ? new Set() : new Set(visibleDomainIds));

  const toggleHidden = (domainId: string) =>
    setHiddenDomains((prev) => {
      const next = new Set(prev);
      if (next.has(domainId)) next.delete(domainId); else next.add(domainId);
      return next;
    });

  const hiddenCount = hiddenDomains.size;

  return createPortal(
    <div
      className="modal-overlay"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div
        className="modal modal--erd"
        style={{
          width: "92vw", height: "88vh", maxWidth: "92vw",
          display: "flex", flexDirection: "column",
          background: "#0f172a", padding: 0, overflow: "hidden",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* ── header ── */}
        <div style={{
          display: "flex", alignItems: "center", gap: "0.5rem",
          padding: "0.6rem 0.75rem", borderBottom: "1px solid #1e293b",
          flexShrink: 0, flexWrap: "wrap",
        }}>
          <span style={{ fontWeight: 600, color: "#e2e8f0", marginRight: "0.25rem" }}>
            Entity Relationship Diagram
          </span>

          {/* domain picker */}
          <div ref={domainPickerRef} style={{ position: "relative" }}>
            <TBtn
              onClick={() => setShowDomainPicker((v) => !v)}
              active={showDomainPicker || hiddenCount > 0}
              title="Show / hide domains"
            >
              <Layers size={11} />
              Domains
              {hiddenCount > 0 && (
                <span style={{
                  background: "#ef4444", color: "#fff",
                  borderRadius: 8, padding: "0 4px", fontSize: 10, lineHeight: "14px",
                }}>
                  -{hiddenCount}
                </span>
              )}
            </TBtn>
            {showDomainPicker && (
              <div style={{
                position: "absolute", top: "calc(100% + 4px)", left: 0,
                background: "#1e293b", border: "1px solid #334155",
                borderRadius: 6, padding: "6px 0", zIndex: 100,
                minWidth: 180, boxShadow: "0 4px 12px rgba(0,0,0,0.4)",
              }}>
                {allDomainIds.map((id) => (
                  <label
                    key={id}
                    style={{
                      display: "flex", alignItems: "center", gap: 8,
                      padding: "4px 12px", cursor: "pointer", fontSize: 12,
                      color: hiddenDomains.has(id) ? "#475569" : "#e2e8f0",
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={!hiddenDomains.has(id)}
                      onChange={() => toggleHidden(id)}
                      style={{ accentColor: labelColor(id) }}
                    />
                    <span
                      style={{
                        display: "inline-block", width: 8, height: 8,
                        borderRadius: "50%", background: labelColor(id), flexShrink: 0,
                      }}
                    />
                    {id}
                  </label>
                ))}
              </div>
            )}
          </div>

          {/* column detail select */}
          <select
            value={columnDetail}
            onChange={(e) => setColumnDetail(e.target.value as ColumnDetail)}
            style={{
              padding: "2px 6px",
              fontSize: 11,
              background: "#1e293b",
              color: "#e2e8f0",
              border: "1px solid #334155",
              borderRadius: 4,
              cursor: "pointer",
            }}
          >
            <option value="all">All cols</option>
            <option value="key">Keys only</option>
            <option value="none">No cols</option>
          </select>

          {/* edge routing select */}
          <select
            value={edgeRouting}
            onChange={(e) => setEdgeRouting(e.target.value as "bezier" | "taxi")}
            style={{
              padding: "2px 6px",
              fontSize: 11,
              background: "#1e293b",
              color: "#e2e8f0",
              border: "1px solid #334155",
              borderRadius: 4,
              cursor: "pointer",
            }}
          >
            <option value="bezier">Curved edges</option>
            <option value="taxi">Right-angle edges</option>
          </select>

          {/* snap to grid */}
          <select
            value={gridSnap}
            onChange={(e) => setGridSnap(parseInt(e.target.value, 10))}
            style={{ fontSize: 11, background: "#1e293b", color: "#e2e8f0", border: "1px solid #334155", borderRadius: 4, padding: "1px 4px" }}
          >
            <option value={0}>No snap</option>
            <option value={5}>Snap 5px</option>
            <option value={10}>Snap 10px</option>
            <option value={15}>Snap 15px</option>
            <option value={20}>Snap 20px</option>
          </select>

          {/* show orphans */}
          <label style={{
            display: "flex", alignItems: "center", gap: 4,
            fontSize: 11, color: showOrphans ? "#e2e8f0" : "#64748b",
            cursor: "pointer", userSelect: "none",
          }}>
            <input
              type="checkbox"
              checked={showOrphans}
              onChange={(e) => setShowOrphans(e.target.checked)}
              style={{ accentColor: "#60a5fa" }}
            />
            Orphans
          </label>

          {/* collapse/expand all */}
          <TBtn onClick={toggleAll} title={allCollapsed ? "Expand all domains" : "Collapse all domains"}>
            {allCollapsed ? <ChevronRight size={12} /> : <ChevronDown size={12} />}
            {allCollapsed ? "Expand all" : "Collapse all"}
          </TBtn>

          <div style={{ flex: 1 }} />

          {/* exports */}
          <TBtn onClick={() => cyRef.current?.fit(undefined, 10)} title="Fit all"><Maximize2 size={11} /></TBtn>
          <TBtn onClick={exportSvg} title="Download SVG"><Download size={11} /> SVG</TBtn>
          <TBtn onClick={exportPng} title="Download PNG"><Download size={11} /> PNG</TBtn>
          <TBtn onClick={exportJpeg} title="Download JPEG"><Download size={11} /> JPEG</TBtn>

          <button
            className="modal-close"
            onClick={onClose}
            style={{ color: "#64748b", marginLeft: "0.25rem" }}
          >
            <X size={16} />
          </button>
        </div>

        {/* ── hint bar ── */}
        {allDomainIds.length > 0 && (
          <div style={{
            fontSize: 10, color: "#475569",
            padding: "3px 12px", borderBottom: "1px solid #1e293b", flexShrink: 0,
          }}>
            Click a domain group to collapse / expand · dashed lines connect collapsed domains
          </div>
        )}

        {/* domain resize handles */}
        {resizeHandleBox && (
          <div
            style={{
              position: "fixed",
              left: resizeHandleBox.x,
              top: resizeHandleBox.y,
              width: resizeHandleBox.w,
              height: resizeHandleBox.h,
              border: "2px dashed #60a5fa",
              borderRadius: 4,
              pointerEvents: "none",
              zIndex: 200,
              boxSizing: "border-box",
            }}
          >
            {(["nw", "ne", "sw", "se"] as const).map((corner) => (
              <div
                key={corner}
                onPointerDown={(e) => onResizePointerDown(corner, e)}
                onPointerMove={onResizePointerMove}
                onPointerUp={onResizePointerUp}
                onMouseEnter={() => { handleHoverRef.current = true; }}
                onMouseLeave={() => {
                  handleHoverRef.current = false;
                  if (!resizeDragRef.current) {
                    setHoveredDomainId(null);
                    setResizeHandleBox(null);
                  }
                }}
                style={{
                  position: "absolute",
                  width: 16, height: 16,
                  background: "#60a5fa",
                  borderRadius: 2,
                  cursor: corner === "se" || corner === "nw" ? "nwse-resize" : "nesw-resize",
                  pointerEvents: "all",
                  ...(corner === "nw" ? { top: -8, left: -8 } :
                      corner === "ne" ? { top: -8, right: -8 } :
                      corner === "sw" ? { bottom: -8, left: -8 } :
                                        { bottom: -8, right: -8 }),
                }}
              />
            ))}
          </div>
        )}

        {/* ── canvas ── */}
        <div ref={containerRef} style={{ flex: 1, background: "#0f172a" }} />
      </div>

      {/* ── tooltip ── */}
      {tooltip.visible && (
        <div style={{
          position: "fixed", left: tooltip.x, top: tooltip.y,
          background: "#1e293b", border: "1px solid #334155",
          borderRadius: 6, padding: "6px 10px", fontSize: 11,
          color: "#e2e8f0", maxWidth: 260, pointerEvents: "none",
          zIndex: 2000, boxShadow: "0 4px 12px rgba(0,0,0,0.4)",
        }}>
          <div style={{ fontWeight: 600, marginBottom: tooltip.body ? 4 : 0 }}>{tooltip.title}</div>
          {tooltip.body && <div style={{ color: "#94a3b8", lineHeight: 1.4 }}>{tooltip.body}</div>}
        </div>
      )}
    </div>,
    document.body,
  );
}

export type { ErdNodeDomain, ErdNodeTable };
