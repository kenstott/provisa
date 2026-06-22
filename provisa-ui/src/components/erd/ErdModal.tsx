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
import fcoseRaw from "cytoscape-fcose";
import cytoscapeSvgRaw from "cytoscape-svg";
import { buildErdElements, buildTableLabel } from "./erd-model";
import type { ColumnDetail, ErdNodeDomain, ErdNodeTable } from "./erd-model";
import type { RegisteredTable, Relationship, Domain, TableColumn } from "../../types/admin";
import { labelColor, darkenColor } from "../graph/graph-model";
import { downloadBlob } from "../graph/graph-export";
import type { CyInstance, CyEvent } from "../graph/cytoscape-types";

// ── cytoscape plugin registration ────────────────────────────────────────────
type CyExt = Parameters<typeof cytoscape.use>[0];
type CyExtModule = { default?: CyExt } | CyExt;
const _interop = (m: CyExtModule): CyExt => (m as { default?: CyExt }).default ?? (m as CyExt);
try { cytoscape.use(_interop(fcoseRaw as CyExtModule)); } catch { /* already registered */ }
try { cytoscape.use(_interop(cytoscapeSvgRaw as CyExtModule)); } catch { /* already registered */ }

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
        "text-rotation": "autorotate",
        "text-background-color": "#0f172a",
        "text-background-opacity": 0.75,
        "text-background-padding": "2px",
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
  const [snapToGrid, setSnapToGrid] = useState(
    () => localStorage.getItem("erd.snapToGrid") === "true",
  );
  const snapToGridRef = useRef(localStorage.getItem("erd.snapToGrid") === "true");
  const [showOrphans, setShowOrphans] = useState(
    () => localStorage.getItem("erd.showOrphans") !== "false",
  );
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
    const node = cyRef.current.$(`#d\\:${domainId}`);
    if (!node || (node as { empty(): boolean }).empty()) { setResizeHandleBox(null); return; }
    const bb = (node as { renderedBoundingBox(opts: object): { x1: number; y1: number; w: number; h: number } })
      .renderedBoundingBox({ includeLabels: false });
    const rect = containerRef.current.getBoundingClientRect();
    setResizeHandleBox({ x: rect.left + bb.x1, y: rect.top + bb.y1, w: bb.w, h: bb.h });
  }, []);

  // Keep refs in sync so Cytoscape event handlers always see current values.
  useEffect(() => { snapToGridRef.current = snapToGrid; }, [snapToGrid]);
  useEffect(() => { edgeRoutingRef.current = edgeRouting; }, [edgeRouting]);

  // Persist toolbar choices across sessions.
  useEffect(() => { localStorage.setItem("erd.columnDetail", columnDetail); }, [columnDetail]);
  useEffect(() => { localStorage.setItem("erd.edgeRouting", edgeRouting); }, [edgeRouting]);
  useEffect(() => { localStorage.setItem("erd.snapToGrid", String(snapToGrid)); }, [snapToGrid]);
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
    const connectedTableIds = showOrphans ? null : new Set<number>(
      relationships.flatMap((r) => [r.sourceTableId, ...(r.targetTableId != null ? [r.targetTableId] : [])]),
    );
    const visibleTables = showOrphans ? tables : tables.filter((t) => connectedTableIds!.has(t.id));
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

    // Compute grid cell size from actual node bounding boxes (width/height + gap).
    const gridSize = () => {
      const nodes = cy.nodes(".erd-table");
      if ((nodes as { empty(): boolean }).empty()) return { gx: 190, gy: 60 };
      let maxW = 0, maxH = 0;
      nodes.forEach((n) => {
        const bb = (n as { boundingBox(o: object): { w: number; h: number } }).boundingBox({});
        if (bb.w > maxW) maxW = bb.w;
        if (bb.h > maxH) maxH = bb.h;
      });
      return { gx: maxW + 20, gy: maxH + 20 };
    };

    // After layout: snap to grid (if enabled) then stack isolated tables.
    cy.on("layoutstop", () => {
      if (snapToGridRef.current) {
        const { gx, gy } = gridSize();
        cy.nodes(".erd-table").forEach((n) => {
          const p = (n as { position(): { x: number; y: number } }).position();
          (n as { position(p: { x: number; y: number }): void }).position({
            x: Math.round(p.x / gx) * gx,
            y: Math.round(p.y / gy) * gy,
          });
        });
      }

      cy.nodes(".erd-domain").forEach((domainNode) => {
        const children = domainNode.children();
        const isolated = children.filter(
          (n) => (n as { degree(includeLoops: boolean): number }).degree(false) === 0,
        );
        if ((isolated as { empty(): boolean }).empty()) return;

        const { gx, gy } = gridSize();
        const stepX = snapToGridRef.current ? gx : gx;
        const stepY = snapToGridRef.current ? gy : gy;
        const cols = Math.max(1, Math.floor(4));

        // Place orphans below the connected nodes, or at the domain top-left if none.
        const connected = children.filter(
          (n) => (n as { degree(includeLoops: boolean): number }).degree(false) > 0,
        );
        let startX: number;
        let startY: number;
        if (!(connected as { empty(): boolean }).empty()) {
          const cbb = (connected as { boundingBox(o: object): { x1: number; y2: number } })
            .boundingBox({});
          startX = cbb.x1;
          startY = cbb.y2 + stepY;
        } else {
          const dbb = (domainNode as { boundingBox(o: object): { x1: number; y1: number } })
            .boundingBox({ includeLabels: false });
          startX = dbb.x1 + stepX * 0.5;
          startY = dbb.y1 + stepY * 0.5;
        }
        if (snapToGridRef.current) {
          startX = Math.round(startX / gx) * gx;
          startY = Math.round(startY / gy) * gy;
        }

        isolated.forEach((n, i) => {
          const col = i % cols;
          const row = Math.floor(i / cols);
          (n as { position(p: { x: number; y: number }): void }).position({
            x: startX + col * stepX,
            y: startY + row * stepY,
          });
        });
      });

      // Collapsed domain leaf nodes placed inside an expanded compound by fCoSE render
      // behind its background and become invisible. Move them outside.
      cy.nodes(".erd-domain").forEach((leafDomain) => {
        if (!(leafDomain.children() as { empty(): boolean }).empty()) return;
        cy.nodes(".erd-domain").forEach((compound) => {
          if (compound.id() === leafDomain.id()) return;
          if ((compound.children() as { empty(): boolean }).empty()) return;
          const cbb = (compound as { boundingBox(o: object): { x1: number; x2: number; y1: number; y2: number } })
            .boundingBox({});
          const pos = (leafDomain as { position(): { x: number; y: number } }).position();
          if (pos.x > cbb.x1 && pos.x < cbb.x2 && pos.y > cbb.y1 && pos.y < cbb.y2) {
            (leafDomain as { position(p: { x: number; y: number }): void })
              .position({ x: cbb.x2 + 120, y: (cbb.y1 + cbb.y2) / 2 });
          }
        });
      });

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
      const id = evt.target.id() as string;
      // Only pin table nodes — compound domain nodes have unreliable positions
      // on fresh cy instances and corrupt fixedNodeConstraint on next rebuild.
      if (!id.startsWith("t:")) return;
      const rawX = evt.target.position("x") as number;
      const rawY = evt.target.position("y") as number;
      let pos = { x: rawX, y: rawY };
      if (snapToGridRef.current) {
        const { gx, gy } = gridSize();
        pos = { x: Math.round(rawX / gx) * gx, y: Math.round(rawY / gy) * gy };
        (evt.target as { position(p: { x: number; y: number }): void }).position(pos);
      }
      pinnedNodesRef.current.set(id, pos);
    });

    // Update resize handles when cy viewport changes
    cy.on("pan zoom", () => {
      setHoveredDomainId((id) => { updateHandleBox(id); return id; });
    });

    // Run layout AFTER registering handlers so layoutstop fires with the handler already attached.
    const existingNodeIds = new Set(cy.nodes().map((n) => (n as { id(): string }).id()));
    // Only pin leaf table nodes — compound parent nodes have unreliable positions in fresh cy
    // instances and cause fCoSE to produce degenerate layouts when used as fixedNodeConstraint.
    const fixedNodeConstraint = [...pinnedNodesRef.current.entries()]
      .filter(([nodeId]) => nodeId.startsWith("t:") && existingNodeIds.has(nodeId))
      .map(([nodeId, position]) => ({ nodeId, position }));

    cy.layout({
      name: "fcose",
      animate: false,
      // Always randomize: fCoSE needs a non-degenerate starting config for fresh cy instances
      // where all unset node positions are (0,0). fixedNodeConstraint is applied after randomize.
      randomize: true,
      quality: "proof",
      numIter: 2500,
      nodeSeparation: 60,
      idealEdgeLength: () => 100,
      nodeRepulsion: () => 8000,
      nodeDimensionsIncludeLabels: true,
      uniformNodeDimensions: false,
      packComponents: false,
      tile: false,
      gravityRangeCompound: 1.5,
      gravityCompound: 1.0,
      gravity: 0.5,
      ...(fixedNodeConstraint.length > 0 ? { fixedNodeConstraint } : {}),
    } as Parameters<typeof cytoscape>[0]["layout"]).run();

    return () => { cy.destroy(); cyRef.current = null; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tables, relationships, domains, collapsedDomains, hiddenDomains, activeDomain, showOrphans]);

  // ── label-only update when columnDetail changes (no re-layout) ──────────
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.batch(() => {
      cy.$(".erd-table").forEach((n) => {
        const cols = n.data("columns") as TableColumn[];
        const name = n.data("tableName") as string;
        const { label, lineCount } = buildTableLabel(name, cols, columnDetail);
        n.data("displayLabel", label);
        n.data("lineCount", lineCount);
      });
    });
    cy.style(buildErdStylesheet() as unknown as Parameters<CyInstance["style"]>[0]);
    // cy.style() resets curve-style to the stylesheet default ("bezier") — restore current routing.
    cy.$(".erd-rel").style("curve-style", edgeRoutingRef.current);
    if (edgeRoutingRef.current === "taxi") {
      const seenPairs = new Set<string>();
      cy.$(".erd-rel").forEach((edge) => {
        const s = edge.data("source") as string;
        const t = edge.data("target") as string;
        const key = [s, t].sort().join("↔");
        edge.style("display", seenPairs.has(key) ? "none" : "element");
        seenPairs.add(key);
      });
    } else {
      cy.$(".erd-rel").style("display", "element");
    }
  }, [columnDetail]);

  // ── snap-to-grid toggle: snap current positions in-place (no re-layout) ─
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy || !snapToGrid) return;
    const nodes = cy.nodes(".erd-table");
    if ((nodes as { empty(): boolean }).empty()) return;
    let maxW = 0, maxH = 0;
    nodes.forEach((n) => {
      const bb = (n as { boundingBox(o: object): { w: number; h: number } }).boundingBox({});
      if (bb.w > maxW) maxW = bb.w;
      if (bb.h > maxH) maxH = bb.h;
    });
    const gx = maxW + 20;
    const gy = maxH + 20;
    cy.batch(() => {
      nodes.forEach((n) => {
        const p = (n as { position(): { x: number; y: number } }).position();
        (n as { position(p: { x: number; y: number }): void }).position({
          x: Math.round(p.x / gx) * gx,
          y: Math.round(p.y / gy) * gy,
        });
      });
    });
  }, [snapToGrid]);

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

    // Spread children so outermost nodes sit at the edges of the target bounds.
    // Compound nodes auto-size to fit children + their padding (32px from stylesheet).
    // Node half-widths: width=170 → halfW=85. Height is variable; read from bbox.
    const COMP_PAD = 32;
    const NODE_HALF_W = 85;

    const domainNode = cy.$(`#d\\:${hoveredDomainId}`);
    // fCoSE sets explicit width/height bypasses on compound nodes after layout.
    // Clearing them lets Cytoscape revert to auto-sizing from children's bounding box.
    domainNode.removeStyle("width height");

    const children = cy.nodes(`[parent = "d:${hoveredDomainId}"]`);
    const n = children.length;
    if (n > 0) {
      const cols = Math.max(1, Math.ceil(Math.sqrt(n * (modelW / modelH))));
      const rows = Math.ceil(n / cols);

      // Average node half-height from actual bounding boxes
      let totalH = 0;
      children.forEach((c) => {
        totalH += (c as { boundingBox(o: object): { h: number } }).boundingBox({}).h;
      });
      const nodeHalfH = (totalH / n) / 2;

      // Usable span for node centers inside the compound
      const spanX = Math.max(0, modelW - 2 * (COMP_PAD + NODE_HALF_W));
      const spanY = Math.max(0, modelH - 2 * (COMP_PAD + nodeHalfH));
      const originX = modelX1 + COMP_PAD + NODE_HALF_W;
      const originY = modelY1 + COMP_PAD + nodeHalfH;

      cy.batch(() => {
        children.forEach((child, i) => {
          const col = i % cols;
          const row = Math.floor(i / cols);
          const xFrac = cols > 1 ? col / (cols - 1) : 0.5;
          const yFrac = rows > 1 ? row / (rows - 1) : 0.5;
          const pos = {
            x: originX + xFrac * spanX,
            y: originY + yFrac * spanY,
          };
          const id = (child as { id(): string }).id();
          pinnedNodesRef.current.delete(id);
          (child as { position(p: { x: number; y: number }): void }).position(pos);
          pinnedNodesRef.current.set(id, pos);
        });
      });
    }

    // One RAF: Cytoscape processes the batch, recomputes compound bounds, repaints.
    // Second RAF reads the freshly painted bounds for the overlay.
    requestAnimationFrame(() => {
      (cy as { forceRender(): void }).forceRender();
      requestAnimationFrame(() => updateHandleBox(hoveredDomainId));
    });
  };

  // ── exports ───────────────────────────────────────────────────────────────
  const exportSvg = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    downloadBlob(new Blob([cy.svg({ full: true, bg: "#0f172a" }) as string], { type: "image/svg+xml" }), "erd.svg");
  }, []);

  const exportPng = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    downloadBlob(cy.png({ output: "blob", full: true, bg: "#0f172a" }) as unknown as Blob, "erd.png");
  }, []);

  const exportJson = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    downloadBlob(
      new Blob([JSON.stringify((cy as unknown as { json(): unknown }).json(), null, 2)], { type: "application/json" }),
      "erd.json",
    );
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
          <label style={{
            display: "flex", alignItems: "center", gap: 4,
            fontSize: 11, color: snapToGrid ? "#e2e8f0" : "#64748b",
            cursor: "pointer", userSelect: "none",
          }}>
            <input
              type="checkbox"
              checked={snapToGrid}
              onChange={(e) => setSnapToGrid(e.target.checked)}
              style={{ accentColor: "#60a5fa" }}
            />
            Snap to grid
          </label>

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
          <TBtn onClick={() => cyRef.current?.fit()} title="Fit all"><Maximize2 size={11} /></TBtn>
          <TBtn onClick={exportSvg} title="Download SVG"><Download size={11} /> SVG</TBtn>
          <TBtn onClick={exportPng} title="Download PNG"><Download size={11} /> PNG</TBtn>
          <TBtn onClick={exportJson} title="Download JSON"><Download size={11} /> JSON</TBtn>

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
