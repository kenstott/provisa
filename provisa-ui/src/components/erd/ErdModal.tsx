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
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Badge,
  Button,
  Checkbox,
  Group,
  Popover,
  Select,
  Stack,
  Text,
} from "@mantine/core";
import { Modal } from "@mantine/core";
import { X, Download, ChevronDown, ChevronRight, Layers, Maximize2 } from "lucide-react";
import cytoscape from "cytoscape";
import elkRaw from "cytoscape-elk";
import cytoscapeSvgRaw from "cytoscape-svg";
import { buildErdElements } from "./erd-model";
import type { ColumnDetail, ErdNodeDomain, ErdNodeTable } from "./erd-model";
import { labelColor } from "../graph/graph-model";
import { downloadBlob } from "../graph/graph-export";
import type { CyInstance, CyEvent, CyLayoutOptions } from "../graph/cytoscape-types";
import { nodesWithEdges, resolveCompoundOverlaps, packDomains, placeIsolatedGrid } from "./sections/erd-layout";
import { buildErdStylesheet } from "./sections/erd-stylesheet";
import type { TooltipState, ErdModalProps } from "./sections/erd-types";

// ── cytoscape plugin registration ────────────────────────────────────────────
type CyExt = Parameters<typeof cytoscape.use>[0];
type CyExtModule = { default?: CyExt } | CyExt;
const _interop = (m: CyExtModule): CyExt => (m as { default?: CyExt }).default ?? (m as CyExt);
try { cytoscape.use(_interop(elkRaw as CyExtModule)); } catch { /* already registered */ }
try { cytoscape.use(_interop(cytoscapeSvgRaw as CyExtModule)); } catch { /* already registered */ }

type Pt = { x: number; y: number };

// ── component ─────────────────────────────────────────────────────────────────
export function ErdModal({ tables, relationships, domains, activeDomain, onClose }: ErdModalProps) {
  const { t } = useTranslation();
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<CyInstance | null>(null);
  // Mantine Modal mounts its portal children a tick after this component's
  // first commit, so a plain ref is still null when the init effect first runs.
  // A callback ref flips this state when the canvas node actually attaches,
  // retriggering the cytoscape init effect with a real container.
  const [containerReady, setContainerReady] = useState(false);
  const setContainerNode = useCallback((el: HTMLDivElement | null) => {
    containerRef.current = el;
    setContainerReady(!!el);
  }, []);
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

      // Pack domain boxes to the container aspect ratio (side-by-side vs stacked)
      // now that each domain's full extent — connected nodes + orphan grid — is
      // known. resolveCompoundOverlaps below is a safety net; packing already
      // separates boxes.
      const crect = containerRef.current?.getBoundingClientRect();
      if (crect && crect.height > 0) packDomains(cy, crect.width / crect.height);

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
    // eslint-disable-next-line react-hooks/exhaustive-deps -- updateHandleBox is stable (useCallback); omitting it avoids infinite rebuild loop
  }, [containerReady, tables, relationships, domains, collapsedDomains, hiddenDomains, columnDetail, activeDomain]);

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

  return (
    <Modal
      opened
      onClose={onClose}
      withCloseButton={false}
      centered
      size="92vw"
      data-testid="erd-modal"
      styles={{
        content: {
          height: "88vh",
          maxHeight: "88vh",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
          background: "#0f172a",
        },
        body: {
          padding: 0,
          display: "flex",
          flexDirection: "column",
          flex: 1,
          overflow: "hidden",
        },
      }}
    >
      <>
        {/* ── header ── */}
        <Group
          gap="0.5rem"
          wrap="wrap"
          data-tour="rels-erd-modal"
          style={{
            padding: "0.6rem 0.75rem", borderBottom: "1px solid #1e293b",
            flexShrink: 0,
          }}
        >
          <Text fw={600} c="#e2e8f0" mr="0.25rem">
            {t("erdModal.title")}
          </Text>

          {/* domain picker */}
          <Popover
            opened={showDomainPicker}
            onChange={setShowDomainPicker}
            position="bottom-start"
            withinPortal
            transitionProps={{ duration: 0 }}
          >
            <Popover.Target>
              <Button
                size="compact-xs"
                variant={showDomainPicker || hiddenCount > 0 ? "filled" : "default"}
                color="gray"
                onClick={() => setShowDomainPicker((v) => !v)}
                title={t("erdModal.domainsTooltip")}
                leftSection={<Layers size={11} />}
                rightSection={
                  hiddenCount > 0 ? (
                    <Badge size="xs" color="red" circle>
                      -{hiddenCount}
                    </Badge>
                  ) : undefined
                }
                data-testid="erd-domains-toggle"
              >
                {t("erdModal.domains")}
              </Button>
            </Popover.Target>
            <Popover.Dropdown>
              <Stack gap={4} miw={180}>
                {allDomainIds.map((id) => (
                  <Checkbox
                    key={id}
                    label={
                      <Group gap={6} wrap="nowrap">
                        <span
                          style={{
                            display: "inline-block", width: 8, height: 8,
                            borderRadius: "50%", background: labelColor(id), flexShrink: 0,
                          }}
                        />
                        <Text size="xs" c={hiddenDomains.has(id) ? "dimmed" : undefined}>
                          {id}
                        </Text>
                      </Group>
                    }
                    checked={!hiddenDomains.has(id)}
                    onChange={() => toggleHidden(id)}
                    color={labelColor(id)}
                    size="xs"
                    data-testid={`erd-domain-toggle-${id}`}
                  />
                ))}
              </Stack>
            </Popover.Dropdown>
          </Popover>

          {/* column detail select */}
          <Select
            aria-label={t("erdModal.columnDetailLabel")}
            value={columnDetail}
            onChange={(v) => v && setColumnDetail(v as ColumnDetail)}
            allowDeselect={false}
            size="xs"
            w={110}
            data={[
              { value: "all", label: t("erdModal.columnDetailAll") },
              { value: "key", label: t("erdModal.columnDetailKey") },
              { value: "none", label: t("erdModal.columnDetailNone") },
            ]}
            data-testid="erd-column-detail"
          />

          {/* edge routing select */}
          <Select
            aria-label={t("erdModal.edgeRoutingLabel")}
            value={edgeRouting}
            onChange={(v) => v && setEdgeRouting(v as "bezier" | "taxi")}
            allowDeselect={false}
            size="xs"
            w={150}
            data={[
              { value: "bezier", label: t("erdModal.edgeRoutingBezier") },
              { value: "taxi", label: t("erdModal.edgeRoutingTaxi") },
            ]}
            data-testid="erd-edge-routing"
          />

          {/* snap to grid */}
          <Select
            aria-label={t("erdModal.gridSnapLabel")}
            value={String(gridSnap)}
            onChange={(v) => v && setGridSnap(parseInt(v, 10))}
            allowDeselect={false}
            size="xs"
            w={110}
            data={[
              { value: "0", label: t("erdModal.gridSnapNone") },
              { value: "5", label: t("erdModal.gridSnap5") },
              { value: "10", label: t("erdModal.gridSnap10") },
              { value: "15", label: t("erdModal.gridSnap15") },
              { value: "20", label: t("erdModal.gridSnap20") },
            ]}
            data-testid="erd-grid-snap"
          />

          {/* show orphans */}
          <Checkbox
            label={t("erdModal.orphans")}
            checked={showOrphans}
            onChange={(e) => setShowOrphans(e.currentTarget.checked)}
            size="xs"
            data-testid="erd-show-orphans"
          />

          {/* collapse/expand all */}
          <Button
            size="compact-xs"
            variant="default"
            onClick={toggleAll}
            title={allCollapsed ? t("erdModal.expandAll") : t("erdModal.collapseAll")}
            leftSection={allCollapsed ? <ChevronRight size={12} /> : <ChevronDown size={12} />}
            data-testid="erd-toggle-all"
          >
            {allCollapsed ? t("erdModal.expandAll") : t("erdModal.collapseAll")}
          </Button>

          <div style={{ flex: 1 }} />

          {/* exports */}
          <ActionIcon
            variant="default"
            aria-label={t("erdModal.fitAll")}
            title={t("erdModal.fitAll")}
            onClick={() => cyRef.current?.fit(undefined, 10)}
            data-testid="erd-fit-all"
          >
            <Maximize2 size={13} />
          </ActionIcon>
          <Button
            size="compact-xs"
            variant="default"
            leftSection={<Download size={11} />}
            onClick={exportSvg}
            title={t("erdModal.downloadSvg")}
            data-testid="erd-export-svg"
          >
            SVG
          </Button>
          <Button
            size="compact-xs"
            variant="default"
            leftSection={<Download size={11} />}
            onClick={exportPng}
            title={t("erdModal.downloadPng")}
            data-testid="erd-export-png"
          >
            PNG
          </Button>
          <Button
            size="compact-xs"
            variant="default"
            leftSection={<Download size={11} />}
            onClick={exportJpeg}
            title={t("erdModal.downloadJpeg")}
            data-testid="erd-export-jpeg"
          >
            JPEG
          </Button>

          <ActionIcon
            variant="subtle"
            color="gray"
            aria-label={t("erdModal.close")}
            onClick={onClose}
            ml="0.25rem"
            data-testid="erd-close"
          >
            <X size={16} />
          </ActionIcon>
        </Group>

        {/* ── hint bar ── */}
        {allDomainIds.length > 0 && (
          <Text
            size="10px"
            c="#475569"
            style={{ padding: "3px 12px", borderBottom: "1px solid #1e293b", flexShrink: 0 }}
          >
            {t("erdModal.hint")}
          </Text>
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
                role="button"
                aria-label={t("erdModal.resizeHandle", { corner })}
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
        <div ref={setContainerNode} style={{ flex: 1, background: "#0f172a" }} />

        {/* ── tooltip ── */}
        {tooltip.visible && (
          <div
            role="tooltip"
            style={{
              position: "fixed", left: tooltip.x, top: tooltip.y,
              background: "#1e293b", border: "1px solid #334155",
              borderRadius: 6, padding: "6px 10px", fontSize: 11,
              color: "#e2e8f0", maxWidth: 260, pointerEvents: "none",
              zIndex: 2000, boxShadow: "0 4px 12px rgba(0,0,0,0.4)",
            }}
          >
            <div style={{ fontWeight: 600, marginBottom: tooltip.body ? 4 : 0 }}>{tooltip.title}</div>
            {tooltip.body && <div style={{ color: "#94a3b8", lineHeight: 1.4 }}>{tooltip.body}</div>}
          </div>
        )}
      </>
    </Modal>
  );
}

export type { ErdNodeDomain, ErdNodeTable };
