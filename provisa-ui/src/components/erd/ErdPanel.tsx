// Copyright (c) 2026 Kenneth Stott
// Canary: a5c66ff8-7531-4422-9caf-151debb92bcf
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useRef, useEffect, useState, useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Button, Checkbox, Group, Select, useComputedColorScheme } from "@mantine/core";
import { ChevronDown, ChevronRight } from "lucide-react";
import cytoscape from "cytoscape";
import elkRaw from "cytoscape-elk";
import cytoscapeSvgRaw from "cytoscape-svg";
import { buildErdElements, erdJson } from "./erd-model";
import { useErdMarks } from "./useErdMarks";
import type { ColumnDetail, ErdNodeDomain, ErdNodeTable } from "./erd-model";
import { downloadBlob } from "../graph/graph-export";
import type { CyInstance, CyEvent } from "../graph/cytoscape-types";
import { nodesWithEdges, placeIsolatedGrid, arrangeErd } from "./sections/erd-layout";
import { buildErdStylesheet } from "./sections/erd-stylesheet";
import { ERD_ELK_LAYOUT } from "./erd-layout-options";
import { applyTaxiLanes, clearTaxiLanes } from "./sections/erd-taxi";
import { getErdPalette } from "./sections/erd-palette";
import type { TooltipState, ErdPanelProps } from "./sections/erd-types";
import { useErdCollapseLink } from "./useErdCollapseLink";
import { Tip } from "../Tip";
import { HelpBubble } from "../HelpBubble";
import { ErdCanvasBar } from "./ErdCanvasBar";
import { FIT_PADDING } from "./erd-viewport";
import { useErdOptions, type ErdOptions } from "./useErdOptions";
import { applyPlaces, readPlaces } from "./erd-saved-layout";
import type { SavedLayout } from "../../api/erdLayouts";

// ── cytoscape plugin registration ────────────────────────────────────────────
type CyExt = Parameters<typeof cytoscape.use>[0];
type CyExtModule = { default?: CyExt } | CyExt;
const _interop = (m: CyExtModule): CyExt => (m as { default?: CyExt }).default ?? (m as CyExt);
try {
  cytoscape.use(_interop(elkRaw as CyExtModule));
} catch {
  /* already registered */
}
try {
  cytoscape.use(_interop(cytoscapeSvgRaw as CyExtModule));
} catch {
  /* already registered */
}

type Pt = { x: number; y: number };

const NO_TABLES: number[] = [];
const NO_RELATIONSHIPS: string[] = [];

// ── component ─────────────────────────────────────────────────────────────────
export function ErdPanel({
  tables,
  relationships,
  domains,
  checkedDomains,
  highlightTableIds = NO_TABLES,
  highlightRelationshipIds = NO_RELATIONSHIPS,
  search = "",
  page = "tables",
  collapsed = null,
  onCollapsed,
  onTableClick,
  onRelationshipClick,
  savedLayout = null,
  onArrange,
  placesRef,
  layoutMenu,
  overlay,
}: ErdPanelProps) {
  const { t } = useTranslation();
  const colorScheme = useComputedColorScheme("dark");
  const isDark = colorScheme === "dark";
  const palette = getErdPalette(isDark);
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<CyInstance | null>(null);
  // The canvas div doesn't exist on this component's first commit, so a plain
  // ref is still null when the init effect first runs. A callback ref flips
  // this state when the canvas node actually attaches, retriggering the
  // cytoscape init effect with a real container.
  const [containerReady, setContainerReady] = useState(false);
  const setContainerNode = useCallback((el: HTMLDivElement | null) => {
    containerRef.current = el;
    setContainerReady(!!el);
  }, []);
  // nodeId → pinned model-space position (no Cytoscape lock — nodes stay draggable)
  const pinnedNodesRef = useRef<Map<string, { x: number; y: number }>>(new Map());

  // The diagram's box, holding the canvas and its bar: what goes full screen.
  const stageRef = useRef<HTMLDivElement>(null);
  const [options, changeOptions] = useErdOptions();
  const { columnDetail, edgeRouting, gridSnap, showOrphans } = options;
  const edgeRoutingRef = useRef(edgeRouting);
  const gridSnapRef = useRef(gridSnap);
  const showOrphansRef = useRef(showOrphans);
  useEffect(() => {
    showOrphansRef.current = showOrphans;
  }, [showOrphans]);
  const isolatedIdsRef = useRef<Set<string>>(new Set());
  const connectedDomainBboxesRef = useRef<Map<string, { x1: number; x2: number; y2: number }>>(
    new Map(),
  );
  const isDraggingRef = useRef(false);
  const [collapsedDomains, setCollapsedDomains] = useState<Set<string>>(new Set());
  // REQ-2189: a saved layout chosen brings its options and what it collapses; its places are
  // stood after the automatic layout (layoutstop), with what was moved since over them.
  const [appliedLayout, setAppliedLayout] = useState<SavedLayout | null>(null);
  if (savedLayout !== appliedLayout) {
    setAppliedLayout(savedLayout);
    if (savedLayout !== null) {
      changeOptions(savedLayout.options);
      setCollapsedDomains(
        new Set(savedLayout.domains.filter((d) => d.collapsed).map((d) => d.domainId)),
      );
    }
  }
  const layoutRef = useRef<SavedLayout | null>(null);
  useEffect(() => {
    layoutRef.current = appliedLayout;
    pinnedNodesRef.current = new Map();
  }, [appliedLayout]);
  // Domains to hide, derived from the host page's domain filter (not the panel's own state).
  const hiddenDomains = useMemo(() => {
    if (!checkedDomains) return new Set<string>();
    const hidden = new Set<string>();
    for (const t of tables) if (!checkedDomains.has(t.domainId)) hidden.add(t.domainId);
    return hidden;
  }, [tables, checkedDomains]);
  const [tooltip, setTooltip] = useState<TooltipState>({
    visible: false,
    x: 0,
    y: 0,
    title: "",
    body: "",
  });

  const { highlighted, applyHighlight } = useErdMarks(
    cyRef,
    highlightTableIds,
    highlightRelationshipIds,
    search,
    page,
  );

  // The host's handlers, read when a click comes: the diagram is not rebuilt when they change.
  const clicks = useRef({ onTableClick, onRelationshipClick, onArrange });
  useEffect(() => {
    clicks.current = { onTableClick, onRelationshipClick, onArrange };
  }, [onTableClick, onRelationshipClick, onArrange]);

  /** An option the reader set: an edit of the arrangement. */
  const setOptions = (change: Partial<ErdOptions>) => {
    changeOptions(change);
    clicks.current.onArrange?.();
  };

  const [hoveredDomainId, setHoveredDomainId] = useState<string | null>(null);
  const [resizeHandleBox, setResizeHandleBox] = useState<{
    x: number;
    y: number;
    w: number;
    h: number;
  } | null>(null);
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
    if (!node || (node as unknown as { empty(): boolean }).empty()) {
      setResizeHandleBox(null);
      return;
    }
    const bb = (
      node as unknown as {
        renderedBoundingBox(opts: object): { x1: number; y1: number; w: number; h: number };
      }
    ).renderedBoundingBox({ includeLabels: false });
    // Rendered coordinates are relative to the canvas, and the box is positioned inside it: a
    // fixed-position box measured against the viewport lands offset inside any transformed
    // ancestor, which becomes the containing block of anything fixed within it.
    setResizeHandleBox({ x: bb.x1, y: bb.y1, w: bb.w, h: bb.h });
  }, []);

  // Keep refs in sync so Cytoscape event handlers always see current values.
  useEffect(() => {
    gridSnapRef.current = gridSnap;
  }, [gridSnap]);
  useEffect(() => {
    edgeRoutingRef.current = edgeRouting;
  }, [edgeRouting]);

  // All domain IDs present in the table list (before hiding).
  const allDomainIds = [...new Set(tables.map((t) => t.domainId))];

  // ── initialise / rebuild on structural changes ────────────────────────────
  useEffect(() => {
    // Built once the canvas has attached (containerReady): a diagram built on the commit before it
    // is destroyed by the rebuild that follows, with its ELK layout still computing.
    if (!containerReady || !containerRef.current) return;
    const visibleTables = tables;
    const elements = buildErdElements(
      visibleTables,
      relationships,
      domains,
      collapsedDomains,
      hiddenDomains,
      columnDetail,
    );
    const allEls = [...elements.nodes, ...elements.edges] as unknown[];

    const cy = cytoscape({
      container: containerRef.current,
      elements: allEls as Parameters<typeof cytoscape>[0]["elements"],
      style: buildErdStylesheet(isDark) as unknown as Parameters<typeof cytoscape>[0]["style"],
      // Layout is run manually below, after all handlers are registered,
      // so that the layoutstop handler fires on the initial layout.
    }) as unknown as CyInstance;

    cyRef.current = cy;

    cy.on("grabon", "node", () => {
      isDraggingRef.current = true;
    });

    // Two-phase post-layout: place orphan grid below each domain's connected bbox.
    cy.on("layoutstop", () => {
      if (isDraggingRef.current) return;
      const crect = containerRef.current?.getBoundingClientRect();
      const arranged = arrangeErd(cy, {
        gridSnap: gridSnapRef.current,
        showOrphans: showOrphansRef.current,
        aspectRatio: crect && crect.height > 0 ? crect.width / crect.height : null,
      });
      // Store for the showOrphans toggle effect (avoids re-running the layout on toggle).
      isolatedIdsRef.current = arranged.isolatedIds;
      connectedDomainBboxesRef.current = arranged.connectedBboxes;

      // REQ-2189: a saved layout's places stand over the automatic ones.
      if (layoutRef.current !== null) applyPlaces(cy, layoutRef.current, pinnedNodesRef.current);

      cy.fit(undefined, FIT_PADDING);
      applyHighlight(cy);

      // Apply current edge routing (handles rebuilds triggered by showOrphans etc.)
      cy.$(".erd-rel").style("curve-style", edgeRoutingRef.current);
      if (edgeRoutingRef.current === "taxi") applyTaxiLanes(cy);
    });

    cy.on("tap", ".erd-domain", (evt: CyEvent) => {
      const domainId = evt.target.data("domainId") as string;
      clicks.current.onArrange?.();
      setCollapsedDomains((prev) => {
        const next = new Set(prev);
        if (next.has(domainId)) next.delete(domainId);
        else next.add(domainId);
        return next;
      });
    });

    cy.on("tap", ".erd-table", (evt: CyEvent) => {
      clicks.current.onTableClick(
        evt.target.data("tableId") as number,
        evt.target.data("tableName") as string,
      );
    });

    cy.on("tap", ".erd-rel", (evt: CyEvent) => {
      clicks.current.onRelationshipClick?.(evt.target.data("relationshipId") as string);
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
          x:
            (containerRef.current?.getBoundingClientRect().left ?? 0) +
            (evt.renderedPosition ?? evt.position).x +
            12,
          y:
            (containerRef.current?.getBoundingClientRect().top ?? 0) +
            (evt.renderedPosition ?? evt.position).y +
            12,
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
        setTooltip({
          visible: true,
          x: pos.x + 12,
          y: pos.y + 12,
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
      clicks.current.onArrange?.();
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
      // A move can swap an edge's dominant axis, which decides which way its lanes spread.
      if (edgeRoutingRef.current === "taxi") applyTaxiLanes(cy);
    });

    // Update resize handles when cy viewport changes
    cy.on("pan zoom", () => {
      setHoveredDomainId((id) => {
        updateHandleBox(id);
        return id;
      });
    });

    // Hide true orphans before layout so ELK only places connected nodes.
    // layoutstop shows them and places them in a per-domain grid below.
    const connectedBeforeLayout = nodesWithEdges(cy);
    cy.nodes(".erd-table").forEach((n) => {
      if (!connectedBeforeLayout.has((n as { id(): string }).id())) {
        n.style("display", "none");
      }
    });

    const layout = cy.layout(ERD_ELK_LAYOUT);
    let computing = true;
    layout.one("layoutstop", () => {
      computing = false;
    });
    layout.run();

    return () => {
      cyRef.current = null;
      if (!computing) {
        cy.destroy();
        return;
      }
      // ELK cannot be stopped, and writes its answer to the instance that asked: a focus changed,
      // or the view left, while it computes leaves that instance off the canvas until it answers.
      cy.off("layoutstop");
      cy.unmount();
      layout.one("layoutstop", () => cy.destroy());
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- updateHandleBox and applyHighlight are stable (useCallback); omitting them avoids infinite rebuild loop
  }, [
    containerReady,
    tables,
    relationships,
    domains,
    collapsedDomains,
    hiddenDomains,
    columnDetail,
    isDark,
    appliedLayout,
  ]);

  // The canvas narrows beside a detail panel and widens when it closes: the diagram follows.
  useEffect(() => {
    const canvas = containerRef.current;
    if (!containerReady || !canvas) return;
    const watcher = new ResizeObserver(() => {
      const cy = cyRef.current;
      if (!cy || canvas.clientWidth === 0) return;
      cy.resize();
      cy.fit(undefined, FIT_PADDING);
      applyHighlight(cy);
    });
    watcher.observe(canvas);
    return () => watcher.disconnect();
  }, [containerReady, applyHighlight]);

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
        let x1 = Infinity,
          x2 = -Infinity,
          y2 = -Infinity,
          hasConnected = false;
        domain.children().forEach((child) => {
          if (isolatedIds.has((child as { id(): string }).id())) return;
          const bb = (
            child as unknown as {
              boundingBox(o: object): { x1: number; x2: number; y1: number; y2: number; w: number };
            }
          ).boundingBox({});
          if (!bb.w) return;
          x1 = Math.min(x1, bb.x1);
          x2 = Math.max(x2, bb.x2);
          y2 = Math.max(y2, bb.y2);
          hasConnected = true;
        });
        if (hasConnected) liveBboxes.set((domain as { id(): string }).id(), { x1, x2, y2 });
      });
      placeIsolatedGrid(cy, isolatedIds, liveBboxes);
      isolatedIds.forEach((id) => {
        (cy.getElementById(id) as unknown as { style(k: string, v: string): void }).style(
          "display",
          "element",
        );
      });
    } else {
      isolatedIds.forEach((id) => {
        (cy.getElementById(id) as unknown as { style(k: string, v: string): void }).style(
          "display",
          "none",
        );
      });
    }
    cy.fit(undefined, FIT_PADDING);
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
      if (edgeRouting === "taxi") applyTaxiLanes(cy);
      else clearTaxiLanes(cy);
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
    if (drag.corner === "se") {
      w = Math.max(80, b.w + dx);
      h = Math.max(60, b.h + dy);
    } else if (drag.corner === "sw") {
      x = b.x + dx;
      w = Math.max(80, b.w - dx);
      h = Math.max(60, b.h + dy);
    } else if (drag.corner === "ne") {
      y = b.y + dy;
      w = Math.max(80, b.w + dx);
      h = Math.max(60, b.h - dy);
    } else if (drag.corner === "nw") {
      x = b.x + dx;
      y = b.y + dy;
      w = Math.max(80, b.w - dx);
      h = Math.max(60, b.h - dy);
    }

    setResizeHandleBox({ x, y, w, h });
  };

  const onResizePointerUp = (e: React.PointerEvent<HTMLDivElement>) => {
    const drag = resizeDragRef.current;
    resizeDragRef.current = null;
    if (!drag || !hoveredDomainId || !cyRef.current || !resizeHandleBox || !containerRef.current)
      return;

    e.stopPropagation();

    // Convert screen-space resize box back to model space
    const cy = cyRef.current;
    const zoom = (cy as { zoom(): number }).zoom();
    const pan = (cy as { pan(): { x: number; y: number } }).pan();

    const modelX1 = (resizeHandleBox.x - pan.x) / zoom;
    const modelY1 = (resizeHandleBox.y - pan.y) / zoom;
    const modelW = resizeHandleBox.w / zoom;
    const modelH = resizeHandleBox.h / zoom;

    const domainNode = cy.getElementById(`d:${hoveredDomainId}`);
    const children = cy.nodes(`[parent = "d:${hoveredDomainId}"]`);
    if (!(children as unknown as { empty(): boolean }).empty()) {
      // Read current compound bounds (before removing fCoSE size bypass).
      const oldBb = (
        domainNode as unknown as {
          boundingBox(o: object): {
            x1: number;
            y1: number;
            x2: number;
            y2: number;
            w: number;
            h: number;
          };
        }
      ).boundingBox({ includeLabels: false });

      // Scale each child's position proportionally from old compound bounds to
      // new model bounds, preserving the two-phase layout structure.
      cy.batch(() => {
        // A saved layout's least size (REQ-2189) gives way to the size the reader draws.
        (domainNode as unknown as { removeStyle(s: string): void }).removeStyle(
          "width height min-width min-height",
        );
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

    onArrange?.();
    // Read freshly computed compound bounds after Cytoscape finishes its render cycle.
    requestAnimationFrame(() => updateHandleBox(hoveredDomainId));
  };

  // ── exports ───────────────────────────────────────────────────────────────
  const EXPORT_BG = palette.bg;
  const EXPORT_PAD = 10;

  // Depends on the palette, so the export helpers below list it: pinned to [] they kept the
  // background colour from the theme in force when the panel first mounted, and an export taken
  // after a light/dark switch came out on the previous theme's ground.
  const addRasterPadding = useCallback(
    (blob: Blob, mimeType: string, quality: number, filename: string) => {
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
    },
    [EXPORT_BG],
  );

  const exportSvg = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    const raw = cy.svg({ full: true, bg: EXPORT_BG }) as string;
    // Inject 10px padding by expanding width/height and wrapping content in a translate group
    const padded = raw
      .replace(
        /(<svg[^>]*\swidth="(\d+(?:\.\d+)?)"[^>]*\sheight="(\d+(?:\.\d+)?)"[^>]*>)/,
        (_match: string, _tag: string, w: string, h: string) => {
          const nw = parseFloat(w) + EXPORT_PAD * 2;
          const nh = parseFloat(h) + EXPORT_PAD * 2;
          return (
            _tag
              .replace(/width="[^"]*"/, `width="${nw}"`)
              .replace(/height="[^"]*"/, `height="${nh}"`) +
            `<rect width="${nw}" height="${nh}" fill="${EXPORT_BG}"/><g transform="translate(${EXPORT_PAD},${EXPORT_PAD})">`
          );
        },
      )
      .replace("</svg>", "</g></svg>");
    downloadBlob(new Blob([padded], { type: "image/svg+xml" }), "erd.svg");
  }, [EXPORT_BG]);

  const exportPng = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    addRasterPadding(
      cy.png({ output: "blob", full: true, bg: EXPORT_BG }) as unknown as Blob,
      "image/png",
      1,
      "erd.png",
    );
  }, [EXPORT_BG, addRasterPadding]);

  const exportJpeg = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    addRasterPadding(
      cy.jpg({ output: "blob", full: true, bg: EXPORT_BG, quality: 0.92 }) as unknown as Blob,
      "image/jpeg",
      0.92,
      "erd.jpg",
    );
  }, [EXPORT_BG, addRasterPadding]);

  // REQ-524: the diagram as data — built by the same function the ERD renders from, with the
  // panel's current collapse/hide/detail state, so it describes exactly what is drawn.
  const exportJson = useCallback(() => {
    const elements = buildErdElements(
      tables,
      relationships,
      domains,
      collapsedDomains,
      hiddenDomains,
      columnDetail,
    );
    const body = JSON.stringify(erdJson(elements), null, 2);
    downloadBlob(new Blob([body], { type: "application/json" }), "erd.json");
  }, [tables, relationships, domains, collapsedDomains, hiddenDomains, columnDetail]);

  // ── collapse all / expand all (visible domains only) ─────────────────────
  const visibleDomainIds = allDomainIds.filter((id) => !hiddenDomains.has(id));
  const allCollapsed =
    visibleDomainIds.length > 0 && visibleDomainIds.every((id) => collapsedDomains.has(id));
  const toggleAll = () => {
    onArrange?.();
    setCollapsedDomains(allCollapsed ? new Set() : new Set(visibleDomainIds));
  };
  useErdCollapseLink(
    collapsed,
    onCollapsed,
    visibleDomainIds,
    collapsedDomains,
    setCollapsedDomains,
  );
  // REQ-2189: what a save reads off the canvas.
  useEffect(() => {
    if (!placesRef) return;
    placesRef.current = () =>
      cyRef.current === null ? null : readPlaces(cyRef.current, options, collapsedDomains);
  }, [placesRef, options, collapsedDomains]);

  return (
    <div
      data-testid="erd-panel"
      data-highlighted={highlighted.join(",")}
      style={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
        background: palette.bg,
      }}
    >
      <>
        {/* ── header ── */}
        <Group
          gap="0.5rem"
          wrap="wrap"
          data-tour="rels-erd-modal"
          style={{
            padding: "0.6rem 0.75rem",
            borderBottom: `1px solid ${palette.panelBorder}`,
            flexShrink: 0,
          }}
        >
          {layoutMenu}

          {/* column detail select */}
          <Select
            aria-label={t("erdModal.columnDetailLabel")}
            value={columnDetail}
            onChange={(v) => v && setOptions({ columnDetail: v as ColumnDetail })}
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
            onChange={(v) => v && setOptions({ edgeRouting: v as "bezier" | "taxi" })}
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
            onChange={(v) => v && setOptions({ gridSnap: parseInt(v, 10) })}
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
          <Group gap="xs" h={30} align="center" wrap="nowrap">
            <Checkbox
              label={t("erdModal.orphans")}
              checked={showOrphans}
              onChange={(e) => setOptions({ showOrphans: e.currentTarget.checked })}
              size="xs"
              styles={{ label: { color: palette.text } }}
              data-testid="erd-show-orphans"
            />
          </Group>

          {/* collapse/expand all */}
          <Tip text={allCollapsed ? t("erdModal.expandAll") : t("erdModal.collapseAll")}>
            <Button
              size="xs"
              h={30}
              variant="default"
              onClick={toggleAll}
              leftSection={allCollapsed ? <ChevronRight size={12} /> : <ChevronDown size={12} />}
              data-testid="erd-toggle-all"
            >
              {allCollapsed ? t("erdModal.expandAll") : t("erdModal.collapseAll")}
            </Button>
          </Tip>

          {/* How the domain boxes work: in the row's (?) help, last and at its end, so nothing is
              drawn over the diagram. */}
          {allDomainIds.length > 0 && (
            <Group ml="auto">
              <HelpBubble
                title={t("erdModal.title")}
                paragraphs={[t("erdModal.hint")]}
                ariaLabel={t("erdModal.hint")}
                testId="erd-hint"
              />
            </Group>
          )}
        </Group>

        {/* ── canvas ── */}
        <div
          ref={stageRef}
          className="erd-stage"
          style={{ flex: 1, position: "relative", background: palette.bg }}
        >
          <div ref={setContainerNode} style={{ width: "100%", height: "100%" }} />
          <ErdCanvasBar
            cyRef={cyRef}
            stageRef={stageRef}
            exports={{ svg: exportSvg, png: exportPng, jpeg: exportJpeg, json: exportJson }}
          />

          {overlay}

          {/* domain resize handles */}
          {resizeHandleBox && (
            <div
              style={{
                position: "absolute",
                left: resizeHandleBox.x,
                top: resizeHandleBox.y,
                width: resizeHandleBox.w,
                height: resizeHandleBox.h,
                border: `2px dashed ${palette.accent}`,
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
                  onMouseEnter={() => {
                    handleHoverRef.current = true;
                  }}
                  onMouseLeave={() => {
                    handleHoverRef.current = false;
                    if (!resizeDragRef.current) {
                      setHoveredDomainId(null);
                      setResizeHandleBox(null);
                    }
                  }}
                  style={{
                    position: "absolute",
                    width: 16,
                    height: 16,
                    background: palette.accent,
                    borderRadius: 2,
                    cursor: corner === "se" || corner === "nw" ? "nwse-resize" : "nesw-resize",
                    pointerEvents: "all",
                    ...(corner === "nw"
                      ? { top: -8, left: -8 }
                      : corner === "ne"
                        ? { top: -8, right: -8 }
                        : corner === "sw"
                          ? { bottom: -8, left: -8 }
                          : { bottom: -8, right: -8 }),
                  }}
                />
              ))}
            </div>
          )}

          {/* ── tooltip ── */}
          {tooltip.visible && (
            <div
              role="tooltip"
              style={{
                position: "absolute",
                left: tooltip.x,
                top: tooltip.y,
                background: palette.tooltipBg,
                border: `1px solid ${palette.tooltipBorder}`,
                borderRadius: 6,
                padding: "6px 10px",
                fontSize: 11,
                color: palette.text,
                maxWidth: 260,
                pointerEvents: "none",
                zIndex: 2000,
                boxShadow: "0 4px 12px rgba(0,0,0,0.4)",
              }}
            >
              <div style={{ fontWeight: 600, marginBottom: tooltip.body ? 4 : 0 }}>
                {tooltip.title}
              </div>
              {tooltip.body && (
                <div style={{ color: palette.textMuted, lineHeight: 1.4 }}>{tooltip.body}</div>
              )}
            </div>
          )}
        </div>
      </>
    </div>
  );
}

export type { ErdNodeDomain, ErdNodeTable };
