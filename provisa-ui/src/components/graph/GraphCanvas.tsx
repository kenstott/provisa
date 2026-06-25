// Copyright (c) 2026 Kenneth Stott
// Canary: b875b07b-1bcc-4c99-8a73-164a5ce03713
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/* eslint-disable react-hooks/refs --
   GraphCanvas is an imperative Cytoscape integration: latest-value refs mirror
   props/state for cytoscape event callbacks, and prev-value refs gate documented
   render-phase adjustments. Ref reads during render are intrinsic to driving the
   imperative graph engine and are intentional throughout this module */

import { useRef, useEffect, useLayoutEffect, useState, useCallback, type ReactNode } from "react";
import type { Relationship } from "../../types/admin";
import { labelColor, darkenColor, clusterColor } from "./graph-model";
import type { GNode, GEdge, GraphStats, RelLineOverride } from "./graph-model";
import { buildClusterElements, buildClusterMetaEdges, cidToId, type ClusterLevel } from "./graph-clusters";
import { buildGraphStylesheet } from "./graph-stylesheet";
import { NodeContextMenu, type NodeCtxMenuState } from "./NodeContextMenu";
import type {
  CyLayoutOptions,
  CyElementDefinition,
  CyElement,
  CyCollection,
  CyInstance,
} from "./cytoscape-types";
import cytoscape from "cytoscape";
import fcoseRaw from "cytoscape-fcose";
import layoutUtilitiesRaw from "cytoscape-layout-utilities";
import cytoscapeSvgRaw from "cytoscape-svg";
// CJS bundles — .default may or may not be present depending on bundler
type CyExt = Parameters<typeof cytoscape.use>[0];
type CyExtModule = { default?: CyExt } | CyExt;
const _interopExt = (m: CyExtModule): CyExt => (m as { default?: CyExt }).default ?? (m as CyExt);
const fcose = _interopExt(fcoseRaw as CyExtModule);
const layoutUtilities = _interopExt(layoutUtilitiesRaw as CyExtModule);
const cytoscapeSvg = _interopExt(cytoscapeSvgRaw as CyExtModule);
try {
  cytoscape.use(fcose);
} catch {
  /* already registered */
}
try {
  cytoscape.use(layoutUtilities);
} catch {
  /* already registered */
}
try {
  cytoscape.use(cytoscapeSvg);
} catch {
  /* already registered */
}

function resolveNodeLabel(n: GNode): string {
  if ("name" in n.properties) return String(n.properties["name"]);
  const nameKey = Object.keys(n.properties).find((k) => k.toLowerCase().includes("name"));
  if (nameKey) return String(n.properties[nameKey]);
  if ("title" in n.properties) return String(n.properties["title"]);
  return String(n.id);
}

interface CanvasProps {
  nodes: Map<string, GNode>;
  edges: Map<string, GEdge>;
  overlayNodes: Map<string, GNode>;
  overlayEdges: Map<string, GEdge>;
  onSelect: (item: { kind: "node"; data: GNode; graphStats?: GraphStats } | { kind: "edge"; data: GEdge } | null) => void;
  colorOverrides: Record<string, string>;
  sizeOverrides: Record<string, number>;
  labelProperty: Record<string, string>;
  sizeByProperty: Record<string, string>;
  sizeMultiplier: Record<string, number>;
  relLineOverrides: Record<string, RelLineOverride>;
  onExcludeNode: (nodeKeys: string[]) => void;
  pkMap: Record<string, string[]>;
  labelToTableLabel: Record<string, string>;
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
  isExpanded?: boolean;
}

type LayoutMode = "force" | "hierarchy";

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

function computeLabelSizeRanges(
  cy: CyInstance,
  sizeByProp: Record<string, string>,
): Map<string, { min: number; max: number }> {
  const ranges = new Map<string, { min: number; max: number }>();
  cy.nodes().forEach((nd) => {
    if (nd.data("_cluster") || nd.data("_port")) return;
    const lbl = nd.data("label") as string;
    const sby = sizeByProp[lbl];
    if (!sby) return;
    const gn = nd.data("_node") as GNode | undefined;
    if (!gn) return;
    const v = Number(gn.properties[sby]);
    if (isNaN(v)) return;
    const cur = ranges.get(sby);
    if (!cur) {
      ranges.set(sby, { min: v, max: v });
    } else {
      ranges.set(sby, { min: Math.min(cur.min, v), max: Math.max(cur.max, v) });
    }
  });
  return ranges;
}

function applyNodeSize(
  node: CyElement,
  lbl: string,
  gn: GNode | undefined,
  sizeByProp: Record<string, string>,
  sizeOverrides: Record<string, number>,
  sizeMultiplier: Record<string, number>,
  ranges: Map<string, { min: number; max: number }>,
): void {
  const base = sizeOverrides[lbl] ?? 44;
  const sby = sizeByProp[lbl];
  const multiplier = sizeMultiplier[lbl] ?? 3;
  const inCluster = node.data("_inCluster") as boolean;
  let sz: number;
  if (sby && gn) {
    const range = ranges.get(sby);
    const v = Number(gn.properties[sby]);
    if (range && !isNaN(v) && range.max > range.min) {
      const t = (v - range.min) / (range.max - range.min);
      sz = base * (1 + t * (multiplier - 1)) * (inCluster ? 0.5 : 1);
    } else {
      sz = inCluster ? base / 2 : base;
    }
  } else {
    sz = inCluster ? base / 2 : base;
  }
  node.style({ width: sz, height: sz, "text-max-width": `${sz - 4}px` });
}

export function GraphCanvas({
  nodes,
  edges,
  overlayNodes,
  overlayEdges,
  onSelect,
  colorOverrides,
  sizeOverrides,
  labelProperty,
  sizeByProperty,
  sizeMultiplier,
  relLineOverrides,
  onExcludeNode,
  pkMap,
  labelToTableLabel,
  relationships,
  showingChildrenNatural,
  onToggleChildrenBatch,
  showingChildrenCircular,
  showingParents,
  onToggleParentsBatch,
  showingParentsCircular,
  onCyReady,
  clusterLevel,
  hullSvgRef,
  isExpanded,
}: CanvasProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<CyInstance | null>(null);
  // Latest-value ref mirrors: cytoscape's imperative style/layout callbacks read
  // these refs (not closures) so they always see current prop values without
  // rebuilding the graph. Writing them during render is the standard mirror pattern.
  const colorOverridesRef = useRef(colorOverrides);
  colorOverridesRef.current = colorOverrides;
  const sizeOverridesRef = useRef(sizeOverrides);
  sizeOverridesRef.current = sizeOverrides;
  const labelPropertyRef = useRef(labelProperty);
  labelPropertyRef.current = labelProperty;
  const sizeByPropertyRef = useRef(sizeByProperty);
  sizeByPropertyRef.current = sizeByProperty;
  const sizeMultiplierRef = useRef(sizeMultiplier);
  sizeMultiplierRef.current = sizeMultiplier;
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
  const pendingNudgesRef = useRef(0);
  const pendingNudgeFreeNodesRef = useRef<Set<string> | undefined>(undefined);
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
  // Latest-value refs for nodes/edges used by computeHulls to build deferred meta-edges
  const nodesRef = useRef(nodes);
  nodesRef.current = nodes;
  const edgesRef = useRef(edges);
  edgesRef.current = edges;
  const overlayEdgesRef = useRef(overlayEdges);
  overlayEdgesRef.current = overlayEdges;
  // SVG hull ellipses drawn over the canvas for cluster visualization
  const [hullCircles, setHullCircles] = useState<
    Array<{ cid: string; x: number; y: number; rx: number; ry: number }>
  >([]);
  // Tracks whether meta-edges have been added to the current cy instance
  const portEdgesAddedRef = useRef(false);
  const [collapsedClusters, setCollapsedClusters] = useState<Set<string>>(new Set());
  const collapsedClustersRef = useRef<Set<string>>(new Set());
  const clusterLevelRef = useRef(clusterLevel);
  const hullDragRef = useRef<{
    cid: string;
    lastX: number;
    lastY: number;
    startX: number;
    startY: number;
  } | null>(null);
  clusterLevelRef.current = clusterLevel;
  // Reset collapsed state when cluster level changes
  useEffect(() => {
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       reset collapsed-cluster state synchronously when the clusterLevel prop
       changes; the previous level's collapse selections are meaningless under
       a new clustering */
    setCollapsedClusters(new Set());
    collapsedClustersRef.current = new Set();
  }, [clusterLevel]);
  const toggleCollapse = useCallback((cid: string) => {
    setCollapsedClusters((prev) => {
      const next = new Set(prev);
      if (next.has(cid)) next.delete(cid);
      else next.add(cid);
      collapsedClustersRef.current = next;
      return next;
    });
  }, []);
  const computeHulls = useCallback(() => {
    const cy = cyRef.current;
    if (!cy || clusterLevelRef.current === "none") {
      setHullCircles([]);
      return;
    }
    const collapsed = collapsedClustersRef.current;
    const zoom = cy.zoom();
    const pan = cy.pan();
    const hulls: Array<{ cid: string; x: number; y: number; rx: number; ry: number }> = [];
    cy.nodes("[?_cluster]").forEach((cn) => {
      const cid = cn.data("_clusterId") as string;
      if (collapsed.has(cid)) return;

      // Compute hull from children positions in graph coords.
      const children = cn.children();
      if (children.length === 0) return;
      let minX_g = Infinity, maxX_g = -Infinity, minY_g = Infinity, maxY_g = -Infinity;
      children.forEach((c) => {
        const p = c.position();
        const hw = c.width() / 2 + 6;
        const hh = c.height() / 2 + 6;
        minX_g = Math.min(minX_g, p.x - hw);
        maxX_g = Math.max(maxX_g, p.x + hw);
        minY_g = Math.min(minY_g, p.y - hh);
        maxY_g = Math.max(maxY_g, p.y + hh);
      });
      const cx_g = (minX_g + maxX_g) / 2;
      const cy_g = (minY_g + maxY_g) / 2;
      // raw half-extents from bounding box
      const raw_rx = Math.max(1, (maxX_g - minX_g) / 2);
      const raw_ry = Math.max(1, (maxY_g - minY_g) / 2);
      // Scale so every node corner lies inside the ellipse: find max ellipse-distance across all corners
      let maxScale = 1;
      children.forEach((c) => {
        const p = c.position();
        const hw = c.width() / 2 + 6;
        const hh = c.height() / 2 + 6;
        for (const dx of [p.x - cx_g - hw, p.x - cx_g + hw]) {
          for (const dy of [p.y - cy_g - hh, p.y - cy_g + hh]) {
            maxScale = Math.max(maxScale, Math.sqrt((dx / raw_rx) ** 2 + (dy / raw_ry) ** 2));
          }
        }
      });
      const rx_g = Math.max(30, raw_rx * maxScale + 20);
      const ry_g = Math.max(30, raw_ry * maxScale + 20);

      hulls.push({
        cid,
        x: cx_g * zoom + pan.x,
        y: cy_g * zoom + pan.y,
        rx: rx_g * zoom,
        ry: ry_g * zoom,
      });

      // Skip port node create/update while layout is running — nudge removes port nodes before
      // fcose starts, and a viewport event fired by the centroid shift must not recreate them
      // or they will participate in the layout as oversized virtual nodes.
      if (!layoutRunningRef.current) {
        const portId = `__port_${clusterLevelRef.current}_${cidToId(cid)}`;
        let portNode = cy.$id(portId);
        if (portNode.length === 0) {
          portNode = cy.add({
            group: "nodes",
            data: {
              id: portId,
              label: "",
              _port: true,
              _clusterId: cid,
              _clusterLevel: clusterLevelRef.current,
            },
            position: { x: cx_g, y: cy_g },
          }) as unknown as CyCollection;
        } else {
          if (portNode.locked()) portNode.unlock();
          portNode.position({ x: cx_g, y: cy_g });
        }
        portNode.style({ width: rx_g * 2, height: ry_g * 2 });
      }
    });
    setHullCircles(hulls);

    // Swap from layout meta-edges (compound AABB routing) to port meta-edges (ellipse routing).
    // Skip if layout is running — nudge removes ports and adds layout meta-edges just before
    // running fcose; a viewport event fired by the centroid shift would otherwise re-add port
    // meta-edges mid-setup and corrupt the nudge edge graph.
    if (!portEdgesAddedRef.current && !layoutRunningRef.current) {
      cy.edges("[?_metaEdge]").remove();
      const metaEdges = buildClusterMetaEdges(
        nodesRef.current,
        edgesRef.current,
        clusterLevelRef.current as Exclude<ClusterLevel, "none">,
        overlayEdgesRef.current,
        collapsedClustersRef.current,
        true,
      );
      if (metaEdges.length > 0) {
        cy.add(metaEdges);
      }
      portEdgesAddedRef.current = true;
    }
  }, []);
  useEffect(() => {
    const onMove = (e: MouseEvent) => {
      const drag = hullDragRef.current;
      if (!drag) return;
      const cy = cyRef.current;
      if (!cy) return;
      const zoom = cy.zoom();
      const dx = (e.clientX - drag.lastX) / zoom;
      const dy = (e.clientY - drag.lastY) / zoom;
      const clusterId = `__cluster_${clusterLevelRef.current}_${cidToId(drag.cid)}`;
      cy.getElementById(clusterId)
        .children()
        .forEach((n) => {
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
      if (dist < 5) {
        toggleCollapse(drag.cid);
      } else {
        const cy = cyRef.current;
        if (cy) {
          const clusterId = `__cluster_${clusterLevelRef.current}_${cidToId(drag.cid)}`;
          cy.getElementById(clusterId).children().forEach((n) => {
            anchoredRef.current.add(n.id() as string);
            n.addClass("pinned");
          });
          // Skip gravity layout — it pulls surrounding clusters toward the dragged
          // one and produces a skewed result. Directly recompute hull/port positions.
          if (portEdgesAddedRef.current) {
            cy.nodes("[?_port]").remove();
            cy.edges("[?_metaEdge]").remove();
            portEdgesAddedRef.current = false;
          }
          computeHulls();
        }
      }
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
    return () => {
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
  }, [computeHulls, toggleCollapse]);
  // Node right-click context menu
  const [nodeCtxMenu, setNodeCtxMenu] = useState<NodeCtxMenuState | null>(null);
  const menuRef = useRef<HTMLDivElement>(null);

  // Node left-click ring menu
  type NodeRingMenuState = { x: number; y: number; nodeKey: string; node: GNode; graphStats?: GraphStats; isLocked: boolean };
  const [nodeRingMenu, setNodeRingMenu] = useState<NodeRingMenuState | null>(null);
  const ringMenuRef = useRef<HTMLDivElement>(null);

  // Track node position as viewport changes (pan / zoom)
  useEffect(() => {
    if (!nodeRingMenu || !cyRef.current) return;
    const cy = cyRef.current;
    const update = () => {
      if (!ringMenuRef.current) return;
      const cyNode = cy.$id(nodeRingMenu.nodeKey);
      if (!cyNode || (cyNode as unknown as { length: number }).length === 0) return;
      const rp = (cyNode as unknown as { renderedPosition: () => { x: number; y: number } }).renderedPosition();
      const zoom = cy.zoom();
      const size = Math.round(Math.max(80, Math.min(240, 120 * zoom)));
      ringMenuRef.current.style.left = `${rp.x}px`;
      ringMenuRef.current.style.top = `${rp.y}px`;
      const svg = ringMenuRef.current.querySelector<SVGSVGElement>("svg");
      if (svg) { svg.setAttribute("width", String(size)); svg.setAttribute("height", String(size)); }
    };
    update();
    cy.on("viewport", update);
    return () => { cy.off("viewport", update); };
  }, [nodeRingMenu]);

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
    cy.nodes().forEach((n) => {
      if (anchored.has(n.id())) n.lock();
    });
    // For force mode with no edges, use grid — it fills the canvas rectangle optimally.
    // For force mode with edges, use fcose — clusters connected components.
    // For hierarchy mode, always use breadthfirst.
    let opts: CyLayoutOptions;
    if (cy.nodes().length === 0) {
      opts = { name: "null" } as CyLayoutOptions;
    } else if (m === "hierarchy") {
      opts = LAYOUT_OPTIONS.hierarchy;
    } else if (cy.edges().length === 0) {
      // No relationships — grid sorted by label so same-type nodes occupy the same rows.
      const labelOrder: string[] = [];
      const labelCount: Record<string, number> = {};
      cy.nodes().forEach((n) => {
        const lbl = n.data("label") as string;
        if (!(lbl in labelCount)) { labelOrder.push(lbl); labelCount[lbl] = 0; }
        labelCount[lbl]++;
      });
      const maxPerLabel = Math.max(...Object.values(labelCount), 1);
      const totalNodes = cy.nodes().length;
      const sqrtCols = Math.ceil(Math.sqrt(totalNodes));
      const cols = Math.min(maxPerLabel, sqrtCols);
      opts = {
        name: "grid",
        animate: false,
        fit: true,
        padding: 30,
        avoidOverlap: true,
        avoidOverlapPadding: 12,
        cols,
        sort: (a: { data: (k: string) => string }, b: { data: (k: string) => string }) => {
          const ai = labelOrder.indexOf(a.data("label"));
          const bi = labelOrder.indexOf(b.data("label"));
          return ai - bi;
        },
      } as CyLayoutOptions;
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
        const labelSizeRange = computeLabelSizeRanges(cy, sizeByPropertyRef.current);
        cy.batch(() => {
          cy.nodes().forEach((node) => {
            if (node.data("_cluster")) return;
            if (node.data("_collapsed")) return;
            if (node.data("_port")) return;
            const lbl = node.data("label") as string;
            const n = node.data("_node") as GNode | undefined;
            const base = colorOverridesRef.current[lbl] ?? labelColor(lbl);
            node.style("background-color", base);
            applyNodeSize(node, lbl, n, sizeByPropertyRef.current, sizeOverridesRef.current, sizeMultiplierRef.current, labelSizeRange);
            if (n) {
              const prop = labelPropertyRef.current[n.label];
              node.style(
                "label",
                prop
                  ? String(n.properties[prop] ?? n.id)
                  : resolveNodeLabel(n),
              );
            }
            if (anchoredRef.current.has(node.id() as string)) node.addClass("pinned");
            else node.removeClass("pinned");
          });
        });
      } catch {
        /* cy may have been destroyed */
      }
    };
    const releaseRun = () => {
      try {
        cy.nodes().forEach((n) => {
          if (anchored.has(n.id())) n.unlock();
        });
      } catch {
        /* cy may have been destroyed */
      }
      layoutRunningRef.current = false;
    };

    const safetyTimer = setTimeout(() => {
      applyStyles();
      releaseRun();
      try {
        cy.fit(undefined, 40);
      } catch {
        /* cy may have been destroyed */
      }
    }, 1000);
    layout.one("layoutstop", () => {
      clearTimeout(safetyTimer);
      applyStyles();
      releaseRun();
      try {
        cy.fit(undefined, 40);
      } catch {
        /* cy may have been destroyed */
      }
    });
    layout.run();
  }, []);

  const nudgeLayout = useCallback(
    (freeNodes?: Set<string>, aggressive = false) => {
      const cy = cyRef.current;
      if (!cy) return;
      if (layoutRunningRef.current) return;
      if (cy.edges().length === 0 || layoutModeRef.current === "hierarchy") {
        runLayout();
        return;
      }
      layoutRunningRef.current = true;
      try {
        // Remove port nodes + port meta-edges before nudge so they don't consume layout space
        // or show edges at stale positions during animation. Restore layout meta-edges so fcose
        // has correct cross-cluster attraction forces during the nudge.
        if (portEdgesAddedRef.current && clusterLevelRef.current !== "none") {
          cy.nodes("[?_port]").remove(); // also removes attached port meta-edges
          const level = clusterLevelRef.current as Exclude<ClusterLevel, "none">;
          const layoutMeta = buildClusterMetaEdges(
            nodesRef.current, edgesRef.current, level,
            overlayEdgesRef.current, collapsedClustersRef.current, false,
          );
          if (layoutMeta.length > 0) cy.add(layoutMeta);
          portEdgesAddedRef.current = false;
        }
        const anchored = anchoredRef.current;
        // When anchored nodes exist (user-dragged group), lock them in place and use
        // gravity=0 so FCose doesn't pull them toward (0,0). Edge forces alone will
        // attract connected clusters toward the pinned group.
        const gravityValue = anchored.size > 0 ? 0 : 0.25;
        // When freeNodes is provided, lock everything except those nodes (and unlock anchored after)
        const tempLocked = new Set<string>();
        cy.nodes().forEach((n) => {
          const id = n.id() as string;
          if (freeNodes && !freeNodes.has(id)) {
            if (!n.locked()) {
              n.lock();
              tempLocked.add(id);
            }
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
          gravity: gravityValue,
          fit: false,
          // Prevent FCose component packing from repositioning the entire component
          // containing anchored (locked) nodes, which would override their locked positions
          // and displace the dragged group off-screen after centroid shift.
          packComponents: false,
        } as CyLayoutOptions;
        const layout = cy.layout(opts);
        activeLayoutRef.current = layout;
        const applyStylesNudge = () => {
          try {
            const labelSizeRange = computeLabelSizeRanges(cy, sizeByPropertyRef.current);
            cy.batch(() => {
              cy.nodes().forEach((node) => {
                if (node.data("_cluster")) return;
                if (node.data("_port")) return;
                const lbl = node.data("label") as string;
                const n = node.data("_node") as GNode | undefined;
                const base = colorOverridesRef.current[lbl] ?? labelColor(lbl);
                node.style("background-color", base);
                applyNodeSize(node, lbl, n, sizeByPropertyRef.current, sizeOverridesRef.current, sizeMultiplierRef.current, labelSizeRange);
                if (n) {
                  const prop = labelPropertyRef.current[n.label];
                  node.style(
                    "label",
                    prop
                      ? String(n.properties[prop] ?? n.id)
                      : resolveNodeLabel(n),
                  );
                }
                if (anchoredRef.current.has(node.id() as string)) node.addClass("pinned");
                else node.removeClass("pinned");
              });
            });
          } catch {
            /* cy may have been destroyed */
          }
        };
        const animDuration = aggressive ? 2000 : 600;
        const releaseNudge = () => {
          try {
            tempLocked.forEach((id) => {
              const n = cy.$id(id);
              if (n.length > 0) n.unlock();
            });
            cy.nodes().forEach((n) => {
              if (anchored.has(n.id())) n.unlock();
            });
          } catch {
            /* cy may have been destroyed */
          }
          layoutRunningRef.current = false;
          if (nudgeHeldRef.current) {
            nudgeLayoutRef.current(undefined, true);
          } else if (pendingNudgesRef.current > 0) {
            pendingNudgesRef.current -= 1;
            nudgeLayoutRef.current(pendingNudgeFreeNodesRef.current);
          } else {
            // layoutstop may fire before animation finishes; final realign after animation settles
            setTimeout(() => { if (!layoutRunningRef.current) computeHulls(); }, animDuration + 50);
          }
        };
        const safetyTimerNudge = setTimeout(
          () => {
            applyStylesNudge();
            releaseNudge();
          },
          aggressive ? 3000 : 1000,
        );
        layout.one("layoutstop", () => {
          clearTimeout(safetyTimerNudge);
          applyStylesNudge();
          releaseNudge();
        });
        layout.run();
      } catch {
        layoutRunningRef.current = false;
      }
    },
    [runLayout],
  );

  // Keep ref in sync so the cytoscape "free" event always calls the latest nudgeLayout
  useEffect(() => {
    nudgeLayoutRef.current = nudgeLayout;
  }, [nudgeLayout]);

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
    const els: CyElementDefinition[] =
      clusterLevel !== "none"
        ? [
            ...buildClusterElements(nodes, edges, clusterLevel, overlayEdges, collapsedClusters),
            ...buildClusterMetaEdges(nodes, edges, clusterLevel, overlayEdges, collapsedClusters, false),
          ]
        : (() => {
            const _els: CyElementDefinition[] = [];
            nodes.forEach((n) => {
              _els.push({
                group: "nodes",
                data: { id: `${n.label}:${n.id}`, label: n.label, _node: n },
              });
            });
            edges.forEach((e) => {
              const srcKey = `${e.startNode.label}:${e.startNode.id}`;
              const tgtKey = `${e.endNode.label}:${e.endNode.id}`;
              if (nodes.has(srcKey) && nodes.has(tgtKey)) {
                _els.push({
                  group: "edges",
                  data: { id: e.identity, source: srcKey, target: tgtKey, label: e.type, _edge: e },
                });
              }
            });
            return _els;
          })();

    const cy = cytoscape({
      container: containerRef.current,
      elements: els,
      style: buildGraphStylesheet({
        colorOverridesRef,
        sizeOverridesRef,
        labelPropertyRef,
        relLineOverridesRef,
      }),
      layout: { name: "null" } as CyLayoutOptions,
      minZoom: 0.05,
      maxZoom: 8,
      userZoomingEnabled: false,
      /* eslint-disable-next-line @typescript-eslint/no-explicit-any --
         cytoscape() returns the library's own Core type; we cast through any to our local CyInstance shim which models only the subset of the API this component uses */
    }) as any as CyInstance;

    cy.on("tap", "node[?_collapsed]", (evt) => {
      const cid = evt.target.data("_clusterId") as string;
      if (cid) toggleCollapse(cid);
    });
    cy.on("tap", "node", (evt) => {
      setNodeCtxMenu(null);
      if (!evt.target.data("_collapsed")) {
        const gn = evt.target.data("_node") as GNode | undefined;
        let graphStats: GraphStats | undefined;
        if (gn) {
          const totalReal = nodesRef.current.size;
          const inDeg = Number(gn.properties.deg_in ?? 0);
          const outDeg = Number(gn.properties.deg_out ?? 0);
          const deg = inDeg + outDeg;
          graphStats = {
            in_degree: inDeg,
            out_degree: outDeg,
            degree: deg,
            degree_centrality: totalReal > 1 ? parseFloat((deg / (totalReal - 1)).toFixed(4)) : 0,
            ...(gn.properties.scl1 != null ? { schema_L1: String(gn.properties.scl1) } : {}),
            ...(gn.properties.scl2 != null ? { schema_L2: String(gn.properties.scl2) } : {}),
            ...(gn.properties.scl3 != null ? { schema_L3: String(gn.properties.scl3) } : {}),
          };
        }
        if (gn) {
          const pos = evt.renderedPosition ?? evt.position;
          const isLocked = anchoredRef.current.has(evt.target.id() as string);
          setNodeRingMenu({ x: pos.x, y: pos.y, nodeKey: evt.target.id() as string, node: gn, graphStats, isLocked });
        }
      }
    });
    cy.on("tap", "edge", (evt) => {
      setNodeCtxMenu(null);
      const edgeData = evt.target.data("_edge") as GEdge | undefined;
      if (edgeData) onSelect({ kind: "edge", data: edgeData });
    });
    cy.on("tap", (evt) => {
      if (evt.target === cy) {
        onSelect(null);
        setNodeCtxMenu(null);
        setNodeRingMenu(null);
      }
    });
    cy.on("cxttap", "node", (evt) => {
      const pos = evt.renderedPosition ?? evt.position;
      const clickedId = evt.target.id() as string;
      const selectedIds = cy.$("node:selected").map((n) => n.id() as string);
      const selectedNodeIds =
        selectedIds.includes(clickedId) && selectedIds.length > 1 ? selectedIds : [clickedId];
      setNodeCtxMenu({ x: pos.x, y: pos.y, nodeId: clickedId, selectedNodeIds });
    });
    cy.on("cxttap", (evt) => {
      if (evt.target === cy) setNodeCtxMenu(null);
    });
    cy.on("mouseover", "node", (evt) => { evt.target.addClass("hovered"); });
    cy.on("mouseout", "node", (evt) => { evt.target.removeClass("hovered"); });
    cy.on("mouseover", "edge", (evt) => { evt.target.addClass("hovered"); });
    cy.on("mouseout", "edge", (evt) => { evt.target.removeClass("hovered"); });
    cy.on("layoutstop", () => { if (pendingNudgesRef.current === 0) computeHulls(); });
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
      pendingNudgesRef.current = 2;
      pendingNudgeFreeNodesRef.current = undefined;
      nudgeLayoutRef.current();
    });

    portEdgesAddedRef.current = false;
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
      portEdgesAddedRef.current = false;
      // Reset prev-overlay refs so the incremental effect re-adds ALL current overlay
      // nodes after the cytoscape instance is rebuilt (e.g. when overlayEdges changes).
      prevOverlayNodesRef.current = new Map();
      prevOverlayEdgesRef.current = new Map();
    };
    /* eslint-disable-next-line react-hooks/exhaustive-deps --
       rebuild the cytoscape instance only when base graph data or clustering changes; overlayEdges
       is intentionally excluded — it is handled by the incremental overlay effect below, which adds
       imputed edges without destroying/recreating cy (avoiding a race where the full rebuild starts
       a layout with layoutRunningRef=true before the incremental effect can call nudgeLayout) */
  }, [nodes, edges, clusterLevel, collapsedClusters]);

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
            cy.add({
              group: "edges",
              data: { id: e.identity, source: srcKey, target: tgtKey, label: e.type, _edge: e },
            });
          }
        }
      });
    });

    // Arrange circular children in a ring around their parents; lock so layout won't move them
    const newCyIds = new Set<string>();
    overlayNodes.forEach((n, k) => {
      if (!prevNodes.has(k)) newCyIds.add(`${n.label}:${n.id}`);
    });
    circularChildParentsRef.current.forEach((parentId) => {
      const parentNode = cy.$id(parentId);
      if (parentNode.length === 0) return;
      const pos = parentNode.position();
      const r = edgeDistanceRef.current;
      const children = parentNode
        .neighborhood("node")
        .filter((n) => newCyIds.has(n.id() as string));
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
    const allNewAreCircular =
      hadNewNodes &&
      [...overlayNodes.keys()]
        .filter((k) => !prevNodes.has(k))
        .every((k) => {
          const n = overlayNodes.get(k)!;
          return cy.$id(`${n.label}:${n.id}`).locked();
        });

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
        const connected = cyNode
          .neighborhood("node")
          .filter((nb: CyElement) => !newCyIdsForNudge.has(nb.id() as string));
        if (connected.length > 0) {
          const parentPos = connected[0].position();
          const angle = Math.random() * 2 * Math.PI;
          const dist = edgeDistanceRef.current * (0.8 + Math.random() * 0.4);
          cyNode.position({
            x: parentPos.x + dist * Math.cos(angle),
            y: parentPos.y + dist * Math.sin(angle),
          });
        }
      });
    }

    prevOverlayNodesRef.current = new Map(overlayNodes);
    prevOverlayEdgesRef.current = new Map(overlayEdges);
    if ((hadNewNodes && !allNewAreCircular) || hadNewEdges)
      nudgeLayout(newCyIdsForNudge.size > 0 ? newCyIdsForNudge : undefined, true);
    /* eslint-disable-next-line react-hooks/exhaustive-deps --
       re-apply overlay nodes/edges only when the overlay data or clustering changes; the imperative apply helpers are stable refs and intentionally excluded */
  }, [overlayNodes, overlayEdges, clusterLevel]);

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
    const labelSizeRange = computeLabelSizeRanges(cy, sizeByPropertyRef.current);
    cy.batch(() => {
      cy.nodes().forEach((node) => {
        const lbl = node.data("label") as string;
        const gn = node.data("_node") as GNode | undefined;
        applyNodeSize(node, lbl, gn, sizeByPropertyRef.current, sizeOverridesRef.current, sizeMultiplierRef.current, labelSizeRange);
      });
    });
  }, [sizeOverrides, sizeByProperty, sizeMultiplier]);

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
          : resolveNodeLabel(n);
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

  useEffect(() => {
    cyRef.current?.userZoomingEnabled(!!isExpanded);
  }, [isExpanded]);

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
        <svg
          ref={hullSvgRef}
          style={{
            position: "absolute",
            inset: 0,
            width: "100%",
            height: "100%",
            pointerEvents: "none",
          }}
        >
          {hullCircles.map(({ cid, x, y, rx, ry }) => (
            <g key={cid}>
              <ellipse
                cx={x}
                cy={y}
                rx={rx}
                ry={ry}
                fill={clusterColor(cid)}
                fillOpacity={0.1}
                stroke={clusterColor(cid)}
                strokeWidth={8}
                strokeOpacity={0}
                style={{ pointerEvents: "stroke", cursor: "grab" }}
                onMouseDown={(e) => {
                  e.preventDefault();
                  const cy = cyRef.current;
                  if (cy) {
                    const clusterId = `__cluster_${clusterLevelRef.current}_${cidToId(cid)}`;
                    cy.getElementById(clusterId).children().forEach((n) => n.unlock());
                  }
                  hullDragRef.current = {
                    cid,
                    lastX: e.clientX,
                    lastY: e.clientY,
                    startX: e.clientX,
                    startY: e.clientY,
                  };
                }}
              />
              <ellipse
                cx={x}
                cy={y}
                rx={rx}
                ry={ry}
                fill="none"
                stroke={clusterColor(cid)}
                strokeWidth={1.5}
                strokeOpacity={0.75}
                style={{ pointerEvents: "none" }}
              />
              <text
                x={x}
                y={y - ry - 6}
                textAnchor="middle"
                fill={clusterColor(cid)}
                fontSize={11}
                fontWeight="bold"
                fontFamily="sans-serif"
                style={{ pointerEvents: "all", cursor: "pointer", userSelect: "none" }}
                onClick={() => toggleCollapse(cid)}
              >
                <title>Click to collapse group</title>
                {cid} ⊟
              </text>
            </g>
          ))}
        </svg>
      )}
      <div className="gf-canvas-controls">
        <button
          className="gf-ctrl-btn"
          onClick={() => cyRef.current?.zoom(cyRef.current.zoom() * 1.3)}
          title="Zoom in"
        >
          +
        </button>
        <button
          className="gf-ctrl-btn"
          onClick={() => cyRef.current?.zoom(cyRef.current.zoom() * 0.77)}
          title="Zoom out"
        >
          −
        </button>
        <button className="gf-ctrl-btn" onClick={fitView} title="Fit to screen">
          ⤢
        </button>
        <div className="gf-ctrl-divider" />
        <button
          className={`gf-ctrl-btn${layoutMode === "hierarchy" ? " active" : ""}`}
          onClick={toggleLayout}
          title={
            layoutMode === "force" ? "Switch to hierarchical layout" : "Switch to force layout"
          }
        >
          {layoutMode === "force" ? "⋮" : "⊟"}
        </button>
        <button
          className="gf-ctrl-btn"
          onMouseDown={() => {
            nudgeHeldRef.current = true;
            const cy = cyRef.current;
            const sel = cy ? cy.nodes(":selected").not("[?_cluster]") : null;
            const freeNodes = sel && sel.length > 0 ? new Set(sel.map((n) => n.id())) : undefined;
            nudgeLayout(freeNodes, true);
          }}
          onMouseUp={() => {
            nudgeHeldRef.current = false;
          }}
          onMouseLeave={() => {
            nudgeHeldRef.current = false;
          }}
          title="Nudge layout — nudges selected nodes (or all if none selected); hold to keep iterating"
        >
          ⟳
        </button>
        <div className="gf-ctrl-divider" />
        <label className="gf-ctrl-label" title="Edge length">
          ↔
        </label>
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
      {nodeRingMenu && (() => {
        const R1 = 26, R2 = 54;
        // Full 120° sector, no gap — separator lines drawn on top
        const arcSector = (centerDeg: number) => {
          const a1 = ((centerDeg - 60) * Math.PI) / 180;
          const a2 = ((centerDeg + 60) * Math.PI) / 180;
          const ox1 = R2 * Math.cos(a1), oy1 = R2 * Math.sin(a1);
          const ox2 = R2 * Math.cos(a2), oy2 = R2 * Math.sin(a2);
          const ix1 = R1 * Math.cos(a2), iy1 = R1 * Math.sin(a2);
          const ix2 = R1 * Math.cos(a1), iy2 = R1 * Math.sin(a1);
          return `M ${ox1.toFixed(2)} ${oy1.toFixed(2)} A ${R2} ${R2} 0 0 1 ${ox2.toFixed(2)} ${oy2.toFixed(2)} L ${ix1.toFixed(2)} ${iy1.toFixed(2)} A ${R1} ${R1} 0 0 0 ${ix2.toFixed(2)} ${iy2.toFixed(2)} Z`;
        };
        // Boundary angles between the 3 sectors (at ±60° from each center)
        const separatorLines = [90, 210, 330].map((deg) => {
          const rad = (deg * Math.PI) / 180;
          return { x1: R1 * Math.cos(rad), y1: R1 * Math.sin(rad), x2: R2 * Math.cos(rad), y2: R2 * Math.sin(rad) };
        });
        const midPos = (centerDeg: number) => {
          const r = (R1 + R2) / 2;
          const rad = (centerDeg * Math.PI) / 180;
          return { x: r * Math.cos(rad), y: r * Math.sin(rad) };
        };
        const sectors: { angle: number; key: string; title: string; active: boolean; iconPath: ReactNode }[] = [
          {
            angle: 270, key: "lock", active: nodeRingMenu.isLocked,
            title: nodeRingMenu.isLocked ? "Unlock position" : "Lock position",
            iconPath: (
              <>
                <rect x="-4" y="-1" width="8" height="6" rx="1" fill="currentColor"/>
                <path d="M-3-1 v-2 a3 3 0 0 1 6 0 v2" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
              </>
            ),
          },
          {
            angle: 30, key: "children", active: showingChildrenNatural.has(nodeRingMenu.nodeKey),
            title: showingChildrenNatural.has(nodeRingMenu.nodeKey) ? "Hide children" : "Show children",
            iconPath: (
              <>
                <circle cx="0" cy="-3.5" r="1.8" fill="currentColor"/>
                <circle cx="-3.5" cy="3" r="1.8" fill="currentColor"/>
                <circle cx="3.5" cy="3" r="1.8" fill="currentColor"/>
                <line x1="0" y1="-1.7" x2="-3.5" y2="1.2" stroke="currentColor" strokeWidth="1.1"/>
                <line x1="0" y1="-1.7" x2="3.5" y2="1.2" stroke="currentColor" strokeWidth="1.1"/>
              </>
            ),
          },
          {
            angle: 150, key: "exclude", active: false,
            title: "Remove node",
            iconPath: (
              <>
                <circle cx="0" cy="0" r="5" fill="none" stroke="currentColor" strokeWidth="1.5"/>
                <line x1="-2.8" y1="-2.8" x2="2.8" y2="2.8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
                <line x1="2.8" y1="-2.8" x2="-2.8" y2="2.8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
              </>
            ),
          },
        ];
        return (
          <>
            <div
              style={{ position: "absolute", inset: 0, zIndex: 899 }}
              onClick={() => setNodeRingMenu(null)}
            />
          <div
            ref={ringMenuRef}
            className="gf-node-ring-menu"
            style={{ left: nodeRingMenu.x, top: nodeRingMenu.y }}
          >
            <svg
              viewBox="-60 -60 120 120"
              width="120"
              height="120"
              style={{ overflow: "visible", display: "block", pointerEvents: "all" }}
            >
              {sectors.map(({ angle, key, title, active, iconPath }) => {
                const mp = midPos(angle);
                return (
                  <g
                    key={key}
                    style={{ cursor: "pointer" }}
                    onClick={(e) => {
                      e.stopPropagation();
                      if (key === "lock") {
                        const cy = cyRef.current;
                        if (!cy) return;
                        const cyNode = cy.$id(nodeRingMenu.nodeKey);
                        if (nodeRingMenu.isLocked) {
                          cyNode.unlock();
                          anchoredRef.current.delete(nodeRingMenu.nodeKey);
                          cyNode.removeClass("pinned");
                        } else {
                          cyNode.lock();
                          anchoredRef.current.add(nodeRingMenu.nodeKey);
                          cyNode.addClass("pinned");
                        }
                      } else if (key === "children") {
                        onToggleChildren(nodeRingMenu.nodeKey);
                      } else {
                        onExcludeNode([nodeRingMenu.nodeKey]);
                      }
                      setNodeRingMenu(null);
                    }}
                  >
                    <title>{title}</title>
                    <path
                      d={arcSector(angle)}
                      fill={active ? "rgba(99,102,241,0.35)" : "rgba(17,19,24,0.92)"}
                      stroke="#3a3d52"
                      strokeWidth="1"
                    />
                    <path d={arcSector(angle)} fill="transparent" stroke="transparent" strokeWidth="10"/>
                    <g
                      transform={`translate(${mp.x.toFixed(2)},${mp.y.toFixed(2)})`}
                      color={active ? "#a5b4fc" : "#9ca3af"}
                    >
                      {iconPath}
                    </g>
                  </g>
                );
              })}
              {separatorLines.map(({ x1, y1, x2, y2 }, i) => (
                <line key={i} x1={x1.toFixed(2)} y1={y1.toFixed(2)} x2={x2.toFixed(2)} y2={y2.toFixed(2)} stroke="#3a3d52" strokeWidth="1.5" style={{ pointerEvents: "none" }} />
              ))}
            </svg>
          </div>
          </>
        );
      })()}
      {nodeCtxMenu && (
        <NodeContextMenu
          menu={nodeCtxMenu}
          menuRef={menuRef}
          nodes={nodes}
          overlayNodes={overlayNodes}
          pkMap={pkMap}
          labelToTableLabel={labelToTableLabel}
          relationships={relationships}
          cyRef={cyRef}
          anchoredRef={anchoredRef}
          activeLayoutRef={activeLayoutRef}
          layoutRunningRef={layoutRunningRef}
          nudgeLayoutRef={nudgeLayoutRef}
          onExcludeNode={onExcludeNode}
          onToggleChildrenBatch={onToggleChildrenBatch}
          onToggleParentsBatch={onToggleParentsBatch}
          onSelect={onSelect}
          setNodeCtxMenu={setNodeCtxMenu}
          showingChildrenNatural={showingChildrenNatural}
          showingChildrenCircular={showingChildrenCircular}
          showingParents={showingParents}
          showingParentsCircular={showingParentsCircular}
        />
      )}
    </div>
  );
}
