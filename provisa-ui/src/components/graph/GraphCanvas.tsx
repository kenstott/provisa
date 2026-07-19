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

import { useRef, useEffect, useLayoutEffect, useState, useCallback } from "react";
import { labelColor, darkenColor } from "./graph-model";
import type { GNode, GEdge, GraphStats } from "./graph-model";
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
import "./canvas/cytoscape-extensions";
import type { CanvasProps, NodeRingMenuState } from "./canvas/canvas-types";
import { computeLabelSizeRanges, applyNodeSize, resolveNodeLabel } from "./canvas/canvas-helpers";
import { HullSvgOverlay } from "./canvas/HullSvgOverlay";
import { CanvasControls } from "./canvas/CanvasControls";
import { NodeRingMenuOverlay } from "./canvas/NodeRingMenuOverlay";
import { useGraphLayout } from "./canvas/use-graph-layout";

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
  onToggleChildren,
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
  // Track nodes that the user has manually dragged; these stay anchored during re-layout
  const anchoredRef = useRef<Set<string>>(new Set());
  // Prevents concurrent layout runs from clobbering each other's unlock step
  const layoutRunningRef = useRef(false);
  // Tracks the active cytoscape layout object so it can be stopped before starting a new one
  const activeLayoutRef = useRef<{ stop: () => void } | null>(null);
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

  // Stable ref so computeHulls can be passed to useGraphLayout before the callback is defined
  const computeHullsRef = useRef<() => void>(() => {});

  const circularChildParentsRef = useRef(showingChildrenCircular);
  circularChildParentsRef.current = showingChildrenCircular;
  const circularParentNodesRef = useRef(showingParentsCircular);
  circularParentNodesRef.current = showingParentsCircular;

  const {
    layoutMode,
    edgeDistance,
    setEdgeDistance,
    edgeDistanceRef,
    nudgeTimerRef,
    nudgeHeldRef,
    pendingNudgesRef,
    pendingNudgeFreeNodesRef,
    nudgeLayoutRef,
    runLayout,
    nudgeLayout,
    toggleLayout,
  } = useGraphLayout({
    cyRef,
    layoutRunningRef,
    activeLayoutRef,
    anchoredRef,
    portEdgesAddedRef,
    nodesRef,
    edgesRef,
    overlayEdgesRef,
    collapsedClustersRef,
    clusterLevelRef,
    sizeByPropertyRef,
    colorOverridesRef,
    labelPropertyRef,
    sizeMultiplierRef,
    sizeOverridesRef,
    computeHullsRef,
  });

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
  // Keep computeHullsRef in sync so the layout hook always calls the latest version
  computeHullsRef.current = computeHulls;

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
  const [nodeRingMenu, setNodeRingMenu] = useState<NodeRingMenuState | null>(null);
  const [hoveredSector, setHoveredSector] = useState<string | null>(null);

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

  // Declared before the full-rebuild effect below because its cleanup resets these refs.
  const prevOverlayNodesRef = useRef<Map<string, GNode>>(new Map());
  const prevOverlayEdgesRef = useRef<Map<string, GEdge>>(new Map());

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
          const inDeg = Number(gn.properties.degIn ?? 0);
          const outDeg = Number(gn.properties.degOut ?? 0);
          const deg = inDeg + outDeg;
          graphStats = {
            inDegree: inDeg,
            outDegree: outDeg,
            degree: deg,
            degreeCentrality: totalReal > 1 ? parseFloat((deg / (totalReal - 1)).toFixed(4)) : 0,
            ...(gn.properties.scl1 != null ? { schema_L1: String(gn.properties.scl1) } : {}),
            ...(gn.properties.scl2 != null ? { schema_L2: String(gn.properties.scl2) } : {}),
            ...(gn.properties.scl3 != null ? { schema_L3: String(gn.properties.scl3) } : {}),
          };
        }
        if (gn) {
          const pos = evt.renderedPosition ?? evt.position;
          const isLocked = anchoredRef.current.has(evt.target.id() as string);
          setNodeRingMenu({ x: pos.x, y: pos.y, nodeKey: evt.target.id() as string, node: gn, graphStats, isLocked });
          onSelect({ kind: "node", data: gn, graphStats });
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
    if (els.length > 0) runLayout();
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
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- userZoomingEnabled is a runtime cytoscape method not present on the typed Core surface
    (cyRef.current as any)?.userZoomingEnabled(!!isExpanded);
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
      <HullSvgOverlay
        hullCircles={hullCircles}
        hullSvgRef={hullSvgRef}
        clusterLevelRef={clusterLevelRef}
        cyRef={cyRef}
        hullDragRef={hullDragRef}
        toggleCollapse={toggleCollapse}
      />
      <CanvasControls
        cyRef={cyRef}
        layoutMode={layoutMode}
        toggleLayout={toggleLayout}
        nudgeHeldRef={nudgeHeldRef}
        nudgeLayout={nudgeLayout}
        nudgeLayoutRef={nudgeLayoutRef}
        edgeDistance={edgeDistance}
        setEdgeDistance={setEdgeDistance}
        edgeDistanceRef={edgeDistanceRef}
        nudgeTimerRef={nudgeTimerRef}
      />
      {nodeRingMenu && (
        <NodeRingMenuOverlay
          nodeRingMenu={nodeRingMenu}
          setNodeRingMenu={setNodeRingMenu}
          hoveredSector={hoveredSector}
          setHoveredSector={setHoveredSector}
          cyRef={cyRef}
          anchoredRef={anchoredRef}
          nudgeLayoutRef={nudgeLayoutRef}
          showingChildrenNatural={showingChildrenNatural}
          onToggleChildren={onToggleChildren}
        />
      )}
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
