// Copyright (c) 2026 Kenneth Stott
// Canary: 4c9d3e72-b1f8-4e2a-8c6d-f5a0b7e3d194
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useRef, useState, useCallback, useEffect, type MutableRefObject } from "react";
import { labelColor } from "../graph-model";
import type { GNode, GEdge } from "../graph-model";
import { buildClusterMetaEdges } from "../graph-clusters";
import type { ClusterLevel } from "../graph-clusters";
import type { CyInstance, CyLayoutOptions } from "../cytoscape-types";
import { LAYOUT_OPTIONS, type LayoutMode } from "./canvas-types";
import { computeLabelSizeRanges, applyNodeSize, resolveNodeLabel } from "./canvas-helpers";

interface GraphLayoutRefs {
  cyRef: MutableRefObject<CyInstance | null>;
  layoutRunningRef: MutableRefObject<boolean>;
  activeLayoutRef: MutableRefObject<{ stop: () => void } | null>;
  anchoredRef: MutableRefObject<Set<string>>;
  portEdgesAddedRef: MutableRefObject<boolean>;
  nodesRef: MutableRefObject<Map<string, GNode>>;
  edgesRef: MutableRefObject<Map<string, GEdge>>;
  overlayEdgesRef: MutableRefObject<Map<string, GEdge>>;
  collapsedClustersRef: MutableRefObject<Set<string>>;
  clusterLevelRef: MutableRefObject<ClusterLevel>;
  sizeByPropertyRef: MutableRefObject<Record<string, string>>;
  colorOverridesRef: MutableRefObject<Record<string, string>>;
  labelPropertyRef: MutableRefObject<Record<string, string>>;
  sizeMultiplierRef: MutableRefObject<Record<string, number>>;
  sizeOverridesRef: MutableRefObject<Record<string, number>>;
  computeHullsRef: MutableRefObject<() => void>;
}

export function useGraphLayout({
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
}: GraphLayoutRefs) {
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
  const nudgeLayoutRef = useRef<(freeNodes?: Set<string>, aggressive?: boolean) => void>(() => {});

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
  }, [cyRef, layoutRunningRef, activeLayoutRef, anchoredRef, clusterLevelRef, sizeByPropertyRef, colorOverridesRef, labelPropertyRef, sizeMultiplierRef, sizeOverridesRef]);

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
          numIter: aggressive ? 6000 : 900,
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
            setTimeout(() => { if (!layoutRunningRef.current) computeHullsRef.current(); }, animDuration + 50);
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
    [runLayout, cyRef, layoutRunningRef, activeLayoutRef, anchoredRef, portEdgesAddedRef, nodesRef, edgesRef, overlayEdgesRef, collapsedClustersRef, clusterLevelRef, sizeByPropertyRef, colorOverridesRef, labelPropertyRef, sizeMultiplierRef, sizeOverridesRef, computeHullsRef],
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

  return {
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
  };
}
