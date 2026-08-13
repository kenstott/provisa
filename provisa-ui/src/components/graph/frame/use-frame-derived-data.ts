// Copyright (c) 2026 Kenneth Stott
// Canary: c3bc1ca0-9ecf-4cce-8fe5-d998ed968e53
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useMemo } from "react";
import type { GNode, GEdge, FrameData } from "../graph-model";

type OverlayMap = Map<string, { nodes: Map<string, GNode>; edges: Map<string, GEdge> }>;

/** Everything GraphFrame derives from `frame` + the navigation overlays.
 *
 * Split out of GraphFrame purely as derived state: each value here is a pure function of the
 * frame result and the overlay map, so the component keeps the interaction logic and this keeps
 * the arithmetic (overlay dedup, degree centrality, per-label counts, expansion-state sets).
 */
export function useFrameDerivedData(frame: FrameData, overlayData: OverlayMap) {
  const overlayNodes = useMemo(() => {
    if (overlayData.size === 0) return new Map<string, GNode>();
    const m = new Map<string, GNode>();
    for (const d of overlayData.values())
      d.nodes.forEach((n, k) => {
        if (!frame.nodes.has(k)) m.set(k, n);
      });
    return m;
  }, [frame.nodes, overlayData]);

  const overlayEdges = useMemo(() => {
    if (overlayData.size === 0) return new Map<string, GEdge>();
    // Dedup against frame edges by both identity key and endpoint+type fingerprint
    const frameFingerprints = new Set<string>();
    frame.edges.forEach((e) => {
      frameFingerprints.add(
        `${e.startNode.label}:${e.startNode.id}→${e.endNode.label}:${e.endNode.id}:${e.type}`,
      );
      // Also store reversed fingerprint so backward-traversal frame edges match canonical imputed edges
      frameFingerprints.add(
        `${e.endNode.label}:${e.endNode.id}→${e.startNode.label}:${e.startNode.id}:${e.type}`,
      );
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

  const augmentedNodes = useMemo(() => {
    const degIn = new Map<string, number>();
    const degOut = new Map<string, number>();
    const allEdges =
      overlayEdges.size > 0 ? new Map([...frame.edges, ...overlayEdges]) : frame.edges;
    allEdges.forEach((e) => {
      const srcKey = `${e.startNode.label}:${e.startNode.id}`;
      const tgtKey = `${e.endNode.label}:${e.endNode.id}`;
      degOut.set(srcKey, (degOut.get(srcKey) ?? 0) + 1);
      degIn.set(tgtKey, (degIn.get(tgtKey) ?? 0) + 1);
    });
    const totalNodes = frame.nodes.size;
    const result = new Map<string, GNode>();
    frame.nodes.forEach((n, k) => {
      const i = degIn.get(k) ?? 0;
      const o = degOut.get(k) ?? 0;
      const deg = i + o;
      const degreeCentrality = totalNodes > 1 ? parseFloat((deg / (totalNodes - 1)).toFixed(4)) : 0;
      result.set(k, {
        ...n,
        properties: { ...n.properties, degIn: i, degOut: o, degTotal: deg, degreeCentrality },
      });
    });
    return result;
  }, [frame.nodes, frame.edges, overlayEdges]);

  const overviewData = useMemo(() => {
    const allNodes =
      overlayNodes.size > 0 ? new Map([...augmentedNodes, ...overlayNodes]) : augmentedNodes;
    const allEdges =
      overlayEdges.size > 0 ? new Map([...frame.edges, ...overlayEdges]) : frame.edges;
    const labelCounts = new Map<string, number>();
    allNodes.forEach((n) => {
      labelCounts.set(n.label, (labelCounts.get(n.label) ?? 0) + 1);
    });
    const typeCounts = new Map<string, number>();
    allEdges.forEach((e) => typeCounts.set(e.type, (typeCounts.get(e.type) ?? 0) + 1));
    return {
      nodesByLabel: [...labelCounts.entries()].sort((a, b) => b[1] - a[1]),
      edgesByType: [...typeCounts.entries()].sort((a, b) => b[1] - a[1]),
      nodeCount: allNodes.size,
      edgeCount: allEdges.size,
    };
  }, [augmentedNodes, overlayNodes, frame.edges, overlayEdges]);

  const showingChildrenNatural = useMemo(
    () =>
      new Set(
        Array.from(overlayData.keys())
          .filter((k) => k.endsWith(":children"))
          .map((k) => k.slice(0, -":children".length)),
      ),
    [overlayData],
  );
  const showingChildrenCircular = useMemo(
    () =>
      new Set(
        Array.from(overlayData.keys())
          .filter((k) => k.endsWith(":children:circular"))
          .map((k) => k.slice(0, -":children:circular".length)),
      ),
    [overlayData],
  );
  const showingParents = useMemo(
    () =>
      new Set(
        Array.from(overlayData.keys())
          .filter((k) => k.endsWith(":parents"))
          .map((k) => k.slice(0, -":parents".length)),
      ),
    [overlayData],
  );
  const showingParentsCircular = useMemo(
    () =>
      new Set(
        Array.from(overlayData.keys())
          .filter((k) => k.endsWith(":parents:circular"))
          .map((k) => k.slice(0, -":parents:circular".length)),
      ),
    [overlayData],
  );

  return {
    overlayNodes,
    overlayEdges,
    augmentedNodes,
    overviewData,
    showingChildrenNatural,
    showingChildrenCircular,
    showingParents,
    showingParentsCircular,
  };
}
