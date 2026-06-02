// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { clusterColor } from "./graph-model";
import type { GNode, GEdge } from "./graph-model";
import type { CyElementDefinition } from "./cytoscape-types";

export type ClusterLevel = "none" | "l1" | "l2" | "l3" | string;

// Sanitize a property value for use in a Cytoscape element ID
export function cidToId(val: string): string {
  return val.replace(/[^a-zA-Z0-9_-]/g, "_");
}

export function buildClusterElements(
  nodes: Map<string, GNode>,
  edges: Map<string, GEdge>,
  level: Exclude<ClusterLevel, "none">,
  overlayEdges?: Map<string, GEdge>,
  collapsedClusters: Set<string> = new Set(),
): CyElementDefinition[] {
  const clusterKey =
    level === "l1" || level === "schema_L1"
      ? "scl1"
      : level === "l2" || level === "schema_L2"
        ? "scl2"
        : level === "l3" || level === "schema_L3"
          ? "scl3"
          : level;

  // 1. Cluster nodes — compound hull (expanded) or collapsed super-node
  const clusterLabels = new Map<string, Set<string>>();
  const clusterSizes = new Map<string, number>();
  nodes.forEach((n) => {
    const raw = n.properties[clusterKey];
    if (raw === null || raw === undefined) return;
    const cid = String(raw);
    if (!clusterLabels.has(cid)) {
      clusterLabels.set(cid, new Set());
      clusterSizes.set(cid, 0);
    }
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
          id: `__collapsed_${level}_${cidToId(cid)}`,
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
          id: `__cluster_${level}_${cidToId(cid)}`,
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
    const parentId = cid !== null ? `__cluster_${level}_${cidToId(cid)}` : undefined;
    els.push({
      group: "nodes",
      data: {
        id: k,
        label: n.label,
        _node: n,
        ...(parentId ? { parent: parentId, _inCluster: true } : {}),
      },
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
    if (collapsedClusters.has(cid)) return `__collapsed_${level}_${cidToId(cid)}`;
    return `__cluster_${level}_${cidToId(cid)}`;
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
        els.push({
          group: "edges",
          data: { id: e.identity, source: srcKey, target: tgtKey, label: e.type, _edge: e },
        });
      }
      return;
    }

    const srcRouting = routingId(srcKey, srcCid);
    const tgtRouting = routingId(tgtKey, tgtCid);

    // Both free: plain data edge
    if (srcRouting === null && tgtRouting === null) {
      els.push({
        group: "edges",
        data: { id: e.identity, source: srcKey, target: tgtKey, label: e.type, _edge: e },
      });
      return;
    }

    // At least one side is clustered: consolidate into meta-edge
    const srcId = srcRouting ?? srcKey;
    const tgtId = tgtRouting ?? tgtKey;
    const metaKey = `${srcId}→${tgtId}:${e.type}`;
    const existing = metaEdges.get(metaKey);
    if (existing) {
      existing.count += 1;
    } else {
      metaEdges.set(metaKey, { src: srcId, tgt: tgtId, type: e.type, count: 1 });
    }
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
