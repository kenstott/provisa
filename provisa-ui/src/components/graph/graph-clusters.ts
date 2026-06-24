// Copyright (c) 2026 Kenneth Stott
// Canary: 7b8fe871-d069-46e6-9010-37aa1fa22995
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

function clusterKeyFor(level: Exclude<ClusterLevel, "none">): string {
  return level === "l1" || level === "schema_L1"
    ? "scl1"
    : level === "l2" || level === "schema_L2"
      ? "scl2"
      : level === "l3" || level === "schema_L3"
        ? "scl3"
        : level;
}

function nodeClusterId(n: GNode, clusterKey: string): string | null {
  if (clusterKey === "domain") {
    const colonIdx = n.label.indexOf(":");
    return colonIdx > 0 ? n.label.slice(0, colonIdx) : n.label || null;
  }
  const raw = n.properties[clusterKey];
  return raw !== null && raw !== undefined ? String(raw) : null;
}

// Returns compound nodes, child data nodes, intra-cluster edges, and free↔free edges.
// Port nodes and meta-edges are deferred — call buildClusterMetaEdges after layout.
export function buildClusterElements(
  nodes: Map<string, GNode>,
  edges: Map<string, GEdge>,
  level: Exclude<ClusterLevel, "none">,
  overlayEdges?: Map<string, GEdge>,
  collapsedClusters: Set<string> = new Set(),
): CyElementDefinition[] {
  const clusterKey = clusterKeyFor(level);

  // 1. Cluster nodes — compound hull (expanded) or collapsed super-node
  const clusterLabels = new Map<string, Set<string>>();
  const clusterSizes = new Map<string, number>();
  nodes.forEach((n) => {
    const cid = nodeClusterId(n, clusterKey);
    if (cid === null) return;
    if (!clusterLabels.has(cid)) {
      clusterLabels.set(cid, new Set());
      clusterSizes.set(cid, 0);
    }
    clusterLabels.get(cid)!.add(n.tableLabel || n.label);
    clusterSizes.set(cid, (clusterSizes.get(cid) ?? 0) + 1);
  });

  const els: CyElementDefinition[] = [];
  clusterLabels.forEach((labels, cid) => {
    if (collapsedClusters.has(cid)) {
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
    nodeToCid.set(k, nodeClusterId(n, clusterKey));
  });

  nodes.forEach((n, k) => {
    const cid = nodeToCid.get(k) ?? null;
    if (cid !== null && collapsedClusters.has(cid)) return;
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

  // 3. Intra-cluster edges (expanded) and free↔free edges only.
  //    Cross-cluster / free↔cluster meta-edges are managed externally via
  //    buildClusterMetaEdges so they can be swapped between layout and port routing.
  const allEdges = overlayEdges ? new Map([...edges, ...overlayEdges]) : edges;
  allEdges.forEach((e) => {
    const srcKey = `${e.startNode.label}:${e.startNode.id}`;
    const tgtKey = `${e.endNode.label}:${e.endNode.id}`;
    if (!nodes.has(srcKey) || !nodes.has(tgtKey)) return;
    const srcCid = nodeToCid.get(srcKey) ?? null;
    const tgtCid = nodeToCid.get(tgtKey) ?? null;

    if (srcCid !== null && srcCid === tgtCid) {
      if (!collapsedClusters.has(srcCid)) {
        els.push({
          group: "edges",
          data: { id: e.identity, source: srcKey, target: tgtKey, label: e.type, _edge: e },
        });
      }
      return;
    }

    if (srcCid === null && tgtCid === null) {
      els.push({
        group: "edges",
        data: { id: e.identity, source: srcKey, target: tgtKey, label: e.type, _edge: e },
      });
    }
    // Cross-cluster edges handled by buildClusterMetaEdges
  });

  return els;
}

// Computes meta-edges (cross-cluster / cluster↔free).
// usePort=true  → target __port_* nodes (ellipse routing, for display after layout)
// usePort=false → target __cluster_* compounds (AABB routing, for fcose forces during layout/nudge)
export function buildClusterMetaEdges(
  nodes: Map<string, GNode>,
  edges: Map<string, GEdge>,
  level: Exclude<ClusterLevel, "none">,
  overlayEdges?: Map<string, GEdge>,
  collapsedClusters: Set<string> = new Set(),
  usePort = true,
): CyElementDefinition[] {
  const clusterKey = clusterKeyFor(level);

  const nodeToCid = new Map<string, string | null>();
  nodes.forEach((n, k) => {
    nodeToCid.set(k, nodeClusterId(n, clusterKey));
  });

  const routingId = (cid: string | null): string | null => {
    if (cid === null) return null;
    if (collapsedClusters.has(cid)) return `__collapsed_${level}_${cidToId(cid)}`;
    return usePort ? `__port_${level}_${cidToId(cid)}` : `__cluster_${level}_${cidToId(cid)}`;
  };

  const allEdges = overlayEdges ? new Map([...edges, ...overlayEdges]) : edges;
  const metaEdges = new Map<string, { src: string; tgt: string; type: string; count: number }>();

  allEdges.forEach((e) => {
    const srcKey = `${e.startNode.label}:${e.startNode.id}`;
    const tgtKey = `${e.endNode.label}:${e.endNode.id}`;
    if (!nodes.has(srcKey) || !nodes.has(tgtKey)) return;
    const srcCid = nodeToCid.get(srcKey) ?? null;
    const tgtCid = nodeToCid.get(tgtKey) ?? null;

    // Same cluster or both free: handled by buildClusterElements
    if (srcCid !== null && srcCid === tgtCid) return;
    if (srcCid === null && tgtCid === null) return;

    const srcRouting = routingId(srcCid);
    const tgtRouting = routingId(tgtCid);
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

  const els: CyElementDefinition[] = [];
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
