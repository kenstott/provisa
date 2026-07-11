// Copyright (c) 2026 Kenneth Stott
// Canary: c3bc1ca0-9ecf-4cce-8fe5-d998ed968e53
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback } from "react";
import type { Dispatch, SetStateAction } from "react";
import type { Relationship } from "../../../types/admin";
import type { GNode, GEdge } from "../graph-model";
import { extractElements } from "../graph-model";
import { tableLabel as dbTableLabel } from "../../../naming";

type OverlayMap = Map<string, { nodes: Map<string, GNode>; edges: Map<string, GEdge> }>;
type MergedOverlay = { nodes: Map<string, GNode>; edges: Map<string, GEdge> };

interface UseOverlayNavigationParams {
  overlayData: OverlayMap;
  setOverlayData: Dispatch<SetStateAction<OverlayMap>>;
  frameNodes: Map<string, GNode>;
  relationships: Relationship[] | undefined;
}

export function useOverlayNavigation({
  overlayData,
  setOverlayData,
  frameNodes,
  relationships,
}: UseOverlayNavigationParams) {
  const _resolveNodeForKey = useCallback(
    (nodeKey: string): GNode | undefined => {
      let gNode: GNode | undefined = frameNodes.get(nodeKey);
      if (!gNode) {
        for (const d of overlayData.values()) {
          gNode = d.nodes.get(nodeKey);
          if (gNode) break;
        }
      }
      return gNode;
    },
    [frameNodes, overlayData],
  );

  const _fetchNeighbors = useCallback(
    async (
      cypherQuery: string,
    ): Promise<{ nodes: Map<string, GNode>; edges: Map<string, GEdge> } | null> => {
      const res = await fetch("/data/cypher", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: cypherQuery, params: {} }),
      });
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        let err: unknown;
        try {
          err = JSON.parse(text);
        } catch {
          err = text;
        }
        console.error("show neighbors query failed (HTTP", res.status, "):", err);
        return null;
      }
      const data = await res.json();
      const rows: Record<string, unknown>[] = data.rows ?? [];
      return extractElements(rows);
    },
    [],
  );

  const _fetchChildrenForNode = useCallback(
    async (nodeKey: string): Promise<MergedOverlay | null> => {
      const gNode = _resolveNodeForKey(nodeKey);
      if (!gNode || gNode.id == null) return null;
      const tableLabel = gNode.tableLabel;
      const rels = (relationships ?? []).filter((r) => dbTableLabel(r.sourceTableName) === tableLabel);
      if (rels.length === 0) return null;
      const merged: MergedOverlay = { nodes: new Map(), edges: new Map() };
      await Promise.all(
        rels.map(async (r) => {
          const relType = (r.alias ?? r.computedCypherAlias ?? "").toUpperCase();
          const q = `MATCH (n:${gNode.label})-[r:${relType}]->(child) WHERE id(n) IN [${gNode.id}] RETURN n, r, child`;
          const result = await _fetchNeighbors(q);
          if (result) {
            result.nodes.forEach((n, k) => merged.nodes.set(k, n));
            result.edges.forEach((e, k) => merged.edges.set(k, e));
          }
        }),
      );
      return merged.nodes.size > 0 || merged.edges.size > 0 ? merged : null;
    },
    [relationships, _resolveNodeForKey, _fetchNeighbors],
  );

  const _fetchParentsForNode = useCallback(
    async (nodeKey: string): Promise<MergedOverlay | null> => {
      const gNode = _resolveNodeForKey(nodeKey);
      if (!gNode || gNode.id == null) return null;
      const tableLabel = gNode.tableLabel;
      const rels = (relationships ?? []).filter((r) => dbTableLabel(r.targetTableName) === tableLabel);
      if (rels.length === 0) return null;
      const merged: MergedOverlay = { nodes: new Map(), edges: new Map() };
      await Promise.all(
        rels.map(async (r) => {
          const relType = (r.alias ?? r.computedCypherAlias ?? "").toUpperCase();
          const q = `MATCH (parent)-[r:${relType}]->(n:${gNode.label}) WHERE id(n) IN [${gNode.id}] RETURN n, r, parent`;
          const result = await _fetchNeighbors(q);
          if (result) {
            result.nodes.forEach((n, k) => merged.nodes.set(k, n));
            result.edges.forEach((e, k) => merged.edges.set(k, e));
          }
        }),
      );
      return merged.nodes.size > 0 || merged.edges.size > 0 ? merged : null;
    },
    [relationships, _resolveNodeForKey, _fetchNeighbors],
  );

  const handleToggleChildren = useCallback(
    async (nodeKey: string) => {
      const overlayKey = `${nodeKey}:children`;
      if (overlayData.has(overlayKey)) {
        setOverlayData((prev) => {
          const next = new Map(prev);
          next.delete(overlayKey);
          return next;
        });
        return;
      }
      const merged = await _fetchChildrenForNode(nodeKey);
      if (merged) setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
    },
    [overlayData, setOverlayData, _fetchChildrenForNode],
  );

  const handleToggleChildrenCircular = useCallback(
    async (nodeKey: string) => {
      const overlayKey = `${nodeKey}:children:circular`;
      if (overlayData.has(overlayKey)) {
        setOverlayData((prev) => {
          const next = new Map(prev);
          next.delete(overlayKey);
          return next;
        });
        return;
      }
      const merged = await _fetchChildrenForNode(nodeKey);
      if (merged) setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
    },
    [overlayData, setOverlayData, _fetchChildrenForNode],
  );

  const handleToggleChildrenBatch = useCallback(
    async (nodeKeys: string[], circular = false) => {
      const suffix = circular ? ":children:circular" : ":children";
      const toRemove = nodeKeys.filter((id) => overlayData.has(`${id}${suffix}`));
      const toAdd = nodeKeys.filter((id) => !overlayData.has(`${id}${suffix}`));
      if (toAdd.length === 0) {
        setOverlayData((prev) => {
          const next = new Map(prev);
          toRemove.forEach((id) => next.delete(`${id}${suffix}`));
          return next;
        });
        return;
      }
      // All nodes fetched in parallel off-screen; single setOverlayData call renders them all at once.
      const results = await Promise.all(toAdd.map((id) => _fetchChildrenForNode(id)));
      setOverlayData((prev) => {
        const next = new Map(prev);
        toAdd.forEach((id, i) => {
          if (results[i]) next.set(`${id}${suffix}`, results[i]!);
        });
        return next;
      });
    },
    [overlayData, setOverlayData, _fetchChildrenForNode],
  );

  const handleToggleParents = useCallback(
    async (nodeKey: string) => {
      const overlayKey = `${nodeKey}:parents`;
      if (overlayData.has(overlayKey)) {
        setOverlayData((prev) => {
          const next = new Map(prev);
          next.delete(overlayKey);
          return next;
        });
        return;
      }
      const merged = await _fetchParentsForNode(nodeKey);
      if (merged) setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
    },
    [overlayData, setOverlayData, _fetchParentsForNode],
  );

  const handleToggleParentsCircular = useCallback(
    async (nodeKey: string) => {
      const overlayKey = `${nodeKey}:parents:circular`;
      if (overlayData.has(overlayKey)) {
        setOverlayData((prev) => {
          const next = new Map(prev);
          next.delete(overlayKey);
          return next;
        });
        return;
      }
      const merged = await _fetchParentsForNode(nodeKey);
      if (merged) setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
    },
    [overlayData, setOverlayData, _fetchParentsForNode],
  );

  const handleToggleParentsBatch = useCallback(
    async (nodeKeys: string[], circular = false) => {
      const suffix = circular ? ":parents:circular" : ":parents";
      const toRemove = nodeKeys.filter((id) => overlayData.has(`${id}${suffix}`));
      const toAdd = nodeKeys.filter((id) => !overlayData.has(`${id}${suffix}`));
      if (toAdd.length === 0) {
        setOverlayData((prev) => {
          const next = new Map(prev);
          toRemove.forEach((id) => next.delete(`${id}${suffix}`));
          return next;
        });
        return;
      }
      const results = await Promise.all(toAdd.map((id) => _fetchParentsForNode(id)));
      setOverlayData((prev) => {
        const next = new Map(prev);
        toAdd.forEach((id, i) => {
          if (results[i]) next.set(`${id}${suffix}`, results[i]!);
        });
        return next;
      });
    },
    [overlayData, setOverlayData, _fetchParentsForNode],
  );

  return {
    handleToggleChildren,
    handleToggleChildrenCircular,
    handleToggleChildrenBatch,
    handleToggleParents,
    handleToggleParentsCircular,
    handleToggleParentsBatch,
  };
}
