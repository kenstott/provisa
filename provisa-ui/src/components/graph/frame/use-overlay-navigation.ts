// Copyright (c) 2026 Kenneth Stott
// Canary: a3900f0b-e13c-40cd-8b89-42c82a16ef34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback } from "react";
import type { Dispatch, SetStateAction } from "react";
import type { SchemaRel } from "../graph-schema-types";
import type { GNode, GEdge } from "../graph-model";
import { extractElements } from "../graph-model";
import type { PathScope } from "../dropPath";
import { neighboursQuery } from "../cypherEndpoints";

type OverlayMap = Map<string, { nodes: Map<string, GNode>; edges: Map<string, GEdge> }>;
type MergedOverlay = { nodes: Map<string, GNode>; edges: Map<string, GEdge> };

interface UseOverlayNavigationParams {
  overlayData: OverlayMap;
  setOverlayData: Dispatch<SetStateAction<OverlayMap>>;
  frameNodes: Map<string, GNode>;
  /** The acting role's relationship types (/data/graph-schema): the only types a query is written
   * with, so the translator never meets one it does not register. */
  schemaRels: SchemaRel[];
  /** The labels no expansion reaches: their tables take arguments to read. */
  parameterized: ReadonlySet<string>;
  /** Where a node's children and parents may be: the selected domains (REQ-2135). */
  pathScope: PathScope;
}

export function useOverlayNavigation({
  overlayData,
  setOverlayData,
  frameNodes,
  schemaRels,
  parameterized,
  pathScope,
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
      if (data.redirect) {
        // REQ-1224: over the large-result threshold, the neighbours were delivered to a file
        // rather than returned; none can be drawn, which is said, as a failed query is.
        console.error("show neighbors query was delivered to a file, not drawn:", data.redirect);
        return null;
      }
      const rows: Record<string, unknown>[] = data.rows ?? [];
      return extractElements(rows);
    },
    [],
  );

  /** A node's children (its parents): one query, its other ends labelled from the schema within
   * the selected domains, one MATCH per label pair, unioned (REQ-2135). */
  const _fetchNeighboursOfNode = useCallback(
    async (nodeKey: string, way: "children" | "parents"): Promise<MergedOverlay | null> => {
      const gNode = _resolveNodeForKey(nodeKey);
      if (!gNode || gNode.id == null) return null;
      const q = neighboursQuery(gNode.label, gNode.id, way, schemaRels, parameterized, pathScope);
      if (q === null) return null;
      const result = await _fetchNeighbors(q);
      return result && (result.nodes.size > 0 || result.edges.size > 0) ? result : null;
    },
    [schemaRels, parameterized, pathScope, _resolveNodeForKey, _fetchNeighbors],
  );
  const _fetchChildrenForNode = useCallback(
    (nodeKey: string) => _fetchNeighboursOfNode(nodeKey, "children"),
    [_fetchNeighboursOfNode],
  );
  const _fetchParentsForNode = useCallback(
    (nodeKey: string) => _fetchNeighboursOfNode(nodeKey, "parents"),
    [_fetchNeighboursOfNode],
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
