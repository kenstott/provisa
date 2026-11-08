// Copyright (c) 2026 Kenneth Stott
// Canary: 91c4f2e8-7a05-4d36-be19-2f6d3a80c5e4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useMemo } from "react";
import type { FrameData, GNode } from "../graph-model";

/** REQ-2135: a SPARQL node expands by a DESCRIBE of its resource, merged into the frame. Which of
 * ``nodes`` are expanded, and how a node asks: ``toggle`` takes an expansion back, a drag only adds. */
export function useSparqlExpansion(
  frame: FrameData,
  nodes: Map<string, GNode>,
  onExpand: ((frameId: string, iri: string, toggle: boolean) => void) | undefined,
) {
  const resourceOf = useCallback(
    (nodeKey: string) => {
      const iri = nodes.get(nodeKey)?.properties.iri;
      return typeof iri === "string" ? iri : null;
    },
    [nodes],
  );
  const expandedNodes = useMemo(() => {
    const held = frame.sparqlExpansions ?? {};
    return new Set(
      [...nodes.keys()].filter((k) => {
        const iri = resourceOf(k);
        return iri !== null && iri in held;
      }),
    );
  }, [frame.sparqlExpansions, nodes, resourceOf]);
  const expandNode = useCallback(
    (nodeKey: string, toggle: boolean) => {
      const iri = resourceOf(nodeKey);
      if (iri !== null) onExpand?.(frame.id, iri, toggle);
    },
    [resourceOf, onExpand, frame.id],
  );
  return { expandedNodes, expandNode };
}
