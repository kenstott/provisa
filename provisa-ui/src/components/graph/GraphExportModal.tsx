// Copyright (c) 2026 Kenneth Stott
// Canary: 4b96129a-0294-4ed6-9594-cd34c5544400
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: each tab of the graph visualizer offers its own language's exports. The Cypher tab's
// is the Neo4j export of every frame's drawing; the SPARQL tab's writes a frame's answer as RDF or
// SPARQL results.

import type { FrameData, GEdge, GNode } from "./graph-model";
import { Neo4jExportModal } from "./Neo4jExportModal";
import { SparqlExportModal } from "./SparqlExportModal";

interface GraphExportModalProps {
  sparql: boolean;
  /** The frames, newest first. */
  frames: FrameData[];
  /** Each frame's drawing as its canvas shows it, where it has one. */
  effective: Map<string, { nodes: Map<string, GNode>; edges: Map<string, GEdge> }>;
  roleHeader: string | undefined;
  onClose: () => void;
}

export function GraphExportModal({
  sparql,
  frames,
  effective,
  roleHeader,
  onClose,
}: GraphExportModalProps) {
  if (sparql)
    return (
      <SparqlExportModal
        frames={frames.flatMap((f) =>
          f.status === "done" && f.sparqlForm
            ? [{ id: f.id, query: f.query, form: f.sparqlForm }]
            : [],
        )}
        roleHeader={roleHeader}
        onClose={onClose}
      />
    );
  const allNodes = new Map<string, GNode>();
  const allEdges = new Map<string, GEdge>();
  for (const f of frames) {
    const drawn = effective.get(f.id) ?? f;
    drawn.nodes.forEach((n, k) => allNodes.set(k, n));
    drawn.edges.forEach((e, k) => allEdges.set(k, e));
  }
  return (
    <Neo4jExportModal
      nodes={[...allNodes.values()]}
      edges={[...allEdges.values()]}
      onClose={onClose}
    />
  );
}
