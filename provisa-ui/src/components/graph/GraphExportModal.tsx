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
import { frameTriples } from "./sparql";
import type { SparqlExportable } from "./sparqlExport";

/** A SPARQL frame's export: a graph one query answers is asked of the endpoint again, as it is
 * answered; a graph drawn from rows, or holding what was merged into it, is written from its
 * triples. */
function sparqlExportable(f: FrameData, prefixes: Record<string, string>): SparqlExportable[] {
  if (f.status !== "done" || !f.sparqlForm) return [];
  const own = { id: f.id, query: f.query, form: f.sparqlForm };
  const merged = Object.keys(f.sparqlExpansions ?? {}).length > 0;
  if (f.sparqlForm === "graph" && !merged) return [own];
  const triples = frameTriples(f);
  return [
    triples.length > 0
      ? { ...own, triples, prefixes }
      : { ...own, unnamedEdges: f.sparqlPairing !== undefined },
  ];
}

interface GraphExportModalProps {
  sparql: boolean;
  /** The frames, newest first. */
  frames: FrameData[];
  /** Each frame's drawing as its canvas shows it, where it has one. */
  effective: Map<string, { nodes: Map<string, GNode>; edges: Map<string, GEdge> }>;
  roleHeader: string | undefined;
  /** The prefixes a SPARQL graph's Turtle is written with. */
  prefixes: Record<string, string>;
  onClose: () => void;
}

export function GraphExportModal({
  sparql,
  frames,
  effective,
  roleHeader,
  prefixes,
  onClose,
}: GraphExportModalProps) {
  if (sparql)
    return (
      <SparqlExportModal
        frames={frames.flatMap((f) => sparqlExportable(f, prefixes))}
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
