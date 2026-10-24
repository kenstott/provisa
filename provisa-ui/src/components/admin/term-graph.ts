// Copyright (c) 2026 Kenneth Stott
// Canary: bd4f23d2-ca72-4e02-915b-0432b14ed539
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2149: a term's neighbourhood as layers, left to right -- the terms whose relationships
// point to it, the term, the terms its relationships point to, then what it refers to (its
// columns, and for a Class term its tables). One hop each way, each arrow named by its
// relationship type, each node by its catalog name.

import type { GlossaryTermDetail, GlossaryTermTable } from "../../api/glossary";

export type TermGraphKind = "term" | "self" | "column" | "table";

export interface TermGraphNode {
  id: string;
  label: string;
  kind: TermGraphKind;
  /** Its layer, 0 leftmost. */
  layer: number;
  /** The term a term node is, for navigating to it. */
  termId?: number;
}

export interface TermGraphEdge {
  id: string;
  source: string;
  target: string;
  /** The relationship's type, worded; none on the arrow to what the term refers to. */
  label?: string;
}

const termNode = (termId: number) => `term:${termId}`;

/** The layers and arrows of ``term``'s graph. ``tables`` are the tables it is grounded in, once
 * read; ``relLabel`` words a relationship type. */
export function termGraph(
  term: GlossaryTermDetail,
  tables: GlossaryTermTable[] | null,
  relLabel: (relType: string) => string,
): { nodes: TermGraphNode[]; edges: TermGraphEdge[] } {
  const nodes = new Map<string, TermGraphNode>();
  const edges: TermGraphEdge[] = [];
  const self = termNode(term.id);
  nodes.set(self, { id: self, label: term.name, kind: "self", layer: 1, termId: term.id });

  for (const edge of term.edges_in) {
    const id = termNode(edge.term_id);
    // A term both before and after this one is drawn once, before it.
    if (!nodes.has(id))
      nodes.set(id, { id, label: edge.name, kind: "term", layer: 0, termId: edge.term_id });
    edges.push({
      id: `in:${edge.term_id}:${edge.rel_type}`,
      source: id,
      target: self,
      label: relLabel(edge.rel_type),
    });
  }
  for (const edge of term.edges_out) {
    const id = termNode(edge.term_id);
    if (!nodes.has(id))
      nodes.set(id, { id, label: edge.name, kind: "term", layer: 2, termId: edge.term_id });
    edges.push({
      id: `out:${edge.term_id}:${edge.rel_type}`,
      source: self,
      target: id,
      label: relLabel(edge.rel_type),
    });
  }
  for (const ref of term.refs) {
    const id = `column:${ref.table_id}:${ref.column_name}`;
    nodes.set(id, {
      id,
      label: `${ref.alias || ref.table_name}.${ref.column_name}`,
      kind: "column",
      layer: 3,
    });
    edges.push({ id: `ref:${id}`, source: self, target: id });
  }
  for (const table of tables ?? []) {
    const id = `table:${table.table_id}`;
    nodes.set(id, {
      id,
      label: `${table.domain_id}.${table.table_name}`,
      kind: "table",
      layer: 3,
    });
    edges.push({ id: `ref:${id}`, source: self, target: id });
  }
  return { nodes: [...nodes.values()], edges };
}
