// Copyright (c) 2026 Kenneth Stott
// Canary: 7b1e5d93-2c48-4a06-9f3e-8d0a6c4b2e17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27: lineage works like the 2D ontology graph, keeping its own
// differences such as transforms as line labels), REQ-1665, REQ-1627: the column lineage model
// (lineage-layout's boxes and rows) as the one graph component draws it. A relation drawn open is a
// group, its fields its nodes, each field's role in the flow its node type; a relation drawn
// collapsed is one node, its fields' lines rolled up onto it. A line runs field to field, its
// arrow at the field made from the other, its label the transform. A line is dashed where it is
// indirect: it reads through views not drawn, or an OpenLineage event reported it rather than the
// model deriving it (REQ-2229). The view says what each thing is; the graph says how it is drawn.

import type { GraphEdgeSpec, GraphGroupSpec, GraphNodeSpec, GraphSpec } from "../provisa-graph/model";
import type { NodeType } from "../provisa-graph/tokens";
import type { LineageBox, LineageLayoutModel, LineageRow, Role } from "./lineage-layout";
import { edgeText } from "./lineage-layout";

/** The node type a field of each role is drawn as: its band, icon and type line. */
export const ROLE_TYPE: Record<Role, NodeType> = {
  source: "source",
  intermediate: "intermediate",
  output: "output",
  command: "command",
  dataset: "table",
  native: "native",
  job: "job",
  external: "external",
};

/** The words a lineage graph's cards say, already translated. */
export interface LineageWords {
  reported: string;
  /** "<n> columns" */
  columns: (count: number) => string;
  /** "<n> unused" */
  unused: (count: number) => string;
  materialized: string;
  feedbackCycle: string;
  errorCycle: string;
}

/** What a field's card says beside its name: its description, its state. */
function rowNotes(r: LineageRow, words: LineageWords, described: string | null): string {
  return [
    described,
    r.materialized ? words.materialized : null,
    r.cycle === "feedback" ? words.feedbackCycle : r.cycle === "error" ? words.errorCycle : null,
  ]
    .filter(Boolean)
    .join(" · ");
}

/** A native box's source, as its card says it: the source's type and the address it is read from. */
const nativeText = (b: LineageBox) =>
  b.native ? [b.native.typeLabel, ...b.native.lines].join(" · ") : "";

export function lineageSpec(
  model: LineageLayoutModel,
  words: LineageWords,
  describe?: (relation: string, column: string) => string | null,
): GraphSpec {
  const groups: GraphGroupSpec[] = [];
  const nodes: GraphNodeSpec[] = [];
  for (const b of model.boxes) {
    const count = b.rows.length + b.unused;
    if (b.collapsed) {
      nodes.push({
        id: b.id,
        type: ROLE_TYPE.dataset,
        name: b.label,
        description: words.columns(count),
        data: { relation: b.relation ?? "", collapsed: "yes" },
      });
      continue;
    }
    groups.push({
      id: b.id,
      label: b.label,
      description: nativeText(b),
      data: { relation: b.relation ?? "" },
    });
    for (const r of b.rows) {
      const described = b.relation && describe ? describe(b.relation, r.label) : null;
      nodes.push({
        id: r.id,
        type: ROLE_TYPE[r.role],
        name: r.label,
        parent: b.id,
        description: rowNotes(r, words, described),
        data: { row: "yes", relation: b.relation ?? "" },
      });
    }
    // The fields no line touches, folded into one node: counted, not drawn.
    if (b.unused > 0)
      nodes.push({
        id: `${b.id}:unused`,
        type: "more",
        kind: "more",
        name: words.unused(b.unused),
        parent: b.id,
      });
  }
  const edges: GraphEdgeSpec[] = model.edges.map((e) => ({
    id: e.id,
    source: e.source,
    target: e.target,
    label: edgeText(e, words.reported),
    arrow: "target",
    ...(e.reported || e.via.length > 0 ? { dashed: true } : {}),
    data: { commandName: e.commandName ?? "" },
  }));
  return { nodes, edges, groups };
}

/** Every node a field's lines reach, upstream and downstream: what hovering it brings forward. */
export function lineageTrace(
  spec: GraphSpec,
  id: string,
): { nodes: Set<string>; links: Set<string> } {
  const out = new Map<string, GraphEdgeSpec[]>();
  const into = new Map<string, GraphEdgeSpec[]>();
  for (const e of spec.edges) {
    (out.get(e.source) ?? out.set(e.source, []).get(e.source)!).push(e);
    (into.get(e.target) ?? into.set(e.target, []).get(e.target)!).push(e);
  }
  const nodes = new Set<string>([id]);
  const links = new Set<string>();
  const walk = (next: Map<string, GraphEdgeSpec[]>, end: "source" | "target") => {
    const seen = new Set<string>([id]);
    const stack = [id];
    while (stack.length > 0) {
      const at = stack.pop() as string;
      for (const e of next.get(at) ?? []) {
        links.add(e.id);
        const far = e[end];
        nodes.add(far);
        if (!seen.has(far)) {
          seen.add(far);
          stack.push(far);
        }
      }
    }
  };
  walk(out, "target");
  walk(into, "source");
  return { nodes, links };
}
