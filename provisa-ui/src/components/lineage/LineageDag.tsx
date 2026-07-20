// Copyright (c) 2026 Kenneth Stott
// Canary: 2d7b9e14-6a03-4c58-8f21-9b0c3e7a5d46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1160/REQ-1161: column-level lineage DAG viz. Nodes are columns coloured by kind (source /
// derived / command boundary) with a materialized ring; edges carry the named transform; cycle
// members are ringed by classification (feedback vs error). Left-to-right, source → output.

import { useEffect, useRef } from "react";
import cytoscape from "cytoscape";
import type { LineageGraphData } from "../../api/lineage";

interface LineageDagProps {
  graph: LineageGraphData;
  height?: number;
  onNodeClick?: (nodeId: string) => void;
}

// Colour by ROLE in the flow (computed from in/out degree), not just kind: a column that is both
// produced AND consumed is an intermediate hand-off between datasets and gets its own colour.
const ROLE_COLOR: Record<string, string> = {
  source: "#2f9e44", // green — a base column leaf (no upstream)
  intermediate: "#0c8599", // teal — produced here AND consumed downstream (a dataset hand-off)
  output: "#1c7ed6", // blue — produced here, not consumed further (a terminal result column)
  command: "#9c36b5", // purple — an opaque command boundary
};

export function LineageDag({ graph, height = 520, onNodeClick }: LineageDagProps): React.ReactElement {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const clickRef = useRef(onNodeClick);
  clickRef.current = onNodeClick;

  useEffect(() => {
    if (!containerRef.current) return;

    // Tag cycle membership so nodes on a cycle can be ringed by classification.
    const cycleClass: Record<string, string> = {};
    for (const c of graph.cycles ?? []) {
      for (const n of c.nodes) cycleClass[n] = c.classification;
    }
    const outputs = new Set(graph.outputs);

    // In/out degree drives the role colour: a node consumed downstream (hasOut) that is also
    // produced (hasIn) is an intermediate. Sources and commands keep their kind colour.
    const hasIn = new Set(graph.edges.map((e) => e.target));
    const hasOut = new Set(graph.edges.map((e) => e.source));
    const roleOf = (n: { id: string; kind: string }): string => {
      if (n.kind === "command") return "command";
      if (n.kind === "source") return "source";
      return hasIn.has(n.id) && hasOut.has(n.id) ? "intermediate" : "output";
    };

    // Group columns under a parent box per relation (dataset), so columns read as members of a
    // table/view/dataset rather than free-standing nodes. Nodes without a relation stay top-level.
    const relations = Array.from(
      new Set(graph.nodes.map((n) => n.relation).filter((r): r is string => !!r)),
    );
    const parentId = (relation: string) => `rel:${relation}`;

    const elements = [
      ...relations.map((relation) => ({
        data: { id: parentId(relation), label: relation, isParent: "yes" },
      })),
      ...graph.nodes.map((n) => ({
        data: {
          id: n.id,
          parent: n.relation ? parentId(n.relation) : undefined,
          label: n.column,
          kind: n.kind,
          role: roleOf(n),
          materialized: n.materialized ? "yes" : "no",
          cycle: cycleClass[n.id] ?? "no",
          output: outputs.has(n.id) ? "yes" : "no",
        },
      })),
      ...graph.edges.map((e, i) => ({
        data: {
          id: `e${i}`,
          source: e.source,
          target: e.target,
          // A pure passthrough (identity/constant) has no transform to name — leave the edge
          // unlabelled rather than repeating the column name. Only real ops (functions, operators,
          // commands) get labelled, rendered as a formula with their literal args: substring(1, 3).
          label: (e.ops ?? [])
            .filter((o) => o.kind !== "identity" && o.kind !== "constant")
            .map((o) => (o.args?.length ? `${o.name}(${o.args.join(", ")})` : o.name))
            .join(" "),
          command: (e.ops ?? []).some((o) => o.kind === "command") ? "yes" : "no",
        },
      })),
    ];

    const cy = cytoscape({
      container: containerRef.current,
      elements,
      style: [
        {
          selector: "node",
          style: {
            "background-color": (el: cytoscape.NodeSingular) => ROLE_COLOR[el.data("role")] ?? "#868e96",
            label: "data(label)",
            "text-wrap": "wrap",
            "text-valign": "center",
            "text-halign": "center",
            color: "#fff",
            "font-size": 9,
            width: 96,
            height: 40,
            shape: "round-rectangle",
            "text-max-width": "90px",
          },
        },
        {
          selector: 'node[output = "yes"]',
          style: { "border-width": 3, "border-color": "#f08c00" }, // final output columns
        },
        {
          selector: 'node[materialized = "yes"]',
          style: { "border-width": 3, "border-color": "#495057", "border-style": "double" },
        },
        {
          selector: 'node[cycle = "error"]',
          style: { "border-width": 4, "border-color": "#e03131" },
        },
        {
          selector: 'node[cycle = "feedback"]',
          style: { "border-width": 4, "border-color": "#f59f00" },
        },
        {
          selector: "edge",
          style: {
            width: 1.5,
            "line-color": "#adb5bd",
            "target-arrow-color": "#adb5bd",
            "target-arrow-shape": "triangle",
            "curve-style": "bezier",
            label: "data(label)",
            "font-size": 8,
            color: "#868e96",
            "text-rotation": "autorotate",
            "text-background-color": "#fff",
            "text-background-opacity": 0.8,
            "text-background-padding": "1px",
          },
        },
        {
          selector: 'edge[command = "yes"]',
          style: { "line-color": "#9c36b5", "target-arrow-color": "#9c36b5", width: 2, "line-style": "dashed" },
        },
        {
          // The dataset container: a labelled box wrapping its columns.
          selector: 'node[isParent = "yes"]',
          style: {
            label: "data(label)",
            "background-color": "#f1f3f5",
            "background-opacity": 0.6,
            "border-width": 1,
            "border-color": "#ced4da",
            shape: "round-rectangle",
            "text-valign": "top",
            "text-halign": "center",
            "font-size": 10,
            "font-weight": "bold",
            color: "#495057",
            padding: 12,
          },
        },
      ],
      layout: { name: "breadthfirst", directed: true, spacingFactor: 1.3, padding: 20 },
      wheelSensitivity: 0.2,
    });

    // Column clicks drive federation focus; ignore taps on the dataset container itself.
    cy.on("tap", "node", (evt) => {
      if (evt.target.data("isParent") !== "yes") clickRef.current?.(evt.target.id());
    });

    return () => cy.destroy();
  }, [graph]);

  return <div ref={containerRef} style={{ width: "100%", height }} data-testid="lineage-dag" />;
}
