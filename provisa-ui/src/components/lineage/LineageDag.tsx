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

const KIND_COLOR: Record<string, string> = {
  source: "#2f9e44", // green — a real base column
  derived: "#1c7ed6", // blue — produced by SQL
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

    const elements = [
      ...graph.nodes.map((n) => ({
        data: {
          id: n.id,
          label: n.relation ? `${n.relation}\n${n.column}` : n.column,
          kind: n.kind,
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
          label: (e.ops ?? []).map((o) => o.name).join(" ") || e.transform,
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
            "background-color": (el: cytoscape.NodeSingular) => KIND_COLOR[el.data("kind")] ?? "#868e96",
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
      ],
      layout: { name: "breadthfirst", directed: true, spacingFactor: 1.3, padding: 20 },
      wheelSensitivity: 0.2,
    });

    cy.on("tap", "node", (evt) => clickRef.current?.(evt.target.id()));

    return () => cy.destroy();
  }, [graph]);

  return <div ref={containerRef} style={{ width: "100%", height }} data-testid="lineage-dag" />;
}
