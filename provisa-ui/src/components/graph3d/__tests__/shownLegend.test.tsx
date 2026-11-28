// Copyright (c) 2026 Kenneth Stott
// Canary: 2ba76fc0-85b8-4fb9-a242-6b71849a5dad
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27): the ontology sidebar's Shown list is a legend in the canvas
// colours of its view: in 2D the shared graph's band colour of each type (a term by its role), in
// 3D the colour of each look. A term is one colour in both.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { NODE_TYPES } from "../../provisa-graph/tokens";
import { lookColour, nodeLook, shownKinds } from "../nodeLook";

vi.mock("../graphScene", () => ({
  GraphScene: class {
    constructor() {
      for (const m of [
        "setTheme",
        "setRotate",
        "setGrid",
        "dispose",
        "resetView",
        "setHover",
        "setData",
        "setSelected",
        "focus",
        "setFlat",
        "setArcs",
      ])
        (this as Record<string, unknown>)[m] = vi.fn();
    }
  },
}));
// jsdom cannot draw cytoscape; the sidebar is what is read here.
vi.mock("../LaneStage", () => ({ LaneStage: () => <div data-testid="x-lanes" /> }));

import { GraphExplorer } from "../GraphExplorer";
import { ONTOLOGY_SCOPE, type GraphData } from "../graphModel";

const DATA: GraphData = {
  nodes: [
    { id: "term:1", type: "term", label: "Customer", domains: ["sales"], status: "approved" },
    { id: "term:2", type: "term", label: "Party", domains: ["sales"], status: "approved" },
    { id: "term:3", type: "term", label: "Order", domains: ["sales"], status: "approved" },
  ],
  edges: [{ id: "e1", a: "term:1", b: "term:2", kind: "term_edge", label: "KIND_OF" }],
};

const explorer = (flat: boolean) => (
  <GraphExplorer
    data={DATA}
    scope={ONTOLOGY_SCOPE}
    preset="ontology"
    depth={2}
    settingsKey={{ kind: `shown-${flat ? "2d" : "3d"}`, subject: null }}
    exportName="ontology"
    testId="x"
    opensFlat={flat}
  />
);

const swatch = (key: string): string => {
  const dot = screen.getByTestId(`x-shown-${key}`).querySelector(".p3d-dot") as HTMLElement;
  return dot.style.background;
};

/** A hex colour as jsdom reports an inline background. */
const rgb = (hex: string): string => {
  const [r, g, b] = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));
  return `rgb(${r}, ${g}, ${b})`;
};

describe("the Shown legend takes the canvas colour of its view", () => {
  it("in 2D draws a term in the band colour the 2D canvas draws it", () => {
    render(explorer(true));
    expect(swatch("graph3d.type_term")).toBe(rgb(NODE_TYPES.term.colour));
  });

  it("in 3D draws a term in the colour the 3D scene draws it", () => {
    render(explorer(false));
    const term = lookColour(nodeLook(DATA.nodes[0]).colour);
    expect(swatch("graph3d.type_term")).toBe(rgb(term));
    // One term colour everywhere.
    expect(term).toBe(NODE_TYPES.term.colour);
  });
});

describe("shownKinds", () => {
  const nodes: GraphData["nodes"] = [
    { id: "t1", type: "term", label: "a", role: "class" },
    { id: "t2", type: "term", label: "b", role: "class" },
    { id: "t3", type: "term", label: "c", role: "property" },
    { id: "c1", type: "column", label: "x", inProduct: true },
    { id: "c2", type: "column", label: "y" },
    { id: "f", type: "column", label: "+2", folded: ["p", "q"] },
    { id: "tb", type: "table", label: "orders" },
  ];

  it("in 2D keys each type the shared graph draws, in its band colour", () => {
    expect(shownKinds(nodes, true)).toEqual([
      { key: "graph3d.look_class", colour: NODE_TYPES.class.colour, count: 2 },
      { key: "graph3d.type_column", colour: NODE_TYPES.column.colour, count: 2 },
      { key: "graph3d.look_property", colour: NODE_TYPES.property.colour, count: 1 },
      { key: "graph3d.type_table", colour: NODE_TYPES.table.colour, count: 1 },
    ]);
  });

  it("in 3D keys each look the scene draws, in its colour", () => {
    expect(shownKinds(nodes, false)).toEqual([
      { key: "graph3d.look_class", colour: NODE_TYPES.class.colour, count: 2 },
      { key: "graph3d.look_property", colour: NODE_TYPES.property.colour, count: 1 },
      { key: "graph3d.look_in_product", colour: NODE_TYPES.product.colour, count: 1 },
      { key: "graph3d.look_out_of_product", colour: NODE_TYPES.column.colour, count: 1 },
      { key: "graph3d.type_table", colour: NODE_TYPES.table.colour, count: 1 },
    ]);
  });
});
