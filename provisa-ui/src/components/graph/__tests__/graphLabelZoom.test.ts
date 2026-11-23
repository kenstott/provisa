// Copyright (c) 2026 Kenneth Stott
// Canary: 8e2d5b71-3f4a-4c9e-b1d6-a7c09e3f2b54
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Reported 2026-11-22 (Explore > Cypher, zoomed in): relationship-type labels were written level
// whatever their line's direction, so on a steep line, or one of a pair between the same nodes,
// the label stood off the line and over its neighbour's and over node captions. Every label is
// drawn at the graph's scale and follows the zoom with the nodes, as in the schema graph; a
// relationship's label is centred on its line, turned with it, on a strip of the canvas.

import { describe, expect, it } from "vitest";
import cytoscape from "cytoscape";
import { EDGE_LABEL_FONT, buildGraphStylesheet, trackEdgeLabelRoom } from "../graph-stylesheet";
import type { CyInstance } from "../cytoscape-types";
import { graphTokens } from "../../provisa-graph/tokens";
import type { GNode } from "../graph-model";

const t = graphTokens(true);
type Core = ReturnType<typeof cytoscape>;

function graph() {
  const node = (id: number, name: string): GNode => ({
    id,
    label: "Neighborhood",
    tableLabel: "neighborhoods",
    properties: { name },
  });
  const a = node(1, "Eastgate");
  const b = node(2, "N2");
  return cytoscape({
    headless: true,
    styleEnabled: true,
    elements: [
      {
        group: "nodes",
        data: { id: "a", label: "Neighborhood", _node: a },
        position: { x: 0, y: 0 },
      },
      {
        group: "nodes",
        data: { id: "b", label: "Neighborhood", _node: b },
        position: { x: 30, y: 300 },
      },
      {
        group: "edges",
        data: { id: "ab", source: "a", target: "b", label: "PERMIT_NEIGHBORHOOD_BUDGET" },
      },
      { group: "edges", data: { id: "ba", source: "b", target: "a", label: "ISSUED_IN" } },
    ],
    style: buildGraphStylesheet(
      {
        colorOverridesRef: { current: {} },
        sizeOverridesRef: { current: {} },
        labelPropertyRef: { current: {} },
        relLineOverridesRef: { current: {} },
      },
      t,
    ) as never,
  });
}

const edgeLook = (cy: Core, id: string) => {
  const e = cy.$id(id);
  return {
    rotation: e.style("text-rotation"),
    marginX: e.style("text-margin-x"),
    marginY: e.style("text-margin-y"),
    backgroundOpacity: e.style("text-background-opacity"),
  };
};
const nodeLook = (cy: Core, id: string) => {
  const n = cy.$id(id);
  return {
    label: n.style("label"),
    valign: n.style("text-valign"),
    maxWidth: n.style("text-max-width"),
    font: n.style("font-size"),
  };
};

describe("labels at every zoom", () => {
  it("writes a relationship's type along its line, centred on it, on the canvas", () => {
    const cy = graph();
    for (const id of ["ab", "ba"])
      expect(edgeLook(cy, id)).toEqual({
        rotation: "autorotate",
        marginX: "0px",
        marginY: "0px",
        backgroundOpacity: "1",
      });
    // The strip is the canvas's own colour.
    const strip = cytoscape({
      headless: true,
      styleEnabled: true,
      style: [{ selector: "node", style: { "background-color": t.canvas } }],
      elements: [{ group: "nodes", data: { id: "c" } }],
    });
    expect(cy.$id("ab").style("text-background-color")).toBe(
      strip.$id("c").style("background-color"),
    );
  });

  it("keeps every label in graph units, so captions and edge labels follow the zoom together", () => {
    const cy = graph();
    const at1 = { a: nodeLook(cy, "a"), b: nodeLook(cy, "b"), ab: edgeLook(cy, "ab") };
    for (const z of [2, 4]) {
      cy.zoom(z);
      // Nothing about a label is restyled by the zoom: its font, its place and its room are the
      // graph's, and the zoom scales them all by the same factor.
      expect({ a: nodeLook(cy, "a"), b: nodeLook(cy, "b"), ab: edgeLook(cy, "ab") }).toEqual(at1);
      expect(cy.$id("ab").style("font-size")).toBe(`${EDGE_LABEL_FONT}px`);
      expect(cy.$id("ab").style("min-zoomed-font-size")).toBe("0px");
      expect(cy.$id("a").style("min-zoomed-font-size")).toBe("0px");
    }
    // A caption that fits stays inside its circle; a longer one goes beneath it, within its room.
    expect(at1.b).toMatchObject({ label: "N2", valign: "center", maxWidth: "36px" });
    expect(at1.a).toMatchObject({ label: "Eastgate", valign: "bottom", maxWidth: "68px" });
  });

  it("ends a type longer than its line in an ellipsis between the nodes' rims, as nodes move", async () => {
    const cy = graph();
    trackEdgeLabelRoom(cy as unknown as CyInstance);
    const ab = cy.$id("ab");
    expect(ab.style("text-wrap")).toBe("ellipsis");
    const frame = () => new Promise((r) => requestAnimationFrame(() => r(null)));
    cy.$id("b").position({ x: 30, y: 300 });
    await frame();
    // 30 across and 300 down: about 301.5 between centres, less each node's reach toward the other
    // (to its box: 24px, with its border; headless cytoscape measures no caption) and a 4px gap
    // at each end.
    expect(ab.style("text-max-width")).toBe("245px");
    cy.$id("b").position({ x: 0, y: 90 });
    await frame();
    expect(ab.style("text-max-width")).toBe("34px");
    // Nodes that touch leave no room: the label is not drawn over them.
    cy.$id("b").position({ x: 0, y: 40 });
    await frame();
    expect(ab.style("text-max-width")).toBe("0px");
  });
});
