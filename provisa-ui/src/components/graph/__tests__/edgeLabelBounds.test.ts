// Copyright (c) 2026 Kenneth Stott
// Canary: 5d0b7e1f-9a3c-4e62-8f14-c2b6a9d3e781
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Reported 2026-11-28 (Explore > Cypher, a path result, the inspector opened and closed): the
// relationship types along the edges (IN_DOMAIN) were drawn cut short and askew. Cytoscape draws a
// label from a texture the size of the label's box. An edge's room (text-max-width) is restyled a
// frame after its nodes move; cytoscape applies a restyle lazily, so a bounding box taken before
// the next frame draws (the fit after a layout or a resize) applied it mid-measure and kept a box
// measured from the label's old, ellipsized width. No later frame corrected it while the nodes
// stayed put, and the label was clipped to that box.

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import cytoscape from "cytoscape";
import { EDGE_LABEL_FONT, buildGraphStylesheet, trackEdgeLabelRoom } from "../graph-stylesheet";
import type { CyInstance } from "../cytoscape-types";
import { graphTokens } from "../../provisa-graph/tokens";
import { textWidth } from "../../provisa-graph/model";

const realGetContext = HTMLCanvasElement.prototype.getContext;

// jsdom has no canvas: a context that accepts every call and measures text as the app's textWidth.
beforeEach(() => {
  HTMLCanvasElement.prototype.getContext = function (this: HTMLCanvasElement) {
    const state: Record<string, unknown> = { font: `${EDGE_LABEL_FONT}px sans-serif` };
    return new Proxy(state, {
      get: (t, k: string) =>
        k in t
          ? t[k]
          : k === "measureText"
            ? (s: string) => ({
                width: textWidth(s, parseFloat(String(t.font).match(/(\d+(\.\d+)?)px/)![1])),
              })
            : k === "canvas"
              ? this
              : () => undefined,
      set: (t, k: string, v) => {
        t[k] = v;
        return true;
      },
    });
  } as unknown as typeof HTMLCanvasElement.prototype.getContext;
});
afterEach(() => {
  HTMLCanvasElement.prototype.getContext = realGetContext;
  document.body.innerHTML = "";
});

const frame = () => new Promise((r) => requestAnimationFrame(() => r(null)));

function graph() {
  const container = document.createElement("div");
  Object.defineProperty(container, "clientWidth", { value: 900 });
  Object.defineProperty(container, "clientHeight", { value: 600 });
  document.body.appendChild(container);
  const cy = cytoscape({
    container,
    layout: { name: "preset" },
    elements: [
      { group: "nodes", data: { id: "t", label: "Table" }, position: { x: 0, y: 0 } },
      // The two nodes start close, as a layout begins: the edge has little room.
      { group: "nodes", data: { id: "d", label: "Domain" }, position: { x: 60, y: 0 } },
      { group: "edges", data: { id: "td", source: "t", target: "d", label: "IN_DOMAIN" } },
    ],
    style: buildGraphStylesheet(
      {
        colorOverridesRef: { current: {} },
        sizeOverridesRef: { current: {} },
        labelPropertyRef: { current: {} },
        relLineOverridesRef: { current: {} },
      },
      graphTokens(false),
    ) as never,
  });
  trackEdgeLabelRoom(cy as unknown as CyInstance);
  return cy;
}

/** The width of the edge's label box, unturned: cytoscape draws the label from a texture this size
 * (its label texture cache reads ``labelBounds.main``), and clips the text to it. */
const labelBoxWidth = (cy: ReturnType<typeof cytoscape>) => {
  const edge = cy.$id("td");
  edge.boundingBox();
  return (edge[0] as unknown as { _private: { labelBounds: { main: { w: number } } } })._private
    .labelBounds.main.w;
};

describe("an edge's label box follows its room as its nodes move", () => {
  it("measures the whole type though a box is taken before the frame that draws it", async () => {
    const cy = graph();
    await frame();
    await frame();
    // Close together the edge has room for an ellipsis only.
    expect(labelBoxWidth(cy)).toBeLessThan(textWidth("IN_DOMAIN", EDGE_LABEL_FONT));
    // The layout ends with the nodes apart, and a frame draws them there before the edge's room
    // is restyled (here, the box the frame measures). In the frame that restyles the room the
    // graph is fitted, after a layout or a resize.
    cy.$id("d").position({ x: 300, y: 0 });
    cy.elements().boundingBox();
    requestAnimationFrame(() => cy.fit());
    await frame();
    await frame();
    await frame();
    expect(labelBoxWidth(cy)).toBeGreaterThanOrEqual(textWidth("IN_DOMAIN", EDGE_LABEL_FONT));
    cy.destroy();
  });
});
