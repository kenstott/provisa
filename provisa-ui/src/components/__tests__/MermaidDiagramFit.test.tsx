// Copyright (c) 2026 Kenneth Stott
// Canary: 5d0c2f7e-48a1-4b36-9e1d-b7a3c60e9f12
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (a diagram opens readable): a plan diagram is drawn at its natural size and shown
// through the canvas's view. It opens whole when that keeps it readable, never scaled past
// MAX_DIAGRAM_SCALE; a larger one opens at the readable zoom and is moved, not squeezed to the
// pane's width. jsdom has no layout, so Mermaid is mocked to return SVGs of a given size and the
// canvas is given one.
import { afterEach, describe, it, expect, vi } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";
import { MermaidDiagram } from "../MermaidDiagram";
import { HINT_BAND, MAX_DIAGRAM_SCALE } from "../mermaidFit";
import { READABLE_ZOOM } from "../provisa-graph/viewport";
import { sizeStages, viewOf } from "./mermaidMock";

vi.mock("mermaid", () => ({
  default: {
    initialize: vi.fn(),
    parse: async () => ({ diagramType: "flowchart-v2" }),
    render: async (id: string, chart: string) => {
      const first = chart.split("\n")[0];
      if (first === "flowchart noviewbox") return { svg: `<svg id="${id}"></svg>` };
      const [w, h] = first.replace("flowchart ", "").split("x");
      return {
        svg: `<svg id="${id}" width="100%" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${w} ${h}" style="max-width: ${w}px;"></svg>`,
      };
    },
  },
}));

let restore: (() => void) | null = null;
afterEach(() => restore?.());

async function opened(chart: string, stageWidth: number) {
  restore?.();
  restore = sizeStages(stageWidth, 480);
  const r = render(<MermaidDiagram chart={chart} />);
  await waitFor(() => expect(r.container.querySelector("[data-zoom]")).not.toBeNull());
  return { ...r, view: viewOf(r.container), svg: r.container.querySelector(".plan-stage svg")! };
}

describe("MermaidDiagram opening view", () => {
  it("a chart that fits opens whole, at any pane width", async () => {
    for (const pane of [400, 1100]) {
      const { view, unmount } = await opened("flowchart 300x300", pane);
      expect(view.zoom).toBeGreaterThanOrEqual(READABLE_ZOOM);
      expect(view.x).toBeGreaterThanOrEqual(0);
      expect(view.x + 300 * view.zoom).toBeLessThanOrEqual(pane);
      unmount();
    }
  });

  it("a tall chain is drawn at its natural size and opens readable from its top, clear of the hint", async () => {
    const { view, svg } = await opened("flowchart 220x9000", 900);
    expect(svg.getAttribute("width")).toBe("220");
    expect(svg.getAttribute("height")).toBe("9000");
    expect(Math.abs(view.zoom - READABLE_ZOOM)).toBeLessThan(1e-12);
    // Its top just below the band the hint line sits in.
    expect(view.y).toBe(HINT_BAND);
  });

  it("a tiny chart opens at the maximum scale, no larger", async () => {
    const { view } = await opened("flowchart 100x60", 1100);
    expect(MAX_DIAGRAM_SCALE).toBe(1.5);
    expect(view.zoom).toBe(MAX_DIAGRAM_SCALE);
  });

  it("an SVG without a viewBox size is reported in the diagram's place", async () => {
    restore?.();
    restore = sizeStages(800, 480);
    render(<MermaidDiagram chart="flowchart noviewbox" />);
    const box = await screen.findByTestId("mermaid-error");
    expect(box.textContent).toMatch(/viewBox/);
  });
});
