// Copyright (c) 2026 Kenneth Stott
// Canary: 5d0c2f7e-48a1-4b36-9e1d-b7a3c60e9f12
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A plan diagram fits the pane's width and the pane scrolls for the height; a short chart is
// never scaled past MAX_DIAGRAM_SCALE. jsdom has no layout, so Mermaid is mocked to return SVGs
// shaped like its own (viewBox, width="100%", inline max-width) and the tests read the sizing
// the component leaves on them, which the browser resolves against the pane.
import { describe, it, expect, vi } from "vitest";
import { render, waitFor } from "../../test-utils/render";
import { MermaidDiagram } from "../MermaidDiagram";
import { MAX_DIAGRAM_SCALE, fitSvgToWidth } from "../mermaidFit";

vi.mock("mermaid", () => ({
  default: {
    initialize: vi.fn(),
    parse: async () => ({ diagramType: "flowchart-v2" }),
    render: async (id: string, chart: string) => {
      const [w, h] = chart.split("\n")[0].replace("flowchart ", "").split("x");
      return {
        svg: `<svg id="${id}" width="100%" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${w} ${h}" style="max-width: ${w}px;"></svg>`,
      };
    },
  },
}));

async function drawn(chart: string, paneWidth: string): Promise<SVGSVGElement> {
  const { container } = render(
    <div style={{ width: paneWidth }}>
      <MermaidDiagram chart={chart} />
    </div>,
  );
  await waitFor(() => expect(container.querySelector(".stats-mermaid svg")).not.toBeNull());
  return container.querySelector(".stats-mermaid svg") as SVGSVGElement;
}

describe("MermaidDiagram fit to width", () => {
  it("the width style tracks the container: 100% at any pane width", async () => {
    for (const pane of ["400px", "1100px"]) {
      const svg = await drawn("flowchart 300x300", pane);
      expect(svg.style.width).toBe("100%");
    }
  });

  it("a tall chain gets width 100% and height auto, with no fixed size attributes", async () => {
    const svg = await drawn("flowchart 220x9000", "900px");
    expect(svg.style.width).toBe("100%");
    expect(svg.style.height).toBe("auto");
    expect(svg.hasAttribute("width")).toBe(false);
    expect(svg.hasAttribute("height")).toBe(false);
  });

  it("a tiny chart is capped at the maximum scale, not Mermaid's natural-width max", async () => {
    const svg = await drawn("flowchart 100x60", "1100px");
    expect(MAX_DIAGRAM_SCALE).toBe(1.5);
    expect(svg.style.maxWidth).toBe("150px");
  });

  it("fitSvgToWidth rejects an SVG without a viewBox width", () => {
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    expect(() => fitSvgToWidth(svg)).toThrow(/viewBox/);
  });
});
