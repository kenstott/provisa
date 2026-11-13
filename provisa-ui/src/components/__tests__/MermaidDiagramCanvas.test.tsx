// Copyright (c) 2026 Kenneth Stott
// Canary: 5f7cef96-39bc-4f7a-8fa6-588dfa806ea8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (the view controls sit in the canvas; fit means the whole image; a diagram opens
// readable): the query plan diagram has the hover controls every other diagram has, opens at the
// readable zoom with its root in view, follows the theme, and marks the operator picked.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen, waitFor } from "../../test-utils/render";
import { MantineProvider } from "@mantine/core";
import { theme } from "../../theme/theme";
import { MermaidDiagram } from "../MermaidDiagram";
import { READABLE_ZOOM } from "../provisa-graph/viewport";
import { HINT_BAND } from "../mermaidFit";
import { graphTokens } from "../provisa-graph/tokens";
import { mockSvg, sizeStages, viewOf } from "./mermaidMock";

const initialize = vi.fn();
vi.mock("mermaid", () => ({
  default: {
    initialize: (cfg: unknown) => initialize(cfg),
    parse: async () => ({ diagramType: "flowchart-v2" }),
    render: async (id: string, chart: string) => ({ svg: mockSvg(id, chart) }),
  },
}));

const STAGE_W = 800;
const STAGE_H = 500;
// A large plan: an image far bigger than its canvas, its root at the top middle.
const LARGE = [
  "flowchart TD",
  "%% size 4000x3000",
  "%% node route 2000 30",
  "%% node p1 2000 120",
  "%% node p2 1000 220",
  "%% node p3 3800 2900",
].join("\n");

let restore: () => void;
beforeEach(() => {
  initialize.mockClear();
  restore = sizeStages(STAGE_W, STAGE_H);
});
afterEach(() => restore());

async function drawn(ui: React.ReactElement) {
  const r = render(ui);
  await waitFor(() => expect(r.container.querySelector(".plan-stage svg")).not.toBeNull());
  return r;
}

const close = (a: number, b: number) => Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(b));

describe("plan diagram controls", () => {
  it("renders the shared hover controls, each by its accessible name, and the hint", async () => {
    await drawn(<MermaidDiagram chart={LARGE} />);
    const bar = screen.getByRole("toolbar", { name: "Canvas controls" });
    for (const name of [
      "Zoom in",
      "Zoom out",
      "Fit all",
      "Fit width",
      "Full screen",
      "Download",
      "Copy as Mermaid",
    ]) {
      expect(bar.contains(screen.getByRole("button", { name }))).toBe(true);
    }
    expect(screen.getByText("Drag to pan · scroll to move · Ctrl/⌘-scroll to zoom")).toBeTruthy();
  });

  it("zoom in and zoom out change the view's zoom", async () => {
    const { container } = await drawn(<MermaidDiagram chart={LARGE} />);
    const before = viewOf(container).zoom;
    fireEvent.click(screen.getByRole("button", { name: "Zoom in" }));
    const zoomedIn = viewOf(container).zoom;
    expect(zoomedIn).toBeGreaterThan(before);
    fireEvent.click(screen.getByRole("button", { name: "Zoom out" }));
    fireEvent.click(screen.getByRole("button", { name: "Zoom out" }));
    expect(viewOf(container).zoom).toBeLessThan(before);
  });

  it("fit all shows the whole image, however small; fit width fits its width", async () => {
    const { container } = await drawn(<MermaidDiagram chart={LARGE} />);
    fireEvent.click(screen.getByRole("button", { name: "Fit all" }));
    const whole = viewOf(container);
    // The whole 4000x3000 image inside the 800x500 stage: a 10px margin round it, the hint's band above.
    expect(close(whole.zoom, Math.min((STAGE_W - 20) / 4000, (STAGE_H - HINT_BAND - 10) / 3000))).toBe(true);
    expect(whole.zoom).toBeLessThan(READABLE_ZOOM);
    expect(whole.x).toBeGreaterThanOrEqual(0);
    expect(whole.x + 4000 * whole.zoom).toBeLessThanOrEqual(STAGE_W);
    expect(whole.y).toBeGreaterThanOrEqual(0);
    expect(whole.y + 3000 * whole.zoom).toBeLessThanOrEqual(STAGE_H);
    fireEvent.click(screen.getByRole("button", { name: "Fit width" }));
    expect(close(viewOf(container).zoom, (STAGE_W - 20) / 4000)).toBe(true);
  });

  it("opens at the readable zoom with the root in view, not squeezed to the width", async () => {
    const { container } = await drawn(<MermaidDiagram chart={LARGE} />);
    const v = viewOf(container);
    expect(close(v.zoom, READABLE_ZOOM)).toBe(true);
    const rootX = v.x + 2000 * v.zoom;
    const rootY = v.y + 30 * v.zoom;
    expect(rootX).toBeGreaterThan(0);
    expect(rootX).toBeLessThan(STAGE_W);
    expect(rootY).toBeGreaterThan(0);
    expect(rootY).toBeLessThan(STAGE_H);
  });

  it("scrolling moves the view; Ctrl-scroll zooms it", async () => {
    const { container } = await drawn(<MermaidDiagram chart={LARGE} />);
    const stage = container.querySelector(".plan-stage") as HTMLElement;
    const before = viewOf(container);
    act(() => {
      stage.dispatchEvent(
        new WheelEvent("wheel", { deltaY: 100, bubbles: true, cancelable: true }),
      );
    });
    const moved = viewOf(container);
    expect(moved.y).toBeLessThan(before.y);
    expect(moved.zoom).toBe(before.zoom);
    act(() => {
      stage.dispatchEvent(
        new WheelEvent("wheel", { deltaY: -100, ctrlKey: true, bubbles: true, cancelable: true }),
      );
    });
    expect(viewOf(container).zoom).toBeGreaterThan(before.zoom);
  });

  it("dragging pans the view", async () => {
    const { container } = await drawn(<MermaidDiagram chart={LARGE} />);
    const stage = container.querySelector(".plan-stage") as HTMLElement;
    const before = viewOf(container);
    fireEvent.pointerDown(stage, { clientX: 400, clientY: 300, button: 0 });
    fireEvent.pointerMove(stage, { clientX: 340, clientY: 220 });
    fireEvent.pointerUp(stage, { clientX: 340, clientY: 220 });
    const after = viewOf(container);
    expect(after.x).toBe(before.x - 60);
    expect(after.y).toBe(before.y - 80);
  });

  it("follows the theme: light and dark tokens of the one graph, on the canvas and the boxes", async () => {
    for (const scheme of ["light", "dark"] as const) {
      initialize.mockClear();
      const tokens = graphTokens(scheme === "dark");
      const { container, unmount } = await drawn(
        <MantineProvider theme={theme} forceColorScheme={scheme}>
          <MermaidDiagram chart={LARGE} />
        </MantineProvider>,
      );
      const cfg = initialize.mock.calls.at(-1)?.[0] as {
        theme: string;
        themeVariables: Record<string, string>;
      };
      expect(cfg.theme).toBe("base");
      expect(cfg.themeVariables.primaryColor).toBe(tokens.nodeFill);
      expect(cfg.themeVariables.primaryBorderColor).toBe(tokens.nodeBorder);
      expect(cfg.themeVariables.primaryTextColor).toBe(tokens.text);
      expect(cfg.themeVariables.lineColor).toBe(tokens.edge);
      const stage = container.querySelector(".plan-stage") as HTMLElement;
      expect(stage.style.getPropertyValue("--pg-canvas")).toBe(tokens.canvas);
      unmount();
    }
  });

  it("a click on a node names it; the node selected is marked", async () => {
    const onNodeClick = vi.fn();
    const { container, rerender } = await drawn(
      <MermaidDiagram chart={LARGE} onNodeClick={onNodeClick} />,
    );
    const p2 = container.querySelector('g.node[id*="-flowchart-p2-"]') as Element;
    fireEvent.click(p2.querySelector("rect") as Element);
    expect(onNodeClick).toHaveBeenCalledWith("p2");
    const before = viewOf(container);
    rerender(<MermaidDiagram chart={LARGE} onNodeClick={onNodeClick} selected="p2" />);
    await waitFor(() => expect(p2.classList.contains("plan-node-selected")).toBe(true));
    // Picked where the reader is looking: the view stays as it is.
    expect(viewOf(container)).toEqual(before);
  });

  it("a node selected from outside is brought into view", async () => {
    const { container, rerender } = await drawn(<MermaidDiagram chart={LARGE} />);
    rerender(<MermaidDiagram chart={LARGE} selected="p3" />);
    await waitFor(() => {
      const v = viewOf(container);
      const x = v.x + 3800 * v.zoom;
      const y = v.y + 2900 * v.zoom;
      expect(x > 0 && x < STAGE_W && y > 0 && y < STAGE_H).toBe(true);
    });
  });
});
