// Copyright (c) 2026 Kenneth Stott
// Canary: edbd6b8a-2aa4-4a80-8aec-fc4b1d9a4d2f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (FIT MEANS THE WHOLE IMAGE), REQ-2277, on every graph surface (the lead's decision): the
// result graph's fit control fits the whole graph as the ERD's does, and its zoom controls zoom about
// the canvas's centre and make the view the reader's own, which a resize keeps. The fit was
// cytoscape's own, held at the least zoom and leaving no mode for a resize to keep.
// REQ-2239 (amended 2026-11-27): the network graph's hover command bar is the one every graph shares
// (GraphControls): zoom, fit, full screen and download, then its own commands. Its downloads go
// through the one image export every graph uses (exportImage.ts): on the canvas colour, named after
// the view, with the cluster hulls.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import type { CyInstance } from "../cytoscape-types";

const { fitWhole, zoomAboutCentre, exportSvg, exportPng, exportJpeg } = vi.hoisted(() => ({
  fitWhole: vi.fn(),
  zoomAboutCentre: vi.fn(),
  exportSvg: vi.fn(),
  exportPng: vi.fn(),
  exportJpeg: vi.fn(),
}));
vi.mock("../../provisa-graph/viewport", async (actual) => ({
  ...(await actual<typeof import("../../provisa-graph/viewport")>()),
  fitWhole,
  zoomAboutCentre,
}));
vi.mock("../../provisa-graph/exportImage", async (actual) => ({
  ...(await actual<typeof import("../../provisa-graph/exportImage")>()),
  exportSvg,
  exportPng,
  exportJpeg,
}));

import { ResultGraphControls } from "../canvas/ResultGraphControls";
import { FIT_MARGIN, type LayoutMode } from "../canvas/canvas-types";

function controls(layoutMode: LayoutMode = "force", toggleLayout = vi.fn()) {
  const cy = {} as CyInstance;
  const stage = document.createElement("div");
  render(
    <ResultGraphControls
      cyRef={{ current: cy }}
      stageRef={{ current: stage }}
      canvas="#101113"
      exportName="cypher-graph"
      layoutMode={layoutMode}
      toggleLayout={toggleLayout}
      nudgeHeldRef={{ current: false }}
      nudgeLayout={vi.fn()}
      nudgeLayoutRef={{ current: vi.fn() }}
      edgeDistance={1}
      setEdgeDistance={vi.fn()}
      edgeDistanceRef={{ current: 1 }}
      nudgeTimerRef={{ current: null }}
    />,
  );
  return { cy, stage };
}

describe("the result graph's camera controls", () => {
  it("the fit control fits the whole graph", () => {
    const { cy } = controls();
    fireEvent.click(screen.getByTestId("canvas-fit-all"));
    expect(fitWhole).toHaveBeenCalledWith(cy, FIT_MARGIN);
  });

  it("the zoom controls zoom about the canvas's centre", () => {
    const { cy } = controls();
    fireEvent.click(screen.getByTestId("canvas-zoom-in"));
    expect(zoomAboutCentre).toHaveBeenLastCalledWith(cy, 1.3);
    fireEvent.click(screen.getByTestId("canvas-zoom-out"));
    expect(zoomAboutCentre).toHaveBeenLastCalledWith(cy, 1 / 1.3);
  });
});

describe("the result graph's command bar is the shared one", () => {
  it("is the shared vertical bar: zoom, fit, full screen, download, then its own commands", () => {
    controls();
    const bar = screen.getByTestId("canvas-canvas-bar");
    expect(bar).toHaveClass("pg-controls");
    expect(bar).toHaveAttribute("aria-orientation", "vertical");
    const order = [
      "canvas-zoom-in",
      "canvas-zoom-out",
      "canvas-fit-all",
      "canvas-fullscreen",
      "canvas-download",
      "canvas-toggle-layout",
      "canvas-nudge-layout",
      "canvas-edge-distance",
    ];
    const found = order.map((id) => within(bar).getByTestId(id));
    for (let i = 1; i < found.length; i++)
      expect(
        found[i - 1].compareDocumentPosition(found[i]) & Node.DOCUMENT_POSITION_FOLLOWING,
      ).toBeTruthy();
  });

  it("full screen puts the canvas's own box full screen", () => {
    const { stage } = controls();
    const requestFullscreen = vi.fn(() => Promise.resolve());
    stage.requestFullscreen = requestFullscreen;
    fireEvent.click(screen.getByTestId("canvas-fullscreen"));
    expect(requestFullscreen).toHaveBeenCalled();
  });

  it("download offers SVG, PNG and JPEG through the one image export, on the canvas colour, named after the view, with the hulls", async () => {
    const { cy, stage } = controls();
    const hulls = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    hulls.setAttribute("class", "gf-hulls");
    stage.appendChild(hulls);
    fireEvent.click(screen.getByTestId("canvas-download"));
    expect(await screen.findByTestId("canvas-export-svg")).toBeInTheDocument();
    expect(screen.queryByTestId("canvas-export-json")).toBeNull();
    const image = { cy, bg: "#101113", name: "cypher-graph", hulls };
    fireEvent.click(screen.getByTestId("canvas-export-svg"));
    expect(exportSvg).toHaveBeenCalledWith(image);
    fireEvent.click(screen.getByTestId("canvas-download"));
    fireEvent.click(await screen.findByTestId("canvas-export-png"));
    expect(exportPng).toHaveBeenLastCalledWith(image);
    fireEvent.click(screen.getByTestId("canvas-download"));
    fireEvent.click(await screen.findByTestId("canvas-export-jpeg"));
    expect(exportJpeg).toHaveBeenLastCalledWith(image);
  });

  it("with no cluster drawn, an image has no hulls", async () => {
    const { cy } = controls();
    fireEvent.click(screen.getByTestId("canvas-download"));
    fireEvent.click(await screen.findByTestId("canvas-export-png"));
    expect(exportPng).toHaveBeenLastCalledWith({
      cy,
      bg: "#101113",
      name: "cypher-graph",
      hulls: null,
    });
  });

  it("the layout toggle shows the hierarchy layout pressed and switches on click", () => {
    const toggle = vi.fn();
    controls("hierarchy", toggle);
    const button = screen.getByTestId("canvas-toggle-layout");
    expect(button).toHaveAttribute("aria-pressed", "true");
    fireEvent.click(button);
    expect(toggle).toHaveBeenCalledTimes(1);
  });
});
