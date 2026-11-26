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

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import type { CyInstance } from "../cytoscape-types";

const { fitWhole, zoomAboutCentre } = vi.hoisted(() => ({
  fitWhole: vi.fn(),
  zoomAboutCentre: vi.fn(),
}));
vi.mock("../../provisa-graph/viewport", async (actual) => ({
  ...(await actual<typeof import("../../provisa-graph/viewport")>()),
  fitWhole,
  zoomAboutCentre,
}));

import { CanvasControls } from "../canvas/CanvasControls";
import { FIT_MARGIN } from "../canvas/canvas-types";

function controls() {
  const cy = {} as CyInstance;
  render(
    <CanvasControls
      cyRef={{ current: cy }}
      layoutMode="force"
      toggleLayout={vi.fn()}
      nudgeHeldRef={{ current: false }}
      nudgeLayout={vi.fn()}
      nudgeLayoutRef={{ current: vi.fn() }}
      edgeDistance={1}
      setEdgeDistance={vi.fn()}
      edgeDistanceRef={{ current: 1 }}
      nudgeTimerRef={{ current: null }}
    />,
  );
  return cy;
}

describe("the result graph's camera controls", () => {
  it("the fit control fits the whole graph", () => {
    const cy = controls();
    fireEvent.click(screen.getByTestId("canvas-fit-view"));
    expect(fitWhole).toHaveBeenCalledWith(cy, FIT_MARGIN);
  });

  it("the zoom controls zoom about the canvas's centre", () => {
    const cy = controls();
    fireEvent.click(screen.getByTestId("canvas-zoom-in"));
    expect(zoomAboutCentre).toHaveBeenLastCalledWith(cy, 1.3);
    fireEvent.click(screen.getByTestId("canvas-zoom-out"));
    expect(zoomAboutCentre).toHaveBeenLastCalledWith(cy, 0.77);
  });
});
