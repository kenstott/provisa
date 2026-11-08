// Copyright (c) 2026 Kenneth Stott
// Canary: 2b9f6a14-5c07-4e8d-a1d3-90c4e7b3f852
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: the ground symbol is a lucide-style icon (24 grid, 2px stroke), green when grounded,
// muted when its path awaits approval, named only when given a label.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { GroundIcon } from "../GroundIcon";
import { GROUND_PATHS } from "../groundPaths";
import { groundClass } from "../graph3d/nodeLook";
import { GraphLegend } from "../graph3d/GraphStage";
import type { GraphData } from "../graph3d/graphModel";

describe("GroundIcon", () => {
  it("is drawn on a 24 grid with a 2px round stroke in the text colour", () => {
    const { container } = render(<GroundIcon state="grounded" />);
    const svg = container.querySelector("svg")!;
    expect(svg).toHaveAttribute("viewBox", "0 0 24 24");
    expect(svg).toHaveAttribute("stroke", "currentColor");
    expect(svg).toHaveAttribute("stroke-width", "2");
    expect(svg).toHaveAttribute("stroke-linecap", "round");
    expect(svg).toHaveAttribute("stroke-linejoin", "round");
    expect(svg).toHaveAttribute("fill", "none");
    expect([...svg.querySelectorAll("path")].map((p) => p.getAttribute("d"))).toEqual([
      ...GROUND_PATHS,
    ]);
  });

  it("is green when grounded and muted when pending", () => {
    const { container, rerender } = render(<GroundIcon state="grounded" />);
    const svg = () => container.querySelector("svg")!;
    expect(svg()).toHaveAttribute("color", "var(--mantine-color-teal-6)");
    rerender(<GroundIcon state="grounded_pending" />);
    expect(svg()).toHaveAttribute("color", "var(--mantine-color-dimmed)");
    expect(svg()).toHaveAttribute("data-ground-state", "grounded_pending");
  });

  it("is named when given a label and hidden from assistive technology when not", () => {
    const { container, rerender } = render(<GroundIcon state="grounded" label="Grounded" />);
    expect(container.querySelector("svg")).toHaveAttribute("role", "img");
    expect(container.querySelector("svg")).toHaveAttribute("aria-label", "Grounded");
    rerender(<GroundIcon state="grounded" />);
    expect(container.querySelector("svg")).toHaveAttribute("aria-hidden", "true");
    expect(container.querySelector("svg")).not.toHaveAttribute("role");
  });
});

describe("the ground symbol on a graph label", () => {
  const CSS = readFileSync(resolve(__dirname, "../graph3d/graph3d.css"), "utf8");

  it("has a class per grounded state and none for a term that reaches no data", () => {
    expect(groundClass("grounded")).toBe("g3d-label-grounded");
    expect(groundClass("grounded_pending")).toBe("g3d-label-grounded-pending");
    expect(groundClass("not_grounded")).toBe("");
    expect(groundClass(undefined)).toBe("");
  });

  it("is cut from the same strokes as the icon", () => {
    for (const d of GROUND_PATHS) expect(CSS).toContain(`%3Cpath d='${d}'/%3E`);
    expect(CSS).toContain("stroke-width='2'");
    expect(CSS).toContain(".g3d-label-grounded::before");
    expect(CSS).toContain(".g3d-label-grounded-pending::before");
  });
});

describe("the legend", () => {
  const graph = (grounding?: "grounded" | "grounded_pending") =>
    ({
      nodes: [{ id: "term:1", type: "term", label: "Customer", ...(grounding ? { grounding } : {}) }],
      edges: [],
    }) as unknown as GraphData;

  it("keys both grounded states with the icon when a term is grounded", () => {
    const { getByTestId } = render(<GraphLegend graph={graph("grounded")} />);
    expect(getByTestId("legend-grounding-grounded").querySelector("svg")).toHaveAttribute(
      "data-ground-state",
      "grounded",
    );
    expect(getByTestId("legend-grounding-grounded_pending").querySelector("svg")).toHaveAttribute(
      "data-ground-state",
      "grounded_pending",
    );
  });

  it("omits the grounding keys when no node carries one", () => {
    const { queryByTestId } = render(<GraphLegend graph={graph()} />);
    expect(queryByTestId("legend-grounding-grounded")).toBeNull();
  });
});
