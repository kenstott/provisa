// Copyright (c) 2026 Kenneth Stott
// Canary: b0eff562-73b0-47ff-8bf1-1632cdb8d5d5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// How close a force layout draws its nodes: the layout's spacing follows the attraction, and the
// graph's slider moves it, relaying out where the camera already is. And an edge, hovered, names
// its type with its source and target.

import { act } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../../test-utils/render";
import { forceLayout } from "../forceLayout";
import type { GraphData } from "../../../../components/graph3d/graphModel";

type Calls = Record<string, ReturnType<typeof vi.fn>>;
type Hovers = { onHover: (h: unknown, x: number, y: number) => void };
const made = vi.hoisted(() => ({ scenes: [] as Calls[], callbacks: [] as Hovers[] }));
vi.mock("../../../../components/graph3d/graphScene", () => ({
  GraphScene: class {
    constructor(_canvas: unknown, _labels: unknown, callbacks: Hovers) {
      made.callbacks.push(callbacks);
      const calls: Calls = {};
      for (const m of [
        "setTheme",
        "setRotate",
        "dispose",
        "resetView",
        "setHover",
        "setData",
        "setSelected",
        "focus",
        "setFrontal",
      ])
        calls[m] = vi.fn();
      Object.assign(this, calls);
      made.scenes.push(calls);
    }
  },
}));

import { GraphExplorer } from "../../../../components/graph3d/GraphExplorer";
import { GLOSSARY_SCOPE } from "../../../../components/graph3d/graphModel";

const extent = (at: Map<string, [number, number, number]>) =>
  Math.max(...[...at.values()].map(([x, y, z]) => Math.hypot(x, y, z)));

const IDS = ["a", "b", "c", "d"];
const SPRINGS = [
  { a: "a", b: "b", strength: 0.5 },
  { a: "c", b: "d", strength: 0.5 },
];

describe("a force layout's attraction", () => {
  it("lands the nodes closer together as it rises, and 1 is the usual spacing", () => {
    const usual = forceLayout(IDS, SPRINGS, 6);
    expect(forceLayout(IDS, SPRINGS, 6, 1)).toEqual(usual);
    expect(extent(forceLayout(IDS, SPRINGS, 6, 2))).toBeCloseTo(extent(usual) / 2, 5);
    expect(extent(forceLayout(IDS, SPRINGS, 6, 0.5))).toBeCloseTo(extent(usual) * 2, 5);
  });
});

describe("the graph's Attraction slider", () => {
  beforeEach(() => {
    made.scenes.length = 0;
    made.callbacks.length = 0;
    window.sessionStorage.clear();
  });

  const g: GraphData = {
    nodes: [
      { id: "term:1", type: "term", label: "Customer" },
      { id: "term:2", type: "term", label: "Party" },
      { id: "term:3", type: "term", label: "Order" },
    ],
    edges: [{ id: "e", a: "term:1", b: "term:2", kind: "term_edge", label: "KIND_OF" }],
  };

  it("tightens the layout where the camera is, and keeps its setting for the session", async () => {
    const view = render(
      <GraphExplorer data={g} scope={GLOSSARY_SCOPE} preset="ontology" zero={[]} testId="x" />,
    );
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    const drawn = () =>
      made.scenes[0].setData.mock.lastCall as [
        GraphData,
        Map<string, [number, number, number]>,
        boolean,
      ];
    await waitFor(() => expect(drawn()).toBeDefined());
    const before = extent(drawn()[1]);
    const slider = screen.getByRole("slider", { name: "Attraction" });
    slider.focus();
    for (let i = 0; i < 5; i++)
      fireEvent.keyDown(slider, { key: "ArrowRight", code: "ArrowRight" });
    await waitFor(() => expect(extent(drawn()[1])).toBeLessThan(before));
    // Relaid out in place: the camera is not reset.
    expect(drawn()[2]).toBe(false);
    view.unmount();
    render(
      <GraphExplorer data={g} scope={GLOSSARY_SCOPE} preset="ontology" zero={[]} testId="x" />,
    );
    expect(screen.getByRole("slider", { name: "Attraction" })).toHaveAttribute(
      "aria-valuenow",
      "1.5",
    );
  });

  it("names a hovered edge's type with its source and target", async () => {
    render(
      <GraphExplorer data={g} scope={GLOSSARY_SCOPE} preset="ontology" zero={[]} testId="x" />,
    );
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    const edge = (made.scenes[0].setData.mock.lastCall?.[0] as GraphData).edges[0];
    act(() => made.callbacks[0].onHover({ kind: "edge", id: edge.id }, 10, 10));
    const tip = await screen.findByTestId("x-stage-tip");
    expect(tip).toHaveTextContent("Customer → Party");
    expect(tip).toHaveTextContent("KIND_OF");
  });

  it("is left out of a graph laid out by a fixed rule", () => {
    render(
      <GraphExplorer data={g} scope={GLOSSARY_SCOPE} preset="taxonomy" zero={[]} testId="x" />,
    );
    expect(screen.queryByRole("slider", { name: "Attraction" })).toBeNull();
  });
});
