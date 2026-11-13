// Copyright (c) 2026 Kenneth Stott
// Canary: 0fc34c5c-45b1-4656-8302-3b58802889b4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Every 3D network graph's canvas controls have one Grid toggle (REQ-2239, amended 2026-11-13), released at first: no floor grid is drawn until the
// reader turns Grid on. Pressing it tells the scene to draw the floor grid, and the choice, on or off,
// is kept across reloads, per graph.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../../test-utils/render";
import type { GraphData } from "../../../../components/graph3d/graphModel";

type Calls = Record<string, ReturnType<typeof vi.fn>>;
const made = vi.hoisted(() => ({ scenes: [] as Calls[] }));
vi.mock("../../../../components/graph3d/graphScene", () => ({
  GraphScene: class {
    constructor() {
      const calls: Calls = {};
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
        calls[m] = vi.fn();
      Object.assign(this, calls);
      made.scenes.push(calls);
    }
  },
}));

import { GraphExplorer } from "../../../../components/graph3d/GraphExplorer";
import { ONTOLOGY_SCOPE } from "../../../../components/graph3d/graphModel";

const g: GraphData = {
  nodes: [
    { id: "term:1", type: "term", label: "Customer" },
    { id: "term:2", type: "term", label: "Party" },
  ],
  edges: [{ id: "e", a: "term:1", b: "term:2", kind: "term_edge", label: "KIND_OF" }],
};

describe("a 3D network graph's Grid toggle", () => {
  beforeEach(() => {
    made.scenes.length = 0;
  });

  const open = (subject: string | null = null, testId = "x") => (
    <GraphExplorer
      data={g}
      scope={ONTOLOGY_SCOPE}
      preset="ontology"
      depth={2}
      settingsKey={{ kind: "test", subject }}
      exportName="test"
      testId={testId}
    />
  );
  const grid = () => screen.getByRole("button", { name: "Grid", hidden: true });

  it("is released at first and draws no grid", async () => {
    render(open());
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    await waitFor(() => expect(made.scenes[0].setGrid).toHaveBeenLastCalledWith(false));
    expect(grid()).toHaveAttribute("aria-pressed", "false");
  });

  it("is on after a remount once turned on, and off after a remount once turned off again", async () => {
    const first = render(open());
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    fireEvent.click(grid());
    await waitFor(() => expect(made.scenes[0].setGrid).toHaveBeenLastCalledWith(true));
    first.unmount();
    const second = render(open());
    expect(grid()).toHaveAttribute("aria-pressed", "true");
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    await waitFor(() => expect(made.scenes[1].setGrid).toHaveBeenLastCalledWith(true));
    fireEvent.click(grid());
    await waitFor(() => expect(made.scenes[1].setGrid).toHaveBeenLastCalledWith(false));
    second.unmount();
    render(open());
    expect(grid()).toHaveAttribute("aria-pressed", "false");
    await waitFor(() => expect(made.scenes).toHaveLength(3));
    await waitFor(() => expect(made.scenes[2].setGrid).toHaveBeenLastCalledWith(false));
  });

  it("is its own per graph", () => {
    render(
      <GraphExplorer
        data={g}
        scope={ONTOLOGY_SCOPE}
        preset="ontology"
        depth={2}
        settingsKey={{ kind: "test", subject: null }}
        exportName="test"
        testId="x"
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: "Grid", hidden: true }));
    render(
      <GraphExplorer
        data={g}
        scope={ONTOLOGY_SCOPE}
        preset="ontology"
        depth={2}
        settingsKey={{ kind: "test", subject: "another" }}
        exportName="test"
        testId="y"
      />,
    );
    expect(screen.getAllByRole("button", { name: "Grid", hidden: true })[1]).toHaveAttribute(
      "aria-pressed",
      "false",
    );
  });
});
