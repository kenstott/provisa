// Copyright (c) 2026 Kenneth Stott
// Canary: 0fc34c5c-45b1-4656-8302-3b58802889b4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Every 3D network graph's toolbar has one Grid checkbox, checked at first. Clearing it tells the
// scene to leave the floor grid out, and the choice is kept across reloads, per graph.

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
import { GLOSSARY_SCOPE } from "../../../../components/graph3d/graphModel";

const g: GraphData = {
  nodes: [
    { id: "term:1", type: "term", label: "Customer" },
    { id: "term:2", type: "term", label: "Party" },
  ],
  edges: [{ id: "e", a: "term:1", b: "term:2", kind: "term_edge", label: "KIND_OF" }],
};

describe("a 3D network graph's Grid checkbox", () => {
  beforeEach(() => {
    made.scenes.length = 0;
  });

  it("is checked at first, turns the scene's grid off, and keeps that across reloads", async () => {
    const view = render(
      <GraphExplorer
        data={g}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        settingsKey={{ kind: "test", subject: null }}
        exportName="test"
        testId="x"
      />,
    );
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    const scene = made.scenes[0];
    await waitFor(() => expect(scene.setGrid).toHaveBeenLastCalledWith(true));
    const box = screen.getByRole("checkbox", { name: "Grid" });
    expect(box).toBeChecked();
    fireEvent.click(box);
    await waitFor(() => expect(scene.setGrid).toHaveBeenLastCalledWith(false));
    view.unmount();
    render(
      <GraphExplorer
        data={g}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        settingsKey={{ kind: "test", subject: null }}
        exportName="test"
        testId="x"
      />,
    );
    expect(screen.getByRole("checkbox", { name: "Grid" })).not.toBeChecked();
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    await waitFor(() => expect(made.scenes[1].setGrid).toHaveBeenLastCalledWith(false));
  });

  it("is its own per graph", () => {
    render(
      <GraphExplorer
        data={g}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        settingsKey={{ kind: "test", subject: null }}
        exportName="test"
        testId="x"
      />,
    );
    fireEvent.click(screen.getByRole("checkbox", { name: "Grid" }));
    render(
      <GraphExplorer
        data={g}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        settingsKey={{ kind: "test", subject: "another" }}
        exportName="test"
        testId="y"
      />,
    );
    expect(screen.getAllByRole("checkbox", { name: "Grid" })[1]).toBeChecked();
  });
});
