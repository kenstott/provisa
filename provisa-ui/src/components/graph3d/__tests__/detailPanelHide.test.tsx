// Copyright (c) 2026 Kenneth Stott
// Canary: da87427a-0713-411f-a78e-7628825ae70e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: the graph view's detail panel is hidden and shown again by the control on its edge;
// the canvas takes the full width and the graph is fitted to it again; the choice is kept per view.
// In 3D the scene is fitted to its new width (BaseScene.fitToStage); in 2D the lanes' graph stays
// mounted, so its own resize observer follows the canvas (provisa-graph resizeKeepsView.test.ts).

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";

const scenes = vi.hoisted(() => [] as Record<string, ReturnType<typeof vi.fn>>[]);
const laneMounts = vi.hoisted(() => ({ count: 0 }));

vi.mock("../graphScene", () => ({
  GraphScene: class {
    constructor() {
      for (const m of [
        "setTheme",
        "setRotate",
        "setGrid",
        "dispose",
        "resetView",
        "fitToStage",
        "setHover",
        "setData",
        "setSelected",
        "focus",
        "setFlat",
        "setArcs",
      ])
        (this as Record<string, unknown>)[m] = vi.fn();
      scenes.push(this as unknown as Record<string, ReturnType<typeof vi.fn>>);
    }
  },
}));
vi.mock("../LaneStage", async () => {
  const { useEffect } = await import("react");
  return {
    LaneStage: () => {
      useEffect(() => {
        laneMounts.count += 1;
      }, []);
      return <div data-testid="x-lanes" />;
    },
  };
});

import { GraphExplorer } from "../GraphExplorer";
import { ONTOLOGY_SCOPE, type GraphData } from "../graphModel";
import { readCollapsibleStates } from "../../collapsible/collapsibleState";

const DATA: GraphData = {
  nodes: [
    { id: "term:1", type: "term", label: "Customer", domains: ["sales"], status: "approved" },
    { id: "term:2", type: "term", label: "Party", domains: ["sales"], status: "approved" },
  ],
  edges: [{ id: "e1", a: "term:1", b: "term:2", kind: "term_edge", label: "KIND_OF" }],
};

const explorer = (kind: string, flat: boolean) => (
  <GraphExplorer
    data={DATA}
    scope={ONTOLOGY_SCOPE}
    preset="ontology"
    depth={2}
    selected="term:1"
    settingsKey={{ kind, subject: null }}
    exportName="ontology"
    testId="x"
    opensFlat={flat}
  />
);

const legend = () => screen.queryByTestId("x-shown-graph3d.type_term");

beforeEach(() => {
  window.localStorage.clear();
  scenes.length = 0;
  laneMounts.count = 0;
});

/** The stage drawn: in 3D the scene, made once; in 2D the lanes' graph, mounted once. */
async function stageUp(flat: boolean, made = 1) {
  if (flat) await waitFor(() => expect(laneMounts.count).toBe(made));
  else await waitFor(() => expect(scenes).toHaveLength(made));
}

describe.each([
  ["3D", false],
  ["2D", true],
])("the %s graph's detail panel", (_name, flat) => {
  it("is shown with the picked node and the Shown legend until the person hides it", async () => {
    render(explorer("panel-view", flat));
    await stageUp(flat);
    expect(screen.getByTestId("x-detail")).toBeInTheDocument();
    expect(legend()).toBeInTheDocument();
    expect(screen.getByTestId("x-side-panel")).not.toHaveClass("is-hidden");
  });

  it("hides and shows again, the graph fitted to its canvas each time", async () => {
    render(explorer("panel-view", flat));
    await stageUp(flat);
    const scene = scenes[0];
    if (!flat) expect(scene.fitToStage).not.toHaveBeenCalled();

    fireEvent.click(screen.getByTestId("x-side-panel-toggle"));
    expect(screen.getByTestId("x-side-panel")).toHaveClass("is-hidden");
    expect(screen.queryByTestId("x-detail")).not.toBeInTheDocument();
    expect(legend()).not.toBeInTheDocument();
    if (!flat) await waitFor(() => expect(scene.fitToStage).toHaveBeenCalledTimes(1));

    fireEvent.click(screen.getByTestId("x-side-panel-toggle"));
    expect(screen.getByTestId("x-side-panel")).not.toHaveClass("is-hidden");
    expect(screen.getByTestId("x-detail")).toBeInTheDocument();
    if (!flat) await waitFor(() => expect(scene.fitToStage).toHaveBeenCalledTimes(2));
    // The stage is the same one throughout, never drawn afresh: in 2D its own resize observer
    // fits the graph to the canvas's new width.
    expect(flat ? laneMounts.count : scenes.length).toBe(1);
    if (flat) expect(scenes).toHaveLength(0);
  });

  it("keeps the choice per view for the person", async () => {
    const first = render(explorer("panel-view", flat));
    await stageUp(flat);
    fireEvent.click(screen.getByTestId("x-side-panel-toggle"));
    first.unmount();
    expect(readCollapsibleStates()["side-panels"]).toEqual({ "panel-view": false });

    // The same view opens with the panel hidden, and the graph is not refitted for it.
    const again = render(explorer("panel-view", flat));
    await stageUp(flat, 2);
    expect(screen.getByTestId("x-side-panel")).toHaveClass("is-hidden");
    expect(legend()).not.toBeInTheDocument();
    if (!flat) expect(scenes[1].fitToStage).not.toHaveBeenCalled();
    again.unmount();

    // Another view keeps its own choice.
    render(explorer("other-view", flat));
    expect(screen.getByTestId("x-side-panel")).not.toHaveClass("is-hidden");
    expect(legend()).toBeInTheDocument();
  });
});
