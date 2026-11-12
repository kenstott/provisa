// Copyright (c) 2026 Kenneth Stott
// Canary: 3e8b5f21-9c47-4d6a-b0e3-71f2a8c94d15
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-11, VIEW CONTROLS SIT IN THE CANVAS, IN 2D AND 3D): every 3D view -- the
// ontology graph and the four table-profile views -- has the one hover stack of the 2D graphs in its
// canvas, holding Reset view, Rotate, Grid (where the scene draws a floor grid), Full screen and
// Download. Rotate and Grid are toggles that show their state and read and write the settings the
// toolbar's pills did; no toolbar holds Rotate, Grid or Reset view. jsdom has no WebGL, so each
// three.js scene is replaced by a recorder.

import type { ReactElement } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../../test-utils/render";
import type { GraphData } from "../../../../components/graph3d/graphModel";

type Calls = Record<string, ReturnType<typeof vi.fn>>;
const made = vi.hoisted(() => ({ scenes: [] as { kind: string; calls: Calls }[] }));
const api = vi.hoisted(() => ({ fetchProfileRun: vi.fn() }));
vi.mock("../../../../api/profiler", () => api);

const recorder = vi.hoisted(
  () => (kind: string, extra: string[]) =>
    class {
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
          ...extra,
        ])
          calls[m] = vi.fn();
        Object.assign(this, calls);
        made.scenes.push({ kind, calls });
      }
    },
);
vi.mock("../terrain/terrainScene", () => ({
  TerrainScene: recorder("terrain", ["setOptions", "setOrder"]),
}));
vi.mock("../relations/relationsScene", () => ({
  RelationsScene: recorder("relations", ["setEdges"]),
}));
vi.mock("../joint/jointScene", () => ({ JointScene: recorder("joint", ["setColor"]) }));
vi.mock("../drift/driftScene", () => ({ DriftScene: recorder("drift", []) }));
vi.mock("../../../../components/graph3d/graphScene", () => ({
  GraphScene: recorder("graph", ["setFlat", "setArcs"]),
}));

import { ProfileTerrain } from "../terrain/ProfileTerrain";
import { ProfileRelations } from "../relations/ProfileRelations";
import { ProfileJointCloud } from "../joint/ProfileJointCloud";
import { ProfileDriftStrata } from "../drift/ProfileDriftStrata";
import { GraphExplorer } from "../../../../components/graph3d/GraphExplorer";
import { ONTOLOGY_SCOPE } from "../../../../components/graph3d/graphModel";

const RESULTS = {
  columns: [
    { column_name: "amount", family: "numeric", row_count: 100, null_count: 0, null_share: 0 },
    { column_name: "plan", family: "text", row_count: 100, null_count: 0, null_share: 0 },
    { column_name: "age", family: "numeric", row_count: 100, null_count: 10, null_share: 0.1 },
  ],
  quantiles: Array.from({ length: 101 }, (_, i) => ({
    column_name: "amount",
    measure: "value",
    q: i / 100,
    value: i,
  })),
  correlations: [
    { column_name: "amount", other_column: "age", measure: "spearman", value: 0.9, rows: 90 },
  ],
  dependencies: [
    {
      column_name: "amount",
      parent_1: "plan",
      parent_2: null,
      in_network: true,
      uncertainty: 0.5,
      mutual_information: 0.3,
    },
  ],
  joint_counts: [
    {
      column_name: "amount",
      parent_1: "plan",
      parent_2: null,
      target_bucket: 0,
      target_value: null,
      parent_1_value: "free",
      parent_1_bucket: null,
      row_count: 60,
    },
  ],
};

const GRAPH: GraphData = {
  nodes: [
    { id: "term:1", type: "term", label: "Customer" },
    { id: "term:2", type: "term", label: "Party" },
  ],
  edges: [{ id: "e", a: "term:1", b: "term:2", kind: "term_edge", label: "KIND_OF" }],
};

interface View {
  name: string;
  kind: string;
  /** The view's own test id: its toolbar's controls and the stack's are named after it. */
  id: string;
  stage: string;
  /** The scene draws a floor grid: the stack offers Grid. */
  grid: boolean;
  draw: () => ReactElement;
}

const VIEWS: View[] = [
  {
    name: "the terrain",
    kind: "terrain",
    id: "terrain",
    stage: "terrain-stage",
    grid: false,
    draw: () => <ProfileTerrain results={RESULTS} tableId={7} tableName="orders" />,
  },
  {
    name: "the relationships map",
    kind: "relations",
    id: "relations",
    stage: "relations-stage",
    grid: true,
    draw: () => <ProfileRelations results={RESULTS} tableId={7} tableName="orders" />,
  },
  {
    name: "the joint cloud",
    kind: "joint",
    id: "joint",
    stage: "joint-stage",
    grid: false,
    draw: () => <ProfileJointCloud results={RESULTS} tableId={7} tableName="orders" />,
  },
  {
    name: "the drift strata",
    kind: "drift",
    id: "strata",
    stage: "strata-stage",
    grid: false,
    draw: () => (
      <ProfileDriftStrata
        tableId={7}
        role="analyst"
        runs={[{ run_id: "r1", run_time: "2026-10-01T00:00:00Z", status: "succeeded" }] as never}
        selectedRunId="r1"
        results={RESULTS}
        tableName="orders"
      />
    ),
  },
  {
    name: "the ontology graph",
    kind: "graph",
    id: "x",
    stage: "x-stage",
    grid: true,
    draw: () => (
      <GraphExplorer
        data={GRAPH}
        scope={ONTOLOGY_SCOPE}
        preset="ontology"
        depth={2}
        settingsKey={{ kind: "test", subject: null }}
        exportName="test"
        testId="x"
      />
    ),
  },
];

const scene = (kind: string) => {
  const found = made.scenes.filter((s) => s.kind === kind);
  return found[found.length - 1].calls;
};

/** The one in-place copy of the view's stage (an expanded copy is not open). */
const stageOf = (v: View) => screen.getByTestId(v.stage);
const stackOf = (v: View) =>
  within(stageOf(v)).getByRole("toolbar", { name: "Canvas controls", hidden: true });

beforeEach(() => {
  made.scenes.length = 0;
  api.fetchProfileRun.mockReset();
  window.localStorage.clear();
});

describe.each(VIEWS)("$name's view controls", (v) => {
  it("sit in one stack in its canvas: Reset view, Rotate, Grid, Full screen, Download", async () => {
    render(v.draw());
    await waitFor(() => expect(made.scenes.some((s) => s.kind === v.kind)).toBe(true));
    const stack = stackOf(v);
    expect(stack).toHaveClass("pg-controls");
    const names = within(stack)
      .getAllByRole("button", { hidden: true })
      .map((b) => b.getAttribute("aria-label"));
    expect(names).toEqual([
      "Reset view",
      "Rotate",
      ...(v.grid ? ["Grid"] : []),
      "Full screen",
      "Download",
    ]);
  });

  it("leave no Rotate, Grid or Reset view in a toolbar", async () => {
    const { container } = render(v.draw());
    await waitFor(() => expect(made.scenes.some((s) => s.kind === v.kind)).toBe(true));
    const toolbars = [...container.querySelectorAll<HTMLElement>(".p3d-toolbar")];
    expect(toolbars.length).toBeGreaterThan(0);
    for (const bar of toolbars)
      for (const name of ["Rotate", "Grid", "Reset view"]) {
        expect(within(bar).queryByRole("button", { name, hidden: true })).toBeNull();
        expect(within(bar).queryByRole("checkbox", { name, hidden: true })).toBeNull();
      }
  });

  it("turn Rotate on and off, showing its state, and keep it as the pill did", async () => {
    const view = render(v.draw());
    await waitFor(() => expect(scene(v.kind).setRotate).toHaveBeenCalled());
    const rotate = within(stackOf(v)).getByTestId(`${v.id}-rotate`);
    // The view's own default, whichever it is.
    const first = rotate.getAttribute("aria-pressed") === "true";
    expect(scene(v.kind).setRotate).toHaveBeenLastCalledWith(first);
    fireEvent.click(rotate);
    expect(rotate).toHaveAttribute("aria-pressed", String(!first));
    await waitFor(() => expect(scene(v.kind).setRotate).toHaveBeenLastCalledWith(!first));
    view.unmount();
    render(v.draw());
    // The view draws its stage once what it shows is read.
    await waitFor(() =>
      expect(within(stackOf(v)).getByTestId(`${v.id}-rotate`)).toHaveAttribute(
        "aria-pressed",
        String(!first),
      ),
    );
    // Set Defaults brings it back.
    fireEvent.click(screen.getByTestId(`${v.id}-reset-settings`));
    await waitFor(() =>
      expect(within(stackOf(v)).getByTestId(`${v.id}-rotate`)).toHaveAttribute(
        "aria-pressed",
        String(first),
      ),
    );
  });

  it("reset the camera from the stack", async () => {
    render(v.draw());
    await waitFor(() => expect(made.scenes.some((s) => s.kind === v.kind)).toBe(true));
    fireEvent.click(within(stackOf(v)).getByTestId(`${v.id}-reset`));
    await waitFor(() => expect(scene(v.kind).resetView).toHaveBeenCalled());
  });

  it("put the stage itself full screen, so the stack is still there", async () => {
    render(v.draw());
    await waitFor(() => expect(made.scenes.some((s) => s.kind === v.kind)).toBe(true));
    const stage = stageOf(v);
    const asked = vi.fn(() => Promise.resolve());
    stage.requestFullscreen = asked;
    fireEvent.click(within(stackOf(v)).getByTestId(`${v.stage}-fullscreen`));
    expect(asked).toHaveBeenCalled();
    expect(stage.contains(stackOf(v))).toBe(true);
  });

  it("place the legend clear of the stack", async () => {
    render(v.draw());
    await waitFor(() => expect(made.scenes.some((s) => s.kind === v.kind)).toBe(true));
    const legend = stageOf(v).querySelector(".p3d-legend");
    expect(legend).toHaveClass("p3d-legend-beside-controls");
  });
});

describe.each(VIEWS.filter((v) => v.grid))("$name's Grid toggle", (v) => {
  it("shows its state, sets the scene's floor grid and keeps it as the pill did", async () => {
    const view = render(v.draw());
    await waitFor(() => expect(scene(v.kind).setGrid).toHaveBeenLastCalledWith(true));
    const grid = within(stackOf(v)).getByTestId(`${v.id}-grid`);
    expect(grid).toHaveAttribute("aria-pressed", "true");
    fireEvent.click(grid);
    expect(grid).toHaveAttribute("aria-pressed", "false");
    await waitFor(() => expect(scene(v.kind).setGrid).toHaveBeenLastCalledWith(false));
    view.unmount();
    render(v.draw());
    await waitFor(() =>
      expect(within(stackOf(v)).getByTestId(`${v.id}-grid`)).toHaveAttribute(
        "aria-pressed",
        "false",
      ),
    );
    fireEvent.click(screen.getByTestId(`${v.id}-reset-settings`));
    await waitFor(() =>
      expect(within(stackOf(v)).getByTestId(`${v.id}-grid`)).toHaveAttribute(
        "aria-pressed",
        "true",
      ),
    );
  });
});
