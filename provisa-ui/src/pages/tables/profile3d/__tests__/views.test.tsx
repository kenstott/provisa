// Copyright (c) 2026 Kenneth Stott
// Canary: ceb8df7c-562f-4640-a8ad-d1d1d2e5287a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2124: the relationships map, joint cloud and drift strata tabs. jsdom has no WebGL, so each
// three.js scene is replaced by a recorder.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../../test-utils/render";

type Calls = Record<string, ReturnType<typeof vi.fn>>;
const made = vi.hoisted(() => ({ scenes: [] as { kind: string; calls: Calls }[] }));
const api = vi.hoisted(() => ({ fetchProfileRun: vi.fn() }));
vi.mock("../../../../api/profiler", () => api);

function recorder(kind: string, methods: string[]) {
  return class {
    constructor() {
      const calls: Calls = {};
      for (const m of methods) calls[m] = vi.fn();
      Object.assign(this, calls);
      made.scenes.push({ kind, calls });
    }
  };
}
const common = ["setTheme", "setRotate", "setGrid", "dispose", "resetView", "setHover"];
vi.mock("../relations/relationsScene", () => ({
  RelationsScene: recorder("relations", [...common, "setData", "setEdges", "setSelected", "focus"]),
}));
vi.mock("../joint/jointScene", () => ({
  JointScene: recorder("joint", [...common, "setData", "setColor"]),
}));
vi.mock("../drift/driftScene", () => ({
  DriftScene: recorder("drift", [...common, "setData", "setSelected"]),
}));

import { ProfileRelations } from "../relations/ProfileRelations";
import { ProfileJointCloud } from "../joint/ProfileJointCloud";
import { ProfileDriftStrata } from "../drift/ProfileDriftStrata";

const last = (kind: string) => {
  const found = made.scenes.filter((s) => s.kind === kind);
  return found[found.length - 1].calls;
};

beforeEach(() => {
  made.scenes.length = 0;
  api.fetchProfileRun.mockReset();
});

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
    {
      column_name: "plan",
      other_column: "amount",
      measure: "correlation_ratio",
      value: 0.2,
      rows: 100,
    },
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
    {
      column_name: "amount",
      parent_1: "plan",
      parent_2: null,
      target_bucket: 9,
      target_value: null,
      parent_1_value: "pro",
      parent_1_bucket: null,
      row_count: 40,
    },
  ],
};

describe("ProfileRelations", () => {
  it("opens on the busiest column and filters edges by strength", async () => {
    render(<ProfileRelations tableId={7} results={RESULTS} tableName="orders" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(screen.getByTestId("relations-detail")).toHaveTextContent("amount");
    const [, edges] = last("relations").setData.mock.calls[0];
    expect(edges.map((e: { id: string }) => e.id).sort()).toEqual([
      "dependence:plan>amount",
      "spearman:age|amount",
    ]);
    fireEvent.click(screen.getByRole("radio", { name: "≥ 0.1" }));
    await waitFor(() => expect(last("relations").setEdges).toHaveBeenCalled());
    expect(last("relations").setEdges.mock.calls.at(-1)?.[0]).toHaveLength(3);
  });

  it("selects the other end of a relationship and zooms to it", async () => {
    render(<ProfileRelations tableId={7} results={RESULTS} tableName="orders" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    fireEvent.click(screen.getByTestId("relations-link-spearman:age|amount"));
    expect(screen.getByTestId("relations-detail")).toHaveTextContent("age");
    fireEvent.click(screen.getByTestId("relations-focus"));
    await waitFor(() => expect(last("relations").focus).toHaveBeenCalledWith("age"));
  });

  it("expands into a modal with its own scene", async () => {
    render(<ProfileRelations tableId={7} results={RESULTS} tableName="orders" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    fireEvent.click(screen.getByTestId("relations-expand"));
    expect(await screen.findByTestId("relations-expanded")).toBeInTheDocument();
    await waitFor(() => expect(made.scenes).toHaveLength(2));
  });

  it("says so when the run found no relationships", () => {
    render(
      <ProfileRelations tableId={7} results={{ columns: RESULTS.columns }} tableName="orders" />,
    );
    expect(screen.getByTestId("relations-empty")).toBeInTheDocument();
  });
});

describe("ProfileJointCloud", () => {
  it("draws the first column's joint counts and switches colouring", async () => {
    render(<ProfileJointCloud tableId={7} results={RESULTS} tableName="orders" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    const [model] = last("joint").setData.mock.calls[0];
    expect(model.target).toBe("amount");
    expect(screen.getByTestId("joint-detail")).toHaveTextContent("amount");
    fireEvent.click(screen.getByRole("radio", { name: "Rows" }));
    await waitFor(() => expect(last("joint").setColor).toHaveBeenLastCalledWith("rows"));
  });

  it("lights a listed combination in the cloud", async () => {
    render(<ProfileJointCloud tableId={7} results={RESULTS} tableName="orders" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    const rows = screen.getAllByTestId(/^joint-cell-/);
    fireEvent.mouseEnter(rows[0]);
    await waitFor(() =>
      expect(last("joint").setHover).toHaveBeenLastCalledWith({ cell: expect.any(Number) }),
    );
  });

  it("says so when the run holds no joint counts", () => {
    render(<ProfileJointCloud tableId={7} results={{ columns: [] }} tableName="orders" />);
    expect(screen.getByTestId("joint-empty")).toBeInTheDocument();
  });
});

describe("ProfileDriftStrata", () => {
  const run = (id: string, day: number, status = "succeeded") => ({
    run_id: id,
    run_time: `2026-10-0${day}T00:00:00Z`,
    status,
  });

  it("loads the runs up to the selected one and stacks them oldest first", async () => {
    const older = {
      ...RESULTS,
      quantiles: RESULTS.quantiles.map((q) => ({ ...q, value: q.value + 50 })),
    };
    api.fetchProfileRun.mockResolvedValue(older);
    const runs = [run("r3", 3), run("r2", 2), run("bad", 1, "failed"), run("r4", 4)];
    render(
      <ProfileDriftStrata
        tableId={7}
        role="analyst"
        runs={runs as never}
        selectedRunId="r3"
        results={RESULTS}
        tableName="orders"
      />,
    );
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(api.fetchProfileRun).toHaveBeenCalledTimes(1);
    expect(api.fetchProfileRun).toHaveBeenCalledWith(7, "r2", "analyst");
    const [model] = last("drift").setData.mock.calls[0];
    expect(model.column).toBe("amount");
    expect(model.runs.map((r: { runId: string }) => r.runId)).toEqual(["r2", "r3"]);
    expect(last("drift").setSelected).toHaveBeenLastCalledWith(1);
  });

  it("shows a run that failed to load as an error", async () => {
    api.fetchProfileRun.mockRejectedValue(new Error("boom"));
    render(
      <ProfileDriftStrata
        tableId={7}
        role="analyst"
        runs={[run("r1", 1), run("r2", 2)] as never}
        selectedRunId="r2"
        results={RESULTS}
        tableName="orders"
      />,
    );
    expect(await screen.findByTestId("strata-error")).toHaveTextContent("boom");
  });
});
