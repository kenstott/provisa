// Copyright (c) 2026 Kenneth Stott
// Canary: e1385537-91dd-44d5-b0bd-61862296ec11
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: Polly's navigate to the Data Science area with code and autoRun waits for the run and
// answers with what it printed, its value and its error.

import { describe, it, expect, vi, afterEach } from "vitest";
import { executeClientTool, type ClientToolContext } from "../mcp/clientTools";
import { reportRun, summarise } from "../dataScience/runReports";

type Go = (route: string, options?: { state: Record<string, unknown> }) => void;

function ctx() {
  const go = vi.fn<Go>();
  const c: ClientToolContext = {
    navigate: go as unknown as ClientToolContext["navigate"],
    confirm: vi.fn(),
    runMutation: vi.fn(),
    presentChoice: vi.fn(),
  };
  return { c, go };
}

/** The router state the area was handed. */
const handed = (go: ReturnType<typeof vi.fn<Go>>) =>
  go.mock.calls[0][1]?.state as { code: string; autoRun: boolean; runId: string };

afterEach(() => vi.useRealTimers());

describe("navigate to the Python tab", () => {
  it("hands the area the code under a run id and answers with the run's outcome", async () => {
    const { c, go } = ctx();
    const pending = executeClientTool(
      "navigate",
      { route: "/python", state: { code: "print(1)\ndf", autoRun: true } },
      c,
    );
    expect(go.mock.calls[0][0]).toBe("/python");
    const state = handed(go);
    expect(state).toMatchObject({ code: "print(1)\ndf", autoRun: true });
    reportRun(state.runId, { stdout: "1\n", value: 'columns: ["n"]', error: null });
    const out = await pending;
    expect(out.success).toBe(true);
    expect(out.message).toContain("Ran the code in the Python tab.");
    expect(out.message).toContain("Printed:\n1");
    expect(out.message).toContain('Value:\ncolumns: ["n"]');
  });

  it("answers a failed run as a failure carrying the error", async () => {
    const { c, go } = ctx();
    const pending = executeClientTool(
      "navigate",
      { route: "/python", state: { code: "x", autoRun: true } },
      c,
    );
    const { runId } = handed(go);
    reportRun(runId, { stdout: "", value: "", error: "NameError: name 'x' is not defined" });
    const out = await pending;
    expect(out.success).toBe(false);
    expect(out.message).toContain("Error:\nNameError");
  });

  it("answers after its timeout when no run reports", async () => {
    vi.useFakeTimers();
    const { c } = ctx();
    const pending = executeClientTool(
      "navigate",
      { route: "/python", state: { code: "while True: pass", autoRun: true } },
      c,
    );
    await vi.advanceTimersByTimeAsync(180_000);
    const out = await pending;
    expect(out.success).toBe(false);
    expect(out.message).toContain("No result within 180 s.");
  });

  it("navigates without waiting when there is no code to run", async () => {
    const { c } = ctx();
    const out = await executeClientTool("navigate", { route: "/python" }, c);
    expect(out).toEqual({ success: true, message: "Navigated to /python" });
  });
});

describe("navigate to the Notebook tab", () => {
  it("hands over cells under a run id and answers with the notebook's outcome", async () => {
    const { c, go } = ctx();
    const pending = executeClientTool(
      "navigate",
      {
        route: "/notebook",
        state: { cells: ["%%sql eu\nSELECT 1"], autoRun: true },
      },
      c,
    );
    expect(go.mock.calls[0][0]).toBe("/notebook");
    const state = handed(go) as unknown as { cells: string[]; runId: string };
    expect(state.cells).toEqual(["%%sql eu\nSELECT 1"]);
    reportRun(state.runId, { stdout: "", value: "   n\n0  1", error: null });
    const out = await pending;
    expect(out.message).toContain("Ran the cells in the Notebook tab.");
    expect(out.message).toContain("Value:\n   n\n0  1");
  });
});

describe("summarise", () => {
  it("shows a table as its columns, first rows and row count, and counts figures", () => {
    const rows = Array.from({ length: 30 }, (_, i) => [i]);
    const report = summarise({
      stdout: "",
      value: { kind: "table", columns: ["n"], rows, total: 1000 },
      images: ["a", "b"],
      error: null,
    });
    expect(report.value.split("\n")[0]).toBe('columns: ["n"]');
    expect(report.value).toContain("(1000 rows; first 20 shown)");
    expect(report.value).toContain("(2 figure(s) drawn)");
  });
});
