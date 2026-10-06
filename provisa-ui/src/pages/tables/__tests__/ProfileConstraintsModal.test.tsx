// Copyright (c) 2026 Kenneth Stott
// Canary: c5e18f37-0a9b-4d26-b843-f2d7a6e1c904
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1934 PROPOSED CONSTRAINTS: the latest run's proposals are accepted, edited or dismissed in the
// table editor; an accepted one may be exported to a checker, refused by name where none scans the
// table. The runs view creates the drift and expectation checks the same way.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";

const api = vi.hoisted(() => ({
  fetchConstraints: vi.fn(),
  fetchProfileRuns: vi.fn(),
  fetchProfileRun: vi.fn(),
  decideConstraint: vi.fn(),
  forgetConstraint: vi.fn(),
  exportConstraint: vi.fn(),
  fetchProfileChecks: vi.fn(),
  createDriftCheck: vi.fn(),
  createExpectationCheck: vi.fn(),
}));
vi.mock("../../../api/profiler", () => api);
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ role: { id: "steward" } }) }));

import { ProfileConstraintsModal } from "../ProfileConstraintsModal";
import { ProfileChecksBar } from "../ProfileChecksBar";

const PROPOSAL = {
  run_id: "r1",
  constraint: "not_null",
  column_name: "id",
  other_column: null,
  definition: "{}",
  evidence: "0 of 10 rows profiled are null",
  share: 1,
  sampled: true,
};
const ACCEPTED = {
  id: "c1",
  kind: "unique",
  column_name: "id",
  other_column: null,
  definition: "{}",
  evidence: "e",
  share: 1,
  sampled: false,
  status: "accepted",
  run_id: "r0",
};

beforeEach(() => {
  Object.values(api).forEach((fn) => fn.mockReset());
  api.fetchProfileRuns.mockResolvedValue([{ run_id: "r1", status: "succeeded" }]);
  api.fetchProfileRun.mockResolvedValue({ runs: [], constraints: [PROPOSAL] });
  api.fetchConstraints.mockResolvedValue({ decisions: [ACCEPTED], checkers: [] });
  api.decideConstraint.mockResolvedValue({ id: "c2" });
});

describe("ProfileConstraintsModal", () => {
  it("accepts a proposal as it stands, with the run and sample it rests on", async () => {
    render(<ProfileConstraintsModal tableId={7} tableName="orders" onClose={vi.fn()} />);
    fireEvent.click(await screen.findByTestId("constraint-accept-0"));
    await waitFor(() =>
      expect(api.decideConstraint).toHaveBeenCalledWith(7, {
        kind: "not_null",
        column: "id",
        otherColumn: null,
        definition: {},
        evidence: "0 of 10 rows profiled are null",
        share: 1,
        sampled: true,
        status: "accepted",
        runId: "r1",
      }),
    );
    expect(api.fetchProfileRun).toHaveBeenCalledWith(7, "r1", "steward");
  });

  it("accepts an edited definition and dismisses another", async () => {
    render(<ProfileConstraintsModal tableId={7} tableName="orders" onClose={vi.fn()} />);
    fireEvent.click(await screen.findByTestId("constraint-editbtn-0"));
    fireEvent.change(screen.getByTestId("constraint-edit-0"), {
      target: { value: '{"note": "edited"}' },
    });
    fireEvent.click(screen.getByTestId("constraint-save-0"));
    await waitFor(() =>
      expect(api.decideConstraint).toHaveBeenCalledWith(
        7,
        expect.objectContaining({ definition: { note: "edited" }, status: "accepted" }),
      ),
    );
  });

  it("shows the refusal when no checker scans the table", async () => {
    api.exportConstraint.mockRejectedValue(
      new Error("no Soda or Great Expectations checker source scans table 'orders'"),
    );
    render(<ProfileConstraintsModal tableId={7} tableName="orders" onClose={vi.fn()} />);
    fireEvent.click(await screen.findByTestId("constraint-export-c1"));
    expect(await screen.findByTestId("constraints-notice")).toHaveTextContent(
      "no Soda or Great Expectations checker source scans table 'orders'",
    );
    expect(api.exportConstraint).toHaveBeenCalledWith(7, "c1", null);
  });
});

describe("ProfileChecksBar", () => {
  it("creates the drift check and reports which contract took it", async () => {
    api.fetchProfileChecks.mockResolvedValue({
      driftTableRegistered: true,
      checkers: [],
      expectationTables: [],
    });
    api.createDriftCheck.mockResolvedValue({
      checkerTable: { id: 3, tableName: "drift_checks", sourceId: "soda", checker: "soda" },
      added: true,
    });
    render(<ProfileChecksBar tableId={7} />);
    fireEvent.click(screen.getByTestId("create-drift-check"));
    expect(await screen.findByTestId("profile-checks-notice")).toHaveTextContent("drift_checks");
    expect(api.createDriftCheck).toHaveBeenCalledWith(7, null);
  });
});
