// Copyright (c) 2026 Kenneth Stott
// Canary: 0addd96f-c0ff-4e00-bfd2-f5ebb4747144
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2147: the latest run's classification suggestions are accepted or rejected by the steward;
// accepting says what it did -- the term the column now means (or that a key column keeps its Class
// term), pii, and the mask the kind offers. A decided suggestion shows its decision, and a decision
// can be withdrawn.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";

const api = vi.hoisted(() => ({
  fetchClassifications: vi.fn(),
  fetchProfileRuns: vi.fn(),
  fetchProfileRun: vi.fn(),
  decideClassification: vi.fn(),
  forgetClassification: vi.fn(),
}));
vi.mock("../../../api/profiler", () => api);
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "steward" }) }));

import { ProfileClassificationsModal } from "../ProfileClassificationsModal";

const SSN = {
  run_id: "r1",
  column_name: "tax_id",
  physical_column: "tax_id",
  kind: "us_ssn",
  sensitive: true,
  values_tested: 100,
  values_passed: 97,
  share: 0.97,
  threshold: 0.9,
  evidence: "customers.tax_id looks like a US social security number: 97% of sampled values pass",
  status: "suggested",
};
const CITY = {
  ...SSN,
  column_name: "city",
  physical_column: "city",
  kind: "city",
  sensitive: false,
  share: 0.99,
};

beforeEach(() => {
  Object.values(api).forEach((fn) => fn.mockReset());
  api.fetchProfileRuns.mockResolvedValue([{ run_id: "r1", status: "succeeded" }]);
  api.fetchProfileRun.mockResolvedValue({ runs: [], classifications: [SSN, CITY] });
  api.fetchClassifications.mockResolvedValue({ decisions: [] });
});

describe("ProfileClassificationsModal", () => {
  it("says what each suggestion found, in words", async () => {
    render(<ProfileClassificationsModal tableId={7} tableName="customers" onClose={vi.fn()} />);
    expect(
      await screen.findByText(
        "customers.tax_id looks like a US social security number: 97% of sampled values pass its test.",
      ),
    ).toBeInTheDocument();
    expect(screen.getByText("World city")).toBeInTheDocument();
  });

  it("accepts a sensitive suggestion and names the term, pii and the mask offered", async () => {
    api.decideClassification.mockResolvedValue({
      status: "accepted",
      term: "us social security number",
      termNote: null,
      pii: true,
      maskPreset: "ssn_last4",
    });
    render(<ProfileClassificationsModal tableId={7} tableName="customers" onClose={vi.fn()} />);
    fireEvent.click(await screen.findByTestId("classification-accept-0"));
    await waitFor(() =>
      expect(api.decideClassification).toHaveBeenCalledWith(7, {
        column: "tax_id",
        kind: "us_ssn",
        status: "accepted",
        evidence: SSN.evidence,
        runId: "r1",
      }),
    );
    const notice = await screen.findByTestId("classifications-notice");
    expect(notice).toHaveTextContent("The column now means us social security number.");
    expect(notice).toHaveTextContent("It is marked PII.");
    expect(notice).toHaveTextContent("Suggested mask: SSN, last 4.");
  });

  it("says a key column keeps its Class term", async () => {
    api.decideClassification.mockResolvedValue({
      status: "accepted",
      term: null,
      termNote: "a key column's meaning is its Class term",
      pii: true,
      maskPreset: "ssn_last4",
    });
    render(<ProfileClassificationsModal tableId={7} tableName="customers" onClose={vi.fn()} />);
    fireEvent.click(await screen.findByTestId("classification-accept-0"));
    expect(await screen.findByTestId("classifications-notice")).toHaveTextContent(
      "The column keeps its Class term",
    );
  });

  it("rejects a suggestion", async () => {
    api.decideClassification.mockResolvedValue({ status: "rejected" });
    render(<ProfileClassificationsModal tableId={7} tableName="customers" onClose={vi.fn()} />);
    fireEvent.click(await screen.findByTestId("classification-reject-1"));
    await waitFor(() =>
      expect(api.decideClassification).toHaveBeenCalledWith(
        7,
        expect.objectContaining({ column: "city", kind: "city", status: "rejected" }),
      ),
    );
  });

  it("shows a decided suggestion's decision and withdraws it", async () => {
    api.fetchClassifications.mockResolvedValue({
      decisions: [
        {
          table_id: 7,
          physical_column: "tax_id",
          kind: "us_ssn",
          status: "rejected",
          evidence: SSN.evidence,
          run_id: "r0",
          decided_by: "sam",
          decided_at: "2026-10-24T00:00:00Z",
        },
      ],
    });
    api.forgetClassification.mockResolvedValue({ ok: true });
    render(<ProfileClassificationsModal tableId={7} tableName="customers" onClose={vi.fn()} />);
    expect(await screen.findByTestId("classification-status-0")).toHaveTextContent("Rejected");
    expect(screen.queryByTestId("classification-accept-0")).toBeNull();
    fireEvent.click(screen.getByTestId("classification-withdraw-tax_id-us_ssn"));
    await waitFor(() =>
      expect(api.forgetClassification).toHaveBeenCalledWith(7, "tax_id", "us_ssn"),
    );
  });

  it("says when the latest run suggests nothing", async () => {
    api.fetchProfileRun.mockResolvedValue({ runs: [], classifications: [] });
    render(<ProfileClassificationsModal tableId={7} tableName="customers" onClose={vi.fn()} />);
    expect(await screen.findByTestId("classifications-none")).toBeInTheDocument();
  });
});
