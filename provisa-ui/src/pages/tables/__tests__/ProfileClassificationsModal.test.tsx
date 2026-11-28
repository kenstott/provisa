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
  fetchClassificationKinds: vi.fn(),
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
  api.fetchClassificationKinds.mockResolvedValue({
    kinds: [{ id: "custom.employee_id", label: "Employee ID", custom: true }],
    checks: [],
    masks: [],
  });
});

describe("ProfileClassificationsModal", () => {
  it("names a custom kind by the org's label, and drops one the org has deleted", async () => {
    // REQ-2147 (amended 2026-11-25): a custom kind is suggested exactly as a shipped one.
    const badge = {
      ...SSN,
      column_name: "badge",
      physical_column: "badge",
      kind: "custom.employee_id",
      sensitive: false,
    };
    const gone = { ...badge, column_name: "old", physical_column: "old", kind: "custom.gone" };
    api.fetchProfileRun.mockResolvedValue({ runs: [], classifications: [badge, gone] });
    render(<ProfileClassificationsModal tableId={7} tableName="customers" onClose={vi.fn()} />);
    expect(
      await screen.findByText(
        "customers.badge looks like Employee ID: 97% of sampled values pass its test.",
      ),
    ).toBeInTheDocument();
    expect(screen.getByText("Employee ID")).toBeInTheDocument();
    expect(screen.queryByText("old")).toBeNull();
  });

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

  // A column whose sample held too few non-null values is said so in words, apart from the
  // suggestions: it is not a column that matched no kind.
  it("says which columns had too few values to classify, apart from those matching no kind", async () => {
    api.fetchProfileRun.mockResolvedValue({
      runs: [],
      classifications: [],
      columns: [
        { column_name: "note", classification: "too_few_values", classification_values: 3 },
        { column_name: "code", classification: "no_kind", classification_values: 500 },
        { column_name: "opened", classification: null, classification_values: null },
      ],
    });
    render(<ProfileClassificationsModal tableId={7} tableName="customers" onClose={vi.fn()} />);
    expect(await screen.findByTestId("classification-too-few-note")).toHaveTextContent(
      "note: 3 values in the sample, too few to classify.",
    );
    expect(screen.getByText("Too few values to classify")).toBeInTheDocument();
    expect(screen.queryByTestId("classification-too-few-code")).toBeNull();
    expect(screen.queryByTestId("classification-too-few-opened")).toBeNull();
    expect(screen.getByTestId("classifications-none")).toBeInTheDocument();
  });

  it("shows no too-few section when every tested column had enough values", async () => {
    api.fetchProfileRun.mockResolvedValue({
      runs: [],
      classifications: [SSN],
      columns: [{ column_name: "tax_id", classification: "matched", classification_values: 500 }],
    });
    render(<ProfileClassificationsModal tableId={7} tableName="customers" onClose={vi.fn()} />);
    expect(await screen.findByTestId("classification-suggestion-0")).toBeInTheDocument();
    expect(screen.queryByTestId("classifications-too-few")).toBeNull();
  });

  it("says too few values for a masked or pii column whose row shows its shape only", async () => {
    const shapeOnly = {
      run_id: "r1",
      data_type: "varchar",
      null_share: 0,
      distinct_ratio: 1,
      length_min: 1,
      length_max: 9,
      min_value: null,
      max_value: null,
      mean: null,
      row_count: null,
    };
    api.fetchProfileRun.mockResolvedValue({
      runs: [],
      classifications: [],
      columns: [
        {
          ...shapeOnly,
          column_name: "email",
          classification: "too_few_values",
          classification_values: 3,
        },
        {
          ...shapeOnly,
          column_name: "ssn",
          classification: "too_few_values",
          classification_values: 0,
        },
      ],
    });
    render(<ProfileClassificationsModal tableId={7} tableName="customers" onClose={vi.fn()} />);
    expect(await screen.findByTestId("classification-too-few-email")).toHaveTextContent(
      "email: 3 values in the sample, too few to classify.",
    );
    expect(screen.getByTestId("classification-too-few-ssn")).toBeInTheDocument();
  });
});
