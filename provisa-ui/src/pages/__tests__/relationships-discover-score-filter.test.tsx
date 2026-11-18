// Copyright (c) 2026 Kenneth Stott
// Canary: 2bb967ae-fbd7-41dd-9b45-9c5fd1f0e8e4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413 (amended 2026-11-18): a run's results are the standard list with a filter on Score.
// Score >= 10% is applied when the results open and shown like any other filter, so it can be
// changed or cleared; the value a person sets is kept for their next runs, as Tick from is. A line
// says how many rows the filter hides, and Accept and the select-all act only on rows it shows.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor, within } from "@testing-library/react";
import type { RunProposal, ScoreFilter } from "../../api/admin";
import en from "../../i18n/locales/en/proposalsTable.json";
import enResult from "../../i18n/locales/en/discoverResult.json";
import {
  REPORT,
  SLOW,
  acceptBox,
  applyButton,
  applyMock,
  openDialog,
  proposal,
  proposalsMock,
  resetDiscoveryMocks,
  runButton,
  runMock,
  runOf,
  saveScoreFilterMock,
} from "./discoverDialog.fixtures";

vi.mock("../../hooks/useAdminQueries", async () =>
  (await import("./discoverDialog.state")).adminQueries(),
);
vi.mock("../../context/AuthContext", async () => {
  const { AUTH } = await import("./discoverDialog.state");
  return { useAuth: () => AUTH };
});
vi.mock("../../context/DomainFilterContext", async () => {
  const { FILTER } = await import("./discoverDialog.state");
  return { useDomainFilter: () => FILTER };
});
vi.mock("../../api/admin", async (importOriginal) =>
  (await import("./discoverDialog.state")).adminApi(
    await importOriginal<typeof import("../../api/admin")>(),
  ),
);
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../components/TagControl", () => ({ TagControl: () => null }));

beforeEach(resetDiscoveryMocks);

const DEFAULT_FILTER: ScoreFilter = { op: "ge", a: "10" };

/** A strong pair, a middling one, and two scored under 10% by a run that compared every pair. */
const ROWS: RunProposal[] = [
  proposal({ id: 601, source_column: "customer_id", confidence: 0.95 }),
  proposal({ id: 602, source_column: "agent_id", confidence: 0.4, reason: "below_threshold" }),
  proposal({ id: 603, source_column: "note", confidence: 0.05, reason: "below_threshold" }),
  proposal({ id: 604, source_column: "memo", confidence: 0, reason: "below_threshold" }),
];

async function results(filter: ScoreFilter | null, threshold = 0.7) {
  runMock.mockResolvedValue(runOf({ status: "done", report: REPORT }));
  proposalsMock.mockResolvedValue({
    proposals: ROWS,
    threshold,
    kept_at: 0.7,
    score_filter: filter,
  });
  await openDialog();
  fireEvent.click(runButton());
  await screen.findByTestId("proposal-601", {}, SLOW);
}

const listed = () =>
  within(screen.getByTestId("discover-proposals"))
    .getAllByTestId(/^proposal-\d+$/)
    .map((row) => row.getAttribute("data-testid"));

describe("Discovery results: the Score filter", () => {
  it("opens with Score >= 10% applied, shown as a filter, and says how many rows it hides", async () => {
    await results(DEFAULT_FILTER);
    expect(listed()).toEqual(["proposal-601", "proposal-602"]);
    expect(screen.getByTestId("discover-filter-chip-score")).toHaveTextContent("Score >= 10");
    expect(screen.getByTestId("discover-filter-hidden")).toHaveTextContent(
      "2 rows are hidden by the filter.",
    );
    // The column's own filter button shows it is in force.
    expect(screen.getByTestId("col-filter-Score-btn")).toHaveAttribute("data-active", "true");
  });

  it("cleared, every row shows, the line goes, and the clearing is kept", async () => {
    await results(DEFAULT_FILTER);
    fireEvent.click(screen.getByTestId("discover-filter-chip-remove-score"));
    expect(listed()).toEqual(["proposal-601", "proposal-602", "proposal-603", "proposal-604"]);
    expect(screen.queryByTestId("discover-filter-hidden")).toBeNull();
    expect(screen.queryByTestId("discover-filter-chip-score")).toBeNull();
    await waitFor(() => expect(saveScoreFilterMock).toHaveBeenCalledWith(null));
  });

  it("a filter set in the column's menu applies and is kept for the next runs", async () => {
    await results(DEFAULT_FILTER);
    fireEvent.click(screen.getByTestId("col-filter-Score-btn"));
    const value = await screen.findByTestId("col-filter-Score-a");
    fireEvent.change(value, { target: { value: "50" } });
    fireEvent.click(screen.getByTestId("col-filter-Score-apply"));
    expect(listed()).toEqual(["proposal-601"]);
    expect(screen.getByTestId("discover-filter-hidden")).toHaveTextContent(
      "3 rows are hidden by the filter.",
    );
    await waitFor(() => expect(saveScoreFilterMock).toHaveBeenCalledWith({ op: "ge", a: "50" }));
  });

  it("opens with the filter the person kept, and with none where they cleared it", async () => {
    await results({ op: "ge", a: "50" });
    expect(listed()).toEqual(["proposal-601"]);
    expect(screen.getByTestId("discover-filter-chip-score")).toHaveTextContent("Score >= 50");
  });

  it("opens with every row where the person cleared the filter", async () => {
    await results(null);
    expect(listed()).toHaveLength(4);
    expect(screen.queryByTestId("discover-filter-hidden")).toBeNull();
  });

  it("Accept and the select-all act only on the rows the filter shows", async () => {
    // Tick from 0% ticks every row, the hidden ones too; only the shown are accepted.
    await results(DEFAULT_FILTER, 0);
    expect(acceptBox(601).checked).toBe(true);
    expect(acceptBox(602).checked).toBe(true);
    expect(applyButton()).toHaveTextContent("Accept 2");
    applyMock.mockResolvedValue([]);
    fireEvent.click(applyButton());
    await waitFor(() =>
      expect(applyMock).toHaveBeenCalledWith(7, {
        accept: [{ id: 601 }, { id: 602 }],
        not_again: [],
      }),
    );
    const all = within(screen.getByTestId("discover-proposals")).getByTestId("list-select-all");
    // Every shown row ticked: the header's box unticks them all, and ticks only them again.
    fireEvent.click(all);
    expect(acceptBox(601).checked).toBe(false);
    expect(applyButton()).toHaveTextContent("Apply");
    fireEvent.click(all);
    expect(applyButton()).toHaveTextContent("Accept 2");
    fireEvent.click(screen.getByTestId("discover-filter-chip-remove-score"));
    // The hidden rows were never ticked by the select-all.
    expect(acceptBox(603).checked).toBe(false);
    expect(acceptBox(604).checked).toBe(false);
  });

  it("names none of the reasons a run no longer has", () => {
    const vanished = [
      "primary_key_to_primary_key",
      "both_repeating",
      "pending",
      "matches_foreign_key",
      "weaker",
      "many_to_many",
    ];
    const reasons = Object.keys(en.proposalsTable.reason);
    const removed = Object.keys(enResult.discoverResult.removed);
    for (const kind of vanished) {
      expect(reasons).not.toContain(kind);
      expect(removed).not.toContain(kind);
    }
  });
});
