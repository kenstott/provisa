// Copyright (c) 2026 Kenneth Stott
// Canary: 5d1c8e27-93a4-4b6f-a0e2-7f3b91c4d6a8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413 (amended 2026-11-18): every column of a run's results has the standard list's column
// filter: a number filter on the scores, a text filter with its checklist on the rest. The Score
// filter still opens at >= 10%; the others start cleared, add a chip each, and narrow the rows the
// Score filter shows. Accept and the select-all act only on the rows every filter shows.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor, within } from "@testing-library/react";
import type { RunProposal } from "../../api/admin";
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

/** Three pairs over 10% (one of them one-to-one, one Jev scored), and one under it. */
const ROWS: RunProposal[] = [
  proposal({ id: 701, source_column: "customer_id", confidence: 0.95, scored: 0.9 }),
  proposal({ id: 702, source_column: "agent_id", confidence: 0.6, reasoning: "Agents serve" }),
  proposal({ id: 703, source_column: "agent_code", confidence: 0.3, cardinality: "one-to-one" }),
  proposal({ id: 704, source_column: "agent_memo", confidence: 0.05, reason: "below_threshold" }),
];

async function results() {
  runMock.mockResolvedValue(runOf({ status: "done", report: REPORT }));
  proposalsMock.mockResolvedValue({
    proposals: ROWS,
    threshold: 0,
    kept_at: 0.7,
    score_filter: { op: "ge", a: "10" },
  });
  await openDialog();
  fireEvent.click(runButton());
  await screen.findByTestId("proposal-701", {}, SLOW);
}

const listed = () =>
  within(screen.getByTestId("discover-proposals"))
    .getAllByTestId(/^proposal-\d+$/)
    .map((row) => row.getAttribute("data-testid"));

const COLUMNS = [
  "Score",
  "Confidence",
  "Jev",
  "Source",
  "Target",
  "Cardinality",
  "Measured",
  "Kind",
  "Reasoning",
  "Note",
];

async function textFilter(col: string, value: string) {
  fireEvent.click(screen.getByTestId(`col-filter-${col}-btn`));
  fireEvent.change(await screen.findByTestId(`col-filter-${col}-a`), { target: { value } });
  fireEvent.click(screen.getByTestId(`col-filter-${col}-apply`));
}

describe("Discovery results: every column has a filter", () => {
  it("gives every column the list's filter, with only the Score filter in force", async () => {
    await results();
    const head = within(screen.getByTestId("discover-proposals")).getAllByRole("columnheader");
    for (const col of COLUMNS) {
      const btn = screen.getByTestId(`col-filter-${col}-btn`);
      expect(head.some((th) => th.contains(btn))).toBe(true);
      expect(btn.getAttribute("data-active")).toBe(col === "Score" ? "true" : null);
    }
    // The default Score >= 10% filter still applies.
    expect(listed()).toEqual(["proposal-701", "proposal-702", "proposal-703"]);
  });

  it("a text filter narrows the rows the Score filter shows, with its own chip", async () => {
    await results();
    await textFilter("Source", "agent");
    // 704 holds "agent" too, and the Score filter still hides it.
    expect(listed()).toEqual(["proposal-702", "proposal-703"]);
    expect(screen.getByTestId("col-filter-Source-btn")).toHaveAttribute("data-active", "true");
    expect(screen.getByTestId("discover-filter-chip-source")).toHaveTextContent("Source");
    expect(screen.getByTestId("discover-filter-chip-score")).toHaveTextContent("Score >= 10");
    expect(screen.getByTestId("discover-filter-hidden")).toHaveTextContent(
      "2 rows are hidden by the filter.",
    );
    fireEvent.click(screen.getByTestId("discover-filter-chip-remove-source"));
    expect(listed()).toEqual(["proposal-701", "proposal-702", "proposal-703"]);
    expect(screen.queryByTestId("discover-filter-chip-source")).toBeNull();
  });

  it("a categorical column filters by its checklist of the values held", async () => {
    await results();
    fireEvent.click(screen.getByTestId("col-filter-Cardinality-btn"));
    fireEvent.click(await screen.findByTestId("col-filter-Cardinality-op"));
    fireEvent.click(await screen.findByRole("option", { name: "is one of", hidden: true }));
    fireEvent.click(await screen.findByTestId("col-filter-Cardinality-value-one-to-one"));
    fireEvent.click(screen.getByTestId("col-filter-Cardinality-apply"));
    expect(listed()).toEqual(["proposal-703"]);
  });

  it("a number filter on Confidence and one on Reasoning apply together", async () => {
    await results();
    fireEvent.click(screen.getByTestId("col-filter-Confidence-btn"));
    fireEvent.change(await screen.findByTestId("col-filter-Confidence-a"), {
      target: { value: "50" },
    });
    fireEvent.click(screen.getByTestId("col-filter-Confidence-apply"));
    expect(listed()).toEqual(["proposal-701", "proposal-702"]);
    await textFilter("Reasoning", "Agents");
    expect(listed()).toEqual(["proposal-702"]);
  });

  it("Accept and the select-all act only on the rows every filter shows", async () => {
    await results();
    await textFilter("Source", "agent");
    // Tick from 0% ticks every row; only the listed are accepted.
    expect(applyButton()).toHaveTextContent("Accept 2");
    const all = within(screen.getByTestId("discover-proposals")).getByTestId("list-select-all");
    fireEvent.click(all);
    expect(applyButton()).toHaveTextContent("Apply");
    fireEvent.click(all);
    expect(acceptBox(702).checked).toBe(true);
    applyMock.mockResolvedValue([]);
    fireEvent.click(applyButton());
    await waitFor(() =>
      expect(applyMock).toHaveBeenCalledWith(7, {
        accept: [{ id: 702 }, { id: 703 }],
        not_again: [],
      }),
    );
  });
});
