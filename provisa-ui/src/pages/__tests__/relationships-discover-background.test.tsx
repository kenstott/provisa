// Copyright (c) 2026 Kenneth Stott
// Canary: 2d7f4b19-8e3a-4c61-b5d2-a9e0c6f3b781
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413 (amended 2026-11-16): a discovery run is one collapsible block in the dialog for its
// whole life: held open on its progress with Stop and Run in background while it goes, and its
// summary collapsed to its first line once it has ended. Runs going at once are each a line of
// it. Run in background closes the dialog and the run goes on. The Relationships page shows only
// the block's first line, with Open, and the line goes once the run's suggestions are all decided.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor, within } from "@testing-library/react";
import { render } from "../../test-utils/render";
import { RelationshipsPage } from "../RelationshipsPage";
import {
  REPORT,
  SLOW,
  applyMock,
  dialog,
  discoverButton,
  openDialog,
  proposal,
  proposalsMock,
  resetDiscoveryMocks,
  runButton,
  runMock,
  runOf,
  runsMock,
  stopMock,
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

const GOING = runOf({ requests_total: 4, requests_done: 1, suggestions: 2 });
const ENDED = runOf({ status: "done", requests_total: 4, requests_done: 4, report: REPORT });
const toggle = () => screen.getByTestId("discover-run-toggle");
const pageLine = () => screen.getByTestId("rels-discover-line");

describe("Discovery: the run's block while it goes", () => {
  it("holds the block open on the progress, with Stop and Run in background", async () => {
    runMock.mockResolvedValue(GOING);
    await openDialog();
    fireEvent.click(runButton());
    // The run as started, then as read about a second later.
    await waitFor(
      () =>
        expect(screen.getByTestId("discover-run-line")).toHaveTextContent(
          "Requests: 1 of 4 · Suggestions so far: 2",
        ),
      SLOW,
    );
    expect(toggle()).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByTestId("discover-going-7")).toHaveTextContent(
      "Model: anthropic:claude-haiku-4-5",
    );
    fireEvent.click(screen.getByTestId("discover-stop-7"));
    await waitFor(() => expect(stopMock).toHaveBeenCalledWith(7));
    expect(screen.getByTestId("discover-background")).toBeInTheDocument();
  });

  it("Run in background closes the dialog; the run goes on and the page shows its line", async () => {
    runMock.mockResolvedValue(GOING);
    await openDialog();
    fireEvent.click(runButton());
    fireEvent.click(await screen.findByTestId("discover-background", {}, SLOW));
    await waitFor(() => expect(screen.queryByTestId("discover-run-line")).toBeNull());
    expect(stopMock).not.toHaveBeenCalled();
    expect(discoverButton()).toHaveAttribute("data-running", "true");
    await waitFor(
      () => expect(pageLine()).toHaveTextContent("Requests: 1 of 4 · Suggestions so far: 2"),
      SLOW,
    );
    // The run is read on while the dialog is closed: the page's line follows it.
    runMock.mockResolvedValue({ ...GOING, requests_done: 3, suggestions: 5 });
    await waitFor(
      () => expect(pageLine()).toHaveTextContent("Requests: 3 of 4 · Suggestions so far: 5"),
      SLOW,
    );
    fireEvent.click(screen.getByTestId("rels-discover-open"));
    expect(await screen.findByTestId("discover-run-line")).toHaveTextContent("Requests: 3 of 4");
    expect(screen.queryByTestId("discover-run")).toBeNull();
  });

  it("shows each run going at once as a line of the block", async () => {
    const other = runOf({ id: 8, requests_total: 10, requests_done: 6, model: "openai:gpt-4.1" });
    runsMock.mockResolvedValue([other, GOING]);
    runMock.mockImplementation(async (id) => (id === 8 ? other : GOING));
    render(<RelationshipsPage />);
    await waitFor(() => expect(discoverButton()).toHaveAttribute("data-running", "true"));
    fireEvent.click(discoverButton());
    const block = await screen.findByTestId("discover-progress");
    // The newest is the block's first line; its own line adds the model and its Stop.
    expect(within(block).getByTestId("discover-run-line")).toHaveTextContent(
      "Requests: 6 of 10 · Suggestions so far: 0",
    );
    expect(within(block).getByTestId("discover-going-8")).toHaveTextContent(
      "Model: openai:gpt-4.1",
    );
    // The other run going is a line of its own: how far it has got, its suggestions and model.
    const other7 = within(block).getByTestId("discover-going-7");
    expect(other7).toHaveTextContent("Requests: 1 of 4");
    expect(other7).toHaveTextContent("Suggestions so far: 2 · Model: anthropic:claude-haiku-4-5");
    expect(within(block).getByTestId("discover-stop-8")).toBeEnabled();
    fireEvent.click(within(block).getByTestId("discover-stop-7"));
    await waitFor(() => expect(stopMock).toHaveBeenCalledWith(7));
  });
});

describe("Discovery: the ended run's block and the page's line", () => {
  it("collapses the summary to its first line, which opens to the whole summary", async () => {
    runMock.mockResolvedValue(ENDED);
    proposalsMock.mockResolvedValue({
      proposals: [proposal({}), proposal({ id: 502 })],
      threshold: 0.7,
      kept_at: 0.7,
      score_filter: null,
    });
    await openDialog();
    fireEvent.click(runButton());
    await screen.findByTestId("proposal-501", {}, SLOW);
    expect(screen.getByTestId("discover-run-line")).toHaveTextContent(
      "Finished. 2 suggestions wait for a decision.",
    );
    expect(toggle()).toHaveAttribute("aria-expanded", "false");
    fireEvent.click(toggle());
    expect(toggle()).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByTestId("discover-result")).toHaveTextContent(
      "Foreign keys: 3 found, 1 suggested.",
    );
    expect(screen.queryByTestId("discover-background")).toBeNull();
  });

  it("the page shows a run that ended meanwhile as one line, with no summary, and Open shows it", async () => {
    runsMock.mockResolvedValue([ENDED]);
    proposalsMock.mockResolvedValue({
      proposals: [
        proposal({}),
        // Left out by the run's rules: not among the suggestions waiting.
        proposal({ id: 503, reason: "below_threshold", confidence: 0.4 }),
        proposal({ id: 505, reason: "not_comparable", decidable: false }),
      ],
      threshold: 0.7,
      kept_at: 0.7,
      score_filter: null,
    });
    render(<RelationshipsPage />);
    await waitFor(() =>
      expect(pageLine()).toHaveTextContent("Finished. 1 suggestion waits for a decision."),
    );
    expect(screen.queryByTestId("rels-discover-result")).toBeNull();
    expect(pageLine()).not.toHaveTextContent("Foreign keys");
    fireEvent.click(screen.getByTestId("rels-discover-open"));
    expect(await within(dialog()).findByTestId("proposal-501", {}, SLOW)).toBeInTheDocument();
    expect(screen.getByTestId("discover-run-line")).toHaveTextContent(
      "Finished. 1 suggestion waits for a decision.",
    );
  });

  it("the page's line goes once the run's suggestions are all decided", async () => {
    runsMock.mockResolvedValue([ENDED]);
    proposalsMock.mockResolvedValue({
      proposals: [proposal({})],
      threshold: 0.7,
      kept_at: 0.7,
      score_filter: null,
    });
    render(<RelationshipsPage />);
    await waitFor(() => expect(pageLine()).toBeInTheDocument());
    fireEvent.click(screen.getByTestId("rels-discover-open"));
    await screen.findByTestId("proposal-501", {}, SLOW);
    applyMock.mockResolvedValue([
      { id: 501, action: "accept", ok: true, relationship_id: "customer-orders" },
    ]);
    proposalsMock.mockResolvedValue({
      proposals: [],
      threshold: 0.7,
      kept_at: 0.7,
      score_filter: null,
    });
    fireEvent.click(screen.getByTestId("discover-apply"));
    await waitFor(() =>
      expect(applyMock).toHaveBeenCalledWith(7, { accept: [{ id: 501 }], not_again: [] }),
    );
    await waitFor(() => expect(screen.queryByTestId("rels-discover-line")).toBeNull());
  });

  it("shows no line before any run", async () => {
    render(<RelationshipsPage />);
    await waitFor(() => expect(runsMock).toHaveBeenCalled());
    expect(screen.queryByTestId("rels-discover-line")).toBeNull();
  });
});
