// Copyright (c) 2026 Kenneth Stott
// Canary: 5c8e1a47-2f93-4b6d-a0e5-8d3f7b2c9e16
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: relationship discovery asks before it runs. The dialog sets the run's scope (kinds,
// model, source and target sets), says about how many requests it makes, runs it in the background
// with its progress and a Stop, and lists every proposal it read in one table: ticked to accept,
// ticked not to be recommended again, or left undecided, with one Apply. A run after the first
// starts from the fields of the one before.
// The run is the server's: the button animates while it runs, after the dialog is closed too.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { render } from "../../test-utils/render";
import { RelationshipsPage } from "../RelationshipsPage";
import type { RunProposal } from "../../api/admin";
import {
  COUNT,
  NO_VENDOR,
  PROPOSALS,
  REPORT,
  SCOPE,
  SLOW,
  acceptBox,
  applyButton,
  applyMock,
  candidatesMock,
  countMock,
  dialog,
  discoverButton,
  endedRun,
  fkBox,
  lastScope,
  limitsMock,
  modelsMock,
  notAgainBox,
  openDialog,
  pick,
  proposal,
  proposalsMock,
  resetDiscoveryMocks,
  runButton,
  runMock,
  runOf,
  runsMock,
  sameBox,
  saveThresholdMock,
  semanticsBox,
  startMock,
  stopMock,
  suggestion,
  vendorModelsMock,
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

describe("Discovery dialog: the run's scope", () => {
  it("opens with both kinds ticked, the ticked domains as source, and runs nothing yet", async () => {
    await openDialog();
    expect(within(dialog()).getByText("Discover relationships")).toBeInTheDocument();
    expect(fkBox()).toBeChecked();
    expect(semanticsBox()).toBeChecked();
    expect(semanticsBox()).toBeEnabled();
    expect(sameBox()).toBeChecked();
    expect(within(screen.getByTestId("discover-source-domains")).getByText("shop")).toBeVisible();
    expect(startMock).not.toHaveBeenCalled();
  });

  it("never offers a system domain", async () => {
    await openDialog();
    await userEvent.click(screen.getByRole("textbox", { name: "Source domains" }));
    const offered = (await screen.findAllByRole("option", { hidden: true })).map(
      (o) => o.textContent,
    );
    expect(offered).toContain("crm");
    expect(offered).not.toContain("meta");
  });

  it("never offers a system domain's table, which a run would leave out", async () => {
    await openDialog();
    // Every domain offered (shop is ticked from the Domains picker): every table a run could read.
    await pick("Source domains", "crm");
    await userEvent.click(screen.getByRole("textbox", { name: "Source tables" }));
    const offered = (await screen.findAllByRole("option", { hidden: true })).map(
      (o) => o.textContent,
    );
    expect(offered).toEqual(expect.arrayContaining(["orders", "customers", "accounts"]));
    expect(offered).not.toContain("registered_tables");
  });

  it("sends the scope it shows: kinds, source domains, every table, same as source, assigned model", async () => {
    await openDialog();
    fireEvent.click(runButton());
    await waitFor(() => expect(startMock).toHaveBeenCalledTimes(1));
    expect(startMock).toHaveBeenCalledWith(SCOPE);
  });

  it("sends the source tables chosen", async () => {
    await openDialog();
    await pick("Source tables", "orders");
    await waitFor(() => expect(lastScope().source_tables).toEqual([1]), SLOW);
    fireEvent.click(runButton());
    await waitFor(() => expect(startMock).toHaveBeenCalledWith({ ...SCOPE, source_tables: [1] }));
  });

  it("hides the target while Same as source is ticked, and sends the target set when it is not", async () => {
    await openDialog();
    expect(screen.queryByRole("textbox", { name: "Target domains" })).not.toBeInTheDocument();
    fireEvent.click(sameBox());
    await pick("Target domains", "crm");
    await waitFor(
      () =>
        expect(lastScope()).toEqual({
          ...SCOPE,
          target_same_as_source: false,
          target_domains: ["crm"],
        }),
      SLOW,
    );
    fireEvent.click(runButton());
    await waitFor(() =>
      expect(startMock).toHaveBeenCalledWith({
        ...SCOPE,
        target_same_as_source: false,
        target_domains: ["crm"],
      }),
    );
  });

  it("greys out Semantics, unticked, with the reason, when no vendor has a key", async () => {
    modelsMock.mockResolvedValue(NO_VENDOR);
    await openDialog();
    expect(semanticsBox()).toBeDisabled();
    expect(semanticsBox()).not.toBeChecked();
    expect(
      screen.getByLabelText(
        "Semantics: not available. No AI vendor has a key. Set one under Admin, AI Models.",
      ),
    ).toBe(semanticsBox());
    fireEvent.click(runButton());
    await waitFor(() =>
      expect(startMock).toHaveBeenCalledWith({ ...SCOPE, kinds: ["foreign_keys"] }),
    );
  });

  it("Run is refused with nothing ticked", async () => {
    await openDialog();
    fireEvent.click(fkBox());
    fireEvent.click(semanticsBox());
    expect(runButton()).toBeDisabled();
  });

  it("Cancel closes the dialog and runs nothing", async () => {
    await openDialog();
    fireEvent.click(screen.getByTestId("discover-cancel"));
    await waitFor(() => expect(screen.queryByTestId("discover-run")).not.toBeInTheDocument());
    expect(startMock).not.toHaveBeenCalled();
  });
});

describe("Discovery dialog: the request count before Run", () => {
  it("shows at most how many requests the run makes, by type group, and that it is a bound", async () => {
    await openDialog();
    await waitFor(
      () =>
        expect(screen.getByTestId("discover-count")).toHaveTextContent(
          "At most 3 requests to the model: whole numbers 2, text 1.",
        ),
      SLOW,
    );
    expect(screen.getByTestId("discover-count")).toHaveTextContent(
      "Source tables: 2. Target tables: 2.",
    );
    expect(screen.getByTestId("discover-count-bound")).toHaveTextContent(
      "Counted from what is registered, without reading any table. The run may make fewer.",
    );
    // Every table of the scope is read by the run: nothing to say of tables not read.
    expect(screen.queryByTestId("discover-not-read")).not.toBeInTheDocument();
    expect(lastScope()).toEqual(SCOPE);
  });

  it("says how many tables are not read, and why", async () => {
    countMock.mockResolvedValue({
      ...COUNT,
      source_tables: 29,
      target_tables: 29,
      tables_not_read: 8,
    });
    await openDialog();
    await waitFor(
      () =>
        expect(screen.getByTestId("discover-not-read")).toHaveTextContent(
          "8 tables require a parameter and are not read: they are compared on their names and descriptions alone.",
        ),
      SLOW,
    );
  });

  it("says one table is not read in the singular", async () => {
    countMock.mockResolvedValue({ ...COUNT, tables_not_read: 1 });
    await openDialog();
    await waitFor(
      () =>
        expect(screen.getByTestId("discover-not-read")).toHaveTextContent(
          "1 table requires a parameter and is not read: it is compared on its names and descriptions alone.",
        ),
      SLOW,
    );
  });

  it("shows the tables not read for foreign keys alone, with no bound to explain", async () => {
    countMock.mockResolvedValue({
      ...COUNT,
      requests: 0,
      by_group: {},
      model: null,
      tables_not_read: 2,
    });
    await openDialog();
    fireEvent.click(screen.getByTestId("discover-kind-semantics"));
    await waitFor(
      () =>
        expect(screen.getByTestId("discover-count")).toHaveTextContent("No requests to the model."),
      SLOW,
    );
    expect(screen.queryByTestId("discover-count-bound")).not.toBeInTheDocument();
    expect(screen.getByTestId("discover-not-read")).toHaveTextContent(
      "2 tables require a parameter",
    );
  });

  it("counts again when the scope changes", async () => {
    await openDialog();
    await waitFor(() => expect(countMock).toHaveBeenCalled(), SLOW);
    countMock.mockResolvedValue({ ...COUNT, requests: 5, by_group: { text: 5 } });
    await pick("Source domains", "crm");
    await waitFor(() => expect(lastScope().source_domains).toEqual(["shop", "crm"]), SLOW);
    await waitFor(() =>
      expect(screen.getByTestId("discover-count")).toHaveTextContent(
        "At most 5 requests to the model: text 5.",
      ),
    );
  });

  it("shows a refusal of the count and refuses Run with it", async () => {
    countMock.mockRejectedValue(new Error("The model has no room for a request."));
    render(<RelationshipsPage />);
    fireEvent.click(discoverButton());
    await waitFor(
      () =>
        expect(screen.getByTestId("discover-count-error")).toHaveTextContent(
          "The model has no room for a request.",
        ),
      SLOW,
    );
    expect(runButton()).toBeDisabled();
  });
});

describe("Discovery dialog: the model for this run", () => {
  it("opens on the assigned model, with its limits", async () => {
    await openDialog();
    expect(screen.getByTestId("discover-vendor")).toHaveValue("anthropic");
    expect(screen.getByTestId("discover-model")).toHaveValue("claude-haiku-4-5");
    expect(screen.getByTestId("discover-limits")).toHaveTextContent(
      "Reads up to 200,000 tokens; answers up to 64,000 tokens.",
    );
  });

  it("runs with another vendor's model, counting and showing its limits, for this run only", async () => {
    await openDialog();
    fireEvent.click(screen.getByTestId("discover-vendor"));
    fireEvent.click(await screen.findByRole("option", { name: "openai", hidden: true }));
    await waitFor(() => expect(vendorModelsMock).toHaveBeenCalledWith("openai"));
    expect(runButton()).toBeDisabled();
    fireEvent.click(screen.getByTestId("discover-model"));
    fireEvent.click(await screen.findByRole("option", { name: "gpt-4.1", hidden: true }));
    await waitFor(() =>
      expect(screen.getByTestId("discover-limits")).toHaveTextContent(
        "Reads up to 1,047,576 tokens; answers up to 32,768 tokens.",
      ),
    );
    expect(limitsMock).toHaveBeenCalledWith("openai", "gpt-4.1");
    await waitFor(() => expect(lastScope().model).toBe("gpt-4.1"), SLOW);
    expect(lastScope().vendor).toBe("openai");
    await waitFor(() => expect(runButton()).toBeEnabled());
    fireEvent.click(runButton());
    await waitFor(() =>
      expect(startMock).toHaveBeenCalledWith({ ...SCOPE, vendor: "openai", model: "gpt-4.1" }),
    );
  });

  it("lets the model be typed when the vendor does not list its models", async () => {
    const { DiscoveryError } =
      await vi.importActual<typeof import("../../api/admin")>("../../api/admin");
    vendorModelsMock.mockRejectedValue(
      new DiscoveryError(404, "discovery.vendor_unknown", "No such vendor listing."),
    );
    await openDialog();
    fireEvent.click(screen.getByTestId("discover-vendor"));
    fireEvent.click(await screen.findByRole("option", { name: "openai", hidden: true }));
    const typed = await screen.findByTestId("discover-model-typed");
    fireEvent.change(typed, { target: { value: "my-model" } });
    await waitFor(() => expect(lastScope().model).toBe("my-model"), SLOW);
    expect(lastScope().vendor).toBe("openai");
  });
});

describe("Discovery dialog: the run in progress", () => {
  it("shows the progress as it is read, then the result naming the model that ran", async () => {
    runMock
      .mockResolvedValueOnce(
        runOf({ report: { phase: "reading", tables_read: 1, tables_total: 2 } }),
      )
      .mockResolvedValueOnce(
        runOf({ requests_total: 3, requests_done: 1, suggestions: 2, report: REPORT }),
      )
      .mockResolvedValue(
        runOf({
          status: "done",
          requests_total: 3,
          requests_done: 3,
          suggestions: 2,
          report: REPORT,
        }),
      );
    await openDialog();
    fireEvent.click(runButton());
    const progress = await screen.findByTestId("discover-progress");
    await waitFor(() => expect(progress).toHaveTextContent("Reading tables: 1 of 2"), SLOW);
    await waitFor(() => expect(progress).toHaveTextContent("Requests: 1 of 3"), SLOW);
    expect(progress).toHaveTextContent("Suggestions so far: 2");
    await waitFor(() => expect(progress).toHaveTextContent("Finished."), SLOW);
    const result = screen.getByTestId("discover-result");
    expect(result).toHaveTextContent("Model: anthropic:claude-haiku-4-5");
    expect(result).toHaveTextContent("Foreign keys: 3 found, 1 suggested.");
    expect(result).toHaveTextContent("Semantics: 1 suggestion.");
    expect(result).toHaveTextContent(
      "Not suggested: scored less than 70% (1), already registered (2), rejected before (1).",
    );
  });

  it("a run whose every request failed says so, in the dialog and in the page's banner", async () => {
    const failed = (group: "whole_number" | "date") => ({
      group,
      code: "llm.request_failed",
      params: { reason: "overloaded" },
      message: "The request to the model failed: overloaded",
    });
    const report = {
      ...REPORT,
      suggestions: 1,
      removed: {},
      semantics: {
        ...REPORT.semantics,
        suggested: 0,
        empty_reason: "requests_failed" as const,
        requests_total: 2,
        requests_done: 2,
        proposed: 0,
        below_threshold: 0,
        failed: [failed("whole_number"), failed("date")],
      },
    };
    runMock.mockResolvedValue(
      runOf({ status: "done", requests_total: 2, requests_done: 2, suggestions: 1, report }),
    );
    await openDialog();
    fireEvent.click(runButton());
    const progress = await screen.findByTestId("discover-progress");
    await waitFor(() => expect(progress).toHaveTextContent("Finished."), SLOW);
    for (const shown of [
      screen.getByTestId("discover-result"),
      screen.getByTestId("rels-discover-result"),
    ]) {
      expect(shown).toHaveTextContent("Semantics failed: all 2 requests to the model failed.");
      expect(shown).toHaveTextContent(
        "For whole numbers columns: The request to the model failed: overloaded",
      );
      expect(shown).toHaveTextContent(
        "For dates columns: The request to the model failed: overloaded",
      );
      expect(shown).not.toHaveTextContent("proposed no relationships");
    }
  });

  it("Stop asks the server to stop and keeps what was found", async () => {
    proposalsMock.mockResolvedValue({ proposals: [proposal({})], threshold: 0.7, kept_at: 0.7 });
    candidatesMock.mockResolvedValue([
      suggestion(101, "customer_id", "run:7"),
      suggestion(55, "buyer_id", "cross-domain"),
    ]);
    runMock.mockResolvedValue(
      runOf({ requests_total: 3, requests_done: 1, suggestions: 1, report: REPORT }),
    );
    await openDialog();
    fireEvent.click(runButton());
    fireEvent.click(await screen.findByTestId("discover-stop"));
    await waitFor(() => expect(stopMock).toHaveBeenCalledWith(7));
    await waitFor(() =>
      expect(screen.getByTestId("discover-progress")).toHaveTextContent(
        "Stopping after the request in flight.",
      ),
    );
    expect(screen.getByTestId("discover-stop")).toBeDisabled();
    runMock.mockResolvedValue(
      runOf({
        status: "stopped",
        stop_requested: true,
        requests_total: 3,
        requests_done: 1,
        suggestions: 1,
        report: REPORT,
      }),
    );
    await waitFor(
      () =>
        expect(screen.getByTestId("discover-progress")).toHaveTextContent(
          "Stopped. What was found is kept.",
        ),
      SLOW,
    );
    // What the stopped run read is listed in the dialog: its own proposals, read from the run.
    const listed = within(dialog());
    expect(await listed.findByTestId("proposal-501")).toHaveTextContent("shop.orders.customer_id");
    expect(proposalsMock).toHaveBeenCalledWith(7);
    expect(listed.queryByText("orders-buyer_id")).not.toBeInTheDocument();
  });

  it("shows a failed run's reason", async () => {
    runMock.mockResolvedValue(runOf({ status: "failed", error: "The model failed: 401" }));
    await openDialog();
    fireEvent.click(runButton());
    await waitFor(
      () =>
        expect(screen.getByTestId("discover-progress")).toHaveTextContent(
          "The run failed: The model failed: 401",
        ),
      SLOW,
    );
  });

  it("shows a refused start in the dialog and stays on the scope", async () => {
    startMock.mockRejectedValue(new Error("No vendor key for anthropic."));
    await openDialog();
    fireEvent.click(runButton());
    await waitFor(() =>
      expect(screen.getByTestId("discover-start-error")).toHaveTextContent(
        "No vendor key for anthropic.",
      ),
    );
    expect(runButton()).toBeInTheDocument();
  });
});

describe("Discovery: the button animates while a run is in progress", () => {
  it("animates after the dialog is closed, and opening it again shows the run", async () => {
    runMock.mockResolvedValue(runOf({ requests_total: 3, requests_done: 1, report: REPORT }));
    await openDialog();
    expect(discoverButton()).not.toHaveAttribute("data-running");
    fireEvent.click(runButton());
    await screen.findByTestId("discover-progress");
    expect(discoverButton()).toHaveAttribute("data-running", "true");
    fireEvent.keyDown(dialog(), { key: "Escape" });
    await waitFor(() => expect(screen.queryByTestId("discover-progress")).not.toBeInTheDocument());
    expect(discoverButton()).toHaveAttribute("data-running", "true");
    expect(discoverButton()).toBeEnabled();
    runsMock.mockResolvedValue([runOf({ requests_total: 3, requests_done: 2, report: REPORT })]);
    fireEvent.click(discoverButton());
    await waitFor(() =>
      expect(screen.getByTestId("discover-progress")).toHaveTextContent("Requests: 2 of 3"),
    );
    expect(screen.queryByTestId("discover-run")).not.toBeInTheDocument();
  });

  it("animates for a run the server says is still going when the page opens", async () => {
    runsMock.mockResolvedValue([runOf({ requests_total: 3, requests_done: 1, report: REPORT })]);
    runMock.mockResolvedValue(runOf({ requests_total: 3, requests_done: 1, report: REPORT }));
    render(<RelationshipsPage />);
    await waitFor(() => expect(discoverButton()).toHaveAttribute("data-running", "true"));
    fireEvent.click(discoverButton());
    expect(await screen.findByTestId("discover-progress")).toHaveTextContent("Requests: 1 of 3");
  });

  it("stops animating when the run ends", async () => {
    runMock.mockResolvedValue(runOf({ status: "done", report: REPORT }));
    await openDialog();
    fireEvent.click(runButton());
    await waitFor(() => expect(discoverButton()).toHaveAttribute("data-running", "true"));
    await waitFor(() => expect(discoverButton()).not.toHaveAttribute("data-running"), SLOW);
  });
});

describe("Discovery dialog: the run's proposals, in one table", () => {
  it("lists every proposal the run read, the highest score first, with its kind and why", async () => {
    await endedRun();
    expect(proposalsMock).toHaveBeenCalledWith(7);
    const rows = within(screen.getByTestId("discover-proposals"))
      .getAllByTestId(/^proposal-\d+$/)
      .map((row) => row.getAttribute("data-testid"));
    expect(rows).toEqual(PROPOSALS.map((p) => `proposal-${p.id}`));

    const constraint = screen.getByTestId("proposal-500");
    expect(constraint).toHaveTextContent("100%");
    expect(constraint).toHaveTextContent("shop.orders.store_id");
    expect(constraint).toHaveTextContent("shop.customers.id");
    expect(constraint).toHaveTextContent("many-to-one");
    expect(constraint).toHaveTextContent("Constraint");
    expect(screen.getByTestId("proposal-501")).toHaveTextContent("Semantic");
    expect(screen.getByTestId("proposal-503")).toHaveTextContent(
      "Scored under the 70% a suggestion is kept at.",
    );
    // The model's score is the row's; Jev's is shown beside it, in its own column.
    expect(screen.getByTestId("discover-proposals")).toHaveTextContent("Jev");
    expect(screen.getByTestId("proposal-504")).toHaveTextContent("92%");
    expect(screen.getByTestId("proposal-jev-504")).toHaveTextContent("31%");
    expect(screen.getByTestId("proposal-jev-501")).toHaveTextContent(/^$/);
    expect(screen.getByTestId("proposal-504")).not.toHaveTextContent("threshold");
  });

  it("starts the rows scoring 70% or more ticked to accept, and none marked", async () => {
    await endedRun();
    expect(acceptBox(500).checked).toBe(true);
    expect(acceptBox(501).checked).toBe(true);
    // Already waiting from an earlier run: an ordinary row.
    expect(acceptBox(502).checked).toBe(true);
    expect(acceptBox(503).checked).toBe(false);
    // The model's score decides: Jev's 31% beside its 92% does not untick it.
    expect(acceptBox(504).checked).toBe(true);
    for (const id of [500, 501, 502, 503, 504]) expect(notAgainBox(id).checked).toBe(false);
    expect(applyButton()).toHaveTextContent("Accept 4");
    expect(applyButton()).toBeEnabled();
  });

  it("lists a proposal that cannot be decided with its reason and no box", async () => {
    await endedRun();
    const repeated = screen.getByTestId("proposal-505");
    expect(repeated).toHaveTextContent("Matches a foreign key found in this run");
    const unknown = screen.getByTestId("proposal-506");
    expect(unknown).toHaveTextContent("Names a table or column the request did not compare");
    expect(unknown).toHaveTextContent(
      'As the model wrote it: {"source_table_id": 1, "target_table_id": 9}',
    );
    for (const row of [repeated, unknown]) {
      expect(within(row).queryAllByRole("checkbox")).toEqual([]);
    }
  });

  it("a row is ticked to accept or not to be recommended again, never both", async () => {
    await endedRun();
    fireEvent.click(notAgainBox(501));
    expect(notAgainBox(501).checked).toBe(true);
    expect(acceptBox(501).checked).toBe(false);
    expect(applyButton()).toHaveTextContent("Accept 3, don't recommend 1 again");
    fireEvent.click(acceptBox(501));
    expect(acceptBox(501).checked).toBe(true);
    expect(notAgainBox(501).checked).toBe(false);
    expect(applyButton()).toHaveTextContent("Accept 4");
  });

  it("the header's box ticks every row that can be decided, or none", async () => {
    await endedRun();
    fireEvent.click(notAgainBox(503));
    fireEvent.click(
      within(screen.getByTestId("discover-proposals")).getByTestId("list-select-all"),
    );
    for (const id of [500, 501, 502, 503, 504]) expect(acceptBox(id).checked).toBe(true);
    expect(notAgainBox(503).checked).toBe(false);
    expect(applyButton()).toHaveTextContent("Accept 5");
    fireEvent.click(
      within(screen.getByTestId("discover-proposals")).getByTestId("list-select-all"),
    );
    for (const id of [500, 501, 502, 503, 504]) expect(acceptBox(id).checked).toBe(false);
  });

  it("with nothing ticked Apply is disabled, and with only marks it says so", async () => {
    await endedRun();
    for (const id of [500, 501, 502, 504]) fireEvent.click(acceptBox(id));
    expect(applyButton()).toBeDisabled();
    expect(applyButton()).toHaveTextContent("Apply");
    fireEvent.click(notAgainBox(503));
    fireEvent.click(notAgainBox(504));
    expect(applyButton()).toBeEnabled();
    expect(applyButton()).toHaveTextContent("Don't recommend 2 again");
  });

  it("Apply sends what is ticked, each way, and leaves the rest undecided", async () => {
    await endedRun();
    fireEvent.click(acceptBox(502));
    fireEvent.click(notAgainBox(504));
    applyMock.mockResolvedValue([
      { id: 500, action: "accept", ok: true, relationship_id: "order-store" },
      {
        id: 501,
        action: "accept",
        ok: false,
        error: "A relationship named 'customer-orders' already exists",
      },
      { id: 504, action: "not_again", ok: true },
    ]);
    proposalsMock.mockResolvedValue({
      proposals: PROPOSALS.filter((p) => ![500, 504].includes(p.id)),
      threshold: 0.7,
      kept_at: 0.7,
    });
    expect(applyButton()).toHaveTextContent("Accept 2, don't recommend 1 again");
    fireEvent.click(applyButton());
    await waitFor(() =>
      expect(applyMock).toHaveBeenCalledWith(7, {
        accept: [
          { id: 500, name: "order-store" },
          { id: 501, name: "customer-orders" },
        ],
        not_again: [504],
      }),
    );
    expect(await screen.findByTestId("discover-applied")).toHaveTextContent(
      "1 accepted, 1 not to be recommended again.",
    );
    expect(screen.getByTestId("discover-apply-failures")).toHaveTextContent(
      "customer-orders: A relationship named 'customer-orders' already exists",
    );
    // The table is read again: what was decided has left it, the undecided rows are still there.
    await waitFor(() => expect(screen.queryByTestId("proposal-500")).toBeNull());
    expect(screen.getByTestId("proposal-502")).toBeInTheDocument();
    expect(screen.getByTestId("proposal-503")).toBeInTheDocument();
  });

  it("sorts by a column when its header is pressed", async () => {
    await endedRun();
    fireEvent.click(screen.getByTestId("discover-proposals-sort-score"));
    const rows = within(screen.getByTestId("discover-proposals"))
      .getAllByTestId(/^proposal-\d+$/)
      .map((row) => row.getAttribute("data-testid"));
    // Ascending by the model's score: the one with no score, then the lowest.
    expect(rows.slice(0, 2)).toEqual(["proposal-506", "proposal-503"]);
    fireEvent.click(screen.getByTestId("discover-proposals-sort-jev"));
    const byJev = within(screen.getByTestId("discover-proposals"))
      .getAllByTestId(/^proposal-\d+$/)
      .map((row) => row.getAttribute("data-testid"));
    // Jev's column sorts too: the one Jev scored comes last, ascending.
    expect(byJev[byJev.length - 1]).toBe("proposal-504");
  });

  it("says the scope the run ran with in its result", async () => {
    runMock.mockResolvedValue(
      runOf({
        status: "done",
        report: {
          ...REPORT,
          source_domains: ["shop"],
          target_domains: ["shop"],
          source_tables: 2,
          target_tables: 2,
          target_same_as_source: true,
        },
      }),
    );
    await openDialog();
    fireEvent.click(runButton());
    const result = await screen.findByTestId("discover-result", {}, SLOW);
    expect(result).toHaveTextContent("Looked for: Foreign keys, Semantics.");
    expect(result).toHaveTextContent("Model: anthropic:claude-haiku-4-5");
    expect(result).toHaveTextContent("Source: 2 tables of shop.");
    expect(result).toHaveTextContent("Target: the same as the source.");
  });
});

describe("Discovery dialog: each pair measured on its data, ticked from the person's threshold", () => {
  const side = (values: number, distinct: boolean) => ({
    values,
    more: false,
    distinct,
    unmeasured: null,
  });
  const MEASURED: RunProposal[] = [
    // The permit key: the model gave 45%, its values are contained in a distinct target.
    proposal({
      id: 601,
      source_column: "permit_id",
      target_column: "permit_id",
      confidence: 0.45,
      score: 0.95,
      measured: true,
      measure: {
        bound: 10000,
        source: side(30, false),
        target: side(100, true),
        source_in_target: 30,
        target_in_source: 30,
      },
    }),
    // A shared neighborhood, a key of neither table: many-to-many, under the threshold.
    proposal({
      id: 602,
      source_column: "neighborhood",
      target_column: "neighborhood",
      cardinality: "many-to-many",
      confidence: 0.9,
      score: 0.6,
      measured: true,
      reason: "below_threshold",
      acceptable: false,
      measure: {
        bound: 10000,
        source: side(5, false),
        target: side(5, false),
        source_in_target: 5,
        target_in_source: 5,
      },
    }),
    // A table that requires a parameter: only the target was read.
    proposal({
      id: 603,
      source_column: "owner_ref",
      confidence: 0.8,
      score: 0.8,
      measured: false,
      reduced_information: true,
      measure: {
        bound: 10000,
        source: { values: null, more: false, distinct: null, unmeasured: "not_read" },
        target: side(100, true),
        source_in_target: null,
        target_in_source: null,
      },
    }),
    proposal({
      id: 604,
      source_column: "code",
      confidence: 0.95,
      score: 0.55,
      measured: true,
      reason: "below_threshold",
    }),
  ];

  async function measuredRun(threshold = 0.7) {
    runMock.mockResolvedValue(runOf({ status: "done", report: REPORT }));
    proposalsMock.mockResolvedValue({ proposals: MEASURED, threshold, kept_at: 0.7 });
    await openDialog();
    fireEvent.click(runButton());
    await screen.findByTestId("proposal-601", {}, SLOW);
  }

  it("shows each row's measure, its score by the measure and the model's beside it", async () => {
    await measuredRun();
    expect(screen.getByTestId("proposal-score-601")).toHaveTextContent("95%");
    expect(screen.getByTestId("proposal-model-601")).toHaveTextContent("45%");
    expect(screen.getByTestId("proposal-measure-601")).toHaveTextContent(
      "The target is a key. 30 of 30 source values in the target. " +
        "30 of 100 target values in the source.",
    );
    expect(screen.getByTestId("proposal-cardinality-602")).toHaveTextContent("many-to-many");
    expect(screen.getByTestId("proposal-measure-602")).toHaveTextContent(
      "Neither column is a key.",
    );
    expect(screen.getByTestId("proposal-measure-603")).toHaveTextContent(
      "The source was not read: its table requires a parameter. " +
        "The target is a key. Scored by the model: no values were compared.",
    );
  });

  it("a many-to-many row cannot be accepted, says why, and can be marked not to be recommended", async () => {
    await measuredRun();
    const row = screen.getByTestId("proposal-602");
    expect(within(row).queryByTestId("list-select-602")).toBeNull();
    expect(row).toHaveTextContent("Neither column is a key, so it cannot be accepted.");
    expect(notAgainBox(602)).toBeInTheDocument();
  });

  it("the threshold set in the dialog re-ticks the rows and is kept for the person", async () => {
    await measuredRun();
    const threshold = screen.getByTestId("discover-threshold") as HTMLInputElement;
    expect(threshold.value).toBe("70%");
    expect(acceptBox(601).checked).toBe(true);
    expect(acceptBox(603).checked).toBe(true);
    expect(acceptBox(604).checked).toBe(false);
    fireEvent.change(threshold, { target: { value: "50" } });
    // Under the run's own bar but above the person's: it starts ticked now.
    await waitFor(() => expect(acceptBox(604).checked).toBe(true));
    await waitFor(() => expect(saveThresholdMock).toHaveBeenCalledWith(0.5), SLOW);
    expect(saveThresholdMock).toHaveBeenCalledTimes(1);
    fireEvent.change(threshold, { target: { value: "90" } });
    await waitFor(() => expect(acceptBox(603).checked).toBe(false));
    expect(acceptBox(601).checked).toBe(true);
  });

  it("opens with the threshold the person kept, read back from the server", async () => {
    await measuredRun(0.9);
    expect((screen.getByTestId("discover-threshold") as HTMLInputElement).value).toBe("90%");
    expect(acceptBox(601).checked).toBe(true);
    expect(acceptBox(603).checked).toBe(false);
  });
});
