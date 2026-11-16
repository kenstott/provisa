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
// with its progress and a Stop, and lists its suggestions with Accept all behind a confirmation.
// The run is the server's: the button animates while it runs, after the dialog is closed too.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { render } from "../../test-utils/render";
import { RelationshipsPage } from "../RelationshipsPage";
import {
  acceptCandidates,
  countDiscoveryRun,
  fetchCandidates,
  fetchDiscoveryModels,
  fetchDiscoveryRun,
  fetchDiscoveryRuns,
  fetchModelLimits,
  fetchVendorModels,
  startDiscoveryRun,
  stopDiscoveryRun,
  type DiscoveryCount,
  type DiscoveryModels,
  type DiscoveryRun,
  type DiscoveryScope,
} from "../../api/admin";

const RELS = { relationships: [], loading: false, refetch: vi.fn() };
const table = (id: number, domainId: string, tableName: string) => ({
  id,
  domainId,
  sourceId: `${domainId}-db`,
  schemaName: domainId,
  tableName,
  alias: null,
  columns: [],
});
const TABLES = {
  tables: [
    table(1, "shop", "orders"),
    table(2, "shop", "customers"),
    table(3, "crm", "accounts"),
    table(4, "meta", "registered_tables"),
  ],
  loading: false,
};
const AUTH = { capabilities: ["create_relationship"] };
// The Domains picker has shop ticked and crm not: the source domains start as shop.
const FILTER = {
  domainsEnabled: true,
  domains: ["shop", "crm", "meta"],
  checkedDomains: new Set<string>(["shop"]),
};

vi.mock("../../hooks/useAdminQueries", () => ({
  // REQ-2249: no code resolver set is in service here.
  useCodeResolvers: () => ({
    codeResolvers: [],
    loading: false,
    saveCodeResolver: vi.fn(),
    retireCodeResolver: vi.fn(),
  }),
  useRelationships: () => RELS,
  useAllRelationships: () => RELS,
  useTables: () => TABLES,
  useDomains: () => ({ domains: [] }),
  useUpsertRelationship: () => ({ upsertRelationship: vi.fn() }),
  useDeleteRelationship: () => ({ deleteRelationship: vi.fn() }),
  useDeleteRelationships: () => vi.fn(),
}));
vi.mock("../../context/AuthContext", () => ({ useAuth: () => AUTH }));
vi.mock("../../context/DomainFilterContext", () => ({ useDomainFilter: () => FILTER }));
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchCandidates: vi.fn(),
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
  fetchSettings: vi.fn().mockResolvedValue({
    naming: { convention: "apollo_graphql", use_domains: false, default_domain: "" },
  }),
  fetchDiscoveryModels: vi.fn(),
  fetchVendorModels: vi.fn(),
  fetchModelLimits: vi.fn(),
  countDiscoveryRun: vi.fn(),
  startDiscoveryRun: vi.fn(),
  fetchDiscoveryRun: vi.fn(),
  fetchDiscoveryRuns: vi.fn(),
  stopDiscoveryRun: vi.fn(),
  acceptCandidates: vi.fn(),
}));
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../components/TagControl", () => ({ TagControl: () => null }));

const HAIKU = {
  vendor: "anthropic",
  model: "claude-haiku-4-5",
  input_tokens: 200000,
  output_tokens: 64000,
  limits_source: "known" as const,
};
const MODELS: DiscoveryModels = {
  assigned: { ...HAIKU, has_key: true },
  vendors: ["anthropic", "openai"],
  semantics_available: true,
};
const NO_VENDOR: DiscoveryModels = { assigned: null, vendors: [], semantics_available: false };
const COUNT: DiscoveryCount = {
  requests: 3,
  requests_are: "upper_bound",
  by_group: { whole_number: 2, text: 1 },
  suggestions_per_request: 27,
  model: HAIKU,
  source_tables: 2,
  target_tables: 2,
  tables_not_read: 0,
};
const SCOPE: DiscoveryScope = {
  kinds: ["foreign_keys", "semantics"],
  source_domains: ["shop"],
  source_tables: null,
  target_same_as_source: true,
  target_domains: [],
  target_tables: null,
  vendor: null,
  model: null,
};
const runOf = (over: Partial<DiscoveryRun>): DiscoveryRun => ({
  id: 7,
  status: "running",
  scope: SCOPE,
  model: "anthropic:claude-haiku-4-5",
  requests_total: 0,
  requests_done: 0,
  suggestions: 0,
  report: null,
  error: null,
  stop_requested: false,
  stopped_by: null,
  started_by: "user-1",
  started_at: "2026-11-15T10:00:00+00:00",
  finished_at: null,
  ...over,
});
const REPORT = {
  suggestions: 2,
  stored_ids: [101, 102],
  removed: { existing: 2, rejected: 1 },
  foreign_keys: { found: 3, suggested: 1 },
  semantics: {
    suggested: 1,
    empty_reason: null,
    requests_total: 3,
    requests_done: 1,
    split: 0,
    proposed: 2,
    invalid: 0,
    below_threshold: 1,
    threshold: 0.7,
    cut_short: 0,
    unreadable: 0,
    failed: [],
    stopped: false,
  },
  model: HAIKU,
};
const suggestion = (id: number, column: string, scope: string) => ({
  id,
  source_table_id: 1,
  source_column: column,
  target_table_id: 2,
  target_column: "id",
  cardinality: "many-to-one",
  confidence: 0.9,
  reasoning: "Same customer key",
  suggested_name: `orders-${column}`,
  scope,
});

const modelsMock = vi.mocked(fetchDiscoveryModels);
const vendorModelsMock = vi.mocked(fetchVendorModels);
const limitsMock = vi.mocked(fetchModelLimits);
const countMock = vi.mocked(countDiscoveryRun);
const startMock = vi.mocked(startDiscoveryRun);
const runMock = vi.mocked(fetchDiscoveryRun);
const runsMock = vi.mocked(fetchDiscoveryRuns);
const stopMock = vi.mocked(stopDiscoveryRun);
const candidatesMock = vi.mocked(fetchCandidates);
const acceptAllMock = vi.mocked(acceptCandidates);

const discoverButton = () => screen.getByTestId("rels-discover");
const dialog = () => screen.getByTestId("discover-dialog");
const fkBox = () => screen.getByTestId("discover-kind-foreign-keys") as HTMLInputElement;
const semanticsBox = () => screen.getByTestId("discover-kind-semantics") as HTMLInputElement;
const runButton = () => screen.getByTestId("discover-run");
const sameBox = () => screen.getByTestId("discover-same-as-source") as HTMLInputElement;
const SLOW = { timeout: 4000 };

async function openDialog() {
  render(<RelationshipsPage />);
  await waitFor(() => expect(runsMock).toHaveBeenCalled());
  fireEvent.click(discoverButton());
  await waitFor(() => expect(modelsMock).toHaveBeenCalled());
  await waitFor(() => expect(runButton()).toBeEnabled(), SLOW);
}

// Pick an option from the one select named: every select's options stay in the document, so an
// option is looked for in the list its own textbox controls.
async function pick(selectName: string, optionName: string) {
  const box = screen.getByRole("textbox", { name: selectName });
  await userEvent.click(box);
  const list = await waitFor(() => {
    const found = document.getElementById(box.getAttribute("aria-controls") ?? "");
    expect(found).not.toBeNull();
    return found as HTMLElement;
  });
  await userEvent.click(within(list).getByRole("option", { name: optionName, hidden: true }));
}

const lastScope = () => countMock.mock.calls[countMock.mock.calls.length - 1][0];

beforeEach(() => {
  modelsMock.mockReset().mockResolvedValue(MODELS);
  vendorModelsMock
    .mockReset()
    .mockImplementation(async (vendor) =>
      vendor === "openai" ? ["gpt-4.1", "gpt-4.1-mini"] : ["claude-haiku-4-5", "claude-sonnet-4-5"],
    );
  limitsMock.mockReset().mockImplementation(async (vendor, model) => ({
    vendor,
    model,
    input_tokens: 1047576,
    output_tokens: 32768,
    source: "known",
  }));
  countMock.mockReset().mockResolvedValue(COUNT);
  startMock.mockReset().mockResolvedValue(runOf({}));
  runMock.mockReset().mockResolvedValue(runOf({}));
  runsMock.mockReset().mockResolvedValue([]);
  stopMock.mockReset().mockResolvedValue(runOf({ stop_requested: true }));
  candidatesMock.mockReset().mockResolvedValue([]);
  acceptAllMock.mockReset();
});

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
      "Left out: below the 70% confidence threshold (1), already registered (2), rejected before (1).",
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
    // Only this run's suggestions are listed in the dialog.
    const listed = within(dialog());
    expect(listed.getByText("orders-customer_id")).toBeInTheDocument();
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

describe("Discovery dialog: the run's suggestions", () => {
  it("Accept all asks in the non-destructive confirmation, with the count, then accepts them", async () => {
    candidatesMock.mockResolvedValue([
      suggestion(101, "customer_id", "run:7"),
      suggestion(102, "payer_id", "run:7"),
    ]);
    runMock.mockResolvedValue(runOf({ status: "done", suggestions: 2, report: REPORT }));
    acceptAllMock.mockResolvedValue([
      { id: 101, ok: true },
      { id: 102, ok: true },
    ]);
    await openDialog();
    fireEvent.click(runButton());
    await waitFor(
      () => expect(within(dialog()).getByText("orders-payer_id")).toBeInTheDocument(),
      SLOW,
    );
    fireEvent.click(within(dialog()).getByTestId("candidates-accept-all"));
    // The page's own suggestions list has the same confirmation: the open one is asked.
    const ask = await screen.findByRole("dialog", { name: "Accept all suggestions" });
    expect(ask).toHaveTextContent(
      "Accept all 2 listed suggestions? Each becomes a registered relationship.",
    );
    const confirm = within(ask).getByTestId("candidates-accept-all-confirm-confirm");
    expect(confirm).toHaveAttribute("data-tone", "confirm");
    fireEvent.click(confirm);
    await waitFor(() =>
      expect(acceptAllMock).toHaveBeenCalledWith([
        { id: 101, name: "orders-customer_id" },
        { id: 102, name: "orders-payer_id" },
      ]),
    );
  });

  it("New run sets up another run after one has ended", async () => {
    runMock.mockResolvedValue(runOf({ status: "done", report: REPORT }));
    await openDialog();
    fireEvent.click(runButton());
    fireEvent.click(await screen.findByTestId("discover-new-run", {}, SLOW));
    expect(await screen.findByTestId("discover-run")).toBeInTheDocument();
  });
});
