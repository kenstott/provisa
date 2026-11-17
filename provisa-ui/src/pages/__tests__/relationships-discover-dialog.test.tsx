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
import {
  applyRunDecisions,
  countDiscoveryRun,
  fetchCandidates,
  fetchDiscoveryModels,
  fetchDiscoveryRun,
  fetchDiscoveryRuns,
  fetchModelLimits,
  fetchRunProposals,
  fetchVendorModels,
  startDiscoveryRun,
  stopDiscoveryRun,
  type DiscoveryCount,
  type DiscoveryModels,
  type DiscoveryRun,
  type DiscoveryScope,
  type RunProposal,
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
  fetchRunProposals: vi.fn(),
  applyRunDecisions: vi.fn(),
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
const proposalsMock = vi.mocked(fetchRunProposals);
const applyMock = vi.mocked(applyRunDecisions);
const proposal = (over: Partial<RunProposal>): RunProposal => ({
  id: 501,
  run_id: 7,
  kind: "semantics",
  source_table_id: 1,
  source_column: "customer_id",
  target_table_id: 2,
  target_column: "id",
  cardinality: "many-to-one",
  confidence: 0.95,
  scored: null,
  reasoning: "Each order names its customer",
  suggested_name: "customer-orders",
  reason: null,
  answer: null,
  decidable: true,
  reduced_information: false,
  ...over,
});
/** A run's table: a constraint, a suggestion, one under the threshold, one Jev scored down, one
 *  already waiting, one repeating the constraint, and one that named no two compared columns. */
const PROPOSALS: RunProposal[] = [
  proposal({
    id: 500,
    kind: "foreign_keys",
    source_column: "store_id",
    confidence: 1,
    reasoning: "Foreign key constraint",
    suggested_name: "order-store",
  }),
  proposal({}),
  proposal({
    id: 502,
    source_column: "payer_id",
    confidence: 0.9,
    reason: "pending",
    suggested_name: "order-payer",
  }),
  proposal({
    id: 503,
    source_column: "agent_id",
    confidence: 0.55,
    reason: "below_threshold",
    reasoning: "Perhaps the agent is a customer",
    suggested_name: "order-agent",
  }),
  proposal({
    id: 504,
    source_column: "ref",
    confidence: 0.92,
    scored: 0.31,
    reason: "scored_below_threshold",
    suggested_name: "order-ref",
  }),
  proposal({
    id: 505,
    source_column: "store_id",
    confidence: 0.9,
    reason: "matches_foreign_key",
    decidable: false,
  }),
  proposal({
    id: 506,
    source_table_id: null,
    source_column: null,
    target_table_id: null,
    target_column: null,
    cardinality: null,
    confidence: null,
    reasoning: null,
    suggested_name: null,
    reason: "unknown_column",
    answer: '{"source_table_id": 1, "target_table_id": 9}',
    decidable: false,
  }),
];
const acceptBox = (id: number) => screen.getByTestId(`list-select-${id}`) as HTMLInputElement;
const notAgainBox = (id: number) =>
  screen.getByTestId(`proposal-not-again-${id}`) as HTMLInputElement;
const applyButton = () => screen.getByTestId("discover-apply");
async function endedRun() {
  runMock.mockResolvedValue(runOf({ status: "done", report: REPORT }));
  proposalsMock.mockResolvedValue({ proposals: PROPOSALS, threshold: 0.7 });
  await openDialog();
  fireEvent.click(runButton());
  await screen.findByTestId("proposal-501", {}, SLOW);
}

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
  // A panel opens as this viewer left it: each test starts with nothing remembered.
  window.localStorage.clear();
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
  proposalsMock.mockReset().mockResolvedValue({ proposals: [], threshold: 0.7 });
  applyMock.mockReset().mockResolvedValue([]);
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
      "Not suggested: given less than 70% confidence by the model (1), already registered (2), rejected before (1).",
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
    proposalsMock.mockResolvedValue({ proposals: [proposal({})], threshold: 0.7 });
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
      "The model's confidence is below the 70% threshold.",
    );
    // Jev's score is the row's; the note says both.
    expect(screen.getByTestId("proposal-504")).toHaveTextContent("31%");
    expect(screen.getByTestId("proposal-504")).toHaveTextContent(
      "Jev scored it 31%, below the 70% threshold. The model gave 92%.",
    );
  });

  it("starts the rows scoring 70% or more ticked to accept, and none marked", async () => {
    await endedRun();
    expect(acceptBox(500).checked).toBe(true);
    expect(acceptBox(501).checked).toBe(true);
    // Already waiting from an earlier run: an ordinary row.
    expect(acceptBox(502).checked).toBe(true);
    expect(acceptBox(503).checked).toBe(false);
    // The score is Jev's where Jev gave one.
    expect(acceptBox(504).checked).toBe(false);
    for (const id of [500, 501, 502, 503, 504]) expect(notAgainBox(id).checked).toBe(false);
    expect(applyButton()).toHaveTextContent("Accept 3");
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
    expect(applyButton()).toHaveTextContent("Accept 2, don't recommend 1 again");
    fireEvent.click(acceptBox(501));
    expect(acceptBox(501).checked).toBe(true);
    expect(notAgainBox(501).checked).toBe(false);
    expect(applyButton()).toHaveTextContent("Accept 3");
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
    for (const id of [500, 501, 502]) fireEvent.click(acceptBox(id));
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
    // Ascending: the one with no score, then the lowest.
    expect(rows.slice(0, 3)).toEqual(["proposal-506", "proposal-504", "proposal-503"]);
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

const LAST: DiscoveryScope = {
  kinds: ["semantics"],
  source_domains: ["crm"],
  source_tables: [3],
  target_same_as_source: false,
  target_domains: ["shop"],
  target_tables: [2],
  vendor: "openai",
  model: "gpt-4.1-mini",
};

describe("Discovery dialog: a run starts from the fields of the one before", () => {
  it("New run opens the form with the ended run's fields, not the defaults", async () => {
    runMock.mockResolvedValue(runOf({ status: "done", scope: LAST, report: REPORT }));
    startMock.mockResolvedValue(runOf({ scope: LAST }));
    await openDialog();
    fireEvent.click(runButton());
    fireEvent.click(await screen.findByTestId("discover-new-run", {}, SLOW));
    await waitFor(() => expect(runButton()).toBeEnabled(), SLOW);
    expect(screen.getByTestId("discover-from-last-run")).toHaveTextContent(
      "These are the fields of your last run.",
    );
    expect(fkBox().checked).toBe(false);
    expect(semanticsBox().checked).toBe(true);
    expect(sameBox().checked).toBe(false);
    await waitFor(() => expect(lastScope()).toEqual(LAST), SLOW);
    expect(screen.queryByTestId("discover-restore-notes")).toBeNull();
  });

  it("opening the dialog starts from the latest run the signed-in person started", async () => {
    runsMock.mockImplementation(async (_limit, mine) =>
      mine ? [runOf({ id: 5, status: "done", scope: LAST })] : [],
    );
    await openDialog();
    expect(runsMock).toHaveBeenCalledWith(1, true);
    expect(fkBox().checked).toBe(false);
    expect(sameBox().checked).toBe(false);
    await waitFor(() => expect(lastScope()).toEqual(LAST), SLOW);
  });

  it("starts from the defaults when the person has run none", async () => {
    await openDialog();
    expect(screen.queryByTestId("discover-from-last-run")).toBeNull();
    expect(screen.queryByTestId("discover-reset-defaults")).toBeNull();
    expect(fkBox().checked).toBe(true);
    await waitFor(() => expect(lastScope()).toEqual(SCOPE), SLOW);
  });

  it("leaves out what no longer holds, says which, and keeps the rest", async () => {
    const stale: DiscoveryScope = {
      ...LAST,
      source_domains: ["crm", "legacy"],
      source_tables: [3, 99],
      target_tables: [2, 98],
      vendor: "mistral",
      model: "mistral-large",
    };
    runsMock.mockImplementation(async (_limit, mine) =>
      mine ? [runOf({ id: 5, status: "done", scope: stale })] : [],
    );
    await openDialog();
    const notes = await screen.findByTestId("discover-restore-notes");
    expect(notes).toHaveTextContent("Left out of them, because it no longer holds:");
    expect(notes).toHaveTextContent("The domain legacy is no longer available.");
    expect(notes).toHaveTextContent("2 tables are no longer registered there.");
    expect(notes).toHaveTextContent("mistral has no key, so the model is the assigned one.");
    // The rest is kept: the kinds, the domains and tables still there, and the target set.
    await waitFor(() => expect(lastScope()).toEqual({ ...LAST, vendor: null, model: null }), SLOW);
  });

  it("leaves out a model its vendor no longer lists", async () => {
    runsMock.mockImplementation(async (_limit, mine) =>
      mine ? [runOf({ id: 5, status: "done", scope: { ...LAST, model: "gpt-3" } })] : [],
    );
    await openDialog();
    expect(await screen.findByTestId("discover-restore-notes")).toHaveTextContent(
      "openai no longer lists gpt-3, so the model is the assigned one.",
    );
    await waitFor(() => expect(lastScope()).toEqual({ ...LAST, vendor: null, model: null }), SLOW);
  });

  it("Reset to defaults sets the form up as a first run", async () => {
    runsMock.mockImplementation(async (_limit, mine) =>
      mine ? [runOf({ id: 5, status: "done", scope: LAST })] : [],
    );
    await openDialog();
    expect(fkBox().checked).toBe(false);
    fireEvent.click(screen.getByTestId("discover-reset-defaults"));
    await waitFor(() => expect(fkBox().checked).toBe(true));
    expect(sameBox().checked).toBe(true);
    expect(screen.queryByTestId("discover-from-last-run")).toBeNull();
    await waitFor(() => expect(lastScope()).toEqual(SCOPE), SLOW);
  });

  it("New run sets up another run after one has ended", async () => {
    runMock.mockResolvedValue(runOf({ status: "done", report: REPORT }));
    await openDialog();
    fireEvent.click(runButton());
    fireEvent.click(await screen.findByTestId("discover-new-run", {}, SLOW));
    expect(await screen.findByTestId("discover-run")).toBeInTheDocument();
  });
});
