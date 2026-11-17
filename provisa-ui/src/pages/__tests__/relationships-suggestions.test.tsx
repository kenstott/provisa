// Copyright (c) 2026 Kenneth Stott
// Canary: 9a3d6f20-4b7e-4c18-b5e2-1f8c0a7d3e95
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: the suggestions still waiting are decided on the page as a run's result is decided in
// the dialog: the accept box, "Don't recommend again", one Apply stating its counts. A decided
// suggestion leaves the list; the run's result never covers the list of registered relationships.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { screen, fireEvent, waitFor, within } from "@testing-library/react";
import { render } from "../../test-utils/render";
import { RelationshipsPage } from "../RelationshipsPage";
import {
  applySuggestionDecisions,
  startDiscoveryRun,
  fetchDiscoveryRun,
  fetchDiscoveryRuns,
  fetchDiscoveryModels,
  countDiscoveryRun,
  fetchSuggestions,
  type RunProposal,
} from "../../api/admin";

const REGISTERED = {
  id: "inquiries-to-pets",
  sourceTableId: 1,
  targetTableId: 2,
  sourceTableName: "orders",
  targetTableName: "customers",
  sourceColumn: "customer_id",
  targetColumn: "id",
  cardinality: "many-to-one",
  materialize: false,
  refreshInterval: 300,
  targetFunctionName: null,
  functionArg: null,
  alias: null,
  graphqlAlias: null,
};
const RELS = { relationships: [REGISTERED], loading: false, refetch: vi.fn() };
const TABLES = {
  tables: [
    {
      id: 1,
      domainId: "shop",
      sourceId: "shop-db",
      schemaName: "shop",
      tableName: "orders",
      alias: null,
      columns: [],
    },
    {
      id: 2,
      domainId: "shop",
      sourceId: "shop-db",
      schemaName: "shop",
      tableName: "customers",
      alias: null,
      columns: [],
    },
  ],
  loading: false,
};
const AUTH = { capabilities: ["create_relationship"] as string[] };
const FILTER = { domainsEnabled: false, domains: [], checkedDomains: new Set<string>() };

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
  fetchSuggestions: vi.fn(),
  applySuggestionDecisions: vi.fn(),
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
  fetchSettings: vi.fn().mockResolvedValue({
    naming: { convention: "apollo_graphql", use_domains: false, default_domain: "" },
  }),
  fetchDiscoveryModels: vi.fn(),
  countDiscoveryRun: vi.fn(),
  startDiscoveryRun: vi.fn(),
  fetchDiscoveryRun: vi.fn(),
  fetchDiscoveryRuns: vi.fn(),
  fetchRunProposals: vi.fn().mockResolvedValue({ proposals: [], threshold: 0.7 }),
}));
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../components/TagControl", () => ({ TagControl: () => null }));

const suggestion = (id: number, column: string, over: Partial<RunProposal> = {}): RunProposal => ({
  id,
  run_id: 3,
  kind: "semantics",
  source_table_id: 1,
  source_column: column,
  target_table_id: 2,
  target_column: "id",
  cardinality: "many-to-one",
  confidence: 0.9,
  scored: null,
  reasoning: `orders name their customer by ${column}`,
  suggested_name: null,
  reason: null,
  answer: null,
  decidable: true,
  reduced_information: false,
  ...over,
});
// A foreign key constraint of the run, a suggestion named by the model, one with no name (its
// relationship takes the name made from its columns) and one found on reduced information.
const WAITING = [
  suggestion(7, "customer_id", {
    kind: "foreign_keys",
    confidence: 1,
    reasoning: "Foreign key constraint",
  }),
  suggestion(8, "buyer_id", { suggested_name: "buyer" }),
  suggestion(9, "payer_id", { reduced_information: true }),
];

const suggestionsMock = vi.mocked(fetchSuggestions);
const applyMock = vi.mocked(applySuggestionDecisions);

beforeEach(() => {
  AUTH.capabilities = ["create_relationship"];
  suggestionsMock.mockReset().mockResolvedValue({ proposals: WAITING, threshold: 0.7 });
  applyMock.mockReset();
  vi.mocked(fetchDiscoveryModels).mockReset();
  vi.mocked(countDiscoveryRun).mockReset();
  vi.mocked(startDiscoveryRun).mockReset();
  vi.mocked(fetchDiscoveryRun).mockReset();
  vi.mocked(fetchDiscoveryRuns).mockReset().mockResolvedValue([]);
});

async function showSuggestions() {
  render(<RelationshipsPage />);
  await waitFor(() => expect(screen.getByTestId("suggestions-apply")).toBeInTheDocument());
}

describe("RelationshipsPage: the suggestions waiting are a decision table", () => {
  it("lists each as its run listed it, every one at the threshold ticked to accept", async () => {
    await showSuggestions();
    const section = screen.getByTestId("rels-suggestions");
    expect(within(section).getByText("Suggested Relationships")).toBeInTheDocument();
    for (const id of [7, 8, 9]) expect(screen.getByTestId(`suggestion-${id}`)).toBeInTheDocument();
    expect(within(screen.getByTestId("suggestion-7")).getByText("Constraint")).toBeInTheDocument();
    expect(within(screen.getByTestId("suggestion-8")).getByText("Semantic")).toBeInTheDocument();
    expect(
      within(screen.getByTestId("suggestion-9")).getByText(
        "Reduced information: no values were compared",
      ),
    ).toBeInTheDocument();
    expect(screen.getByTestId("suggestions-apply")).toHaveTextContent("Accept 3");
  });

  it("offers one way to decide: no Accept all, Reject all or a row's own Accept and Reject", async () => {
    await showSuggestions();
    const section = screen.getByTestId("rels-suggestions");
    for (const name of [/^Accept all$/, /^Reject all$/, /^Accept$/, /^Reject$/]) {
      expect(within(section).queryByRole("button", { name })).toBeNull();
    }
  });

  it("applies the ticks: accepts each under its name, marks the one not to recommend again, and the decided leave", async () => {
    applyMock.mockResolvedValue([
      { id: 7, action: "accept", ok: true, relationship_id: "orders-customer_id-to-customers" },
      { id: 8, action: "accept", ok: true, relationship_id: "buyer" },
      { id: 9, action: "not_again", ok: true },
    ]);
    await showSuggestions();
    fireEvent.click(screen.getByTestId("suggestion-not-again-9"));
    const apply = screen.getByTestId("suggestions-apply");
    expect(apply).toHaveTextContent("Accept 2, don't recommend 1 again");
    suggestionsMock.mockResolvedValue({ proposals: [], threshold: 0.7 });
    fireEvent.click(apply);
    await waitFor(() =>
      expect(applyMock).toHaveBeenCalledWith({
        accept: [
          { id: 7, name: "orders-customer_id-to-customers" },
          { id: 8, name: "buyer" },
        ],
        not_again: [9],
      }),
    );
    // Nothing waits any more: the list leaves the page and the relationships are read again.
    await waitFor(() => expect(screen.queryByTestId("rels-suggestions")).toBeNull());
    expect(RELS.refetch).toHaveBeenCalled();
  });

  it("leaves an unticked suggestion waiting and names one that could not be accepted", async () => {
    applyMock.mockResolvedValue([
      { id: 7, action: "accept", ok: false, error: "A relationship named 'x' already exists" },
    ]);
    await showSuggestions();
    fireEvent.click(
      screen
        .getByTestId("suggestions-proposals")
        .querySelector('[data-testid="suggestion-8"] input[type="checkbox"]') as HTMLElement,
    );
    fireEvent.click(
      screen.getByTestId("suggestion-9").querySelector('input[type="checkbox"]') as HTMLElement,
    );
    suggestionsMock.mockResolvedValue({ proposals: WAITING, threshold: 0.7 });
    fireEvent.click(screen.getByTestId("suggestions-apply"));
    await waitFor(() =>
      expect(applyMock).toHaveBeenCalledWith({
        accept: [{ id: 7, name: "orders-customer_id-to-customers" }],
        not_again: [],
      }),
    );
    const failures = await screen.findByTestId("suggestions-apply-failures");
    expect(failures).toHaveTextContent(
      "orders-customer_id-to-customers: A relationship named 'x' already exists",
    );
  });

  it("is not read for a person who may not create relationships", async () => {
    AUTH.capabilities = [];
    render(<RelationshipsPage />);
    await screen.findByTestId("relationships-split");
    expect(suggestionsMock).not.toHaveBeenCalled();
    expect(screen.queryByTestId("rels-suggestions")).toBeNull();
  });
});

describe("RelationshipsPage: the result banner does not cover the registered relationships", () => {
  const css = readFileSync(resolve(process.cwd(), "src", "App.css"), "utf8");
  const ruleBody = (selector: string): string => {
    const idx = css.indexOf(`\n${selector} {`);
    expect(idx, `no rule for "${selector}" in App.css`).toBeGreaterThan(-1);
    const start = css.indexOf("{", idx);
    return css.slice(start + 1, css.indexOf("}", start)).replace(/\/\*[\s\S]*?\*\//g, "");
  };

  it("keeps the registered list at its own height while the suggestions are shown", () => {
    // The split pane alone may shrink to nothing (flex: 1 1 auto; min-height: 0): squeezed between
    // the banner and a suggestions list at its natural height, it showed only a clipped header.
    const body = ruleBody(".page-sticky-head:has(> .page-aux) > .split-pane");
    expect(body).toMatch(/flex\s*:\s*0 0 auto/);
    expect(ruleBody(".page-sticky-head:has(> .page-aux)")).toMatch(/overflow\s*:\s*auto/);
  });

  it("renders the banner, the registered relationships and the suggestions as the page's items", async () => {
    vi.mocked(fetchDiscoveryModels).mockResolvedValue({
      assigned: {
        vendor: "anthropic",
        model: "test",
        input_tokens: 32000,
        output_tokens: 4096,
        limits_source: "default",
        has_key: true,
      },
      vendors: ["anthropic"],
      semantics_available: true,
    });
    vi.mocked(countDiscoveryRun).mockResolvedValue({
      requests: 0,
      requests_are: "upper_bound",
      by_group: {},
      model: null,
      source_tables: 2,
      target_tables: 2,
      tables_not_read: 0,
    });
    const run = {
      id: 3,
      status: "running" as const,
      scope: {
        kinds: ["foreign_keys" as const],
        source_domains: ["shop"],
        source_tables: null,
        target_same_as_source: true,
        target_domains: [],
        target_tables: null,
        vendor: null,
        model: null,
      },
      model: null,
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
    };
    vi.mocked(startDiscoveryRun).mockResolvedValue(run);
    vi.mocked(fetchDiscoveryRun).mockResolvedValue({
      ...run,
      status: "done",
      suggestions: 3,
      report: {
        suggestions: 3,
        stored_ids: [7, 8, 9],
        removed: {},
        foreign_keys: { found: 3, suggested: 3 },
      },
    });
    await showSuggestions();
    fireEvent.click(screen.getByTestId("rels-discover"));
    await waitFor(() => expect(screen.getByTestId("discover-run")).toBeEnabled());
    fireEvent.click(screen.getByTestId("discover-run"));
    // The dialog is closed; the result stays on the page.
    await screen.findByTestId("discover-progress");
    fireEvent.keyDown(screen.getByTestId("discover-dialog"), { key: "Escape" });
    const banner = await screen.findByTestId("rels-discover-result", {}, { timeout: 4000 });

    const page = banner.parentElement as HTMLElement;
    expect(page).toHaveClass("page-sticky-head");
    const split = screen.getByTestId("relationships-split");
    const suggestions = screen.getByText("Suggested Relationships").closest(".page-aux");
    const items = Array.from(page.children);
    const at = (el: Element | null) => items.indexOf(el as Element);
    expect(at(banner)).toBeGreaterThan(-1);
    expect(at(split.closest(".split-pane"))).toBeGreaterThan(at(banner));
    expect(at(suggestions)).toBeGreaterThan(at(split.closest(".split-pane")));
    // The registered relationship is listed beneath the banner.
    expect(within(split).getByText(/orders/)).toBeInTheDocument();
  });
});
