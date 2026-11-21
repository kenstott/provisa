// Copyright (c) 2026 Kenneth Stott
// Canary: 9a3d6f20-4b7e-4c18-b5e2-1f8c0a7d3e95
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413 (amended 2026-11-16): the Relationships page lists registered relationships only. It has
// no Suggested Relationships section: no list with Accept all and Reject all or a row's own Accept
// and Reject, and no decision table. A suggestion is decided only in a Discover run's result, and
// the page never reads the suggestions waiting.

import { describe, it, expect, vi, beforeEach, afterEach, type MockInstance } from "vitest";
import { screen, fireEvent, waitFor, within } from "@testing-library/react";
import { render } from "../../test-utils/render";
import { RelationshipsPage } from "../RelationshipsPage";
import {
  startDiscoveryRun,
  fetchDiscoveryRun,
  fetchDiscoveryRuns,
  fetchDiscoveryModels,
  countDiscoveryRun,
  fetchRunProposals,
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
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
  fetchSettings: vi.fn().mockResolvedValue({
    naming: { convention: "apollo_graphql", use_domains: false, default_domain: "" },
  }),
  fetchDiscoveryModels: vi.fn(),
  countDiscoveryRun: vi.fn(),
  startDiscoveryRun: vi.fn(),
  fetchDiscoveryRun: vi.fn(),
  fetchDiscoveryRuns: vi.fn(),
  fetchRunProposals: vi
    .fn()
    .mockResolvedValue({ proposals: [], threshold: 0.7, kept_at: 0.7, score_filter: null }),
  // REQ-413: the run settings of a person who has set none; the form waits for them.
  fetchDiscoverySettings: vi.fn().mockImplementation(async () => {
    const defaults = {
      threshold: 0.7,
      samples_per_column: 5,
      key_values: {
        chosen: true,
        cutoff: 0.2,
        sample_size: 10000,
        contained_at: 0.95,
        shared_ceiling: 0.6,
      },
      score_filter: { op: "ge", a: "10" },
    };
    return { ...defaults, defaults };
  }),
}));
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../components/TagControl", () => ({ TagControl: () => null }));

let fetchSpy: MockInstance<typeof fetch>;

beforeEach(() => {
  AUTH.capabilities = ["create_relationship"];
  vi.mocked(fetchDiscoveryModels).mockReset();
  vi.mocked(countDiscoveryRun).mockReset();
  vi.mocked(startDiscoveryRun).mockReset();
  vi.mocked(fetchDiscoveryRun).mockReset();
  vi.mocked(fetchDiscoveryRuns).mockReset().mockResolvedValue([]);
  fetchSpy = vi.spyOn(globalThis, "fetch");
});

afterEach(() => {
  fetchSpy.mockRestore();
});

/** Every URL the page asked the server for, other than through the mocked API calls. */
const asked = (): string[] => fetchSpy.mock.calls.map(([url]) => String(url));

const NO_SECTION = [
  /^Accept all$/,
  /^Reject all$/,
  /^Accept$/,
  /^Reject$/,
  /^Apply$/,
  /^Accept \d/,
];

function expectNoSuggestedSection() {
  expect(screen.queryByText("Suggested Relationships")).toBeNull();
  expect(screen.queryByTestId("rels-suggestions")).toBeNull();
  expect(screen.queryByTestId("suggestions-proposals")).toBeNull();
  const page = screen.getByTestId("relationships-split").closest(".page") as HTMLElement;
  for (const name of NO_SECTION) expect(within(page).queryByRole("button", { name })).toBeNull();
  expect(asked().filter((u) => /\/admin\/discover\/(candidates$|suggestions)/.test(u))).toEqual([]);
}

describe("RelationshipsPage: no suggested section", () => {
  it("has none for a person who may create relationships, and reads no suggestions", async () => {
    render(<RelationshipsPage />);
    await screen.findByTestId("relationships-split");
    await waitFor(() => expect(vi.mocked(fetchDiscoveryRuns)).toHaveBeenCalled());
    expectNoSuggestedSection();
  });

  it("has none for a person who may not create relationships", async () => {
    AUTH.capabilities = [];
    render(<RelationshipsPage />);
    await screen.findByTestId("relationships-split");
    expectNoSuggestedSection();
  });
});

describe("RelationshipsPage: a run's result leaves the page without a suggested section", () => {
  it("renders the run's one line above the registered relationships and no suggestions after the run", async () => {
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
      table_pairs: 0,
      table_pairs_per_request: 27,
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
        threshold: 0.7,
        samples_per_column: 5,
        measure_cutoff: null,
        sample_size: null,
        contained_at: null,
        shared_ceiling: null,
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
    // The run's three suggestions wait for a decision in its result.
    const waiting = [7, 8, 9].map((id) => ({ id, decidable: true, reason: null }) as RunProposal);
    vi.mocked(fetchRunProposals).mockResolvedValue({
      proposals: waiting,
      threshold: 0.7,
      kept_at: 0.7,
      score_filter: null,
    });
    render(<RelationshipsPage />);
    await screen.findByTestId("relationships-split");
    fireEvent.click(screen.getByTestId("rels-discover"));
    await waitFor(() => expect(screen.getByTestId("discover-run")).toBeEnabled());
    fireEvent.click(screen.getByTestId("discover-run"));
    // The dialog is closed; the page keeps one line about the run (REQ-413, amended 2026-11-16),
    // never its summary.
    await screen.findByTestId("discover-progress");
    fireEvent.keyDown(screen.getByTestId("discover-dialog"), { key: "Escape" });
    // Going when the dialog closed; read about a second later, it has ended, and the line
    // comes back once its suggestions waiting are counted.
    await waitFor(
      () =>
        expect(screen.getByTestId("rels-discover-line")).toHaveTextContent(
          "Finished. 3 suggestions wait for a decision.",
        ),
      { timeout: 4000 },
    );
    const banner = screen.getByTestId("rels-discover-line");
    expect(screen.queryByTestId("rels-discover-result")).toBeNull();

    const page = banner.parentElement as HTMLElement;
    expect(page).toHaveClass("page-sticky-head");
    const split = screen.getByTestId("relationships-split");
    const items = Array.from(page.children);
    const at = (el: Element | null) => items.indexOf(el as Element);
    expect(at(banner)).toBeGreaterThan(-1);
    expect(at(split.closest(".split-pane"))).toBeGreaterThan(at(banner));
    // The registered relationship is listed beneath the line.
    expect(within(split).getByText(/orders/)).toBeInTheDocument();
    // The run stored three suggestions: they are decided in its result, never listed here.
    await waitFor(() => expect(vi.mocked(fetchDiscoveryRun)).toHaveBeenCalled());
    expectNoSuggestedSection();
  });
});
