// Copyright (c) 2026 Kenneth Stott
// Canary: 9a3d6f20-4b7e-4c18-b5e2-1f8c0a7d3e95
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: the suggestions list is accepted or rejected as a whole, each after a confirmation that
// names the count, and a suggestion that fails is named; the run's result never covers the list of
// registered relationships.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { screen, fireEvent, waitFor, within } from "@testing-library/react";
import { render } from "../../test-utils/render";
import { RelationshipsPage } from "../RelationshipsPage";
import {
  acceptCandidates,
  startDiscoveryRun,
  fetchDiscoveryRun,
  fetchDiscoveryRuns,
  fetchDiscoveryModels,
  countDiscoveryRun,
  fetchCandidates,
  rejectCandidate,
  rejectCandidates,
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
const AUTH = { capabilities: ["create_relationship"] };
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
  fetchCandidates: vi.fn(),
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
  acceptCandidates: vi.fn(),
  rejectCandidates: vi.fn(),
  rejectCandidate: vi.fn(),
}));
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../components/TagControl", () => ({ TagControl: () => null }));

const candidate = (id: number, column: string, name: string | null) => ({
  id,
  source_table_id: 1,
  source_column: column,
  target_table_id: 2,
  target_column: "id",
  cardinality: "many_to_one",
  confidence: 0.9,
  reasoning: "Foreign key constraint",
  suggested_name: name,
});
// The third has no suggested name: Accept all gives it the name its row's Accept would.
const SUGGESTED = [
  candidate(7, "customer_id", "purchase-history"),
  candidate(8, "buyer_id", "buyer"),
  candidate(9, "payer_id", null),
];

const candidatesMock = vi.mocked(fetchCandidates);
const acceptAllMock = vi.mocked(acceptCandidates);
const rejectAllMock = vi.mocked(rejectCandidates);
const rejectOneMock = vi.mocked(rejectCandidate);

beforeEach(() => {
  candidatesMock.mockReset().mockResolvedValue(SUGGESTED);
  acceptAllMock.mockReset();
  rejectAllMock.mockReset();
  rejectOneMock.mockReset().mockResolvedValue(undefined);
  vi.mocked(fetchDiscoveryModels).mockReset();
  vi.mocked(countDiscoveryRun).mockReset();
  vi.mocked(startDiscoveryRun).mockReset();
  vi.mocked(fetchDiscoveryRun).mockReset();
  vi.mocked(fetchDiscoveryRuns).mockReset().mockResolvedValue([]);
});

async function showSuggestions() {
  render(<RelationshipsPage />);
  await waitFor(() => expect(screen.getByTestId("candidates-accept-all")).toBeInTheDocument());
}

describe("RelationshipsPage: Accept all and Reject all", () => {
  it("puts both actions at the head of the suggestions list", async () => {
    await showSuggestions();
    const heading = screen.getByText("Suggested Relationships");
    const head = heading.parentElement as HTMLElement;
    expect(within(head).getByTestId("candidates-accept-all")).toHaveTextContent("Accept all");
    expect(within(head).getByTestId("candidates-reject-all")).toHaveTextContent("Reject all");
  });

  it("asks before accepting, naming the count, and sends nothing on Cancel", async () => {
    await showSuggestions();
    fireEvent.click(screen.getByTestId("candidates-accept-all"));
    const ask = await screen.findByRole("dialog");
    expect(ask).toHaveTextContent(
      "Accept all 3 listed suggestions? Each becomes a registered relationship.",
    );
    // Accepting destroys nothing: the confirmation's non-destructive form.
    expect(within(ask).getByTestId("candidates-accept-all-confirm-confirm")).toHaveAttribute(
      "data-tone",
      "confirm",
    );
    fireEvent.click(within(ask).getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(acceptAllMock).not.toHaveBeenCalled();
  });

  it("accepts every suggestion in one request, each under its row's name", async () => {
    acceptAllMock.mockResolvedValue([
      { id: 7, ok: true, relationship_id: "purchase-history" },
      { id: 8, ok: true, relationship_id: "buyer" },
      { id: 9, ok: true, relationship_id: "orders-payer_id-to-customers" },
    ]);
    await showSuggestions();
    fireEvent.click(screen.getByTestId("candidates-accept-all"));
    fireEvent.click(await screen.findByTestId("candidates-accept-all-confirm-confirm"));
    await waitFor(() => expect(acceptAllMock).toHaveBeenCalledTimes(1));
    expect(acceptAllMock).toHaveBeenCalledWith([
      { id: 7, name: "purchase-history" },
      { id: 8, name: "buyer" },
      { id: 9, name: "orders-payer_id-to-customers" },
    ]);
    await waitFor(() =>
      expect(screen.getByTestId("rels-discover-msg")).toHaveTextContent("3 suggestions accepted."),
    );
    expect(screen.queryByTestId("rels-bulk-failures")).not.toBeInTheDocument();
  });

  it("names each suggestion that could not be accepted, with its reason", async () => {
    acceptAllMock.mockResolvedValue([
      { id: 7, ok: true, relationship_id: "purchase-history" },
      { id: 8, ok: false, error: "relationship buyer already exists" },
      { id: 9, ok: true, relationship_id: "orders-payer_id-to-customers" },
    ]);
    await showSuggestions();
    fireEvent.click(screen.getByTestId("candidates-accept-all"));
    fireEvent.click(await screen.findByTestId("candidates-accept-all-confirm-confirm"));
    const failures = await screen.findByTestId("rels-bulk-failures");
    expect(failures).toHaveTextContent("1 suggestion could not be accepted:");
    expect(failures).toHaveTextContent("buyer: relationship buyer already exists");
    expect(screen.getByTestId("rels-discover-msg")).toHaveTextContent("2 suggestions accepted.");
  });

  it("rejects every suggestion in one request with the reason a row's Reject gives", async () => {
    rejectAllMock.mockResolvedValue([
      { id: 7, ok: true },
      { id: 8, ok: true },
      { id: 9, ok: true },
    ]);
    await showSuggestions();
    // The reason a single row's Reject sends.
    fireEvent.click(screen.getByRole("button", { name: "Reject suggested relationship buyer" }));
    await waitFor(() => expect(rejectOneMock).toHaveBeenCalled());
    const rowReason = rejectOneMock.mock.calls[0][1];

    fireEvent.click(screen.getByTestId("candidates-reject-all"));
    const ask = await screen.findByRole("dialog");
    expect(ask).toHaveTextContent(
      "Reject all 2 listed suggestions? They are not suggested again until rejections are cleared.",
    );
    fireEvent.click(within(ask).getByTestId("candidates-reject-all-confirm-confirm"));
    await waitFor(() => expect(rejectAllMock).toHaveBeenCalledTimes(1));
    expect(rejectAllMock).toHaveBeenCalledWith([7, 9], rowReason);
    await waitFor(() =>
      expect(screen.queryByText("Suggested Relationships")).not.toBeInTheDocument(),
    );
  });

  it("keeps a suggestion that could not be rejected listed, and names it", async () => {
    rejectAllMock.mockResolvedValue([
      { id: 7, ok: true },
      { id: 8, ok: false, error: "Candidate 8 not found or not in suggested status" },
      { id: 9, ok: true },
    ]);
    await showSuggestions();
    fireEvent.click(screen.getByTestId("candidates-reject-all"));
    fireEvent.click(await screen.findByTestId("candidates-reject-all-confirm-confirm"));
    const failures = await screen.findByTestId("rels-bulk-failures");
    expect(failures).toHaveTextContent("1 suggestion could not be rejected:");
    expect(failures).toHaveTextContent("buyer: Candidate 8 not found or not in suggested status");
    expect(
      screen.getByRole("button", { name: "Accept suggested relationship buyer" }),
    ).toBeVisible();
    expect(
      screen.queryByRole("button", { name: "Accept suggested relationship purchase-history" }),
    ).not.toBeInTheDocument();
  });

  it("shows a request that failed as a whole as an error", async () => {
    acceptAllMock.mockRejectedValue(new Error("Accept all failed: HTTP 500"));
    await showSuggestions();
    fireEvent.click(screen.getByTestId("candidates-accept-all"));
    fireEvent.click(await screen.findByTestId("candidates-accept-all-confirm-confirm"));
    await waitFor(() =>
      expect(screen.getByTestId("rels-discover-error")).toHaveTextContent(
        "Accept all failed: HTTP 500",
      ),
    );
  });
});

// jsdom computes no layout, so what is checked is the arrangement the layout depends on: the
// result banner, the registered relationships and the suggestions are each the page column's own
// item, and with the suggestions shown the registered list keeps its height (the page scrolls).
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
