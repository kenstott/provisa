// Copyright (c) 2026 Kenneth Stott
// Canary: 7a2d9e41-3c58-4b16-a0f7-e8b4c1d65f92
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1591 (amended 2026-11-18): a term's name is unique only within a domain. Where two terms
// of one name are shown together -- the list, a term's relationships -- each carries its domains;
// a name no other shown term holds stays plain.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

// jsdom draws no canvas: the term page's ontology snippet is tested on its own (TermDag.test.tsx).
vi.mock("../components/graph3d/OntologyGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../components/graph3d/OntologyGraph")>()),
  OntologySnippet: () => null,
}));
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  // REQ-1592: the author/expert picker's roster.
  listAttributablePeople: vi.fn(async () => []),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
  generateGlossaryRelationships: vi.fn(),
  suggestResolvingTerms: vi.fn(),
  createGlossaryTerm: vi.fn(),
  addGlossaryEdge: vi.fn(),
  listTermTables: vi.fn(async () => []),
  fetchGlossaryFindings: vi.fn(async () => []),
}));
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useTables: () => ({ tables: [], loading: false, error: undefined, refetch: vi.fn() }),
  useRelationships: () => ({
    relationships: [],
    loading: false,
    error: undefined,
    refetch: vi.fn(),
  }),
}));
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));
vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    capabilities: ["glossary_read", "glossary_rw"],
    activeOrgId: "acme",
    actingRoles: "org_admin",
  }),
}));
// Single-domain mode: a new term declares no domains.
const filter = {
  domains: [],
  setDomains: () => {},
  selectedDomain: "all",
  setSelectedDomain: () => {},
  checkedDomains: new Set<string>(),
  toggleDomain: () => {},
  ensureDomainChecked: () => {},
  domainsEnabled: false,
};
vi.mock("../context/DomainFilterContext", () => ({ useDomainFilter: () => filter }));

import { fetchGlossaryTerm, listGlossaryTerms } from "../api/glossary";

const mockList = vi.mocked(listGlossaryTerms);
const mockFetchTerm = vi.mocked(fetchGlossaryTerm);

const REVENUE: GlossaryTermSummary = {
  id: 1,
  name: "Revenue",
  definition: "Money in.",
  calculation_method: null,
  is_abstract: false,
  deprecated: false,
  ref_count: 1,
  table_count: 0,
  identity_gap: false,
  related_to: 0,
  findings: 0,
  status: "approved",
  status_reason: null,
  status_by: null,
  retire_on: null,
  export_excluded: false,
  retired: false,
  grounding: "grounded",
  agent_visible: true,
  domains: [],
};
const detailOf = (term: GlossaryTermSummary): GlossaryTermDetail => ({
  ...term,
  refs: [],
  edges_out: [],
  edges_in: [],
  experts: [],
  tags: [],
  pending_on: [],
  curatable: true,
  iri: null,
});
const REVENUE_DETAIL: GlossaryTermDetail = {
  ...detailOf(REVENUE),
  edges_out: [{ term_id: 2, rel_type: "RELATED_TO", name: "Churn", domains: [] }],
};

const CLIENT_SALES: GlossaryTermSummary = { ...REVENUE, id: 7, name: "client", domains: ["sales"] };
const CLIENT_PETS: GlossaryTermSummary = {
  ...REVENUE,
  id: 8,
  name: "client",
  domains: ["pet-store"],
};

describe("same-named terms are told apart by domain", () => {
  beforeEach(() => {
    window.localStorage.clear();
    mockList.mockReset();
    mockFetchTerm.mockReset();
  });

  it("labels the list's colliding names with their domains and leaves a unique one plain", async () => {
    mockList.mockResolvedValue([CLIENT_SALES, CLIENT_PETS, REVENUE]);
    mockFetchTerm.mockResolvedValue(REVENUE_DETAIL);
    render(<GlossaryTab />);
    expect(await screen.findByTestId("glossary-item-7")).toHaveTextContent("client (sales)");
    expect(screen.getByTestId("glossary-item-8")).toHaveTextContent("client (pet-store)");
    expect(screen.getByTestId("glossary-item-1")).toHaveTextContent("Revenue");
    expect(screen.getByTestId("glossary-item-1")).not.toHaveTextContent("Revenue (");
  });

  it("labels a term's related terms that share a name with their domains", async () => {
    mockList.mockResolvedValue([REVENUE, CLIENT_SALES, CLIENT_PETS]);
    mockFetchTerm.mockResolvedValue({
      ...REVENUE_DETAIL,
      edges_out: [
        { term_id: 7, rel_type: "RELATED_TO", name: "client", domains: ["sales"] },
        { term_id: 2, rel_type: "PART_OF", name: "Churn", domains: [] },
      ],
      edges_in: [{ term_id: 8, rel_type: "KIND_OF", name: "client", domains: ["pet-store"] }],
    });
    render(<GlossaryTab />);
    fireEvent.click(await screen.findByTestId("glossary-item-1"));
    await waitFor(() =>
      expect(screen.getByTestId("glossary-edge-out-link-7")).toHaveTextContent("client (sales)"),
    );
    expect(screen.getByTestId("glossary-edge-out-link-2")).toHaveTextContent("Churn");
    expect(screen.getByTestId("glossary-edge-out-link-2")).not.toHaveTextContent("Churn (");
    expect(screen.getByTestId("glossary-edge-in-8-KIND_OF")).toHaveTextContent(
      "client (pet-store)",
    );
  });
});
