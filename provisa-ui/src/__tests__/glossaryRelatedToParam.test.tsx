// Copyright (c) 2026 Kenneth Stott
// Canary: 8a2c6e14-5b93-4f7d-a1c8-3e9b7d2f5a06
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1386 (ONTOLOGY QUALITY IS A STANDARD REPORT): a placeholder row of the Ontology quality
// report opens the Ontology page at its term with the list filtered to the terms on a Related to
// edge (?relatedTo=1). Mocks as glossaryRelatedTo.test.tsx.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "../test-utils/render";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

// jsdom draws no canvas: the term graph is tested on its own (TermDag.test.tsx).
vi.mock("../components/admin/TermDag", () => ({ TermDag: () => null }));
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
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
vi.mock("../api/admin", () => ({ fetchOrgMembers: vi.fn(async () => []) }));
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
const CHURN: GlossaryTermSummary = { ...REVENUE, id: 2, name: "Churn", ref_count: 0 };
const detailOf = (term: GlossaryTermSummary): GlossaryTermDetail => ({
  ...term,
  refs: [],
  edges_out: [],
  edges_in: [],
  experts: [],
  tags: [],
  pending_on: [],
});
const REVENUE_DETAIL: GlossaryTermDetail = {
  ...detailOf(REVENUE),
  edges_out: [{ term_id: 2, rel_type: "RELATED_TO", name: "Churn" }],
};
const CHURN_DETAIL: GlossaryTermDetail = {
  ...detailOf(CHURN),
  edges_in: [{ term_id: 1, rel_type: "RELATED_TO", name: "Revenue" }],
};

describe("the RELATED_TO review link", () => {
  beforeEach(() => {
    window.localStorage.clear();
    mockList.mockReset();
    mockList.mockResolvedValue([{ ...REVENUE, related_to: 1 }, CHURN]);
    mockFetchTerm.mockReset();
    mockFetchTerm.mockImplementation(async (id) => (id === 2 ? CHURN_DETAIL : REVENUE_DETAIL));
  });

  it("opens the list filtered to the terms on a Related to edge", async () => {
    render(<GlossaryTab />, { initialEntries: ["/admin/ontology?term=1&relatedTo=1"] });
    await screen.findByTestId("glossary-item-1");
    expect(screen.queryByTestId("glossary-item-2")).toBeNull();
    expect(screen.getByTestId("glossary-related-to-only")).toBeChecked();
  });

  it("without it, the list is not filtered", async () => {
    render(<GlossaryTab />, { initialEntries: ["/admin/ontology?term=1"] });
    await screen.findByTestId("glossary-item-2");
    expect(screen.getByTestId("glossary-related-to-only")).not.toBeChecked();
  });
});
