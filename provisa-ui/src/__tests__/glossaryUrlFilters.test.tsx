// Copyright (c) 2026 Kenneth Stott
// Canary: 134d8715-7eef-477f-87d3-ac6e29de641f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259, REQ-2277: the Ontology page's term list filters are named in its URL -- visibility to
// agents, status (approval), grounding (reaching data) and To review -- so each Terms count on
// Context Health opens the list showing exactly the terms it counts. Mocks as
// glossaryRelatedToParam.test.tsx.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "../test-utils/render";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermSummary } from "../api/glossary";

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

import { listGlossaryTerms } from "../api/glossary";
import { storageKey as scopedKey } from "../lib/browserStorage";

const mockList = vi.mocked(listGlossaryTerms);

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
const term = (
  id: number,
  status: GlossaryTermSummary["status"],
  grounding: GlossaryTermSummary["grounding"],
  extra: Partial<GlossaryTermSummary> = {},
): GlossaryTermSummary => ({
  ...REVENUE,
  id,
  name: `t${id}`,
  status,
  grounding,
  agent_visible: false,
  ...extra,
});

// The terms Context Health's test counts: 3 visible to agents, 2 approved reaching data, 1
// approved reaching none, 3 drafts.
const TERMS = [
  term(1, "approved", "grounded", { agent_visible: true }),
  term(2, "approved", "grounded", { agent_visible: true }),
  term(3, "approved", "not_grounded"),
  term(4, "draft", "grounded_pending", { agent_visible: true, findings: 1 }),
  term(5, "draft", "not_grounded"),
  term(6, "draft", "not_grounded"),
  term(7, "deprecated", "grounded"),
];

const listed = () =>
  TERMS.map((t) => t.id).filter((id) => screen.queryByTestId(`glossary-item-${id}`) !== null);

async function open(url: string) {
  render(<GlossaryTab />, { initialEntries: [url] });
  await screen.findByTestId("glossary-list");
  await waitFor(() => expect(listed().length).toBeGreaterThan(0));
}

describe("the term list filtered by its URL", () => {
  beforeEach(() => {
    window.localStorage.clear();
    mockList.mockReset();
    mockList.mockResolvedValue(TERMS);
  });

  it.each([
    ["/context/ontology?panel=terms&agentVisible=1", [1, 2, 4]],
    ["/context/ontology?panel=terms&status=approved&grounding=grounded", [1, 2]],
    ["/context/ontology?panel=terms&status=approved&grounding=not_grounded", [3]],
    ["/context/ontology?panel=terms&status=draft", [4, 5, 6]],
    ["/context/ontology?panel=terms&toReview=1", [4]],
  ])("%s lists exactly %j", async (url, ids) => {
    await open(url);
    await waitFor(() => expect(listed()).toEqual(ids));
  });

  it("shows the filters the link names", async () => {
    await open("/context/ontology?panel=terms&agentVisible=1&toReview=1");
    expect(screen.getByTestId("glossary-agent-visible-only")).toBeChecked();
    expect(screen.getByTestId("glossary-to-review-only")).toBeChecked();
    expect(screen.getByTestId("glossary-related-to-only")).not.toBeChecked();
  });

  it("decides every filter that visit: the ones left from before do not narrow it", async () => {
    window.localStorage.setItem(scopedKey("glossary.status"), "retired");
    window.localStorage.setItem(scopedKey("glossary.toReview"), "true");
    window.localStorage.setItem(scopedKey("glossary.search"), "nothing");
    await open("/context/ontology?panel=terms&status=draft");
    await waitFor(() => expect(listed()).toEqual([4, 5, 6]));
    expect(mockList).toHaveBeenLastCalledWith("", true, null);
  });

  it("opens the list, not the graph left open before", async () => {
    window.localStorage.setItem(scopedKey("glossary.view"), "ontology");
    await open("/context/ontology?panel=terms&agentVisible=1");
    await waitFor(() => expect(listed()).toEqual([1, 2, 4]));
  });

  it("narrows to the terms agents see from the list's own filter", async () => {
    await open("/context/ontology");
    await waitFor(() => expect(listed()).toEqual([1, 2, 3, 4, 5, 6, 7]));
    fireEvent.click(screen.getByTestId("glossary-agent-visible-only"));
    await waitFor(() => expect(listed()).toEqual([1, 2, 4]));
  });

  it("without a filter in the link, reopens with the filters left", async () => {
    window.localStorage.setItem(scopedKey("glossary.agentVisible"), "true");
    await open("/context/ontology");
    await waitFor(() => expect(listed()).toEqual([1, 2, 4]));
  });
});
