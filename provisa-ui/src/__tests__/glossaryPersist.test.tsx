// Copyright (c) 2026 Kenneth Stott
// Canary: 3f8c2a91-6d4e-4b7a-9c15-e2a07b5d8f43
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Ontology page reopens as it was left: the view (list or Ontology View), the search and the
// To review filter are saved in the browser's scoped storage and restored on the next visit. A
// deep link (?term=, ?search=) decides that visit instead, and is not saved over what was left.
// The graph's own menus are saved by graphSettings (graphSettings.test.ts).

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "../test-utils/render";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

// jsdom draws no canvas: the term graph is tested on its own (TermDag.test.tsx).
vi.mock("../components/admin/TermDag", () => ({ TermDag: () => null }));
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
  fetchGlossaryFindings: vi.fn(async () => []),
}));

vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

const auth = { capabilities: ["glossary_read", "glossary_rw"], activeOrgId: "acme" };
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));

vi.mock("../api/admin", () => ({ fetchOrgMembers: vi.fn(async () => []) }));

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

// The 3D view itself is not under test: it stands in with the search it is handed.
vi.mock("../components/graph3d/GlossaryOntologyView", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../components/graph3d/GlossaryOntologyView")>()),
  GlossaryOntologyView: ({ search }: { search: string }) => (
    <div data-testid="ontology-view" data-search={search} />
  ),
}));

import { fetchGlossaryTerm, listGlossaryTerms } from "../api/glossary";

const mockList = vi.mocked(listGlossaryTerms);
const mockFetch = vi.mocked(fetchGlossaryTerm);

const LIVE: GlossaryTermSummary = {
  id: 1,
  name: "Revenue",
  definition: "Money in.",
  is_abstract: false,
  deprecated: false,
  ref_count: 1,
  table_count: 0,
  identity_gap: false,
  findings: 0,
  status: "live",
  status_reason: null,
  status_by: null,
  retire_on: null,
  export_excluded: false,
  retired: false,
  live: true,
  grounded: true,
  domains: [],
};
// A term with a finding: what To review keeps.
const FLAGGED: GlossaryTermSummary = { ...LIVE, id: 2, name: "Customer", findings: 1 };

const LIVE_DETAIL: GlossaryTermDetail = {
  ...LIVE,
  refs: [],
  edges_out: [],
  edges_in: [],
  experts: [],
  tags: [],
};

const search = () =>
  screen.getByTestId("glossary-search").querySelector("input") ??
  (screen.getByTestId("glossary-search") as HTMLInputElement);

describe("the Ontology page remembers how it was left", () => {
  beforeEach(() => {
    mockList.mockReset();
    mockList.mockResolvedValue([LIVE, FLAGGED]);
    mockFetch.mockReset();
    mockFetch.mockResolvedValue(LIVE_DETAIL);
    window.localStorage.clear();
  });

  it("reopens in the Ontology View after a reload", async () => {
    const { unmount } = render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    fireEvent.click(screen.getByTestId("glossary-ontology-toggle"));
    expect(screen.getByTestId("ontology-view")).toBeInTheDocument();
    unmount();

    render(<GlossaryTab />);
    expect(screen.getByTestId("ontology-view")).toBeInTheDocument();
    expect(screen.getByTestId("glossary-ontology-toggle")).toHaveAttribute("aria-pressed", "true");
  });

  it("reopens in the list once the Ontology View is closed", async () => {
    const { unmount } = render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    fireEvent.click(screen.getByTestId("glossary-ontology-toggle"));
    fireEvent.click(screen.getByTestId("glossary-ontology-toggle"));
    unmount();

    render(<GlossaryTab />);
    expect(await screen.findByTestId("glossary-item-1")).toBeInTheDocument();
    expect(screen.queryByTestId("ontology-view")).not.toBeInTheDocument();
  });

  it("restores the search", async () => {
    const { unmount } = render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    fireEvent.change(search(), { target: { value: "revenue" } });
    unmount();

    mockList.mockClear();
    render(<GlossaryTab />);
    expect(search()).toHaveValue("revenue");
    await screen.findByTestId("glossary-item-1");
    expect(mockList).toHaveBeenCalledWith("revenue", true, null);
  });

  it("restores To review", async () => {
    const { unmount } = render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    fireEvent.click(screen.getByTestId("glossary-to-review-only"));
    expect(screen.queryByTestId("glossary-item-1")).not.toBeInTheDocument();
    unmount();

    render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-2");
    expect(screen.getByTestId("glossary-to-review-only")).toBeChecked();
    expect(screen.queryByTestId("glossary-item-1")).not.toBeInTheDocument();
  });

  it("a ?term= link opens the list for that visit, and the saved view is kept", async () => {
    const first = render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    fireEvent.click(screen.getByTestId("glossary-ontology-toggle"));
    first.unmount();

    const linked = render(<GlossaryTab />, { initialEntries: ["/glossary?term=1"] });
    expect(await screen.findByTestId("glossary-item-1")).toBeInTheDocument();
    await screen.findByText("Money in.");
    expect(screen.queryByTestId("ontology-view")).not.toBeInTheDocument();
    linked.unmount();

    render(<GlossaryTab />);
    expect(screen.getByTestId("ontology-view")).toBeInTheDocument();
  });

  it("a ?search= link searches for that visit, and the saved search is kept", async () => {
    const first = render(<GlossaryTab />);
    await screen.findByTestId("glossary-item-1");
    fireEvent.change(search(), { target: { value: "revenue" } });
    first.unmount();

    const linked = render(<GlossaryTab />, { initialEntries: ["/glossary?search=customer"] });
    expect(search()).toHaveValue("customer");
    await screen.findByTestId("glossary-item-1");
    linked.unmount();

    render(<GlossaryTab />);
    expect(search()).toHaveValue("revenue");
  });
});
