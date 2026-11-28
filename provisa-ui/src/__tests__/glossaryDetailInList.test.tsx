// Copyright (c) 2026 Kenneth Stott
// Canary: 9ba53b49-9865-4073-bfb4-eb21551580ff
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 9, the ontology: with the Domains selector on Ops, a link to a term of another domain
// (/context/ontology?term=2, clinic in pet-store) opened its detail beside a list saying "No terms."
// The detail shows a term only while the list shows its row; the page says once above the list
// which term the link named and why it is not listed, and adding its domain brings it in.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import { forgetDetailForEveryone } from "../components/list/useDetailParam";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

vi.mock("../components/graph3d/OntologyGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../components/graph3d/OntologyGraph")>()),
  OntologySnippet: () => null,
  OntologyView: () => <div data-testid="ontology-view" />,
}));
vi.mock("../components/admin/TermReadView", () => ({
  TermReadView: () => <div data-testid="term-read-view" />,
}));
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  listAttributablePeople: vi.fn(async () => []),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
  fetchGlossaryFindings: vi.fn(async () => []),
}));
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));
const auth = { capabilities: ["glossary_read"], activeOrgId: "acme" };
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));
const filter = {
  domains: ["ops", "pet-store", "sales"],
  setDomains: () => {},
  selectedDomain: "all",
  setSelectedDomain: () => {},
  checkedDomains: new Set<string>(["ops"]),
  toggleDomain: () => {},
  ensureDomainChecked: vi.fn((id: string) => {
    filter.checkedDomains = new Set([...filter.checkedDomains, id]);
  }),
  domainsEnabled: true,
  domainsPending: false,
};
vi.mock("../context/DomainFilterContext", () => ({ useDomainFilter: () => filter }));

import { fetchGlossaryTerm, listGlossaryTerms } from "../api/glossary";

const summary = (id: number, name: string, domains: string[]) =>
  ({
    id,
    name,
    status: "approved",
    grounding: "grounded",
    related_to: 0,
    findings: 0,
    domains,
  }) as unknown as GlossaryTermSummary;
const CLINIC = summary(2, "clinic", ["pet-store"]);
const RUNBOOK = summary(5, "runbook", ["ops"]);
const ALL = [CLINIC, RUNBOOK];

const detailOf = (term: GlossaryTermSummary) =>
  ({
    ...term,
    definition: "",
    refs: [],
    edges_out: [],
    edges_in: [],
    experts: [],
    tags: [],
    pending_on: [],
    curatable: false,
  }) as unknown as GlossaryTermDetail;

beforeEach(() => {
  filter.checkedDomains = new Set(["ops"]);
  // The server narrows the list to the domains asked for, and to the search.
  vi.mocked(listGlossaryTerms).mockImplementation(async (q, _all, domains) =>
    ALL.filter(
      (term) =>
        (!domains || term.domains.some((d) => domains.includes(d))) &&
        term.name.includes(q),
    ),
  );
  vi.mocked(fetchGlossaryTerm).mockImplementation(async (id) =>
    detailOf(ALL.find((term) => term.id === id)!),
  );
});

afterEach(() => {
  vi.clearAllMocks();
  forgetDetailForEveryone("ontology");
});

describe("the ontology shows a term's detail only while its list shows the term", () => {
  it("opens no detail for a linked term outside the domains in view, and says why", async () => {
    render(<GlossaryTab />, { initialEntries: ["/context/ontology?term=2"] });
    expect(await screen.findByTestId("glossary-item-5")).toBeInTheDocument();
    const notice = await screen.findByTestId("detail-out-of-list");
    expect(notice).toHaveTextContent("clinic is not in the list");
    expect(notice).toHaveTextContent("its domain (pet-store) is not in view");
    expect(screen.queryByTestId("glossary-item-2")).toBeNull();
    expect(screen.queryByTestId("term-read-view")).toBeNull();
    expect(screen.getByTestId("glossary-no-term")).toBeInTheDocument();
    expect(fetchGlossaryTerm).not.toHaveBeenCalled();
  });

  it("adds the term's domain to the view, and then shows it", async () => {
    const view = render(<GlossaryTab />, { initialEntries: ["/context/ontology?term=2"] });
    fireEvent.click(await screen.findByTestId("detail-out-of-list-show"));
    expect(filter.ensureDomainChecked).toHaveBeenCalledWith("pet-store");
    // The navbar's selection is the provider's: re-rendered with it, the list reads the domain.
    view.rerender(<GlossaryTab />);
    expect(await screen.findByTestId("glossary-item-2")).toBeInTheDocument();
    expect(await screen.findByTestId("term-read-view")).toBeInTheDocument();
    expect(screen.queryByTestId("detail-out-of-list")).toBeNull();
  });

  it("closes the open term when the search leaves it out of the list", async () => {
    render(<GlossaryTab />, { initialEntries: ["/context/ontology?term=5"] });
    expect(await screen.findByTestId("term-read-view")).toBeInTheDocument();
    fireEvent.change(screen.getByTestId("glossary-search"), { target: { value: "zzz" } });
    await waitFor(() => expect(screen.queryByTestId("term-read-view")).toBeNull());
    expect(screen.getByTestId("glossary-no-term")).toBeInTheDocument();
    expect(screen.queryByTestId("detail-out-of-list")).toBeNull();
  });
});
