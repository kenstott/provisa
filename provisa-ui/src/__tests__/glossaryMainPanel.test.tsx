// Copyright (c) 2026 Kenneth Stott
// Canary: eaf70883-69e2-4e11-a325-ac9acd71ab10
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Glossary page's main panel reopens as it was left: the graph, a term read, a term in its
// form, or the stewardship summary. A ?term= link decides its own visit and is not saved over; a
// remembered term since deleted is dropped.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import { getStored, setStored } from "../lib/browserStorage";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

vi.mock("../components/admin/TermDag", () => ({ TermDag: () => null }));
vi.mock("../components/graph3d/GlossaryOntologyView", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../components/graph3d/GlossaryOntologyView")>()),
  GlossaryOntologyView: () => <div data-testid="ontology-view" />,
}));
vi.mock("../components/admin/StewardshipSummary", () => ({
  StewardshipSummary: () => <div data-testid="summary" />,
}));
vi.mock("../components/admin/TermEditForm", () => ({
  TermEditForm: () => <div data-testid="term-edit-form" />,
}));
vi.mock("../components/admin/TermReadView", () => ({
  TermReadView: () => <div data-testid="term-read-view" />,
}));
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

import { fetchGlossaryTerm, GlossaryTermGone, listGlossaryTerms } from "../api/glossary";

const TERM = {
  id: 1,
  name: "Revenue",
  definition: "Money in.",
  status: "approved",
  domains: [],
  refs: [],
  edges_out: [],
  edges_in: [],
  experts: [],
  tags: [],
  pending_on: [],
} as unknown as GlossaryTermDetail;

const open = (entries?: string[]) => render(<GlossaryTab />, { initialEntries: entries });

beforeEach(() => {
  vi.mocked(listGlossaryTerms).mockResolvedValue([
    { id: 1, name: "Revenue", status: "approved" } as GlossaryTermSummary,
  ]);
  vi.mocked(fetchGlossaryTerm).mockReset().mockResolvedValue(TERM);
  auth.capabilities = ["glossary_read", "glossary_rw"];
});

afterEach(() => vi.clearAllMocks());

describe("the Glossary's main panel, after a reload", () => {
  it("is the summary, with nothing saved", async () => {
    open();
    expect(await screen.findByTestId("summary")).toBeInTheDocument();
  });

  it("is the graph", async () => {
    const view = open();
    fireEvent.click(await screen.findByTestId("glossary-ontology-toggle"));
    view.unmount();
    open();
    expect(await screen.findByTestId("ontology-view")).toBeInTheDocument();
  });

  it("is the graph, where a term was open under it", async () => {
    setStored("glossary.term", "1");
    setStored("glossary.view", "ontology");
    open();
    expect(await screen.findByTestId("ontology-view")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("glossary-ontology-toggle"));
    expect(await screen.findByTestId("term-read-view")).toBeInTheDocument();
  });

  it("is the term that was open, read", async () => {
    const view = open();
    fireEvent.click(await screen.findByText("Revenue"));
    expect(await screen.findByTestId("term-read-view")).toBeInTheDocument();
    view.unmount();
    open();
    expect(await screen.findByTestId("term-read-view")).toBeInTheDocument();
    expect(fetchGlossaryTerm).toHaveBeenLastCalledWith(1);
  });

  it("is the term's form", async () => {
    const view = open();
    fireEvent.click(await screen.findByText("Revenue"));
    fireEvent.click(await screen.findByTestId("glossary-edit-btn"));
    expect(await screen.findByTestId("term-edit-form")).toBeInTheDocument();
    view.unmount();
    open();
    expect(await screen.findByTestId("term-edit-form")).toBeInTheDocument();
    // The form holds through the opening of the term, not only its first paint.
    await new Promise((r) => setTimeout(r, 10));
    expect(screen.getByTestId("term-edit-form")).toBeInTheDocument();
  });

  it("is the term's form on a reload of its own URL", async () => {
    setStored("glossary.term", "1");
    setStored("glossary.editing", "true");
    open(["/glossary?term=1"]);
    expect(await screen.findByTestId("term-edit-form")).toBeInTheDocument();
  });

  it("opens the term read for a viewer who may no longer curate", async () => {
    setStored("glossary.term", "1");
    setStored("glossary.editing", "true");
    auth.capabilities = ["glossary_read"];
    open();
    expect(await screen.findByTestId("term-read-view")).toBeInTheDocument();
    expect(screen.queryByTestId("term-edit-form")).not.toBeInTheDocument();
  });

  it("is the summary again once it is chosen", async () => {
    const view = open();
    fireEvent.click(await screen.findByText("Revenue"));
    await screen.findByTestId("term-read-view");
    fireEvent.click(screen.getByTestId("glossary-stewardship-btn"));
    view.unmount();
    open();
    expect(await screen.findByTestId("summary")).toBeInTheDocument();
  });

  it("drops a remembered term since deleted", async () => {
    setStored("glossary.term", "1");
    vi.mocked(fetchGlossaryTerm).mockRejectedValue(new GlossaryTermGone(1));
    open();
    await waitFor(() => expect(getStored("glossary.term")).toBe(""));
    expect(await screen.findByTestId("summary")).toBeInTheDocument();
  });

  it("yields to a ?term= link, and does not save over what was remembered", async () => {
    setStored("glossary.view", "ontology");
    setStored("glossary.term", "5");
    open(["/glossary?term=1"]);
    expect(await screen.findByTestId("term-read-view")).toBeInTheDocument();
    expect(fetchGlossaryTerm).toHaveBeenLastCalledWith(1);
    expect(getStored("glossary.view")).toBe("ontology");
    expect(getStored("glossary.term")).toBe("5");
  });
});
