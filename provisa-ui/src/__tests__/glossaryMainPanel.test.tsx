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
import { TEST_VIEWER } from "../test-setup";
import { readScreen } from "../mcp/screenState";
import { runUiActions } from "../mcp/uiActions";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

// jsdom draws no canvas: the term page's ontology snippet is tested on its own (TermDag.test.tsx).
vi.mock("../components/graph3d/OntologyGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../components/graph3d/OntologyGraph")>()),
  OntologySnippet: () => null,
  OntologyView: () => <div data-testid="ontology-view" />,
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
vi.mock("../components/admin/FindTermsDialog", () => ({
  FindTermsDialog: () => <div data-testid="find-terms-dialog" />,
}));
vi.mock("../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/glossary")>()),
  // REQ-1592: the author/expert picker's roster.
  listAttributablePeople: vi.fn(async () => []),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
  fetchGlossaryFindings: vi.fn(async () => []),
}));
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));
const auth = { capabilities: ["glossary_read", "glossary_rw"], activeOrgId: "acme" };
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));
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
  curatable: true,
} as unknown as GlossaryTermDetail;

// REQ-2260: the term left open, remembered for the viewer as every list page's item is.
const TERM_LEFT_OPEN = `detail.${TEST_VIEWER}.ontology`;

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
    setStored(TERM_LEFT_OPEN, "1");
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
    setStored(TERM_LEFT_OPEN, "1");
    setStored("glossary.editing", "true");
    open(["/glossary?term=1"]);
    expect(await screen.findByTestId("term-edit-form")).toBeInTheDocument();
  });

  it("opens the term read for a viewer who may no longer curate", async () => {
    setStored(TERM_LEFT_OPEN, "1");
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
    setStored(TERM_LEFT_OPEN, "1");
    vi.mocked(fetchGlossaryTerm).mockRejectedValue(new GlossaryTermGone(1));
    open();
    await waitFor(() => expect(getStored(TERM_LEFT_OPEN)).toBeNull());
    expect(await screen.findByTestId("summary")).toBeInTheDocument();
  });

  // REQ-2260: the term a link opens is the term left open from then; the view is not saved over.
  it("yields to a ?term= link, and does not save over the view remembered", async () => {
    setStored("glossary.view", "ontology");
    setStored(TERM_LEFT_OPEN, "5");
    open(["/glossary?term=1"]);
    expect(await screen.findByTestId("term-read-view")).toBeInTheDocument();
    expect(fetchGlossaryTerm).toHaveBeenLastCalledWith(1);
    expect(getStored("glossary.view")).toBe("ontology");
    expect(getStored(TERM_LEFT_OPEN)).toBe("1");
  });

  it("does not reopen a remembered term the viewer can no longer read, and forgets it", async () => {
    setStored(TERM_LEFT_OPEN, "7");
    open();
    expect(await screen.findByTestId("summary")).toBeInTheDocument();
    await waitFor(() => expect(getStored(TERM_LEFT_OPEN)).toBeNull());
    expect(fetchGlossaryTerm).not.toHaveBeenCalled();
  });
});

// REQ-2115: Polly opens Find terms from the glossary's menu, and read_screen says it is open.
describe("Polly on the glossary", () => {
  it("opens Find terms with her steps and reads that it is open", async () => {
    HTMLElement.prototype.checkVisibility = () => true;
    window.history.pushState({}, "", "/context/ontology");
    const inputs = async () => {
      const read = await readScreen(500);
      if (typeof read === "string") throw new Error(read);
      return read.inputs;
    };
    open();
    await screen.findByTestId("summary");
    expect(await inputs()).toMatchObject({ findTerms: false });
    const out = await runUiActions(
      [
        { do: "press", target: { testId: "glossary-generate-menu" } },
        { do: "press", target: { testId: "glossary-find-terms-btn" } },
      ],
      { confirm: vi.fn(), paceMs: 0, waitMs: 500 },
    );
    expect(out.success).toBe(true);
    expect(await screen.findByTestId("find-terms-dialog")).toBeInTheDocument();
    expect(await inputs()).toMatchObject({ findTerms: true });
  });
});
