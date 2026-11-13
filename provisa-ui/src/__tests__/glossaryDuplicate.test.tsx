// Copyright (c) 2026 Kenneth Stott
// Canary: 3d0029e6-ba64-4db8-b2be-376da86efb6a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Creating a term: "+ New term" opens an empty form whatever term was open or duplicated and
// cancelled before it, and Duplicate (beside Edit and Delete in the term's panel) opens the same
// create form filled from the open term under a new name: its definition and domains, not its
// links (columns, tables, relationships) or its reviews (rule:
// components/list/__tests__/panelDuplicateGuard.test.ts).

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../test-utils/render";
import { expectHoldsNoneOf } from "../test-utils/createForm";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import type { GlossaryTermDetail, GlossaryTermSummary } from "../api/glossary";

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
  listAttributablePeople: vi.fn(async () => []),
  listGlossaryTerms: vi.fn(),
  fetchGlossaryTerm: vi.fn(),
  fetchGlossaryFindings: vi.fn(async () => []),
  createGlossaryTerm: vi.fn(),
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

import { createGlossaryTerm, fetchGlossaryTerm, listGlossaryTerms } from "../api/glossary";

const TERM = {
  id: 1,
  name: "net revenue",
  definition: "Revenue after refunds and discounts.",
  status: "approved",
  domains: [],
  refs: [{ kind: "column" }],
  edges_out: [{ to: 2 }],
  edges_in: [],
  experts: [],
  tags: [],
  pending_on: [],
} as unknown as GlossaryTermDetail;

const VALUES = ["net revenue", "Revenue after refunds and discounts.", "net revenue-copy"];
const dialog = () => within(screen.getByRole("dialog"));

async function openTerm() {
  render(<GlossaryTab />);
  fireEvent.click(await screen.findByText("net revenue"));
  await screen.findByTestId("term-read-view");
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(listGlossaryTerms).mockResolvedValue([
    { id: 1, name: "net revenue", status: "approved" } as GlossaryTermSummary,
  ]);
  vi.mocked(fetchGlossaryTerm).mockResolvedValue(TERM);
  vi.mocked(createGlossaryTerm).mockResolvedValue({ id: 9 });
});

describe("+ New term opens an empty form", () => {
  it("holds none of the values of the term open before it", async () => {
    await openTerm();
    fireEvent.click(screen.getByTestId("glossary-new-btn"));
    await screen.findByRole("dialog");
    expectHoldsNoneOf(screen.getByRole("dialog"), VALUES);
  });

  it("holds none of them after a duplicate of the term was cancelled", async () => {
    await openTerm();
    fireEvent.click(await screen.findByTestId("glossary-duplicate-btn"));
    await screen.findByRole("dialog");
    fireEvent.click(dialog().getByText("Cancel"));
    fireEvent.click(screen.getByTestId("glossary-new-btn"));
    await screen.findByRole("dialog");
    expectHoldsNoneOf(screen.getByRole("dialog"), VALUES);
  });
});

describe("Duplicate, in a term's panel", () => {
  it("is beside Edit and Delete, named Duplicate", async () => {
    await openTerm();
    expect(await screen.findByTestId("glossary-duplicate-btn")).toHaveAccessibleName("Duplicate");
  });

  it("is not offered to a caller who may not write the glossary", async () => {
    auth.capabilities = ["glossary_read"];
    await openTerm();
    expect(screen.queryByTestId("glossary-duplicate-btn")).toBeNull();
    auth.capabilities = ["glossary_read", "glossary_rw"];
  });

  it("opens the create form filled with the definition, under a new name", async () => {
    await openTerm();
    fireEvent.click(await screen.findByTestId("glossary-duplicate-btn"));
    await screen.findByRole("dialog");
    expect(dialog().getByTestId("glossary-add-name-input")).toHaveValue("net revenue-copy");
    expect(dialog().getByTestId("glossary-add-definition-input")).toHaveValue(
      "Revenue after refunds and discounts.",
    );
  });

  it("creates the copy as a new term with no links", async () => {
    await openTerm();
    fireEvent.click(await screen.findByTestId("glossary-duplicate-btn"));
    await screen.findByRole("dialog");
    fireEvent.click(dialog().getByTestId("glossary-add-save-btn"));
    await waitFor(() => expect(createGlossaryTerm).toHaveBeenCalledTimes(1));
    expect(createGlossaryTerm).toHaveBeenCalledWith({
      name: "net revenue-copy",
      definition: "Revenue after refunds and discounts.",
    });
  });
});
