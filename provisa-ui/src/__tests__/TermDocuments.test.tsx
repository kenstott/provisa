// Copyright (c) 2026 Kenneth Stott
// Canary: 8d1f4a62-3c7e-4b95-a0e2-5f9c6b81d3a7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2208, REQ-2179: a term's Documents section lists each library document and section the term
// cites as a link to that section, with what review proposes marked so; a curator picks a document
// and a section, accepts a proposed one (the review queue's merge into the term), rejects one, and
// removes one; a reader only reads, and a reader without the Library's right reads names, not links.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";
import { notifications } from "@mantine/notifications";
import i18n from "../i18n";
import { TermDocuments } from "../components/admin/TermDocuments";
import type { TermDocumentCitation } from "../api/referenceLibrary";

const t = i18n.getFixedT("en");

const api = vi.hoisted(() => ({
  fetchLibrary: vi.fn(),
  fetchLibraryDocument: vi.fn(),
  fetchTermCitations: vi.fn(),
  citeForTerm: vi.fn(),
  unciteForTerm: vi.fn(),
}));
vi.mock("../api/referenceLibrary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/referenceLibrary")>()),
  ...api,
}));
const review = vi.hoisted(() => ({ mergeDraft: vi.fn(), rejectDraft: vi.fn() }));
vi.mock("../api/termFinding", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/termFinding")>()),
  ...review,
}));
const auth = vi.hoisted(() => ({ capabilities: ["glossary_read", "glossary_rw"] as string[] }));
vi.mock("../context/AuthContext", () => ({ useAuth: () => auth }));

const scan: TermDocumentCitation = {
  key: "scan:11:1:revenue is earned.",
  origin: "scan",
  status: "accepted",
  document_id: 3,
  document_title: "Data Policy",
  section_heading: "Terms",
  section_page: 2,
  state: "current",
  needs_review: false,
  quote: "Revenue is earned.",
  ordinal: 1,
  draft_id: 11,
  cite_key: null,
  cited_by: "ann",
};
const picked: TermDocumentCitation = {
  ...scan,
  key: "picked:section:3:heading:Scope",
  origin: "picked",
  section_heading: "Scope",
  section_page: null,
  state: "drifted",
  needs_review: true,
  quote: null,
  ordinal: null,
  draft_id: null,
  cite_key: "section:3:heading:Scope",
};
const proposed: TermDocumentCitation = {
  ...scan,
  key: "scan:12:1:revenue is earned.",
  status: "proposed",
  draft_id: 12,
  cited_by: null,
};
const pagedOnly: TermDocumentCitation = {
  ...picked,
  key: "picked:section:3:page:4",
  section_heading: null,
  section_page: 4,
  state: "current",
  needs_review: false,
  cite_key: "section:3:page:4",
};

const ID = "glossary-term-documents";

beforeEach(() => {
  vi.clearAllMocks();
  auth.capabilities = ["glossary_read", "glossary_rw"];
  api.fetchTermCitations.mockResolvedValue([scan, picked, proposed]);
  api.citeForTerm.mockResolvedValue(picked);
  api.unciteForTerm.mockResolvedValue(undefined);
  review.mergeDraft.mockResolvedValue({ term_id: 7 });
  review.rejectDraft.mockResolvedValue(undefined);
  api.fetchLibrary.mockResolvedValue([{ id: 3, title: "Data Policy", index_status: "ready" }]);
  api.fetchLibraryDocument.mockResolvedValue({
    id: 3,
    passage_list: [
      { ordinal: 0, section: "Scope", page: 1, text: "a" },
      { ordinal: 1, section: "Terms", page: 2, text: "Revenue is earned." },
    ],
  });
});

async function choose(testId: string, label: string) {
  const input = screen.getByTestId(testId);
  fireEvent.click(input);
  await waitFor(() => {
    if (!input.getAttribute("aria-controls")) throw new Error("dropdown not open");
  });
  const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
  fireEvent.click(
    within(listbox as HTMLElement).getByRole("option", { name: label, hidden: true }),
  );
}

describe("TermDocuments", () => {
  it("lists each citation as a link to its section, with what review proposes", async () => {
    render(<TermDocuments termId={7} canEdit />);
    const row = await screen.findByTestId(`${ID}-row-${scan.key}`);
    expect(api.fetchTermCitations).toHaveBeenCalledWith(7);
    expect(row).toHaveTextContent("Data Policy · Terms");
    expect(within(row).getByRole("link")).toHaveAttribute(
      "href",
      "/context/documents?document=3&tab=sections&at=h%3ATerms",
    );
    expect(screen.getByTestId(`${ID}-detail-${scan.key}`)).toHaveTextContent("Revenue is earned.");
    expect(within(row).queryByTestId(`${ID}-proposed-${scan.key}`)).toBeNull();
    const pending = screen.getByTestId(`${ID}-row-${proposed.key}`);
    expect(within(pending).getByTestId(`${ID}-proposed-${proposed.key}`)).toHaveTextContent(
      t("tagCitations.proposed"),
    );
    // A picked section whose text changed is flagged, and confirmed by citing it again.
    const drifted = screen.getByTestId(`${ID}-row-${picked.key}`);
    expect(within(drifted).getByTestId("citation-state-drifted")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId(`${ID}-confirm-${picked.key}`));
    await waitFor(() =>
      expect(api.citeForTerm).toHaveBeenCalledWith(7, {
        document_id: 3,
        section_heading: "Scope",
        section_page: null,
      }),
    );
  });

  it("links a section with no heading by its page", async () => {
    api.fetchTermCitations.mockResolvedValue([pagedOnly]);
    render(<TermDocuments termId={7} canEdit />);
    const row = await screen.findByTestId(`${ID}-row-${pagedOnly.key}`);
    expect(within(row).getByRole("link")).toHaveAttribute(
      "href",
      "/context/documents?document=3&tab=sections&at=p%3A4",
    );
  });

  it("adds a citation: a library document, then one of its sections", async () => {
    render(<TermDocuments termId={7} canEdit />);
    await screen.findByTestId(`${ID}-row-${scan.key}`);
    await choose(`${ID}-picker-document`, "Data Policy");
    await choose(`${ID}-picker-section`, "Terms");
    fireEvent.click(screen.getByTestId(`${ID}-picker-add`));
    await waitFor(() =>
      expect(api.citeForTerm).toHaveBeenCalledWith(7, {
        document_id: 3,
        section_heading: "Terms",
        section_page: null,
      }),
    );
    // The list is read again after the change.
    await waitFor(() => expect(api.fetchTermCitations).toHaveBeenCalledTimes(2));
  });

  it("accepts a proposed citation by merging its draft into the term", async () => {
    render(<TermDocuments termId={7} canEdit />);
    fireEvent.click(await screen.findByTestId(`${ID}-accept-${proposed.key}`));
    await waitFor(() => expect(review.mergeDraft).toHaveBeenCalledWith(12, { term_id: 7 }));
    expect(screen.queryByTestId(`${ID}-accept-${scan.key}`)).toBeNull();
  });

  it("rejects a proposed citation and removes an accepted one", async () => {
    render(<TermDocuments termId={7} canEdit />);
    fireEvent.click(await screen.findByTestId(`${ID}-remove-${proposed.key}`));
    await waitFor(() => expect(review.rejectDraft).toHaveBeenCalledWith(12));
    fireEvent.click(screen.getByTestId(`${ID}-remove-${scan.key}`));
    await waitFor(() => expect(api.unciteForTerm).toHaveBeenCalledWith(7, scan));
    expect(api.unciteForTerm).not.toHaveBeenCalledWith(7, proposed);
  });

  it("says why a change was refused", async () => {
    review.mergeDraft.mockRejectedValue(new Error("term 7 is authored by ann"));
    const shown = vi.spyOn(notifications, "show");
    render(<TermDocuments termId={7} canEdit />);
    fireEvent.click(await screen.findByTestId(`${ID}-accept-${proposed.key}`));
    await waitFor(() =>
      expect(shown).toHaveBeenCalledWith({ color: "red", message: "term 7 is authored by ann" }),
    );
  });

  it("offers a reader nothing to change", async () => {
    render(<TermDocuments termId={7} canEdit={false} />);
    await screen.findByTestId(`${ID}-row-${scan.key}`);
    expect(screen.queryByTestId(`${ID}-picker`)).toBeNull();
    expect(screen.queryByTestId(`${ID}-accept-${proposed.key}`)).toBeNull();
    expect(screen.queryByTestId(`${ID}-remove-${scan.key}`)).toBeNull();
    expect(screen.queryByTestId(`${ID}-confirm-${picked.key}`)).toBeNull();
  });

  it("names a document without a link for a reader who may not open the Library", async () => {
    auth.capabilities = [];
    render(<TermDocuments termId={7} canEdit={false} />);
    const row = await screen.findByTestId(`${ID}-row-${scan.key}`);
    expect(row).toHaveTextContent("Data Policy · Terms");
    expect(within(row).queryByRole("link")).toBeNull();
  });

  it("says nothing is cited yet", async () => {
    api.fetchTermCitations.mockResolvedValue([]);
    render(<TermDocuments termId={7} canEdit={false} />);
    expect(await screen.findByText(t("tagCitations.none"))).toBeInTheDocument();
  });
});
