// Copyright (c) 2026 Kenneth Stott
// Canary: 6c3d8e15-2f9a-4b70-9d41-a8e5b7c0f3d2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The library citations of a tag's definition and of one assignment of it: listed with where they
// point, added by picking a document then a section, removed, and confirmed when flagged. An
// assignment's are a field of its one form, saved with its reason and expiry by one assignTag.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../test-utils/render";
import i18n from "../i18n";
import { TagCitations } from "../components/TagCitations";
import { TagControl } from "../components/TagControl";
import type { LibraryCitation, Tag, TagAssignment } from "../types/admin";

const t = i18n.getFixedT("en");

const api = vi.hoisted(() => ({ fetchLibrary: vi.fn(), fetchLibraryDocument: vi.fn() }));
vi.mock("../api/referenceLibrary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/referenceLibrary")>()),
  ...api,
}));

const cite = vi.fn(async (..._a: unknown[]) => ({ success: true, message: "ok" }));
const uncite = vi.fn(async (..._a: unknown[]) => ({ success: true, message: "ok" }));
const citeAssignment = vi.fn(async (..._a: unknown[]) => ({ success: true, message: "ok" }));
const unciteAssignment = vi.fn(async (..._a: unknown[]) => ({ success: true, message: "ok" }));
const assignTag = vi.fn(async (..._a: unknown[]) => ({ success: true, message: "ok" }));

let mockTags: Tag[] = [];
let mockAssignments: TagAssignment[] = [];
vi.mock("../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useTagQueries")>()),
  useTags: () => ({ tags: mockTags, loading: false }),
  useTagAssignments: () => ({ tagAssignments: mockAssignments, refetch: vi.fn() }),
  useAssignTag: () => ({ assignTag }),
  useUnassignTag: () => ({ unassignTag: vi.fn() }),
  useTagCitations: () => ({
    citeDefinition: cite,
    unciteDefinition: uncite,
    citeAssignment,
    unciteAssignment,
  }),
}));

const current: LibraryCitation = {
  documentId: 3,
  documentTitle: "Data Policy",
  documentSource: "policy.md",
  sectionHeading: "Scope",
  sectionPage: null,
  textSha256: "a",
  citedBy: "ann",
  citedAt: "2026-11-13T00:00:00",
  state: "current",
  needsReview: false,
};
const drifted: LibraryCitation = {
  ...current,
  sectionHeading: "Retention",
  state: "drifted",
  needsReview: true,
};

const tag = (over: Partial<Tag> = {}): Tag => ({
  id: "finance",
  description: "Finance domain",
  appliesTo: ["table"],
  isSystem: false,
  derived: false,
  reasonPolicy: "hidden",
  expiresPolicy: "hidden",
  paramPolicy: "none",
  paramValues: [],
  requiresCitation: false,
  required: false,
  citations: [],
  needsReview: false,
  ...over,
});

beforeEach(() => {
  cite.mockClear();
  uncite.mockClear();
  citeAssignment.mockClear();
  unciteAssignment.mockClear();
  assignTag.mockClear();
  api.fetchLibrary.mockResolvedValue([
    { id: 3, title: "Data Policy", index_status: "ready" },
    { id: 4, title: "Half indexed", index_status: "indexing" },
  ]);
  api.fetchLibraryDocument.mockResolvedValue({
    id: 3,
    passage_list: [
      { ordinal: 0, section: "Scope", page: 1, text: "a" },
      { ordinal: 1, section: "Scope", page: 1, text: "b" },
      { ordinal: 2, section: null, page: 4, text: "c" },
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

describe("TagCitations", () => {
  const props = () => ({
    onAdd: cite,
    onRemove: uncite,
    onConfirm: cite,
    testId: "cites",
  });

  it("adds a citation: a library document, then one of its sections", async () => {
    render(<TagCitations citations={[]} {...props()} />);
    await choose("cites-picker-document", "Data Policy");
    await choose("cites-picker-section", "Scope");
    fireEvent.click(screen.getByTestId("cites-picker-add"));
    await waitFor(() =>
      expect(cite).toHaveBeenCalledWith(
        { documentId: 3, sectionHeading: "Scope", sectionPage: null },
        "Data Policy",
      ),
    );
  });

  it("offers a page for a section with no heading, and the whole document", async () => {
    render(<TagCitations citations={[]} {...props()} />);
    await choose("cites-picker-document", "Data Policy");
    await choose("cites-picker-section", t("tagCitations.page", { page: 4 }));
    fireEvent.click(screen.getByTestId("cites-picker-add"));
    await waitFor(() =>
      expect(cite).toHaveBeenCalledWith(
        { documentId: 3, sectionHeading: null, sectionPage: 4 },
        "Data Policy",
      ),
    );
    cite.mockClear();
    await choose("cites-picker-document", "Data Policy");
    fireEvent.click(screen.getByTestId("cites-picker-add"));
    await waitFor(() =>
      expect(cite).toHaveBeenCalledWith(
        { documentId: 3, sectionHeading: null, sectionPage: null },
        "Data Policy",
      ),
    );
  });

  it("lists only documents that are ready to be cited", async () => {
    render(<TagCitations citations={[]} {...props()} />);
    const input = screen.getByTestId("cites-picker-document");
    await waitFor(() => expect(api.fetchLibrary).toHaveBeenCalled());
    fireEvent.click(input);
    await waitFor(() => {
      if (!input.getAttribute("aria-controls")) throw new Error("dropdown not open");
    });
    const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
    expect(
      within(listbox as HTMLElement).queryByRole("option", { name: "Half indexed", hidden: true }),
    ).toBeNull();
  });

  it("lists a citation as a link to its document, and removes it", async () => {
    render(<TagCitations citations={[current]} {...props()} />);
    const row = screen.getByTestId("cites-row-3-h:Scope");
    expect(within(row).getByRole("link")).toHaveAttribute("href", "/library?document=3");
    expect(row).toHaveTextContent("Data Policy · Scope");
    expect(within(row).queryByTestId("citation-state-drifted")).toBeNull();
    expect(within(row).queryByText(t("tagCitations.confirm"))).toBeNull();
    fireEvent.click(screen.getByTestId("cites-remove-3-h:Scope"));
    await waitFor(() => expect(uncite).toHaveBeenCalledWith(current));
  });

  it("marks a drifted citation for review, and Confirm cites the same place again", async () => {
    render(<TagCitations citations={[drifted]} {...props()} />);
    const row = screen.getByTestId("cites-row-3-h:Retention");
    expect(within(row).getByTestId("citation-state-drifted")).toHaveTextContent(
      t("tagCitations.drifted"),
    );
    fireEvent.click(screen.getByTestId("cites-confirm-3-h:Retention"));
    await waitFor(() => expect(cite).toHaveBeenCalledWith(drifted));
    expect(uncite).not.toHaveBeenCalled();
  });

  it("marks a citation whose section is gone, with no link once its document is", () => {
    const broken: LibraryCitation = {
      ...current,
      documentId: null,
      state: "broken",
      needsReview: true,
    };
    render(<TagCitations citations={[broken]} {...props()} />);
    expect(screen.getByTestId("citation-state-broken")).toBeInTheDocument();
    expect(screen.queryByRole("link")).toBeNull();
  });
});

const place = (c: LibraryCitation) => ({
  documentId: c.documentId,
  sectionHeading: c.sectionHeading,
  sectionPage: c.sectionPage,
});

describe("TagControl: one form for an assignment and its citations", () => {
  const finance = (assignment: Partial<TagAssignment> = {}, over: Partial<Tag> = {}) => {
    mockTags = [tag({ citations: [current], requiresCitation: true, ...over })];
    mockAssignments = [
      { tagId: "finance", objectType: "table", tableId: 7, citations: [], ...assignment },
    ];
  };

  it("picking a tag opens one form of reason, expiry and citations, saved by one call", async () => {
    mockTags = [tag({ reasonPolicy: "required", expiresPolicy: "optional" })];
    mockAssignments = [];
    render(<TagControl objectType="table" tableId={7} />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    fireEvent.click(await screen.findByTestId("tag-option-finance"));
    const form = await screen.findByTestId("tag-form-finance");
    expect(within(form).getByTestId("tag-reason-finance")).toBeInTheDocument();
    expect(within(form).getByTestId("tag-expires-finance")).toBeInTheDocument();
    expect(within(form).getByTestId("tag-citations-finance")).toBeInTheDocument();
    expect(within(form).getAllByTestId(/^tag-apply-/)).toHaveLength(1);
    fireEvent.change(screen.getByTestId("tag-reason-finance"), { target: { value: "GL feed" } });
    fireEvent.change(screen.getByTestId("tag-expires-finance"), {
      target: { value: "2027-01-31" },
    });
    await choose("tag-citations-finance-picker-document", "Data Policy");
    await choose("tag-citations-finance-picker-section", "Scope");
    fireEvent.click(screen.getByTestId("tag-citations-finance-picker-add"));
    // Picked, not yet saved: listed in the form, and no citation mutation made.
    expect(await screen.findByTestId("tag-citations-finance-row-3-h:Scope")).toBeInTheDocument();
    expect(citeAssignment).not.toHaveBeenCalled();
    fireEvent.click(screen.getByTestId("tag-apply-finance"));
    await waitFor(() => expect(assignTag).toHaveBeenCalledTimes(1));
    const [assignment, cited] = assignTag.mock.calls[0];
    expect(assignment).toMatchObject({
      tagId: "finance",
      objectType: "table",
      tableId: 7,
      reason: "GL feed",
      expiresOn: "2027-01-31",
    });
    expect(cited).toEqual([{ documentId: 3, sectionHeading: "Scope", sectionPage: null }]);
    expect(citeAssignment).not.toHaveBeenCalled();
  });

  it("opens the same form from the chip, filled from the assignment", async () => {
    finance(
      { reason: "Ledger", expiresOn: "2027-03-01", citations: [current] },
      { reasonPolicy: "optional", expiresPolicy: "optional" },
    );
    render(<TagControl objectType="table" tableId={7} />);
    fireEvent.click(screen.getByTestId("tag-pill-finance"));
    const form = await screen.findByTestId("tag-form-finance");
    expect(within(form).getByTestId("tag-reason-finance")).toHaveValue("Ledger");
    expect(within(form).getByTestId("tag-expires-finance")).toHaveValue("2027-03-01");
    expect(within(form).getByTestId("tag-citations-finance-row-3-h:Scope")).toBeInTheDocument();
    fireEvent.change(screen.getByTestId("tag-reason-finance"), { target: { value: "Ledger 2" } });
    fireEvent.click(screen.getByTestId("tag-apply-finance"));
    await waitFor(() => expect(assignTag).toHaveBeenCalledTimes(1));
    const [assignment, cited] = assignTag.mock.calls[0];
    expect(assignment).toMatchObject({ reason: "Ledger 2", expiresOn: "2027-03-01" });
    expect(cited).toEqual([place(current)]);
  });

  it("reaches the tag's defining citation at the head of the chip's form, linked to the Library", async () => {
    finance();
    render(<TagControl objectType="table" tableId={7} />);
    fireEvent.click(screen.getByTestId("tag-pill-finance"));
    const link = await screen.findByTestId("tag-definition-link-finance");
    expect(link).toHaveAttribute("href", "/library?document=3");
    expect(link).toHaveTextContent("Data Policy · Scope");
    expect(
      within(screen.getByTestId("tag-defined-by-finance")).getByTestId(
        "tag-definition-link-finance",
      ),
    ).toBe(link);
  });

  it("marks the citations as needed for a tag that requires one, and saves without one", async () => {
    mockTags = [tag({ requiresCitation: true })];
    mockAssignments = [];
    const { rerender } = render(<TagControl objectType="table" tableId={7} />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    fireEvent.click(await screen.findByTestId("tag-option-finance"));
    expect(await screen.findByTestId("tag-citations-needed-finance")).toHaveTextContent(
      t("tagCitations.neededForCoverage"),
    );
    expect(screen.getByText(t("tagCitations.gapHelp"))).toBeInTheDocument();
    const apply = screen.getByTestId("tag-apply-finance");
    expect(apply).not.toBeDisabled();
    fireEvent.click(apply);
    await waitFor(() => expect(assignTag).toHaveBeenCalledTimes(1));
    expect(assignTag.mock.calls[0][1]).toEqual([]);
    // Saved without one, the assignment reads back as a gap.
    mockAssignments = [
      { tagId: "finance", objectType: "table", tableId: 7, citations: [], citationGap: true },
    ];
    rerender(<TagControl objectType="table" tableId={7} />);
    expect(screen.getByTestId("tag-gap-finance")).toHaveTextContent(t("tagCitations.gap"));
  });

  it("shows a gap mark on an assignment that lacks the citation its tag requires", () => {
    finance({ citationGap: true });
    render(<TagControl objectType="table" tableId={7} />);
    expect(screen.getByTestId("tag-gap-finance")).toHaveTextContent(t("tagCitations.gap"));
  });

  it("shows no gap mark when the assignment is cited", () => {
    finance({ citations: [current], citationGap: false });
    render(<TagControl objectType="table" tableId={7} />);
    expect(screen.queryByTestId("tag-gap-finance")).toBeNull();
    expect(screen.queryByTestId("tag-review-finance")).toBeNull();
  });

  it("confirms a drifted citation at once; a removed one leaves the set the form saves", async () => {
    finance({ citations: [current, drifted], needsReview: true });
    render(<TagControl objectType="table" tableId={7} />);
    expect(screen.getByTestId("tag-review-finance")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("tag-pill-finance"));
    fireEvent.click(await screen.findByTestId("tag-citations-finance-confirm-3-h:Retention"));
    await waitFor(() => expect(citeAssignment).toHaveBeenCalledTimes(1));
    const [assignment, confirmed] = citeAssignment.mock.calls[0];
    expect(assignment).toMatchObject({ tagId: "finance", objectType: "table", tableId: 7 });
    expect(confirmed).toMatchObject({ sectionHeading: "Retention" });
    await waitFor(() =>
      expect(screen.queryByTestId("tag-citations-finance-confirm-3-h:Retention")).toBeNull(),
    );
    fireEvent.click(screen.getByTestId("tag-citations-finance-remove-3-h:Scope"));
    expect(screen.queryByTestId("tag-citations-finance-row-3-h:Scope")).toBeNull();
    expect(unciteAssignment).not.toHaveBeenCalled();
    fireEvent.click(screen.getByTestId("tag-apply-finance"));
    await waitFor(() => expect(assignTag).toHaveBeenCalledTimes(1));
    expect(assignTag.mock.calls[0][1]).toEqual([place(drifted)]);
  });

  it("cancel closes the chip's form and saves nothing", async () => {
    finance({ citations: [current] });
    render(<TagControl objectType="table" tableId={7} />);
    fireEvent.click(screen.getByTestId("tag-pill-finance"));
    fireEvent.click(await screen.findByTestId("tag-citations-finance-remove-3-h:Scope"));
    fireEvent.click(screen.getByTestId("tag-cancel-finance"));
    await waitFor(() => expect(screen.queryByTestId("tag-form-finance")).toBeNull());
    expect(assignTag).not.toHaveBeenCalled();
  });

  it("reads the same form, with no inputs, on a read only control", async () => {
    finance(
      { reason: "Ledger", citations: [drifted], needsReview: true },
      { reasonPolicy: "optional", expiresPolicy: "optional" },
    );
    render(<TagControl objectType="table" tableId={7} readOnly />);
    fireEvent.click(screen.getByTestId("tag-pill-finance"));
    const form = await screen.findByTestId("tag-form-finance");
    expect(within(form).getByTestId("tag-definition-link-finance")).toBeInTheDocument();
    expect(within(form).getByText("Ledger")).toBeInTheDocument();
    expect(within(form).getByTestId("tag-citations-finance-row-3-h:Retention")).toBeInTheDocument();
    expect(form.querySelectorAll("input, textarea, select, button")).toHaveLength(0);
    expect(screen.queryByTestId("tag-apply-finance")).toBeNull();
    expect(screen.queryByTestId("tag-citations-finance-confirm-3-h:Retention")).toBeNull();
    expect(screen.queryByTestId("tag-citations-finance-picker-add")).toBeNull();
  });
});
