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
// point, added by picking a document then a section, removed, and confirmed when flagged.

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

let mockTags: Tag[] = [];
let mockAssignments: TagAssignment[] = [];
vi.mock("../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useTagQueries")>()),
  useTags: () => ({ tags: mockTags, loading: false }),
  useTagAssignments: () => ({ tagAssignments: mockAssignments, refetch: vi.fn() }),
  useAssignTag: () => ({ assignTag: vi.fn() }),
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
  citations: [],
  needsReview: false,
  ...over,
});

beforeEach(() => {
  cite.mockClear();
  uncite.mockClear();
  citeAssignment.mockClear();
  unciteAssignment.mockClear();
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
      expect(cite).toHaveBeenCalledWith({
        documentId: 3,
        sectionHeading: "Scope",
        sectionPage: null,
      }),
    );
  });

  it("offers a page for a section with no heading, and the whole document", async () => {
    render(<TagCitations citations={[]} {...props()} />);
    await choose("cites-picker-document", "Data Policy");
    await choose("cites-picker-section", t("tagCitations.page", { page: 4 }));
    fireEvent.click(screen.getByTestId("cites-picker-add"));
    await waitFor(() =>
      expect(cite).toHaveBeenCalledWith({ documentId: 3, sectionHeading: null, sectionPage: 4 }),
    );
    cite.mockClear();
    await choose("cites-picker-document", "Data Policy");
    fireEvent.click(screen.getByTestId("cites-picker-add"));
    await waitFor(() =>
      expect(cite).toHaveBeenCalledWith({ documentId: 3, sectionHeading: null, sectionPage: null }),
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

describe("TagControl citations", () => {
  const finance = (assignment: Partial<TagAssignment> = {}) => {
    mockTags = [tag({ citations: [current], requiresCitation: true })];
    mockAssignments = [
      { tagId: "finance", objectType: "table", tableId: 7, citations: [], ...assignment },
    ];
  };

  it("reaches the tag's defining citation from the chip, linked to the Library", async () => {
    finance();
    render(<TagControl objectType="table" tableId={7} />);
    fireEvent.click(screen.getByTestId("tag-pill-finance"));
    const link = await screen.findByTestId("tag-definition-link-finance");
    expect(link).toHaveAttribute("href", "/library?document=3");
    expect(link).toHaveTextContent("Data Policy · Scope");
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

  it("lists the assignment's citations in the chip's detail and cites through the assignment", async () => {
    finance({ citations: [current] });
    render(<TagControl objectType="table" tableId={7} />);
    fireEvent.click(screen.getByTestId("tag-pill-finance"));
    expect(await screen.findByTestId("tag-citations-finance-row-3-h:Scope")).toBeInTheDocument();
    await choose("tag-citations-finance-picker-document", "Data Policy");
    fireEvent.click(screen.getByTestId("tag-citations-finance-picker-add"));
    await waitFor(() => expect(citeAssignment).toHaveBeenCalled());
    const [assignment, place] = citeAssignment.mock.calls[0];
    expect(assignment).toMatchObject({ tagId: "finance", objectType: "table", tableId: 7 });
    expect(place).toEqual({ documentId: 3, sectionHeading: null, sectionPage: null });
  });

  it("marks a drifted assignment citation, confirms and removes it", async () => {
    finance({ citations: [drifted], needsReview: true });
    render(<TagControl objectType="table" tableId={7} />);
    expect(screen.getByTestId("tag-review-finance")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("tag-pill-finance"));
    fireEvent.click(await screen.findByTestId("tag-citations-finance-confirm-3-h:Retention"));
    await waitFor(() => expect(citeAssignment).toHaveBeenCalledTimes(1));
    expect(citeAssignment.mock.calls[0][1]).toMatchObject({ sectionHeading: "Retention" });
    fireEvent.click(screen.getByTestId("tag-citations-finance-remove-3-h:Retention"));
    await waitFor(() => expect(unciteAssignment).toHaveBeenCalledTimes(1));
  });

  it("lists the citations read only, with no editing, on a read only control", async () => {
    finance({ citations: [drifted], needsReview: true });
    render(<TagControl objectType="table" tableId={7} readOnly />);
    fireEvent.click(screen.getByTestId("tag-pill-finance"));
    expect(
      await screen.findByTestId("tag-citations-finance-row-3-h:Retention"),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("tag-citations-finance-confirm-3-h:Retention")).toBeNull();
    expect(screen.queryByTestId("tag-citations-finance-picker-add")).toBeNull();
  });
});
