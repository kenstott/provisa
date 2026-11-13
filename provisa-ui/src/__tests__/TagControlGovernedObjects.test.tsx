// Copyright (c) 2026 Kenneth Stott
// Canary: 9c2e7a41-3f6b-4d18-a5c0-7b1e8f4d2a63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1634, REQ-2145, REQ-2208: the tag chip tables use, on a data product, a glossary term, a
// reference library document and each of its sections: its pills are that object's own
// assignments, its picker the tags that apply to it, and Apply names the object to the server.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import type { Tag, TagAssignment, TagObjectType } from "../types/admin";
import { TagControl } from "../components/TagControl";

const assignSpy = vi.fn(async (_input: TagAssignment) => ({ success: true, message: "ok" }));
const unassignSpy = vi.fn(async (_input: TagAssignment) => ({ success: true, message: "ok" }));

let mockTags: Tag[] = [];
let mockAssignments: TagAssignment[] = [];

// Spread the real module: a replace-everything factory leaks into other files (TagControl.test).
vi.mock("../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useTagQueries")>()),
  useTags: () => ({ tags: mockTags, loading: false }),
  useTagAssignments: () => ({ tagAssignments: mockAssignments, refetch: vi.fn() }),
  useAssignTag: () => ({ assignTag: assignSpy }),
  useUnassignTag: () => ({ unassignTag: unassignSpy }),
}));

function tag(id: string, appliesTo: TagObjectType[]): Tag {
  return {
    id,
    description: `${id} tag`,
    appliesTo,
    isSystem: false,
    derived: false,
    reasonPolicy: "optional",
    expiresPolicy: "hidden",
    paramPolicy: "none",
    requiresCitation: false,
    required: false,
    citations: [],
    needsReview: false,
    paramValues: [],
  };
}

interface Case {
  name: string;
  props: Parameters<typeof TagControl>[0];
  /** What Apply sends to name the object. */
  target: Partial<TagAssignment>;
  /** Assignments on this object, and on another of its kind. */
  own: TagAssignment;
  other: TagAssignment;
}

const CASES: Case[] = [
  {
    name: "a data product",
    props: { objectType: "product", productId: "sales_kpis" },
    target: { objectType: "product", productId: "sales_kpis" },
    own: { tagId: "certified", objectType: "product", productId: "sales_kpis" },
    other: { tagId: "certified", objectType: "product", productId: "ledger" },
  },
  {
    name: "a glossary term",
    props: { objectType: "term", termId: 4 },
    target: { objectType: "term", termId: 4 },
    own: { tagId: "certified", objectType: "term", termId: 4 },
    other: { tagId: "certified", objectType: "term", termId: 5 },
  },
  {
    name: "a library document",
    props: { objectType: "document", documentId: 7 },
    target: { objectType: "document", documentId: 7 },
    own: { tagId: "certified", objectType: "document", documentId: 7 },
    other: { tagId: "certified", objectType: "document", documentId: 8 },
  },
  {
    name: "a library section, by its heading",
    props: { objectType: "section", documentId: 7, sectionHeading: "Personal data" },
    target: { objectType: "section", documentId: 7, sectionHeading: "Personal data" },
    own: {
      tagId: "certified",
      objectType: "section",
      documentId: 7,
      sectionHeading: "Personal data",
    },
    other: {
      tagId: "certified",
      objectType: "section",
      documentId: 7,
      sectionHeading: "Retention",
    },
  },
  {
    name: "a library section, by its page",
    props: { objectType: "section", documentId: 9, sectionHeading: null, sectionPage: 3 },
    target: { objectType: "section", documentId: 9, sectionPage: 3 },
    own: { tagId: "certified", objectType: "section", documentId: 9, sectionPage: 3 },
    other: { tagId: "certified", objectType: "section", documentId: 9, sectionPage: 4 },
  },
];

describe.each(CASES)("the tag chip on $name", ({ props, target, own, other }) => {
  beforeEach(() => {
    assignSpy.mockClear();
    unassignSpy.mockClear();
    mockTags = [
      tag("certified", ["product", "term", "document", "section"]),
      tag("finance", ["table"]),
      tag("reviewed", ["product", "term", "document", "section"]),
    ];
    mockAssignments = [];
  });

  it("shows the object's own tags, not another's of its kind", () => {
    mockAssignments = [other];
    const { unmount } = render(<TagControl {...props} />);
    expect(screen.queryByTestId("tag-pill-certified")).toBeNull();
    unmount();
    mockAssignments = [own];
    render(<TagControl {...props} />);
    expect(screen.getByTestId("tag-pill-certified")).toBeInTheDocument();
  });

  it("offers only the tags that apply to it", async () => {
    render(<TagControl {...props} />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    await screen.findByTestId("tag-option-reviewed");
    expect(screen.queryByTestId("tag-option-finance")).toBeNull();
  });

  it("names the object when a tag is applied, then says it changed", async () => {
    const onChanged = vi.fn();
    render(<TagControl {...props} onChanged={onChanged} />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    fireEvent.click(await screen.findByTestId("tag-option-reviewed"));
    fireEvent.click(screen.getByTestId("tag-apply-reviewed"));
    await waitFor(() => expect(onChanged).toHaveBeenCalledTimes(1));
    expect(assignSpy).toHaveBeenCalledWith(
      expect.objectContaining({ tagId: "reviewed", ...target }),
      [],
    );
  });

  it("names the object when a tag is taken off, then says it changed", async () => {
    mockAssignments = [own];
    const onChanged = vi.fn();
    render(<TagControl {...props} onChanged={onChanged} />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    fireEvent.click(await screen.findByTestId("tag-option-certified"));
    await waitFor(() => expect(onChanged).toHaveBeenCalledTimes(1));
    expect(unassignSpy).toHaveBeenCalledWith(
      expect.objectContaining({ tagId: "certified", ...target }),
    );
  });

  it("is only read where the reader may not change it", () => {
    mockAssignments = [own];
    render(<TagControl {...props} readOnly />);
    expect(screen.getByTestId("tag-pill-certified")).toBeInTheDocument();
    expect(screen.queryByTestId("tag-picker-toggle")).toBeNull();
  });
});

describe("a refused change", () => {
  it("is not said to have changed the object", async () => {
    mockTags = [tag("reviewed", ["product"])];
    mockAssignments = [];
    assignSpy.mockResolvedValueOnce({ success: false, message: "No access to domain 'finance'" });
    const onChanged = vi.fn();
    render(<TagControl objectType="product" productId="ledger" onChanged={onChanged} />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    fireEvent.click(await screen.findByTestId("tag-option-reviewed"));
    fireEvent.click(screen.getByTestId("tag-apply-reviewed"));
    await waitFor(() => expect(assignSpy).toHaveBeenCalled());
    expect(onChanged).not.toHaveBeenCalled();
  });
});
