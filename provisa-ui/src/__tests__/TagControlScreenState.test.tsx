// Copyright (c) 2026 Kenneth Stott
// Canary: 2f8d6b03-a4c7-4e19-9b52-d70e3a1c6f84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Polly's read_screen reports, for each object whose tag chips are on screen, its kind and key and
// each tag with how it got there and what needs attention. TagControl publishes it, so every page
// that mounts it reports the same shape.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render } from "../test-utils/render";
import type { Tag, TagAssignment } from "../types/admin";
import { TagControl } from "../components/TagControl";
import { readScreen, type Screen } from "../mcp/screenState";
import { resetDataAddresses } from "../lib/dataAddress";

let mockTags: Tag[] = [];
let mockAssignments: TagAssignment[] = [];

// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files (see ScheduledTasks.test.tsx).
vi.mock("../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useTagQueries")>()),
  useTags: () => ({ tags: mockTags, loading: false }),
  useTagAssignments: () => ({ tagAssignments: mockAssignments, refetch: vi.fn() }),
  useAssignTag: () => ({ assignTag: vi.fn() }),
  useUnassignTag: () => ({ unassignTag: vi.fn() }),
}));

function tag(id: string, over: Partial<Tag> = {}): Tag {
  return {
    id,
    description: id,
    appliesTo: ["column", "table"],
    isSystem: false,
    derived: false,
    reasonPolicy: "optional",
    expiresPolicy: "optional",
    paramPolicy: "none",
    paramValues: [],
    requiresCitation: false,
    citations: [],
    needsReview: false,
    ...over,
  };
}

async function screenTags() {
  const screen = (await readScreen(0)) as Screen;
  return screen.tags;
}

beforeEach(() => {
  resetDataAddresses();
  // jsdom lays nothing out; every element counts as drawn unless a test says otherwise.
  HTMLElement.prototype.checkVisibility = function () {
    return this.dataset.offscreen !== "true";
  };
  window.history.pushState({}, "", "/sources");
  mockTags = [
    tag("finance", { requiresCitation: true }),
    tag("pii", { isSystem: true }),
    tag("primary_key", { derived: true, isSystem: true }),
    tag("deprecated", { isSystem: true }),
  ];
  mockAssignments = [
    {
      tagId: "finance",
      objectType: "column",
      tableId: 12,
      columnName: "email",
      citationGap: true,
      needsReview: true,
    },
    { tagId: "primary_key", objectType: "column", tableId: 12, columnName: "email" },
    {
      tagId: "deprecated",
      objectType: "column",
      tableId: 12,
      columnName: "email",
      reason: "replaced",
      expiresOn: "2020-01-01",
    },
  ];
});

afterEach(() => cleanup());

describe("TagControl on read_screen", () => {
  it("publishes each tag of the object: assigned with a gap, inherited, derived, expired", async () => {
    render(<TagControl objectType="column" tableId={12} columnName="email" piiTerm="Email" />);
    expect(await screenTags()).toEqual([
      {
        kind: "column",
        key: "column:12:email",
        control: "tag-control-column-12-email",
        readOnly: false,
        tags: [
          {
            id: "finance",
            via: "assigned",
            citationGap: true,
            needsReview: true,
            expired: false,
          },
          { id: "pii", via: "inherited", citationGap: false, needsReview: false, expired: false },
          {
            id: "primary_key",
            via: "derived",
            citationGap: false,
            needsReview: false,
            expired: false,
          },
          {
            id: "deprecated",
            via: "assigned",
            citationGap: false,
            needsReview: false,
            expired: true,
          },
        ],
      },
    ]);
  });

  it("reports an assignment's parameter with its tag id", async () => {
    mockTags = [tag("entity", { paramPolicy: "required" })];
    mockAssignments = [
      { tagId: "entity:customer", objectType: "column", tableId: 12, columnName: "email" },
    ];
    render(<TagControl objectType="column" tableId={12} columnName="email" readOnly />);
    const [object] = await screenTags();
    expect(object.readOnly).toBe(true);
    expect(object.tags.map((t) => t.id)).toEqual(["entity:customer"]);
  });

  it("reports only the objects whose tag control is on screen, each once", async () => {
    render(
      <>
        <TagControl objectType="table" tableId={12} />
        <div>
          <TagControl objectType="table" tableId={13} />
        </div>
      </>,
    );
    const offscreen = document.querySelector<HTMLElement>("[data-testid='tag-control-table-13']");
    offscreen!.dataset.offscreen = "true";
    expect((await screenTags()).map((o) => o.key)).toEqual(["table:12"]);
  });

  it("names a library section by its heading, or its page, as the store keys it", async () => {
    mockTags = [];
    mockAssignments = [];
    render(
      <>
        <TagControl objectType="section" documentId={7} sectionHeading="Retention" />
        <TagControl objectType="section" documentId={7} sectionPage={3} />
      </>,
    );
    expect((await screenTags()).map((o) => o.key)).toEqual([
      "section:7:heading:Retention",
      "section:7:page:3",
    ]);
  });

  it("clears the object when the control unmounts", async () => {
    const { unmount } = render(<TagControl objectType="column" tableId={12} columnName="email" />);
    expect(await screenTags()).toHaveLength(1);
    unmount();
    expect(await screenTags()).toEqual([]);
  });
});
