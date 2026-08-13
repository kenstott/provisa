// Copyright (c) 2026 Kenneth Stott
// Canary: 31f1e910-3f8b-4916-8d26-02bfea36ed33
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../test-utils/render";
import i18n from "../i18n";
import { TagsTab } from "../components/admin/TagsTab";
import type { Tag, TagAssignment } from "../types/admin";

const t = i18n.getFixedT("en");

const upsertSpy = vi.fn(async () => ({ success: true, message: "ok" }));
const deleteSpy = vi.fn(async () => ({ success: true, message: "ok" }));

let mockTags: Tag[] = [];
let mockAssignments: TagAssignment[] = [];

// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files (see ScheduledTasks.test.tsx).
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useTags: () => ({ tags: mockTags, loading: false, refetch: vi.fn() }),
  useTagAssignments: () => ({ tagAssignments: mockAssignments, loading: false }),
  useUpsertTag: () => ({ upsertTag: upsertSpy, loading: false }),
  useDeleteTag: () => ({ deleteTag: deleteSpy, loading: false }),
}));

const SYSTEM_TAG: Tag = {
  id: "pii",
  description: "Personally identifiable",
  appliesTo: ["column", "table"],
  isSystem: true,
  derived: false,
  reasonPolicy: "optional",
  expiresPolicy: "optional",
};
const USER_TAG: Tag = {
  id: "finance",
  description: "Finance domain",
  appliesTo: ["source", "table"],
  isSystem: false,
  derived: false,
  reasonPolicy: "required",
  expiresPolicy: "hidden",
};
// REQ-1443: computed off each table's own registration, so it carries no stored assignments.
const DERIVED_TAG: Tag = {
  id: "data_quality",
  description: "Lands data-quality scan outcomes",
  appliesTo: ["table"],
  isSystem: true,
  derived: true,
  reasonPolicy: "hidden",
  expiresPolicy: "hidden",
};
const COMMAND_TAG: Tag = {
  id: "audit",
  description: "Audited command",
  appliesTo: ["command"],
  isSystem: false,
  derived: false,
  reasonPolicy: "optional",
  expiresPolicy: "optional",
};

// Mantine Select in jsdom: floating-ui hides the detached dropdown (all rects are 0),
// so visible-only role queries miss the options. Scope by the input's aria-controls
// listbox and query with hidden: true.
async function pickPolicy(testid: string, label: string) {
  const input = screen.getByTestId(testid);
  fireEvent.click(input);
  await waitFor(() => {
    if (!input.getAttribute("aria-controls")) throw new Error("dropdown not open");
  });
  const listbox = document.getElementById(input.getAttribute("aria-controls") as string);
  fireEvent.click(
    within(listbox as HTMLElement).getByRole("option", { name: label, hidden: true }),
  );
}

describe("TagsTab", () => {
  beforeEach(() => {
    upsertSpy.mockClear();
    deleteSpy.mockClear();
    mockTags = [USER_TAG, SYSTEM_TAG, COMMAND_TAG, DERIVED_TAG];
    mockAssignments = [
      { tagId: "pii", objectType: "column", tableId: 1, columnName: "ssn" },
      { tagId: "pii", objectType: "column", tableId: 1, columnName: "email" },
      { tagId: "finance", objectType: "source", sourceId: "erp" },
    ];
  });

  it("filters the registry by the selected scope and counts assignments", () => {
    render(<TagsTab />);
    // Default scope is "source": only the user tag applies.
    expect(screen.getByTestId("tags-row-finance")).toBeTruthy();
    expect(screen.queryByTestId("tags-row-pii")).toBeNull();
    expect(within(screen.getByTestId("tags-row-finance")).getByText("1")).toBeTruthy();

    // Switch to "table": both tags apply, system tag sorts first.
    const bar = screen.getByTestId("tags-scope-bar");
    fireEvent.click(within(bar).getByLabelText(t("tagsTab.scope_table")));
    const rows = screen.getAllByTestId(/^tags-row-/);
    // Derived tags are code-defined like system ones, so they sort with them, ahead of user tags.
    expect(rows.map((r) => r.getAttribute("data-testid"))).toEqual([
      "tags-row-data_quality",
      "tags-row-pii",
      "tags-row-finance",
    ]);
    expect(within(screen.getByTestId("tags-row-pii")).getByText("2")).toBeTruthy();

    // "relationship": neither applies.
    fireEvent.click(within(bar).getByLabelText(t("tagsTab.scope_relationship")));
    expect(screen.queryAllByTestId(/^tags-row-/)).toHaveLength(0);
    expect(screen.getByText(t("tagsTab.empty"))).toBeTruthy();

    // "command" segment exists; only the command-scoped tag applies.
    fireEvent.click(within(bar).getByLabelText(t("tagsTab.scope_command")));
    const cmdRows = screen.getAllByTestId(/^tags-row-/);
    expect(cmdRows.map((r) => r.getAttribute("data-testid"))).toEqual(["tags-row-audit"]);
  });

  it("shows a lock instead of edit/delete actions for a system tag", () => {
    render(<TagsTab />);
    fireEvent.click(
      within(screen.getByTestId("tags-scope-bar")).getByLabelText(t("tagsTab.scope_table")),
    );
    const systemRow = screen.getByTestId("tags-row-pii");
    expect(screen.queryByTestId("tags-delete-pii")).toBeNull();
    expect(within(systemRow).getByLabelText(t("tagsTab.systemTag"))).toBeTruthy();
    // The user tag keeps its delete action.
    expect(screen.getByTestId("tags-delete-finance")).toBeTruthy();
  });

  it("marks a derived tag rather than counting assignments it can never have", () => {
    render(<TagsTab />);
    fireEvent.click(
      within(screen.getByTestId("tags-scope-bar")).getByLabelText(t("tagsTab.scope_table")),
    );
    const row = screen.getByTestId("tags-row-data_quality");
    expect(within(row).getByText(t("tagsTab.derivedTag"))).toBeTruthy();
    // A "0" here would read as an unused tag, when the tag may hold on every table in the estate.
    expect(within(row).queryByText("0")).toBeNull();
    expect(within(row).getByLabelText(t("tagsTab.derivedTagHelp"))).toBeTruthy();
  });

  it("creates a tag through upsertTag with id, description and appliesTo", async () => {
    render(<TagsTab />);
    fireEvent.click(screen.getByTestId("tags-create-button"));
    fireEvent.change(screen.getByLabelText(t("tagsTab.idLabel")), {
      target: { value: "gdpr" },
    });
    fireEvent.change(screen.getByLabelText(t("tagsTab.descriptionLabel")), {
      target: { value: "GDPR relevant" },
    });
    // "source" is preselected from the active scope; add "column".
    fireEvent.click(screen.getByRole("checkbox", { name: t("tagsTab.scope_column") }));
    fireEvent.click(screen.getByRole("button", { name: t("tagsTab.createButton") }));
    await waitFor(() =>
      expect(upsertSpy).toHaveBeenCalledWith(
        "gdpr",
        "GDPR relevant",
        ["source", "column"],
        "optional",
        "optional",
      ),
    );
  });

  it("passes the selected reason and expiry policies to upsertTag", async () => {
    render(<TagsTab />);
    fireEvent.click(screen.getByTestId("tags-create-button"));
    fireEvent.change(screen.getByLabelText(t("tagsTab.idLabel")), {
      target: { value: "gdpr" },
    });
    await pickPolicy("tags-reason-policy", t("tagsTab.policy_required"));
    await pickPolicy("tags-expires-policy", t("tagsTab.policy_hidden"));
    fireEvent.click(screen.getByRole("button", { name: t("tagsTab.createButton") }));
    await waitFor(() =>
      expect(upsertSpy).toHaveBeenCalledWith("gdpr", "", ["source"], "required", "hidden"),
    );
  });

  it("shows policy badges in the row only when a policy is not optional", () => {
    render(<TagsTab />);
    // Default scope "source": finance (required/hidden) shows both badges.
    const financeRow = screen.getByTestId("tags-row-finance");
    expect(
      within(financeRow).getByText(
        t("tagsTab.reasonPolicyBadge", { policy: t("tagsTab.policy_required") }),
      ),
    ).toBeTruthy();
    expect(
      within(financeRow).getByText(
        t("tagsTab.expiresPolicyBadge", { policy: t("tagsTab.policy_hidden") }),
      ),
    ).toBeTruthy();
    // pii is optional/optional: no badges in its row.
    fireEvent.click(
      within(screen.getByTestId("tags-scope-bar")).getByLabelText(t("tagsTab.scope_table")),
    );
    const piiRow = screen.getByTestId("tags-row-pii");
    expect(within(piiRow).queryByText(/Reason:/)).toBeNull();
    expect(within(piiRow).queryByText(/Expiry:/)).toBeNull();
  });

  it("edit pre-fills the tag's policies", () => {
    render(<TagsTab />);
    fireEvent.click(
      within(screen.getByTestId("tags-row-finance")).getByLabelText(
        t("tagsTab.editTag", { id: "finance" }),
      ),
    );
    expect(screen.getByTestId("tags-reason-policy")).toHaveValue(t("tagsTab.policy_required"));
    expect(screen.getByTestId("tags-expires-policy")).toHaveValue(t("tagsTab.policy_hidden"));
  });

  it("deletes a tag after window.confirm", async () => {
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(true);
    render(<TagsTab />);
    fireEvent.click(screen.getByTestId("tags-delete-finance"));
    await waitFor(() => expect(deleteSpy).toHaveBeenCalledWith("finance"));
    expect(confirmSpy).toHaveBeenCalled();
    confirmSpy.mockRestore();
  });

  it("does not delete when the confirm is dismissed", async () => {
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(false);
    render(<TagsTab />);
    fireEvent.click(screen.getByTestId("tags-delete-finance"));
    expect(deleteSpy).not.toHaveBeenCalled();
    confirmSpy.mockRestore();
  });
});
