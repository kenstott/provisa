// Copyright (c) 2026 Kenneth Stott
// Canary: d4b7f0e2-8a15-4c39-b6d1-5f9e2a7c8310
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "../test-utils/render";
import type { Tag, TagAssignment } from "../types/admin";
import { TagControl } from "../components/TagControl";

const assignSpy = vi.fn(async () => ({ success: true, message: "ok" }));
const unassignSpy = vi.fn(async () => ({ success: true, message: "ok" }));

let mockTags: Tag[] = [];
let mockAssignments: TagAssignment[] = [];

// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files (see ScheduledTasks.test.tsx).
vi.mock("../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useTags: () => ({ tags: mockTags, loading: false }),
  useTagAssignments: () => ({ tagAssignments: mockAssignments, refetch: vi.fn() }),
  useAssignTag: () => ({ assignTag: assignSpy }),
  useUnassignTag: () => ({ unassignTag: unassignSpy }),
}));

describe("TagControl", () => {
  beforeEach(() => {
    assignSpy.mockClear();
    unassignSpy.mockClear();
    mockTags = [
      {
        id: "deprecated",
        description: "Marked for removal",
        appliesTo: ["source", "table", "column", "relationship"],
        isSystem: true,
        derived: false,
        reasonPolicy: "required",
        expiresPolicy: "optional",
        paramPolicy: "none",
        paramValues: [],
      },
      {
        id: "pii",
        description: "Personally identifiable",
        appliesTo: ["column"],
        isSystem: true,
        derived: false,
        reasonPolicy: "optional",
        expiresPolicy: "hidden",
        paramPolicy: "none",
        paramValues: [],
      },
      {
        id: "finance",
        description: "Finance domain",
        appliesTo: ["table"],
        isSystem: false,
        derived: false,
        reasonPolicy: "hidden",
        expiresPolicy: "required",
        paramPolicy: "none",
        paramValues: [],
      },
      {
        id: "beta",
        description: "Beta command",
        appliesTo: ["command"],
        isSystem: false,
        derived: false,
        reasonPolicy: "optional",
        expiresPolicy: "optional",
        paramPolicy: "none",
        paramValues: [],
      },
      {
        id: "data_quality",
        description: "Lands data-quality scan outcomes",
        appliesTo: ["table"],
        isSystem: true,
        derived: true,
        reasonPolicy: "hidden",
        expiresPolicy: "hidden",
        paramPolicy: "none",
        paramValues: [],
      },
      {
        id: "entity",
        description: "Names the entity type this column holds",
        appliesTo: ["column"],
        isSystem: true,
        derived: false,
        reasonPolicy: "hidden",
        expiresPolicy: "hidden",
        paramPolicy: "required",
        paramValues: [
          { value: "customer", description: "A buying party" },
          { value: "vendor", description: "" },
        ],
      },
    ];
    mockAssignments = [
      {
        tagId: "deprecated",
        objectType: "table",
        tableId: 7,
        reason: "Replaced by orders_v2",
        expiresOn: "2020-01-01",
      },
      { tagId: "finance", objectType: "table", tableId: 8 },
      { tagId: "pii", objectType: "column", tableId: 7, columnName: "email" },
      { tagId: "deprecated", objectType: "source", sourceId: "pg", reason: "Legacy source" },
      { tagId: "beta", objectType: "command", commandName: "send_email" },
      { tagId: "entity:customer", objectType: "column", tableId: 7, columnName: "cust_name" },
    ];
  });

  it("renders pills only for this object's assignments", () => {
    render(<TagControl objectType="table" tableId={7} />);
    expect(screen.getByTestId("tag-pill-deprecated")).toBeInTheDocument();
    expect(screen.queryByTestId("tag-pill-finance")).toBeNull();
    expect(screen.queryByTestId("tag-pill-pii")).toBeNull();
  });

  it("picker lists only tags whose appliesTo includes the objectType", async () => {
    render(<TagControl objectType="table" tableId={7} />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    await screen.findByTestId("tag-option-deprecated");
    expect(screen.getByTestId("tag-option-finance")).toBeInTheDocument();
    expect(screen.queryByTestId("tag-option-pii")).toBeNull();
  });

  it("picker never offers a derived tag, which the server refuses to assign", async () => {
    // REQ-1443: the tag reports what the table's own registration already says. A checkbox for it
    // would be a control whose only outcome is an error.
    render(<TagControl objectType="table" tableId={7} />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    await screen.findByTestId("tag-option-deprecated");
    expect(screen.queryByTestId("tag-option-data_quality")).toBeNull();
  });

  it("checking opens the inline form; Apply calls assignTag with identifiers and reason", async () => {
    render(<TagControl objectType="column" tableId={7} columnName="name" />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    const option = await screen.findByTestId("tag-option-pii");
    fireEvent.click(option);
    // Checking no longer assigns immediately; it opens the inline form.
    expect(assignSpy).not.toHaveBeenCalled();
    fireEvent.click(screen.getByTestId("tag-apply-pii"));
    expect(assignSpy).toHaveBeenCalledWith({
      tagId: "pii",
      objectType: "column",
      sourceId: undefined,
      tableId: 7,
      columnName: "name",
      relationshipId: undefined,
      reason: null,
      expiresOn: null,
    });
    expect(unassignSpy).not.toHaveBeenCalled();
  });

  it("cancel collapses the form without assigning", async () => {
    render(<TagControl objectType="table" tableId={8} />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    fireEvent.click(await screen.findByTestId("tag-option-deprecated"));
    fireEvent.click(screen.getByTestId("tag-cancel-deprecated"));
    expect(screen.queryByTestId("tag-apply-deprecated")).toBeNull();
    expect(screen.getByTestId("tag-option-deprecated")).not.toBeChecked();
    expect(assignSpy).not.toHaveBeenCalled();
  });

  it("hidden reasonPolicy hides the reason input; required expiresPolicy gates Apply", async () => {
    render(<TagControl objectType="table" tableId={9} />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    fireEvent.click(await screen.findByTestId("tag-option-finance"));
    expect(screen.queryByTestId("tag-reason-finance")).toBeNull();
    const apply = screen.getByTestId("tag-apply-finance");
    expect(apply).toBeDisabled();
    fireEvent.change(screen.getByTestId("tag-expires-finance"), {
      target: { value: "2027-06-30" },
    });
    expect(apply).not.toBeDisabled();
    fireEvent.click(apply);
    expect(assignSpy).toHaveBeenCalledWith({
      tagId: "finance",
      objectType: "table",
      sourceId: undefined,
      tableId: 9,
      columnName: undefined,
      relationshipId: undefined,
      reason: null,
      expiresOn: "2027-06-30",
    });
  });

  it("hidden expiresPolicy hides the expiry input", async () => {
    render(<TagControl objectType="column" tableId={7} columnName="name" />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    fireEvent.click(await screen.findByTestId("tag-option-pii"));
    expect(screen.getByTestId("tag-reason-pii")).toBeInTheDocument();
    expect(screen.queryByTestId("tag-expires-pii")).toBeNull();
    expect(screen.getByTestId("tag-apply-pii")).not.toBeDisabled();
  });

  it("required reasonPolicy disables Apply until a reason is entered; Apply passes reason and expiresOn", async () => {
    render(<TagControl objectType="table" tableId={8} />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    fireEvent.click(await screen.findByTestId("tag-option-deprecated"));
    const apply = screen.getByTestId("tag-apply-deprecated");
    expect(apply).toBeDisabled();
    fireEvent.change(screen.getByTestId("tag-reason-deprecated"), {
      target: { value: "Superseded" },
    });
    fireEvent.change(screen.getByTestId("tag-expires-deprecated"), {
      target: { value: "2027-01-31" },
    });
    expect(apply).not.toBeDisabled();
    fireEvent.click(apply);
    expect(assignSpy).toHaveBeenCalledWith({
      tagId: "deprecated",
      objectType: "table",
      sourceId: undefined,
      tableId: 8,
      columnName: undefined,
      relationshipId: undefined,
      reason: "Superseded",
      expiresOn: "2027-01-31",
    });
  });

  it("unchecking an assigned tag calls unassignTag immediately", async () => {
    render(<TagControl objectType="source" sourceId="pg" />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    const option = await screen.findByTestId("tag-option-deprecated");
    expect(option).toBeChecked();
    fireEvent.click(option);
    expect(unassignSpy).toHaveBeenCalledWith({
      tagId: "deprecated",
      objectType: "source",
      sourceId: "pg",
      tableId: undefined,
      columnName: undefined,
      relationshipId: undefined,
    });
    expect(assignSpy).not.toHaveBeenCalled();
  });

  it("pill tooltip shows the registry description, reason, and planned removal", async () => {
    render(<TagControl objectType="table" tableId={7} />);
    fireEvent.mouseEnter(screen.getByTestId("tag-pill-deprecated"));
    expect(await screen.findByText("Marked for removal")).toBeInTheDocument();
    expect(screen.getByText("Replaced by orders_v2")).toBeInTheDocument();
    expect(screen.getByText("Planned removal: 2020-01-01")).toBeInTheDocument();
  });

  it("deprecated pill switches to filled variant when expiresOn is past", () => {
    render(<TagControl objectType="table" tableId={7} />);
    expect(screen.getByTestId("tag-pill-deprecated")).toHaveAttribute("data-variant", "filled");
  });

  it("edit affordance pre-fills the form and Apply re-assigns with the new reason", async () => {
    render(<TagControl objectType="source" sourceId="pg" />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    fireEvent.click(await screen.findByTestId("tag-edit-deprecated"));
    const reasonInput = screen.getByTestId("tag-reason-deprecated");
    expect(reasonInput).toHaveValue("Legacy source");
    fireEvent.change(reasonInput, { target: { value: "Retiring Q3" } });
    fireEvent.click(screen.getByTestId("tag-apply-deprecated"));
    expect(assignSpy).toHaveBeenCalledWith({
      tagId: "deprecated",
      objectType: "source",
      sourceId: "pg",
      tableId: undefined,
      columnName: undefined,
      relationshipId: undefined,
      reason: "Retiring Q3",
      expiresOn: null,
    });
  });

  it("command objectType: pills match by commandName, picker filters to command-scoped tags, assign carries commandName", async () => {
    render(<TagControl objectType="command" commandName="refund_order" />);
    // "send_email" holds the beta assignment; this command has none.
    expect(screen.queryByTestId("tag-pill-beta")).toBeNull();
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    const option = await screen.findByTestId("tag-option-beta");
    expect(screen.queryByTestId("tag-option-deprecated")).toBeNull();
    expect(screen.queryByTestId("tag-option-finance")).toBeNull();
    expect(screen.queryByTestId("tag-option-pii")).toBeNull();
    expect(option).not.toBeChecked();
    fireEvent.click(option);
    fireEvent.click(screen.getByTestId("tag-apply-beta"));
    expect(assignSpy).toHaveBeenCalledWith({
      tagId: "beta",
      objectType: "command",
      sourceId: undefined,
      tableId: undefined,
      columnName: undefined,
      relationshipId: undefined,
      commandName: "refund_order",
      reason: null,
      expiresOn: null,
    });
  });

  it("command objectType: renders the pill for its own assignment", () => {
    render(<TagControl objectType="command" commandName="send_email" />);
    expect(screen.getByTestId("tag-pill-beta")).toBeInTheDocument();
  });

  // REQ-1467: a parameterized tag is assigned as "entity:customer" — the parameter is part of the
  // assignment, and the registry row it belongs to is the base id.
  it("pill shows the assignment's parameter, not the bare registry id", () => {
    render(<TagControl objectType="column" tableId={7} columnName="cust_name" />);
    expect(screen.getByTestId("tag-pill-entity")).toHaveTextContent("entity:customer");
  });

  it("required paramPolicy gates Apply until a value is picked, then sends the parameterized id", async () => {
    render(<TagControl objectType="column" tableId={7} columnName="supplier" />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    fireEvent.click(await screen.findByTestId("tag-option-entity"));
    const apply = screen.getByTestId("tag-apply-entity");
    expect(apply).toBeDisabled();
    fireEvent.click(screen.getByTestId("tag-param-entity"));
    fireEvent.click(await screen.findByRole("option", { name: "vendor" }));
    expect(apply).not.toBeDisabled();
    fireEvent.click(apply);
    expect(assignSpy).toHaveBeenCalledWith({
      tagId: "entity:vendor",
      objectType: "column",
      sourceId: undefined,
      tableId: 7,
      columnName: "supplier",
      relationshipId: undefined,
      commandName: undefined,
      reason: null,
      expiresOn: null,
    });
  });

  it("editing an existing parameterized assignment seeds the picked value", async () => {
    render(<TagControl objectType="column" tableId={7} columnName="cust_name" />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    fireEvent.click(await screen.findByTestId("tag-edit-entity"));
    expect(screen.getByTestId("tag-param-entity")).toHaveValue("customer — A buying party");
  });

  it("unassigning a parameterized tag sends the base id, which the server matches on", async () => {
    render(<TagControl objectType="column" tableId={7} columnName="cust_name" />);
    fireEvent.click(screen.getByTestId("tag-picker-toggle"));
    const option = await screen.findByTestId("tag-option-entity");
    expect(option).toBeChecked();
    fireEvent.click(option);
    expect(unassignSpy).toHaveBeenCalledWith({
      tagId: "entity",
      objectType: "column",
      sourceId: undefined,
      tableId: 7,
      columnName: "cust_name",
      relationshipId: undefined,
      commandName: undefined,
    });
  });

  it("readOnly renders pills without a picker icon", () => {
    render(<TagControl objectType="table" tableId={7} readOnly />);
    expect(screen.getByTestId("tag-pill-deprecated")).toBeInTheDocument();
    expect(screen.queryByTestId("tag-picker-toggle")).toBeNull();
  });
});
