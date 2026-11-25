// Copyright (c) 2026 Kenneth Stott
// Canary: 3d8f1b64-9a2e-4c70-b5e1-6f4a0c9d7e28
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-25): a command's output columns are on a Columns tab of their own,
// edited and read with the table columns editor, carrying visibility, mask, SQL alias and description.

import { useState } from "react";
import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import type { TagAssignment } from "../../../types/admin";
import type { DatasetColumn } from "../../../api/actions";
import { CommandColumnsFields, CommandColumnsRead } from "../CommandColumns";
import { CommandSectionTabs } from "../CommandBindings";
import { COMMAND_SECTIONS } from "../bindingLanguages";

vi.mock("../../../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/AuthContext")>()),
  useAuth: () => ({ capabilities: ["table_registration", "glossary_read", "sensitive_data"] }),
}));

let mockAssignments: TagAssignment[] = [];
vi.mock("../../../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useTagQueries")>()),
  useTags: () => ({
    tags: [
      {
        id: "pii",
        description: "Personally identifiable information",
        appliesTo: ["column", "command_column"],
        isSystem: true,
        derived: false,
        reasonPolicy: "required",
        expiresPolicy: "hidden",
        paramPolicy: "none",
        requiresCitation: false,
        required: false,
        allowedDomains: [],
        citations: [],
        needsReview: false,
        paramValues: [],
      },
    ],
    loading: false,
  }),
  useTagAssignments: () => ({ tagAssignments: mockAssignments, refetch: vi.fn() }),
}));
vi.mock("../../../api/glossary", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/glossary")>()),
  fetchCommandColumnTerms: vi.fn(async () => ({ ssn: { id: 7, name: "tax identifier" } })),
}));
vi.mock("../../../api/profiler", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/profiler")>()),
  fetchCommandClassifications: vi.fn(async () => ({
    decisions: [
      {
        command_name: "lookup",
        physical_column: "ssn",
        kind: "us_ssn",
        status: "accepted",
        evidence: "a person's SSN",
        decided_by: "u1",
        decided_at: "2026-11-25T00:00:00Z",
      },
    ],
  })),
}));

const COLS: DatasetColumn[] = [
  { name: "ssn", type: "varchar", visible_to: ["finance"], alias: "tax_id", description: "Tax id" },
  { name: "status", type: "varchar" },
];

const ROLES = [
  { id: "finance", label: "finance" },
  { id: "support", label: "support" },
];

function Harness({ onChange }: { onChange?: (c: DatasetColumn[]) => void }) {
  const [cols, setCols] = useState(COLS);
  return (
    <CommandColumnsFields
      columns={cols}
      onChange={(c) => {
        setCols(c);
        onChange?.(c);
      }}
      roleOptions={ROLES}
    />
  );
}

describe("the Columns tab (REQ-1159)", () => {
  it("is the third section, beside Command and Bindings", () => {
    expect(COMMAND_SECTIONS).toEqual(["command", "bindings", "columns"]);
    render(<CommandSectionTabs value="command" onChange={() => {}} />);
    expect(screen.getByTestId("command-section-columns")).toHaveTextContent("Columns");
  });

  it("opens its section when asked for", () => {
    const onChange = vi.fn();
    render(<CommandSectionTabs value="command" onChange={onChange} />);
    fireEvent.click(screen.getByTestId("command-section-columns"));
    expect(onChange).toHaveBeenCalledWith("columns");
  });

  it("edits a column's alias and description with the table columns editor", () => {
    const onChange = vi.fn();
    render(<Harness onChange={onChange} />);
    fireEvent.change(screen.getAllByRole("textbox", { name: "SQL Alias" })[1], {
      target: { value: "state" },
    });
    expect(onChange).toHaveBeenLastCalledWith([
      COLS[0],
      { name: "status", type: "varchar", alias: "state" },
    ]);
    fireEvent.change(screen.getAllByRole("textbox", { name: "Description" })[1], {
      target: { value: "Order state" },
    });
    expect(onChange).toHaveBeenLastCalledWith([
      COLS[0],
      { name: "status", type: "varchar", alias: "state", description: "Order state" },
    ]);
  });

  it("sets who sees a column and its masks on the Access topic", () => {
    render(<Harness />);
    fireEvent.click(screen.getByTestId("column-tab-access"));
    expect(screen.getByTestId("register-mask-toggle-ssn")).toBeInTheDocument();
    expect(screen.getByTestId("register-mask-toggle-status")).toBeInTheDocument();
  });

  it("offers no stored-data settings, no type, key or scope", () => {
    render(<Harness />);
    expect(screen.queryByTestId("column-tab-type")).toBeNull();
    expect(screen.queryByTestId("column-tab-writeGrants")).toBeNull();
    expect(screen.queryByTestId("column-tab-testing")).toBeNull();
    expect(screen.queryByRole("checkbox")).toBeNull();
  });

  it("reads the columns as the table's read view does", () => {
    render(<CommandColumnsRead name="lookup" columns={COLS} />);
    expect(screen.getByTestId("command-columns-table-col-ssn")).toHaveTextContent("tax_id");
    expect(screen.getByTestId("command-columns-table-col-ssn")).toHaveTextContent("Tax id");
    fireEvent.click(screen.getByTestId("column-tab-access"));
    expect(screen.getByTestId("command-columns-table-col-ssn")).toHaveTextContent("finance");
  });

  it("says so when the command declares no columns", () => {
    render(<CommandColumnsRead name="lookup" columns={[]} />);
    expect(screen.getByTestId("command-columns-table-empty")).toBeInTheDocument();
  });

  it("shows each column's tags, term and classification read-only, linked to where they are edited", async () => {
    mockAssignments = [
      { tagId: "pii", objectType: "command_column", commandName: "lookup", columnName: "ssn" },
    ];
    render(<CommandColumnsRead name="lookup" columns={COLS} />);
    const ssn = screen.getByTestId("command-column-governance-ssn");
    expect(ssn).toHaveTextContent("pii");
    expect(screen.getByTestId("command-column-governance-status")).not.toHaveTextContent("pii");
    expect(await screen.findByText("tax identifier")).toBeInTheDocument();
    expect(await screen.findByText("us_ssn")).toBeInTheDocument();
    expect(screen.queryByTestId("tag-picker-toggle")).toBeNull();
    expect(screen.getByTestId("command-columns-tags-link")).toHaveTextContent("Tags page");
    expect(screen.getByTestId("command-columns-glossary-link")).toHaveTextContent("Glossary");
  });
});
