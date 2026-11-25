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
import type { DatasetColumn } from "../../../api/actions";
import { CommandColumnsFields, CommandColumnsRead } from "../CommandColumns";
import { CommandSectionTabs } from "../CommandBindings";
import { COMMAND_SECTIONS } from "../bindingLanguages";

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
    render(<CommandColumnsRead columns={COLS} />);
    expect(screen.getByTestId("command-columns-table-col-ssn")).toHaveTextContent("tax_id");
    expect(screen.getByTestId("command-columns-table-col-ssn")).toHaveTextContent("Tax id");
    fireEvent.click(screen.getByTestId("column-tab-access"));
    expect(screen.getByTestId("command-columns-table-col-ssn")).toHaveTextContent("finance");
  });

  it("says so when the command declares no columns", () => {
    render(<CommandColumnsRead columns={[]} />);
    expect(screen.getByTestId("command-columns-table-empty")).toBeInTheDocument();
  });
});
