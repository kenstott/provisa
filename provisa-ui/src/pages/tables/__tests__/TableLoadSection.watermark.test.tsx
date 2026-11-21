// Copyright (c) 2026 Kenneth Stott
// Canary: 4e7b2d9a-6c13-4f85-b0a7-8d3e1f5c2b96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234 (EDITORS FIRST): Register Table asks no watermark: a table is registered with the one
// discovery gives it (its updated_at), and the panel's Load and timeliness section edits it, on
// a table opened unsaved through Register and on a saved one alike.

import { useEffect, useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, within } from "../../../test-utils/render";
import type { RegisteredTable } from "../../../types/admin";

vi.mock("../TableClassTerm", () => ({ TableClassTerm: () => <span>class term</span> }));

const { TableEditForm } = await import("../TableEditForm");
const { draftFromRegistration } = await import("../draftTable");

const seen: { table: RegisteredTable | null } = { table: null };
const report = (table: RegisteredTable | null) => {
  seen.table = table;
};

function Panel({ saved }: { saved: boolean }) {
  const [table, setTable] = useState<RegisteredTable | null>(() => {
    const draft = draftFromRegistration({
      input: {
        sourceId: "pg",
        domainId: "sales",
        schemaName: "public",
        tableName: "orders",
        watermarkColumn: "updated_at",
        columns: [
          { name: "id", dataType: "integer" },
          { name: "updated_at", dataType: "timestamp" },
          { name: "created_at", dataType: "timestamp" },
          { name: "note", dataType: "text" },
        ],
      },
      pagination: null,
      pagingKind: null,
      pagingCeilingRows: null,
      rowRules: [],
    });
    return saved ? { ...draft, id: 4, draft: false } : draft;
  });
  useEffect(() => report(table), [table]);
  if (table === null) return null;
  return (
    <TableEditForm
      relationships={[]}
      section="load"
      onSectionChange={vi.fn()}
      savedProfilerId={null}
      editingTable={table}
      savedTable={saved ? table : null}
      setEditingTable={setTable}
      editingColumnTypes={{}}
      cacheTtlEdits={{}}
      setCacheTtlEdits={vi.fn()}
      sources={[{ id: "pg", type: "postgresql" } as never]}
      roles={[]}
      dataProducts={[]}
      settings={null}
      saving={false}
      generateTableDescription={vi.fn()}
      generateColumnDescription={vi.fn()}
      cancelEditing={vi.fn()}
      handleSaveEdit={vi.fn()}
      updateEditCol={vi.fn()}
    />
  );
}

describe("the watermark in the table's Load and timeliness section", () => {
  it.each([
    ["an unsaved table opened through Register", false],
    ["a saved table", true],
  ])("is edited on %s, from its watermark-eligible columns", (_what, saved) => {
    render(<Panel saved={saved} />);
    const field = screen.getByTestId("table-edit-watermark");
    expect(field).toHaveValue("updated_at (timestamp)");
    fireEvent.click(field);
    // Mantine keeps the drop-down out of the accessibility tree in jsdom: reach it through the
    // input's aria-controls listbox.
    const listbox = document.getElementById(field.getAttribute("aria-controls") as string);
    const options = within(listbox as HTMLElement).getAllByRole("option", { hidden: true });
    expect(options.map((o) => o.textContent)).toEqual([
      "id (integer)",
      "updated_at (timestamp)",
      "created_at (timestamp)",
    ]);
    fireEvent.click(options[2]);
    expect(seen.table?.watermarkColumn).toBe("created_at");
  });
});
