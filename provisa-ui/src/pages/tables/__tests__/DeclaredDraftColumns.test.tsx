// Copyright (c) 2026 Kenneth Stott
// Canary: 9d4a1c7e-2f58-4b36-8e09-5c1b7a3f6d24
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1924, REQ-2234: an OpenAPI table whose operation declares no response schema is registered
// on its own from Register Table, opened unsaved in its panel; the Columns section declares its
// columns there, and Save registers it with them. A saved table, or one whose source describes
// its columns, has no such editor.

import { useEffect, useState } from "react";
import { describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen } from "../../../test-utils/render";
import type { RegisteredTable } from "../../../types/admin";

// The table's class term reads the meta domain as the user: its own tests (termGrounding) hold it.
vi.mock("../TableClassTerm", () => ({ TableClassTerm: () => <span>class term</span> }));
vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
  fetchIrTypes: vi.fn().mockResolvedValue(["text", "bigint"]),
}));

const { TableEditForm } = await import("../TableEditForm");
const { draftFromRegistration } = await import("../draftTable");
const { registrationInput } = await import("../useCreateTable");

const REGISTRATION = {
  input: {
    sourceId: "shelter",
    domainId: "pets",
    schemaName: "openapi",
    tableName: "listDogs",
    columns: [
      {
        name: "_nf_site",
        dataType: "text",
        nativeFilterType: "path_param",
        nativeFilterRequired: true,
        visibleTo: ["admin"],
      },
    ],
  },
  pagination: null,
  pagingKind: null,
  pagingCeilingRows: null,
  rowRules: [],
  declaresColumns: true,
};

const seen: { table: RegisteredTable | null } = { table: null };
const report = (table: RegisteredTable | null) => {
  seen.table = table;
};

function Panel({ saved, declares }: { saved: boolean; declares: boolean }) {
  const [table, setTable] = useState<RegisteredTable | null>(() => {
    const draft = draftFromRegistration(REGISTRATION);
    return saved ? { ...draft, id: 9 } : draft;
  });
  useEffect(() => report(table), [table]);
  if (table === null) return null;
  return (
    <TableEditForm
      relationships={[]}
      section="columns"
      onSectionChange={vi.fn()}
      savedProfilerId={null}
      editingTable={table}
      savedTable={saved ? table : null}
      setEditingTable={setTable}
      editingColumnTypes={{}}
      cacheTtlEdits={{}}
      setCacheTtlEdits={vi.fn()}
      sources={[{ id: "shelter", type: "openapi" } as never]}
      roles={[{ id: "admin" } as never]}
      dataProducts={[]}
      settings={null}
      saving={false}
      generateTableDescription={vi.fn()}
      generateColumnDescription={vi.fn()}
      cancelEditing={vi.fn()}
      handleSaveEdit={vi.fn()}
      updateEditCol={vi.fn()}
      declaresColumns={declares}
    />
  );
}

describe("declaring an OpenAPI table's columns in its panel", () => {
  it("adds a column to the unsaved table, which its registration sends", async () => {
    render(<Panel saved={false} declares />);
    expect(screen.getByTestId("declared-columns")).toBeInTheDocument();
    const user = userEvent.setup();
    await user.click(screen.getByTestId("declared-column-add"));
    await user.type(await screen.findByTestId("declared-column-name-1"), "name");
    const table = seen.table as RegisteredTable;
    expect(table.columns.map((c) => [c.columnName, c.dataType])).toEqual([
      ["_nf_site", "text"],
      ["name", "text"],
    ]);
    const sent = registrationInput(table, REGISTRATION, null).columns as {
      name: string;
      nativeFilterType?: string;
    }[];
    expect(sent.map((c) => c.name)).toEqual(["_nf_site", "name"]);
    // The operation's parameter stays a parameter.
    expect(sent[0].nativeFilterType).toBe("path_param");
  });

  it("is not offered on a saved table, nor on one whose source describes its columns", () => {
    const { unmount } = render(<Panel saved declares />);
    expect(screen.queryByTestId("declared-columns")).toBeNull();
    unmount();
    render(<Panel saved={false} declares={false} />);
    expect(screen.queryByTestId("declared-columns")).toBeNull();
  });
});
