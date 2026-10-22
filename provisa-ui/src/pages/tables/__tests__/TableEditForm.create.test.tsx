// Copyright (c) 2026 Kenneth Stott
// Canary: 858c808c-25f3-4756-bac6-9d1fa39c4d2a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2109 (settled): every setting of a table or view is offered where it is created. The
// create form is the editor's own sections on the table as it will be registered: the same seven
// tabs, each with the editor's fields; what needs a saved table (writing a description from it,
// running a check) waits until it exists, and its write grants are kept to be saved with it.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../../test-utils/render";

vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
}));

import { TableEditForm } from "../TableEditForm";
import { draftFromRegistration, UNREGISTERED_ID } from "../draftTable";
import { registrationInput } from "../useCreateTable";
import { TABLE_SECTIONS, type TableSection } from "../tableSections";
import type { RegisteredTable } from "../../../types/admin";

const REGISTRATION = {
  input: {
    sourceId: "sales-pg",
    domainId: "sales",
    schemaName: "public",
    tableName: "trades",
    alias: "trades",
    description: "Trades as booked",
    discover: false,
    columns: [
      { name: "trade_id", dataType: "integer", visibleTo: ["analyst"], scope: "domain" },
      { name: "trade_date", dataType: "date", visibleTo: ["analyst"], scope: "domain" },
      { name: "updated_at", dataType: "timestamp", visibleTo: ["analyst"], scope: "domain" },
    ],
  },
  pagination: null,
  pagingKind: null,
  pagingCeilingRows: null,
  rowRules: [],
};

function renderForm(
  table: RegisteredTable,
  saved: RegisteredTable | null,
  section: TableSection,
  setEditingTable = vi.fn(),
) {
  render(
    <TableEditForm
      relationships={[]}
      section={section}
      onSectionChange={vi.fn()}
      savedProfilerId={null}
      editingTable={table}
      savedTable={saved}
      setEditingTable={setEditingTable}
      editingColumnTypes={{}}
      cacheTtlEdits={{}}
      setCacheTtlEdits={vi.fn()}
      sources={[]}
      roles={[{ id: "analyst" } as never]}
      dataProducts={[]}
      settings={null}
      saving={false}
      generateTableDescription={vi.fn()}
      generateColumnDescription={vi.fn()}
      cancelEditing={vi.fn()}
      handleSaveEdit={vi.fn()}
      updateEditCol={vi.fn()}
    />,
  );
  return setEditingTable;
}

const draft = draftFromRegistration(REGISTRATION);

describe("TableEditForm creating a table (REQ-2109)", () => {
  it("is the table as it will be registered, not yet given an id", () => {
    expect(draft).toMatchObject({
      id: UNREGISTERED_ID,
      tableName: "trades",
      draft: true,
      materialize: false,
      businessKey: [],
    });
    expect(draft.columns.map((c) => c.columnName)).toEqual([
      "trade_id",
      "trade_date",
      "updated_at",
    ]);
  });

  for (const section of TABLE_SECTIONS) {
    it(`shows the ${section} section the editor shows`, () => {
      renderForm(draft, null, section);
      for (const tab of TABLE_SECTIONS)
        expect(screen.getByTestId(`table-section-${tab}`)).toBeInTheDocument();
      expect(screen.getByTestId(`table-section-${section}`)).toHaveAttribute(
        "aria-selected",
        "true",
      );
      expect(screen.getByTestId("table-edit-save")).toBeInTheDocument();
    });
  }

  it("offers the same fields on each section as the editor of the saved table", () => {
    const saved = { ...draft, id: 4 };
    for (const section of ["identity", "load", "query", "quality"] as TableSection[]) {
      renderForm(saved, saved, section);
      const editing = [...document.querySelectorAll("label")].map((l) => l.textContent);
      document.body.innerHTML = "";
      renderForm(draft, null, section);
      const creating = [...document.querySelectorAll("label")].map((l) => l.textContent);
      document.body.innerHTML = "";
      expect(creating, section).toEqual(editing);
    }
  });

  it("does not offer to write a description from a table that does not exist yet", () => {
    renderForm(draft, null, "identity");
    expect(screen.queryByTestId("description-field-generate")).toBeNull();
  });

  it("keeps its write grants to save with the table", async () => {
    const setEditingTable = renderForm(draft, null, "writes");
    fireEvent.click(screen.getByTestId("write-grant-add"));
    fireEvent.click(screen.getByTestId("write-grants-save"));
    await waitFor(() =>
      expect(setEditingTable).toHaveBeenCalledWith(
        expect.objectContaining({
          writeGrants: [expect.objectContaining({ operation: "insert" })],
        }),
      ),
    );
    expect(await screen.findByTestId("write-grants-saved")).toHaveTextContent(
      "Kept: saved with the table.",
    );
  });

  it("registers every setting of the form in one input, its own-mutation settings with it", () => {
    const set: RegisteredTable = {
      ...draft,
      enableAggregates: true,
      alias: "trade_log",
      gqlNamingConvention: "snake",
      roleTtl: [{ role: "analyst", ttl: 60 }],
      replicate: -1,
      draft: false,
      writeGrants: [
        {
          role: "analyst",
          operation: "update",
          columns: ["trade_date"],
          rows: null,
          check: null,
          presets: [],
        },
      ],
    };
    const input = registrationInput(set, REGISTRATION, 600);
    expect(input).toMatchObject({
      sourceId: "sales-pg",
      tableName: "trades",
      alias: "trade_log",
      enableAggregates: true,
      cacheTtl: 600,
      discover: false,
      gqlNamingConvention: "snake",
      roleTtl: [{ role: "analyst", ttl: 60 }],
      replicate: -1,
      draft: false,
      writeGrants: [{ role: "analyst", operation: "update", columns: ["trade_date"] }],
    });
    expect((input.columns as { name: string }[]).map((c) => c.name)).toEqual([
      "trade_id",
      "trade_date",
      "updated_at",
    ]);
  });

  it("sends no write grants when the form set none: they are governance of their own", () => {
    expect(registrationInput(draft, REGISTRATION, null)).not.toHaveProperty("writeGrants");
  });
});
