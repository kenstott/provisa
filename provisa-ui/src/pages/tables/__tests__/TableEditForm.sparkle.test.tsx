// Copyright (c) 2026 Kenneth Stott
// Canary: 6b2e4f81-9c3a-4d57-a0e2-5f7d1c8b3e96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A table picked from a source's table list goes on to the table editor before it is registered.
// There the table description and each column's description carry the AI describe (the sparkle):
// it is written from the source table the form will register -- its source, schema, name and
// columns -- through the same queries the Register table form uses.

import { describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import type { MockedResponse } from "@apollo/client/testing";
import { render, screen, waitFor, within } from "../../../test-utils/render";

vi.mock("../TableClassTerm", () => ({ TableClassTerm: () => <span>class term</span> }));

import { TableEditForm } from "../TableEditForm";
import { draftFromRegistration } from "../draftTable";
import type { TableSection } from "../tableSections";
import type { RegisteredTable } from "../../../types/admin";
import {
  GenerateUnregisteredColumnDescription,
  GenerateUnregisteredTableDescription,
} from "../../../hooks/admin.graphql";

const REGISTRATION = {
  input: {
    sourceId: "sales-pg",
    domainId: "sales",
    schemaName: "public",
    tableName: "trades",
    alias: "trades",
    description: "",
    discover: false,
    columns: [
      { name: "trade_id", dataType: "integer", visibleTo: ["analyst"], scope: "domain" },
      { name: "trade_date", dataType: "date", visibleTo: ["analyst"], scope: "domain" },
    ],
  },
  pagination: null,
  pagingKind: null,
  pagingCeilingRows: null,
  rowRules: [],
};
const draft = draftFromRegistration(REGISTRATION);
const TRADES = {
  sourceId: "sales-pg",
  schemaName: "public",
  tableName: "trades",
  columns: ["trade_id", "trade_date"],
};

function renderForm(
  table: RegisteredTable,
  saved: RegisteredTable | null,
  section: TableSection,
  mocks: MockedResponse[] = [],
) {
  const setEditingTable = vi.fn();
  const updateEditCol = vi.fn();
  const generateTableDescription = vi.fn().mockResolvedValue("Saved table described.");
  const generateColumnDescription = vi.fn().mockResolvedValue("Saved column described.");
  render(
    <TableEditForm
      relationships={[]}
      section={section}
      onSectionChange={vi.fn()}
      savedProfilerId={null}
      domainChoices={[]}
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
      generateTableDescription={generateTableDescription}
      generateColumnDescription={generateColumnDescription}
      cancelEditing={vi.fn()}
      handleSaveEdit={vi.fn()}
      updateEditCol={updateEditCol}
    />,
    { mocks },
  );
  return { setEditingTable, updateEditCol, generateTableDescription, generateColumnDescription };
}

const sparkles = () => screen.getAllByTestId("description-field-generate");

describe("TableEditForm describes a table it is creating", () => {
  it("fills the table description from the source table it will register", async () => {
    const { setEditingTable, generateTableDescription } = renderForm(draft, null, "identity", [
      {
        request: { query: GenerateUnregisteredTableDescription, variables: TRADES },
        result: { data: { generateTableDescription: "Trades as booked." } },
      },
    ]);
    await userEvent
      .setup()
      .click(
        within(screen.getByTestId("table-section-identity-panel")).getByTestId(
          "description-field-generate",
        ),
      );
    await waitFor(() =>
      expect(setEditingTable).toHaveBeenCalledWith(
        expect.objectContaining({ description: "Trades as booked." }),
      ),
    );
    expect(generateTableDescription).not.toHaveBeenCalled();
  });

  it("fills a column's description from that column of the source table", async () => {
    const { updateEditCol, generateColumnDescription } = renderForm(draft, null, "columns", [
      {
        request: {
          query: GenerateUnregisteredColumnDescription,
          variables: { ...TRADES, columnName: "trade_date" },
        },
        result: { data: { generateColumnDescription: "The day the trade was booked." } },
      },
    ]);
    // One sparkle per column, in column order.
    expect(sparkles()).toHaveLength(2);
    await userEvent.setup().click(sparkles()[1]);
    await waitFor(() =>
      expect(updateEditCol).toHaveBeenCalledWith(1, "description", "The day the trade was booked."),
    );
    expect(generateColumnDescription).not.toHaveBeenCalled();
  });

  it("offers none on a view being created: it reads no source table", () => {
    renderForm({ ...draft, viewSql: "SELECT 1 AS x" }, null, "identity");
    expect(screen.queryByTestId("description-field-generate")).toBeNull();
  });

  it("describes a saved table by its id, as before", async () => {
    const saved = { ...draft, id: 4 };
    const { setEditingTable, generateTableDescription } = renderForm(saved, saved, "identity");
    await userEvent.setup().click(sparkles()[0]);
    await waitFor(() =>
      expect(setEditingTable).toHaveBeenCalledWith(
        expect.objectContaining({ description: "Saved table described." }),
      ),
    );
    expect(generateTableDescription).toHaveBeenCalledWith(4);
  });

  it("describes a saved table's column by its id, as before", async () => {
    const saved = { ...draft, id: 4 };
    const { updateEditCol, generateColumnDescription } = renderForm(saved, saved, "columns");
    await userEvent.setup().click(sparkles()[0]);
    await waitFor(() =>
      expect(updateEditCol).toHaveBeenCalledWith(0, "description", "Saved column described."),
    );
    expect(generateColumnDescription).toHaveBeenCalledWith(4, "trade_id");
  });
});
