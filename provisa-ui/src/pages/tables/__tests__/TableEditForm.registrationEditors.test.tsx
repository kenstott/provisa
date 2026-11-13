// Copyright (c) 2026 Kenneth Stott
// Canary: 6b0e2f4a-91c3-4d58-a7e6-3c2d8f5b1a94
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234 (EDITORS FIRST): what Register Table sets that only some tables have -- a files
// table's glob and source-file column (REQ-788), a ServiceNow table's import set (REQ-1954), and
// reading its columns from the live source (REQ-252) -- is edited in the table's detail panel too:
// each shown only for the tables it applies to, saved through updateTable, and read back in view.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import { AvailableImportSets } from "../../../hooks/admin.graphql";

// The table's class term reads the meta domain as the user: its own tests (termGrounding) hold it.
vi.mock("../TableClassTerm", () => ({ TableClassTerm: () => <span>class term</span> }));

import { TableEditForm } from "../TableEditForm";
import { IdentityRead } from "../TableSectionsRead";
import { draftFromRegistration } from "../draftTable";
import { buildTableUpdateInput } from "../helpers";
import { registrationInput } from "../useCreateTable";
import type { TableSection } from "../tableSections";
import type { RegisteredTable, Source } from "../../../types/admin";

const REGISTRATION = {
  input: {
    sourceId: "src",
    domainId: "sales",
    schemaName: "main",
    tableName: "orders",
    columns: [{ name: "id", dataType: "integer", visibleTo: ["analyst"], scope: "domain" }],
  },
  pagination: null,
  pagingKind: null,
  pagingCeilingRows: null,
  rowRules: [],
};

const saved = (over: Partial<RegisteredTable> = {}): RegisteredTable => ({
  ...draftFromRegistration(REGISTRATION),
  id: 7,
  ...over,
});

const source = (type: string): Source => ({ id: "src", type }) as Source;

const importSets = (tableName: string) => [
  {
    request: {
      query: AvailableImportSets,
      variables: { sourceId: "src", schemaName: "main", tableName },
    },
    result: {
      data: {
        availableImportSets: [
          { __typename: "ImportSetType", name: "Order Import", importSet: "imp_order" },
        ],
      },
    },
  },
];

function renderForm(
  table: RegisteredTable,
  type: string,
  section: TableSection,
  opts: { hidingOnly?: boolean; mocks?: ReturnType<typeof importSets> } = {},
) {
  const setEditingTable = vi.fn();
  render(
    <TableEditForm
      relationships={[]}
      section={section}
      onSectionChange={vi.fn()}
      savedProfilerId={null}
      editingTable={table}
      savedTable={table}
      setEditingTable={setEditingTable}
      editingColumnTypes={{}}
      cacheTtlEdits={{}}
      setCacheTtlEdits={vi.fn()}
      sources={[source(type)]}
      roles={[{ id: "analyst" } as never]}
      dataProducts={[]}
      settings={null}
      saving={false}
      generateTableDescription={vi.fn()}
      generateColumnDescription={vi.fn()}
      cancelEditing={vi.fn()}
      handleSaveEdit={vi.fn()}
      updateEditCol={vi.fn()}
      hidingOnly={opts.hidingOnly}
    />,
    { mocks: opts.mocks ?? [] },
  );
  return setEditingTable;
}

/** The table a call of setEditingTable leaves, whether it was given a table or an updater. */
function applied(set: ReturnType<typeof vi.fn>, table: RegisteredTable): RegisteredTable {
  const arg = set.mock.calls.at(-1)?.[0];
  return typeof arg === "function" ? arg(table) : arg;
}

describe("a files table's glob and source-file column in its editor", () => {
  it("are edited on Identity for a table of a files source", () => {
    const table = saved({ fileGlob: "orders/*.csv", sourceFileColumn: "_file" });
    const set = renderForm(table, "files", "identity");
    expect(screen.getByTestId("register-table-file-glob")).toHaveValue("orders/*.csv");
    expect(screen.getByTestId("register-table-source-file-column")).toHaveValue("_file");
    fireEvent.change(screen.getByTestId("register-table-file-glob"), {
      target: { value: "orders/2026/*.csv" },
    });
    expect(applied(set, table)).toMatchObject({
      fileGlob: "orders/2026/*.csv",
      sourceFileColumn: "_file",
    });
    fireEvent.change(screen.getByTestId("register-table-source-file-column"), {
      target: { value: "loaded_from" },
    });
    expect(applied(set, table).sourceFileColumn).toBe("loaded_from");
  });

  it("clears the source-file column with the glob: one file names no file per row", () => {
    const table = saved({ fileGlob: "orders/*.csv", sourceFileColumn: "_file" });
    const set = renderForm(table, "csv", "identity");
    fireEvent.change(screen.getByTestId("register-table-file-glob"), { target: { value: "" } });
    expect(applied(set, table)).toMatchObject({ fileGlob: null, sourceFileColumn: null });
  });

  it("are not offered for a table of any other source, nor for a view", () => {
    renderForm(saved(), "postgresql", "identity");
    expect(screen.queryByTestId("register-table-file-glob")).toBeNull();
    expect(screen.queryByTestId("register-table-source-file-column")).toBeNull();
    document.body.innerHTML = "";
    renderForm(saved({ viewSql: "SELECT 1 AS id" }), "files", "identity");
    expect(screen.queryByTestId("register-table-file-glob")).toBeNull();
  });
});

describe("a ServiceNow table's import set in its editor", () => {
  it("is edited on Identity, keeping the stored one while the maps are asked for", async () => {
    const table = saved({ importSet: "imp_order" });
    const set = renderForm(table, "servicenow", "identity", { mocks: importSets("orders") });
    const select = await screen.findByTestId("register-table-import-set");
    expect(select).toHaveValue("Order Import (imp_order)");
    // Nothing was cleared while the instance's maps were being read.
    expect(set).not.toHaveBeenCalled();
    fireEvent.click(select);
    fireEvent.click(
      await screen.findByRole("option", {
        name: "None: write inserts to the table directly",
        hidden: true,
      }),
    );
    await waitFor(() => expect(set).toHaveBeenCalled());
    expect(applied(set, table).importSet).toBeNull();
  });

  it("is not offered for a table of any other source", async () => {
    renderForm(saved(), "postgresql", "identity", { mocks: importSets("orders") });
    await new Promise((r) => setTimeout(r, 50));
    expect(screen.queryByTestId("register-table-import-set")).toBeNull();
  });
});

describe("reading a table's columns from the live source in its editor", () => {
  it("is offered on Columns, and staged for the next save", () => {
    const table = saved();
    const set = renderForm(table, "mongodb", "columns");
    const tick = screen.getByTestId("discover-columns-checkbox");
    expect(tick).not.toBeChecked();
    fireEvent.click(tick);
    expect(applied(set, table).discover).toBe(true);
  });

  it.each([
    ["a view", "postgresql", { viewSql: "SELECT 1 AS id" }],
    ["a Cypher projection", "neo4j", { queryTemplate: "MATCH (n) RETURN n.id AS id" }],
    ["a native pipeline", "mongodb", { queryTemplate: "[]" }],
    ["a checker's results", "soda", {}],
    ["a profiler's results", "data_profiler", {}],
  ])("is not offered for %s", (_what, type, over) => {
    renderForm(saved(over as Partial<RegisteredTable>), type, "columns");
    expect(screen.queryByTestId("discover-columns-checkbox")).toBeNull();
  });

  it("is not offered to a caller who may change only how columns are hidden", () => {
    renderForm(saved(), "mongodb", "columns", { hidingOnly: true });
    expect(screen.queryByTestId("discover-columns-checkbox")).toBeNull();
  });
});

describe("what the editors set is saved through updateTable", () => {
  it("sends the glob, the source-file column, the import set and the reading of columns", () => {
    const input = buildTableUpdateInput(
      saved({
        fileGlob: "orders/*.csv",
        sourceFileColumn: "_file",
        importSet: "imp_order",
        discover: true,
      }),
    );
    expect(input).toMatchObject({
      fileGlob: "orders/*.csv",
      sourceFileColumn: "_file",
      importSet: "imp_order",
      discover: true,
    });
  });

  it("keeps the import set a save does not change, and reads no columns unless asked", () => {
    // Left out, updateTable stores no import set: a save of anything else unbound the table.
    const input = buildTableUpdateInput(saved({ importSet: "imp_order" }));
    expect(input).toMatchObject({ importSet: "imp_order", discover: false });
  });

  it("registers with what Register Table chose: its import set and its reading of columns", () => {
    const registration = {
      ...REGISTRATION,
      input: { ...REGISTRATION.input, importSet: "imp_order", discover: true },
    };
    const draft = draftFromRegistration(registration);
    expect(registrationInput(draft, registration, null)).toMatchObject({
      importSet: "imp_order",
      discover: true,
    });
  });
});

describe("the same values in the read view", () => {
  it("shows a ServiceNow table's import set, or that it has none", () => {
    render(
      <IdentityRead table={saved({ importSet: "imp_order" })} source={source("servicenow")} />,
    );
    expect(screen.getByTestId("table-identity-import-set")).toHaveTextContent("imp_order");
    document.body.innerHTML = "";
    render(<IdentityRead table={saved({ importSet: null })} source={source("servicenow")} />);
    expect(screen.getByTestId("table-identity-import-set")).toHaveTextContent(
      "None: write inserts to the table directly",
    );
  });

  it("shows no import set for a table of any other source", () => {
    render(<IdentityRead table={saved()} source={source("postgresql")} />);
    expect(screen.queryByTestId("table-identity-import-set")).toBeNull();
  });

  it("shows a files table's glob and source-file column", () => {
    render(
      <IdentityRead
        table={saved({ fileGlob: "orders/*.csv", sourceFileColumn: "_file" })}
        source={source("files")}
      />,
    );
    expect(screen.getByText("orders/*.csv")).toBeInTheDocument();
    expect(screen.getByText("_file")).toBeInTheDocument();
  });
});
