// Copyright (c) 2026 Kenneth Stott
// Canary: d4a9c1e7-25b8-4f3e-8c60-7e1b9a2f5d13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234 (EDITORS FIRST): a table that cannot be picked among several -- it needs input, or
// its source needs a Cache TTL for each table -- is registered on its own by a Register action on
// its row: it is handed over as it will be registered, with discovery's defaults, and opened in
// the table's detail panel in edit, where what is missing is given; its Save registers it as a
// draft.

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../../../test-utils/render";
import type { ColumnMetadata } from "../../../api/admin";

vi.mock("../../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({ ensureDomainChecked: vi.fn() }),
}));

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["public"], loading: false }),
  useAvailableTables: () => ({
    tables: [
      { name: "orders", comment: "Every order", pagingKind: null, pagination: null },
      { name: "events", comment: null, columnsDeclared: true },
      { name: "blobs", comment: "Raw payloads" },
      { name: "invoices", comment: null },
    ],
    loading: false,
  }),
}));

// The kind's facts: whether a table of it lands (and so needs a Cache TTL its source lacks).
const kind = { tables_land: false };
vi.mock("../../sources/sourceKind", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../sources/sourceKind")>()),
  useSourceKind: () => kind,
}));

const COLUMNS: Record<string, ColumnMetadata[]> = {
  orders: [
    { name: "id", dataType: "integer", comment: null, nativeFilterType: null, isPrimaryKey: true },
  ],
  events: [],
  blobs: [
    { name: "id", dataType: "integer", comment: null, nativeFilterType: null, isPrimaryKey: true },
    { name: "payload", dataType: "", comment: null, nativeFilterType: null, isPrimaryKey: false },
  ],
};

const readColumns = vi.fn();
vi.mock("../../../hooks/useAvailableColumnsMetadataLazy", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAvailableColumnsMetadataLazy")>()),
  useReadColumns: () => readColumns,
}));

const registerTables = vi.fn();
vi.mock("../../../hooks/useTableQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useTableQueries")>()),
  useRegisterTables: () => ({ registerTables, loading: false }),
}));

vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
  fetchTableUniqueConstraints: vi.fn().mockResolvedValue([]),
}));

// The table's class term reads the meta domain as the user: its own tests (termGrounding) hold it.
vi.mock("../TableClassTerm", () => ({ TableClassTerm: () => <span>class term</span> }));

const { RegisterTableForm } = await import("../RegisterTableForm");
const { TableEditForm } = await import("../TableEditForm");
const { draftFromRegistration, UNREGISTERED_ID } = await import("../draftTable");
const { registrationInput } = await import("../useCreateTable");

beforeEach(() => {
  kind.tables_land = false;
  readColumns.mockReset().mockImplementation(async (_s: string, _schema: string, table: string) => {
    if (table === "invoices") throw new Error("permission denied for table invoices");
    return COLUMNS[table];
  });
  registerTables.mockReset();
});

function renderForm(over: { source?: Record<string, unknown>; registered?: string[] } = {}) {
  const onContinue = vi.fn();
  const setError = vi.fn();
  render(
    <RegisterTableForm
      sources={[{ id: "pg", type: "mysql", allowedDomains: [], ...over.source } as never]}
      domainHints={["sales"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled={true}
      tables={(over.registered ?? []).map(
        (name, i) => ({ id: i + 1, sourceId: "pg", tableName: name, columns: [] }) as never,
      )}
      roles={[{ id: "admin" } as never, { id: "analyst" } as never]}
      suggestTableAlias={vi.fn(async (name: string) => `${name}_v`)}
      onContinue={onContinue}
      onCancel={() => {}}
      onProductsRegistered={vi.fn()}
      onRegisteredMany={vi.fn()}
      setError={setError}
      initialSourceId="pg"
      severalFirst
    />,
  );
  return { onContinue, setError };
}

const action = (name: string) => screen.queryByTestId(`register-many-table-register-${name}`);

// Every listed table read: the last one's answer is in.
async function listed() {
  await screen.findByText("Its columns could not be read: permission denied for table invoices");
  await waitFor(() => expect(action("blobs")).not.toBeNull());
}

describe("Register on the row of a table that cannot be picked among several", () => {
  it("is offered on each table that needs input, and on no other", async () => {
    renderForm({ registered: ["orders"] });
    await listed();
    expect(action("blobs")).toHaveTextContent("Register");
    expect(action("events")).not.toBeNull();
    // Registered already; or its columns could not be read, so nothing is known to register.
    expect(action("orders")).toBeNull();
    expect(action("invoices")).toBeNull();
  });

  it("is not offered on a table picked with the others", async () => {
    renderForm();
    await listed();
    await waitFor(() => expect(screen.getByTestId("register-many-table-orders")).toBeChecked());
    expect(action("orders")).toBeNull();
  });

  it("hands the table over as it will be registered, with discovery's defaults", async () => {
    const { onContinue } = renderForm();
    await listed();
    await userEvent.click(action("blobs") as HTMLElement);
    await waitFor(() => expect(onContinue).toHaveBeenCalledTimes(1));
    expect(registerTables).not.toHaveBeenCalled();
    const registration = onContinue.mock.calls[0][0];
    expect(registration.input).toMatchObject({
      sourceId: "pg",
      domainId: "sales",
      schemaName: "public",
      tableName: "blobs",
      alias: "blobs_v",
      description: "Raw payloads",
      discover: false,
    });
    // The column discovery could not type is handed over untyped: the editor asks its type.
    expect(
      (registration.input.columns as { name: string; dataType: string }[]).map((c) => [
        c.name,
        c.dataType,
      ]),
    ).toEqual([
      ["id", "integer"],
      ["payload", ""],
    ]);
  });

  it("opens it in the table's editor, where Save registers it as a draft", async () => {
    const { onContinue } = renderForm();
    await listed();
    await userEvent.click(action("blobs") as HTMLElement);
    await waitFor(() => expect(onContinue).toHaveBeenCalledTimes(1));
    const registration = onContinue.mock.calls[0][0];
    // TablesPage's startCreating: the table as it will be registered, in its edit form.
    const draft = draftFromRegistration(registration);
    expect(draft).toMatchObject({ id: UNREGISTERED_ID, tableName: "blobs", draft: true });
    document.body.innerHTML = "";
    render(
      <TableEditForm
        relationships={[]}
        section="identity"
        onSectionChange={vi.fn()}
        savedProfilerId={null}
        editingTable={draft}
        savedTable={null}
        setEditingTable={vi.fn()}
        editingColumnTypes={{}}
        cacheTtlEdits={{}}
        setCacheTtlEdits={vi.fn()}
        sources={[]}
        roles={[]}
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
    expect(screen.getByTestId("table-edit-table")).toHaveValue("blobs");
    expect(screen.getByTestId("table-edit-save")).toBeInTheDocument();
    expect(registrationInput(draft, registration, null)).toMatchObject({
      tableName: "blobs",
      draft: true,
    });
  });

  it("says why, and offers Register, on every table when its source needs a Cache TTL for each", async () => {
    kind.tables_land = true;
    const { onContinue } = renderForm({ source: { changeSignal: "ttl", cacheTtl: null } });
    await listed();
    const orders = screen.getByTestId("register-many-table-orders");
    expect(orders).not.toBeChecked();
    expect(orders).toBeDisabled();
    expect(
      screen.getByText(
        "Needs input: its source needs a Cache TTL for each table. Register it on its own.",
      ),
    ).toBeInTheDocument();
    await userEvent.click(action("orders") as HTMLElement);
    await waitFor(() => expect(onContinue).toHaveBeenCalledTimes(1));
    expect(onContinue.mock.calls[0][0].input.tableName).toBe("orders");
  });
});
