// Copyright (c) 2026 Kenneth Stott
// Canary: 8a3f6d21-4c97-4e5b-b0a2-d7e1c9f4b638
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234: Register Table picks several tables of a schema at once and registers each, in one
// action, with the defaults its registration alone would take. A table that needs input is
// listed, unticked, saying why; a table registered already cannot be picked again.

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor, within } from "../../../test-utils/render";
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
      { name: "customers", comment: null, pagingKind: null, pagination: null },
      { name: "events", comment: null, columnsDeclared: true },
      { name: "blobs", comment: null },
      { name: "invoices", comment: null },
    ],
    loading: false,
  }),
}));

const COLUMNS: Record<string, ColumnMetadata[]> = {
  orders: [
    { name: "id", dataType: "integer", comment: null, nativeFilterType: null, isPrimaryKey: true },
    {
      name: "updatedAt",
      dataType: "timestamp",
      comment: "Last change",
      nativeFilterType: null,
      isPrimaryKey: false,
    },
    {
      name: "updated_at",
      dataType: "timestamp",
      comment: null,
      nativeFilterType: null,
      isPrimaryKey: false,
    },
  ],
  customers: [
    { name: "id", dataType: "integer", comment: null, nativeFilterType: null, isPrimaryKey: true },
    // A parameter read from a typed schema needs no input: a query supplies it.
    {
      name: "_nf_region",
      dataType: "varchar",
      comment: null,
      nativeFilterType: "query_param",
      nativeFilterRequired: true,
      isPrimaryKey: false,
    },
  ],
  events: [],
  blobs: [
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

const { RegisterTableForm } = await import("../RegisterTableForm");

beforeEach(() => {
  readColumns.mockReset().mockImplementation(async (_s: string, _schema: string, table: string) => {
    if (table === "invoices") throw new Error("permission denied for table invoices");
    return COLUMNS[table];
  });
  registerTables.mockReset();
});

function renderForm(over: { severalFirst?: boolean; registered?: string[] } = {}) {
  const onRegisteredMany = vi.fn();
  const setError = vi.fn();
  const suggestTableAlias = vi.fn(async (name: string) => `${name}_v`);
  const view = render(
    <RegisterTableForm
      sources={[{ id: "pg", type: "mysql", allowedDomains: [] } as never]}
      domainHints={["sales"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled={true}
      tables={(over.registered ?? []).map(
        (name, i) => ({ id: i + 1, sourceId: "pg", tableName: name, columns: [] }) as never,
      )}
      roles={[{ id: "admin" } as never, { id: "analyst" } as never]}
      suggestTableAlias={suggestTableAlias}
      onContinue={vi.fn()}
      onCancel={() => {}}
      onProductsRegistered={vi.fn()}
      onRegisteredMany={onRegisteredMany}
      setError={setError}
      initialSourceId={over.severalFirst ? "pg" : ""}
      severalFirst={over.severalFirst}
    />,
  );
  return { onRegisteredMany, setError, suggestTableAlias, ...view };
}

const row = (name: string) => screen.getByTestId(`register-many-table-${name}`);

// Every listed table read: the last one's answer is in (an effect run twice by StrictMode drops
// the first run's read, so the reads are not counted).
async function listed() {
  await screen.findByText("Its columns could not be read: permission denied for table invoices");
  await waitFor(() => expect(row("customers")).toBeChecked());
}

describe("Register Table — several tables at once", () => {
  it("is offered beside one table, and lists the schema's tables when chosen", async () => {
    renderForm();
    const user = userEvent.setup();
    await user.selectOptions(screen.getByTestId("register-table-source-select"), "pg");
    expect(screen.getByTestId("register-table-table-select")).toBeInTheDocument();
    await user.click(screen.getByRole("radio", { name: "Several tables" }));
    expect(screen.queryByTestId("register-table-table-select")).toBeNull();
    expect(screen.queryByTestId("register-table-alias")).toBeNull();
    await listed();
    expect(screen.getByTestId("register-many-tables-submit")).toHaveTextContent(
      "Register 2 tables",
    );
  });

  it("ticks the tables that need no input, and says why each other one cannot be ticked", async () => {
    renderForm({ severalFirst: true, registered: ["orders"] });
    await listed();
    expect(row("orders")).not.toBeChecked();
    expect(row("orders")).toBeDisabled();
    expect(row("customers")).toBeChecked();
    expect(row("events")).toBeDisabled();
    expect(row("blobs")).toBeDisabled();
    expect(row("invoices")).toBeDisabled();
    const group = screen.getByTestId("register-many-tables");
    expect(within(group).getByText("Registered already.")).toBeInTheDocument();
    expect(
      within(group).getByText(
        "Needs input: its source does not describe its columns. Register it on its own.",
      ),
    ).toBeInTheDocument();
    expect(
      within(group).getByText(
        "Needs input: no type was found for payload. Register it on its own.",
      ),
    ).toBeInTheDocument();
    expect(
      within(group).getByText(
        "Its columns could not be read: permission denied for table invoices",
      ),
    ).toBeInTheDocument();
    // The registered table's columns are never read again.
    expect(readColumns.mock.calls.map((c) => c[2])).not.toContain("orders");
  });

  it("registers every ticked table in one action, each with the defaults it gets on its own", async () => {
    registerTables.mockResolvedValue([
      { tableName: "orders", result: { success: true, message: "", params: { id: 11 } } },
      { tableName: "customers", result: { success: true, message: "", params: { id: 12 } } },
    ]);
    const { onRegisteredMany, suggestTableAlias } = renderForm({ severalFirst: true });
    await listed();
    await userEvent.click(screen.getByTestId("register-many-tables-submit"));
    await waitFor(() => expect(registerTables).toHaveBeenCalledTimes(1));

    const inputs = registerTables.mock.calls[0][0];
    expect(inputs.map((i: { tableName: string }) => i.tableName)).toEqual(["orders", "customers"]);
    const orders = inputs[0];
    expect(orders).toMatchObject({
      sourceId: "pg",
      domainId: "sales",
      schemaName: "public",
      alias: "orders_v",
      description: "Every order",
      watermarkColumn: "updated_at",
      discover: false,
    });
    expect(orders.columns[1]).toMatchObject({
      name: "updatedAt",
      alias: "updated_at",
      dataType: "timestamp",
      description: "Last change",
      visibleTo: ["admin", "analyst"],
      scope: "domain",
    });
    // A parameter from a typed schema is registered as a parameter column, with no input.
    expect(inputs[1].columns[1]).toMatchObject({
      name: "_nf_region",
      nativeFilterType: "query_param",
      nativeFilterRequired: true,
    });
    expect(suggestTableAlias).toHaveBeenCalledWith("orders", "sales", "pg");
    expect(onRegisteredMany).toHaveBeenCalledWith(await registerTables.mock.results[0].value);
  });

  it("registers only the tables left ticked", async () => {
    registerTables.mockResolvedValue([
      { tableName: "customers", result: { success: true, message: "", params: { id: 12 } } },
    ]);
    renderForm({ severalFirst: true });
    await listed();
    await userEvent.click(row("orders"));
    expect(screen.getByTestId("register-many-tables-submit")).toHaveTextContent("Register 1 table");
    await userEvent.click(screen.getByTestId("register-many-tables-submit"));
    await waitFor(() => expect(registerTables).toHaveBeenCalled());
    expect(registerTables.mock.calls[0][0].map((i: { tableName: string }) => i.tableName)).toEqual([
      "customers",
    ]);
  });

  it("names each table when none of them was registered, and stays on the step", async () => {
    registerTables.mockResolvedValue([
      { tableName: "orders", result: { success: false, message: "Missing capability" } },
      { tableName: "customers", result: { success: false, message: "Missing capability" } },
    ]);
    const { onRegisteredMany, setError } = renderForm({ severalFirst: true });
    await listed();
    await userEvent.click(screen.getByTestId("register-many-tables-submit"));
    await waitFor(() =>
      expect(setError).toHaveBeenLastCalledWith(
        "orders was not registered: Missing capability\ncustomers was not registered: Missing capability",
      ),
    );
    expect(onRegisteredMany).not.toHaveBeenCalled();
  });
});
