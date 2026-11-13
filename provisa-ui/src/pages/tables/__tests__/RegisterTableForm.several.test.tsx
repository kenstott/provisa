// Copyright (c) 2026 Kenneth Stott
// Canary: 8a3f6d21-4c97-4e5b-b0a2-d7e1c9f4b638
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234: Register Table holds Source, Domain, Schema when the source has more than one, and one
// Tables field: the standard multi-select, which takes one table or many and registers each, in
// one action, with the defaults its registration alone would take. A table registered already is
// not offered; a table that needs input is offered, cannot be picked, and says why.

import { beforeEach, describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../../../test-utils/render";
import type { ColumnMetadata } from "../../../api/admin";

vi.mock("../../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({ ensureDomainChecked: vi.fn() }),
}));

// The schemas the source lists: one, unless a test says more.
const listedSchemas = { schemas: ["public"] };
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: listedSchemas.schemas, loading: false }),
  useAvailableTables: (_source: string | null, schema: string | null) => ({
    tables: schema
      ? [
          { name: "orders", comment: "Every order", pagingKind: null, pagination: null },
          { name: "customers", comment: null, pagingKind: null, pagination: null },
          { name: "events", comment: null, columnsDeclared: true },
          { name: "blobs", comment: null },
          { name: "invoices", comment: null },
          { name: "refunds", comment: null },
        ]
      : [],
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
  refunds: [
    { name: "id", dataType: "integer", comment: null, nativeFilterType: null, isPrimaryKey: true },
  ],
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
  listedSchemas.schemas = ["public"];
  readColumns.mockReset().mockImplementation(async (_s: string, _schema: string, table: string) => {
    if (table === "invoices") throw new Error("permission denied for table invoices");
    return COLUMNS[table];
  });
  registerTables.mockReset();
});

function renderForm(
  over: { pickReady?: boolean; registered?: string[]; source?: string; domains?: string[] } = {},
) {
  const onRegisteredMany = vi.fn();
  const onContinue = vi.fn();
  const setError = vi.fn();
  const suggestTableAlias = vi.fn(async (name: string) => `${name}_v`);
  const view = render(
    <RegisterTableForm
      sources={[{ id: "pg", type: "mysql", allowedDomains: [] } as never]}
      domainHints={over.domains ?? ["sales"]}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled={true}
      tables={(over.registered ?? []).map(
        (name, i) => ({ id: i + 1, sourceId: "pg", tableName: name, columns: [] }) as never,
      )}
      roles={[{ id: "admin" } as never, { id: "analyst" } as never]}
      suggestTableAlias={suggestTableAlias}
      onContinue={onContinue}
      onCancel={() => {}}
      onProductsRegistered={vi.fn()}
      onRegisteredMany={onRegisteredMany}
      setError={setError}
      initialSourceId={over.source ?? "pg"}
      pickReady={over.pickReady}
    />,
  );
  return { onRegisteredMany, onContinue, setError, suggestTableAlias, ...view };
}

const submit = () => screen.getByTestId("register-many-tables-submit");
const tablesField = () => screen.getByRole("textbox", { name: "Tables" });
const option = (name: string) =>
  screen.getByRole("option", { name: new RegExp(`^${name}`), hidden: true });
const held = (name: string) => option(name).hasAttribute("data-combobox-disabled");

// The list open, every offered table read: the last one's answer is in (an effect run twice by
// StrictMode drops the first run's read, so the reads are not counted).
async function opened() {
  const user = userEvent.setup();
  await waitFor(() => expect(tablesField()).not.toBeDisabled());
  await user.click(tablesField());
  await waitFor(() => expect(held("refunds")).toBe(false));
  await waitFor(() => expect(screen.getByTestId("option-action-blobs")).toBeInTheDocument());
  return user;
}

async function expectReason(
  user: ReturnType<typeof userEvent.setup>,
  name: string,
  reason: string,
) {
  await user.hover(screen.getByTestId(`option-not-held-${name}`));
  await waitFor(() =>
    expect(
      screen.getAllByRole("tooltip", { hidden: true }).map((tip) => tip.textContent),
    ).toContain(reason),
  );
}

describe("Register Table's fields", () => {
  it("are Source, Domain and Tables when the source has one schema", async () => {
    const { container } = renderForm({ domains: ["sales", "finance"] });
    await waitFor(() => expect(tablesField()).toBeInTheDocument());
    expect(screen.getByTestId("register-table-source-select")).toBeInTheDocument();
    expect(screen.getByTestId("register-table-domain-select")).toBeInTheDocument();
    // Absent, not disabled.
    expect(screen.queryByTestId("register-table-schema-select")).toBeNull();
    // Nothing else: no switch between one table and several, no per-table setting.
    expect(screen.queryByRole("radio")).toBeNull();
    expect(screen.queryByTestId("register-table-alias")).toBeNull();
    expect(screen.queryByTestId("register-many-tables")).toBeNull();
    const fields = container.querySelectorAll("select, input:not([type=hidden]), textarea");
    expect(fields).toHaveLength(3);
  });

  it("add Schema when the source has more than one, and list its tables once it is chosen", async () => {
    listedSchemas.schemas = ["public", "archive"];
    const user = userEvent.setup();
    renderForm();
    const schema = await screen.findByTestId("register-table-schema-select");
    expect(tablesField()).toBeDisabled();
    await user.selectOptions(schema, "public");
    await waitFor(() => expect(tablesField()).not.toBeDisabled());
  });

  it("count no table and cannot be submitted before one is picked", async () => {
    renderForm();
    await opened();
    expect(submit()).toHaveTextContent("Register 0 tables");
    expect(submit()).toBeDisabled();
  });
});

describe("the Tables field", () => {
  it("does not offer a table registered already", async () => {
    renderForm({ registered: ["orders"] });
    await opened();
    expect(screen.queryByRole("option", { name: /^orders/, hidden: true })).toBeNull();
    expect(option("customers")).toBeInTheDocument();
    // Its columns are never read.
    expect(readColumns.mock.calls.map((c) => c[2])).not.toContain("orders");
  });

  it("offers a table that needs input, which cannot be picked and says why", async () => {
    renderForm();
    const user = await opened();
    for (const name of ["events", "blobs", "invoices"]) expect(held(name)).toBe(true);
    await expectReason(
      user,
      "blobs",
      "Needs input: no type was found for payload. Register it on its own.",
    );
    await user.unhover(screen.getByTestId("option-not-held-blobs"));
    await expectReason(
      user,
      "invoices",
      "Its columns could not be read: permission denied for table invoices",
    );
    await user.click(screen.getByTestId("option-not-held-blobs"));
    expect(submit()).toHaveTextContent("Register 0 tables");
  });

  it("registers every table picked in one action, each with the defaults it gets on its own", async () => {
    registerTables.mockResolvedValue([
      { tableName: "orders", result: { success: true, message: "", params: { id: 11 } } },
      { tableName: "customers", result: { success: true, message: "", params: { id: 12 } } },
      { tableName: "refunds", result: { success: true, message: "", params: { id: 13 } } },
    ]);
    const { onRegisteredMany, suggestTableAlias } = renderForm();
    const user = await opened();
    for (const name of ["orders", "customers", "refunds"]) await user.click(option(name));
    expect(submit()).toHaveTextContent("Register 3 tables");
    await user.click(submit());
    await waitFor(() => expect(registerTables).toHaveBeenCalledTimes(1));

    const inputs = registerTables.mock.calls[0][0];
    expect(inputs.map((i: { tableName: string }) => i.tableName)).toEqual([
      "orders",
      "customers",
      "refunds",
    ]);
    expect(inputs[0]).toMatchObject({
      sourceId: "pg",
      domainId: "sales",
      schemaName: "public",
      alias: "orders_v",
      description: "Every order",
      watermarkColumn: "updated_at",
      discover: false,
    });
    expect(inputs[0].columns[1]).toMatchObject({
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

  it("takes one table", async () => {
    registerTables.mockResolvedValue([
      { tableName: "customers", result: { success: true, message: "", params: { id: 12 } } },
    ]);
    renderForm();
    const user = await opened();
    await user.click(option("customers"));
    expect(submit()).toHaveTextContent("Register 1 table");
    await user.click(submit());
    await waitFor(() => expect(registerTables).toHaveBeenCalled());
    expect(registerTables.mock.calls[0][0].map((i: { tableName: string }) => i.tableName)).toEqual([
      "customers",
    ]);
  });

  it("starts with every table that needs no input picked, on a source just saved", async () => {
    renderForm({ pickReady: true });
    await opened();
    await waitFor(() => expect(submit()).toHaveTextContent("Register 3 tables"));
  });

  it("names each table when none of them was registered, and stays on the step", async () => {
    registerTables.mockResolvedValue([
      { tableName: "orders", result: { success: false, message: "Missing capability" } },
      { tableName: "customers", result: { success: false, message: "Missing capability" } },
    ]);
    const { onRegisteredMany, setError } = renderForm();
    const user = await opened();
    await user.click(option("orders"));
    await user.click(option("customers"));
    await user.click(submit());
    await waitFor(() =>
      expect(setError).toHaveBeenLastCalledWith(
        "orders was not registered: Missing capability\ncustomers was not registered: Missing capability",
      ),
    );
    expect(onRegisteredMany).not.toHaveBeenCalled();
  });
});
