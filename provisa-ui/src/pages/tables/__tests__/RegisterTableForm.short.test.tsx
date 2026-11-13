// Copyright (c) 2026 Kenneth Stott
// Canary: 956da361-be4f-4b70-8320-c8e665bcd0de
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Register Table (REQ-2234) asks what names the tables -- source, domain, schema, tables -- and
// nothing the table's panel holds: every other setting is edited there once it is registered.

import { describe, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../../../test-utils/render";
import { held, openTables, submitTables, tableOption } from "./registerFormHelpers";

vi.mock("../../../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../context/DomainFilterContext")>()),
  useDomainFilter: () => ({ ensureDomainChecked: vi.fn() }),
}));

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["openapi"], loading: false }),
  useAvailableTables: () => ({
    tables: [{ name: "getStore", comment: "The store", pagingKind: null, pagination: null }],
    loading: false,
  }),
}));
const readColumns = async () => [
  { name: "id", dataType: "integer", isPrimaryKey: true, comment: null, nativeFilterType: null },
  { name: "name", dataType: "varchar", isPrimaryKey: false, comment: null, nativeFilterType: null },
];
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

function renderForm(domainHints: string[]) {
  registerTables
    .mockReset()
    .mockResolvedValue([
      { tableName: "getStore", result: { success: true, message: "", params: { id: 1 } } },
    ]);
  render(
    <RegisterTableForm
      sources={[{ id: "petstore", type: "openapi", allowedDomains: [] } as never]}
      domainHints={domainHints}
      domainAccess={["*"]}
      checkedDomains={new Set<string>()}
      domainsEnabled={true}
      tables={[]}
      roles={[{ id: "admin" } as never]}
      suggestTableAlias={vi.fn().mockResolvedValue("")}
      onContinue={vi.fn()}
      onCancel={() => {}}
      onProductsRegistered={vi.fn()}
      onRegisteredMany={vi.fn()}
      setError={vi.fn()}
      initialSourceId="petstore"
    />,
  );
}

/** getStore picked, in `domain` when one is asked, and registered; what it sent. */
async function register(domain?: string) {
  if (domain)
    await userEvent.selectOptions(screen.getByTestId("register-table-domain-select"), domain);
  const user = await openTables();
  await waitFor(() => expect(held("getStore")).toBe(false));
  await user.click(tableOption("getStore"));
  await user.click(submitTables());
  await waitFor(() => expect(registerTables).toHaveBeenCalled());
  return registerTables.mock.calls[0][0][0];
}

describe("RegisterTableForm asks only what names the tables", () => {
  it("asks none of what the table's panel holds", async () => {
    renderForm(["sales"]);
    await openTables();
    for (const absent of [
      "register-table-alias",
      "register-table-domain-select",
      "register-table-region-select",
      "register-table-watermark-select",
      "register-table-cache-ttl",
      "register-table-description",
      "register-table-col-selected-id",
      "uniques-panel",
      "discover-columns-checkbox",
      "declared-columns",
      "register-table-vector-models",
    ])
      expect(screen.queryByTestId(absent)).toBeNull();
  });

  it("asks the domain when there is more than one to choose from", async () => {
    renderForm(["sales", "finance"]);
    expect((await register("finance")).domainId).toBe("finance");
  });

  it("registers every discovered column with its type, in the one domain", async () => {
    renderForm(["sales"]);
    const input = await register();
    expect(input.domainId).toBe("sales");
    expect(input.description).toBe("The store");
    expect(
      input.columns.map((c: { name: string; dataType: string }) => [c.name, c.dataType]),
    ).toEqual([
      ["id", "integer"],
      ["name", "varchar"],
    ]);
  });
});
