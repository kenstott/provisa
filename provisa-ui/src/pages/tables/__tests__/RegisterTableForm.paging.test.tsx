// Copyright (c) 2026 Kenneth Stott
// Canary: 9b5b15b5-f722-4862-8ff7-b66893ec5b4c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-318: registering an OpenAPI table shows the paging its source suggests; the steward
// accepts or edits it, and the table is registered with what is declared.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "../../../test-utils/render";
import { NO_PAGING, pagingInput } from "../paging";
import { held, openTables, submitTables, tableOption } from "./registerFormHelpers";

const OFFERED = [
  {
    name: "listPets",
    comment: null,
    pagingKind: "endpoint",
    pagination: { ...NO_PAGING, type: "offset", pageParam: "offset", pageSizeParam: "limit" },
    pagingCeilingRows: null,
  },
  { name: "getStore", comment: null, pagingKind: "endpoint", pagination: null },
];

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["openapi"], loading: false }),
  useAvailableTables: () => ({ tables: OFFERED, loading: false }),
}));
const readColumns = async () => [
  { name: "id", dataType: "integer", comment: null, nativeFilterType: null, isPrimaryKey: true },
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

function renderForm() {
  render(
    <RegisterTableForm
      sources={[{ id: "petstore", type: "openapi", allowedDomains: [] } as never]}
      domainHints={["sales"]}
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

/** The table picked and registered; what its registration sent. */
async function registered(name: string) {
  registerTables.mockResolvedValue([
    { tableName: name, result: { success: true, message: "", params: { id: 1 } } },
  ]);
  renderForm();
  const user = await openTables();
  await waitFor(() => expect(held(name)).toBe(false));
  await user.click(tableOption(name));
  await user.click(submitTables());
  await waitFor(() => expect(registerTables).toHaveBeenCalled());
  return registerTables.mock.calls[0][0][0];
}

describe("RegisterTableForm paging (REQ-318)", () => {
  beforeEach(() => vi.clearAllMocks());

  // Paging is not asked on Register Table: a table picked is registered with the paging its
  // source proposes, which the panel's Load and timeliness section edits (REQ-2109, REQ-318).
  it("registers the paging a source proposes without asking it", async () => {
    const input = await registered("listPets");
    expect(screen.queryByRole("textbox", { name: "Paging type" })).toBeNull();
    expect(input.pagination).toEqual(pagingInput(OFFERED[0].pagination as never));
  });

  it("registers a table whose source suggests none with no paging", async () => {
    const input = await registered("getStore");
    expect(input.pagination).toBeUndefined();
  });
});
