// Copyright (c) 2026 Kenneth Stott
// Canary: 6db7cc68-3ef2-4442-be87-3a252ea1b60c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: a new table starts in its source's region, else the region the operator is
// connected to; the operator changes it on the table's Identity tab. With no regions declared,
// the table is registered with none. Register Table (REQ-2234) never asks it.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "../../../test-utils/render";
import { held, openTables, submitTables, tableOption } from "./registerFormHelpers";

const choices = vi.hoisted(() => ({
  value: { regions: ["eu", "us"], connected: "us" as string | null },
}));

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useAvailableSchemas: () => ({ schemas: ["public"], loading: false }),
  useAvailableTables: () => ({
    tables: [{ name: "orders", comment: null, pagingKind: null, pagination: null }],
    loading: false,
  }),
}));
vi.mock("../../../hooks/useRegionQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useRegionQueries")>()),
  useRegionChoices: () => choices.value,
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

const SOURCES = [
  { id: "crm", type: "postgresql", allowedDomains: [], region: "eu" },
  { id: "erp", type: "postgresql", allowedDomains: [], region: null },
];

/** orders of the source picked and registered; what its registration sent. */
async function registered(sourceId: string) {
  registerTables.mockResolvedValue([
    { tableName: "orders", result: { success: true, message: "", params: { id: 1 } } },
  ]);
  render(
    <RegisterTableForm
      sources={SOURCES as never}
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
      initialSourceId={sourceId}
    />,
  );
  const user = await openTables();
  await waitFor(() => expect(held("orders")).toBe(false));
  await user.click(tableOption("orders"));
  await user.click(submitTables());
  await waitFor(() => expect(registerTables).toHaveBeenCalled());
  return registerTables.mock.calls[0][0][0];
}

describe("RegisterTableForm region (REQ-1921)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    choices.value = { regions: ["eu", "us"], connected: "us" };
  });

  it("starts a table in its source's region", async () => {
    expect((await registered("crm")).region).toBe("eu");
  });

  it("starts a table of a source naming none in the connected region", async () => {
    expect((await registered("erp")).region).toBe("us");
  });

  it("does not ask the region", async () => {
    await registered("crm");
    expect(screen.queryByTestId("register-table-region-select")).toBeNull();
  });

  it("sends none when the platform declares no regions", async () => {
    choices.value = { regions: [], connected: null };
    expect("region" in (await registered("crm"))).toBe(false);
  });
});
