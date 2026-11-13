// Copyright (c) 2026 Kenneth Stott
// Canary: ed348491-54da-4c25-9f29-5d2dc3ca7eb7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1907 (#208): a table that will be a replica, registered under a ttl signal, needs a Cache
// TTL -- its own or its source's -- or the server refuses it. Register Table asks none (REQ-2234):
// exactly when one is needed -- the server says the kind's tables land here, and the source
// states none -- each table is offered but cannot be picked, says why, and is registered on its
// own, opened in the table's panel where its Cache TTL is given. Otherwise it is picked and
// registered with none.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "../../../test-utils/render";
import {
  expectReason,
  held,
  openTables,
  registerAction,
  submitTables,
  tableOption,
} from "./registerFormHelpers";

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
  useRegionChoices: () => ({ regions: [], connected: null }),
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

// mongodb: the server says its tables land; postgresql: read in place.
const LANDS: Record<string, boolean> = { mongodb: true, postgresql: false };
const SOURCES = [
  { id: "docs", type: "mongodb", allowedDomains: [], changeSignal: "ttl", cacheTtl: null },
  { id: "docs-ttl", type: "mongodb", allowedDomains: [], changeSignal: "ttl", cacheTtl: 300 },
  { id: "docs-push", type: "mongodb", allowedDomains: [], changeSignal: "native", cacheTtl: null },
  { id: "pg", type: "postgresql", allowedDomains: [], changeSignal: "ttl", cacheTtl: null },
];

function renderForm(sourceId: string) {
  const onContinue = vi.fn();
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
      onContinue={onContinue}
      onCancel={() => {}}
      onProductsRegistered={vi.fn()}
      onRegisteredMany={vi.fn()}
      setError={vi.fn()}
      initialSourceId={sourceId}
    />,
  );
  return { onContinue };
}

describe("RegisterTableForm Cache TTL", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    registerTables
      .mockReset()
      .mockResolvedValue([
        { tableName: "orders", result: { success: true, message: "", params: { id: 1 } } },
      ]);
    vi.spyOn(globalThis, "fetch").mockImplementation(async (url) => {
      const kind = decodeURIComponent(String(url).split("/admin/source-kinds/")[1]);
      return new Response(
        JSON.stringify({
          cache_ttl_required: false,
          sentinel_path: true,
          tables_land: LANDS[kind] ?? false,
        }),
      );
    });
  });

  it("is needed: the table cannot be picked, says why, and is registered on its own", async () => {
    const { onContinue } = renderForm("docs");
    const user = await openTables();
    await waitFor(() => expect(registerAction("orders")).not.toBeNull());
    expect(held("orders")).toBe(true);
    await expectReason(
      user,
      "orders",
      "Needs input: its source needs a Cache TTL for each table. Register it on its own.",
    );
    await user.click(registerAction("orders") as HTMLElement);
    await waitFor(() => expect(onContinue).toHaveBeenCalledTimes(1));
    // The panel's Load section gives it; nothing is registered from the list.
    expect(onContinue.mock.calls[0][0].input).toMatchObject({ tableName: "orders" });
    expect(onContinue.mock.calls[0][0].input.cacheTtl).toBeUndefined();
    expect(registerTables).not.toHaveBeenCalled();
  });

  it.each([
    ["its source states a Cache TTL", "docs-ttl"],
    ["its source's signal has its own clock", "docs-push"],
    ["the engine reads the source in place", "pg"],
  ])("is not needed when %s: the table is picked and sends none", async (_why, sourceId) => {
    renderForm(sourceId);
    const user = await openTables();
    await waitFor(() => expect(globalThis.fetch).toHaveBeenCalled());
    await waitFor(() => expect(held("orders")).toBe(false));
    expect(registerAction("orders")).toBeNull();
    await user.click(tableOption("orders"));
    await user.click(submitTables());
    await waitFor(() => expect(registerTables).toHaveBeenCalled());
    expect(registerTables.mock.calls[0][0][0].cacheTtl).toBeUndefined();
    expect(screen.queryByTestId("register-table-cache-ttl")).toBeNull();
  });
});
