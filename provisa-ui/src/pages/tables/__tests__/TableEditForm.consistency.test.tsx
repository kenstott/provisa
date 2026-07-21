// Copyright (c) 2026 Kenneth Stott
// Canary: 9e99076c-e8fb-4e48-baae-f67da7a4c7d0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-879: consistency is NOT a per-MV choice — it follows the deployment's materialization store.
// The form no longer renders a selector; instead it warns only when the resolved store is
// instance-local (a per-instance copy that diverges across instances behind a load balancer).

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { TableEditForm } from "../TableEditForm";
import type { RegisteredTable } from "../../../types/admin";

// Control useMaterializeStoreInfo directly via a hoisted holder (importOriginal keeps every other
// hook real). This is leak-immune under vmThreads — a store-info mock leaking in from another test
// file cannot flip this test's value, and this file's mock re-applies when it runs.
const storeHolder = vi.hoisted(() => ({ info: null as unknown }));
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../../hooks/useAdminQueries")>();
  return {
    ...actual,
    useMaterializeStoreInfo: () => ({
      materializeStoreInfo: storeHolder.info,
      loading: false,
      error: undefined,
      refetch: vi.fn(),
    }),
  };
});

function makeTable(overrides: Partial<RegisteredTable> = {}): RegisteredTable {
  return {
    id: 1,
    sourceId: "src",
    domainId: "dom",
    schemaName: "public",
    tableName: "orders_view",
    alias: null,
    description: null,
    cacheTtl: null,
    preferMaterialized: null,
    loadProtected: null,
    offPeakWindow: null,
    offPeakTz: null,
    refreshPolicySummary: null,
    gqlNamingConvention: null,
    watermarkColumn: null,
    changeSignal: null,
    probeQuery: null,
    probeType: null,
    columns: [],
    columnPresets: [],
    apiEndpoint: null,
    viewSql: "SELECT 1",
    materialize: true,
    mvRefreshInterval: 300,
    mvDebounceQuiet: 0,
    mvDebounceMaxDelay: 5,
    mvConsistency: "shared",
    mvPreprocess: null,
    mvBitemporalMode: null,
    mvBitemporalKey: [],
    mvPersist: "replace",
    mvPrimaryKey: [],
    mvIncremental: false,
    mvCalendar: null,
    mvGrain: null,
    mvAllowedLateness: 0,
    mvExpectedEvents: null,
    mvBusinessDayGrain: false,
    dataProduct: false,
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    live: null,
    uniqueConstraints: [],
    ...overrides,
  };
}

function formEl(table: RegisteredTable) {
  return (
    <TableEditForm
      editingTable={table}
      setEditingTable={vi.fn()}
      editingColumnTypes={{}}
      cacheTtlEdits={{}}
      setCacheTtlEdits={vi.fn()}
      sources={[]}
      roles={[]}
      settings={null}
      saving={false}
      generatingDesc={false}
      setGeneratingDesc={vi.fn()}
      generatingColDesc={null}
      setGeneratingColDesc={vi.fn()}
      generateTableDescription={vi.fn()}
      generateColumnDescription={vi.fn()}
      cancelEditing={vi.fn()}
      handleSaveEdit={vi.fn()}
      updateEditCol={vi.fn()}
    />
  );
}

function setStore(instanceLocalStore: boolean | null) {
  storeHolder.info =
    instanceLocalStore === null
      ? null
      : {
          engineName: "duckdb",
          storeRef: instanceLocalStore ? "duckdb:///x.duckdb" : "postgresql://h/db",
          mvCount: 0,
          instanceLocalStore,
        };
}

describe("TableEditForm — MV store consistency (REQ-879)", () => {
  it("no longer renders a consistency selector", () => {
    setStore(null);
    render(formEl(makeTable()));
    expect(screen.queryByTestId("mv-consistency")).toBeNull();
  });

  it("shows no local-store warning when store info is unresolved", () => {
    setStore(null);
    render(formEl(makeTable()));
    expect(screen.queryByTestId("mv-local-store-warning")).toBeNull();
  });

  it("warns when the resolved materialization store is instance-local", () => {
    setStore(true);
    render(formEl(makeTable()));
    expect(screen.getByTestId("mv-local-store-warning")).toBeInTheDocument();
  });

  it("does not warn when the resolved store is shared", () => {
    setStore(false);
    render(formEl(makeTable()));
    expect(screen.queryByTestId("mv-local-store-warning")).toBeNull();
  });

  it("hides MV controls entirely when the table is not materialized", () => {
    render(formEl(makeTable({ materialize: false })));
    expect(screen.queryByTestId("mv-persist")).toBeNull();
  });
});
