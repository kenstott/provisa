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
import type { MockedResponse } from "@apollo/client/testing";
import { render, screen } from "../../../test-utils/render";
import { TableEditForm } from "../TableEditForm";
import { MaterializeStoreInfo as MATERIALIZE_STORE_INFO_QUERY } from "../../../hooks/admin.graphql";
import type { RegisteredTable } from "../../../types/admin";

// Drive useMaterializeStoreInfo through the REAL Apollo hook via a MockedProvider result rather than
// mocking the hook module. A module mock (vi.mock) is not leak-immune under `pool: 'vmThreads'` +
// `fileParallelism: false`: a sibling test file that renders TableEditForm loads MaterializedViewPanels
// into the shared VM module cache with the real hook already bound, so this file's vi.mock cannot
// re-bind it and the warning silently never renders. Feeding the query result exercises the real code
// path end-to-end and is order-independent.
function storeMock(instanceLocalStore: boolean | null): MockedResponse {
  return {
    request: { query: MATERIALIZE_STORE_INFO_QUERY },
    result: {
      data: {
        materializeStoreInfo:
          instanceLocalStore === null
            ? null
            : {
                __typename: "MaterializeStoreInfo",
                engineName: "duckdb",
                storeRef: instanceLocalStore ? "duckdb:///x.duckdb" : "postgresql://h/db",
                mvCount: 0,
                instanceLocalStore,
              },
      },
    },
    // cache-and-network refetches; supply the value for every re-request in a render.
    maxUsageCount: Number.POSITIVE_INFINITY,
  };
}

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

describe("TableEditForm — MV store consistency (REQ-879)", () => {
  it("no longer renders a consistency selector", () => {
    render(formEl(makeTable()), { mocks: [storeMock(null)] });
    expect(screen.queryByTestId("mv-consistency")).toBeNull();
  });

  it("shows no local-store warning when store info is unresolved", () => {
    render(formEl(makeTable()), { mocks: [storeMock(null)] });
    expect(screen.queryByTestId("mv-local-store-warning")).toBeNull();
  });

  it("warns when the resolved materialization store is instance-local", async () => {
    render(formEl(makeTable()), { mocks: [storeMock(true)] });
    expect(await screen.findByTestId("mv-local-store-warning")).toBeInTheDocument();
  });

  it("does not warn when the resolved store is shared", () => {
    render(formEl(makeTable()), { mocks: [storeMock(false)] });
    expect(screen.queryByTestId("mv-local-store-warning")).toBeNull();
  });

  it("hides MV controls entirely when the table is not materialized", () => {
    render(formEl(makeTable({ materialize: false })), { mocks: [storeMock(null)] });
    expect(screen.queryByTestId("mv-persist")).toBeNull();
  });
});
