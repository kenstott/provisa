// Copyright (c) 2026 Kenneth Stott
// Canary: 7c3f2a95-1e6d-4b80-a4c9-d52e8f10b6a3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1944: a governance-only viewer of a table opens its editor (how its columns are hidden) and
// previews it, but is not offered the table editor's acts -- deploy, profile, delete.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { TableReadView } from "../TableReadView";
import type { RegisteredTable } from "../../../types/admin";

function makeTable(overrides: Partial<RegisteredTable> = {}): RegisteredTable {
  return {
    id: 2,
    sourceId: "wh",
    domainId: "sales",
    schemaName: "public",
    tableName: "orders",
    alias: null,
    description: null,
    cacheTtl: null,
    roleTtl: [],
    pagingKind: null,
    pagination: null,
    pagingCeilingRows: null,
    replicate: null,
    region: null,
    draft: false,
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
    implicitMeasures: [],
    implicitDimensions: [],
    apiEndpoint: null,
    viewSql: null,
    dqContract: null,
    materialize: false,
    mvRefreshInterval: 300,
    mvDebounceQuiet: 0,
    mvDebounceMaxDelay: 5,
    pushDebounceQuiet: 0,
    pushDebounceMaxDelay: 5,
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
    productId: null,
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    writeOps: ["delete", "insert", "update"],
    live: null,
    uniqueConstraints: [],
    graphqlFieldName: null,
    dqDataset: null,
    ...overrides,
  };
}

const TABLE = makeTable({ id: 7, sourceId: "pg", canDeployToDb: true });

function renderView(hidingOnly: boolean) {
  render(
    <TableReadView
      t={TABLE}
      hidingOnly={hidingOnly}
      dataProducts={[]}
      navigate={vi.fn()}
      viewsOnly={false}
      deploying={{}}
      setDeploying={vi.fn()}
      deployMsg={{}}
      setDeployMsg={vi.fn()}
      tableProfiles={{}}
      deployViewToDb={vi.fn()}
      reload={vi.fn()}
      startEditing={vi.fn()}
      handleDelete={vi.fn()}
      handleProfile={vi.fn()}
      onPreview={vi.fn()}
    />,
  );
}

describe("TableReadView for a governance-only viewer (REQ-1944)", () => {
  it("offers the editor and the preview, not deploy, profile or delete", () => {
    renderView(true);
    expect(screen.getByTestId("table-read-view-edit")).toBeInTheDocument();
    expect(screen.getByTestId("table-read-view-preview")).toBeInTheDocument();
    expect(screen.queryByTestId("table-read-view-deploy")).not.toBeInTheDocument();
    expect(screen.queryByTestId("table-read-view-profile")).not.toBeInTheDocument();
    expect(screen.queryByTestId("table-read-view-delete")).not.toBeInTheDocument();
  });

  it("offers a table editor every act", () => {
    renderView(false);
    expect(screen.getByTestId("table-read-view-deploy")).toBeInTheDocument();
    expect(screen.getByTestId("table-read-view-profile")).toBeInTheDocument();
    expect(screen.getByTestId("table-read-view-delete")).toBeInTheDocument();
  });
});
