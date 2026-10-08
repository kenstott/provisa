// Copyright (c) 2026 Kenneth Stott
// Canary: 1e7b3d94-0a56-4c82-9f6d-5b2a8e4c7d30
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1958/REQ-1134: a caller without view_governance is answered null grant lists and mask
// settings. The read view says they are not shown -- never "all" or "none" -- and does not offer
// the editor, whose save replaces grant lists the caller cannot see. With them shown, nothing
// changes.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import "../../../i18n";
import { TableReadView } from "../TableReadView";
import type { RegisteredTable, TableColumn } from "../../../types/admin";

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


function column(name: string, grants: Partial<TableColumn>): TableColumn {
  return {
    id: 1,
    columnName: name,
    visibleTo: [],
    writableBy: [],
    unmaskedTo: [],
    maskType: null,
    maskPattern: null,
    maskReplace: null,
    maskValue: null,
    maskPrecision: null,
    alias: null,
    computedSqlAlias: name,
    computedGqlAlias: name,
    description: null,
    ...grants,
  } as TableColumn;
}

function renderView(table: RegisteredTable, startEditing = vi.fn()) {
  render(
    <TableReadView
      t={table}
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
      startEditing={startEditing}
      handleDelete={vi.fn()}
      handleProfile={vi.fn()}
      onPreview={vi.fn()}
    />,
  );
  return startEditing;
}

describe("TableReadView with grant lists withheld (REQ-1958)", () => {
  const withheld = makeTable({
    columns: [column("id", { visibleTo: null, writableBy: null, unmaskedTo: null })],
  });

  it("says the grants are not shown, never 'all' or 'none'", () => {
    renderView(withheld);
    expect(screen.getByTestId("grants-not-shown")).toHaveTextContent("not shown");
    expect(screen.getAllByText("not shown")).toHaveLength(3); // visible to, writable by, mask
    expect(screen.queryByText("all")).not.toBeInTheDocument();
    expect(screen.queryByText("none")).not.toBeInTheDocument();
  });

  it("does not offer the editor", () => {
    const startEditing = renderView(withheld);
    const edit = screen.getByTestId("table-read-view-edit");
    expect(edit).toBeDisabled();
    edit.click();
    expect(startEditing).not.toHaveBeenCalled();
  });
});

describe("TableReadView with grant lists shown", () => {
  const shown = makeTable({
    columns: [column("id", { visibleTo: ["analyst"], writableBy: [], unmaskedTo: [] })],
  });

  it("shows them and offers the editor", () => {
    const startEditing = renderView(shown);
    expect(screen.getByText("analyst")).toBeInTheDocument();
    expect(screen.queryByTestId("grants-not-shown")).not.toBeInTheDocument();
    const edit = screen.getByTestId("table-read-view-edit");
    expect(edit).not.toBeDisabled();
    edit.click();
    expect(startEditing).toHaveBeenCalledTimes(1);
  });
});

import { governanceWithheld, shownGrants } from "../helpers";

describe("grant lists withheld from a caller", () => {
  it("an empty list is a grant list; null is withheld", () => {
    expect(governanceWithheld({ columns: [{ visibleTo: [] }] })).toBe(false);
    expect(governanceWithheld({ columns: [{ visibleTo: null }] })).toBe(true);
    expect(governanceWithheld({ columns: [] })).toBe(false);
  });

  it("a withheld list is never turned into a value to save", () => {
    expect(shownGrants(["analyst"])).toEqual(["analyst"]);
    expect(shownGrants([])).toEqual([]);
    expect(() => shownGrants(null)).toThrow(/withheld/);
  });
});
