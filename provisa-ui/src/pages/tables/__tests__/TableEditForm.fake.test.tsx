// Copyright (c) 2026 Kenneth Stott
// Canary: 3ea66092-fd64-43d6-94c7-cf3421abc071
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1494: a column's kind of fake and whether it is stable are edited in the table editor and
// sent with the table's columns; the server checks them when the table is saved.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";

import { TableEditForm } from "../TableEditForm";
import { buildTableUpdateInput } from "../helpers";
import type { RegisteredTable, Source, TableColumn } from "../../../types/admin";

function makeTable(overrides: Partial<RegisteredTable> = {}): RegisteredTable {
  return {
    id: 2,
    sourceId: "wh",
    origin: "admin",
    domainId: "sales",
    schemaName: "quality",
    tableName: "orders_scans",
    alias: null,
    description: null,
    cacheTtl: null,
    roleTtl: [],
    pagingKind: null,
    pagination: null,
    pagingCeilingRows: null,
    replicate: null,
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

function renderForm(table: RegisteredTable, sources: Source[], updateEditCol = vi.fn()) {
  render(
    <TableEditForm
      savedProfilerId={null}
      editingTable={table}
      setEditingTable={vi.fn()}
      editingColumnTypes={{}}
      cacheTtlEdits={{}}
      setCacheTtlEdits={vi.fn()}
      sources={sources}
      roles={[]}
      dataProducts={[]}
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
      updateEditCol={updateEditCol}
    />,
  );
}

const SOURCE = { id: "wh", type: "postgresql" } as unknown as Source;

const EMAIL = {
  id: 1,
  columnName: "email",
  visibleTo: [],
  writableBy: [],
  unmaskedTo: [],
  maskType: null,
  maskPattern: null,
  maskReplace: null,
  maskValue: null,
  maskPrecision: null,
  alias: null,
  computedSqlAlias: "email",
  computedGqlAlias: "email",
  description: null,
  dataType: "varchar",
  nativeFilterType: null,
  isPrimaryKey: false,
  isForeignKey: false,
  isAlternateKey: false,
  scope: "domain",
  isImplicitMeasure: false,
  isImplicitDimension: false,
} satisfies TableColumn;

describe("TableEditForm — a column's fake (REQ-1494)", () => {
  it("edits the declared fake and its stable flag", () => {
    const updateEditCol = vi.fn();
    renderForm(makeTable({ columns: [{ ...EMAIL, fake: "email()" }] }), [SOURCE], updateEditCol);
    const input = screen.getByTestId("table-edit-col-fake-email") as HTMLInputElement;
    expect(input.value).toBe("email()");
    fireEvent.change(input, { target: { value: "categories((a, b))" } });
    expect(updateEditCol).toHaveBeenCalledWith(0, "fake", "categories((a, b))");
    fireEvent.click(screen.getByTestId("table-edit-col-fake-stable-email"));
    expect(updateEditCol).toHaveBeenCalledWith(0, "fakeStable", true);
  });

  it("sends the fake with the column, and none when blank", () => {
    const [faked] = buildTableUpdateInput(
      makeTable({ columns: [{ ...EMAIL, fake: " email() ", fakeStable: true }] }),
    ).columns as Record<string, unknown>[];
    expect(faked).toMatchObject({ fake: "email()", fakeStable: true });
    const [plain] = buildTableUpdateInput(makeTable({ columns: [{ ...EMAIL, fake: "  " }] }))
      .columns as Record<string, unknown>[];
    expect(plain.fake).toBeUndefined();
    expect(plain.fakeStable).toBeUndefined();
  });
});
