// Copyright (c) 2026 Kenneth Stott
// Canary: 5d1e8b3a-2c7f-4a96-b0e4-8f3a6c21d957
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1944: an editor holding a hiding right but not table_registration edits only how the
// columns are hidden. The table-level settings are not shown; a column's type, alias,
// description, key, write grants and scope are read-only; its read grants and masks stay editable.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";

vi.mock("../../../api/fakes", () => ({
  fetchFakeCatalog: vi.fn().mockResolvedValue({ kinds: [], methods: [] }),
  checkColumnFake: vi.fn(),
  proposeFakes: vi.fn(),
}));

import { TableEditForm } from "../TableEditForm";
import type { RegisteredTable, Source, TableColumn } from "../../../types/admin";

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

const SOURCE = { id: "wh", type: "postgresql" } as unknown as Source;

function renderForm(hidingOnly: boolean) {
  render(
    <TableEditForm
      hidingOnly={hidingOnly}
      savedProfilerId={null}
      editingTable={makeTable({ columns: [EMAIL] })}
      setEditingTable={vi.fn()}
      editingColumnTypes={{}}
      cacheTtlEdits={{}}
      setCacheTtlEdits={vi.fn()}
      sources={[SOURCE]}
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
      updateEditCol={vi.fn()}
    />,
  );
}

function control(row: HTMLElement, label: string): HTMLInputElement {
  const el = row.querySelector<HTMLInputElement>(`input[aria-label="${label}"]`);
  if (el === null) throw new Error(`no control labelled ${label}`);
  return el;
}

describe("TableEditForm for a governance-only editor (REQ-1944)", () => {
  it("shows only the columns, naming what may be changed", () => {
    renderForm(true);
    expect(screen.getByTestId("table-edit-hiding-only")).toBeInTheDocument();
    expect(screen.queryByTestId("load-management-panel-toggle")).not.toBeInTheDocument();
  });

  it("keeps grants and masks editable and the rest of a column read-only", () => {
    renderForm(true);
    const row = screen.getByTestId("column-row-email");
    expect(control(row, "SQL Alias")).toBeDisabled();
    expect(control(row, "Data Type")).toBeDisabled();
    expect(control(row, "Primary Key")).toBeDisabled();
    expect(control(row, "Masking")).not.toBeDisabled();
    expect(control(row, "Visible To (Read)")).not.toBeDisabled();
  });

  it("is the whole editor for a table editor", () => {
    renderForm(false);
    expect(screen.queryByTestId("table-edit-hiding-only")).not.toBeInTheDocument();
    expect(screen.getByTestId("load-management-panel-toggle")).toBeInTheDocument();
    expect(control(screen.getByTestId("column-row-email"), "SQL Alias")).not.toBeDisabled();
  });
});
