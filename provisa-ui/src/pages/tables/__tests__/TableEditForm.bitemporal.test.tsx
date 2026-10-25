// Copyright (c) 2026 Kenneth Stott
// Canary: 326b547a-2d9e-40a3-8348-7b20e7ef43b3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2109: the table editor and the view editor each offer the business key, valid time and
// recorded time, each with its hint below the field and its longer help behind the (?).

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";

// vmThreads shares the module registry across test files, so a neighbor's partial
// vi.mock("../../../api/admin") (e.g. table-description-autofill's) can leak in and drop
// fetchIrTypes, which TableEditForm calls on mount. Declare a complete pass-through mock so
// this file always sees the real api/admin exports regardless of run order.
vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
}));

import { TableEditForm } from "../TableEditForm";
import type { RegisteredTable, TableColumn } from "../../../types/admin";
import i18n from "../../../i18n";

const t = i18n.getFixedT("en");

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
    roleTtl: [],
    pagingKind: null,
    pagination: null,
    pagingCeilingRows: null,
    replicate: null,
    region: null,
    draft: false,
    rowMaterialize: false,
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
    implicitMeasures: [],
    implicitDimensions: [],
    apiEndpoint: null,
    viewSql: "SELECT 1",
    dqContract: null,
    materialize: true,
    mvRefreshInterval: 300,
    mvDebounceQuiet: 0,
    mvDebounceMaxDelay: 5,
    pushDebounceQuiet: 0,
    pushDebounceMaxDelay: 5,
    mvConsistency: "shared",
    mvPreprocess: null,
    mvHoldOnFailingChecks: false,
    businessKey: [],
    validTime: null,
    recordedTime: null,
    mvPersist: "replace",
    mvPrimaryKey: [],
    mvIncremental: false,
    mvCalendar: null,
    mvGrain: null,
    mvAllowedLateness: 0,
    mvExpectedEvents: null,
    mvBusinessDayGrain: false,
    productId: null, // REQ-1634
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    writeOps: ["delete", "insert", "update"],
    live: null,
    writeGrants: [],
    uniqueConstraints: [],
    graphqlFieldName: null,
    dqDataset: null,
    qualityCheckers: [],
    ...overrides,
  };
}

function renderForm(table: RegisteredTable, setEditingTable = vi.fn()) {
  render(
    <TableEditForm
      // What this file tests is on the load section.
      relationships={[]}
      section="load"
      onSectionChange={vi.fn()}
      savedProfilerId={null}
      editingTable={table}
      savedTable={table}
      setEditingTable={setEditingTable}
      editingColumnTypes={{}}
      cacheTtlEdits={{}}
      setCacheTtlEdits={vi.fn()}
      sources={[]}
      roles={[]}
      dataProducts={[]}
      settings={null}
      saving={false}
      generateTableDescription={vi.fn()}
      generateColumnDescription={vi.fn()}
      cancelEditing={vi.fn()}
      handleSaveEdit={vi.fn()}
      updateEditCol={vi.fn()}
    />,
  );
  return setEditingTable;
}
const COLUMNS: TableColumn[] = ["trade_id", "trade_date", "updated_at"].map((name, i) => ({
  id: i + 1,
  columnName: name,
  visibleTo: [],
  masks: [],
  alias: null,
  computedSqlAlias: name,
  computedGqlAlias: name,
  description: null,
  dataType: "varchar",
  nativeFilterType: null,
  isPrimaryKey: false,
  isForeignKey: false,
  isAlternateKey: false,
  scope: "domain",
  isImplicitMeasure: false,
  isImplicitDimension: false,
}));

const FIELDS = [
  ["business-key", "businessKey"],
  ["valid-time", "validTime"],
  ["recorded-time", "recordedTime"],
] as const;

function expectTheThree() {
  for (const [testId, key] of FIELDS) {
    expect(screen.getByTestId(testId)).toBeInTheDocument();
    expect(screen.getByText(t(`tableEditForm.${key}Label`))).toBeInTheDocument();
    expect(screen.getByText(t(`tableEditForm.${key}Hint`))).toBeInTheDocument();
    expect(screen.getByLabelText(t(`tableEditForm.${key}Help`))).toBeInTheDocument();
  }
}

describe("TableEditForm — the three history columns (REQ-2109)", () => {
  it("the view editor offers them with their hints once the view keeps history", () => {
    renderForm(makeTable({ columns: COLUMNS }));
    expect(screen.queryByTestId("business-key")).toBeNull();
    fireEvent.click(screen.getByRole("textbox", { name: t("tableEditForm.timeTravelLabel") }));
    fireEvent.click(screen.getByRole("option", { name: t("tableEditForm.timeTravelBitemporal") }));
    expectTheThree();
  });

  it("the table editor offers them with their hints once Bitemporal is picked", () => {
    renderForm(
      makeTable({
        viewSql: null,
        materialize: false,
        columns: COLUMNS,
      }),
    );
    expect(screen.queryByTestId("business-key")).toBeNull();
    fireEvent.click(screen.getByRole("textbox", { name: t("tableEditForm.timeTravelLabel") }));
    fireEvent.click(screen.getByRole("option", { name: t("tableEditForm.timeTravelBitemporal") }));
    expectTheThree();
  });

  it("a choice is staged on the table", () => {
    const setEditingTable = renderForm(
      makeTable({
        viewSql: null,
        materialize: false,
        columns: COLUMNS,
      }),
    );
    fireEvent.click(screen.getByRole("textbox", { name: t("tableEditForm.timeTravelLabel") }));
    fireEvent.click(screen.getByRole("option", { name: t("tableEditForm.timeTravelBitemporal") }));
    fireEvent.click(screen.getByRole("textbox", { name: t("tableEditForm.validTimeLabel") }));
    fireEvent.click(screen.getByRole("option", { name: "trade_date" }));
    expect(setEditingTable).toHaveBeenCalledWith(
      expect.objectContaining({ validTime: "trade_date" }),
    );
  });
});
