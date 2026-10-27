// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4e2a71-3b8d-4f05-a6e2-1d7f0b5c8e39
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2133: a materialized view's Hold on failing checks option, and the code it applies, shown
// for a steward to read and copy into the view's own preflight.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";

vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
}));

import { TableEditForm } from "../TableEditForm";
import type { RegisteredTable } from "../../../types/admin";
import { HoldOnFailingChecksSource } from "../../../hooks/admin.graphql";
import i18n from "../../../i18n";

const t = i18n.getFixedT("en");

const CODE =
  "import provisa_services as ps\n\n\ndef preflight(streams, ctx):\n    return ctx.ok()\n";
const SOURCE_MOCK = {
  request: { query: HoldOnFailingChecksSource },
  result: { data: { holdOnFailingChecksSource: CODE } },
};

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
    rowMaterialize: false,
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
    { mocks: [SOURCE_MOCK] },
  );
  return setEditingTable;
}

describe("TableEditForm — Hold on failing checks (REQ-2133)", () => {
  it("stages the option through setEditingTable", () => {
    const setEditingTable = renderForm(makeTable());
    fireEvent.click(screen.getByTestId("mv-hold-on-failing-checks"));
    expect(setEditingTable).toHaveBeenCalledWith(
      expect.objectContaining({ mvHoldOnFailingChecks: true }),
    );
  });

  it("shows the code it applies, on asking, for a steward to copy", async () => {
    renderForm(makeTable({ mvHoldOnFailingChecks: true }));
    expect(screen.queryByTestId("mv-hold-on-failing-checks-code")).toBeNull();
    fireEvent.click(screen.getByTestId("mv-hold-on-failing-checks-code-toggle"));
    const code = await screen.findByTestId("mv-hold-on-failing-checks-code");
    expect(code.textContent).toBe(CODE);
    expect(
      screen.getByRole("button", { name: t("tableEditForm.holdOnFailingChecksHideCode") }),
    ).toBeInTheDocument();
  });

  it("is not offered for a view that is not materialized", () => {
    renderForm(makeTable({ materialize: false }));
    expect(screen.queryByTestId("mv-hold-on-failing-checks")).toBeNull();
  });
});
