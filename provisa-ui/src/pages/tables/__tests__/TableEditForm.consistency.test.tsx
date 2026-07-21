// Copyright (c) 2026 Kenneth Stott
// Canary: 9e99076c-e8fb-4e48-baae-f67da7a4c7d0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-879: the MV consistency selector renders for a materialized view and
// stages the chosen tier through the shared setEditingTable save path.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, within } from "../../../test-utils/render";
import { TableEditForm } from "../TableEditForm";
import type { RegisteredTable } from "../../../types/admin";
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

function renderForm(table: RegisteredTable, setEditingTable = vi.fn()) {
  render(
    <TableEditForm
      editingTable={table}
      setEditingTable={setEditingTable}
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
    />,
  );
  return setEditingTable;
}

describe("TableEditForm — MV consistency (REQ-879)", () => {
  it("renders the selector with the current tier for a materialized view", () => {
    renderForm(makeTable({ mvConsistency: "distributed" }));
    const sel = screen.getByRole("textbox", {
      name: t("tableEditForm.mvConsistencyAria"),
    }) as HTMLInputElement;
    expect(sel).toBeTruthy();
    expect(sel.value).toBe(t("tableEditForm.consistencyDistributed"));
  });

  it("stages the chosen tier through setEditingTable", async () => {
    const setEditingTable = renderForm(makeTable());
    fireEvent.click(
      screen.getByRole("textbox", { name: t("tableEditForm.mvConsistencyAria") }),
    );
    const listbox = await screen.findByRole("listbox");
    fireEvent.click(
      within(listbox).getByText(t("tableEditForm.consistencyDistributed")),
    );
    expect(setEditingTable).toHaveBeenCalledWith(
      expect.objectContaining({ mvConsistency: "distributed" }),
    );
  });

  it("hides the selector when the table is not materialized", () => {
    renderForm(makeTable({ materialize: false }));
    expect(
      screen.queryByRole("textbox", { name: t("tableEditForm.mvConsistencyAria") }),
    ).toBeNull();
  });
});
