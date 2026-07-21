// Copyright (c) 2026 Kenneth Stott
// Canary: 6d2a8f14-90c7-4e35-b1a8-3f5c7e9d0a42
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1165: the preflight-check editor renders for a materialized view and stages
// its Python source through the shared setEditingTable save path.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";
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

describe("TableEditForm — MV preflight check (REQ-1165)", () => {
  it("renders the editor with the current check source for a materialized view", () => {
    const src = "def preflight(streams, ctx):\n    return ctx.ok()";
    renderForm(makeTable({ mvPreprocess: src }));
    const box = screen.getByRole("textbox", {
      name: t("tableEditForm.preprocessAria"),
    }) as HTMLTextAreaElement;
    expect(box).toBeTruthy();
    expect(box.value).toBe(src);
  });

  it("stages the edited hook source through setEditingTable", () => {
    const setEditingTable = renderForm(makeTable());
    const box = screen.getByRole("textbox", {
      name: t("tableEditForm.preprocessAria"),
    });
    fireEvent.change(box, {
      target: { value: "def preprocess(rows, ctx):\n    return []" },
    });
    expect(setEditingTable).toHaveBeenCalledWith(
      expect.objectContaining({
        mvPreprocess: "def preprocess(rows, ctx):\n    return []",
      }),
    );
  });

  it("clears the hook to null when emptied", () => {
    const setEditingTable = renderForm(
      makeTable({ mvPreprocess: "def preprocess(rows, ctx):\n    return rows" }),
    );
    const box = screen.getByRole("textbox", {
      name: t("tableEditForm.preprocessAria"),
    });
    fireEvent.change(box, { target: { value: "" } });
    expect(setEditingTable).toHaveBeenCalledWith(
      expect.objectContaining({ mvPreprocess: null }),
    );
  });

  it("hides the editor when the table is not materialized", () => {
    renderForm(makeTable({ materialize: false }));
    expect(
      screen.queryByRole("textbox", { name: t("tableEditForm.preprocessAria") }),
    ).toBeNull();
  });
});
