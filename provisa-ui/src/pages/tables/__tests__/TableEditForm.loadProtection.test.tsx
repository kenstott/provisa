// Copyright (c) 2026 Kenneth Stott
// Canary: 8f4c2a91-b3d7-4e16-9a05-6d7b1c4f2e69
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1141/1143: the load-protection controls (load_protected + off-peak window) and the
// server-derived refresh-policy summary banner (with misconfiguration warning) render.

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
    tableName: "orders",
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
    viewSql: null,
    materialize: false,
    mvRefreshInterval: 300,
    mvDebounceQuiet: 0,
    mvDebounceMaxDelay: 5,
    mvConsistency: "shared",
    mvPreprocess: null,
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

describe("TableEditForm — load protection + refresh-policy summary (REQ-1141/1143)", () => {
  it("does not render the summary banner when none is provided", () => {
    renderForm(makeTable());
    expect(screen.queryByTestId("refresh-policy-summary")).toBeNull();
  });

  it("renders the scheduled-snapshot summary text", () => {
    renderForm(
      makeTable({
        refreshPolicySummary: {
          text: "Scheduled snapshot — refreshed during 01:00–03:00 UTC; queries never touch the source.",
          serving: "scheduled",
          warning: null,
        },
      }),
    );
    const banner = screen.getByTestId("refresh-policy-summary");
    expect(within(banner).getByText(/Scheduled snapshot/)).toBeTruthy();
    expect(within(banner).getByText(/queries never touch the source/)).toBeTruthy();
  });

  it("surfaces a misconfiguration warning when present", () => {
    renderForm(
      makeTable({
        refreshPolicySummary: {
          text: "Live — reached directly, always fresh.",
          serving: "live",
          warning: "prefer_materialized has no effect on this engine: served live.",
        },
      }),
    );
    const banner = screen.getByTestId("refresh-policy-summary");
    expect(within(banner).getByText(/has no effect on this engine/)).toBeTruthy();
  });

  it("stages an off-peak window edit through setEditingTable", () => {
    const setEditingTable = renderForm(makeTable());
    const input = screen.getByPlaceholderText(t("tableEditForm.offPeakWindowPlaceholder"));
    fireEvent.change(input, { target: { value: "01:00-03:00" } });
    expect(setEditingTable).toHaveBeenCalledWith(
      expect.objectContaining({ offPeakWindow: "01:00-03:00" }),
    );
  });
});
