// Copyright (c) 2026 Kenneth Stott
// Canary: 7d4b2f81-6a39-4c58-9e12-0f3c8d5b7a46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-962/1168: the collapsible Snapshot Schedule panel — a calendar picker, grain (nesting or
// nth-weekday), allowed-lateness, and business-day gate — staged through the shared save path.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";
import { TableEditForm } from "../TableEditForm";
import type { RegisteredTable } from "../../../types/admin";

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

describe("TableEditForm — Snapshot Schedule panel (REQ-962/1168)", () => {
  it("is collapsed by default and expands on toggle", () => {
    renderForm(makeTable());
    const toggle = screen.getByTestId("mv-snapshot-panel-toggle");
    expect(toggle).toHaveAttribute("aria-expanded", "false"); // collapsed when no schedule set
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByTestId("mv-calendar")).toBeInTheDocument();
  });

  it("auto-opens and shows grain/lateness/business-day when a calendar is configured", () => {
    renderForm(makeTable({ mvCalendar: "fiscal-us", mvGrain: "monthly", mvAllowedLateness: 3600 }));
    expect(screen.getByTestId("mv-calendar")).toBeInTheDocument();
    expect(screen.getByTestId("mv-grain")).toBeInTheDocument();
    expect(screen.getByTestId("mv-allowed-lateness")).toBeInTheDocument();
    expect(screen.getByTestId("mv-business-day-grain")).toBeInTheDocument();
  });

  it("staging a business-day gate flows through setEditingTable", () => {
    const setEditingTable = renderForm(makeTable({ mvCalendar: "fiscal-us", mvGrain: "monthly" }));
    fireEvent.click(screen.getByTestId("mv-business-day-grain"));
    expect(setEditingTable).toHaveBeenCalledWith(
      expect.objectContaining({ mvBusinessDayGrain: true }),
    );
  });

  it("defaults the preflight gate to 'verify all inputs' and hides the custom list", () => {
    renderForm(makeTable({ mvCalendar: "fiscal-us", mvGrain: "monthly", mvExpectedEvents: null }));
    expect(screen.getByTestId("mv-expected-all")).toBeChecked();
    expect(screen.queryByTestId("mv-expected-events")).not.toBeInTheDocument();
  });

  it("switches to a custom required-inputs list when 'verify all' is unchecked", () => {
    const setEditingTable = renderForm(
      makeTable({ mvCalendar: "fiscal-us", mvGrain: "monthly", mvExpectedEvents: null }),
    );
    fireEvent.click(screen.getByTestId("mv-expected-all"));
    expect(setEditingTable).toHaveBeenCalledWith(
      expect.objectContaining({ mvExpectedEvents: [] }),
    );
  });

  it("shows the tags input when a custom required-inputs list is set", () => {
    renderForm(
      makeTable({ mvCalendar: "fiscal-us", mvGrain: "monthly", mvExpectedEvents: ["raw.orders"] }),
    );
    expect(screen.getByTestId("mv-expected-all")).not.toBeChecked();
    expect(screen.getByTestId("mv-expected-events")).toBeInTheDocument();
  });

  it("is hidden entirely for a non-materialized table", () => {
    renderForm(makeTable({ materialize: false }));
    expect(screen.queryByTestId("mv-snapshot-panel-toggle")).not.toBeInTheDocument();
  });

  it("time-travel toggle switches Store As into history (delta) mode", () => {
    // off by default → turning it on defaults the store mode to delta (append revisions)
    const setEditingTable = vi.fn();
    renderForm(makeTable(), setEditingTable);
    fireEvent.click(screen.getByRole("checkbox", { name: /time travel/i }));
    expect(setEditingTable).toHaveBeenCalledWith(
      expect.objectContaining({ mvBitemporalMode: "delta" }),
    );
  });

  it("reflects a set bitemporal mode as time-travel on, with a history key field", () => {
    renderForm(makeTable({ mvBitemporalMode: "snapshot" }));
    expect(screen.getByRole("checkbox", { name: /time travel/i })).toBeChecked();
    expect(screen.getByTestId("mv-bitemporal-key")).toBeInTheDocument();
  });

  it("opens the new-calendar modal from the picker's + button", async () => {
    renderForm(makeTable({ mvCalendar: "fiscal-us", mvGrain: "monthly" }));
    expect(screen.queryByTestId("calendar-name")).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("mv-calendar-new"));
    // Mantine Modal renders through a portal with a transition — resolve asynchronously
    expect(await screen.findByTestId("calendar-name")).toBeInTheDocument();
    expect(screen.getByTestId("calendar-base-system")).toBeInTheDocument();
  });

  it("renders the MV config as collapsible panels", () => {
    renderForm(makeTable());
    expect(screen.getByTestId("mv-refresh-panel-toggle")).toBeInTheDocument();
    expect(screen.getByTestId("mv-snapshot-panel-toggle")).toBeInTheDocument();
    const refresh = screen.getByTestId("mv-refresh-panel-toggle");
    expect(refresh).toHaveAttribute("aria-expanded", "true"); // defaults open
    fireEvent.click(refresh);
    expect(refresh).toHaveAttribute("aria-expanded", "false"); // collapses
  });
});
