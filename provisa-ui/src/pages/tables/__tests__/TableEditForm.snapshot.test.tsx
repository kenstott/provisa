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
import { render, screen, fireEvent, within } from "../../../test-utils/render";

// vmThreads shares the module registry across test files, so a neighbor's partial
// vi.mock("../../../api/admin") (e.g. table-description-autofill's) can leak in and drop
// fetchIrTypes, which TableEditForm calls on mount. Declare a complete pass-through mock so
// this file always sees the real api/admin exports regardless of run order.
vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
}));

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
    timeTravel: null,
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
      // Saved before any history was declared: what this file stages is still open to change.
      savedTable={{ ...table, businessKey: [], validTime: null, recordedTime: null }}
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

describe("TableEditForm — Snapshot Schedule panel (REQ-962/1168)", () => {
  it("is open with no toggle, with no schedule set", () => {
    renderForm(makeTable());
    expect(screen.queryByTestId("mv-snapshot-panel-toggle")).not.toBeInTheDocument();
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
    expect(setEditingTable).toHaveBeenCalledWith(expect.objectContaining({ mvExpectedEvents: [] }));
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
    expect(screen.queryByTestId("mv-snapshot-panel")).not.toBeInTheDocument();
  });

  it("Time Travel set to Bitemporal offers the three history fields in place of Store As", () => {
    // REQ-2109: off by default; turning it on shows the business key, valid time and recorded time.
    renderForm(makeTable());
    expect(screen.getByTestId("mv-persist")).toBeInTheDocument();
    expect(screen.queryByTestId("business-key")).toBeNull();
    fireEvent.click(screen.getByRole("textbox", { name: t("tableEditForm.timeTravelLabel") }));
    fireEvent.click(screen.getByRole("option", { name: t("tableEditForm.timeTravelBitemporal") }));
    expect(screen.getByTestId("business-key")).toBeInTheDocument();
    expect(screen.getByTestId("valid-time")).toBeInTheDocument();
    expect(screen.getByTestId("recorded-time")).toBeInTheDocument();
    expect(screen.queryByTestId("mv-persist")).toBeNull();
  });

  it("reflects a declared business key as time-travel on, with the history fields", () => {
    renderForm(makeTable({ businessKey: ["id"], validTime: "booked_at", recordedTime: "ts" }));
    expect(screen.getByRole("textbox", { name: t("tableEditForm.timeTravelLabel") })).toHaveValue(
      t("tableEditForm.timeTravelBitemporal"),
    );
    expect(screen.getByTestId("business-key")).toBeInTheDocument();
  });

  it("turning time travel off clears the three", () => {
    const setEditingTable = vi.fn();
    renderForm(
      makeTable({ businessKey: ["id"], validTime: "booked_at", recordedTime: "ts" }),
      setEditingTable,
    );
    fireEvent.click(screen.getByRole("textbox", { name: t("tableEditForm.timeTravelLabel") }));
    fireEvent.click(screen.getByRole("option", { name: t("tableEditForm.timeTravelOff") }));
    expect(setEditingTable).toHaveBeenCalledWith(
      expect.objectContaining({ businessKey: [], validTime: null, recordedTime: null }),
    );
  });

  it("opens the new-calendar modal from the picker's + button", async () => {
    renderForm(makeTable({ mvCalendar: "fiscal-us", mvGrain: "monthly" }));
    expect(screen.queryByTestId("calendar-name")).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("mv-calendar-new"));
    // Mantine Modal renders through a portal with a transition — resolve asynchronously
    expect(await screen.findByTestId("calendar-name")).toBeInTheDocument();
    expect(screen.getByTestId("calendar-base-system")).toBeInTheDocument();
  });

  it("renders the MV config open on the Load tab, with no toggles", () => {
    renderForm(makeTable());
    expect(screen.getByTestId("mv-refresh-panel")).toBeInTheDocument();
    expect(screen.getByTestId("mv-snapshot-panel")).toBeInTheDocument();
    expect(screen.queryByTestId("mv-refresh-panel-toggle")).not.toBeInTheDocument();
    expect(screen.queryByTestId("mv-snapshot-panel-toggle")).not.toBeInTheDocument();
    expect(
      within(screen.getByTestId("mv-refresh-panel")).getByRole("textbox", {
        name: /Refresh Interval/,
      }),
    ).toBeVisible();
  });
});
