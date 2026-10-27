// Copyright (c) 2026 Kenneth Stott
// Canary: 4edc2ff1-8e89-4c12-882e-ae128dfb152c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2109: one Time Travel section -- Off or Bitemporal -- in the sections that create and edit
// a table or a view, and in the model form. Once versions exist the editor no longer offers the
// switch, the business key or the valid time.

import { useState } from "react";
import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, renderInForm, screen, waitFor } from "../../../test-utils/render";
import { MVList } from "../../../hooks/admin.graphql";

import type { RegisteredTable, TableColumn } from "../../../types/admin";
import i18n from "../../../i18n";
import { TableTimeTravel } from "../TimeTravelSection";
import { MaterializedViewPanels } from "../MaterializedViewPanels";
import { ModelingForm } from "../ModelingForm";

const t = i18n.getFixedT("en");

const col = (name: string, isPrimaryKey = false): TableColumn => ({
  id: 1,
  columnName: name,
  visibleTo: [],
  masks: [],
  alias: null,
  computedSqlAlias: name,
  computedGqlAlias: name,
  description: null,
  dataType: "varchar",
  nativeFilterType: null,
  isPrimaryKey,
  isForeignKey: false,
  isAlternateKey: false,
  scope: "domain",
  isImplicitMeasure: false,
  isImplicitDimension: false,
});

function makeTable(overrides: Partial<RegisteredTable> = {}): RegisteredTable {
  return {
    id: 1,
    sourceId: "src",
    domainId: "dom",
    schemaName: "views",
    tableName: "trades",
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
    columns: [col("trade_id"), col("trade_date"), col("updated_at")],
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
    productId: null,
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    writeOps: [],
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

const DECLARED = {
  businessKey: ["trade_id"],
  validTime: "trade_date",
  recordedTime: "updated_at",
};

const mvList = (lastRefreshAt: number | null) => ({
  request: { query: MVList },
  result: {
    data: {
      mvList: [
        {
          __typename: "MVType",
          id: "view-trades",
          sourceTables: [],
          targetTable: "mv_trades",
          refreshInterval: 300,
          enabled: true,
          status: "fresh",
          lastRefreshAt,
          rowCount: lastRefreshAt === null ? null : 12,
          lastError: null,
        },
      ],
    },
  },
});

function pickTimeTravel(choice: "timeTravelBitemporal" | "timeTravelOff") {
  fireEvent.click(screen.getByRole("textbox", { name: t("tableEditForm.timeTravelLabel") }));
  fireEvent.click(screen.getByRole("option", { name: t(`tableEditForm.${choice}`) }));
}

describe("Time Travel — one section, Off or Bitemporal", () => {
  it("Bitemporal shows the three fields and Off hides and clears them", () => {
    let seen: RegisteredTable = makeTable({ viewSql: null, materialize: false });
    function Harness() {
      const [table, setTable] = useState<RegisteredTable | null>(seen);
      seen = table!;
      return (
        <TableTimeTravel
          editingTable={table!}
          setEditingTable={(next) => setTable(next)}
          saved={null}
        />
      );
    }
    renderInForm(<Harness />);
    expect(screen.getByTestId("time-travel-section")).toHaveTextContent(
      t("tableEditForm.timeTravelPanel"),
    );
    expect(screen.queryByTestId("business-key")).toBeNull();
    pickTimeTravel("timeTravelBitemporal");
    for (const id of ["business-key", "valid-time", "recorded-time"])
      expect(screen.getByTestId(id)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("textbox", { name: t("tableEditForm.validTimeLabel") }));
    fireEvent.click(screen.getByRole("option", { name: "trade_date" }));
    expect(seen.validTime).toBe("trade_date");
    pickTimeTravel("timeTravelOff");
    expect(screen.queryByTestId("valid-time")).toBeNull();
    expect(seen).toMatchObject({ businessKey: [], validTime: null, recordedTime: null });
  });

  it("the section's (?) explains the word once", () => {
    const table = makeTable({ viewSql: null, materialize: false });
    renderInForm(<TableTimeTravel editingTable={table} setEditingTable={vi.fn()} saved={null} />);
    expect(screen.getByTestId("time-travel-section-info")).toBeInTheDocument();
    expect(t("tableEditForm.timeTravelSectionHelp")).toContain(
      "keeps both when it happened and when it was recorded",
    );
  });

  it("New model asks Time Travel in place of a history mode; its key is the business key", () => {
    render(<ModelingForm domains={[]} onSuccess={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.getByTestId("time-travel-section")).toBeInTheDocument();
    expect(screen.queryByTestId("modeling-history")).toBeNull();
    expect(
      screen.getByRole("textbox", { name: t("tableEditForm.businessKeyLabel") }),
    ).toBeInTheDocument();
    pickTimeTravel("timeTravelBitemporal");
    expect(screen.getByTestId("valid-time")).toBeInTheDocument();
    expect(screen.getByTestId("recorded-time")).toBeInTheDocument();
    // The key above is the business key: it is not asked twice.
    expect(screen.queryByTestId("business-key")).toBeNull();
  });
});

/** Once versions exist: no switch, no business key, no valid time; the recorded time stays. */
function expectLocked() {
  expect(screen.getByTestId("time-travel-section")).toBeInTheDocument();
  expect(screen.queryByTestId("time-travel")).toBeNull();
  expect(screen.queryByTestId("business-key")).toBeNull();
  expect(screen.queryByTestId("valid-time")).toBeNull();
  expect(screen.getByTestId("recorded-time")).toBeInTheDocument();
}

function expectOffered() {
  expect(screen.getByTestId("time-travel")).toBeInTheDocument();
  expect(screen.getByTestId("business-key")).toBeInTheDocument();
  expect(screen.getByTestId("valid-time")).toBeInTheDocument();
}

describe("Time Travel — the editor hides what can no longer change once versions exist", () => {
  it("a built bitemporal view: the switch, business key and valid time are not offered", async () => {
    const saved = makeTable(DECLARED);
    renderInForm(
      <MaterializedViewPanels editingTable={saved} setEditingTable={vi.fn()} saved={saved} />,
      { mocks: [mvList(1_700_000_000)] },
    );
    await waitFor(expectLocked);
  });

  it("a bitemporal view not yet built can still change", async () => {
    const saved = makeTable(DECLARED);
    renderInForm(
      <MaterializedViewPanels editingTable={saved} setEditingTable={vi.fn()} saved={saved} />,
      { mocks: [mvList(null)] },
    );
    await waitFor(expectOffered);
  });

  it("a view whose build cannot be read is treated as built", async () => {
    const saved = makeTable(DECLARED);
    renderInForm(
      <MaterializedViewPanels editingTable={saved} setEditingTable={vi.fn()} saved={saved} />,
      { mocks: [{ request: { query: MVList }, error: new Error("no observability") }] },
    );
    await waitFor(expectLocked);
  });

  it("a table updated in place (business key = primary key) keeps a captured log: hidden", () => {
    const saved = makeTable({
      ...DECLARED,
      viewSql: null,
      materialize: false,
      columns: [col("trade_id", true), col("trade_date"), col("updated_at")],
    });
    renderInForm(<TableTimeTravel editingTable={saved} setEditingTable={vi.fn()} saved={saved} />);
    expectLocked();
  });

  it("a table whose versions are its own rows may map them anew", () => {
    const saved = makeTable({ ...DECLARED, viewSql: null, materialize: false });
    renderInForm(<TableTimeTravel editingTable={saved} setEditingTable={vi.fn()} saved={saved} />);
    expectOffered();
  });

  it("a table being created offers everything", () => {
    const draft = makeTable({
      ...DECLARED,
      viewSql: null,
      materialize: false,
      columns: [col("trade_id", true), col("trade_date"), col("updated_at")],
    });
    renderInForm(<TableTimeTravel editingTable={draft} setEditingTable={vi.fn()} saved={null} />);
    expectOffered();
  });
});
