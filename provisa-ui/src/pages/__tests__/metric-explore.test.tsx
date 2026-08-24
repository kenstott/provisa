// Copyright (c) 2026 Kenneth Stott
// Canary: a1674923-88d5-43b8-9870-264c5ae5eaff
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1322: role-aware schema browser groups + metric-driven canvas semantic SQL.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import { SchemaBrowser } from "../sql/SchemaBrowser";
import { JoinCanvas } from "../sql/JoinCanvas";
import { groupModelingTables, buildSemanticMetricSql } from "../sql/sqlHelpers";
import type { Metric, RegisteredTable, Relationship, TableColumn } from "../../types/admin";

function makeCol(overrides: Partial<TableColumn> = {}): TableColumn {
  return {
    id: 1,
    columnName: "col",
    computedSqlAlias: "col",
    visibleTo: [],
    writableBy: [],
    unmaskedTo: [],
    maskType: null,
    maskPattern: null,
    maskReplace: null,
    maskValue: null,
    maskPrecision: null,
    alias: null,
    description: null,
    dataType: null,
    nativeFilterType: null,
    isPrimaryKey: false,
    isForeignKey: false,
    isAlternateKey: false,
    scope: "public",
    isImplicitMeasure: false,
    isImplicitDimension: false,
    ...overrides,
  };
}

function makeTable(overrides: Partial<RegisteredTable> = {}): RegisteredTable {
  return {
    id: 1,
    sourceId: "src1",
    domainId: "sales",
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
    implicitMeasures: [],
    implicitDimensions: [],
    apiEndpoint: null,
    viewSql: null,
    dqContract: null,
    materialize: false,
    mvRefreshInterval: 0,
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
    modelingRole: null,
    ...overrides,
  };
}

function makeRel(overrides: Partial<Relationship> = {}): Relationship {
  return {
    id: 1,
    sourceTableId: 1,
    targetTableId: 2,
    sourceTableName: "orders",
    sourceDomainId: "sales",
    targetTableName: "customers",
    sourceColumn: "customer_id",
    targetColumn: "id",
    cardinality: "many-to-one",
    materialize: false,
    refreshInterval: 0,
    targetFunctionName: null,
    functionArg: null,
    alias: null,
    graphqlAlias: null,
    physicalName: null,
    computedCypherAlias: null,
    autoSuggested: false,
    disableCypher: false,
    // REQ-1586: an FK/PK relationship declares no junction.
    viaTableName: null,
    viaSourceColumn: null,
    viaTargetColumn: null,
    viaTypeColumn: null,
    viaTypeValue: null,
    viaLabelSource: null,
    ownerDomainId: null,
    ...overrides,
  };
}

const factTable = makeTable({
  id: 1,
  tableName: "orders",
  modelingRole: "fact",
  columns: [makeCol({ id: 1, columnName: "amount", computedSqlAlias: "amount" })],
});
const dimTable = makeTable({
  id: 2,
  tableName: "customers",
  modelingRole: "dimension",
  columns: [makeCol({ id: 2, columnName: "region", computedSqlAlias: "region" })],
});
const plainTable = makeTable({ id: 3, tableName: "audit_log", modelingRole: null });

const revenueMetric: Metric = {
  name: "revenue",
  expression: "SUM(orders.amount)",
  datatype: "numeric",
  description: "Total revenue",
  aiContext: null,
  visibleTo: ["admin"],
  fromFact: "orders",
};

describe("groupModelingTables", () => {
  it("splits tables into facts and dimensions by modelingRole", () => {
    const { facts, dimensions } = groupModelingTables([factTable, dimTable, plainTable]);
    expect(facts.map((t) => t.tableName)).toEqual(["orders"]);
    expect(dimensions.map((t) => t.tableName)).toEqual(["customers"]);
  });
});

describe("SchemaBrowser modeling groups (REQ-1322)", () => {
  function renderBrowser() {
    const insertAtCursor = vi.fn();
    render(
      <SchemaBrowser
        sidebarOpen
        setSidebarOpen={vi.fn()}
        domainGroups={{ sales: [factTable, dimTable, plainTable] }}
        domainMap={{}}
        expandedDomains={new Set()}
        expandedTables={new Set()}
        domainPages={{}}
        topTab="sql"
        insertAtCursor={insertAtCursor}
        toggleDomain={vi.fn()}
        toggleTable={vi.fn()}
        setDomainPages={vi.fn()}
        metrics={[revenueMetric]}
        tables={[factTable, dimTable, plainTable]}
      />,
    );
    return { insertAtCursor };
  }

  it("renders Metrics / Facts / Dimensions groups with the right members", async () => {
    const user = userEvent.setup();
    renderBrowser();
    // Metrics group is open by default and lists the metric
    expect(screen.getByTestId("schema-metric-revenue")).toBeInTheDocument();

    await user.click(screen.getByTestId("schema-group-facts"));
    expect(screen.getByTestId("schema-fact-table-orders")).toBeInTheDocument();
    expect(screen.queryByTestId("schema-fact-table-customers")).not.toBeInTheDocument();
    expect(screen.queryByTestId("schema-fact-table-audit_log")).not.toBeInTheDocument();

    await user.click(screen.getByTestId("schema-group-dimensions"));
    expect(screen.getByTestId("schema-dimension-table-customers")).toBeInTheDocument();
    expect(screen.queryByTestId("schema-dimension-table-orders")).not.toBeInTheDocument();

    // Tables also stay in their domain groups
    expect(screen.getByTestId("schema-domain-sales")).toBeInTheDocument();
  });

  it("clicking a metric inserts the semantic query", async () => {
    const user = userEvent.setup();
    const { insertAtCursor } = renderBrowser();
    await user.click(screen.getByTestId("schema-metric-revenue"));
    expect(insertAtCursor).toHaveBeenCalledWith("SELECT value FROM metrics.revenue");
  });
});

describe("JoinCanvas metric drop (REQ-1322 / REQ-1321)", () => {
  function dropMetric(name: string) {
    fireEvent.drop(screen.getByTestId("join-canvas"), {
      dataTransfer: {
        getData: (key: string) => (key === "metricName" ? name : ""),
      },
      clientX: 200,
      clientY: 200,
    });
  }

  function renderCanvas() {
    const onGenerateSql = vi.fn();
    render(
      <JoinCanvas
        tables={[factTable, dimTable, plainTable]}
        existingRels={[makeRel()]}
        metrics={[revenueMetric]}
        onGenerateSql={onGenerateSql}
      />,
    );
    return { onGenerateSql };
  }

  it("renders a metric card with dimensions from expression tables plus one hop", () => {
    renderCanvas();
    dropMetric("revenue");
    expect(screen.getByTestId("canvas-metric-card-revenue")).toBeInTheDocument();
    // orders.amount from the expression table; customers.region one relationship hop away
    expect(screen.getByTestId("canvas-metric-dim-revenue-orders.amount")).toBeInTheDocument();
    expect(screen.getByTestId("canvas-metric-dim-revenue-customers.region")).toBeInTheDocument();
    // audit_log is unrelated — not a valid dimension source
    expect(screen.queryByTestId("canvas-metric-dim-revenue-audit_log.col")).not.toBeInTheDocument();
  });

  it("generates exactly the semantic SQL — no joins, no aggregation", async () => {
    const user = userEvent.setup();
    const { onGenerateSql } = renderCanvas();
    dropMetric("revenue");

    // No dims checked → bare semantic query
    await user.click(screen.getByText("→ SQL"));
    expect(onGenerateSql).toHaveBeenLastCalledWith("SELECT value FROM metrics.revenue");

    // Check a dimension → SELECT dim, value ... GROUP BY dim
    await user.click(screen.getByTestId("canvas-metric-dim-revenue-customers.region"));
    await user.click(screen.getByText("→ SQL"));
    expect(onGenerateSql).toHaveBeenLastCalledWith(
      "SELECT customers.region, value\nFROM metrics.revenue\nGROUP BY customers.region",
    );
    // Never contains a JOIN or aggregate emitted by the UI
    const sql = onGenerateSql.mock.calls.at(-1)![0] as string;
    expect(sql).not.toMatch(/\bJOIN\b/i);
    expect(sql).not.toMatch(/\bSUM\s*\(/i);
  });
});

describe("buildSemanticMetricSql", () => {
  it("emits the exact semantic form", () => {
    expect(buildSemanticMetricSql("m", [])).toBe("SELECT value FROM metrics.m");
    expect(buildSemanticMetricSql("m", ["a.b", "c.d"])).toBe(
      "SELECT a.b, c.d, value\nFROM metrics.m\nGROUP BY a.b, c.d",
    );
  });
});
