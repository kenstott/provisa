// Copyright (c) 2026 Kenneth Stott
// Canary: d5e2f8a1-4b7c-4d9e-8f3a-2c1b6e5a9d7f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect } from "vitest";
import { buildTableLabel, buildErdElements } from "../components/erd/erd-model";
import type { RegisteredTable, Relationship, Domain, TableColumn } from "../types/admin";

// ── fixtures ──────────────────────────────────────────────────────────────────

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
    cardinality: "many_to_one",
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
    viaTableId: null,
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

const DOMAIN_SALES: Domain = { id: "sales", description: "Sales domain" };
const DOMAIN_HR: Domain = { id: "hr", description: "HR domain" };
const NO_HIDDEN = new Set<string>();

// ── buildTableLabel ───────────────────────────────────────────────────────────

describe("buildTableLabel", () => {
  const cols = [
    makeCol({ id: 1, columnName: "id", computedSqlAlias: "id", isPrimaryKey: true }),
    makeCol({
      id: 2,
      columnName: "customer_id",
      computedSqlAlias: "customer_id",
      isForeignKey: true,
    }),
    makeCol({ id: 3, columnName: "amount", computedSqlAlias: "amount" }),
  ];

  it("none: returns just the name, lineCount 1", () => {
    const { label, lineCount } = buildTableLabel("orders", cols, "none");
    expect(label).toBe("orders");
    expect(lineCount).toBe(1);
  });

  it("key: returns name + separator + pk + fk cols only", () => {
    const { label, lineCount } = buildTableLabel("orders", cols, "key");
    expect(label).toContain("orders");
    expect(label).toContain("id");
    expect(label).toContain("customer_id");
    expect(label).not.toContain("amount");
    expect(lineCount).toBeGreaterThan(1);
  });

  it("all: returns all columns", () => {
    const { label, lineCount } = buildTableLabel("orders", cols, "all");
    expect(label).toContain("amount");
    expect(lineCount).toBe(cols.length + 2); // name + separator + cols
  });

  it("key with no key columns falls back to name only", () => {
    const noCols = [makeCol({ isPrimaryKey: false, isForeignKey: false })];
    const { label, lineCount } = buildTableLabel("foo", noCols, "key");
    expect(label).toBe("foo");
    expect(lineCount).toBe(1);
  });

  it("all with no columns returns just the name", () => {
    const { label, lineCount } = buildTableLabel("foo", [], "all");
    expect(label).toBe("foo");
    expect(lineCount).toBe(1);
  });

  it("uses alias (computedSqlAlias) in the label", () => {
    const col = makeCol({ columnName: "raw", computedSqlAlias: "nice_name" });
    const { label } = buildTableLabel("t", [col], "all");
    expect(label).toContain("nice_name");
    expect(label).not.toContain("raw");
  });
});

// ── buildErdElements ──────────────────────────────────────────────────────────

describe("buildErdElements", () => {
  const t1 = makeTable({ id: 1, domainId: "sales", tableName: "orders" });
  const t2 = makeTable({ id: 2, domainId: "sales", tableName: "customers" });
  const t3 = makeTable({ id: 3, domainId: "hr", tableName: "employees" });
  // cross-domain relationship: sales.orders → hr.employees
  const crossRel = makeRel({
    id: 2,
    sourceTableId: 1,
    targetTableId: 3,
    sourceDomainId: "sales",
    targetTableName: "employees",
  });
  const intraRel = makeRel({ id: 1, sourceTableId: 1, targetTableId: 2 });

  const tables = [t1, t2, t3];
  const domains = [DOMAIN_SALES, DOMAIN_HR];

  it("creates domain nodes for each used domain", () => {
    const { nodes } = buildErdElements(tables, [intraRel], domains, new Set(), NO_HIDDEN, "none");
    const domainNodes = nodes.filter((n) => n.classes === "erd-domain");
    expect(domainNodes.map((n) => n.data.domainId)).toEqual(
      expect.arrayContaining(["sales", "hr"]),
    );
  });

  it("creates table nodes as children of domain nodes", () => {
    const { nodes } = buildErdElements(tables, [intraRel], domains, new Set(), NO_HIDDEN, "none");
    const tableNodes = nodes.filter((n) => n.classes === "erd-table");
    expect(tableNodes).toHaveLength(3);
    const ordersNode = tableNodes.find((n) => n.data.type === "table" && n.data.tableId === 1);
    expect(ordersNode?.data.type === "table" && ordersNode.data.parent).toBe("d:sales");
  });

  it("creates edges for visible table pairs", () => {
    const { edges } = buildErdElements(tables, [intraRel], domains, new Set(), NO_HIDDEN, "none");
    expect(edges).toHaveLength(1);
    expect(edges[0].data.source).toBe("t:1");
    expect(edges[0].data.target).toBe("t:2");
    expect(edges[0].data.proxy).toBe(false);
  });

  it("collapsed domain hides its table nodes but keeps domain node", () => {
    const { nodes } = buildErdElements(
      tables,
      [intraRel],
      domains,
      new Set(["sales"]),
      NO_HIDDEN,
      "none",
    );
    const domainNodes = nodes.filter((n) => n.classes === "erd-domain");
    const tableNodes = nodes.filter((n) => n.classes === "erd-table");
    expect(domainNodes.map((n) => n.data.domainId)).toContain("sales");
    expect(tableNodes.every((n) => n.data.domainId !== "sales")).toBe(true);
  });

  it("collapsed source domain produces proxy edge from domain node to target table", () => {
    const { edges } = buildErdElements(
      tables,
      [crossRel],
      domains,
      new Set(["sales"]),
      NO_HIDDEN,
      "none",
    );
    expect(edges).toHaveLength(1);
    expect(edges[0].data.source).toBe("d:sales");
    expect(edges[0].data.target).toBe("t:3");
    expect(edges[0].data.proxy).toBe(true);
  });

  it("collapsed target domain produces proxy edge from source table to domain node", () => {
    const { edges } = buildErdElements(
      tables,
      [crossRel],
      domains,
      new Set(["hr"]),
      NO_HIDDEN,
      "none",
    );
    expect(edges).toHaveLength(1);
    expect(edges[0].data.source).toBe("t:1");
    expect(edges[0].data.target).toBe("d:hr");
    expect(edges[0].data.proxy).toBe(true);
  });

  it("both domains collapsed: proxy edge domain→domain", () => {
    const { edges } = buildErdElements(
      tables,
      [crossRel],
      domains,
      new Set(["sales", "hr"]),
      NO_HIDDEN,
      "none",
    );
    expect(edges).toHaveLength(1);
    expect(edges[0].data.source).toBe("d:sales");
    expect(edges[0].data.target).toBe("d:hr");
    expect(edges[0].data.proxy).toBe(true);
  });

  it("intra-domain collapsed: no edge (same collapsed domain source and target)", () => {
    const { edges } = buildErdElements(
      tables,
      [intraRel],
      domains,
      new Set(["sales"]),
      NO_HIDDEN,
      "none",
    );
    expect(edges).toHaveLength(0);
  });

  it("proxy edges deduplicate when multiple rels collapse to same domain pair", () => {
    const rel2 = makeRel({ id: 3, sourceTableId: 1, targetTableId: 3 });
    const rel3 = makeRel({ id: 4, sourceTableId: 2, targetTableId: 3 });
    const { edges } = buildErdElements(
      tables,
      [crossRel, rel2, rel3],
      domains,
      new Set(["sales", "hr"]),
      NO_HIDDEN,
      "none",
    );
    // All three collapse to d:sales → d:hr; should appear once
    expect(edges).toHaveLength(1);
  });

  it("hidden domain: tables and domain node excluded, no edges to/from it", () => {
    const { nodes, edges } = buildErdElements(
      tables,
      [crossRel],
      domains,
      new Set(),
      new Set(["hr"]),
      "none",
    );
    const domainNodes = nodes.filter((n) => n.classes === "erd-domain");
    const tableNodes = nodes.filter((n) => n.classes === "erd-table");
    expect(domainNodes.every((n) => n.data.domainId !== "hr")).toBe(true);
    expect(tableNodes.every((n) => n.data.domainId !== "hr")).toBe(true);
    expect(edges).toHaveLength(0);
  });

  it("hiddenDomains filters out unwanted domain's tables", () => {
    // activeDomain was removed in 312e63a3; callers now pass hiddenDomains to scope the view.
    const { nodes } = buildErdElements(tables, [], domains, new Set(), new Set(["sales"]), "none");
    const tableNodes = nodes.filter((n) => n.classes === "erd-table");
    expect(tableNodes).toHaveLength(1);
    expect(tableNodes[0].data.type === "table" && tableNodes[0].data.tableId).toBe(3);
  });

  it("skips edges where targetTableId is null", () => {
    const fnRel = makeRel({ targetTableId: null });
    const { edges } = buildErdElements([t1], [fnRel], domains, new Set(), NO_HIDDEN, "none");
    expect(edges).toHaveLength(0);
  });

  it("domain node carries description from Domain list", () => {
    const { nodes } = buildErdElements(tables, [], domains, new Set(), NO_HIDDEN, "none");
    const salesNode = nodes.find((n) => n.classes === "erd-domain" && n.data.domainId === "sales");
    expect(salesNode?.data.description).toBe("Sales domain");
  });

  it("uses table alias when set", () => {
    const aliased = makeTable({ id: 4, domainId: "sales", tableName: "ord", alias: "Orders" });
    const { nodes } = buildErdElements([aliased], [], domains, new Set(), NO_HIDDEN, "none");
    const tableNode = nodes.find(
      (n) => n.classes === "erd-table" && n.data.type === "table" && n.data.tableId === 4,
    );
    expect(tableNode?.data.type === "table" && tableNode.data.tableName).toBe("Orders");
  });
});

// ── junction-backed relationships (REQ-1588) ──────────────────────────────────

describe("buildErdElements — junction relationships", () => {
  const pets = makeTable({ id: 10, domainId: "sales", tableName: "pets" });
  const companions = makeTable({
    id: 11,
    domainId: "sales",
    tableName: "pet_companions",
    columns: [makeCol({ id: 1, columnName: "pet_id", isForeignKey: true })],
  });
  const owners = makeTable({ id: 12, domainId: "sales", tableName: "owners" });
  const domains = [DOMAIN_SALES, DOMAIN_HR];

  function viaRel(overrides: Partial<Relationship> = {}): Relationship {
    return makeRel({
      id: 100,
      sourceTableId: 10,
      targetTableId: 10,
      viaTableId: 11,
      viaTableName: "pet_companions",
      viaSourceColumn: "pet_id",
      viaTargetColumn: "companion_pet_id",
      viaTypeColumn: "relation_type",
      viaTypeValue: "bonded_pair",
      viaLabelSource: "column",
      cardinality: "many_to_many",
      ...overrides,
    });
  }

  it("draws two legs through the junction node instead of one direct edge", () => {
    const { edges } = buildErdElements(
      [pets, companions],
      [viaRel()],
      domains,
      new Set(),
      NO_HIDDEN,
      "none",
    );
    expect(edges).toHaveLength(2);
    const inLeg = edges.find((e) => e.data.target === "t:11");
    const outLeg = edges.find((e) => e.data.source === "t:11");
    expect(inLeg?.data.source).toBe("t:10");
    expect(outLeg?.data.target).toBe("t:10");
    expect(edges.every((e) => e.data.via)).toBe(true);
    expect(edges.every((e) => e.classes.includes("erd-rel--via"))).toBe(true);
  });

  it("legs carry cardinality; the type is written once, at the junction end", () => {
    const { edges } = buildErdElements(
      [pets, companions],
      [viaRel()],
      domains,
      new Set(),
      NO_HIDDEN,
      "none",
    );
    expect(edges.map((e) => e.data.label)).toEqual(["N:M", "N:M"]);
    const inLeg = edges.find((e) => e.data.target === "t:11");
    const outLeg = edges.find((e) => e.data.source === "t:11");
    expect(inLeg?.data.pathLabel).toBe("BONDED_PAIR");
    expect(outLeg?.data.pathLabel).toBe("");
    expect(outLeg?.data.pathType).toBe("BONDED_PAIR");
  });

  it("one junction node carries a distinct labeled path per relationship", () => {
    const rels = [
      viaRel({ id: 100, viaTypeValue: "bonded_pair" }),
      viaRel({ id: 101, viaTypeValue: "littermate" }),
      viaRel({ id: 102, viaTypeValue: "shares_enclosure" }),
    ];
    const { nodes, edges } = buildErdElements(
      [pets, companions],
      rels,
      domains,
      new Set(),
      NO_HIDDEN,
      "none",
    );
    expect(nodes.filter((n) => n.classes.includes("erd-junction"))).toHaveLength(1);
    expect(edges).toHaveLength(6);
    expect(new Set(edges.map((e) => e.data.pathType))).toEqual(
      new Set(["BONDED_PAIR", "LITTERMATE", "SHARES_ENCLOSURE"]),
    );
    expect(new Set(edges.map((e) => e.data.id)).size).toBe(6);
  });

  it("the junction node is a diamond showing its name only", () => {
    const { nodes } = buildErdElements(
      [pets, companions],
      [viaRel()],
      domains,
      new Set(),
      NO_HIDDEN,
      "all",
    );
    const jn = nodes.find((n) => n.data.type === "table" && n.data.tableId === 11);
    expect(jn?.classes).toBe("erd-table erd-junction");
    expect(jn?.data.type === "table" && jn.data.junction).toBe(true);
    expect(jn?.data.type === "table" && jn.data.displayLabel).toBe("pet_companions");
    const entity = nodes.find((n) => n.data.type === "table" && n.data.tableId === 10);
    expect(entity?.data.type === "table" && entity.data.junction).toBe(false);
  });

  it("a hidden junction collapses to a direct A→B edge labeled with the type", () => {
    const hrCompanions = makeTable({ id: 11, domainId: "hr", tableName: "pet_companions" });
    const { edges } = buildErdElements(
      [pets, owners, hrCompanions],
      [viaRel({ targetTableId: 12 })],
      domains,
      new Set(),
      new Set(["hr"]),
      "none",
    );
    expect(edges).toHaveLength(1);
    expect(edges[0].data.source).toBe("t:10");
    expect(edges[0].data.target).toBe("t:12");
    expect(edges[0].data.label).toBe("BONDED_PAIR");
    expect(edges[0].data.via).toBe(false);
  });

  it("a collapsed junction domain does not become a routing hop", () => {
    const hrCompanions = makeTable({ id: 11, domainId: "hr", tableName: "pet_companions" });
    const { edges } = buildErdElements(
      [pets, owners, hrCompanions],
      [viaRel({ targetTableId: 12 })],
      domains,
      new Set(["hr"]),
      NO_HIDDEN,
      "none",
    );
    expect(edges).toHaveLength(1);
    expect(edges[0].data.source).toBe("t:10");
    expect(edges[0].data.target).toBe("t:12");
    expect(edges[0].data.via).toBe(false);
  });

  it("a self-referencing junction relationship still renders", () => {
    // pets → pets through pet_companions: the direct form would be dropped as a self-loop.
    const { edges } = buildErdElements(
      [pets, companions],
      [viaRel()],
      domains,
      new Set(),
      NO_HIDDEN,
      "none",
    );
    expect(edges).toHaveLength(2);
  });
});
