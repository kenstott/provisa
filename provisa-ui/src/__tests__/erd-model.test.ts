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
    id: 1, columnName: "col", computedSqlAlias: "col",
    visibleTo: [], writableBy: [], unmaskedTo: [],
    maskType: null, maskPattern: null, maskReplace: null,
    maskValue: null, maskPrecision: null,
    alias: null, description: null, dataType: null,
    nativeFilterType: null, isPrimaryKey: false, isForeignKey: false,
    isAlternateKey: false, scope: "public",
    ...overrides,
  };
}

function makeTable(overrides: Partial<RegisteredTable> = {}): RegisteredTable {
  return {
    id: 1, sourceId: "src1", domainId: "sales", schemaName: "public",
    tableName: "orders", alias: null, description: null,
    cacheTtl: null, gqlNamingConvention: null, watermarkColumn: null,
    columns: [], columnPresets: [], apiEndpoint: null, viewSql: null,
    materialize: false, mvRefreshInterval: 0, dataProduct: false, enableAggregates: false, enableGroupBy: false, canDeployToDb: false,
    ...overrides,
  };
}

function makeRel(overrides: Partial<Relationship> = {}): Relationship {
  return {
    id: 1, sourceTableId: 1, targetTableId: 2,
    sourceTableName: "orders", sourceDomainId: "sales",
    targetTableName: "customers", sourceColumn: "customer_id",
    targetColumn: "id", cardinality: "many_to_one",
    materialize: false, refreshInterval: 0,
    targetFunctionName: null, functionArg: null,
    alias: null, graphqlAlias: null, computedCypherAlias: null,
    autoSuggested: false, disableCypher: false, ownerDomainId: null,
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
    makeCol({ id: 2, columnName: "customer_id", computedSqlAlias: "customer_id", isForeignKey: true }),
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
  const crossRel = makeRel({ id: 2, sourceTableId: 1, targetTableId: 3, sourceDomainId: "sales", targetTableName: "employees" });
  const intraRel = makeRel({ id: 1, sourceTableId: 1, targetTableId: 2 });

  const tables = [t1, t2, t3];
  const domains = [DOMAIN_SALES, DOMAIN_HR];

  it("creates domain nodes for each used domain", () => {
    const { nodes } = buildErdElements(tables, [intraRel], domains, new Set(), NO_HIDDEN, "none", null);
    const domainNodes = nodes.filter((n) => n.classes === "erd-domain");
    expect(domainNodes.map((n) => n.data.domainId)).toEqual(
      expect.arrayContaining(["sales", "hr"]),
    );
  });

  it("creates table nodes as children of domain nodes", () => {
    const { nodes } = buildErdElements(tables, [intraRel], domains, new Set(), NO_HIDDEN, "none", null);
    const tableNodes = nodes.filter((n) => n.classes === "erd-table");
    expect(tableNodes).toHaveLength(3);
    const ordersNode = tableNodes.find((n) => n.data.type === "table" && n.data.tableId === 1);
    expect(ordersNode?.data.type === "table" && ordersNode.data.parent).toBe("d:sales");
  });

  it("creates edges for visible table pairs", () => {
    const { edges } = buildErdElements(tables, [intraRel], domains, new Set(), NO_HIDDEN, "none", null);
    expect(edges).toHaveLength(1);
    expect(edges[0].data.source).toBe("t:1");
    expect(edges[0].data.target).toBe("t:2");
    expect(edges[0].data.proxy).toBe(false);
  });

  it("collapsed domain hides its table nodes but keeps domain node", () => {
    const { nodes } = buildErdElements(tables, [intraRel], domains, new Set(["sales"]), NO_HIDDEN, "none", null);
    const domainNodes = nodes.filter((n) => n.classes === "erd-domain");
    const tableNodes = nodes.filter((n) => n.classes === "erd-table");
    expect(domainNodes.map((n) => n.data.domainId)).toContain("sales");
    expect(tableNodes.every((n) => n.data.domainId !== "sales")).toBe(true);
  });

  it("collapsed source domain produces proxy edge from domain node to target table", () => {
    const { edges } = buildErdElements(
      tables, [crossRel], domains, new Set(["sales"]), NO_HIDDEN, "none", null,
    );
    expect(edges).toHaveLength(1);
    expect(edges[0].data.source).toBe("d:sales");
    expect(edges[0].data.target).toBe("t:3");
    expect(edges[0].data.proxy).toBe(true);
  });

  it("collapsed target domain produces proxy edge from source table to domain node", () => {
    const { edges } = buildErdElements(
      tables, [crossRel], domains, new Set(["hr"]), NO_HIDDEN, "none", null,
    );
    expect(edges).toHaveLength(1);
    expect(edges[0].data.source).toBe("t:1");
    expect(edges[0].data.target).toBe("d:hr");
    expect(edges[0].data.proxy).toBe(true);
  });

  it("both domains collapsed: proxy edge domain→domain", () => {
    const { edges } = buildErdElements(
      tables, [crossRel], domains, new Set(["sales", "hr"]), NO_HIDDEN, "none", null,
    );
    expect(edges).toHaveLength(1);
    expect(edges[0].data.source).toBe("d:sales");
    expect(edges[0].data.target).toBe("d:hr");
    expect(edges[0].data.proxy).toBe(true);
  });

  it("intra-domain collapsed: no edge (same collapsed domain source and target)", () => {
    const { edges } = buildErdElements(
      tables, [intraRel], domains, new Set(["sales"]), NO_HIDDEN, "none", null,
    );
    expect(edges).toHaveLength(0);
  });

  it("proxy edges deduplicate when multiple rels collapse to same domain pair", () => {
    const rel2 = makeRel({ id: 3, sourceTableId: 1, targetTableId: 3 });
    const rel3 = makeRel({ id: 4, sourceTableId: 2, targetTableId: 3 });
    const { edges } = buildErdElements(
      tables, [crossRel, rel2, rel3], domains, new Set(["sales", "hr"]), NO_HIDDEN, "none", null,
    );
    // All three collapse to d:sales → d:hr; should appear once
    expect(edges).toHaveLength(1);
  });

  it("hidden domain: tables and domain node excluded, no edges to/from it", () => {
    const { nodes, edges } = buildErdElements(
      tables, [crossRel], domains, new Set(), new Set(["hr"]), "none", null,
    );
    const domainNodes = nodes.filter((n) => n.classes === "erd-domain");
    const tableNodes = nodes.filter((n) => n.classes === "erd-table");
    expect(domainNodes.every((n) => n.data.domainId !== "hr")).toBe(true);
    expect(tableNodes.every((n) => n.data.domainId !== "hr")).toBe(true);
    expect(edges).toHaveLength(0);
  });

  it("activeDomain filters to only that domain's tables", () => {
    const { nodes } = buildErdElements(tables, [], domains, new Set(), NO_HIDDEN, "none", "hr");
    const tableNodes = nodes.filter((n) => n.classes === "erd-table");
    expect(tableNodes).toHaveLength(1);
    expect(tableNodes[0].data.type === "table" && tableNodes[0].data.tableId).toBe(3);
  });

  it("skips edges where targetTableId is null", () => {
    const fnRel = makeRel({ targetTableId: null });
    const { edges } = buildErdElements([t1], [fnRel], domains, new Set(), NO_HIDDEN, "none", null);
    expect(edges).toHaveLength(0);
  });

  it("domain node carries description from Domain list", () => {
    const { nodes } = buildErdElements(tables, [], domains, new Set(), NO_HIDDEN, "none", null);
    const salesNode = nodes.find((n) => n.classes === "erd-domain" && n.data.domainId === "sales");
    expect(salesNode?.data.description).toBe("Sales domain");
  });

  it("uses table alias when set", () => {
    const aliased = makeTable({ id: 4, domainId: "sales", tableName: "ord", alias: "Orders" });
    const { nodes } = buildErdElements([aliased], [], domains, new Set(), NO_HIDDEN, "none", null);
    const tableNode = nodes.find((n) => n.classes === "erd-table" && n.data.type === "table" && n.data.tableId === 4);
    expect(tableNode?.data.type === "table" && tableNode.data.tableName).toBe("Orders");
  });
});
