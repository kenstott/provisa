// Copyright (c) 2026 Kenneth Stott
// Canary: 4c8e2a17-93d5-4b0f-be61-7a2f5d9c3e08
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The ERD's automatic layout -- ELK, then the arrangement ErdPanel runs when ELK stops -- leaves no
// two tables of a domain overlapping. The diagram is the five domains of J562's renders, where the
// gap pass pushed carts onto orders, budgets onto ledger and fx_rates onto payments.

import { describe, expect, it } from "vitest";
import cytoscape from "cytoscape";
import { buildErdElements, type ColumnDetail } from "../components/erd/erd-model";
import { erdGraph } from "../components/erd/erd-graph";
import { graphElements } from "../components/provisa-graph/model";
import { graphStylesheet } from "../components/provisa-graph/stylesheet";
import { graphTokens } from "../components/provisa-graph/tokens";
import { ERD_ELK_LAYOUT } from "../components/provisa-graph/presets";
import provisaElk, { LAYOUT_FAIL, PROVISA_ELK } from "../components/provisa-graph/elkLayout";
import { arrangeLayered, nodesWithEdges } from "../components/provisa-graph/arrange";
import type { CyInstance } from "../components/graph/cytoscape-types";
import type { Domain, ListedTable, Relationship } from "../types/admin";

// The layout the ERD's graph runs: Provisa's own ELK adapter.
if (!(cytoscape as unknown as (type: string, name: string) => unknown)("layout", PROVISA_ELK)) {
  cytoscape.use(provisaElk as unknown as Parameters<typeof cytoscape.use>[0]);
}

const SPEC: Record<string, string[]> = {
  sales: [
    "orders",
    "order_lines",
    "customers",
    "products",
    "shipments",
    "returns",
    "carts",
    "coupons",
  ],
  finance: ["ledger", "invoices", "payments", "accounts", "budgets", "fx_rates"],
  hr: ["employees", "departments", "payroll", "roles"],
  marketing: ["campaigns", "leads", "channels", "touches", "segments"],
  ops: ["audit_log", "scratch", "jobs"],
};

const col = (columnName: string, isPrimaryKey = false) => ({
  columnName,
  dataType: "integer",
  isPrimaryKey,
});

const TABLES = Object.entries(SPEC)
  .flatMap(([domainId, names]) =>
    names.map((tableName) => ({
      tableName,
      domainId,
      schemaName: "public",
      sourceId: "warehouse",
      columns: [col("id", true), col(`${tableName}_ref`), col("amount")],
    })),
  )
  .map((t, i) => ({ ...t, id: i + 1 }) as unknown as ListedTable);

const tableId = (name: string) => TABLES.find((t) => t.tableName === name)!.id;

const REL_PAIRS: Array<[string, string]> = [
  ["order_lines", "orders"],
  ["orders", "customers"],
  ["order_lines", "products"],
  ["shipments", "orders"],
  ["returns", "order_lines"],
  ["carts", "customers"],
  ["coupons", "orders"],
  ["invoices", "accounts"],
  ["payments", "invoices"],
  ["ledger", "accounts"],
  ["budgets", "accounts"],
  ["fx_rates", "ledger"],
  ["employees", "departments"],
  ["payroll", "employees"],
  ["roles", "employees"],
  ["leads", "campaigns"],
  ["touches", "leads"],
  ["touches", "channels"],
  ["segments", "campaigns"],
  ["invoices", "orders"],
  ["leads", "customers"],
];

const RELS = REL_PAIRS.map(
  ([a, b], i) =>
    ({
      id: `rel-${i + 1}`,
      sourceTableId: tableId(a),
      targetTableId: tableId(b),
      sourceTableName: a,
      targetTableName: b,
      sourceDomainId: TABLES.find((t) => t.tableName === a)!.domainId,
      sourceColumn: `${b}_ref`,
      targetColumn: "id",
      cardinality: "many-to-one",
    }) as unknown as Relationship,
);

const DOMAINS = Object.keys(SPEC).map((id) => ({ id, description: `${id} domain` }) as Domain);

type BB = { x1: number; x2: number; y1: number; y2: number };

/** The diagram laid out as the ERD's graph (ProvisaGraph's layered preset) lays it out: orphans
 * hidden, ELK, then the arrangement. */
async function laidOut(columnDetail: ColumnDetail, gridSnap: number, aspectRatio: number) {
  const erd = buildErdElements(TABLES, RELS, DOMAINS, new Set(), new Set(), "all");
  const elements = graphElements(erdGraph(erd, TABLES), (type) => type, columnDetail);
  const cy = cytoscape({
    headless: true,
    styleEnabled: true,
    elements: elements as unknown as Parameters<typeof cytoscape>[0]["elements"],
    style: graphStylesheet(graphTokens(true)) as unknown as Parameters<
      typeof cytoscape
    >[0]["style"],
  }) as unknown as CyInstance;
  const connected = nodesWithEdges(cy);
  cy.nodes(".pg-node").forEach((n) => {
    if (!connected.has((n as { id(): string }).id())) n.style("display", "none");
  });
  const layout = cy.layout(ERD_ELK_LAYOUT);
  // A failed layout fails the test, with ELK's error, rather than leaving it waiting.
  const stopped = new Promise<void>((resolve, reject) => {
    layout.one(LAYOUT_FAIL as "layoutstop", (_evt: unknown, error?: unknown) => reject(error));
    layout.one("layoutstop", () => resolve());
  });
  layout.run();
  await stopped;
  arrangeLayered(cy, { gridSnap, showOrphans: true, aspectRatio });
  return cy;
}

/** Each pair of tables in one domain whose boxes intersect. */
function overlaps(cy: CyInstance): string[] {
  const tables: Array<{ name: string; domainId: string; bb: BB }> = [];
  cy.nodes(".pg-node").forEach((n) => {
    tables.push({
      name: n.data("tableName") as string,
      domainId: n.data("domainId") as string,
      bb: (n as unknown as { boundingBox(o: object): BB }).boundingBox({}),
    });
  });
  const found: string[] = [];
  for (let i = 0; i < tables.length; i++) {
    for (let j = i + 1; j < tables.length; j++) {
      const a = tables[i];
      const b = tables[j];
      if (a.domainId !== b.domainId) continue;
      if (a.bb.x1 < b.bb.x2 && b.bb.x1 < a.bb.x2 && a.bb.y1 < b.bb.y2 && b.bb.y1 < a.bb.y2) {
        found.push(`${a.name} / ${b.name}`);
      }
    }
  }
  return found;
}

describe("the ERD's automatic layout", () => {
  for (const columnDetail of ["key", "all"] as const) {
    for (const gridSnap of [0, 10, 20]) {
      for (const aspectRatio of [1280 / 900, 1900 / 900]) {
        it(`overlaps no two tables of a domain (${columnDetail} columns, snap ${gridSnap}, pane ${aspectRatio.toFixed(2)})`, async () => {
          const cy = await laidOut(columnDetail, gridSnap, aspectRatio);
          expect((cy.nodes(".pg-node") as unknown as { length: number }).length).toBe(
            TABLES.length,
          );
          expect(overlaps(cy)).toEqual([]);
        });
      }
    }
  }
});
