// Copyright (c) 2026 Kenneth Stott
// Canary: 3c1f8e27-9a54-4d0b-b6e2-7f40c9d81a35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1586, REQ-2135: a drop's path stays in the selected domains. The maintainer's report: with
// PetStore and Shelter selected, Employees dropped on a Pets frame was joined through
// Meta:RegisteredTables by the model's own HAS_TABLE, a node of no selected domain. A path passes
// only through labels of the domains the Domains picker selects, and never through a system
// domain's labels unless the node dropped or the node dropped on is of that domain.

import { describe, it, expect } from "vitest";
import {
  buildDropExpansion,
  nodePathQuery,
  type DropExpansion,
  type PathScope,
} from "../pages/graph-drop";
import type { GNode } from "../components/graph/graph-model";
import type { SchemaRel } from "../components/graph/graph-schema-types";

const PETS = "PetStore:Pets";
const EMPLOYEES = "Shelter:Employees";
const ASSIGNMENTS = "Shelter:Assignments";
const REGISTERED = "Meta:RegisteredTables";
const PRODUCTS = "Meta:DataProducts";
const STOCK = "Warehouse:Stock";

/** Each label as /data/graph-schema answers it: its domain, and whether that is a system domain. */
const LABELS = [
  { domainLabel: "PetStore", domainId: "petstore", tableLabel: "Pets", systemDomain: false },
  { domainLabel: "Shelter", domainId: "shelter", tableLabel: "Employees", systemDomain: false },
  { domainLabel: "Shelter", domainId: "shelter", tableLabel: "Assignments", systemDomain: false },
  { domainLabel: "Meta", domainId: "meta", tableLabel: "RegisteredTables", systemDomain: true },
  { domainLabel: "Meta", domainId: "meta", tableLabel: "DataProducts", systemDomain: true },
  { domainLabel: "Warehouse", domainId: "warehouse", tableLabel: "Stock", systemDomain: false },
];

const scope = (...checked: string[]): PathScope => ({
  nodeLabels: LABELS,
  checkedDomains: new Set(checked),
});

const rel = (type: string, source: string, target: string): SchemaRel => ({
  type,
  source,
  target,
  properties: [],
  join: `${type} ${source} ${target}`,
});

/** The model's own link of every table to its registration. */
const HAS_TABLE = [rel("HAS_TABLE", PETS, REGISTERED), rel("HAS_TABLE", EMPLOYEES, REGISTERED)];
const PETS_FRAME = "MATCH (n:PetStore:Pets) RETURN n LIMIT 25";

function drop(query: string, label: string, rels: SchemaRel[], s: PathScope) {
  return buildDropExpansion(query, label, rels, new Set(), s);
}

function expanded(query: string, label: string, rels: SchemaRel[], s: PathScope): DropExpansion {
  const done = drop(query, label, rels, s);
  if ("refused" in done) throw new Error(`refused: ${done.refused.key}`);
  return done;
}

describe("a drop's path stays in the selected domains", () => {
  it("refuses by name a drop joined only through the model's metadata, and adds nothing", () => {
    expect(drop(PETS_FRAME, EMPLOYEES, HAS_TABLE, scope("petstore", "shelter"))).toEqual({
      refused: {
        key: "graphFrame.sparqlDropNoPath",
        values: { name: EMPLOYEES, classes: PETS },
      },
    });
  });

  it("refuses it with meta selected too: a system domain is no way through", () => {
    const done = drop(PETS_FRAME, EMPLOYEES, HAS_TABLE, scope("petstore", "shelter", "meta"));
    expect("refused" in done && done.refused.key).toBe("graphFrame.sparqlDropNoPath");
  });

  it("writes the path through the selected domains, and not the one through meta", () => {
    const rels = [
      ...HAS_TABLE,
      rel("ASSIGNED_PET", ASSIGNMENTS, PETS),
      rel("IS_EMPLOYEE", ASSIGNMENTS, EMPLOYEES),
    ];
    const out = expanded(PETS_FRAME, EMPLOYEES, rels, scope("petstore", "shelter"));
    expect(out.query).toContain(
      "OPTIONAL MATCH (n)-[rAssignments:ASSIGNED_PET]-(mAssignments:Shelter:Assignments)" +
        "-[rEmployees:IS_EMPLOYEE]-(mEmployees:Shelter:Employees)",
    );
    expect(out.query).not.toContain("HAS_TABLE");
    expect(out.query).not.toContain(REGISTERED);
    expect(out.query.match(/OPTIONAL MATCH/g)).toHaveLength(1);
  });

  it("may use meta's relationships when the frame holds a meta label", () => {
    const rels = [...HAS_TABLE, rel("DESCRIBES", PRODUCTS, REGISTERED)];
    const out = expanded(
      `MATCH (d:${PRODUCTS}) RETURN d`,
      EMPLOYEES,
      rels,
      scope("petstore", "shelter", "meta"),
    );
    expect(out.query).toContain(
      "OPTIONAL MATCH (d)-[rRegisteredTa:DESCRIBES]-(mRegisteredTa:Meta:RegisteredTables)" +
        "-[rEmployees:HAS_TABLE]-(mEmployees:Shelter:Employees)",
    );
  });

  it("may use meta's relationships when the label dropped is meta's", () => {
    const rels = [...HAS_TABLE, rel("DESCRIBES", PRODUCTS, REGISTERED)];
    const out = expanded(PETS_FRAME, PRODUCTS, rels, scope("petstore", "shelter", "meta"));
    expect(out.query).toContain("-[rRegisteredTa:HAS_TABLE]-(mRegisteredTa:Meta:RegisteredTables)");
    expect(out.query).toContain("-[rDataProducts:DESCRIBES]-(mDataProducts:Meta:DataProducts)");
  });

  it("does not write a path through a label of a data domain not selected", () => {
    const rels = [rel("STOCKS", STOCK, PETS), rel("STAFFED_BY", STOCK, EMPLOYEES)];
    const done = drop(PETS_FRAME, EMPLOYEES, rels, scope("petstore", "shelter"));
    expect(done).toEqual({
      refused: { key: "graphFrame.sparqlDropNoPath", values: { name: EMPLOYEES, classes: PETS } },
    });
    // Selected, the same path is written.
    const out = expanded(PETS_FRAME, EMPLOYEES, rels, scope("petstore", "shelter", "warehouse"));
    expect(out.query).toContain("(mStock:Warehouse:Stock)");
  });

  it("reads no selection as every domain, as the sidebar does, and still keeps meta out", () => {
    const rels = [...HAS_TABLE, rel("STOCKS", STOCK, PETS), rel("STAFFED_BY", STOCK, EMPLOYEES)];
    const out = expanded(PETS_FRAME, EMPLOYEES, rels, scope());
    expect(out.query).toContain("(mStock:Warehouse:Stock)");
    expect(out.query).not.toContain(REGISTERED);
  });
});

describe("the paths between two nodes dragged together stay in the selected domains", () => {
  const node = (label: string, id: number): GNode => ({
    id,
    label,
    tableLabel: label.split(":").pop() ?? label,
    properties: { id },
  });
  const key = () => "id";
  const rels = [
    ...HAS_TABLE,
    rel("ASSIGNED_PET", ASSIGNMENTS, PETS),
    rel("IS_EMPLOYEE", ASSIGNMENTS, EMPLOYEES),
    rel("STOCKS", STOCK, PETS),
  ];

  it("names in its pattern only the relationship types whose labels it may pass through", () => {
    const out = nodePathQuery(
      node(PETS, 1),
      node(EMPLOYEES, 2),
      key,
      rels,
      new Set(),
      scope("petstore", "shelter"),
    );
    expect(out).toEqual({
      query: [
        "MATCH p = allShortestPaths((a:PetStore:Pets)-[:ASSIGNED_PET|IS_EMPLOYEE*..4]-(b:Shelter:Employees))",
        "WHERE a.id = 1 AND b.id = 2",
        "RETURN p",
        "LIMIT 25",
      ].join("\n"),
    });
  });

  it("says by name when no path stays in the selected domains, and runs nothing", () => {
    expect(
      nodePathQuery(
        node(PETS, 1),
        node(EMPLOYEES, 2),
        key,
        HAS_TABLE,
        new Set(),
        scope("petstore", "shelter"),
      ),
    ).toEqual({
      refused: { key: "graphFrame.sparqlDropNoPath", values: { name: EMPLOYEES, classes: PETS } },
    });
  });

  it("says a node with no key needs one, as before", () => {
    const keyless: GNode = { id: 3, label: PETS, tableLabel: "Pets", properties: {} };
    expect(
      nodePathQuery(keyless, node(EMPLOYEES, 2), key, rels, new Set(), scope("petstore", "shelter")),
    ).toEqual({
      refused: { key: "graphFrame.cypherPathNoKey", values: { from: PETS, to: EMPLOYEES } },
    });
  });
});
