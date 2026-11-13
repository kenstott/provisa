// Copyright (c) 2026 Kenneth Stott
// Canary: 4e7b2c19-8d3a-4f61-b5e2-7a9c1d0f3b86
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-12, A HOP IS A STEP BETWEEN TERMS): the maintainer's chain (test3
// PART_OF test2 PART_OF test RELATED_TO animal breed name, J1230) with associations added: test3
// RELATED_TO kennel kin, a term reached only by that link, with its own column, table and product;
// kennel kin SYNONYM_OF kin's kin, two association steps from test3, with its own too.

import type { GraphData } from "../graphModel";

export const TEST3 = "term:11";
export const TEST2 = "term:12";
export const TEST = "term:13";
export const BREED_NAME = "term:14";
export const KIN = "term:17";
export const KIN2 = "term:18";

const term = (id: string, label: string) => ({ id, type: "term" as const, label });
const column = (table: number, name: string) => ({
  id: `c:${table}:${name}`,
  type: "column" as const,
  label: name,
  parent: `t:${table}`,
});
const table = (id: number, label: string) => ({ id: `t:${id}`, type: "table" as const, label });
const product = (id: string, label: string) => ({ id, type: "product" as const, label });
const termEdge = (id: string, a: string, b: string, label: string) => ({
  id,
  a,
  b,
  kind: "term_edge" as const,
  label,
});
/** A term bound to a column of a table a product holds. */
const grounded = (term: string, tableId: number, col: string, productId: string) => [
  { id: `b:${term}`, a: term, b: `c:${tableId}:${col}`, kind: "term_binding" as const },
  { id: `h:${tableId}:${col}`, a: `t:${tableId}`, b: `c:${tableId}:${col}`, kind: "has_column" as const },
  { id: `m:${productId}:${tableId}`, a: productId, b: `t:${tableId}`, kind: "member" as const },
];

export const ASSOCIATION_CHAIN: GraphData = {
  nodes: [
    term(TEST3, "test3"),
    term(TEST2, "test2"),
    term(TEST, "test"),
    term(BREED_NAME, "animal breed name"),
    term(KIN, "kennel kin"),
    term(KIN2, "kin's kin"),
    table(201, "breeds"),
    table(202, "kennels"),
    table(203, "kin"),
    table(204, "kin2"),
    column(201, "breed_name"),
    column(202, "label"),
    column(203, "kin"),
    column(204, "kin2"),
    product("p:ph", "Pet Health"),
    product("p:kn", "Kennel"),
    product("p:kin", "Kin"),
    product("p:kin2", "Kin Two"),
  ],
  edges: [
    termEdge("e1", TEST3, TEST2, "PART_OF"),
    termEdge("e2", TEST2, TEST, "PART_OF"),
    termEdge("e3", TEST, BREED_NAME, "RELATED_TO"),
    termEdge("e4", TEST3, KIN, "RELATED_TO"),
    termEdge("e5", KIN, KIN2, "SYNONYM_OF"),
    ...grounded(TEST3, 202, "label", "p:kn"),
    ...grounded(BREED_NAME, 201, "breed_name", "p:ph"),
    ...grounded(KIN, 203, "kin", "p:kin"),
    ...grounded(KIN2, 204, "kin2", "p:kin2"),
  ],
};
