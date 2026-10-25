// Copyright (c) 2026 Kenneth Stott
// Canary: 0d2f7b51-93ae-4c1e-8a65-4f0e9c2d7b18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Every node a graph draws links to its own page, focused on it: one href per kind, read out of
// the node's id.

import { describe, expect, it } from "vitest";
import type { GraphNode, NodeType } from "../graphModel";
import { nodeHref } from "../nodeHref";

const node = (type: NodeType, id: string, parent?: string): GraphNode =>
  ({ id, type, label: id, parent }) as GraphNode;

// One node of each kind, keyed by kind so a kind added to NodeType without an entry here fails
// to compile.
const EACH: Record<NodeType, [GraphNode, string][]> = {
  term: [[node("term", "term:12"), "/admin/ontology?term=12"]],
  product: [[node("product", "p:c360"), "/data-products?product=c360"]],
  command: [[node("command", "f:refresh"), "/commands?command=refresh"]],
  table: [[node("table", "t:7"), "/tables?table=7"]],
  column: [
    [node("column", "c:7:email"), "/tables?table=7&section=columns&column=email"],
    // A column name holding a colon keeps it.
    [node("column", "c:7:a:b"), "/tables?table=7&section=columns&column=a%3Ab"],
  ],
  domain: [[node("domain", "d:sales"), "/admin/domains?domain=sales"]],
  tag: [[node("tag", "g:gold"), "/admin/tags?tag=gold"]],
  checker: [[node("checker", "k:7"), "/tables?table=7&section=quality"]],
  profile: [[node("profile", "r:3", "t:7"), "/tables?table=7&section=quality"]],
  relation: [
    [node("relation", "x:9"), "/tables?table=9"],
    [node("relation", "l:raw.orders"), "/lineage?focus=raw.orders"],
  ],
};

describe("a graph node's page", () => {
  for (const [kind, cases] of Object.entries(EACH))
    for (const [n, href] of cases)
      it(`${kind} ${n.id} -> ${href}`, () => {
        expect(nodeHref(n)).toBe(href);
      });
});
