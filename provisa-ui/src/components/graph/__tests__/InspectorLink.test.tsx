// Copyright (c) 2026 Kenneth Stott
// Canary: 5e8a1c74-2b9d-4f60-a3e7-91c4d0b6f285
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Cypher canvas's inspector links what it shows to its own page: a node to the table its
// label reads, an edge to the relationship that declares it.

import { describe, it, expect } from "vitest";
import { screen } from "@testing-library/react";
import { render } from "../../../test-utils/render";
import { Inspector } from "../Inspector";
import type { GEdge, GNode } from "../graph-model";
import type { Relationship } from "../../../types/admin";

const from: GNode = { id: 1, label: "sales:Order", tableLabel: "orders", properties: {} };
const to: GNode = { id: 2, label: "sales:Customer", tableLabel: "customers", properties: {} };
const edge: GEdge = {
  identity: "e1",
  start: 1,
  end: 2,
  type: "PLACED_BY",
  properties: {},
  startNode: from,
  endNode: to,
};
const rel = {
  id: 7,
  alias: "PLACED_BY",
  graphqlAlias: null,
  cypherType: "PLACED_BY",
} as unknown as Relationship;

function show(selected: { kind: "node"; data: GNode } | { kind: "edge"; data: GEdge }) {
  render(
    <Inspector
      selected={selected}
      colorOverrides={{}}
      onColorChange={() => {}}
      onClose={() => {}}
      width={320}
      onResizeStart={() => {}}
      relationships={[rel]}
      labelToTableId={{ "sales:Order": 41 }}
    />,
  );
}

describe("the inspector's link", () => {
  it("opens a node's table", () => {
    show({ kind: "node", data: from });
    expect(screen.getByTestId("inspector-open")).toHaveAttribute("href", "/tables?table=41");
  });

  it("opens an edge's relationship", () => {
    show({ kind: "edge", data: edge });
    expect(screen.getByTestId("inspector-open")).toHaveAttribute(
      "href",
      "/relationships?relationship=7",
    );
  });

  it("offers none for a node whose label reads no registered table", () => {
    show({ kind: "node", data: to });
    expect(screen.queryByTestId("inspector-open")).toBeNull();
  });
});
