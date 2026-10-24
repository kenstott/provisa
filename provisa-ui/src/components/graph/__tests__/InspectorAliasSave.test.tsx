// Copyright (c) 2026 Kenneth Stott
// Canary: 92ff2d21-c53e-464d-95ab-1cd2fc9d80cc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// An edge's aliases saved from the graph inspector: the server's refusal (an alias in use, a
// reserved word) is said, and so is an error it raised; the save button is free again either way.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, screen } from "@testing-library/react";
import { render } from "../../../test-utils/render";
import { Inspector } from "../Inspector";
import type { GEdge, GNode } from "../graph-model";
import type { Relationship } from "../../../types/admin";

const from: GNode = { id: 1, label: "Order", tableLabel: "orders", properties: {} };
const to: GNode = { id: 2, label: "Customer", tableLabel: "customers", properties: {} };
const edge: GEdge = {
  identity: "e1",
  start: 1,
  end: 2,
  type: "PLACED_BY",
  properties: {},
  startNode: from,
  endNode: to,
};
const rel = { id: 7, alias: "PLACED_BY", graphqlAlias: null } as unknown as Relationship;

function open(onSaveEdgeAlias: (id: number, cql: string, gql: string) => Promise<string | null>) {
  render(
    <Inspector
      selected={{ kind: "edge", data: edge }}
      colorOverrides={{}}
      onColorChange={() => {}}
      onClose={() => {}}
      width={320}
      onResizeStart={() => {}}
      relationships={[rel]}
      onSaveEdgeAlias={onSaveEdgeAlias}
      labelToTableId={{}}
    />,
  );
  fireEvent.change(screen.getByTestId("inspector-gql-alias"), { target: { value: "placedBy" } });
  fireEvent.click(screen.getByTestId("inspector-save-alias"));
}

describe("the inspector's alias save", () => {
  it("says the server's refusal", async () => {
    open(vi.fn(async () => "GraphQL alias placedBy is already used by relationship 3"));
    expect(await screen.findByTestId("inspector-alias-error")).toHaveTextContent(
      "GraphQL alias placedBy is already used by relationship 3",
    );
    expect(screen.getByTestId("inspector-save-alias")).not.toHaveAttribute("data-loading");
  });

  it("says an error the save raised, and frees the button", async () => {
    open(
      vi.fn(async () => {
        throw new Error("Ambiguous table name: orders");
      }),
    );
    expect(await screen.findByTestId("inspector-alias-error")).toHaveTextContent(
      "Ambiguous table name: orders",
    );
    expect(screen.getByTestId("inspector-save-alias")).not.toHaveAttribute("data-loading");
  });

  it("says nothing once saved", async () => {
    const save = vi.fn(async () => null);
    open(save);
    await vi.waitFor(() => expect(save).toHaveBeenCalledWith(7, "PLACED_BY", "placedBy"));
    expect(screen.queryByTestId("inspector-alias-error")).toBeNull();
  });
});
