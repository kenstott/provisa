// Copyright (c) 2026 Kenneth Stott
// Canary: 12f6f758-717e-4bc7-b8bc-b5883c24467e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A node's panel shows the node's own properties: its key as the server gives it (`_id`) first,
// then every column by its name -- a column named `id` shows its own value, never the drawing's
// internal node id, and no value is made up for a column the row did not carry.

import { describe, it, expect } from "vitest";
import { render } from "../../../test-utils/render";
import { Inspector } from "../Inspector";
import type { GNode } from "../graph-model";

function rows(node: GNode): [string, string][] {
  const { container } = render(
    <Inspector
      selected={{ kind: "node", data: node }}
      colorOverrides={{}}
      onColorChange={() => {}}
      onClose={() => {}}
      width={320}
      onResizeStart={() => {}}
      relationships={[]}
      labelToTableId={{}}
    />,
  );
  return [...container.querySelectorAll(".gf-prop-row")].map((row) => [
    row.querySelector(".gf-prop-key")?.textContent ?? "",
    row.querySelector(".gf-prop-val")?.textContent ?? "",
  ]);
}

describe("a node's properties in its panel", () => {
  it("show a column named id by its own value, never the node's internal id", () => {
    const domain: GNode = {
      id: 4,
      label: "Meta:Domains",
      tableLabel: "Domains",
      properties: { id: "pet-store", description: "Pets" },
    };
    expect(rows(domain)).toEqual([
      ["description", "Pets"],
      ["id", "pet-store"],
    ]);
  });

  it("show the node's key first, as _id", () => {
    const domain: GNode = {
      id: 4,
      label: "Meta:Domains",
      tableLabel: "Domains",
      properties: { id: "pet-store", _id: "pet-store", description: "Pets" },
    };
    expect(rows(domain)[0]).toEqual(["_id", "pet-store"]);
  });

  it("show no row the node does not carry: no <id>, no made-up key value", () => {
    const pet: GNode = { id: 9, label: "Pets", tableLabel: "Pets", properties: { name: "Rex" } };
    expect(rows(pet)).toEqual([["name", "Rex"]]);
  });
});
