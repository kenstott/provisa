// Copyright (c) 2026 Kenneth Stott
// Canary: 4b1c33df-756a-4e92-a49d-126eb7fd091a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A relationship's panel shows every value of the relationship, under its edit form's labels, an
// empty one as "—". A junction-backed edge shows the Cypher type a pattern matches, as its list
// row does, not the alias the server would derive for a plain edge.

import { describe, it, expect, vi } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import type { Relationship } from "../../../types/admin";
import { RelationshipDetail } from "../RelationshipDetail";

const base: Relationship = {
  id: 7,
  sourceTableId: 1,
  targetTableId: 2,
  sourceTableName: "pets",
  sourceDomainId: "zoo",
  targetTableName: "pets",
  sourceColumn: "id",
  targetColumn: "id",
  cardinality: "one-to-many",
  materialize: true,
  refreshInterval: 600,
  targetFunctionName: null,
  functionArg: null,
  alias: null,
  graphqlAlias: "bondedPairs",
  physicalName: "bonded_pairs",
  computedCypherAlias: "HAS_BONDED_PAIRS",
  autoSuggested: false,
  disableCypher: false,
  sameEntity: false,
  parameters: [],
  viaTableId: 9,
  viaTableName: "pet_relations",
  viaSourceColumn: "pet_id",
  viaTargetColumn: "other_pet_id",
  viaTypeColumn: "relation_type",
  viaTypeValue: "bonded pair",
  viaLabelSource: "column",
  // The type the server serves the junction edge as (its nomination, upper-snaked).
  cypherType: "BONDED_PAIR",
};

function show(rel: Relationship, domainsEnabled = true) {
  render(
    <RelationshipDetail
      rel={rel}
      editingRel={null}
      setEditingRel={vi.fn()}
      canManage={false}
      onStartEdit={vi.fn()}
      onReverse={vi.fn()}
      onDelete={vi.fn()}
      onEditSave={vi.fn()}
      saving={null}
      problem=""
      tables={[]}
      functions={[]}
      tableDomainById={{ "2": "zoo" }}
      domainsEnabled={domainsEnabled}
    />,
  );
}

/** A group's fields, label to value, in the order shown. */
function fields(testId: string): [string, string][] {
  const list = screen.getByTestId(testId);
  const labels = within(list).queryAllByRole("term");
  return labels.map((dt) => [dt.textContent ?? "", dt.nextElementSibling?.textContent ?? ""]);
}

describe("a relationship's read view", () => {
  it("shows every value of a junction-backed edge, under the edit form's labels", () => {
    show(base);
    expect(fields("relationship-read-names")).toEqual([
      // The type a Cypher pattern matches: the discriminator value it is nominated by.
      ["CQL Alias (UPPER_SNAKE)", "BONDED_PAIR"],
      ["GQL Alias (camelCase)", "bondedPairs"],
      ["SQL Name", "bonded_pairs"],
      ["Auto-tracked", "No"],
    ]);
    expect(fields("relationship-read-source")).toEqual([
      ["Domain", "zoo"],
      ["Table", "pets"],
      ["Column", "id"],
    ]);
    expect(fields("relationship-read-target")).toEqual([
      ["Type", "Table"],
      ["Domain", "zoo"],
      ["Table", "pets"],
      ["Column", "id"],
      ["Cardinality", "one-to-many"],
      ["Materialize", "Yes"],
      ["Exclude from Cypher", "No"],
      ["Same entity", "No"],
      ["Refresh Interval (s)", "600"],
    ]);
    expect(fields("relationship-read-junction")).toEqual([
      ["Junction table", "pet_relations"],
      ["Junction column → source", "pet_id"],
      ["Junction column → target", "other_pet_id"],
      ["Discriminator column", "relation_type"],
      ["Discriminator value", "bonded pair"],
      ["Name the edge from", "Discriminator value"],
    ]);
    expect(screen.queryByText("HAS_BONDED_PAIRS")).toBeNull();
  });

  it("shows an empty value as a dash, and never leaves a field out", () => {
    show({
      ...base,
      graphqlAlias: null,
      physicalName: null,
      materialize: false,
      viaTableId: null,
      viaTableName: null,
      viaSourceColumn: null,
      viaTargetColumn: null,
      viaTypeColumn: null,
      viaTypeValue: null,
      viaLabelSource: null,
    });
    const names = Object.fromEntries(fields("relationship-read-names"));
    // A plain edge: no alias of its own, so the one the server derives, marked as derived.
    expect(names["CQL Alias (UPPER_SNAKE)"]).toBe("HAS_BONDED_PAIRS");
    expect(screen.getByText("HAS_BONDED_PAIRS")).toHaveStyle({ fontStyle: "italic" });
    expect(names["GQL Alias (camelCase)"]).toBe("—");
    expect(names["SQL Name"]).toBe("—");
    expect(Object.fromEntries(fields("relationship-read-target"))["Refresh Interval (s)"]).toBe(
      "—",
    );
    for (const [, value] of fields("relationship-read-junction")) expect(value).toBe("—");
  });

  it("lists the parameters the edge feeds, each with its source column (REQ-2099)", () => {
    show({
      ...base,
      parameters: [
        { parameter: "customer_id", sourceColumn: "id" },
        { parameter: "plan", sourceColumn: "plan" },
      ],
    });
    expect(fields("relationship-read-parameters")).toEqual([
      ["customer_id", "id"],
      ["plan", "plan"],
    ]);
  });

  it("shows no parameters section for an edge that feeds none", () => {
    show(base);
    expect(screen.queryByTestId("relationship-read-parameters")).toBeNull();
  });

  it("shows a computed edge's function and argument as its target", () => {
    show({
      ...base,
      targetTableId: null,
      targetTableName: "",
      targetColumn: null,
      targetFunctionName: "pet_age",
      functionArg: "pet_id",
      viaTableName: null,
    });
    expect(fields("relationship-read-target").slice(0, 3)).toEqual([
      ["Type", "Function (computed)"],
      ["Function", "pet_age"],
      ["Function Arg (receives source column)", "pet_id"],
    ]);
    // A computed edge has no junction in its form, nor in its read view.
    expect(screen.queryByTestId("relationship-read-junction")).toBeNull();
  });

  it("leaves out the domain fields where domains are off, as the form does", () => {
    show(base, false);
    expect(fields("relationship-read-source").map(([label]) => label)).toEqual(["Table", "Column"]);
  });
});
