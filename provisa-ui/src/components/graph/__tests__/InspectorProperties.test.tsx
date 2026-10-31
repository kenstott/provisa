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

  it("read a SPARQL column by its name under the node's own class", () => {
    const pet: GNode = {
      id: 4,
      label: "pet-store:pets",
      tableLabel: "pet-store:pets",
      properties: { "pet-store:pets#breedName": "Husky" },
      propertyIris: { "pet-store:pets#breedName": "https://x/pet-store/pets#breedName" },
    };
    expect(rows(pet)).toEqual([["breedName", "Husky"]]);
  });

  it("keep a term, a standard predicate and another class's column as written", () => {
    const pet: GNode = {
      id: 4,
      label: "pet-store:pets",
      tableLabel: "pet-store:pets",
      properties: {
        "term:weight": "4",
        "rdfs:label": "Rex",
        "pet-store:owners#email": "a@b",
      },
      propertyIris: {
        "term:weight": "https://x/term/weight",
        "rdfs:label": "http://www.w3.org/2000/01/rdf-schema#label",
        "pet-store:owners#email": "https://x/pet-store/owners#email",
      },
    };
    expect(rows(pet).map(([k]) => k)).toEqual([
      "pet-store:owners#email",
      "rdfs:label",
      "term:weight",
    ]);
  });

  it("keep the full predicate in the row's title", () => {
    const iri = "https://x/pet-store/pets#breedName";
    const { container } = render(
      <Inspector
        selected={{
          kind: "node",
          data: {
            id: 4,
            label: "pet-store:pets",
            tableLabel: "pet-store:pets",
            properties: { "pet-store:pets#breedName": "Husky" },
            propertyIris: { "pet-store:pets#breedName": iri },
          },
        }}
        colorOverrides={{}}
        onColorChange={() => {}}
        onClose={() => {}}
        width={320}
        onResizeStart={() => {}}
        relationships={[]}
        labelToTableId={{}}
      />,
    );
    expect(container.querySelector(".gf-prop-key")).toHaveAttribute("title", iri);
  });

  it("show a column once where the node is already named by it", () => {
    const pet: GNode = {
      id: 4,
      label: "pet-store:pets",
      tableLabel: "pet-store:pets",
      properties: { name: "Rex", iri: "https://x/id/1", "pet-store:pets#name": "Rex" },
      propertyIris: { "pet-store:pets#name": "https://x/pet-store/pets#name" },
    };
    expect(rows(pet)).toEqual([
      ["iri", "https://x/id/1"],
      ["name", "Rex"],
    ]);
  });

  it("keep a column as written where its short name would read as a different value", () => {
    const pet: GNode = {
      id: 4,
      label: "pet-store:pets",
      tableLabel: "pet-store:pets",
      properties: { iri: "https://x/id/1", "pet-store:pets#iri": "other" },
      propertyIris: { "pet-store:pets#iri": "https://x/pet-store/pets#iri" },
    };
    expect(rows(pet)).toEqual([
      ["iri", "https://x/id/1"],
      ["pet-store:pets#iri", "other"],
    ]);
  });

  it("leave a Cypher node's property alone, even written like a predicate", () => {
    const n: GNode = {
      id: 4,
      label: "Pets",
      tableLabel: "Pets",
      properties: { "Pets#breed": "Husky" },
    };
    expect(rows(n)).toEqual([["Pets#breed", "Husky"]]);
  });

  it("say a literal's datatype on hover of its value", () => {
    const { container } = render(
      <Inspector
        selected={{
          kind: "node",
          data: {
            id: 4,
            label: "pet-store:pets",
            tableLabel: "pet-store:pets",
            properties: { "pet-store:pets#price": "3.25" },
            propertyIris: { "pet-store:pets#price": "https://x/pet-store/pets#price" },
            propertyDatatypes: {
              "pet-store:pets#price": "http://www.w3.org/2001/XMLSchema#decimal",
            },
          },
        }}
        colorOverrides={{}}
        onColorChange={() => {}}
        onClose={() => {}}
        width={320}
        onResizeStart={() => {}}
        relationships={[]}
        labelToTableId={{}}
      />,
    );
    expect(container.querySelector(".gf-prop-val")).toHaveAttribute(
      "title",
      "http://www.w3.org/2001/XMLSchema#decimal",
    );
  });
});
