// Copyright (c) 2026 Kenneth Stott
// Canary: 19b543d4-1ec2-4c2c-8fc2-2f8c3a4aa9f9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A graph frame's header counts what it shows: nodes and relationships for a drawing, rows for a
// table answer (a Cypher or SPARQL read of values only), never "0 nodes · 0 rels" over rows.

import { describe, expect, it } from "vitest";
import i18n from "../../../i18n";
import { buildClusterElements } from "../graph-clusters";
import { frameMetaText, groupOptions } from "../frame/frame-meta";
import type { FrameData, GNode } from "../graph-model";

const t = i18n.getFixedT("en");

function frame(over: Partial<FrameData>): FrameData {
  return {
    id: "f",
    query: "q",
    status: "done",
    nodes: new Map(),
    edges: new Map(),
    rows: [],
    columns: [],
    ...over,
  };
}

const node: GNode = { id: 1, label: "Person", tableLabel: "Person", properties: {} };

describe("a frame's header", () => {
  it("counts the nodes and relationships of a drawing", () => {
    const drawn = frame({ nodes: new Map([["Person:1", node]]), rows: [{ n: 1 }], elapsed: 5 });
    expect(frameMetaText(drawn, t)).toBe("1 nodes · 0 rels · 5ms");
  });

  it("counts the rows of a table answer", () => {
    const table = frame({ rows: [{ n: "Ann" }, { n: "Bo" }, { n: "Cy" }], elapsed: 44 });
    expect(frameMetaText(table, t)).toBe("3 rows · 44ms");
    expect(frameMetaText(frame({ rows: [{ n: "Ann" }] }), t)).toBe("1 row");
  });

  it("counts nothing as no nodes when nothing came back", () => {
    expect(frameMetaText(frame({}), t)).toBe("0 nodes · 0 rels");
  });
});

describe("what a frame can be grouped by", () => {
  const node = (
    id: number,
    label: string,
    properties: Record<string, unknown>,
    propertyIris?: Record<string, string>,
  ): GNode => ({ id, label, tableLabel: label, properties, propertyIris });
  const nodes = (...list: GNode[]) => new Map(list.map((n) => [`${n.label}:${n.id}`, n]));
  const names = (options: { value: string; label: string }[]) => options.map((o) => o.label);

  it("is every property of every node, alphabetically, on Cypher", () => {
    const options = groupOptions(
      nodes(
        node(1, "Pets", { breed: "Husky", name: "Rex" }),
        node(2, "Pets", { breed: "Husky", name: "Bo" }),
        node(3, "Vets", { Clinic: "North" }),
      ),
    );
    expect(names(options)).toEqual(["breed", "Clinic", "domain", "name"]);
  });

  it("names a SPARQL property as the panel does", () => {
    const iris = { "pets:age": "https://x/sales/pets#age" };
    const options = groupOptions(nodes(node(1, "sales:pets", { "pets:age": "4" }, iris)));
    expect(options).toContainEqual({ value: "pets:age", label: "age" });
  });

  it("keeps two classes' columns of one name apart", () => {
    const options = groupOptions(
      nodes(
        node(1, "sales:pets", { "pets:age": "4" }, { "pets:age": "https://x/sales/pets#age" }),
        node(2, "sales:vets", { "vets:age": "40" }, { "vets:age": "https://x/sales/vets#age" }),
      ),
    );
    expect(names(options)).toEqual(expect.arrayContaining(["pets:age", "vets:age"]));
    expect(names(options)).not.toContain("age");
  });

  it("offers what a class has before its nodes were read", () => {
    const options = groupOptions(nodes(node(1, "sales:pets", { name: "x" }, {})), {
      "sales:pets": { properties: ["pets:age"], propertyNames: { "pets:age": "age" } },
      "sales:vets": { properties: ["vets:rank"], propertyNames: { "vets:rank": "rank" } },
    });
    expect(options).toContainEqual({ value: "pets:age", label: "age" });
    expect(names(options)).not.toContain("rank");
  });

  it("puts the nodes that lack the property in a group of their own", () => {
    const els = buildClusterElements(
      nodes(node(1, "Pets", { breed: "Husky" }), node(2, "Vets", {}), node(3, "Vets", {})),
      new Map(),
      "breed",
    );
    const groups = els.filter((e) => e.data._cluster).map((e) => e.data.label);
    expect(groups).toHaveLength(2);
    const children = els.filter((e) => e.data._node);
    expect(children.every((e) => e.data.parent)).toBe(true);
  });

  it("leaves a node out of a schema level it has no value for, as before", () => {
    const els = buildClusterElements(
      nodes(node(1, "Pets", { scl1: 1 }), node(2, "Vets", {})),
      new Map(),
      "schema_L1",
    );
    expect(els.filter((e) => e.data._cluster)).toHaveLength(1);
  });
});
