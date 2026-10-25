// Copyright (c) 2026 Kenneth Stott
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
import { frameMetaText } from "../frame/frame-meta";
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
