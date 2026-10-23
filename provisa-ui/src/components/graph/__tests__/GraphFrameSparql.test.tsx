// Copyright (c) 2026 Kenneth Stott
// Canary: 875e3813-1d66-4ca9-b39f-73c81517f3f9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: a SPARQL frame is a SPARQL query's frame. Its header reads as SPARQL, and the action
// that rewrites a Cypher query (auto-impute) is not offered on it.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { GraphFrame } from "../GraphFrame";

// The canvas draws into a 2D context jsdom does not have; what is asked of here is the header.
vi.mock("../GraphCanvas", () => ({ GraphCanvas: () => null, GraphCutNote: () => null }));
import type { FrameData, GNode } from "../graph-model";
import type { GraphLanguage } from "../graph-persistence";

const ORDER: GNode = { id: 1, label: "sales:orders", tableLabel: "orders", properties: {} };

function frame(query: string): FrameData {
  return {
    id: "f1",
    query,
    status: "done",
    nodes: new Map([["1", ORDER]]),
    edges: new Map(),
    rows: [],
    columns: [],
  };
}

function show(query: string, language?: GraphLanguage) {
  render(
    <GraphFrame
      frame={frame(query)}
      language={language}
      onClose={() => {}}
      onRerun={() => {}}
      schemaRels={[]}
      colorOverrides={{}}
      sizeOverrides={{}}
      labelProperty={{}}
      sizeByProperty={{}}
      sizeMultiplier={{}}
      relLineOverrides={{}}
      onColorChange={() => {}}
      pkMap={{}}
      labelToTableLabel={{}}
      labelToTableId={{}}
    />,
  );
}

describe("a SPARQL frame", () => {
  it("offers no auto-impute, which rewrites a Cypher query", () => {
    show("CONSTRUCT { ?s a ?c } WHERE { ?s a ?c }", "sparql");
    expect(screen.queryByTestId("graph-frame-auto-impute-btn")).toBeNull();
  });

  it("is a Cypher frame's offer still", () => {
    show("MATCH (n) RETURN n");
    expect(screen.getByTestId("graph-frame-auto-impute-btn")).toBeInTheDocument();
  });
});
