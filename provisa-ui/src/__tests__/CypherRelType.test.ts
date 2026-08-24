// Copyright (c) 2026 Kenneth Stott
// Canary: 3d7c4a90-6f21-4b58-8e0d-92a1b5c7e441
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1586: every graph-result surface that names an edge in a generated pattern — show-all-edges,
// dragging a table onto the canvas, expanding a node's children or parents — must derive the type
// the way the compiler does. A junction-backed row carries no usable alias, so deriving from the
// alias would emit MATCH ()-[r:]->() and drop the edge from the result entirely.

import { describe, it, expect } from "vitest";
import { cypherRelType, upperSnake } from "../naming";

describe("REQ-1586 cypherRelType", () => {
  it("upper-snakes a nominated discriminator value", () => {
    expect(
      cypherRelType({
        alias: null,
        computedCypherAlias: "edgesFrom",
        viaTableName: "glossary_term_edges",
        viaTypeValue: "kind of",
        viaLabelSource: "column",
      }),
    ).toBe("KIND_OF");
  });

  it("upper-snakes the junction table name when the table is nominated", () => {
    expect(
      cypherRelType({
        alias: null,
        viaTableName: "glossary_term_edges",
        viaTypeValue: null,
        viaLabelSource: "table",
      }),
    ).toBe("GLOSSARY_TERM_EDGES");
  });

  it("upper-snakes the declared alias when a fixed label is nominated", () => {
    expect(
      cypherRelType({
        alias: "relatedTo",
        viaTableName: "glossary_term_edges",
        viaTypeValue: null,
        viaLabelSource: "fixed",
      }),
    ).toBe("RELATED_TO");
  });

  it("names an ordinary FK edge from its alias, then its computed alias", () => {
    expect(cypherRelType({ alias: "owns", computedCypherAlias: "hasOwner" })).toBe("OWNS");
    expect(cypherRelType({ alias: null, computedCypherAlias: "hasOwner" })).toBe("HASOWNER");
  });

  it("returns null rather than a blank type when nothing names the edge", () => {
    expect(cypherRelType({ alias: null, computedCypherAlias: null })).toBeNull();
    expect(
      cypherRelType({
        alias: null,
        viaTableName: "glossary_term_edges",
        viaTypeValue: null,
        viaLabelSource: "column",
      }),
    ).toBeNull();
  });

  it("matches the Python _upper_snake normalization", () => {
    expect(upperSnake("kindOf")).toBe("KIND_OF");
    expect(upperSnake("  part-of  ")).toBe("PART_OF");
    expect(upperSnake("SYNONYM_OF")).toBe("SYNONYM_OF");
  });
});
