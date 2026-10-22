// Copyright (c) 2026 Kenneth Stott
// Canary: c0f82601-8735-4eac-9a5b-e558432e90fa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A node's look: its shape says its kind and its colour the variant; a term's status is on its
// label alone.

import { describe, expect, it } from "vitest";
import { labelClass, nodeLook } from "../nodeLook";
import type { GraphNode } from "../graphModel";

const n = (over: Partial<GraphNode> & Pick<GraphNode, "type">): GraphNode => ({
  id: "x",
  label: "x",
  ...over,
});

describe("a node's look", () => {
  it("draws terms as diamonds, concrete and abstract in two colours", () => {
    expect(nodeLook(n({ type: "term" }))).toEqual({
      shape: "diamond",
      colour: "cool",
      variant: "concrete",
    });
    expect(nodeLook(n({ type: "term", abstract: true }))).toEqual({
      shape: "diamond",
      colour: "hl",
      variant: "abstract",
    });
  });

  it("draws columns as cubes, in a data product or not in two colours", () => {
    expect(nodeLook(n({ type: "column", inProduct: true }))).toEqual({
      shape: "cube",
      colour: "fam-numeric",
      variant: "in_product",
    });
    expect(nodeLook(n({ type: "column", inProduct: false }))).toEqual({
      shape: "cube",
      colour: "fam-other",
      variant: "out_of_product",
    });
  });

  it("draws domains as spheres", () => {
    expect(nodeLook(n({ type: "domain" }))).toMatchObject({
      shape: "sphere",
      colour: "fam-temporal",
    });
  });

  it("draws a table as a cylinder in every graph, a product's member or a table a column is in", () => {
    expect(nodeLook(n({ type: "table" }))).toEqual({
      shape: "cylinder",
      colour: "fam-numeric",
      variant: "plain",
    });
    expect(nodeLook(n({ type: "table", stored: true }))).toEqual(nodeLook(n({ type: "table" })));
    expect(nodeLook(n({ type: "relation" }))).toMatchObject({
      shape: "cylinder",
      colour: "fam-foreign",
    });
  });

  it("gives a profile result a shape no other kind has", () => {
    const profile = nodeLook(n({ type: "profile" })).shape;
    expect(profile).toBe("torus");
    const others = (
      [
        "product",
        "domain",
        "table",
        "relation",
        "command",
        "checker",
        "tag",
        "term",
        "column",
      ] as const
    ).map((type) => nodeLook(n({ type })).shape);
    expect(others).not.toContain(profile);
  });

  it("gives every kind one look whatever its status, so no node is see-through or resized", () => {
    for (const status of ["live", "proposed", "deprecated", "retired"] as const)
      expect(nodeLook(n({ type: "term", status }))).toEqual(nodeLook(n({ type: "term" })));
  });
});

describe("a term's status on its label", () => {
  it("is none when live, italic proposed, dimmed deprecated, dimmed and struck retired", () => {
    expect(labelClass("live")).toBe("");
    expect(labelClass(undefined)).toBe("");
    expect(labelClass("proposed")).toBe("g3d-label-proposed");
    expect(labelClass("deprecated")).toBe("g3d-label-deprecated");
    expect(labelClass("retired")).toBe("g3d-label-retired");
  });
});
