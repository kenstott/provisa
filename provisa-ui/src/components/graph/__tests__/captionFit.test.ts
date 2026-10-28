// Copyright (c) 2026 Kenneth Stott
// Canary: 1e7b4c93-5a28-4d06-9f3e-b8a2d6c05e71
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A node's caption stays inside its circle where it fits, at the canvas's normal font; a longer one
// goes beneath the node, two lines at most, ending in an ellipsis; the caption itself (what the hover
// shows in full) is the label property's.

import { describe, expect, it } from "vitest";
import { captionFits, captionLines, nodeCaption } from "../graph-stylesheet";
import type { GNode } from "../graph-model";

describe("a caption's place", () => {
  it("stays inside a circle it fits, at the normal font", () => {
    expect(captionFits("Rex", 44)).toBe(true);
    expect(captionFits("Customer", 44)).toBe(false);
  });

  it("is kept whole beneath the node where it fits a line", () => {
    expect(captionLines("Customer", 68)).toBe("Customer");
  });

  it("wraps beneath the node to two lines, at a space where there is one", () => {
    expect(captionLines("Sir Barksalot Bartholomew", 68)).toBe("Sir\nBarksalot…");
    expect(captionLines("Sir Barksalot", 60)).toBe("Sir\nBarksalot");
  });

  it("ends in an ellipsis where two lines do not hold it", () => {
    const lines = captionLines("pet-store:adopters-and-fosterers-of-small-animals", 68).split("\n");
    expect(lines).toHaveLength(2);
    expect(lines[1].endsWith("…")).toBe(true);
  });

  it("is never empty for a narrow node", () => {
    expect(captionLines("abc def", 1)).not.toBe("");
  });
});

describe("a node's caption", () => {
  const n: GNode = {
    id: 1,
    label: "pets",
    tableLabel: "pets",
    properties: { name: "Rex", species: "dog" },
  };

  it("is its name, or the property the label is set to show, else its id", () => {
    expect(nodeCaption(n, "pets", {})).toBe("Rex");
    expect(nodeCaption(n, "pets", { pets: "species" })).toBe("dog");
    expect(nodeCaption({ ...n, properties: {} }, "pets", {})).toBe("1");
    expect(nodeCaption(undefined, "Group", {})).toBe("Group");
  });
});
