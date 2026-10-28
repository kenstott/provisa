// Copyright (c) 2026 Kenneth Stott
// Canary: 1e7b4c93-5a28-4d06-9f3e-b8a2d6c05e71
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A node's caption longer than its circle is drawn at a smaller font, down to a minimum, before
// it is truncated; the caption itself (what the hover shows in full) is the label property's.

import { describe, expect, it } from "vitest";
import { CAPTION_FONT_MIN, fitFont, nodeCaption } from "../graph-stylesheet";
import type { GNode } from "../graph-model";

describe("a caption's font", () => {
  it("stays at the base size where the caption fits its circle", () => {
    expect(fitFont("Rex", 36, 10)).toBe(10);
  });

  it("scales down in proportion where it does not, never below the minimum", () => {
    const scaled = fitFont("Customer", 36, 10);
    expect(scaled).toBeLessThan(10);
    expect(scaled).toBeGreaterThan(CAPTION_FONT_MIN);
    expect(fitFont("a very long caption indeed here", 36, 10)).toBe(CAPTION_FONT_MIN);
  });

  it("is the base size for an empty caption", () => {
    expect(fitFont("", 36, 10)).toBe(10);
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
