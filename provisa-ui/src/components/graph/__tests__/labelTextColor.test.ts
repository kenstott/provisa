// Copyright (c) 2026 Kenneth Stott
// Canary: 7d1c3a52-6b0e-4f0a-9c55-2e8d4a1b9f63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A pill with a label colour draws its text in whichever of black or white contrasts more with that
// ground: one rule for every colour, in either theme, never a per-colour exception.

import { describe, it, expect } from "vitest";
import { PALETTE, labelTextColor, contrastRatio } from "../graph-model";

describe("labelTextColor", () => {
  it("is white on a dark ground and black on a light one", () => {
    expect(labelTextColor("#1a1d27")).toBe("#fff");
    expect(labelTextColor("#FFC454")).toBe("#000");
  });

  it("gives the better contrast of the two on every palette colour", () => {
    for (const bg of PALETTE) {
      const chosen = labelTextColor(bg);
      const other = chosen === "#fff" ? "#000" : "#fff";
      expect(contrastRatio(bg, chosen)).toBeGreaterThanOrEqual(contrastRatio(bg, other));
      expect(contrastRatio(bg, chosen)).toBeGreaterThanOrEqual(4.5);
    }
  });
});
