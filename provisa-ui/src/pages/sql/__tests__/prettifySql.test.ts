// Copyright (c) 2026 Kenneth Stott
// Canary: 29ef277b-c532-44bd-aed4-34cc3bd87fd6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect } from "vitest";
import { prettifySql } from "../prettifySql";

describe("prettifySql", () => {
  it("formats with upper-case keywords and a two-space indent", () => {
    expect(prettifySql("select a, b from t where a = 1")).toBe(
      "SELECT\n  a,\n  b\nFROM\n  t\nWHERE\n  a = 1",
    );
  });

  it("keeps a leading provisa-params comment line intact above the formatted statement", () => {
    const params = "-- provisa-params: $1=5 $2='x'";
    expect(prettifySql(`${params}\nselect a from t where a = $1`)).toBe(
      `${params}\nSELECT\n  a\nFROM\n  t\nWHERE\n  a = $1`,
    );
  });

  it("returns a params-only comment unchanged", () => {
    expect(prettifySql("-- provisa-params: $1=5")).toBe("-- provisa-params: $1=5");
  });
});
