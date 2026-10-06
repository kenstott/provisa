// Copyright (c) 2026 Kenneth Stott
// Canary: 7c65b316-6e64-4d29-8077-1e73c0e95ff1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1494: the column dialog writes the picked kind or method and its arguments as the
// declaration the server reads.

import { describe, expect, it } from "vitest";
import type { FakeCatalog } from "../../../../api/fakes";
import { chosen, compose } from "../declaration";

const CATALOG: FakeCatalog = {
  kinds: [
    {
      category: "values",
      name: "categories",
      positional: true,
      ruleOnly: false,
      args: [
        { name: "values", kind: "list", required: false },
        { name: "shares", kind: "list", required: false },
      ],
    },
    {
      category: "distribution",
      name: "normal",
      positional: false,
      ruleOnly: false,
      args: [
        { name: "mean", kind: "point", required: true },
        { name: "sd", kind: "interval", required: true },
        { name: "min", kind: "point", required: false },
      ],
    },
  ],
  methods: [
    {
      name: "pyint",
      category: "python",
      params: [
        { name: "min_value", required: false, default: "0" },
        { name: "max_value", required: false, default: "9999" },
      ],
      stable: false,
    },
  ],
};

describe("declaration", () => {
  it("writes a positional kind, a named kind and a method with the arguments given", () => {
    expect(compose(CATALOG, { type: "kind", name: "categories" }, { values: "(a, b)" })).toBe(
      "categories((a, b))",
    );
    expect(
      compose(CATALOG, { type: "kind", name: "normal" }, { mean: "5", sd: "1", min: " " }),
    ).toBe("normal(mean=5, sd=1)");
    expect(compose(CATALOG, { type: "method", name: "pyint" }, { max_value: "9" })).toBe(
      "pyint(max_value=9)",
    );
  });

  it("reads which kind or method a declaration names", () => {
    expect(chosen(CATALOG, "normal(mean=5, sd=1)")).toEqual({ type: "kind", name: "normal" });
    expect(chosen(CATALOG, " pyint()")).toEqual({ type: "method", name: "pyint" });
    expect(chosen(CATALOG, "nonesuch()")).toBeNull();
    expect(chosen(CATALOG, "")).toBeNull();
  });
});
