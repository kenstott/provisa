// Copyright (c) 2026 Kenneth Stott
// Canary: 1c7e9b34-4a52-4d6f-8e0b-3d9f5a2c7b18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, expect, it } from "vitest";
import { columnDescriber } from "../column-descriptions";

describe("columnDescriber", () => {
  it("resolves a field's description by the graph's relation name and physical column", () => {
    const describe = columnDescriber([
      {
        domainId: "pet-store",
        tableName: "pets",
        columns: [
          { columnName: "price", description: "List price in USD" },
          { columnName: "name", description: "" },
        ],
      } as never,
    ]);
    expect(describe("pet_store.pets", "price")).toBe("List price in USD");
    expect(describe("pet_store.pets", "name")).toBeNull(); // blank is no description
    expect(describe("pet_store.vets", "price")).toBeNull();
  });
});
