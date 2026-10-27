// Copyright (c) 2026 Kenneth Stott
// Canary: a7c5df0a-9255-46f0-88a0-d03d22d2a523
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A lineage relation as a reader is shown it: a registered table by its catalog name, a relation
// that is no registered table (REQ-1626, REQ-2001) by its relation name.

import { describe, expect, it } from "vitest";
import { catalogTableName, lineageRelationLabel, relationName } from "../naming";

const pets = { domainId: "pet-store", tableName: "pets" };
const catalog = new Map([[relationName(pets), catalogTableName(pets)]]);

describe("a lineage relation's name", () => {
  it("is the catalog's name for a registered table", () => {
    expect(lineageRelationLabel("pet_store.pets", catalog)).toBe("pet-store.pets");
  });

  it("is the relation's own name for one that is no registered table", () => {
    expect(lineageRelationLabel("staging.raw_orders", catalog)).toBe("staging.raw_orders");
  });
});
