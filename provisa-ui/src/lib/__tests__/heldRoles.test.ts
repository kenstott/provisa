// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2155: a role held through an ancestor is held. The acting roles and every role they inherit
// from, as the server's role chains resolve it.

import { describe, expect, it } from "vitest";
import { heldRoles } from "../roles";

const role = (id: string, parents: string[] = []) => ({ id, parents });

describe("the roles a caller holds", () => {
  it("are its acting roles and every ancestor, through roles it can see and ones it cannot", () => {
    const known = [
      role("shop_analyst", ["analyst"]),
      role("analyst", ["viewer"]),
      role("viewer"),
      role("steward", ["data_product_owner"]),
    ];
    // data_product_owner is named as a parent but not itself listed: still held.
    expect([...heldRoles([known[0], known[3]], known)].sort()).toEqual([
      "analyst",
      "data_product_owner",
      "shop_analyst",
      "steward",
      "viewer",
    ]);
  });

  it("stops on a cycle rather than looping", () => {
    const known = [role("a", ["b"]), role("b", ["a"])];
    expect([...heldRoles([known[0]], known)].sort()).toEqual(["a", "b"]);
  });
});
