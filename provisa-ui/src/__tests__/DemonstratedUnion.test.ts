// Copyright (c) 2026 Kenneth Stott
// Canary: 9c2e40b7-51fa-4d68-8e3b-72c4a1d90f6e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1602: which rights a SET of acting roles is shown rather than given. The sandbox role
// (REQ-1597) withholds six rights and demonstrates them; acting as several roles at once has to
// resolve what one withholds and another grants, and the answer is that a held right is used, never
// explained.

import { describe, it, expect } from "vitest";
import { unionDemonstrated } from "../lib/capabilities";
import type { Capability } from "../types/auth";

const role = (demonstrated: Capability[]) => ({ demonstrated });

describe("unionDemonstrated (REQ-1602)", () => {
  it("unions what the acting roles withhold", () => {
    expect(
      unionDemonstrated([role(["environment_management"]), role(["user_management"])], []).sort(),
    ).toEqual(["environment_management", "user_management"]);
  });

  it("says nothing about a right the same set actually holds", () => {
    expect(
      unionDemonstrated([role(["environment_management"]), role([])], ["environment_management"]),
    ).toEqual([]);
  });

  it("is empty for roles that demonstrate nothing", () => {
    expect(unionDemonstrated([role([]), role([])], ["org_settings"])).toEqual([]);
  });

  it("names each right once however many roles withhold it", () => {
    expect(unionDemonstrated([role(["observability"]), role(["observability"])], [])).toEqual([
      "observability",
    ]);
  });
});
