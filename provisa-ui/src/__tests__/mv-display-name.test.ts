// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect } from "vitest";
import { displayMvName } from "../components/admin/mvDisplay";

// Regression: the Materialized Store "View" column showed the internal registry id
// `view-<alias>` (e.g. "view-test") instead of the alias the user typed ("test").
describe("displayMvName", () => {
  it("strips the view- prefix so the user's alias shows", () => {
    expect(displayMvName("view-test")).toBe("test");
  });

  it("only strips a leading prefix, not embedded occurrences", () => {
    expect(displayMvName("view-my-view-test")).toBe("my-view-test");
  });

  it("leaves ids without the prefix unchanged (join/auto MVs)", () => {
    expect(displayMvName("orders_customers_join")).toBe("orders_customers_join");
  });
});
