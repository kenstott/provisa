// Copyright (c) 2026 Kenneth Stott
// Canary: 6cfbdc69-9807-48bb-9944-2820544c275e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The top navigation group that holds Views, Metrics, Commands and Lineage is labelled
// "Derived": those are the things derived from the registered tables. The i18n key, the group id
// and the routes keep their names.

import { describe, it, expect } from "vitest";
import i18n from "../i18n";
import { NAV_GROUPS } from "../components/navGroups";

describe("the nav group of the derived objects", () => {
  it("is labelled Derived", () => {
    const group = NAV_GROUPS.find((g) => g.items.some((i) => i.to === "/views"))!;
    expect(group.items.map((i) => i.to)).toEqual(["/views", "/metrics", "/commands", "/lineage"]);
    expect(i18n.t(group.labelKey)).toBe("Derived");
  });

  it("does not send a reader to a nav group called Model", () => {
    expect(i18n.t("lineagePage.emptyBody")).toContain("(Derived → Views)");
    expect(i18n.t("lineagePage.emptyBody")).not.toContain("Model →");
  });
});
