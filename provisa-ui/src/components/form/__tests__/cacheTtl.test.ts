// Copyright (c) 2026 Kenneth Stott
// Canary: a4d96746-a7ca-4296-a95a-c328c1fce0b1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2031: a Cache TTL in words, and the hint beneath its editor.

import { describe, expect, it } from "vitest";
import i18n from "../../../i18n";
import { cacheTtlHint, ttlText } from "../cacheTtl";

const t = i18n.getFixedT("en");

describe("a Cache TTL", () => {
  it("is seconds, or never expires for -1", () => {
    expect(ttlText(300, t)).toBe("300s");
    expect(ttlText(-1, t)).toBe("never expires");
  });

  it("says what -1 means once it is chosen, and that it may be chosen before", () => {
    expect(cacheTtlHint("-1", t)).toBe(
      "Never expires and lands once.",
    );
    expect(cacheTtlHint(-1, t)).toContain("Never expires");
    for (const v of ["", null, 60, "0"]) expect(cacheTtlHint(v, t)).toBe("-1: never expires");
  });
});
