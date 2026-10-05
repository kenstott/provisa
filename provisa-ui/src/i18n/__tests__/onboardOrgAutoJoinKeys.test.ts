// Copyright (c) 2026 Kenneth Stott
// Canary: 6932deef-31e7-457c-b5ff-09c88b7396e4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1568: the auto-join choice card on OnboardOrgPage reads its strings as onboardOrg.<key>. The
// keys once sat beside the "onboardOrg" object instead of inside it in every locale, so the card
// rendered raw key names. Each locale's catalog must resolve every one of them under onboardOrg.

import { describe, expect, it } from "vitest";
import { resources } from "../index";

const AUTO_JOIN_CARD_KEYS = [
  "autoJoinChoiceTitle",
  "autoJoinChoiceBody",
  "joinOfferedOrg",
  "declineAutoJoin",
];

describe("auto-join choice card strings", () => {
  for (const [lng, { translation }] of Object.entries(resources)) {
    it(`${lng}: every key resolves under onboardOrg, none stray at the top level`, () => {
      const catalog = translation as Record<string, unknown>;
      const onboardOrg = catalog.onboardOrg as Record<string, unknown>;
      for (const key of AUTO_JOIN_CARD_KEYS) {
        expect(typeof onboardOrg[key], `${lng} onboardOrg.${key}`).toBe("string");
        expect((onboardOrg[key] as string).length, `${lng} onboardOrg.${key}`).toBeGreaterThan(0);
        expect(catalog[key], `${lng} top-level ${key}`).toBeUndefined();
      }
    });
  }
});
