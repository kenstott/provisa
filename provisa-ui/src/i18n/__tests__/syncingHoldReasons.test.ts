// Copyright (c) 2026 Kenneth Stott
// Canary: 8e4f2a17-6c93-4b05-9d1e-3a7b5c0f2d68
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2247: an Import run records why it held each difference as a reason code; the Syncing page
// words every code from the catalog, in every locale.

import { describe, expect, it } from "vitest";
import { HOLD_REASON_CODES } from "../../api/syncing";
import { loadCatalog, supportedLngs } from "../index";

describe("the Syncing page's hold reasons", () => {
  for (const lng of supportedLngs) {
    it(`${lng}: every hold reason code has a string`, async () => {
      const catalog = await loadCatalog(lng);
      const strings = catalog.syncingTab as Record<string, string>;
      for (const code of HOLD_REASON_CODES) {
        const text = strings[`holdReason_${code}`];
        expect(typeof text, `${lng} holdReason_${code}`).toBe("string");
        expect(text.trim()).not.toBe("");
        expect(text).not.toMatch(/REQ-\d+/);
      }
    });
  }
});
