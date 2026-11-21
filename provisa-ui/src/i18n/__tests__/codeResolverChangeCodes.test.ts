// Copyright (c) 2026 Kenneth Stott
// Canary: 5b0c3f4e-8a1d-4c2f-9b7e-6d1a2c3e4f50
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2249: each refusal of a code resolver's incremental setting and of its @change refresh is
// worded by the catalog, in every locale, and takes what the server names.

import { describe, expect, it } from "vitest";
import { i18nReady, loadCatalog, supportedLngs } from "../index";
import { serverMessage } from "../serverMessage";

const REFUSALS: Record<string, Record<string, string>> = {
  "schema.incremental_no_change": { table: "store_regions" },
  "schema.incremental_needs_rebuild_every": { table: "store_regions" },
  "mv.change_key_missing": { function: "store_regions_changes", column: "store_id" },
  "mv.change_key_repeated": { function: "store_regions_changes", key: "[7, 3]" },
  "mv.change_cursor_backwards": {
    function: "store_regions_changes",
    returned: "41",
    stored: "58",
  },
};

describe("the refusals of a code resolver's incremental setting", () => {
  for (const [code, params] of Object.entries(REFUSALS)) {
    it(`${code} is worded by the catalog, not by the server's text`, async () => {
      await i18nReady;
      const text = serverMessage({ code, params, message: "server english text" }, "fallback");
      expect(text).not.toBe("server english text");
      expect(text).not.toBe("fallback");
      for (const value of Object.values(params)) expect(text).toContain(value);
    });

    for (const lng of supportedLngs) {
      it(`${lng}: ${code} exists and takes its params`, async () => {
        const catalog = await loadCatalog(lng);
        const [group, name] = code.split(".");
        const text = (catalog.serverErrors as Record<string, Record<string, string>>)[group][name];
        expect(typeof text).toBe("string");
        for (const param of Object.keys(params)) expect(text).toContain(`{{${param}}}`);
        expect(text).not.toMatch(/REQ-\d+/);
      });
    }
  }
});
