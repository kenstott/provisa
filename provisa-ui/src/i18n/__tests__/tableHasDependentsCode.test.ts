// Copyright (c) 2026 Kenneth Stott
// Canary: 4f8c2b19-7a6d-4e35-b0c1-e2d93a75f608
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1986 (amended 2026-11-23): a table that a derived table reads is refused by name. The
// refusal's code is worded by the catalog, in every locale, and names the table.

import { describe, expect, it } from "vitest";
import { i18nReady, loadCatalog, supportedLngs } from "../index";
import { serverMessage } from "../serverMessage";

const CODE = "schema.table_has_dependents";
const PARAMS = { table: 7, name: "stores_1" };

describe("the refusal of a table something still reads", () => {
  it("is worded by the catalog and names the table", async () => {
    await i18nReady;
    const text = serverMessage(
      { code: CODE, params: PARAMS, message: "server english text" },
      "fallback",
    );
    expect(text).toBe(
      "Table 'stores_1' is still read or referred to by other objects: retire or change them first",
    );
  });

  for (const lng of supportedLngs) {
    it(`${lng}: ${CODE} exists and names the table`, async () => {
      const catalog = await loadCatalog(lng);
      const text = (catalog.serverErrors as Record<string, Record<string, string>>).schema
        .table_has_dependents;
      expect(typeof text).toBe("string");
      expect(text).toContain("{{name}}");
    });
  }
});
