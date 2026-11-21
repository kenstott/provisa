// Copyright (c) 2026 Kenneth Stott
// Canary: 8a41c7d2-6e03-4b95-9f1a-2c7d5e8b0a36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Saving a relationship whose id begins with a prefix kept for built-in relationships is refused
// with the code schema.relationship_id_reserved (params id, prefix). The code must resolve to the
// reader's wording, in every locale, and take both params.

import { describe, expect, it } from "vitest";
import i18n, { i18nReady, loadCatalog, supportedLngs } from "../index";
import { serverMessage } from "../serverMessage";

const body = {
  code: "schema.relationship_id_reserved",
  params: { id: "meta:mine", prefix: "meta:" },
  message: "server english text",
};

describe("schema.relationship_id_reserved", () => {
  it("resolves to the catalog message, not the server's raw text", async () => {
    await i18nReady;
    const text = serverMessage(body, "fallback");
    expect(text).not.toBe("server english text");
    expect(text).toContain("meta:mine");
    expect(text).toContain("built-in relationships");
    expect(text).toContain("another id");
    expect(i18n.exists("serverErrors.schema.relationship_id_reserved")).toBe(true);
  });

  for (const lng of supportedLngs) {
    it(`${lng}: the message exists and takes id and prefix`, async () => {
      const catalog = await loadCatalog(lng);
      const schema = (catalog.serverErrors as { schema: Record<string, string> }).schema;
      const text = schema.relationship_id_reserved;
      expect(typeof text, lng).toBe("string");
      expect(text, lng).toContain("{{id}}");
      expect(text, lng).toContain("{{prefix}}");
      expect(text, lng).not.toMatch(/REQ-/);
    });
  }
});
