// Copyright (c) 2026 Kenneth Stott
// Canary: 6c73a80e-4f19-4b25-9e08-13d5a24bc067
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1342: the UI's language comes from the browser and nowhere else.
 *
 * Three properties carry the requirement, and each fails invisibly: a regional variant must
 * resolve to its base catalog (es-MX is Spanish, not a missing language), an unsupported
 * language must land on English rather than on raw keys, and nothing may be cached — a stored
 * language would outlive the browser setting that chose it, with no UI to correct it.
 *
 * The resolution hierarchy is read directly rather than through a rendered page: it is a table,
 * and a component would only ever show one row of it.
 */

import { describe, it, expect } from "vitest";
import i18n from "../i18n";

const hierarchy = (lng: string): string[] =>
  (
    i18n.services.languageUtils as unknown as {
      toResolveHierarchy: (l: string) => string[];
    }
  ).toResolveHierarchy(lng);

describe("REQ-1342 language selection", () => {
  it("reads the language from the browser and caches nothing", () => {
    const detection = i18n.options.detection as { order?: string[]; caches?: string[] } | undefined;

    expect(detection?.order).toEqual(["navigator"]);
    // An empty cache list is the requirement: a stored language would survive the browser
    // setting that chose it, and there is no per-user override to correct it with.
    expect(detection?.caches).toEqual([]);
  });

  it("falls back to English", () => {
    expect(i18n.options.fallbackLng).toEqual(["en"]);
  });

  it.each([
    ["es-MX", "es"],
    ["es-419", "es"],
    ["fr-CA", "fr"],
    ["de-AT", "de"],
    ["pt-BR", "pt"],
  ])("resolves the regional variant %s through its base catalog %s", (variant, base) => {
    // nonExplicitSupportedLngs is what makes this true; without it es-MX is "unsupported" and a
    // Mexican user reads English while the Spanish catalog sits in the bundle.
    expect(i18n.options.nonExplicitSupportedLngs).toBe(true);
    expect(hierarchy(variant)).toEqual([variant, base, "en"]);
  });

  it("keeps Hong Kong Traditional Chinese ahead of the Simplified base", () => {
    // zh-HK has its own catalog. It must be tried before zh, or a Traditional reader is served
    // Simplified characters.
    expect(hierarchy("zh-HK")[0]).toBe("zh-HK");
    expect(hierarchy("zh-CN")).toEqual(["zh-CN", "zh", "en"]);
  });

  it("serves English for a language with no catalog", () => {
    expect(hierarchy("is-IS")).toEqual(["en"]);
  });

  it("ships every catalog it claims to support", () => {
    const supported = (i18n.options.supportedLngs as string[]).filter((l) => l !== "cimode");
    const shipped = Object.keys(i18n.options.resources ?? {});

    expect(supported.length).toBeGreaterThan(0);
    for (const lng of supported) {
      expect(shipped).toContain(lng);
    }
  });
});
