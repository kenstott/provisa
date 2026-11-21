// Copyright (c) 2026 Kenneth Stott
// Canary: 7d1c5e0a-3b84-4f6e-9a52-c8e41b0f7d23
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A list page's own "+ {object}" button is the filled primary (Mantine's default Button, or
// variant "filled"; "outline" while its form is open to close it). Any further add beside it, on a
// page that offers several, is variant "default". The Views page is the Tables page in viewsOnly
// mode, so there "+ View" is the page's own add and must be filled; beside "+ Table" it is not.

import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";

const SRC = resolve(__dirname, "../..");

/** Each list page's own add button: the file and the button's data-testid. */
const OWN_ADD: ReadonlyArray<[string, string]> = [
  ["pages/TablesPage.tsx", "tables-add-toggle"],
  ["pages/TablesPage.tsx", "views-add-toggle"],
  ["pages/RelationshipsPage.tsx", "rels-add-toggle"],
  ["pages/CommandsPage.tsx", "commands-toggle-form"],
  ["pages/SourcesPage.tsx", "sources-add-toggle"],
  ["pages/MetricsPage.tsx", "metrics-new-button"],
  ["pages/DataProductsPage.tsx", "data-products-new-button"],
  ["pages/SecurityPage.tsx", "toggle-role-form"],
  ["pages/security/SecurityRlsPage.tsx", "toggle-rule-form"],
  ["pages/library/LibraryPage.tsx", "reflib-add"],
  ["components/admin/GlossaryTab.tsx", "glossary-new-btn"],
  ["components/admin/TagsTab.tsx", "tags-create-button"],
  ["components/admin/SecretsTab.tsx", "secrets-add"],
  ["components/admin/LocalUsersTab.tsx", "local-user-add"],
  ["components/admin/OrgsTab.tsx", "org-create-toggle"],
  ["components/admin/PlatformRegionsTab.tsx", "platform-regions-add"],
  ["components/admin/ContainerRegistriesTab.tsx", "container-registries-add"],
  ["components/admin/ReportsTab.tsx", "add-report-btn"],
];

/** The opening tag of the <Button> that carries the test id. */
function buttonTag(source: string, testId: string): string {
  const at = source.indexOf(`data-testid="${testId}"`);
  if (at < 0) return "";
  const start = source.lastIndexOf("<Button", at);
  let depth = 0;
  for (let i = start; i < source.length; i++) {
    const c = source[i];
    if (c === "{") depth++;
    else if (c === "}") depth--;
    else if (c === ">" && depth === 0 && source[i - 1] !== "=") return source.slice(start, i + 1);
  }
  return "";
}

/** The variant attribute's value, or null when the button takes the default (filled). */
function variantOf(tag: string): string | null {
  const m = tag.match(/\bvariant=(?:"([^"]*)"|\{([^}]*(?:\{[^}]*\}[^}]*)*)\})/);
  return m ? (m[1] ?? m[2]) : null;
}

/** Filled when the form is shut: nothing, "filled", or a ternary whose shut branch is filled. */
function isFilledPrimary(variant: string | null): boolean {
  if (variant === null || variant === "filled") return true;
  return /\?\s*"outline"\s*:\s*"filled"/.test(variant);
}

describe("the add-button rule fails what it is meant to fail", () => {
  it("reads a default, light or subtle add as not filled, and no variant as filled", () => {
    expect(isFilledPrimary(variantOf(`<Button variant="default" data-testid="x">`))).toBe(false);
    expect(isFilledPrimary(variantOf(`<Button variant="light" data-testid="x">`))).toBe(false);
    expect(isFilledPrimary(variantOf(`<Button data-testid="x">`))).toBe(true);
    expect(isFilledPrimary(variantOf(`<Button variant="filled">`))).toBe(true);
    expect(isFilledPrimary(variantOf(`<Button variant={s ? "outline" : "filled"}>`))).toBe(true);
    expect(isFilledPrimary(variantOf(`<Button variant={viewsOnly ? "filled" : "default"}>`))).toBe(
      false,
    );
  });
});

describe("a list page's own add button is the filled primary", () => {
  for (const [file, testId] of OWN_ADD) {
    if (testId === "views-add-toggle") continue;
    it(`${file}: ${testId}`, () => {
      const tag = buttonTag(readFileSync(join(SRC, file), "utf8"), testId);
      expect(tag, `no <Button data-testid="${testId}"> in ${file}`).not.toBe("");
      expect(isFilledPrimary(variantOf(tag)), `${testId} is ${variantOf(tag)}`).toBe(true);
    });
  }
});

describe("the Tables page's two adds", () => {
  const source = readFileSync(join(SRC, "pages/TablesPage.tsx"), "utf8");

  it("+ View is filled on the Views page (viewsOnly) and secondary beside + Table", () => {
    const v = variantOf(buttonTag(source, "views-add-toggle"));
    expect(v).toMatch(/viewsOnly\s*\?\s*"filled"\s*:\s*"default"/);
  });

  it("+ Model, beside + Table, is variant default", () => {
    expect(variantOf(buttonTag(source, "tables-model-toggle"))).toBe("default");
  });
});
