// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

async function openAddForm(page: import("@playwright/test").Page) {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  await page.getByRole("button", { name: /\+ Table/i }).click();
  await page.waitForSelector(".form-card", { timeout: 5000 });
}

function sourceSelect(page: import("@playwright/test").Page) {
  return page.locator("label").filter({ hasText: /^Source/ }).locator("select");
}
function domainSelect(page: import("@playwright/test").Page) {
  return page.locator("label").filter({ hasText: /^Domain/ }).locator("select");
}
function schemaSelect(page: import("@playwright/test").Page) {
  return page.locator("label").filter({ hasText: /^Schema/ }).locator("select");
}
function tableSelect(page: import("@playwright/test").Page) {
  return page.locator("label").filter({ hasText: /^Table/ }).locator("select");
}

test("sqlite schema dropdown shows 'main' not PG internal schemas", async ({ page }) => {
  await openAddForm(page);

  await sourceSelect(page).selectOption("inquiries-sqlite");

  // Schema dropdown should populate with exactly "main" (the physical SQLite schema).
  // Regression: was empty because native_schemas returned None → Trino fallback
  // returned internal PG schemas like "pet_store", "analytics".
  await page.waitForFunction(
    () => {
      const sel = document.querySelector<HTMLSelectElement>(
        "label select",
      );
      // find the schema select — third <select> in the form
      const selects = Array.from(document.querySelectorAll<HTMLSelectElement>(".form-card select"));
      const schemaEl = selects[2];
      if (!schemaEl) return false;
      return Array.from(schemaEl.options).some((o) => o.value === "main");
    },
    { timeout: 10000 },
  );

  const schemaOptions = await schemaSelect(page).locator("option").allTextContents();
  expect(schemaOptions).toContain("main");
  // Must not show PG-internal schemas that come from the migration layer
  expect(schemaOptions).not.toContain("analytics");
  expect(schemaOptions).not.toContain("pet_store");
  expect(schemaOptions).not.toContain("public");
});

test("sqlite table dropdown loads tables from physical file", async ({ page }) => {
  await openAddForm(page);

  await sourceSelect(page).selectOption("inquiries-sqlite");
  await domainSelect(page).selectOption("pet-store");

  // Single-schema sources auto-select the schema (select is disabled); wait for it.
  await page.waitForFunction(
    () => {
      const selects = Array.from(document.querySelectorAll<HTMLSelectElement>(".form-card select"));
      return selects[2]?.value === "main";
    },
    { timeout: 10000 },
  );

  // Wait for table dropdown to finish loading
  await page.waitForFunction(
    () => {
      const selects = Array.from(document.querySelectorAll<HTMLSelectElement>(".form-card select"));
      const tableEl = selects[3];
      if (!tableEl) return false;
      const placeholder = tableEl.options[0]?.text ?? "";
      return !placeholder.includes("Loading");
    },
    { timeout: 10000 },
  );

  // Both tables are already registered under pet-store, so the dropdown
  // should either show them as available (different domain) or say "All tables already registered".
  // The key invariant: it must NOT be stuck on "Select table..." with zero items loaded.
  const tableEl = tableSelect(page);
  const placeholder = await tableEl.locator("option").first().textContent();
  expect(placeholder).not.toBe("Select table...");
});

test("openapi registered tables appear disabled in available tables dropdown", async ({ page }) => {
  await openAddForm(page);

  await sourceSelect(page).selectOption("petstore-api");
  await domainSelect(page).selectOption("pet-store");

  // openapi sources auto-fix schema to "openapi"
  await page.waitForFunction(
    () => {
      const selects = Array.from(document.querySelectorAll<HTMLSelectElement>(".form-card select"));
      const schemaEl = selects[2];
      return schemaEl?.value === "openapi";
    },
    { timeout: 10000 },
  );

  // Wait for table options to load
  await page.waitForFunction(
    () => {
      const selects = Array.from(document.querySelectorAll<HTMLSelectElement>(".form-card select"));
      const tableEl = selects[3];
      if (!tableEl) return false;
      const placeholder = tableEl.options[0]?.text ?? "";
      return !placeholder.includes("Loading");
    },
    { timeout: 10000 },
  );

  // Design standard: already-registered tables are visible but disabled (not filtered out).
  // Names are normalized (camelCase→snake_case) before comparing against registered names,
  // so "findPetsByStatus" in the dropdown matches "find_pets_by_status" in registered_tables.
  const tableEl = tableSelect(page);
  const allOptions = await tableEl.locator("option").all();
  const optionData = await Promise.all(
    allOptions.map(async (opt) => ({
      text: await opt.textContent(),
      disabled: await opt.evaluate((el) => (el as HTMLOptionElement).disabled),
    })),
  );

  // Available table names are camelCase operation IDs from the OpenAPI spec.
  // Registered table names in the DB are snake_case (from YAML install).
  // The UI normalizes both to snake_case before comparing.
  const alreadyRegistered = ["findPetsByStatus", "findPetsByTags"];
  for (const name of alreadyRegistered) {
    const opt = optionData.find((o) => o.text?.trim() === name);
    expect(opt, `option "${name}" must be present`).toBeTruthy();
    expect(opt!.disabled, `option "${name}" must be disabled`).toBe(true);
  }
});
