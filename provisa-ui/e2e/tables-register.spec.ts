// Copyright (c) 2026 Kenneth Stott
// Canary: 0f990681-4473-4b6c-af76-1580aa274e78
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";
import type { Page } from "./coverage";
import { schemasLoaded, tableOption, tablesField } from "./source-to-query-helpers";

// REQ-2234: Register Table holds Source, Domain, Schema where the source has more than one, and
// one Tables field (the standard multi-select). A table already registered is not offered.

async function openAddForm(page: Page) {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  await page.getByRole("button", { name: /\+ Table/i }).click();
  await page.waitForSelector(".form-card", { timeout: 5000 });
}

test("sqlite lists its one physical schema, 'main', not PG internal schemas", async ({ page }) => {
  await openAddForm(page);
  await page.getByTestId("register-table-source-select").selectOption("inquiries-sqlite");

  // Regression: native_schemas returned None, and the Trino fallback returned internal PG schemas
  // like "pet_store" and "analytics". The SQLite file has one schema, "main": the Schema field is
  // absent, and the tables are listed from it.
  await schemasLoaded(page);
  await expect(page.getByTestId("register-table-schema-select")).toHaveCount(0);
  // Both of the file's tables are registered by the install config, so none is offered.
  await expect(page.getByTestId("register-tables-field")).toContainText(
    "All tables already registered",
    { timeout: 30000 },
  );
});

test("sqlite tables are read from the physical file", async ({ page }) => {
  await openAddForm(page);
  await page.getByTestId("register-table-source-select").selectOption("inquiries-sqlite");
  await page.getByTestId("register-table-domain-select").selectOption("pet-store");

  // The list is read from the file (inquiries, users) and both are registered: the field says so,
  // not "This schema has no tables." and not still listing.
  await schemasLoaded(page);
  const field = page.getByTestId("register-tables-field");
  await expect(field).toContainText("All tables already registered", { timeout: 30000 });
  await expect(field).not.toContainText("This schema has no tables.");
  await expect(tablesField(page)).toBeDisabled();
});

test("openapi tables already registered are not offered", async ({ page }) => {
  await openAddForm(page);
  await page.getByTestId("register-table-source-select").selectOption("petstore-api");
  await page.getByTestId("register-table-domain-select").selectOption("pet-store");

  // An openapi source's schema is fixed to one, so the Schema field is absent.
  await schemasLoaded(page);
  await expect(page.getByTestId("register-table-schema-select")).toHaveCount(0);

  // The spec's operations are listed; the install config registered six of them, so some remain.
  const field = tablesField(page);
  await expect(field).toBeEnabled({ timeout: 30000 });
  await field.click();
  const options = page.locator("[data-combobox-option]");
  await expect(options.first()).toBeVisible({ timeout: 30000 });

  // Names are normalized (camelCase to snake_case) before they are compared with the registered
  // names, so "findPetsByStatus" in the spec matches "find_pets_by_status" registered from YAML.
  for (const name of ["findPetsByStatus", "findPetsByTags", "find_pets_by_status"]) {
    await expect(tableOption(page, name), `${name} is registered, so not offered`).toHaveCount(0);
  }
  await page.keyboard.press("Escape");
});
